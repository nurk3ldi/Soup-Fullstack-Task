"""Live stream of a run's points over Server-Sent Events.

The database is the queue. A connection never holds more than one page of points in memory: it
reads `seq > cursor`, writes the page, and only reads the next page once the client has taken the
previous one. A client that cannot accept a write for SSE_STALL_SECONDS is dropped and counted as
an overflow; when it reconnects it resumes from its cursor and loses nothing.
"""
import asyncio
import json
import os
from collections import defaultdict
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query, Request
from starlette.responses import Response

from app import auth, config
from app.errors import not_found, unauthorized
from app.ingest import point_out, read_after
from app.logging import log
from app.routes import Caller, parse_id

router = APIRouter(route_class=auth.AuthFirstRoute)
debug_router = APIRouter()

PAGE_SIZE = 500  # points per read and per write; the per-connection memory bound
POLL_SECONDS = 2.0  # safety re-read even if no notification arrives
AUTH_CHECK_SECONDS = 5.0  # fallback re-check of the key; revocation normally arrives by NOTIFY
PING_SECONDS = float(os.environ.get("SSE_PING_SECONDS", "15"))
STALL_SECONDS = float(os.environ.get("SSE_STALL_SECONDS", "10"))
TOKEN_TTL_SECONDS = 60


class Stats:
    active_streams = 0
    sse_overflow_total = 0
    max_buffered_points = 0  # largest page any connection ever held


_tasks_by_run: dict[str, set[asyncio.Task]] = defaultdict(set)


class _Dropped(Exception):
    pass


def _format(row) -> bytes:
    data = json.dumps(point_out(row), separators=(",", ":"))
    return f"id: {row['seq']}\nevent: metric\ndata: {data}\n\n".encode()


class EventStream(Response):
    """ASGI response written by hand, because every write needs a timeout (see STALL_SECONDS)."""

    def __init__(self, app, run_id: UUID, key_id: UUID, cursor: int):
        self.status_code = 200
        self.background = None
        self.app = app
        self.run_id = run_id
        self.key_id = key_id
        self.cursor = cursor

    async def __call__(self, scope, receive, send):
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    (b"content-type", b"text/event-stream; charset=utf-8"),
                    (b"x-accel-buffering", b"no"),
                ],
            }
        )
        pump = asyncio.create_task(self._pump(send))
        watcher = asyncio.create_task(self._watch_disconnect(receive))
        tasks = _tasks_by_run[str(self.run_id)]
        tasks.add(pump)
        Stats.active_streams += 1
        try:
            await asyncio.wait({pump, watcher}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            pump.cancel()
            watcher.cancel()
            results = await asyncio.gather(pump, watcher, return_exceptions=True)
            Stats.active_streams -= 1
            tasks.discard(pump)
            if not tasks:
                _tasks_by_run.pop(str(self.run_id), None)

        outcome = results[0]
        if outcome == "complete":
            await send({"type": "http.response.body", "body": b"", "more_body": False})
        elif isinstance(outcome, Exception) and not isinstance(outcome, _Dropped):
            log.error("stream failed", extra={"fields": {"error": type(outcome).__name__}})
        # Otherwise (overflow, test kill, error, client gone) we return without finishing the
        # response, which makes the server drop the connection. The client sees a broken stream
        # and reconnects with its cursor.

    async def _watch_disconnect(self, receive):
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return

    async def _write(self, send, chunk: bytes) -> None:
        try:
            await asyncio.wait_for(
                send({"type": "http.response.body", "body": chunk, "more_body": True}), STALL_SECONDS
            )
        except asyncio.TimeoutError:
            Stats.sse_overflow_total += 1
            log.warning(
                "sse overflow: client stalled, dropping connection",
                extra={"fields": {"run_id": str(self.run_id), "cursor": self.cursor, "sse_overflow_total": Stats.sse_overflow_total}},
            )
            raise _Dropped() from None

    async def _pump(self, send) -> str:
        pool = self.app.state.pool
        loop = asyncio.get_running_loop()
        # Subscribe BEFORE the first read: a commit between "read" and "subscribe" would otherwise
        # be missed until the next poll.
        sub = self.app.state.bus.subscribe(str(self.run_id), str(self.key_id))
        try:
            # Heartbeats are real events, not comments: EventSource hides comments from the page,
            # and the page needs to see them to notice a connection that died without an error.
            hello = json.dumps({"ping_seconds": PING_SECONDS}, separators=(",", ":"))
            await self._write(send, f"retry: 1000\n\nevent: hello\ndata: {hello}\n\n".encode())
            auth_due = loop.time() + AUTH_CHECK_SECONDS  # the key was checked at connect
            ping_due = loop.time() + PING_SECONDS
            while True:
                sub.event.clear()  # anything arriving after this line re-arms the next round

                if sub.auth_dirty or loop.time() >= auth_due:
                    sub.auth_dirty = False
                    if not await auth.key_is_active(pool, self.key_id):
                        await self._write(send, b"event: revoked\ndata: {}\n\n")
                        return "complete"
                    auth_due = loop.time() + AUTH_CHECK_SECONDS

                rows = await read_after(pool, self.run_id, self.cursor, PAGE_SIZE)
                if rows:
                    Stats.max_buffered_points = max(Stats.max_buffered_points, len(rows))
                    await self._write(send, b"".join(_format(r) for r in rows))
                    self.cursor = rows[-1]["seq"]
                    ping_due = loop.time() + PING_SECONDS
                    if len(rows) == PAGE_SIZE:
                        continue  # more may be waiting; do not sleep

                if loop.time() >= ping_due:
                    await self._write(send, b"event: ping\ndata: {}\n\n")
                    ping_due = loop.time() + PING_SECONDS

                timeout = max(0.0, min(POLL_SECONDS, auth_due - loop.time(), ping_due - loop.time()))
                try:
                    await asyncio.wait_for(sub.event.wait(), timeout)
                except asyncio.TimeoutError:
                    pass
        finally:
            self.app.state.bus.unsubscribe(sub)


@router.post("/runs/{run_id}/stream-token")
async def stream_token(run_id: str, caller: Caller, request: Request):
    rid = parse_id(run_id)
    owned = await request.app.state.pool.fetchval(
        "SELECT 1 FROM runs WHERE id = $1 AND project_id = $2", rid, caller.project_id
    )
    if owned is None:
        raise not_found()
    ttl = TOKEN_TTL_SECONDS
    if config.TEST_MODE and "x-test-token-ttl" in request.headers:
        ttl = int(request.headers["x-test-token-ttl"])
    return {"token": auth.mint_stream_token(caller, rid, ttl), "expires_in": ttl}


def _resume_point(request: Request, since_seq: int | None) -> tuple[int, str]:
    """Last-Event-ID wins over ?since_seq, which wins over 0. Also says which one was used."""
    raw = request.headers.get("last-event-id", "")
    if raw.isdecimal() and int(raw) <= 2**63 - 1:
        return int(raw), "last-event-id"
    if since_seq is not None:
        return since_seq, "since_seq"
    return 0, "start"


@router.get("/runs/{run_id}/stream")
async def stream(
    run_id: str,
    request: Request,
    token: str = "",
    since_seq: Annotated[int | None, Query(ge=0, le=2**63 - 1)] = None,
):
    # EventSource cannot send an Authorization header, so the credential is a short-lived token
    # bound to (project, run, key). Every way of failing here is the same 401.
    try:
        rid = UUID(run_id)
    except ValueError:
        raise unauthorized() from None
    principal = auth.verify_stream_token(token, rid)
    if principal is None:
        raise unauthorized()
    # Token is authentic and unexpired; now the facts it claims must still be true.
    still_valid = await request.app.state.pool.fetchval(
        "SELECT 1 FROM api_keys k JOIN runs r ON r.project_id = k.project_id "
        "WHERE k.id = $1 AND k.project_id = $2 AND k.revoked_at IS NULL AND r.id = $3",
        principal.key_id,
        principal.project_id,
        rid,
    )
    if still_valid is None:
        raise unauthorized()
    request.state.project_id = str(principal.project_id)
    cursor, source = _resume_point(request, since_seq)
    log.info(
        "stream open",
        extra={"fields": {"request_id": request.state.request_id, "run_id": str(rid), "cursor": cursor, "resume_source": source}},
    )
    return EventStream(request.app, rid, principal.key_id, cursor)


# --- test-only hooks: this router is mounted only when TEST_MODE=1 ---------------------------


@debug_router.get("/debug/stats")
async def debug_stats(request: Request):
    with open("/proc/self/statm") as f:
        rss_kb = int(f.read().split()[1]) * os.sysconf("SC_PAGE_SIZE") // 1024
    bus = request.app.state.bus
    return {
        "active_streams": Stats.active_streams,
        "sse_overflow_total": Stats.sse_overflow_total,
        "max_buffered_points": Stats.max_buffered_points,
        "page_size": PAGE_SIZE,
        "stall_seconds": STALL_SECONDS,
        "rss_kb": rss_kb,
        "bus_connected": bus.connected,
        "bus_reconnects": bus.reconnects,
    }


@debug_router.post("/debug/kill-streams/{run_id}")
async def debug_kill_streams(run_id: str):
    """Abruptly drop every open stream of a run, as a crashed proxy or server would."""
    tasks = list(_tasks_by_run.get(run_id, ()))
    for task in tasks:
        task.cancel()
    return {"killed": len(tasks)}
