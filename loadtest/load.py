"""Load test: N concurrent runs, each producing R events/s in batches of B, with one SSE subscriber
per run. Not part of pytest. See loadtest/run.sh for how results/load.txt is produced.

    python loadtest/load.py --runs 50 --rate 10 --batch 10 --duration 60

Method notes (they matter for reading the numbers):
- Open loop: every batch is sent at its scheduled time whether or not earlier ones have been
  answered, so a slow server cannot slow the load down and hide its own latency. How late the
  generator itself was is reported as "generator lateness".
- Each point's value is the wall-clock time it was sent; lag = time received on SSE - value.
  Sender and receiver are this one process, so there is no clock skew in the lag.
- Each run has ONE persistent HTTP connection and sends its batches over it one after another,
  like a single training process would. If a response is slow, the next batch waits behind it;
  that wait is included in "latency from schedule" (so slowness cannot hide), while "service
  latency" is only request-sent -> response-received.
- Ingest uses a minimal hand-written HTTP/1.1 client instead of httpx. With httpx the generator
  itself collapsed at 500 req/s (its connection pool spent ~150 ms of CPU per request once
  requests queued up) while the API sat idle; httpx is still used for setup and for SSE.
- A failed or timed-out request is retried with the SAME batch_id (up to 3 times), as a real
  client would; retries and errors are counted.
- The generator is one Python process sharing the machine with the API and Postgres. Its own
  CPU limits what it can measure; compare with the docker stats lines in results/load.txt.
"""
import argparse
import asyncio
import json
import os
import statistics
import time
import uuid
from urllib.parse import urlparse

import httpx


def pct(values: list[float], q: float) -> float:
    if not values:
        return float("nan")
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(len(ordered) * q))]


def describe(values: list[float], unit: str = "ms", scale: float = 1000.0) -> str:
    if not values:
        return "no samples"
    return (f"p50 {pct(values, 0.50) * scale:.1f} {unit}, p95 {pct(values, 0.95) * scale:.1f} {unit}, "
            f"p99 {pct(values, 0.99) * scale:.1f} {unit}, max {max(values) * scale:.1f} {unit} (n={len(values)})")


class Stats:
    def __init__(self):
        self.latency: list[float] = []  # service latency of successful ingest requests
        self.latency_sched: list[float] = []  # same requests, measured from their scheduled send time
        self.lateness: list[float] = []  # how late each batch was fired vs its schedule
        self.lag: list[float] = []  # point sent -> point received on SSE
        self.errors: dict[str, int] = {}
        self.retries = 0
        self.gave_up = 0
        self.points_scheduled = 0
        self.points_acked = 0
        self.duplicate_acks = 0
        self.received = 0
        self.seq_gaps = 0
        self.seq_repeats = 0
        self.sse_reconnects = 0

    def error(self, kind: str):
        self.errors[kind] = self.errors.get(kind, 0) + 1


class RunConnection:
    """One keep-alive HTTP/1.1 connection, used by one run, one request at a time."""

    def __init__(self, host: str, port: int, auth: str):
        self.host, self.port, self.auth = host, port, auth
        self.reader = self.writer = None
        self.lock = asyncio.Lock()

    async def close(self):
        if self.writer is not None:
            self.writer.close()
            self.reader = self.writer = None

    async def post(self, path: str, body: bytes) -> tuple[int, bytes]:
        if self.writer is None:
            self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        head = (f"POST {path} HTTP/1.1\r\nHost: {self.host}\r\nAuthorization: {self.auth}\r\n"
                f"Content-Type: application/json\r\nContent-Length: {len(body)}\r\n\r\n").encode()
        self.writer.write(head + body)
        await self.writer.drain()
        raw = await self.reader.readuntil(b"\r\n\r\n")
        status = int(raw[9:12])
        length = 0
        for line in raw.split(b"\r\n")[1:]:
            name, _, value = line.partition(b":")
            if name.strip().lower() == b"content-length":
                length = int(value)
        return status, await self.reader.readexactly(length)


async def send_batch(conn: RunConnection, stats: Stats, run_id: str, first_step: int, size: int, scheduled: float):
    batch_id = str(uuid.uuid4())
    path = f"/runs/{run_id}/metrics"
    async with conn.lock:  # one request at a time per run, in order
        for attempt in range(4):
            now = time.time()
            points = [{"name": f"m{(first_step + j) % 3}", "step": first_step + j, "value": now} for j in range(size)]
            body = json.dumps({"batch_id": batch_id, "points": points}).encode()
            t0 = time.perf_counter()
            try:
                status, payload = await asyncio.wait_for(conn.post(path, body), timeout=10)
            except (OSError, asyncio.IncompleteReadError, asyncio.LimitOverrunError, asyncio.TimeoutError, ValueError) as exc:
                stats.error(type(exc).__name__)
                await conn.close()
            else:
                if status == 200:
                    done = time.perf_counter()
                    stats.latency.append(done - t0)
                    stats.latency_sched.append(done - scheduled)
                    if b'"duplicate"' in payload:
                        stats.duplicate_acks += 1
                    stats.points_acked += size
                    return
                stats.error(f"HTTP {status}")
            if attempt < 3:
                stats.retries += 1
                await asyncio.sleep(0.2 * (attempt + 1))
    stats.gave_up += 1


async def producer(conn: RunConnection, stats: Stats, run_id: str, index: int, args, t0: float):
    loop = asyncio.get_running_loop()
    interval = args.batch / args.rate
    start = t0 + (index / args.runs) * interval  # spread the runs across one interval
    tasks = []
    for k in range(int(args.duration / interval)):
        target = start + k * interval
        delay = target - loop.time()
        if delay > 0:
            await asyncio.sleep(delay)
        stats.lateness.append(max(0.0, loop.time() - target))  # measured after waking up
        stats.points_scheduled += args.batch
        scheduled = time.perf_counter() - max(0.0, loop.time() - target)
        tasks.append(asyncio.create_task(send_batch(conn, stats, run_id, k * args.batch, args.batch, scheduled)))
    await asyncio.gather(*tasks)
    await conn.close()


async def subscriber(client, stats: Stats, headers: dict, run_id: str, ready: asyncio.Event, stop: asyncio.Event):
    cursor = 0
    while not stop.is_set():
        try:
            r = await client.post(f"/runs/{run_id}/stream-token", headers=headers)
            token = r.json()["token"]
            async with client.stream("GET", f"/runs/{run_id}/stream", params={"token": token},
                                     headers={"Last-Event-ID": str(cursor)}, timeout=httpx.Timeout(10, read=None)) as resp:
                ready.set()
                is_metric = False
                async for line in resp.aiter_lines():
                    if line.startswith("event:"):
                        is_metric = line.endswith("metric")
                    elif is_metric and line.startswith("data:"):
                        point = json.loads(line[5:])
                        stats.lag.append(time.time() - point["value"])
                        stats.received += 1
                        if point["seq"] > cursor + 1:
                            stats.seq_gaps += 1
                        elif point["seq"] <= cursor:
                            stats.seq_repeats += 1
                        cursor = max(cursor, point["seq"])
        except (httpx.HTTPError, KeyError, json.JSONDecodeError):
            pass
        if not stop.is_set():
            stats.sse_reconnects += 1
            await asyncio.sleep(0.5)


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--api", default=os.environ.get("PLAIN_API_URL", "http://localhost:8000"))
    ap.add_argument("--runs", type=int, default=50)
    ap.add_argument("--rate", type=float, default=10, help="events per second per run")
    ap.add_argument("--batch", type=int, default=10, help="points per request")
    ap.add_argument("--duration", type=float, default=60)
    args = ap.parse_args()
    admin = {"Authorization": "Bearer " + os.environ.get("ADMIN_TOKEN", "dev-admin-token")}
    stats = Stats()
    limits = httpx.Limits(max_connections=400, max_keepalive_connections=400)

    async with httpx.AsyncClient(base_url=args.api, timeout=10, limits=limits) as client, \
            httpx.AsyncClient(base_url=args.api, timeout=10, limits=limits) as sse_client:
        project = (await client.post("/projects", json={"name": f"load-b{args.batch}"}, headers=admin)).json()
        headers = {"Authorization": "Bearer " + project["api_key"]["key"]}
        run_ids = [(await client.post("/runs", json={"name": f"load-{i}"}, headers=headers)).json()["id"] for i in range(args.runs)]

        stop = asyncio.Event()
        ready = [asyncio.Event() for _ in run_ids]
        subs = [asyncio.create_task(subscriber(sse_client, stats, headers, rid, ev, stop)) for rid, ev in zip(run_ids, ready)]
        await asyncio.wait_for(asyncio.gather(*(ev.wait() for ev in ready)), timeout=30)
        stats.sse_reconnects = 0

        loop = asyncio.get_running_loop()
        started = time.perf_counter()
        t0 = loop.time() + 0.5
        target = urlparse(args.api)
        conns = [RunConnection(target.hostname, target.port or 80, headers["Authorization"]) for _ in run_ids]
        # Open the connections one by one before the clock starts. Opened in a single burst they
        # all land on whichever server worker wakes first, which hides the effect of extra workers.
        for conn in conns:
            conn.reader, conn.writer = await asyncio.open_connection(conn.host, conn.port)
            await asyncio.sleep(0.02)
        t0 = loop.time() + 0.5
        started = time.perf_counter()
        await asyncio.gather(*(producer(conn, stats, rid, i, args, t0) for i, (rid, conn) in enumerate(zip(run_ids, conns))))
        send_seconds = time.perf_counter() - started - 0.5

        # Give the streams time to deliver what was acknowledged.
        drain_started = time.perf_counter()
        while stats.received < stats.points_acked and time.perf_counter() - drain_started < 30:
            await asyncio.sleep(0.1)
        drain_seconds = time.perf_counter() - drain_started
        stop.set()
        for task in subs:
            task.cancel()
        await asyncio.gather(*subs, return_exceptions=True)

        listed = (await client.get("/runs", headers=headers)).json()["runs"]
        db_points = sum(r["points"] for r in listed)
        db_last_seq = sum(r["last_seq"] for r in listed)

    requests_ok = len(stats.latency)
    print(f"=== load: {args.runs} runs x {args.rate:g} events/s, batch size {args.batch}, {args.duration:g} s, "
          f"{args.runs} SSE subscribers; target {args.runs * args.rate / args.batch:g} req/s, {args.runs * args.rate:g} points/s")
    print(f"target api           : {args.api}")
    print(f"send phase           : {send_seconds:.1f} s; achieved {requests_ok / send_seconds:.1f} req/s, {stats.points_acked / send_seconds:.1f} points/s")
    print(f"ingest service time  : {describe(stats.latency)}")
    print(f"ingest from schedule : {describe(stats.latency_sched)}")
    print(f"generator lateness   : {describe(stats.lateness)}")
    print(f"errors               : {sum(stats.errors.values())} {stats.errors or ''}; retries {stats.retries}; batches given up {stats.gave_up}; "
          f"acks that were duplicate:true {stats.duplicate_acks}")
    print(f"points               : scheduled {stats.points_scheduled}, acknowledged {stats.points_acked}, "
          f"in DB {db_points} (sum of last_seq {db_last_seq}), received over SSE {stats.received}")
    print(f"sse end-to-end lag   : {describe(stats.lag)}")
    print(f"sse integrity        : seq gaps {stats.seq_gaps}, seq repeats {stats.seq_repeats}, reconnects {stats.sse_reconnects}; "
          f"drain after last send {drain_seconds:.1f} s")
    verdict_inputs = {
        "lost_between_ack_and_db": stats.points_acked - db_points,
        "lost_between_db_and_sse": db_points - stats.received,
    }
    print(f"consistency          : {verdict_inputs}")


if __name__ == "__main__":
    asyncio.run(main())
