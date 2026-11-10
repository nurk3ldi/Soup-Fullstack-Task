"""Metric ingestion and the snapshot/catch-up read.

Ingest is one transaction: lock the run row, check the idempotency ledger, insert the points that
are new, advance last_seq, NOTIFY. Either all of it commits or none of it does.
"""
import hashlib
import json
from datetime import timezone
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query, Request
from pydantic import AwareDatetime, BaseModel, BeforeValidator, Field, StringConstraints

from app import config
from app.errors import ApiError, not_found
from app.routes import Caller, parse_id

router = APIRouter()

MAX_POINTS = 1000
NOTIFY_CHANNEL = "run_events"


def _number(v):
    # JSON numbers only: no "0.4" strings, no booleans.
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        raise ValueError("number required")
    try:
        return float(v)
    except OverflowError:
        raise ValueError("out of range") from None


PointName = Annotated[str, StringConstraints(strict=True, min_length=1, max_length=64, pattern=r"^[^\x00-\x1f]+$")]
Step = Annotated[int, Field(strict=True, ge=0, le=2**63 - 1)]
Value = Annotated[float, BeforeValidator(_number), Field(allow_inf_nan=False)]


class Point(BaseModel):
    name: PointName
    step: Step
    value: Value
    ts: AwareDatetime | None = None


class Batch(BaseModel):
    batch_id: UUID
    points: Annotated[list[Point], Field(min_length=1, max_length=MAX_POINTS)]


class FaultInjected(Exception):
    """Raised on purpose in TEST_MODE to prove a failing transaction leaves nothing behind."""


def payload_sha(points: list[Point]) -> bytes:
    """Hash of the parsed points, so a byte-different but equal retry still counts as the same."""
    canonical = [
        [p.name, p.step, p.value.hex(), p.ts.astimezone(timezone.utc).isoformat() if p.ts else None]
        for p in points
    ]
    return hashlib.sha256(json.dumps(canonical, separators=(",", ":")).encode()).digest()


# Survivors are numbered after filtering, so duplicates never consume a seq (no gaps).
# "New" is decided under the run row lock, which every writer of this run holds, so the
# NOT EXISTS check cannot race. There is deliberately no ON CONFLICT here: if that reasoning were
# ever wrong, a primary-key error aborts the batch loudly instead of silently leaving a gap.
# Within one batch the first occurrence of a (name, step) wins.
_INSERT_NEW = """
WITH input AS (
  SELECT * FROM unnest($3::text[], $4::bigint[], $5::float8[], $6::timestamptz[])
    WITH ORDINALITY AS t(name, step, value, client_ts, ord)
), fresh AS (
  SELECT DISTINCT ON (i.name, i.step) i.name, i.step, i.value, i.client_ts
  FROM input i
  WHERE NOT EXISTS (
    SELECT 1 FROM metrics m WHERE m.run_id = $1 AND m.name = i.name AND m.step = i.step
  )
  ORDER BY i.name, i.step, i.ord
), ins AS (
  INSERT INTO metrics (run_id, name, step, value, client_ts, seq, batch_id)
  SELECT $1, name, step, value, client_ts,
         $7::bigint + row_number() OVER (ORDER BY name COLLATE "C", step), $2
  FROM fresh
  RETURNING 1
)
SELECT count(*) FROM ins
"""


async def _insert_new(conn, run_id: UUID, batch_id: UUID, points: list[Point], last_seq: int) -> int:
    return await conn.fetchval(
        _INSERT_NEW,
        run_id,
        batch_id,
        [p.name for p in points],
        [p.step for p in points],
        [p.value for p in points],
        [p.ts for p in points],
        last_seq,
    )


@router.post("/runs/{run_id}/metrics")
async def ingest(run_id: str, body: Batch, caller: Caller, request: Request):
    rid = parse_id(run_id)
    sha = payload_sha(body.points)
    fault = config.TEST_MODE and request.headers.get("x-test-fault") == "mid-insert"

    async with request.app.state.pool.acquire() as conn:
        async with conn.transaction():
            # Row lock: serializes seq assignment for this run only. Also the ownership check.
            last_seq = await conn.fetchval(
                "SELECT last_seq FROM runs WHERE id = $1 AND project_id = $2 FOR UPDATE",
                rid,
                caller.project_id,
            )
            if last_seq is None:
                raise not_found()

            prior = await conn.fetchrow(
                "SELECT payload_sha, accepted FROM batches WHERE run_id = $1 AND batch_id = $2",
                rid,
                body.batch_id,
            )
            if prior is not None:
                if prior["payload_sha"] != sha:
                    raise ApiError(409, "batch_id_reused")
                return {"duplicate": True, "accepted": prior["accepted"]}

            if fault:
                await _insert_new(conn, rid, body.batch_id, body.points[: len(body.points) // 2], last_seq)
                raise FaultInjected()

            accepted = await _insert_new(conn, rid, body.batch_id, body.points, last_seq)
            new_last = last_seq + accepted
            await conn.execute(
                "INSERT INTO batches (run_id, batch_id, payload_sha, accepted) VALUES ($1, $2, $3, $4)",
                rid,
                body.batch_id,
                sha,
                accepted,
            )
            if accepted:
                await conn.execute("UPDATE runs SET last_seq = $2 WHERE id = $1", rid, new_last)
                # Sent by Postgres only if and when this transaction commits.
                await conn.execute("SELECT pg_notify($1, $2)", NOTIFY_CHANNEL, f"{rid}:{new_last}")

    return {"accepted": accepted, "duplicates": len(body.points) - accepted, "last_seq": new_last}


def point_out(row) -> dict:
    return {
        "name": row["name"],
        "step": row["step"],
        "value": row["value"],
        "seq": row["seq"],
        "ts": row["client_ts"].isoformat() if row["client_ts"] else None,
    }


async def read_after(conn, run_id: UUID, since_seq: int, limit: int):
    return await conn.fetch(
        "SELECT name, step, value, seq, client_ts FROM metrics "
        "WHERE run_id = $1 AND seq > $2 ORDER BY seq LIMIT $3",
        run_id,
        since_seq,
        limit,
    )


@router.get("/runs/{run_id}/metrics")
async def read_metrics(
    run_id: str,
    caller: Caller,
    request: Request,
    since_seq: Annotated[int, Query(ge=0, le=2**63 - 1)] = 0,
    limit: Annotated[int, Query(ge=1, le=5000)] = 1000,
):
    rid = parse_id(run_id)
    async with request.app.state.pool.acquire() as conn:
        # One snapshot for both reads, so last_seq and the points always agree with each other.
        async with conn.transaction(isolation="repeatable_read", readonly=True):
            last_seq = await conn.fetchval(
                "SELECT last_seq FROM runs WHERE id = $1 AND project_id = $2", rid, caller.project_id
            )
            if last_seq is None:
                raise not_found()
            rows = await read_after(conn, rid, since_seq, limit)
    points = [point_out(r) for r in rows]
    reached = points[-1]["seq"] if points else since_seq
    return {"points": points, "last_seq": last_seq, "has_more": reached < last_seq}
