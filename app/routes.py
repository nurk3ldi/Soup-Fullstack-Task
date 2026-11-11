"""Projects, API keys and runs. Every query is scoped by the project_id of the caller's key."""
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, StringConstraints

from app import auth
from app.errors import not_found

router = APIRouter(route_class=auth.AuthFirstRoute)

# No control characters: Postgres text cannot hold NUL, and that must be a 422, not a 500.
Name = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=200, pattern=r"^[^\x00-\x1f]+$"),
]
Caller = Annotated[auth.Principal, Depends(auth.require_key)]


class NameIn(BaseModel):
    name: Name


def parse_id(raw: str) -> UUID:
    """A malformed id is just another id that does not exist."""
    try:
        return UUID(raw)
    except ValueError:
        raise not_found() from None


def _key_out(row, plaintext: str) -> dict:
    return {
        "id": str(row["id"]),
        "key": plaintext,  # shown once; only its sha256 is stored
        "prefix": row["prefix"],
        "created_at": row["created_at"].isoformat(),
    }


def _run_out(row) -> dict:
    return {
        "id": str(row["id"]),
        "name": row["name"],
        "created_at": row["created_at"].isoformat(),
        "last_seq": row["last_seq"],
        "points": row["points"],
    }


async def _insert_key(conn, project_id: UUID) -> dict:
    key = auth.new_key()
    row = await conn.fetchrow(
        "INSERT INTO api_keys (project_id, key_hash, prefix) VALUES ($1, $2, $3) "
        "RETURNING id, prefix, created_at",
        project_id,
        auth.hash_key(key),
        key[: auth.KEY_PREFIX_LEN],
    )
    return _key_out(row, key)


@router.post("/projects", status_code=201, dependencies=[Depends(auth.require_admin)])
async def create_project(body: NameIn, request: Request):
    async with request.app.state.pool.acquire() as conn:
        async with conn.transaction():
            project = await conn.fetchrow(
                "INSERT INTO projects (name) VALUES ($1) RETURNING id, name, created_at", body.name
            )
            key = await _insert_key(conn, project["id"])
    return {
        "project": {
            "id": str(project["id"]),
            "name": project["name"],
            "created_at": project["created_at"].isoformat(),
        },
        "api_key": key,
    }


@router.post("/projects/me/keys", status_code=201)
async def create_key(caller: Caller, request: Request):
    async with request.app.state.pool.acquire() as conn:
        return await _insert_key(conn, caller.project_id)


@router.post("/projects/me/keys/{key_id}/revoke")
async def revoke_key(key_id: str, caller: Caller, request: Request):
    # Idempotent: revoking twice keeps the first revocation time.
    # The NOTIFY tells open streams of this key to re-check it now instead of at the next poll.
    row = await request.app.state.pool.fetchrow(
        "WITH revoked AS ("
        "  UPDATE api_keys SET revoked_at = COALESCE(revoked_at, now()) "
        "  WHERE id = $1 AND project_id = $2 RETURNING id"
        ") SELECT id, pg_notify('key_revoked', id::text) FROM revoked",
        parse_id(key_id),
        caller.project_id,
    )
    if row is None:
        raise not_found()
    return {"id": str(row["id"]), "revoked": True}


_RUN_SELECT = (
    "SELECT r.id, r.name, r.created_at, r.last_seq, "
    "(SELECT count(*) FROM metrics m WHERE m.run_id = r.id) AS points "
    "FROM runs r "
)


@router.post("/runs", status_code=201)
async def create_run(body: NameIn, caller: Caller, request: Request):
    row = await request.app.state.pool.fetchrow(
        "INSERT INTO runs (project_id, name) VALUES ($1, $2) "
        "RETURNING id, name, created_at, last_seq, 0::bigint AS points",
        caller.project_id,
        body.name,
    )
    return _run_out(row)


@router.get("/runs")
async def list_runs(caller: Caller, request: Request):
    rows = await request.app.state.pool.fetch(
        _RUN_SELECT + "WHERE r.project_id = $1 ORDER BY r.created_at DESC, r.id", caller.project_id
    )
    return {"runs": [_run_out(r) for r in rows]}


@router.get("/runs/{run_id}")
async def get_run(run_id: str, caller: Caller, request: Request):
    # One query for "missing" and "someone else's": the two cases cannot be told apart.
    row = await request.app.state.pool.fetchrow(
        _RUN_SELECT + "WHERE r.id = $1 AND r.project_id = $2", parse_id(run_id), caller.project_id
    )
    if row is None:
        raise not_found()
    return _run_out(row)
