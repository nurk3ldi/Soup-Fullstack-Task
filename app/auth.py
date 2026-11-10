import hashlib
import hmac
import os
import secrets
import time
from dataclasses import dataclass
from uuid import UUID

from fastapi import Request

from app.errors import unauthorized

KEY_PREFIX_LEN = 8


@dataclass(frozen=True)
class Principal:
    project_id: UUID
    key_id: UUID


def new_key() -> str:
    return "sk_" + secrets.token_urlsafe(32)


def hash_key(key: str) -> bytes:
    # Keys are 256 bits of randomness, so a plain fast hash is enough (no salt/KDF needed).
    return hashlib.sha256(key.encode()).digest()


def _bearer(request: Request) -> str:
    scheme, _, token = request.headers.get("authorization", "").partition(" ")
    return token.strip() if scheme.lower() == "bearer" else ""


async def require_key(request: Request) -> Principal:
    """Resolve the API key to its project. Missing, malformed, unknown and revoked keys all take
    the same path: one hash, one query, the same 401."""
    row = await request.app.state.pool.fetchrow(
        "SELECT id, project_id FROM api_keys WHERE key_hash = $1 AND revoked_at IS NULL",
        hash_key(_bearer(request)),
    )
    if row is None:
        raise unauthorized()
    request.state.project_id = str(row["project_id"])
    return Principal(project_id=row["project_id"], key_id=row["id"])


async def require_admin(request: Request) -> None:
    expected = os.environ.get("ADMIN_TOKEN", "")
    # Fail closed: with no ADMIN_TOKEN configured nobody can create projects.
    if not expected or not hmac.compare_digest(_bearer(request).encode(), expected.encode()):
        raise unauthorized()


async def key_is_active(pool, key_id: UUID) -> bool:
    return await pool.fetchval("SELECT 1 FROM api_keys WHERE id = $1 AND revoked_at IS NULL", key_id) is not None


# --- stream tokens ---------------------------------------------------------------------------
# With several API replicas STREAM_SECRET must be set to the same value on all of them; without
# it each process invents its own secret and tokens only work on the process that minted them.
_STREAM_SECRET = (os.environ.get("STREAM_SECRET") or secrets.token_hex(32)).encode()


def _sign(payload: str) -> str:
    return hmac.new(_STREAM_SECRET, payload.encode(), hashlib.sha256).hexdigest()


def mint_stream_token(principal: Principal, run_id: UUID, ttl_seconds: int) -> str:
    payload = f"{principal.project_id.hex}.{run_id.hex}.{principal.key_id.hex}.{int(time.time()) + ttl_seconds}"
    return f"{payload}.{_sign(payload)}"


def verify_stream_token(token: str, run_id: UUID) -> Principal | None:
    """Authentic, unexpired and minted for exactly this run, or None. Never raises."""
    try:
        payload, _, signature = token.rpartition(".")
        if not hmac.compare_digest(signature.encode(), _sign(payload).encode()):
            return None
        project_hex, run_hex, key_hex, expires = payload.split(".")
        if int(expires) <= time.time() or run_hex != run_id.hex:
            return None
        return Principal(project_id=UUID(project_hex), key_id=UUID(key_hex))
    except (ValueError, UnicodeError):
        return None
