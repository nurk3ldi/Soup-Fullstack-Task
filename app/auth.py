import hashlib
import hmac
import os
import secrets
import time
from dataclasses import dataclass
from uuid import UUID

from fastapi import Request
from fastapi.routing import APIRoute

from app import ratelimit
from app.errors import ApiError, unauthorized

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
    cached = getattr(request.state, "principal", None)
    if cached is not None:  # already resolved for this request by AuthFirstRoute
        return cached
    row = await request.app.state.pool.fetchrow(
        "SELECT id, project_id FROM api_keys WHERE key_hash = $1 AND revoked_at IS NULL",
        hash_key(_bearer(request)),
    )
    if row is None:
        raise unauthorized()
    request.state.project_id = str(row["project_id"])
    request.state.principal = Principal(project_id=row["project_id"], key_id=row["id"])
    return request.state.principal


async def require_admin(request: Request) -> None:
    expected = os.environ.get("ADMIN_TOKEN", "")
    # Fail closed: with no ADMIN_TOKEN configured nobody can create projects.
    if not expected or not hmac.compare_digest(_bearer(request).encode(), expected.encode()):
        raise unauthorized()


def _depends_on(dependant, call) -> bool:
    return any(d.call is call or _depends_on(d, call) for d in dependant.dependencies)


class AuthFirstRoute(APIRoute):
    """Checks the credential before FastAPI touches the request body.

    FastAPI reads and JSON-decodes the body before it runs dependencies, so without this a caller
    with no key and a broken body got 422 instead of 401, and unauthenticated input reached the
    parser. The dependencies still run afterwards; require_key reuses the cached principal.
    """

    def get_route_handler(self):
        handler = super().get_route_handler()
        checks = [c for c in (require_admin, require_key) if _depends_on(self.dependant, c)]

        async def auth_first(request: Request):
            if checks:
                ratelimit.guard_address(request)
                try:
                    for check in checks:
                        await check(request)
                except ApiError as exc:
                    if exc.status == 401:
                        ratelimit.credential_failed(request)
                    raise
                principal = getattr(request.state, "principal", None)
                if principal is not None:
                    ratelimit.guard_project(principal.project_id)
            return await handler(request)

        return auth_first


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
