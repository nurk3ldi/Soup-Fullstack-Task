import os
import uuid

import asyncpg
import httpx
import pytest

API_URL = os.environ.get("API_URL", "http://localhost:8000")
DATABASE_URL = os.environ["DATABASE_URL"]
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "dev-admin-token")
# The same app without TEST_MODE; shares the database with API_URL.
PLAIN_API_URL = os.environ.get("PLAIN_API_URL", "http://localhost:8000")


def make_points(n: int, name: str = "loss", start: int = 0) -> list[dict]:
    return [{"name": name, "step": start + i, "value": round(1.0 / (start + i + 1), 6)} for i in range(n)]


async def send_batch(client, project, run_id: str, points: list[dict], batch_id: str | None = None, headers: dict | None = None):
    body = {"batch_id": batch_id or str(uuid.uuid4()), "points": points}
    return await client.post(f"/runs/{run_id}/metrics", json=body, headers={**project.headers, **(headers or {})})


async def db_points(db, run_id: str) -> list[tuple]:
    rows = await db.fetch("SELECT name, step, value, seq FROM metrics WHERE run_id = $1 ORDER BY seq", uuid.UUID(run_id))
    return [(r["name"], r["step"], r["value"], r["seq"]) for r in rows]


async def db_run_state(db, run_id: str) -> dict:
    rid = uuid.UUID(run_id)
    return {
        "last_seq": await db.fetchval("SELECT last_seq FROM runs WHERE id = $1", rid),
        "points": await db.fetchval("SELECT count(*) FROM metrics WHERE run_id = $1", rid),
        "batches": await db.fetchval("SELECT count(*) FROM batches WHERE run_id = $1", rid),
    }


def bearer(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


class Project:
    """A freshly created project and its first API key, as a test sees them."""

    def __init__(self, payload: dict):
        self.id = payload["project"]["id"]
        self.key = payload["api_key"]["key"]
        self.key_id = payload["api_key"]["id"]
        self.prefix = payload["api_key"]["prefix"]
        self.headers = bearer(self.key)


@pytest.fixture
async def client():
    async with httpx.AsyncClient(base_url=API_URL, timeout=10) as c:
        yield c


@pytest.fixture
async def plain_client():
    async with httpx.AsyncClient(base_url=PLAIN_API_URL, timeout=10) as c:
        yield c


@pytest.fixture
async def db():
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        yield conn
    finally:
        await conn.close()


@pytest.fixture
def make_project(client):
    async def _make(name: str | None = None) -> Project:
        r = await client.post(
            "/projects",
            json={"name": name or f"proj-{uuid.uuid4().hex[:8]}"},
            headers=bearer(ADMIN_TOKEN),
        )
        assert r.status_code == 201, r.text
        return Project(r.json())

    return _make


@pytest.fixture
def make_run(client):
    async def _make(project: Project, name: str = "run") -> dict:
        r = await client.post("/runs", json={"name": name}, headers=project.headers)
        assert r.status_code == 201, r.text
        return r.json()

    return _make


# --- SSE helpers -----------------------------------------------------------------------------


class SSE:
    """Minimal SSE reader over httpx. Use as `async with SSE(...) as s`."""

    def __init__(self, client, url: str, headers: dict | None = None, params: dict | None = None):
        self._cm = client.stream(
            "GET", url, headers=headers or {}, params=params or {}, timeout=httpx.Timeout(10, read=None)
        )
        self.response = None
        self.pings = 0
        self.hello = None
        self.ended = None  # None while open, else "eof" (clean end) or "dropped" (cut mid-stream)

    async def __aenter__(self):
        self.response = await self._cm.__aenter__()
        self._lines = self.response.aiter_lines()
        return self

    async def __aexit__(self, *exc):
        try:
            await self._cm.__aexit__(None, None, None)
        except httpx.HTTPError:
            pass

    async def next_event(self) -> dict | None:
        """Next event that carries data, or None once the stream has ended."""
        event = {"event": "message", "id": None, "data": None}
        try:
            async for line in self._lines:
                if line == "":
                    if event["event"] == "ping":
                        self.pings += 1
                    elif event["event"] == "hello":
                        self.hello = event["data"]
                    elif event["data"] is not None:
                        return event
                    event = {"event": "message", "id": None, "data": None}
                elif line.startswith(":"):
                    pass
                else:
                    field, _, value = line.partition(":")
                    if field in event:
                        event[field] = value.removeprefix(" ")
            self.ended = "eof"
        except (httpx.RemoteProtocolError, httpx.ReadError):
            self.ended = "dropped"
        return None

    async def metrics(self, n: int, timeout: float = 10) -> list[dict]:
        """Exactly the next n metric events, parsed. Fails if the stream ends or time runs out."""
        import asyncio
        import json

        out = []
        async with asyncio.timeout(timeout):
            while len(out) < n:
                ev = await self.next_event()
                assert ev is not None, f"stream ended ({self.ended}) after {len(out)} of {n} events"
                assert ev["event"] == "metric", ev
                point = json.loads(ev["data"])
                assert ev["id"] == str(point["seq"]), ev
                out.append(point)
        return out


async def stream_token(client, project, run_id: str, headers: dict | None = None) -> str:
    r = await client.post(f"/runs/{run_id}/stream-token", headers={**project.headers, **(headers or {})})
    assert r.status_code == 200, r.text
    return r.json()["token"]


async def open_stream(client, project, run_id: str, last_event_id: int | None = None, since_seq: int | None = None) -> SSE:
    params = {"token": await stream_token(client, project, run_id)}
    if since_seq is not None:
        params["since_seq"] = since_seq
    headers = {} if last_event_id is None else {"Last-Event-ID": str(last_event_id)}
    return SSE(client, f"/runs/{run_id}/stream", headers=headers, params=params)


async def debug_stats(client) -> dict:
    r = await client.get("/debug/stats")
    assert r.status_code == 200, r.text
    return r.json()


@pytest.fixture
async def stream_lifetime(client):
    """Set the server's maximum stream lifetime (TEST_MODE hook); restored after the test."""

    async def _set(seconds: float) -> None:
        r = await client.post(f"/debug/stream-lifetime/{seconds}")
        assert r.status_code == 200, r.text

    try:
        yield _set
    finally:
        await client.post("/debug/stream-lifetime/0")


@pytest.fixture
async def rate_limits(client):
    """Switch the server's rate limiters on with the given numbers (TEST_MODE hook). Every call
    starts from empty buckets; the configured values (off, on the test service) come back after."""

    async def _set(**limits) -> dict:
        base = {"project_rps": 0, "project_burst": 0, "auth_fail_rps": 0, "auth_fail_burst": 0}
        r = await client.post("/debug/rate-limits", json={**base, **limits})
        assert r.status_code == 200, r.text
        return r.json()

    try:
        yield _set
    finally:
        await client.post("/debug/rate-limits", json={})


# --- browser ---------------------------------------------------------------------------------


@pytest.fixture
async def page():
    """A fresh headless Chromium page. `page.errors` collects uncaught JS exceptions."""
    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        browser = await p.chromium.launch()
        context = await browser.new_context()
        pg = await context.new_page()
        pg.errors = []
        pg.on("pageerror", lambda exc: pg.errors.append(str(exc)))
        try:
            yield pg
        finally:
            await browser.close()


async def ui_login(page, key: str, path: str = "/", base: str | None = None) -> None:
    await page.goto((base or API_URL) + path)
    await page.fill("#key-input", key)
    await page.click("#login-form button")


async def wait_js(page, expression: str, timeout: float = 10.0) -> None:
    """Poll a JS expression until it is truthy.

    Not page.wait_for_function: that polls with eval() inside the page, which the app's
    Content-Security-Policy (no 'unsafe-eval') rightly blocks. page.evaluate goes through the
    debugger protocol instead, so the CSP stays fully enabled during the tests.
    """
    import asyncio

    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        if await page.evaluate(expression):
            return
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(f"timed out after {timeout}s waiting for: {expression}")
        await asyncio.sleep(0.05)
