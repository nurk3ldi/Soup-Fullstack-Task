import json
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.datastructures import MutableHeaders

from app import config, db, errors, ingest, routes, stream
from app.bus import Bus
from app import logging as applog
from app.logging import log

applog.setup(test_mode=config.TEST_MODE)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await db.create_pool()
    await db.apply_schema(app.state.pool)
    app.state.bus = Bus(os.environ["DATABASE_URL"])
    await app.state.bus.start()
    try:
        yield
    finally:
        await app.state.bus.stop()
        await app.state.pool.close()


class RequestContextMiddleware:
    """Request id, no-cache headers and one JSON log line per request.

    Plain ASGI (not BaseHTTPMiddleware) so that long-lived streaming responses pass through
    untouched.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        request_id = uuid4().hex
        state = scope.setdefault("state", {})
        state["request_id"] = request_id
        started = time.perf_counter()
        status = None

        async def send_wrapper(message):
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
                headers = MutableHeaders(scope=message)
                # Responses depend on the caller's key: never let a cache share them.
                headers["Cache-Control"] = "no-store, private"
                headers.add_vary_header("Authorization")
                headers["X-Request-ID"] = request_id
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        except Exception:
            log.exception("unhandled error", extra={"fields": {"request_id": request_id}})
            if status is None:
                # Compact separators: byte-identical in shape to every other error body.
                body = json.dumps({"error": "internal"}, separators=(",", ":")).encode()
                await send_wrapper(
                    {
                        "type": "http.response.start",
                        "status": 500,
                        "headers": [(b"content-type", b"application/json")],
                    }
                )
                await send_wrapper({"type": "http.response.body", "body": body})
        finally:
            query = scope.get("query_string", b"").decode("latin-1")
            log.info(
                "request",
                extra={
                    "fields": {
                        "request_id": request_id,
                        "method": scope["method"],
                        "path": scope["path"] + ("?" + applog.safe_query(query) if query else ""),
                        "status": status,
                        "duration_ms": round((time.perf_counter() - started) * 1000, 2),
                        "project_id": state.get("project_id"),
                    }
                },
            )


app = FastAPI(title="Mini Experiment Tracker", lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
app.add_middleware(RequestContextMiddleware)
errors.install(app)
app.include_router(routes.router)
app.include_router(ingest.router)
app.include_router(stream.router)
if config.TEST_MODE:
    app.include_router(stream.debug_router)

    @app.get("/debug/logs")
    async def debug_logs():
        return {"lines": list(applog.ring.lines)}


@app.get("/healthz")
async def healthz(request: Request):
    try:
        await request.app.state.pool.fetchval("SELECT 1")
    except Exception:
        return JSONResponse({"error": "unavailable"}, status_code=503)
    return {"status": "ok"}


# --- UI: static pages. They contain no data; everything is fetched by the browser with the key.
STATIC_DIR = Path(__file__).parent / "static"
PAGE_HEADERS = {
    # No inline scripts or styles, nothing from other origins: a metric name cannot become code.
    "Content-Security-Policy": (
        "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; "
        "img-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
    ),
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "no-referrer",
}
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
async def index_page():
    return FileResponse(STATIC_DIR / "index.html", headers=PAGE_HEADERS)


@app.get("/runs/{run_id}/view")
async def run_page(run_id: str):
    # The same file for every id, existing or not, with no lookup: the page itself reveals nothing.
    return FileResponse(STATIC_DIR / "run.html", headers=PAGE_HEADERS)
