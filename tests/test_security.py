"""Security hardening: response headers, request body limit, stream cap, container privileges.

Each test here guards one fix from the security review recorded in SECURITY.md.
"""
import asyncio
import json
import time
import uuid

import httpx

from conftest import db_run_state, debug_stats, make_points, open_stream, send_batch, stream_token

UNAUTHORIZED = b'{"error":"unauthorized"}'
TOO_LARGE = b'{"error":"payload_too_large"}'
JSON = {"Content-Type": "application/json"}

REQUIRED_HEADERS = {
    "x-content-type-options": "nosniff",
    "x-frame-options": "DENY",
    "referrer-policy": "no-referrer",
    "cross-origin-resource-policy": "same-origin",
    "cache-control": "no-store, private",
}


def check_headers(r, label):
    for name, value in REQUIRED_HEADERS.items():
        assert r.headers.get(name) == value, (label, name, r.headers.get(name))
    csp = r.headers.get("content-security-policy", "")
    assert "default-src 'none'" in csp and "frame-ancestors 'none'" in csp, (label, csp)
    for directive in ("unsafe-inline", "unsafe-eval", "*", "data:", "http:"):
        assert directive not in csp, (label, directive, csp)
    assert "permissions-policy" in r.headers, label
    # Nothing that tells an attacker what is running.
    for name in ("server", "x-powered-by"):
        assert name not in r.headers, (label, name, r.headers.get(name))
    assert "set-cookie" not in r.headers, label


async def test_security_headers_on_every_kind_of_response(client, plain_client, make_project, make_run):
    a = await make_project()
    run = await make_run(a)
    rid = run["id"]
    for server, c in (("test-mode api", client), ("plain api", plain_client)):
        cases = {
            "login page": ("GET", "/", {}),
            "run page": ("GET", f"/runs/{rid}/view", {}),
            "script": ("GET", "/static/app.js", {}),
            "stylesheet": ("GET", "/static/style.css", {}),
            "health": ("GET", "/healthz", {}),
            "api 200": ("GET", f"/runs/{rid}", a.headers),
            "api list": ("GET", "/runs", a.headers),
            "api 401": ("GET", "/runs", {}),
            "api 404": ("GET", f"/runs/{uuid.uuid4()}", a.headers),
            "api 405": ("DELETE", f"/runs/{rid}", a.headers),
            "api 422": ("POST", "/runs", {**a.headers, **JSON}),
            "unknown path": ("GET", "/nope", {}),
            "missing static file": ("GET", "/static/nope.js", {}),
        }
        for label, (method, path, headers) in cases.items():
            r = await c.request(method, path, headers=headers)
            check_headers(r, f"{server}: {label}")

    # The pages keep their own, wider policy (they must load their script and stylesheet).
    page = await client.get("/")
    assert "script-src 'self'" in page.headers["content-security-policy"]
    # The stream too: its headers are written by hand, not by the framework.
    async with await open_stream(client, a, rid) as s:
        check_headers(s.response, "event stream")
        assert s.response.headers["content-type"].startswith("text/event-stream")


async def test_body_over_the_limit_is_refused_before_it_is_read(client, db, make_project, make_run):
    a = await make_project()
    run = await make_run(a)
    limit = (await debug_stats(client))["max_body_bytes"]
    url = f"/runs/{run['id']}/metrics"
    headers = {**a.headers, **JSON}

    # 1. Declared size over the limit: refused on the Content-Length header alone.
    big = json.dumps({"batch_id": str(uuid.uuid4()), "points": make_points(1000), "pad": "x" * (limit + 1)}).encode()
    t0 = time.perf_counter()
    r = await client.post(url, content=big, headers=headers, timeout=30)
    declared_ms = (time.perf_counter() - t0) * 1000
    assert (r.status_code, r.content) == (413, TOO_LARGE)

    # 2. No Content-Length (chunked): refused as soon as the running total passes the limit.
    async def chunks():
        yield b'{"batch_id": "%s", "points": [], "pad": "' % str(uuid.uuid4()).encode()
        for _ in range(limit // 65536 + 2):
            yield b"x" * 65536
        yield b'"}'

    r = await client.post(url, content=chunks(), headers=headers, timeout=30)
    assert r.request.headers.get("transfer-encoding") == "chunked"
    assert (r.status_code, r.content) == (413, TOO_LARGE)

    # 3. Other endpoints with a body are covered by the same limit.
    r = await client.post("/runs", content=json.dumps({"name": "n" * (limit + 1)}).encode(), headers=headers, timeout=30)
    assert (r.status_code, r.content) == (413, TOO_LARGE)

    # 4. A caller without a key is told 401, not 413: its body is never looked at.
    r = await client.post(url, content=big, headers=JSON, timeout=30)
    assert (r.status_code, r.content) == (401, UNAUTHORIZED)

    assert await db_run_state(db, run["id"]) == {"last_seq": 0, "points": 0, "batches": 0}

    # 5. The limit leaves room for the largest valid batch: 1000 points, 64-character names made
    #    of characters that JSON escapes to 12 bytes each, with timestamps.
    worst = [{"name": "\U0001f600" * 64, "step": 2**63 - 1 - i, "value": -1.7976931348623157e308, "ts": "2026-11-10T15:00:00.123456+05:00"} for i in range(1000)]
    body = json.dumps({"batch_id": str(uuid.uuid4()), "points": worst}).encode()  # ensure_ascii: \\ud83d\\ude00
    assert len(body) < limit
    r = await client.post(url, content=body, headers=headers, timeout=30)
    assert r.status_code == 200 and r.json()["accepted"] == 1000
    print(f"\n[body-limit] limit={limit} bytes; {len(big)} byte body refused in {declared_ms:.0f} ms; largest valid batch is {len(body)} bytes")


async def test_open_streams_per_project_are_capped(client, make_project, make_run):
    a, b = await make_project(), await make_project()
    ra, rb = await make_run(a), await make_run(b)
    before = await debug_stats(client)
    cap = before["max_streams_per_project"]
    assert cap <= 20, "the test service is expected to run with a small cap"

    streams = [await open_stream(client, a, ra["id"]) for _ in range(cap)]
    opened = []
    try:
        for s in streams:
            await s.__aenter__()
            opened.append(s)
            assert s.response.status_code == 200
        # Racing connects must not overshoot the cap either.
        tokens = [await stream_token(client, a, ra["id"]) for _ in range(5)]
        extra = await asyncio.gather(*(client.get(f"/runs/{ra['id']}/stream", params={"token": t}) for t in tokens))
        for r in extra:
            assert (r.status_code, r.content) == (429, b'{"error":"too_many_streams"}')
            assert r.headers["retry-after"] == "5"
            assert "text/event-stream" not in r.headers["content-type"]

        # Another project is not affected by A being at its cap.
        async with await open_stream(client, b, rb["id"]) as sb:
            assert sb.response.status_code == 200

        stats = await debug_stats(client)
        assert stats["sse_rejected_total"] == before["sse_rejected_total"] + 5

        # Closing one stream frees one place.
        await opened.pop().__aexit__(None, None, None)
        for _ in range(50):
            if (await debug_stats(client))["active_streams"] == before["active_streams"] + cap - 1:
                break
            await asyncio.sleep(0.1)
        async with await open_stream(client, a, ra["id"]) as again:
            assert again.response.status_code == 200
    finally:
        for s in opened:
            await s.__aexit__(None, None, None)
    print(f"\n[stream-cap] cap={cap} per project: {cap} opened, 5 concurrent extra all refused with 429, other project unaffected")


async def test_api_process_is_unprivileged_and_cannot_write_its_filesystem(client):
    stats = await debug_stats(client)
    print(f"\n[container] api-test: uid={stats['uid']} rootfs_writable={stats['rootfs_writable']}")
    assert stats["uid"] != 0, "the API runs as root"
    assert stats["rootfs_writable"] is False, "the API can write to its own code directory"


async def test_request_log_records_the_client_address_and_no_credentials(client, make_project):
    a = await make_project()
    marker = uuid.uuid4().hex
    await client.get(f"/runs/{marker}", headers=a.headers)
    lines = (await client.get("/debug/logs")).json()["lines"]
    mine = [json.loads(line) for line in lines if marker in line]
    assert len(mine) == 1
    entry = mine[0]
    assert entry["status"] == 404 and entry["project_id"] == a.id
    assert entry["client"] not in ("", "-", None)
    assert a.key not in json.dumps(entry) and "authorization" not in json.dumps(entry).lower()
