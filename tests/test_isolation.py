"""SPEC §7.2 — project A must not be able to read, write, subscribe to, or guess its way into
project B. Two projects, A and B; runs ra (A) and rb (B).

Some of this is also touched by other test files; this file is the one place that states the
whole isolation claim.
"""
import asyncio
import json
import re
import statistics
import time
import uuid

import pytest

from conftest import (
    ADMIN_TOKEN,
    SSE,
    bearer,
    db_points,
    db_run_state,
    make_points,
    open_stream,
    send_batch,
    stream_token,
)

NOT_FOUND = b'{"error":"not_found"}'
UNAUTHORIZED = b'{"error":"unauthorized"}'
UUID_RE = re.compile(r"[0-9a-f]{8}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{12}", re.I)
# Headers that legitimately differ between two otherwise identical responses.
VOLATILE_HEADERS = {"date", "x-request-id"}


@pytest.fixture
async def world(client, make_project, make_run):
    """Projects A and B, each with one run holding data that is recognisably its own."""
    a, b = await make_project("project-a"), await make_project("project-b")
    ra, rb = await make_run(a, "run-a"), await make_run(b, "run-b-secret")
    await send_batch(client, a, ra["id"], make_points(5, "metric-a"))
    await send_batch(client, b, rb["id"], make_points(7, "metric-b-secret"))
    return a, b, ra, rb


def stable_headers(response) -> dict:
    return {k: v for k, v in response.headers.items() if k not in VOLATILE_HEADERS}


# --- read / write / subscribe ----------------------------------------------------------------


async def test_every_endpoint_treats_foreign_run_exactly_like_a_missing_one(client, db, world):
    """For each run-scoped endpoint: A's key on B's run vs A's key on a random UUID. Status, body
    bytes and headers must be identical, so the response cannot be used to learn that rb exists."""
    a, b, ra, rb = world
    before_points = await db_points(db, rb["id"])
    before_state = await db_run_state(db, rb["id"])
    batch = {"batch_id": str(uuid.uuid4()), "points": make_points(3, "injected", start=900)}

    def calls(run_id):
        return [
            ("GET", f"/runs/{run_id}", None),
            ("GET", f"/runs/{run_id}/metrics", None),
            ("GET", f"/runs/{run_id}/metrics?since_seq=0&limit=10", None),
            ("POST", f"/runs/{run_id}/metrics", batch),
            ("POST", f"/runs/{run_id}/stream-token", None),
        ]

    for foreign, missing in zip(calls(rb["id"]), calls(str(uuid.uuid4()))):
        r_foreign = await client.request(foreign[0], foreign[1], json=foreign[2], headers=a.headers)
        r_missing = await client.request(missing[0], missing[1], json=missing[2], headers=a.headers)
        label = f"{foreign[0]} {foreign[1].replace(rb['id'], '<rb>')}"
        assert r_foreign.status_code == r_missing.status_code == 404, label
        assert r_foreign.content == r_missing.content == NOT_FOUND, label
        assert stable_headers(r_foreign) == stable_headers(r_missing), label

    # Same for B's API key id.
    r_foreign = await client.post(f"/projects/me/keys/{b.key_id}/revoke", headers=a.headers)
    r_missing = await client.post(f"/projects/me/keys/{uuid.uuid4()}/revoke", headers=a.headers)
    assert r_foreign.status_code == r_missing.status_code == 404
    assert r_foreign.content == r_missing.content == NOT_FOUND
    assert stable_headers(r_foreign) == stable_headers(r_missing)

    # Nothing was written to rb and B is unaffected.
    assert await db_points(db, rb["id"]) == before_points
    assert await db_run_state(db, rb["id"]) == before_state
    assert (await client.get(f"/runs/{rb['id']}", headers=b.headers)).status_code == 200
    assert await db.fetchval("SELECT revoked_at FROM api_keys WHERE id = $1", uuid.UUID(b.key_id)) is None


async def test_run_list_shows_only_own_runs(client, world):
    a, b, ra, rb = world
    list_a = await client.get("/runs", headers=a.headers)
    list_b = await client.get("/runs", headers=b.headers)
    assert [r["id"] for r in list_a.json()["runs"]] == [ra["id"]]
    assert [r["id"] for r in list_b.json()["runs"]] == [rb["id"]]
    assert rb["id"] not in list_a.text and "secret" not in list_a.text


async def test_project_id_from_the_client_is_never_trusted(client, db, world):
    a, b, ra, rb = world
    # In the body, in the query string, in a header: all ignored.
    r = await client.post(
        f"/runs?project_id={b.id}", json={"name": "smuggled", "project_id": b.id}, headers={**a.headers, "X-Project-Id": b.id}
    )
    assert r.status_code == 201
    assert str(await db.fetchval("SELECT project_id FROM runs WHERE id = $1", uuid.UUID(r.json()["id"]))) == a.id
    r = await client.get(f"/runs?project_id={b.id}", headers={**a.headers, "X-Project-Id": b.id})
    assert rb["id"] not in r.text
    assert [x["id"] for x in (await client.get("/runs", headers=b.headers)).json()["runs"]] == [rb["id"]]


async def test_batch_id_of_another_project_is_not_an_oracle(client, db, world):
    """The idempotency ledger is per run: A reusing a batch_id that B used learns nothing (no 409,
    no 'duplicate') and B's batch is not disturbed."""
    a, b, ra, rb = world
    shared = str(uuid.uuid4())
    assert (await send_batch(client, b, rb["id"], make_points(2, "b-only", start=50), batch_id=shared)).status_code == 200
    r = await send_batch(client, a, ra["id"], make_points(2, "a-only", start=50), batch_id=shared)
    assert r.status_code == 200
    assert r.json() == {"accepted": 2, "duplicates": 0, "last_seq": 7}
    r = await send_batch(client, b, rb["id"], make_points(2, "b-only", start=50), batch_id=shared)
    assert r.json() == {"duplicate": True, "accepted": 2}


async def test_stream_token_of_a_does_not_open_stream_of_b(client, world):
    a, b, ra, rb = world
    token_a = await stream_token(client, a, ra["id"])
    payload, _, sig = token_a.rpartition(".")
    project_hex, run_hex, key_hex, exp = payload.split(".")
    rb_hex, b_project_hex, b_key_hex = uuid.UUID(rb["id"]).hex, uuid.UUID(b.id).hex, uuid.UUID(b.key_id).hex

    attempts = {
        "A's token as-is on rb": token_a,
        "run swapped to rb, old signature": f"{project_hex}.{rb_hex}.{key_hex}.{exp}.{sig}",
        "project and run swapped to B, old signature": f"{b_project_hex}.{rb_hex}.{key_hex}.{exp}.{sig}",
        "all of B's ids, old signature": f"{b_project_hex}.{rb_hex}.{b_key_hex}.{exp}.{sig}",
        "all of B's ids, empty signature": f"{b_project_hex}.{rb_hex}.{b_key_hex}.{exp}.",
    }
    for label, token in attempts.items():
        r = await client.get(f"/runs/{rb['id']}/stream", params={"token": token})
        assert r.status_code == 401, label
        assert r.content == UNAUTHORIZED, label  # the entire body: not one byte of rb's data
        assert "secret" not in r.text
    # Control: the same token does open A's own run.
    async with SSE(client, f"/runs/{ra['id']}/stream", params={"token": token_a}) as s:
        assert [p["name"] for p in await s.metrics(5)] == ["metric-a"] * 5


async def test_open_stream_of_a_never_receives_events_of_b(client, world):
    """Both projects write at the same time; A's open stream must carry A's points only."""
    a, b, ra, rb = world
    async with await open_stream(client, a, ra["id"], last_event_id=5) as s:
        await asyncio.sleep(0.2)
        await asyncio.gather(
            *(send_batch(client, b, rb["id"], make_points(10, "metric-b-secret", start=100 + i * 10)) for i in range(10)),
            *(send_batch(client, a, ra["id"], make_points(10, "metric-a", start=100 + i * 10)) for i in range(10)),
        )
        got = await s.metrics(100)
        try:
            async with asyncio.timeout(2.5):
                extra = await s.next_event()
            raise AssertionError(f"unexpected extra event {extra}")
        except TimeoutError:
            pass
    assert {p["name"] for p in got} == {"metric-a"}
    assert [p["seq"] for p in got] == list(range(6, 106))


# --- guessing ids ----------------------------------------------------------------------------


async def test_ids_are_random_uuid4_and_neighbours_lead_nowhere(client, world, make_run):
    a, b, ra, rb = world
    runs = [await make_run(b, f"seq-{i}") for i in range(8)]
    ids = [uuid.UUID(r["id"]) for r in runs]
    for value in ids + [uuid.UUID(x) for x in (a.id, b.id, a.key_id, b.key_id, ra["id"], rb["id"])]:
        assert value.version == 4
    # Created one right after another, yet nowhere near each other (no counter, no timestamp).
    gaps = [abs(x.int - y.int) for x, y in zip(ids, ids[1:])]
    print(f"\n[ids] smallest numeric gap between consecutively created run ids: 2^{min(gaps).bit_length() - 1}")
    assert min(gaps) > 2**64
    assert ids != sorted(ids) and ids != sorted(ids, reverse=True)

    # Walking outwards from a known id of B finds nothing, with A's key or even with B's own.
    base = uuid.UUID(rb["id"]).int
    for delta in (-3, -2, -1, 1, 2, 3):
        neighbour = str(uuid.UUID(int=(base + delta) % 2**128))
        for headers in (a.headers, b.headers):
            r = await client.get(f"/runs/{neighbour}", headers=headers)
            assert r.status_code == 404 and r.content == NOT_FOUND


# --- cache -----------------------------------------------------------------------------------


async def test_responses_are_not_cacheable_and_same_url_differs_per_key(client, world):
    a, b, ra, rb = world
    url = f"/runs/{ra['id']}"
    sequence = [
        ("A", await client.get(url, headers=a.headers)),
        ("B", await client.get(url, headers=b.headers)),
        ("A", await client.get(url, headers=a.headers)),
        ("B", await client.get(url, headers=b.headers)),
        ("none", await client.get(url)),
    ]
    assert [r.status_code for _, r in sequence] == [200, 404, 200, 404, 401]
    assert sequence[0][1].json()["name"] == "run-a"
    for _, r in sequence[1::2]:
        assert r.content == NOT_FOUND  # B never gets A's cached answer

    lists = [(await client.get("/runs", headers=h.headers)).json() for h in (a, b, a)]
    assert lists[0] == lists[2] != lists[1]

    token = await stream_token(client, a, ra["id"])
    async with SSE(client, f"/runs/{ra['id']}/stream", params={"token": token}) as s:
        stream_response = s.response
    everything = [r for _, r in sequence] + [
        await client.get("/runs", headers=a.headers),
        await client.get(f"/runs/{ra['id']}/metrics", headers=a.headers),
        await client.post(f"/runs/{ra['id']}/stream-token", headers=a.headers),
        await client.get("/"),
        await client.get("/static/app.js"),
        stream_response,
    ]
    for r in everything:
        assert r.headers["cache-control"] == "no-store, private", r.url
        assert "authorization" in r.headers["vary"].lower(), r.url
        assert "etag" not in r.headers or r.url.path.startswith("/static") or r.url.path == "/", r.url


# --- error messages --------------------------------------------------------------------------


async def test_error_bodies_reveal_no_ids_prefixes_or_internals(client, world):
    a, b, ra, rb = world
    token_a = await stream_token(client, a, ra["id"])
    responses = [
        await client.get(f"/runs/{rb['id']}", headers=a.headers),
        await client.get(f"/runs/{rb['id']}", headers=bearer(b.key[:-2] + "xx")),
        await client.get(f"/runs/{rb['id']}"),
        await client.post(f"/runs/{rb['id']}/metrics", json={"batch_id": str(uuid.uuid4()), "points": make_points(1)}, headers=a.headers),
        await client.post(f"/runs/{rb['id']}/metrics", json={"batch_id": rb["id"], "points": "x"}, headers=a.headers),
        await client.post(f"/runs/{rb['id']}/stream-token", headers=a.headers),
        await client.get(f"/runs/{rb['id']}/stream", params={"token": token_a}),
        await client.post(f"/projects/me/keys/{b.key_id}/revoke", headers=a.headers),
        await client.post("/projects", json={"name": "x"}, headers=a.headers),
        await client.get(f"/runs/{rb['id']}/metrics?since_seq=abc", headers=a.headers),
        await client.delete(f"/runs/{rb['id']}", headers=a.headers),
        await client.get(f"/nope/{rb['id']}"),
    ]
    expected = [404, 401, 401, 404, 422, 404, 401, 404, 401, 422, 405, 404]
    assert [r.status_code for r in responses] == expected
    leaks = (a.prefix, b.prefix, a.key, b.key, "Traceback", "SELECT", "asyncpg", "project", "secret")
    for r in responses:
        assert set(r.json()) == {"error"}, r.text
        assert re.fullmatch(r"[a-z_]+", r.json()["error"]), r.text
        assert not UUID_RE.search(r.text), r.text
        assert not any(word in r.text for word in leaks), r.text


# --- logs ------------------------------------------------------------------------------------


async def test_logs_contain_neither_api_keys_nor_stream_tokens(client, world):
    """Uses the TEST_MODE log buffer, which holds the same formatted lines the server writes to
    stdout. Secrets are sent every way a client plausibly could, including wrong ways."""
    a, b, ra, rb = world
    token = await stream_token(client, a, ra["id"])
    request_ids = []

    async def call(method, url, **kwargs):
        r = await client.request(method, url, **kwargs)
        request_ids.append(r.headers["x-request-id"])
        return r

    assert (await call("GET", "/runs", headers=a.headers)).status_code == 200
    assert (await call("GET", f"/runs/{rb['id']}", headers=a.headers)).status_code == 404
    assert (await call("GET", "/runs", headers=bearer(a.key + "x"))).status_code == 401
    # Stream: good token, then the same token against the wrong run.
    async with SSE(client, f"/runs/{ra['id']}/stream", params={"token": token, "since_seq": 2}) as s:
        request_ids.append(s.response.headers["x-request-id"])
        await s.metrics(3)
    assert (await call("GET", f"/runs/{rb['id']}/stream", params={"token": token})).status_code == 401
    # Mistakes a client could make: the key or token in the URL instead of the header.
    assert (await call("GET", "/runs", params={"api_key": a.key})).status_code == 401
    assert (await call("GET", f"/runs/{a.key}", headers=a.headers)).status_code == 404
    assert (await call("GET", f"/runs/{token}/stream")).status_code == 401
    assert (await call("GET", "/runs", params={"access": token, "auth": ADMIN_TOKEN})).status_code == 401
    # An unhandled server error (logged with a traceback) while the key is in play.
    r = await send_batch(client, a, ra["id"], make_points(4, start=70), headers={"X-Test-Fault": "mid-insert"})
    request_ids.append(r.headers["x-request-id"])
    assert r.status_code == 500
    # Admin call.
    r = await call("POST", "/projects", json={"name": "log-check"}, headers=bearer(ADMIN_TOKEN))
    new_key = r.json()["api_key"]["key"]

    await asyncio.sleep(0.3)  # the stream's log line is written when the stream closes
    lines = (await client.get("/debug/logs")).json()["lines"]
    text = "\n".join(lines)

    # We are looking at the right logs: every request above has its line, with the right project.
    for rid in request_ids:
        assert rid in text, f"request {rid} is missing from the log buffer"
    mine = [json.loads(line) for line in lines if any(rid in line for rid in request_ids)]
    assert any(e.get("project_id") == a.id and e.get("status") == 200 for e in mine)
    assert any("exc" in e and "FaultInjected" in e["exc"] for e in mine)
    stream_lines = [e for e in mine if e.get("path", "").endswith("since_seq=2")]
    assert [e["path"] for e in stream_lines] == [f"/runs/{ra['id']}/stream?token=REDACTED&since_seq=2"]

    secrets = {"key A": a.key, "key B": b.key, "new key": new_key, "stream token": token,
               "stream token signature": token.rpartition(".")[2], "admin token": ADMIN_TOKEN}
    found = {name: sum(secret in line for line in lines) for name, secret in secrets.items()}
    print(f"\n[logs] scanned {len(lines)} log lines ({len(mine)} from this test); lines containing each secret: {found}")
    assert found == dict.fromkeys(secrets, 0)
    assert "sk_REDACTED" in text and "token=REDACTED" in text  # the mistakes were seen and masked


# --- timing ----------------------------------------------------------------------------------


async def test_timing_foreign_run_vs_missing_run(client, world):
    """Measured and printed, not asserted tightly: latency of 'run of another project' vs 'run that
    does not exist', 200 requests each, interleaved so drift affects both equally."""
    a, b, ra, rb = world
    n = 200
    missing_ids = [str(uuid.uuid4()) for _ in range(n)]
    for _ in range(20):  # warm up connections and plans
        await client.get(f"/runs/{rb['id']}", headers=a.headers)
        await client.get(f"/runs/{uuid.uuid4()}", headers=a.headers)

    samples = {"foreign": [], "missing": [], "own": []}
    for i in range(n):
        order = [("foreign", rb["id"]), ("missing", missing_ids[i]), ("own", ra["id"])]
        if i % 2:
            order.reverse()
        for kind, run_id in order:
            t0 = time.perf_counter()
            r = await client.get(f"/runs/{run_id}", headers=a.headers)
            samples[kind].append((time.perf_counter() - t0) * 1000)
            assert r.status_code == (200 if kind == "own" else 404)

    def summary(values):
        ordered = sorted(values)
        return f"median {statistics.median(ordered):.3f} ms, p95 {ordered[int(len(ordered) * 0.95)]:.3f} ms"

    diff = statistics.median(samples["foreign"]) - statistics.median(samples["missing"])
    spread = statistics.stdev(samples["missing"])
    print(f"\n[timing] GET /runs/<id> with A's key, {n} requests each, interleaved:"
          f"\n[timing]   run of project B : {summary(samples['foreign'])}"
          f"\n[timing]   nonexistent run  : {summary(samples['missing'])}"
          f"\n[timing]   own run (200)    : {summary(samples['own'])}"
          f"\n[timing]   median difference foreign - missing: {diff * 1000:+.1f} µs (stdev of 'missing': {spread * 1000:.1f} µs)")
    # Loose sanity bound only; the real result is the printed numbers.
    assert abs(diff) < 5.0


async def test_timing_unknown_key_vs_revoked_key(client, world):
    a, b, ra, rb = world
    revoked = (await client.post("/projects/me/keys", headers=a.headers)).json()
    await client.post(f"/projects/me/keys/{revoked['id']}/revoke", headers=a.headers)
    n = 200
    samples = {"unknown": [], "revoked": []}
    for i in range(n + 20):
        order = [("unknown", "sk_" + uuid.uuid4().hex + uuid.uuid4().hex[:11]), ("revoked", revoked["key"])]
        if i % 2:
            order.reverse()
        for kind, key in order:
            t0 = time.perf_counter()
            r = await client.get("/runs", headers=bearer(key))
            if i >= 20:
                samples[kind].append((time.perf_counter() - t0) * 1000)
            assert r.status_code == 401 and r.content == UNAUTHORIZED
    m_unknown, m_revoked = statistics.median(samples["unknown"]), statistics.median(samples["revoked"])
    print(f"\n[timing] GET /runs with a bad key, {n} requests each, interleaved:"
          f"\n[timing]   unknown key : median {m_unknown:.3f} ms"
          f"\n[timing]   revoked key : median {m_revoked:.3f} ms"
          f"\n[timing]   median difference revoked - unknown: {(m_revoked - m_unknown) * 1000:+.1f} µs")
    assert abs(m_revoked - m_unknown) < 5.0
