"""Hardening pass: hostile and sloppy input, races, exactness, and whole-database invariants.

These tests were added after the feature tests were green. They do not test new features; they
try to break the two promises of the service from angles the earlier files do not cover:
what the user sees equals what is stored, and a caller sees only its own project.
"""
import asyncio
import json
import struct
import time
import uuid

import httpx

from conftest import ADMIN_TOKEN, bearer, db_points, db_run_state, make_points, open_stream, send_batch, stream_token

UNAUTHORIZED = b'{"error":"unauthorized"}'
NOT_FOUND = b'{"error":"not_found"}'
INVALID = b'{"error":"invalid_request"}'
JSON = {"Content-Type": "application/json"}


def bits(x: float) -> str:
    """Exact bit pattern of a double, so that -0.0 != 0.0 and nothing is rounded."""
    return struct.pack(">d", float(x)).hex()


def assert_error_shape(r, label):
    """Every error is {"error": "<code>"} and nothing else: no detail, no echo, no trace."""
    body = r.json()
    assert list(body) == ["error"], (label, r.text)
    assert isinstance(body["error"], str) and body["error"].isidentifier(), (label, r.text)


# --- authentication comes before everything else -------------------------------------------------


async def test_unauthenticated_request_is_401_no_matter_what_else_is_wrong(client, make_project, make_run):
    a = await make_project()
    run = await make_run(a)
    rid = run["id"]
    good_body = json.dumps({"batch_id": str(uuid.uuid4()), "points": make_points(2)}).encode()
    cases = {
        "valid ingest": ("POST", f"/runs/{rid}/metrics", good_body),
        "malformed json": ("POST", f"/runs/{rid}/metrics", b'{"batch_id": '),
        "invalid body": ("POST", f"/runs/{rid}/metrics", b'{"points": []}'),
        "empty body": ("POST", f"/runs/{rid}/metrics", b""),
        "bad query": ("GET", f"/runs/{rid}/metrics?limit=abc&since_seq=-1", None),
        "malformed id": ("GET", "/runs/not-a-uuid", None),
        "missing run": ("GET", f"/runs/{uuid.uuid4()}", None),
        "run create, bad body": ("POST", "/runs", b"{}"),
        "run create, malformed json": ("POST", "/runs", b"{"),
        "stream token": ("POST", f"/runs/{rid}/stream-token", None),
        "revoke, malformed id": ("POST", "/projects/me/keys/xyz/revoke", None),
        "project create, malformed json": ("POST", "/projects", b"{"),
        "project create, bad body": ("POST", "/projects", b'{"name": ""}'),
    }
    credentials = {
        "no header": {},
        "unknown key": bearer("sk_" + "A" * 43),
        "basic scheme": {"Authorization": "Basic " + a.key},
        "key without scheme": {"Authorization": a.key},
    }
    failures = []
    for cred_label, cred in credentials.items():
        for label, (method, path, content) in cases.items():
            r = await client.request(method, path, content=content, headers={**JSON, **cred})
            if r.status_code != 401 or r.content != UNAUTHORIZED:
                failures.append(f"{cred_label} / {label}: {r.status_code} {r.text}")
    assert not failures, "\n".join(failures)


async def test_credentials_are_accepted_only_in_the_authorization_header(client, make_project, make_run):
    a = await make_project()
    await make_run(a)
    for label, kwargs in {
        "query api_key": {"params": {"api_key": a.key}},
        "query key": {"params": {"key": a.key}},
        "query token": {"params": {"token": a.key}},
        "cookie": {"cookies": {"Authorization": a.key, "api_key": a.key}},
        "x-api-key header": {"headers": {"X-API-Key": a.key}},
    }.items():
        cookies = kwargs.pop("cookies", None)
        async with httpx.AsyncClient(base_url=str(client.base_url), timeout=10, cookies=cookies) as fresh:
            r = await fresh.get("/runs", **kwargs)
        assert r.status_code == 401 and r.content == UNAUTHORIZED, label

    # The admin token creates projects and nothing else; it is not a key of any project.
    for method, path in (("GET", "/runs"), ("POST", "/runs"), ("POST", "/projects/me/keys")):
        r = await client.request(method, path, json={"name": "x"}, headers=bearer(ADMIN_TOKEN))
        assert r.status_code == 401 and r.content == UNAUTHORIZED, (method, path)


async def test_no_cross_origin_access_is_granted(client, make_project, make_run):
    a = await make_project()
    run = await make_run(a)
    evil = {"Origin": "https://evil.example"}
    r = await client.get(f"/runs/{run['id']}", headers={**a.headers, **evil})
    assert r.status_code == 200
    assert not [h for h in r.headers if h.lower().startswith("access-control-")]
    preflight = await client.options(
        f"/runs/{run['id']}/metrics",
        headers={**evil, "Access-Control-Request-Method": "POST", "Access-Control-Request-Headers": "authorization"},
    )
    assert preflight.status_code >= 400
    assert not [h for h in preflight.headers if h.lower().startswith("access-control-")]


# --- malformed input never becomes a 500 ---------------------------------------------------------


async def test_malformed_input_is_a_clean_4xx_and_never_a_500(client, db, make_project, make_run):
    a = await make_project()
    run = await make_run(a)
    rid = run["id"]
    token = await stream_token(client, a, rid)
    bid = str(uuid.uuid4())
    point = '{"name": "loss", "step": %s, "value": %s}'
    batch = '{"batch_id": "' + bid + '", "points": [%s]}'
    ingest = f"/runs/{rid}/metrics"
    cases = {
        "truncated json": ("POST", ingest, b'{"batch_id": "', JSON),
        "empty body": ("POST", ingest, b"", JSON),
        "json null": ("POST", ingest, b"null", JSON),
        "json string": ("POST", ingest, b'"hello"', JSON),
        "not utf-8": ("POST", ingest, b'{"batch_id": "\xff\xfe"}', JSON),
        "text/plain": ("POST", ingest, (batch % (point % (0, 1))).encode(), {"Content-Type": "text/plain"}),
        "form encoded": ("POST", ingest, b"batch_id=1&points=2", {"Content-Type": "application/x-www-form-urlencoded"}),
        "deeply nested": ("POST", ingest, b"[" * 100_000, JSON),
        "400-digit step": ("POST", ingest, (batch % (point % ("9" * 400, 1))).encode(), JSON),
        "400-digit value": ("POST", ingest, (batch % (point % (0, "9" * 400))).encode(), JSON),
        "step 1e3": ("POST", ingest, (batch % (point % ("1e3", 1))).encode(), JSON),
        "step as string": ("POST", ingest, (batch % (point % ('"1"', 1))).encode(), JSON),
        "points is an object": ("POST", ingest, ('{"batch_id": "%s", "points": {"0": 1}}' % bid).encode(), JSON),
        "point is a list": ("POST", ingest, (batch % '["loss", 0, 1]').encode(), JSON),
        "name is an object": ("POST", ingest, (batch % '{"name": {"$ne": ""}, "step": 0, "value": 1}').encode(), JSON),
        "run name is a number": ("POST", "/runs", b'{"name": 5}', JSON),
        "run name 201 chars": ("POST", "/runs", json.dumps({"name": "n" * 201}).encode(), JSON),
        "run name only spaces": ("POST", "/runs", b'{"name": "    "}', JSON),
        "since_seq negative": ("GET", f"/runs/{rid}/metrics?since_seq=-1", None, {}),
        "since_seq 2^63": ("GET", f"/runs/{rid}/metrics?since_seq={2**63}", None, {}),
        "since_seq text": ("GET", f"/runs/{rid}/metrics?since_seq=abc", None, {}),
        "since_seq float": ("GET", f"/runs/{rid}/metrics?since_seq=1.5", None, {}),
        "limit 0": ("GET", f"/runs/{rid}/metrics?limit=0", None, {}),
        "limit 5001": ("GET", f"/runs/{rid}/metrics?limit=5001", None, {}),
        "limit sql": ("GET", f"/runs/{rid}/metrics?limit=1;DROP TABLE metrics", None, {}),
        "stream since_seq text": ("GET", f"/runs/{rid}/stream?token={token}&since_seq=abc", None, {}),
        "stream since_seq negative": ("GET", f"/runs/{rid}/stream?token={token}&since_seq=-1", None, {}),
        "id with NUL": ("GET", "/runs/%00", None, {}),
        "id sql": ("GET", "/runs/1' OR '1'='1", None, {}),
        "id traversal": ("GET", "/runs/..%2F..%2Fetc%2Fpasswd", None, {}),
        "id 10k chars": ("GET", "/runs/" + "a" * 10_000, None, {}),
        "double slash": ("GET", f"/runs/{rid}//stream?token={token}", None, {}),
        "unknown path": ("GET", "/admin", None, {}),
        "wrong method on run": ("DELETE", f"/runs/{rid}", None, {}),
        "wrong method on metrics": ("PUT", ingest, b"{}", JSON),
        "wrong method on projects": ("GET", "/projects", None, {}),
    }
    seen = {}
    for label, (method, path, content, headers) in cases.items():
        r = await client.request(method, path, content=content, headers={**a.headers, **headers})
        seen[label] = r.status_code
        assert 400 <= r.status_code < 500, (label, r.status_code, r.text)
        assert r.headers["content-type"] == "application/json", label
        assert_error_shape(r, label)
        # Nothing the caller sent comes back, and nothing internal leaks out.
        for needle in ("Traceback", "asyncpg", "pydantic", "SELECT", "DROP", bid, rid, "9" * 20):
            assert needle not in r.text, (label, needle)
    print(f"\n[malformed] {len(cases)} cases, statuses: {sorted(set(seen.values()))}")

    assert await db_run_state(db, rid) == {"last_seq": 0, "points": 0, "batches": 0}
    assert await db.fetchval("SELECT to_regclass('metrics') IS NOT NULL")
    assert (await client.get("/healthz")).status_code == 200


async def test_oversized_body_is_rejected_and_stores_nothing(client, db, make_project, make_run):
    a = await make_project()
    run = await make_run(a)
    # ~8 MB of otherwise valid points: far over the 1000-point limit.
    body = json.dumps({"batch_id": str(uuid.uuid4()), "points": make_points(200_000)}).encode()
    started = time.perf_counter()
    r = await client.post(f"/runs/{run['id']}/metrics", content=body, headers={**a.headers, **JSON}, timeout=60)
    elapsed = time.perf_counter() - started
    print(f"\n[body] {len(body) / 1e6:.1f} MB, 200000 points: status={r.status_code} in {elapsed * 1000:.0f} ms")
    assert r.status_code in (413, 422)
    assert_error_shape(r, "oversized")
    assert await db_run_state(db, run["id"]) == {"last_seq": 0, "points": 0, "batches": 0}
    assert (await client.get("/healthz")).status_code == 200


# --- exactness: what is stored is what was sent, to the last bit ---------------------------------

EXACT_VALUES = [
    0.1 + 0.2,  # 0.30000000000000004
    1 / 3,
    5e-324,  # smallest subnormal
    2.2250738585072014e-308,  # smallest normal
    1.7976931348623157e308,  # largest double
    -1.7976931348623157e308,
    -0.0,
    0.0,
    1e-7,
    123456789.123456789,
    9007199254740993,  # 2^53 + 1 as an integer: not representable, must become exactly 2^53
    -1,
    1e21,
    3.141592653589793,
]


async def test_values_survive_ingest_database_snapshot_and_stream_bit_for_bit(client, db, make_project, make_run):
    a = await make_project()
    run = await make_run(a)
    pts = [{"name": "v", "step": i, "value": v} for i, v in enumerate(EXACT_VALUES)]
    r = await send_batch(client, a, run["id"], pts)
    assert r.json() == {"accepted": len(pts), "duplicates": 0, "last_seq": len(pts)}
    want = [bits(v) for v in EXACT_VALUES]

    stored = await db.fetch("SELECT value FROM metrics WHERE run_id = $1 ORDER BY step", uuid.UUID(run["id"]))
    assert [bits(row["value"]) for row in stored] == want

    snap = (await client.get(f"/runs/{run['id']}/metrics", headers=a.headers)).json()["points"]
    assert [bits(p["value"]) for p in sorted(snap, key=lambda p: p["step"])] == want

    async with await open_stream(client, a, run["id"]) as s:
        live = await s.metrics(len(pts))
    assert [bits(p["value"]) for p in sorted(live, key=lambda p: p["step"])] == want
    # The three views agree on seq as well, not only on values.
    assert [(p["step"], p["seq"]) for p in snap] == [(p["step"], p["seq"]) for p in live]
    assert [(p["step"], p["seq"]) for p in snap] == [(row[1], row[3]) for row in await db_points(db, run["id"])]


async def test_retry_that_differs_only_in_spelling_is_still_the_same_batch(client, db, make_project, make_run):
    a = await make_project()
    run = await make_run(a)
    bid = str(uuid.uuid4())
    url = f"/runs/{run['id']}/metrics"
    headers = {**a.headers, **JSON}
    first = '{"batch_id":"%s","points":[{"name":"loss","step":1,"value":1,"ts":"2026-11-10T15:00:00Z"}]}' % bid
    same = [
        '{"points":[{"ts":"2026-11-10T20:00:00+05:00","value":1.0,"step":1,"name":"loss"}],"batch_id":"%s"}' % bid.upper(),
        '{ "batch_id": "%s", "points": [ {"name":"loss","step":1,"value":1e0,"ts":"2026-11-10T15:00:00.000+00:00"} ] }' % bid,
    ]
    different = [
        '{"batch_id":"%s","points":[{"name":"loss","step":1,"value":1.0000000000000002,"ts":"2026-11-10T15:00:00Z"}]}' % bid,
        '{"batch_id":"%s","points":[{"name":"loss","step":1,"value":1,"ts":"2026-11-10T15:00:01Z"}]}' % bid,
        '{"batch_id":"%s","points":[{"name":"loss","step":1,"value":1}]}' % bid,
        '{"batch_id":"%s","points":[{"name":"Loss","step":1,"value":1,"ts":"2026-11-10T15:00:00Z"}]}' % bid,
    ]
    r = await client.post(url, content=first.encode(), headers=headers)
    assert r.json() == {"accepted": 1, "duplicates": 0, "last_seq": 1}
    for raw in same:
        r = await client.post(url, content=raw.encode(), headers=headers)
        assert (r.status_code, r.json()) == (200, {"duplicate": True, "accepted": 1}), raw
    # Python's JSON parser detects UTF-16 on its own, so such a body is read, not rejected. It
    # must then mean exactly the same as its UTF-8 twin.
    r = await client.post(url, content=first.encode("utf-16"), headers=headers)
    assert (r.status_code, r.json()) == (200, {"duplicate": True, "accepted": 1})
    for raw in different:
        r = await client.post(url, content=raw.encode(), headers=headers)
        assert (r.status_code, r.content) == (409, b'{"error":"batch_id_reused"}'), raw
    assert await db_run_state(db, run["id"]) == {"last_seq": 1, "points": 1, "batches": 1}


async def test_metric_names_are_opaque_exact_strings(client, db, make_project, make_run):
    a = await make_project()
    run = await make_run(a)
    names = [
        "loss",
        "Loss",
        "loss ",
        " loss",
        "lóss",  # precomposed o-acute
        "lóss",  # o + combining acute: looks the same, is a different key
        "\U0001f600" * 64,  # 64 characters, 256 bytes
        "'; DROP TABLE metrics; --",
        "<script>alert(1)</script>",
        "a/b?c#d&e=f",
        "%00",
        "\\u0000",
        "‮evil",  # right-to-left override
        "жоғалту",
    ]
    pts = [{"name": n, "step": 0, "value": float(i)} for i, n in enumerate(names)]
    r = await send_batch(client, a, run["id"], pts)
    assert r.json() == {"accepted": len(names), "duplicates": 0, "last_seq": len(names)}

    rows = await db_points(db, run["id"])
    assert {row[0]: row[2] for row in rows} == {n: float(i) for i, n in enumerate(names)}
    # seq follows byte order of the name (COLLATE "C"), independent of the database locale.
    assert [row[0] for row in rows] == sorted(names, key=lambda n: n.encode())

    snap = (await client.get(f"/runs/{run['id']}/metrics", headers=a.headers)).json()["points"]
    assert [(p["name"], p["value"], p["seq"]) for p in snap] == [(row[0], row[2], row[3]) for row in rows]
    async with await open_stream(client, a, run["id"]) as s:
        live = await s.metrics(len(names))
    assert [(p["name"], p["value"], p["seq"]) for p in live] == [(row[0], row[2], row[3]) for row in rows]

    for label, bad in {"65 emoji": "\U0001f600" * 65, "tab": "a\tb", "newline": "a\nb", "escape": "a\x1bb"}.items():
        r = await send_batch(client, a, run["id"], [{"name": bad, "step": 0, "value": 1.0}])
        assert (r.status_code, r.content) == (422, INVALID), label
    assert (await db_run_state(db, run["id"]))["points"] == len(names)


# --- identifiers ---------------------------------------------------------------------------------


async def test_other_spellings_of_an_id_never_reach_another_projects_run(client, make_project, make_run):
    """uuid.UUID() accepts braces, urn:uuid:, no hyphens and upper case. All of them name the same
    run for its owner (seen by hand in Postman); none of them may get past the project filter."""
    a, b = await make_project(), await make_project()
    ra = await make_run(a)
    canonical = ra["id"]
    spellings = [canonical, canonical.upper(), canonical.replace("-", ""), "{" + canonical + "}", "urn:uuid:" + canonical]
    for s in spellings:
        own = await client.get(f"/runs/{s}", headers=a.headers)
        assert own.status_code == 200 and own.json()["id"] == canonical, s
        for method, suffix in (("GET", ""), ("GET", "/metrics"), ("POST", "/stream-token")):
            foreign = await client.request(method, f"/runs/{s}{suffix}", headers=b.headers)
            assert (foreign.status_code, foreign.content) == (404, NOT_FOUND), (s, suffix)
        r = await send_batch(client, b, s, make_points(1))
        assert (r.status_code, r.content) == (404, NOT_FOUND), s

    # A token minted through one spelling is for that run and for no other.
    rb = await make_run(b)
    token = await stream_token(client, a, "{" + canonical + "}")
    r = await client.get(f"/runs/{rb['id']}/stream", params={"token": token})
    assert (r.status_code, r.content) == (401, UNAUTHORIZED)


# --- races ---------------------------------------------------------------------------------------


async def test_racing_writers_of_the_same_keys_leave_one_whole_batch(client, db, make_project, make_run):
    a = await make_project()
    run = await make_run(a)
    writers = 20
    # Every writer sends the same 50 keys with its own value. Batches are atomic and the first
    # write wins, so afterwards all 50 points must carry the value of one single writer.
    batches = [[{"name": "loss", "step": s, "value": float(w)} for s in range(50)] for w in range(writers)]
    responses = await asyncio.gather(*(send_batch(client, a, run["id"], b) for b in batches))
    assert [r.status_code for r in responses] == [200] * writers
    accepted = sorted(r.json()["accepted"] for r in responses)
    assert accepted == [0] * (writers - 1) + [50]

    rows = await db_points(db, run["id"])
    assert [row[3] for row in rows] == list(range(1, 51))
    assert len({row[2] for row in rows}) == 1, "points of several writers were mixed"
    winner = responses[[r.json()["accepted"] for r in responses].index(50)]
    assert winner.json()["last_seq"] == 50
    assert await db_run_state(db, run["id"]) == {"last_seq": 50, "points": 50, "batches": writers}


async def test_one_batch_id_racing_with_two_different_payloads(client, db, make_project, make_run):
    a = await make_project()
    run = await make_run(a)
    bid = str(uuid.uuid4())
    x = make_points(20, "x")
    y = make_points(20, "y")
    order = [x, y] * 6
    responses = await asyncio.gather(*(send_batch(client, a, run["id"], p, batch_id=bid) for p in order))

    rows = await db_points(db, run["id"])
    stored_names = {row[0] for row in rows}
    assert len(rows) == 20 and len(stored_names) == 1, "both payloads were stored under one batch_id"
    winner = stored_names.pop()
    first_writes = 0
    for payload, r in zip(order, responses):
        if payload[0]["name"] == winner:
            assert r.status_code == 200, r.text
            if r.json().get("duplicate"):
                assert r.json() == {"duplicate": True, "accepted": 20}
            else:
                assert r.json() == {"accepted": 20, "duplicates": 0, "last_seq": 20}
                first_writes += 1
        else:
            assert (r.status_code, r.content) == (409, b'{"error":"batch_id_reused"}')
    assert first_writes == 1
    assert await db_run_state(db, run["id"]) == {"last_seq": 20, "points": 20, "batches": 1}


async def test_parallel_load_on_many_runs_of_two_projects_stays_separate(client, db, make_project, make_run):
    projects = [await make_project(), await make_project()]
    runs = []  # (project, run_id, marker)
    for p in projects:
        for _ in range(5):
            runs.append((p, (await make_run(p))["id"], float(len(runs) + 1)))

    async def writer(project, run_id, marker, part):
        pts = [{"name": f"m{part % 3}", "step": part * 20 + i, "value": marker} for i in range(20)]
        return await send_batch(client, project, run_id, pts)

    jobs = [writer(p, rid, marker, part) for p, rid, marker in runs for part in range(10)]
    responses = await asyncio.gather(*jobs)
    assert {r.status_code for r in responses} == {200}

    for project, rid, marker in runs:
        rows = await db_points(db, rid)
        assert [row[3] for row in rows] == list(range(1, 201)), rid
        assert {row[2] for row in rows} == {marker}, "a point landed in the wrong run"
        snap = (await client.get(f"/runs/{rid}/metrics", headers=project.headers)).json()
        assert snap["last_seq"] == 200 and snap["has_more"] is False
        assert [(p["name"], p["step"], p["value"], p["seq"]) for p in snap["points"]] == rows

    for project in projects:
        listed = (await client.get("/runs", headers=project.headers)).json()["runs"]
        assert {r["id"] for r in listed} == {rid for p, rid, _ in runs if p is project}
        assert all(r["points"] == 200 and r["last_seq"] == 200 for r in listed)


async def test_snapshot_is_self_consistent_while_writers_are_running(client, db, make_project, make_run):
    a = await make_project()
    run = await make_run(a)
    url = f"/runs/{run['id']}/metrics"
    done = asyncio.Event()
    snapshots = 0

    async def reader():
        nonlocal snapshots
        while not done.is_set():
            body = (await client.get(url, params={"limit": 5000}, headers=a.headers)).json()
            seqs = [p["seq"] for p in body["points"]]
            # One snapshot: the points are exactly 1..last_seq, never a partial batch, never ahead.
            assert seqs == list(range(1, body["last_seq"] + 1)), (seqs[-3:], body["last_seq"])
            assert body["has_more"] is False
            assert body["last_seq"] % 25 == 0, "a reader saw part of a batch"
            snapshots += 1

    async def stream_reader():
        async with await open_stream(client, a, run["id"]) as s:
            return await s.metrics(1000, timeout=30)

    readers = [asyncio.create_task(reader()) for _ in range(3)]
    live_task = asyncio.create_task(stream_reader())
    try:
        batches = [make_points(25, "loss", start=i * 25) for i in range(40)]
        responses = await asyncio.gather(*(send_batch(client, a, run["id"], b) for b in batches))
        assert {r.status_code for r in responses} == {200}
    finally:
        done.set()
        await asyncio.gather(*readers)
    live = await live_task
    rows = await db_points(db, run["id"])
    assert [p["seq"] for p in live] == list(range(1, 1001))
    assert [(p["name"], p["step"], p["value"], p["seq"]) for p in live] == rows
    print(f"\n[snapshot] {snapshots} snapshot reads during 40 concurrent batches, all self-consistent")
    assert snapshots >= 3


# --- the whole database --------------------------------------------------------------------------


async def test_invariants_hold_for_every_run_in_the_database(db):
    """Not one scenario: every run that any test, load test or manual session ever wrote."""
    async with db.transaction(isolation="repeatable_read", readonly=True):
        totals = await db.fetchrow(
            "SELECT (SELECT count(*) FROM projects) AS projects, (SELECT count(*) FROM runs) AS runs, "
            "(SELECT count(*) FROM metrics) AS points, (SELECT count(*) FROM batches) AS batches"
        )
        broken_seq = await db.fetch(
            "SELECT r.id, r.last_seq, s.n, s.lo, s.hi, s.distinct_seq FROM runs r LEFT JOIN LATERAL ("
            "  SELECT count(*) AS n, min(seq) AS lo, max(seq) AS hi, count(DISTINCT seq) AS distinct_seq"
            "  FROM metrics m WHERE m.run_id = r.id) s ON true "
            "WHERE r.last_seq <> s.n OR s.distinct_seq <> s.n "
            "   OR (s.n > 0 AND (s.lo <> 1 OR s.hi <> s.n))"
        )
        ledger_mismatch = await db.fetch(
            "SELECT b.run_id, b.batch_id, b.accepted, count(m.seq) AS stored FROM batches b "
            "LEFT JOIN metrics m ON m.run_id = b.run_id AND m.batch_id = b.batch_id "
            "GROUP BY b.run_id, b.batch_id, b.accepted HAVING b.accepted <> count(m.seq)"
        )
        orphans = await db.fetchval(
            "SELECT count(*) FROM metrics m WHERE NOT EXISTS ("
            "  SELECT 1 FROM batches b WHERE b.run_id = m.run_id AND b.batch_id = m.batch_id)"
        )
        non_finite = await db.fetchval(
            "SELECT count(*) FROM metrics WHERE value IN ('NaN', 'Infinity', '-Infinity')"
        )
        bad_names = await db.fetchval(
            "SELECT count(*) FROM metrics WHERE char_length(name) NOT BETWEEN 1 AND 64 OR name ~ '[\\x01-\\x1f]'"
        )
        bad_keys = await db.fetchval(
            "SELECT count(*) FROM api_keys WHERE octet_length(key_hash) <> 32 OR char_length(prefix) <> 8"
        )
    print(
        f"\n[invariants] projects={totals['projects']} runs={totals['runs']} points={totals['points']} "
        f"batches={totals['batches']} broken_seq_runs={len(broken_seq)} ledger_mismatches={len(ledger_mismatch)} "
        f"points_without_ledger_row={orphans} non_finite={non_finite}"
    )
    assert [dict(r) for r in broken_seq] == []
    assert [dict(r) for r in ledger_mismatch] == []
    assert non_finite == 0
    assert bad_names == 0
    assert bad_keys == 0
    # `orphans` is printed, not asserted: test_point_committed_without_notify_is_still_delivered_by
    # _safety_poll writes one point straight into the table on purpose, once per suite run.
