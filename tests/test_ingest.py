"""Step 3: batch ingest — seq assignment, dedup policy, validation, snapshot read."""
import asyncio
import json
import random
import uuid

from conftest import db_points, db_run_state, make_points, send_batch

NOT_FOUND = b'{"error":"not_found"}'
INVALID = b'{"error":"invalid_request"}'


async def test_ingest_assigns_gap_free_seq_in_name_step_order(client, db, make_project, make_run):
    a = await make_project()
    run = await make_run(a)
    pts = make_points(5, "loss") + make_points(5, "acc")
    random.Random(1).shuffle(pts)

    r = await send_batch(client, a, run["id"], pts)
    assert r.status_code == 200
    assert r.json() == {"accepted": 10, "duplicates": 0, "last_seq": 10}

    rows = await db_points(db, run["id"])
    assert [row[3] for row in rows] == list(range(1, 11))
    # Deterministic numbering inside a batch: by (name, step), whatever order the client sent.
    assert [(row[0], row[1]) for row in rows] == sorted((p["name"], p["step"]) for p in pts)
    assert await db_run_state(db, run["id"]) == {"last_seq": 10, "points": 10, "batches": 1}


async def test_overlapping_batch_first_write_wins_and_seq_has_no_gaps(client, db, make_project, make_run):
    a = await make_project()
    run = await make_run(a)
    first = make_points(10, "loss")  # steps 0..9
    assert (await send_batch(client, a, run["id"], first)).json()["last_seq"] == 10

    # Steps 5..14: 5 already stored (sent with different values), 5 new.
    overlap = [{"name": "loss", "step": s, "value": 999.0} for s in range(5, 15)]
    r = await send_batch(client, a, run["id"], overlap)
    assert r.status_code == 200
    assert r.json() == {"accepted": 5, "duplicates": 5, "last_seq": 15}

    rows = await db_points(db, run["id"])
    assert [row[3] for row in rows] == list(range(1, 16))
    by_step = {row[1]: row[2] for row in rows}
    for p in first:
        assert by_step[p["step"]] == p["value"]  # original value kept
    assert all(by_step[s] == 999.0 for s in range(10, 15))


async def test_duplicate_key_inside_one_batch_keeps_first_occurrence(client, db, make_project, make_run):
    a = await make_project()
    run = await make_run(a)
    pts = [
        {"name": "loss", "step": 1, "value": 0.5},
        {"name": "loss", "step": 2, "value": 0.4},
        {"name": "loss", "step": 1, "value": 0.9},
    ]
    r = await send_batch(client, a, run["id"], pts)
    assert r.json() == {"accepted": 2, "duplicates": 1, "last_seq": 2}
    assert await db_points(db, run["id"]) == [("loss", 1, 0.5, 1), ("loss", 2, 0.4, 2)]


async def test_fully_duplicate_batch_with_new_id_changes_nothing_but_ledger(client, db, make_project, make_run):
    a = await make_project()
    run = await make_run(a)
    pts = make_points(4)
    await send_batch(client, a, run["id"], pts)
    r = await send_batch(client, a, run["id"], pts)  # new batch_id, same points
    assert r.json() == {"accepted": 0, "duplicates": 4, "last_seq": 4}
    assert await db_run_state(db, run["id"]) == {"last_seq": 4, "points": 4, "batches": 2}


async def test_concurrent_batches_to_one_run_get_unique_gap_free_seq(client, db, make_project, make_run):
    a = await make_project()
    run = await make_run(a)
    batches = [make_points(10, "loss", start=i * 10) for i in range(30)]
    random.Random(2).shuffle(batches)

    responses = await asyncio.gather(*(send_batch(client, a, run["id"], b) for b in batches))
    assert [r.status_code for r in responses] == [200] * 30
    assert sorted(r.json()["last_seq"] for r in responses) == list(range(10, 301, 10))

    rows = await db_points(db, run["id"])
    assert [row[3] for row in rows] == list(range(1, 301))
    assert {row[1] for row in rows} == set(range(300))
    assert await db_run_state(db, run["id"]) == {"last_seq": 300, "points": 300, "batches": 30}


async def test_runs_do_not_share_seq(client, db, make_project, make_run):
    a = await make_project()
    r1, r2 = await make_run(a, "one"), await make_run(a, "two")
    await send_batch(client, a, r1["id"], make_points(3))
    await send_batch(client, a, r2["id"], make_points(7))
    assert (await db_run_state(db, r1["id"]))["last_seq"] == 3
    assert (await db_run_state(db, r2["id"]))["last_seq"] == 7


async def test_ts_is_stored_and_optional(client, db, make_project, make_run):
    a = await make_project()
    run = await make_run(a)
    pts = [
        {"name": "loss", "step": 0, "value": 1.0, "ts": "2026-11-10T15:00:00Z"},
        {"name": "loss", "step": 1, "value": 0.9, "ts": "2026-11-10T20:00:01+05:00"},
        {"name": "loss", "step": 2, "value": 0.8},
    ]
    assert (await send_batch(client, a, run["id"], pts)).status_code == 200
    rows = await db.fetch("SELECT step, client_ts FROM metrics WHERE run_id = $1 ORDER BY step", uuid.UUID(run["id"]))
    assert rows[0]["client_ts"].isoformat() == "2026-11-10T15:00:00+00:00"
    assert rows[1]["client_ts"].isoformat() == "2026-11-10T15:00:01+00:00"
    assert rows[2]["client_ts"] is None


async def test_invalid_batches_are_rejected_and_write_nothing(client, db, make_project, make_run):
    a = await make_project()
    run = await make_run(a)
    ok = {"name": "loss", "step": 0, "value": 0.5}
    bid = str(uuid.uuid4())
    bad_bodies = {
        "too many points": {"batch_id": bid, "points": make_points(1001)},
        "no points": {"batch_id": bid, "points": []},
        "points missing": {"batch_id": bid},
        "batch_id missing": {"points": [ok]},
        "batch_id not a uuid": {"batch_id": "abc", "points": [ok]},
        "name too long": {"batch_id": bid, "points": [{**ok, "name": "n" * 65}]},
        "name empty": {"batch_id": bid, "points": [{**ok, "name": ""}]},
        "name with NUL": {"batch_id": bid, "points": [{**ok, "name": "lo\u0000ss"}]},
        "name not a string": {"batch_id": bid, "points": [{**ok, "name": 7}]},
        "value is a string": {"batch_id": bid, "points": [{**ok, "value": "0.5"}]},
        "value is a bool": {"batch_id": bid, "points": [{**ok, "value": True}]},
        "value is null": {"batch_id": bid, "points": [{**ok, "value": None}]},
        "step negative": {"batch_id": bid, "points": [{**ok, "step": -1}]},
        "step fractional": {"batch_id": bid, "points": [{**ok, "step": 1.5}]},
        "step too big": {"batch_id": bid, "points": [{**ok, "step": 2**63}]},
        "ts without timezone": {"batch_id": bid, "points": [{**ok, "ts": "2026-11-10T15:00:00"}]},
        "ts garbage": {"batch_id": bid, "points": [{**ok, "ts": "yesterday"}]},
        "body is a list": [ok],
    }
    for label, body in bad_bodies.items():
        r = await client.post(f"/runs/{run['id']}/metrics", json=body, headers=a.headers)
        assert r.status_code == 422, label
        assert r.content == INVALID, label

    # Non-finite floats are not valid JSON, so they have to be sent as raw bytes.
    raw_headers = {**a.headers, "Content-Type": "application/json"}
    for literal in ("NaN", "Infinity", "-Infinity", "1e999"):
        raw = '{"batch_id": "%s", "points": [{"name": "loss", "step": 0, "value": %s}]}' % (bid, literal)
        r = await client.post(f"/runs/{run['id']}/metrics", content=raw.encode(), headers=raw_headers)
        assert r.status_code == 422, literal
        assert r.content == INVALID, literal

    assert await db_run_state(db, run["id"]) == {"last_seq": 0, "points": 0, "batches": 0}
    # Boundaries that must be accepted: exactly 1000 points, a 64-char name, the largest step.
    edge = make_points(998) + [{"name": "n" * 64, "step": 0, "value": 0.0}, {"name": "big", "step": 2**63 - 1, "value": -1e308}]
    r = await send_batch(client, a, run["id"], edge)
    assert r.status_code == 200
    assert r.json() == {"accepted": 1000, "duplicates": 0, "last_seq": 1000}


async def test_cannot_ingest_into_another_projects_run(client, db, make_project, make_run):
    a, b = await make_project(), await make_project()
    rb = await make_run(b)
    await send_batch(client, b, rb["id"], make_points(3))
    before = await db_run_state(db, rb["id"])

    other = await send_batch(client, a, rb["id"], make_points(5, start=100))
    missing = await send_batch(client, a, str(uuid.uuid4()), make_points(5))
    for r in (other, missing):
        assert r.status_code == 404
        assert r.content == NOT_FOUND
    assert await db_run_state(db, rb["id"]) == before

    read_other = await client.get(f"/runs/{rb['id']}/metrics", headers=a.headers)
    read_missing = await client.get(f"/runs/{uuid.uuid4()}/metrics", headers=a.headers)
    for r in (read_other, read_missing):
        assert r.status_code == 404
        assert r.content == NOT_FOUND


async def test_snapshot_read_pages_by_seq(client, db, make_project, make_run):
    a = await make_project()
    run = await make_run(a)
    await send_batch(client, a, run["id"], make_points(25))
    url = f"/runs/{run['id']}/metrics"

    seen, since = [], 0
    for _ in range(10):
        page = (await client.get(url, params={"since_seq": since, "limit": 10}, headers=a.headers)).json()
        assert page["last_seq"] == 25
        seen += page["points"]
        if not page["has_more"]:
            break
        since = page["points"][-1]["seq"]
    assert [p["seq"] for p in seen] == list(range(1, 26))
    assert [(p["name"], p["step"], p["value"], p["seq"]) for p in seen] == await db_points(db, run["id"])

    empty = (await client.get(url, params={"since_seq": 25}, headers=a.headers)).json()
    assert empty == {"points": [], "last_seq": 25, "has_more": False}
    for params in ({"since_seq": -1}, {"limit": 0}, {"limit": 5001}, {"since_seq": "x"}):
        r = await client.get(url, params=params, headers=a.headers)
        assert r.status_code == 422 and r.content == INVALID, params


async def test_run_list_reports_points_and_last_seq(client, make_project, make_run):
    a = await make_project()
    run = await make_run(a)
    await send_batch(client, a, run["id"], make_points(6))
    listed = (await client.get("/runs", headers=a.headers)).json()["runs"]
    assert [(r["id"], r["points"], r["last_seq"]) for r in listed] == [(run["id"], 6, 6)]


async def test_fault_header_is_ignored_without_test_mode(plain_client, db, make_project, make_run):
    """On the normal API (TEST_MODE off) the fault-injection header must do nothing."""
    a = await make_project()
    run = await make_run(a)
    r = await send_batch(plain_client, a, run["id"], make_points(4), headers={"X-Test-Fault": "mid-insert"})
    assert r.status_code == 200
    assert r.json() == {"accepted": 4, "duplicates": 0, "last_seq": 4}
    assert await db_run_state(db, run["id"]) == {"last_seq": 4, "points": 4, "batches": 1}
