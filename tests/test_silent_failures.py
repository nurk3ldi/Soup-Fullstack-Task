"""SPEC §7.3 — failures that would otherwise go unnoticed.

Covers: duplicates, partial writes, lost events on reconnect, slow-client overflow, key rotation.
"""
import asyncio
import os
import socket
import time
import uuid
from urllib.parse import urlparse

from conftest import (
    API_URL,
    SSE,
    bearer,
    db_points,
    db_run_state,
    debug_stats,
    make_points,
    open_stream,
    send_batch,
    stream_token,
)

UNAUTHORIZED = b'{"error":"unauthorized"}'

# --- duplicates ------------------------------------------------------------------------------


async def test_same_batch_five_times_concurrently_is_stored_once(client, db, make_project, make_run):
    a = await make_project()
    run = await make_run(a)
    pts = make_points(50)
    bid = str(uuid.uuid4())

    responses = await asyncio.gather(*(send_batch(client, a, run["id"], pts, batch_id=bid) for _ in range(5)))
    assert [r.status_code for r in responses] == [200] * 5
    bodies = [r.json() for r in responses]
    fresh = [b for b in bodies if "duplicate" not in b]
    dupes = [b for b in bodies if b.get("duplicate")]
    print(f"\n[duplicates] 5 concurrent sends of one batch: fresh={len(fresh)} duplicate={len(dupes)} bodies={bodies}")

    assert fresh == [{"accepted": 50, "duplicates": 0, "last_seq": 50}]
    assert dupes == [{"duplicate": True, "accepted": 50}] * 4

    rows = await db_points(db, run["id"])
    assert [row[3] for row in rows] == list(range(1, 51))
    assert await db_run_state(db, run["id"]) == {"last_seq": 50, "points": 50, "batches": 1}


async def test_resend_after_the_fact_is_a_duplicate(client, db, make_project, make_run):
    a = await make_project()
    run = await make_run(a)
    pts = make_points(8)
    bid = str(uuid.uuid4())
    assert (await send_batch(client, a, run["id"], pts, batch_id=bid)).json()["accepted"] == 8
    # Other data arrives in between; the late retry must still report the original result.
    await send_batch(client, a, run["id"], make_points(8, start=100))
    for _ in range(3):
        r = await send_batch(client, a, run["id"], pts, batch_id=bid)
        assert r.status_code == 200
        assert r.json() == {"duplicate": True, "accepted": 8}
    assert await db_run_state(db, run["id"]) == {"last_seq": 16, "points": 16, "batches": 2}


async def test_same_batch_id_with_different_body_is_409(client, db, make_project, make_run):
    a = await make_project()
    run = await make_run(a)
    bid = str(uuid.uuid4())
    assert (await send_batch(client, a, run["id"], make_points(5), batch_id=bid)).status_code == 200
    before = await db_points(db, run["id"])

    changed_value = make_points(5)
    changed_value[2]["value"] = 123.0
    for body in (changed_value, make_points(6), make_points(5, start=50)):
        r = await send_batch(client, a, run["id"], body, batch_id=bid)
        assert r.status_code == 409
        assert r.content == b'{"error":"batch_id_reused"}'

    assert await db_points(db, run["id"]) == before
    assert await db_run_state(db, run["id"]) == {"last_seq": 5, "points": 5, "batches": 1}


async def test_same_batch_id_in_two_runs_is_independent(client, db, make_project, make_run):
    a = await make_project()
    r1, r2 = await make_run(a, "one"), await make_run(a, "two")
    bid = str(uuid.uuid4())
    assert (await send_batch(client, a, r1["id"], make_points(3), batch_id=bid)).json()["accepted"] == 3
    assert (await send_batch(client, a, r2["id"], make_points(4), batch_id=bid)).json()["accepted"] == 4


# --- partial writes --------------------------------------------------------------------------


async def test_one_invalid_point_rejects_the_whole_batch(client, db, make_project, make_run):
    a = await make_project()
    run = await make_run(a)
    bid = str(uuid.uuid4())
    points = ",".join('{"name":"loss","step":%d,"value":0.5}' % i for i in range(999))
    raw = '{"batch_id":"%s","points":[%s,{"name":"loss","step":999,"value":NaN}]}' % (bid, points)

    r = await client.post(
        f"/runs/{run['id']}/metrics", content=raw.encode(), headers={**a.headers, "Content-Type": "application/json"}
    )
    state = await db_run_state(db, run["id"])
    print(f"\n[partial writes] 999 valid + 1 NaN: status={r.status_code} body={r.text} db={state}")
    assert r.status_code == 422
    assert state == {"last_seq": 0, "points": 0, "batches": 0}


async def test_failure_mid_transaction_leaves_nothing_and_retry_succeeds(client, db, make_project, make_run):
    a = await make_project()
    run = await make_run(a)
    await send_batch(client, a, run["id"], make_points(10))  # pre-existing data must survive
    before = await db_points(db, run["id"])
    pts = make_points(100, start=1000)
    bid = str(uuid.uuid4())

    # TEST_MODE hook: the server inserts the first half of the points, then raises.
    r = await send_batch(client, a, run["id"], pts, batch_id=bid, headers={"X-Test-Fault": "mid-insert"})
    state = await db_run_state(db, run["id"])
    print(f"\n[partial writes] fault after 50 of 100 inserts: status={r.status_code} body={r.text} db={state}")
    assert r.status_code == 500
    assert r.content == b'{"error":"internal"}'
    assert state == {"last_seq": 10, "points": 10, "batches": 1}
    assert await db_points(db, run["id"]) == before

    # The client retries the same batch: it is not mistaken for a duplicate, and seq has no gap.
    r = await send_batch(client, a, run["id"], pts, batch_id=bid)
    assert r.status_code == 200
    assert r.json() == {"accepted": 100, "duplicates": 0, "last_seq": 110}
    assert [row[3] for row in await db_points(db, run["id"])] == list(range(1, 111))


# --- lost events on reconnect ----------------------------------------------------------------


async def test_reconnect_with_last_event_id_gets_exactly_the_missed_events(client, db, make_project, make_run):
    a = await make_project()
    run = await make_run(a)
    await send_batch(client, a, run["id"], make_points(40))

    async with await open_stream(client, a, run["id"]) as s:
        first = await s.metrics(40)
    k = first[-1]["seq"]
    assert k == 40

    # Disconnected. 100 more points arrive in 10 batches.
    for i in range(10):
        await send_batch(client, a, run["id"], make_points(10, start=1000 + i * 10))

    async with await open_stream(client, a, run["id"], last_event_id=k) as s:
        missed = await s.metrics(100)
        try:  # and nothing beyond them
            async with asyncio.timeout(2.5):
                extra = await s.next_event()
            raise AssertionError(f"unexpected extra event {extra}")
        except TimeoutError:
            pass
    seqs = [p["seq"] for p in missed]
    print(f"\n[reconnect] read to seq K={k}, ingested 100 while disconnected, resumed with Last-Event-ID={k}: "
          f"received {len(seqs)} events, seq {seqs[0]}..{seqs[-1]}, gaps={sorted(set(range(k + 1, k + 101)) - set(seqs))}")
    assert seqs == list(range(k + 1, k + 101))
    assert [(p["name"], p["step"], p["value"], p["seq"]) for p in first + missed] == await db_points(db, run["id"])


async def test_repeated_drops_while_ingest_is_running_lose_and_repeat_nothing(client, db, make_project, make_run):
    """Ingest runs the whole time; the reader is cut off again and again and resumes each time."""
    a = await make_project()
    run = await make_run(a)
    total = 600

    async def writer():
        for i in range(total // 10):
            await send_batch(client, a, run["id"], make_points(10, start=i * 10))
            await asyncio.sleep(0.01)

    task = asyncio.create_task(writer())
    received, cursor, connections = [], 0, 0
    async with asyncio.timeout(60):
        while len(received) < total:
            connections += 1
            async with await open_stream(client, a, run["id"], last_event_id=cursor) as s:
                take = min(37, total - len(received))  # odd size: cut in the middle of batches
                chunk = await s.metrics(take)
            received += chunk
            cursor = chunk[-1]["seq"]
    await task
    seqs = [p["seq"] for p in received]
    print(f"\n[reconnect] {total} points ingested during {connections} connections: "
          f"received={len(seqs)} unique={len(set(seqs))} in_order={seqs == sorted(seqs)}")
    assert seqs == list(range(1, total + 1))
    assert [(p["name"], p["step"], p["value"], p["seq"]) for p in received] == await db_points(db, run["id"])


# --- slow client / overflow ------------------------------------------------------------------


async def test_stalled_client_is_dropped_and_catches_up_on_reconnect(client, db, make_project, make_run):
    a = await make_project()
    run = await make_run(a)
    before = await debug_stats(client)
    token = await stream_token(client, a, run["id"])

    # A client that opens the stream and then never reads from the socket.
    url = urlparse(API_URL)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)  # small window: stall sooner
    sock.setblocking(False)
    loop = asyncio.get_running_loop()
    await loop.sock_connect(sock, (socket.gethostbyname(url.hostname), url.port))
    request = f"GET /runs/{run['id']}/stream?token={token} HTTP/1.1\r\nHost: {url.hostname}\r\n\r\n"
    await loop.sock_sendall(sock, request.encode())
    try:
        for _ in range(50):
            if (await debug_stats(client))["active_streams"] == before["active_streams"] + 1:
                break
            await asyncio.sleep(0.1)
        assert (await debug_stats(client))["active_streams"] == before["active_streams"] + 1

        sent, t0, stats = 0, time.perf_counter(), before
        for round_no in range(10):
            for _ in range(20):  # 20 000 points per round
                r = await send_batch(client, a, run["id"], make_points(1000, start=sent))
                assert r.status_code == 200
                sent += 1000
            for _ in range(int(before["stall_seconds"] * 10) + 30):
                stats = await debug_stats(client)
                if stats["sse_overflow_total"] > before["sse_overflow_total"]:
                    break
                await asyncio.sleep(0.1)
            if stats["sse_overflow_total"] > before["sse_overflow_total"]:
                break
        elapsed = time.perf_counter() - t0
    finally:
        sock.close()

    print(f"\n[overflow] stalled reader: ingested {sent} points; overflow counted after {elapsed:.1f} s "
          f"(stall timeout {before['stall_seconds']} s); sse_overflow_total {before['sse_overflow_total']} -> "
          f"{stats['sse_overflow_total']}; active_streams {before['active_streams']} -> {stats['active_streams']}; "
          f"max_buffered_points={stats['max_buffered_points']} (page_size {stats['page_size']}); "
          f"rss {before['rss_kb']} kB -> {stats['rss_kb']} kB")
    assert stats["sse_overflow_total"] == before["sse_overflow_total"] + 1
    assert stats["active_streams"] == before["active_streams"]  # the stream was released
    assert stats["max_buffered_points"] <= stats["page_size"]  # memory per connection stayed bounded

    # Ingest was never blocked by the stalled reader, and a fresh connection gets everything.
    assert (await db_run_state(db, run["id"]))["points"] == sent
    async with await open_stream(client, a, run["id"], last_event_id=0) as s:
        got = await s.metrics(sent, timeout=60)
    assert [p["seq"] for p in got] == list(range(1, sent + 1))


# --- key rotation ----------------------------------------------------------------------------


async def test_rotation_kills_api_access_stream_and_token_of_old_key(client, db, make_project, make_run):
    a = await make_project()
    run = await make_run(a)
    await send_batch(client, a, run["id"], make_points(5))
    key2 = (await client.post("/projects/me/keys", headers=a.headers)).json()
    old_token = await stream_token(client, a, run["id"])  # minted with key1, valid for 60 s

    async with await open_stream(client, a, run["id"]) as old_stream, \
            SSE(client, f"/runs/{run['id']}/stream", params={"token": await stream_token(client, _Key(key2), run["id"])}) as new_stream:
        await old_stream.metrics(5)
        await new_stream.metrics(5)

        t0 = time.perf_counter()
        r = await client.post(f"/projects/me/keys/{a.key_id}/revoke", headers=bearer(key2["key"]))
        assert r.status_code == 200

        # 1. API: the very next call with the old key.
        r = await client.get(f"/runs/{run['id']}", headers=a.headers)
        api_ms = (time.perf_counter() - t0) * 1000
        assert r.status_code == 401 and r.content == UNAUTHORIZED

        # 2. The stream opened with the old key is told and closed.
        async with asyncio.timeout(6):
            ev = await old_stream.next_event()
            end = await old_stream.next_event()
        stream_ms = (time.perf_counter() - t0) * 1000
        assert ev["event"] == "revoked"
        assert end is None and old_stream.ended == "eof"

        # 3. A still-unexpired token minted with the old key no longer opens a stream.
        r = await client.get(f"/runs/{run['id']}/stream", params={"token": old_token})
        assert r.status_code == 401 and r.content == UNAUTHORIZED

        # 4. The old key cannot write, and nothing it sends after revocation is stored.
        r = await send_batch(client, a, run["id"], make_points(5, start=100))
        assert r.status_code == 401
        assert (await db_run_state(db, run["id"]))["points"] == 5

        # 5. The stream of the new key is untouched and still live.
        r = await client.post(f"/runs/{run['id']}/metrics", json={"batch_id": str(uuid.uuid4()), "points": make_points(1, start=200)}, headers=bearer(key2["key"]))
        assert r.status_code == 200
        assert (await new_stream.metrics(1))[0]["step"] == 200

    print(f"\n[rotation] after revoke returned: old key rejected by API at +{api_ms:.1f} ms; "
          f"open stream of old key closed at +{stream_ms:.1f} ms (bound: 5000 ms fallback poll)")
    assert stream_ms <= 5000


class _Key:
    """Adapter so helpers that take a Project can be used with a second key of the same project."""

    def __init__(self, key: dict):
        self.headers = bearer(key["key"])


# --- the fallbacks: what happens when a NOTIFY never arrives ---------------------------------


async def test_point_committed_without_notify_is_still_delivered_by_safety_poll(client, db, make_project, make_run):
    """NOTIFY is only a wake-up. Write straight to the database (no NOTIFY at all) and the stream
    must still deliver it, on its periodic re-read."""
    a = await make_project()
    run = await make_run(a)
    await send_batch(client, a, run["id"], make_points(3))
    rid = uuid.UUID(run["id"])

    async with await open_stream(client, a, run["id"]) as s:
        await s.metrics(3)
        t0 = time.perf_counter()
        async with db.transaction():
            await db.execute(
                "INSERT INTO metrics (run_id, name, step, value, seq, batch_id) VALUES ($1, 'loss', 777, 0.1, 4, $2)",
                rid, uuid.uuid4(),
            )
            await db.execute("UPDATE runs SET last_seq = 4 WHERE id = $1", rid)
        got = await s.metrics(1, timeout=5)
        waited = time.perf_counter() - t0
    print(f"\n[fallback] point committed with no NOTIFY was delivered after {waited:.2f} s (safety poll: 2 s)")
    assert got[0]["seq"] == 4 and got[0]["step"] == 777
    assert waited <= 3.0


async def test_key_revoked_without_notify_closes_stream_within_five_seconds(client, db, make_project, make_run):
    """Worst case for rotation: the revocation NOTIFY is lost. The periodic key check must still
    close the stream within its 5 s interval."""
    a = await make_project()
    run = await make_run(a)
    async with await open_stream(client, a, run["id"]) as s:
        await asyncio.sleep(0.3)
        t0 = time.perf_counter()
        await db.execute("UPDATE api_keys SET revoked_at = now() WHERE id = $1", uuid.UUID(a.key_id))
        async with asyncio.timeout(8):
            ev = await s.next_event()
            end = await s.next_event()
        waited = time.perf_counter() - t0
    print(f"\n[fallback] key revoked with no NOTIFY: stream closed after {waited:.2f} s (periodic check: 5 s)")
    assert ev["event"] == "revoked" and end is None
    assert waited <= 5.5


# --- maximum stream lifetime (dead clients the server cannot see) ------------------------------


async def test_stream_is_ended_at_max_lifetime_and_resume_loses_nothing(client, db, make_project, make_run, stream_lifetime):
    a = await make_project()
    run = await make_run(a)
    await send_batch(client, a, run["id"], make_points(5))
    before = await debug_stats(client)
    await stream_lifetime(2)

    t0 = time.perf_counter()
    async with await open_stream(client, a, run["id"]) as s:
        got = await s.metrics(5)
        await send_batch(client, a, run["id"], make_points(5, start=5))  # arrives while open
        got += await s.metrics(5)
        async with asyncio.timeout(10):
            assert await s.next_event() is None  # nothing more: the server ends the stream
        lived = time.perf_counter() - t0
        assert s.ended == "eof", "the stream must end cleanly, not be cut"
    assert 1.7 <= lived <= 3.5, lived  # 2 s +-10 % jitter, plus scheduling slack

    await send_batch(client, a, run["id"], make_points(5, start=10))  # arrives while nobody listens
    async with await open_stream(client, a, run["id"], last_event_id=got[-1]["seq"]) as s:
        got += await s.metrics(5)
    assert [p["seq"] for p in got] == list(range(1, 16))
    assert [(p["name"], p["step"], p["value"], p["seq"]) for p in got] == await db_points(db, run["id"])

    after = await debug_stats(client)
    print(f"\n[lifetime] configured=2s lived={lived:.2f}s recycled={after['sse_recycled_total'] - before['sse_recycled_total']}")
    assert after["sse_recycled_total"] >= before["sse_recycled_total"] + 1


async def test_client_that_never_reads_is_released_at_max_lifetime_even_at_low_rate(client, make_project, make_run, stream_lifetime):
    """The gap left open by the stall timeout: a dead client at a low data rate never blocks a
    write (a few events fit in the kernel buffers), so only the lifetime limit can release it."""
    a = await make_project()
    run = await make_run(a)
    await send_batch(client, a, run["id"], make_points(3))
    before = await debug_stats(client)
    await stream_lifetime(2)
    token = await stream_token(client, a, run["id"])

    url = urlparse(API_URL)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setblocking(False)
    loop = asyncio.get_running_loop()
    await loop.sock_connect(sock, (socket.gethostbyname(url.hostname), url.port))
    request = f"GET /runs/{run['id']}/stream?token={token} HTTP/1.1\r\nHost: {url.hostname}\r\n\r\n"
    await loop.sock_sendall(sock, request.encode())
    try:  # from here on the socket is never read and never closed
        for _ in range(50):
            if (await debug_stats(client))["active_streams"] == before["active_streams"] + 1:
                break
            await asyncio.sleep(0.1)
        assert (await debug_stats(client))["active_streams"] == before["active_streams"] + 1

        t0 = time.perf_counter()
        stats = before
        for _ in range(60):
            stats = await debug_stats(client)
            if stats["active_streams"] == before["active_streams"]:
                break
            await asyncio.sleep(0.1)
        released = time.perf_counter() - t0
        print(
            f"\n[lifetime] never-reading client, 3 points: released after {released:.1f}s "
            f"(limit 2s), overflow_drops={stats['sse_overflow_total'] - before['sse_overflow_total']} "
            f"recycled={stats['sse_recycled_total'] - before['sse_recycled_total']}"
        )
        assert stats["active_streams"] == before["active_streams"], "dead stream is still registered"
        assert stats["sse_overflow_total"] == before["sse_overflow_total"], "released by the stall timeout, not the lifetime"
        assert stats["sse_recycled_total"] == before["sse_recycled_total"] + 1
    finally:
        sock.close()
