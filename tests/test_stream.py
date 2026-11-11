"""Step 4: the SSE stream — tokens, catch-up, live delivery, resume point, test hooks."""
import asyncio
import random
import time
import uuid

from conftest import SSE, db_points, debug_stats, make_points, open_stream, send_batch, stream_token

UNAUTHORIZED = b'{"error":"unauthorized"}'
NOT_FOUND = b'{"error":"not_found"}'


# --- tokens ----------------------------------------------------------------------------------


async def test_stream_token_only_for_own_runs(client, make_project, make_run):
    a, b = await make_project(), await make_project()
    rb = await make_run(b)
    other = await client.post(f"/runs/{rb['id']}/stream-token", headers=a.headers)
    missing = await client.post(f"/runs/{uuid.uuid4()}/stream-token", headers=a.headers)
    for r in (other, missing):
        assert r.status_code == 404
        assert r.content == NOT_FOUND
    r = await client.post(f"/runs/{rb['id']}/stream-token")
    assert r.status_code == 401 and r.content == UNAUTHORIZED


async def test_stream_rejects_bad_tokens_identically(client, make_project, make_run):
    a = await make_project()
    ra = await make_run(a)
    await send_batch(client, a, ra["id"], make_points(3))
    good = await stream_token(client, a, ra["id"])
    payload, _, sig = good.rpartition(".")
    parts = payload.split(".")
    later = ".".join(parts[:3] + [str(int(parts[3]) + 3600)])  # extend expiry, keep old signature

    bad = {
        "missing": None,
        "empty": "",
        "garbage": "not-a-token",
        "signature flipped": payload + "." + ("0" if sig[0] != "0" else "1") + sig[1:],
        "signature dropped": payload,
        "expiry extended": later + "." + sig,
        "non-ascii": payload + ".ключ",
    }
    for label, token in bad.items():
        params = {} if token is None else {"token": token}
        r = await client.get(f"/runs/{ra['id']}/stream", params=params)
        assert r.status_code == 401, label
        assert r.content == UNAUTHORIZED, label
        assert "text/event-stream" not in r.headers["content-type"], label

    r = await client.get("/runs/not-a-uuid/stream", params={"token": good})
    assert r.status_code == 401 and r.content == UNAUTHORIZED


async def test_expired_stream_token_is_rejected(client, make_project, make_run):
    a = await make_project()
    ra = await make_run(a)
    token = await stream_token(client, a, ra["id"], headers={"X-Test-Token-TTL": "1"})  # TEST_MODE hook
    async with SSE(client, f"/runs/{ra['id']}/stream", params={"token": token}) as s:
        assert s.response.status_code == 200  # valid while fresh
    await asyncio.sleep(1.2)
    r = await client.get(f"/runs/{ra['id']}/stream", params={"token": token})
    assert r.status_code == 401 and r.content == UNAUTHORIZED


async def test_token_ttl_header_is_ignored_without_test_mode(plain_client, make_project, make_run):
    a = await make_project()
    ra = await make_run(a)
    r = await plain_client.post(f"/runs/{ra['id']}/stream-token", headers={**a.headers, "X-Test-Token-TTL": "1"})
    assert r.status_code == 200
    assert r.json()["expires_in"] == 60


async def test_token_for_one_run_does_not_open_another(client, make_project, make_run):
    a, b = await make_project(), await make_project()
    ra, ra2, rb = await make_run(a, "ra"), await make_run(a, "ra2"), await make_run(b, "rb")
    await send_batch(client, b, rb["id"], make_points(5, "secret-b"))
    await send_batch(client, a, ra2["id"], make_points(5, "other-run"))
    token = await stream_token(client, a, ra["id"])

    for target in (rb["id"], ra2["id"], str(uuid.uuid4())):
        r = await client.get(f"/runs/{target}/stream", params={"token": token})
        assert r.status_code == 401
        assert r.content == UNAUTHORIZED  # and therefore not a single byte of the other run


# --- delivery --------------------------------------------------------------------------------


async def test_catch_up_then_live(client, db, make_project, make_run):
    a = await make_project()
    run = await make_run(a)
    await send_batch(client, a, run["id"], make_points(20))

    async with await open_stream(client, a, run["id"]) as s:
        assert s.response.status_code == 200
        assert s.response.headers["content-type"].startswith("text/event-stream")
        assert s.response.headers["cache-control"] == "no-store, private"
        got = await s.metrics(20)
        assert [p["seq"] for p in got] == list(range(1, 21))
        assert set(got[0]) == {"name", "step", "value", "seq", "ts"}

        # Live: the point must arrive via NOTIFY, i.e. well before the 2 s safety poll.
        t0 = time.perf_counter()
        await send_batch(client, a, run["id"], make_points(1, start=500))
        live = await s.metrics(1)
        latency_ms = (time.perf_counter() - t0) * 1000
        print(f"\n[stream] live delivery (ingest request start → event received): {latency_ms:.1f} ms")
        assert live[0]["seq"] == 21 and live[0]["step"] == 500
        assert latency_ms < 1000

    stored = await db_points(db, run["id"])
    assert [(p["name"], p["step"], p["value"], p["seq"]) for p in got + live] == stored


async def test_concurrent_ingest_is_delivered_in_seq_order_exactly_once(client, db, make_project, make_run):
    a = await make_project()
    run = await make_run(a)
    batches = [make_points(10, random.choice(["loss", "acc", "lr"]) + str(i), start=0) for i in range(30)]

    async with await open_stream(client, a, run["id"]) as s:
        sender = asyncio.gather(*(send_batch(client, a, run["id"], b) for b in batches))
        got = await s.metrics(300, timeout=20)
        await sender
    assert [p["seq"] for p in got] == list(range(1, 301))
    assert [(p["name"], p["step"], p["value"], p["seq"]) for p in got] == await db_points(db, run["id"])


async def test_large_backlog_is_paged_completely(client, db, make_project, make_run):
    a = await make_project()
    run = await make_run(a)
    for i in range(3):
        await send_batch(client, a, run["id"], make_points(1000, start=i * 1000))
    async with await open_stream(client, a, run["id"]) as s:
        got = await s.metrics(3000, timeout=20)
    assert [p["seq"] for p in got] == list(range(1, 3001))
    assert (await debug_stats(client))["max_buffered_points"] <= 500


async def test_resume_point_precedence(client, make_project, make_run):
    a = await make_project()
    run = await make_run(a)
    await send_batch(client, a, run["id"], make_points(10))

    async with await open_stream(client, a, run["id"], since_seq=7) as s:
        assert [p["seq"] for p in await s.metrics(3)] == [8, 9, 10]
    async with await open_stream(client, a, run["id"], last_event_id=4) as s:
        assert [p["seq"] for p in await s.metrics(6)] == [5, 6, 7, 8, 9, 10]
    # Last-Event-ID wins over ?since_seq.
    async with await open_stream(client, a, run["id"], last_event_id=9, since_seq=2) as s:
        assert [p["seq"] for p in await s.metrics(1)] == [10]
    # A junk Last-Event-ID is ignored rather than trusted or fatal.
    token = await stream_token(client, a, run["id"])
    async with SSE(client, f"/runs/{run['id']}/stream", headers={"Last-Event-ID": "abc"}, params={"token": token, "since_seq": 8}) as s:
        assert [p["seq"] for p in await s.metrics(2)] == [9, 10]


async def test_stream_from_the_end_sends_nothing_until_new_data(client, make_project, make_run):
    a = await make_project()
    run = await make_run(a)
    await send_batch(client, a, run["id"], make_points(5))
    async with await open_stream(client, a, run["id"], last_event_id=5) as s:
        try:
            async with asyncio.timeout(2.5):  # longer than one safety poll
                ev = await s.next_event()
            raise AssertionError(f"unexpected event {ev}")
        except TimeoutError:
            pass


async def test_heartbeat_events_are_sent_on_idle_stream(client, make_project, make_run):
    """The test API pings every 1 s (production default: 15 s). Pings are real events so that the
    browser page can see them; the stream announces the interval in its first event."""
    a = await make_project()
    run = await make_run(a)
    async with await open_stream(client, a, run["id"]) as s:
        try:
            async with asyncio.timeout(3.6):
                await s.next_event()
        except TimeoutError:
            pass
        print(f"\n[stream] hello={s.hello} pings seen on an idle stream in 3.6 s: {s.pings}")
        assert s.hello == '{"ping_seconds":1.0}'
        assert 2 <= s.pings <= 4


# --- test hooks ------------------------------------------------------------------------------


async def test_kill_hook_drops_stream_and_last_event_id_resumes(client, make_project, make_run):
    a = await make_project()
    run = await make_run(a)
    await send_batch(client, a, run["id"], make_points(10))
    async with await open_stream(client, a, run["id"]) as s:
        got = await s.metrics(10)
        r = await client.post(f"/debug/kill-streams/{run['id']}")
        assert r.json() == {"killed": 1}
        async with asyncio.timeout(5):
            assert await s.next_event() is None
        assert s.ended == "dropped"  # cut mid-stream, not a clean end

    await send_batch(client, a, run["id"], make_points(10, start=10))
    async with await open_stream(client, a, run["id"], last_event_id=got[-1]["seq"]) as s:
        assert [p["seq"] for p in await s.metrics(10)] == list(range(11, 21))


async def test_debug_endpoints_do_not_exist_without_test_mode(plain_client, make_project, make_run):
    a = await make_project()
    run = await make_run(a)
    for method, path in (
        ("GET", "/debug/stats"),
        ("GET", "/debug/logs"),
        ("POST", f"/debug/kill-streams/{run['id']}"),
        ("POST", "/debug/stream-lifetime/1"),
    ):
        for headers in ({}, a.headers):
            r = await plain_client.request(method, path, headers=headers)
            assert r.status_code == 404
            assert r.content == NOT_FOUND


async def test_streams_are_released_after_client_disconnect(client, make_project, make_run):
    a = await make_project()
    run = await make_run(a)
    before = (await debug_stats(client))["active_streams"]
    async with await open_stream(client, a, run["id"]):
        async with await open_stream(client, a, run["id"]):
            await asyncio.sleep(0.2)
            assert (await debug_stats(client))["active_streams"] == before + 2
    for _ in range(50):
        if (await debug_stats(client))["active_streams"] == before:
            break
        await asyncio.sleep(0.1)
    assert (await debug_stats(client))["active_streams"] == before
