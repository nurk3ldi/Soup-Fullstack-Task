"""Rate limiting: per project, and per client address for failed credentials.

The limiters are off on the test service (the rest of the suite needs to send bursts and bad
keys freely); each test here switches them on through a TEST_MODE hook with small numbers.
The last test checks that the plain service has them on without any hook.
"""
import asyncio
import json
import time
import uuid

from conftest import ADMIN_TOKEN, bearer, db_run_state, debug_stats, make_points, send_batch, stream_token

UNAUTHORIZED = b'{"error":"unauthorized"}'
LIMITED = b'{"error":"rate_limited"}'
JSON = {"Content-Type": "application/json"}
BAD = bearer("sk_" + "B" * 43)


def assert_limited(r, label=""):
    assert (r.status_code, r.content) == (429, LIMITED), (label, r.status_code, r.text)
    assert int(r.headers["retry-after"]) >= 1, label
    assert r.headers["cache-control"] == "no-store, private", label


async def test_project_rate_limit_is_per_project_and_refills(client, make_project, make_run, rate_limits):
    a, b = await make_project(), await make_project()
    await make_run(a), await make_run(b)
    before = await debug_stats(client)
    await rate_limits(project_rps=5, project_burst=10)

    # 40 requests at once from A, with B calling in the middle of it.
    results = await asyncio.gather(
        *(client.get("/runs", headers=a.headers) for _ in range(20)),
        client.get("/runs", headers=b.headers),
        *(client.get("/runs", headers=a.headers) for _ in range(20)),
    )
    from_b = results[20]
    from_a = results[:20] + results[21:]
    ok = [r for r in from_a if r.status_code == 200]
    limited = [r for r in from_a if r.status_code == 429]
    assert len(ok) + len(limited) == 40
    assert 10 <= len(ok) <= 12, len(ok)  # the burst of 10, plus what refilled while they ran
    for r in limited:
        assert_limited(r)
    assert from_b.status_code == 200, "one project's burst must not cost another project anything"

    await asyncio.sleep(1.1)  # 5 per second come back
    later = [await client.get("/runs", headers=a.headers) for _ in range(12)]
    passed = sum(r.status_code == 200 for r in later)
    assert 5 <= passed <= 8, passed

    stats = await debug_stats(client)
    counted = stats["rate_limited_project_total"] - before["rate_limited_project_total"]
    print(f"\n[rate-limit] project: burst 10, 5/s: 40 at once -> {len(ok)} passed, {len(limited)} refused; after 1.1 s {passed} of 12 passed")
    assert counted == len(limited) + (12 - passed)


async def test_rate_limited_request_has_no_effect_and_its_body_is_not_read(client, db, make_project, make_run, rate_limits):
    a = await make_project()
    run = await make_run(a)
    await rate_limits(project_rps=1, project_burst=3)
    url = f"/runs/{run['id']}/metrics"
    bid = str(uuid.uuid4())
    pts = make_points(5)

    for _ in range(3):  # use up the burst
        assert (await client.get(f"/runs/{run['id']}", headers=a.headers)).status_code == 200

    assert_limited(await send_batch(client, a, run["id"], pts, batch_id=bid), "valid batch")
    # Refused before validation: a body that would be a 422, or a 413, is a 429 all the same.
    assert_limited(await client.post(url, content=b"{not json", headers={**a.headers, **JSON}), "broken body")
    assert_limited(await client.post(url, content=b"x" * 3_000_000, headers={**a.headers, **JSON}), "oversized body")
    assert_limited(await client.post(f"/runs/{run['id']}/stream-token", headers=a.headers), "stream token")
    assert await db_run_state(db, run["id"]) == {"last_seq": 0, "points": 0, "batches": 0}

    # The client waits and retries the same batch: it is stored once, as a first write.
    await asyncio.sleep(1.2)
    r = await send_batch(client, a, run["id"], pts, batch_id=bid)
    assert (r.status_code, r.json()) == (200, {"accepted": 5, "duplicates": 0, "last_seq": 5})


async def test_opening_streams_counts_against_the_project_limit(client, make_project, make_run, rate_limits):
    a = await make_project()
    run = await make_run(a)
    tokens = [await stream_token(client, a, run["id"]) for _ in range(4)]
    await rate_limits(project_rps=1, project_burst=2)
    statuses = []
    for token in tokens:
        async with client.stream("GET", f"/runs/{run['id']}/stream", params={"token": token}) as r:
            statuses.append(r.status_code)
            if r.status_code == 429:
                assert await r.aread() == LIMITED
    assert statuses == [200, 200, 429, 429]


async def test_failed_credentials_use_up_an_address_and_then_everything_is_refused(client, make_project, make_run, rate_limits):
    a = await make_project()
    run = await make_run(a)
    token = await stream_token(client, a, run["id"])
    before = await debug_stats(client)
    await rate_limits(auth_fail_rps=1, auth_fail_burst=5)

    # Five failures of different kinds are answered normally...
    failures = [
        await client.get("/runs", headers=BAD),
        await client.get("/runs"),
        await client.post("/projects", json={"name": "x"}, headers=bearer("wrong-admin-token")),
        await client.get(f"/runs/{run['id']}/stream", params={"token": "forged"}),
        await client.post("/runs", content=b"{", headers={**BAD, **JSON}),
    ]
    for r in failures:
        assert (r.status_code, r.content) == (401, UNAUTHORIZED)

    # ...and then the address is refused, whatever it presents: a guesser must not learn from
    # the answer whether a guess was right.
    refused = {
        "another bad key": await client.get("/runs", headers=BAD),
        "the valid key": await client.get("/runs", headers=a.headers),
        "the right admin token": await client.post("/projects", json={"name": "x"}, headers=bearer(ADMIN_TOKEN)),
        "a valid stream token": await client.get(f"/runs/{run['id']}/stream", params={"token": token}),
    }
    for label, r in refused.items():
        assert_limited(r, label)

    # What needs no credential is never limited.
    for path in ("/healthz", "/", "/static/app.js", f"/runs/{run['id']}/view"):
        assert (await client.get(path)).status_code == 200, path

    # The allowance comes back at 1 per second. A valid request does not use it up; a bad one does.
    await asyncio.sleep(1.2)
    assert (await client.get("/runs", headers=a.headers)).status_code == 200
    assert (await client.get("/runs", headers=a.headers)).status_code == 200
    assert (await client.get("/runs", headers=BAD)).status_code == 401
    assert_limited(await client.get("/runs", headers=a.headers), "valid key right after one more failure")

    stats = await debug_stats(client)
    counted = stats["rate_limited_address_total"] - before["rate_limited_address_total"]
    print(f"\n[rate-limit] address: 5 failed credentials answered 401, then 429 for everything (counted {counted}); 1 allowed again per second")
    assert counted == 5


async def test_guessing_is_slowed_to_the_refill_rate(client, rate_limits):
    await rate_limits(auth_fail_rps=2, auth_fail_burst=5)
    t0 = time.perf_counter()
    answered = 0
    sent = 0
    while time.perf_counter() - t0 < 3.0:
        r = await client.get("/runs", headers=bearer("sk_" + uuid.uuid4().hex + uuid.uuid4().hex[:11]))
        sent += 1
        answered += r.status_code == 401
    elapsed = time.perf_counter() - t0
    print(f"\n[rate-limit] guessing for {elapsed:.1f} s: {sent} attempts sent, {answered} evaluated (burst 5 + 2/s)")
    assert sent > 100, "the loop was too slow to prove anything"
    assert 5 <= answered <= 5 + 2 * elapsed + 1


async def test_plain_service_limits_failed_credentials_without_any_hook(plain_client, make_project):
    """The service without TEST_MODE runs with its defaults: 20 failures, then 1 per second."""
    a = await make_project()
    assert (await plain_client.get("/runs", headers=a.headers)).status_code == 200
    statuses = [(await plain_client.get("/runs", headers=BAD)).status_code for _ in range(40)]
    evaluated = statuses.count(401)
    print(f"\n[rate-limit] plain api, defaults: 40 bad keys in a row -> {evaluated} x 401, {statuses.count(429)} x 429")
    assert set(statuses) == {401, 429}
    assert 20 <= evaluated <= 23  # the burst of 20, plus refill during the loop
    assert statuses[-1] == 429
    assert_limited(await plain_client.get("/runs", headers=a.headers), "valid key while the address is refused")
    await asyncio.sleep(3.0)  # leave the address usable for the tests that follow
    assert (await plain_client.get("/runs", headers=a.headers)).status_code == 200
