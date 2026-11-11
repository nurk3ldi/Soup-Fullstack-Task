"""SPEC §7.1 — consistency, end to end: what the open run page shows must equal what is stored.

A real (headless) Chromium has the run page open while a client ingests 500 points the way an
unreliable network would deliver them: shuffled, some batches re-sent, some overlapping, some
late. In the middle the page's connection is broken in one of four ways. At the end the page's
state, the page's DOM, the API snapshot and the database must be exactly the same set of points.

The browser reaches the API through tests/netproxy.py, a TCP proxy that can cut or silently stall
real connections. (Playwright's set_offline is not used for this: see the last test.)
"""
import asyncio
import json
import random
import time
import uuid
from dataclasses import dataclass, field

from urllib.parse import urlparse

import pytest

from conftest import API_URL, db_run_state, debug_stats, make_points, send_batch, ui_login, wait_js
from netproxy import FlakyProxy

METRICS = {"loss": 200, "accuracy": 200, "lr": 100}  # 500 unique (name, step) keys
SPREAD_SECONDS = 9.0  # ingestion is spread over this long, so it overlaps the disruption


@dataclass
class Op:
    at: float  # seconds after start
    batch_id: str
    points: list
    kind: str  # "first" | "resend" | "overlap" | "late"
    response: dict = field(default_factory=dict)


def build_plan(rng: random.Random) -> list[Op]:
    points = [
        {"name": name, "step": step, "value": rng.uniform(-5, 5) * 10 ** rng.randint(-6, 3)}
        for name, count in METRICS.items()
        for step in range(count)
    ]
    rng.shuffle(points)  # batches mix metrics and carry steps in no order at all
    batches = []
    while points:
        size = rng.randint(4, 20)
        batches.append((str(uuid.uuid4()), points[:size]))
        points = points[size:]

    ops = []
    late = set(rng.sample(range(len(batches)), k=max(1, len(batches) // 10)))
    for i, (batch_id, pts) in enumerate(batches):
        if i in late:  # held back until after everything else
            ops.append(Op(SPREAD_SECONDS + rng.uniform(0.5, 1.5), batch_id, pts, "late"))
        else:
            ops.append(Op(rng.uniform(0, SPREAD_SECONDS), batch_id, pts, "first"))
    # ~20 % of batches are sent again with the same batch_id: some at once, some much later.
    for op in rng.sample(ops, k=len(ops) // 5):
        delay = rng.choice([0.0, 0.0, rng.uniform(0.2, 4.0)])
        ops.append(Op(op.at + delay, op.batch_id, op.points, "resend"))
    # A few NEW batches repeat points that other batches also carry, with different values:
    # whichever arrives first must win, and the page must show that same winner.
    carried = [p for op in ops if op.kind in ("first", "late") for p in op.points]
    for _ in range(5):
        clash = [{**p, "value": p["value"] + 1000.0} for p in rng.sample(carried, k=8)]
        ops.append(Op(rng.uniform(0, SPREAD_SECONDS + 1.5), str(uuid.uuid4()), clash, "overlap"))
    return ops


@pytest.fixture
async def proxy():
    url = urlparse(API_URL)
    p = await FlakyProxy(url.hostname, url.port).start()
    try:
        yield p
    finally:
        await p.stop()


async def read_page(page) -> dict:
    return await page.evaluate("window.__state")


async def read_dom(page) -> list:
    return await page.evaluate(
        """[...document.querySelectorAll('#metrics section')].flatMap(s =>
             [...s.querySelectorAll('tbody tr')].map(r => [s.dataset.metric, Number(r.dataset.step), r.children[1].textContent]))"""
    )


async def stream_opens(client, run_id: str) -> list[tuple[str, int]]:
    """(resume_source, cursor) of every stream the server opened for this run, from its log."""
    lines = (await client.get("/debug/logs")).json()["lines"]
    events = [json.loads(line) for line in lines if '"stream open"' in line and run_id in line]
    return [(e["resume_source"], e["cursor"]) for e in events]


async def run_scenario(client, db, page, proxy, make_project, make_run, *, seed: int, label: str, disrupt):
    rng = random.Random(seed)
    project = await make_project()
    run = await make_run(project, f"e2e-{label}")
    rid = uuid.UUID(run["id"])

    await ui_login(page, project.key, base=proxy.url)  # the browser only ever talks to the proxy
    await page.locator("#runs-body a").click()
    await wait_js(page, "window.__state && window.__state.status === 'connected'")

    ops = build_plan(rng)

    async def fire(op: Op):
        await asyncio.sleep(op.at)
        r = await send_batch(client, project, run["id"], op.points, batch_id=op.batch_id)
        assert r.status_code == 200, (op.kind, r.status_code, r.text)
        op.response = r.json()

    started = time.perf_counter()
    writer = asyncio.gather(*(fire(op) for op in ops))
    try:
        observed = await disrupt(page=page, client=client, db=db, proxy=proxy, run_id=run["id"])
    finally:
        await writer
    ingest_seconds = time.perf_counter() - started

    # --- wait for the page to catch up with the database ---
    db_state = await db_run_state(db, run["id"])
    await wait_js(page, f"window.__state.last_seq === {db_state['last_seq']}", timeout=30)
    await wait_js(page, f"document.querySelectorAll('#metrics tbody tr').length === {db_state['points']}", timeout=10)
    await asyncio.sleep(1.0)  # anything wrongly still in flight gets a chance to show up

    # --- the four views of the run ---
    rows = await db.fetch("SELECT name, step, value, seq FROM metrics WHERE run_id = $1", rid)
    in_db = {(r["name"], r["step"]): (r["value"], r["seq"]) for r in rows}
    state = await read_page(page)
    on_page = {(p["name"], p["step"]): (p["value"], p["seq"]) for p in state["points"]}
    dom = await read_dom(page)
    in_dom = {(name, step): float(text) for name, step, text in dom}
    snapshot = (await client.get(f"/runs/{run['id']}/metrics", params={"limit": 5000}, headers=project.headers)).json()
    in_api = {(p["name"], p["step"]): (p["value"], p["seq"]) for p in snapshot["points"]}

    fresh = [op for op in ops if "duplicate" not in op.response]
    replayed = [op for op in ops if op.response.get("duplicate")]
    opens = await stream_opens(client, run["id"])
    summary = {
        "scenario": label,
        "seed": seed,
        "batches_sent": len(ops),
        "points_sent": sum(len(op.points) for op in ops),
        "re_sent_batches": sum(op.kind == "resend" for op in ops),
        "overlap_batches": sum(op.kind == "overlap" for op in ops),
        "late_batches": sum(op.kind == "late" for op in ops),
        "duplicate_responses": len(replayed),
        "accepted_total": sum(op.response["accepted"] for op in fresh),
        "unique": sum(METRICS.values()),
        "db_rows": len(in_db),
        "db_last_seq": db_state["last_seq"],
        "page_rows": len(state["points"]),
        "page_last_seq": state["last_seq"],
        "dom_rows": len(dom),
        "api_rows": len(snapshot["points"]),
        "reconnects": state["reconnects"],
        "stream_opens": opens,
        "ingest_seconds": round(ingest_seconds, 1),
        **observed,
    }
    print("\n[e2e] " + " ".join(f"{k}={v}" for k, v in summary.items()))

    # --- the claim ---
    assert on_page == in_db, "page state differs from the database"
    assert in_api == in_db, "API snapshot differs from the database"
    assert in_dom == {k: v for k, (v, _) in in_db.items()}, "rendered table differs from the database"
    assert len(dom) == len(in_dom), "the same (metric, step) is rendered more than once"
    assert len(in_db) == sum(METRICS.values()) == state["total"]
    assert state["last_seq"] == db_state["last_seq"] == len(in_db)
    assert sorted(seq for _, seq in in_db.values()) == list(range(1, len(in_db) + 1)), "seq has gaps or repeats"
    assert summary["accepted_total"] == len(in_db)
    assert len(replayed) == summary["re_sent_batches"]
    for name in METRICS:  # tables are ordered by step
        steps = [step for metric, step, _ in dom if metric == name]
        assert steps == sorted(steps)
    assert await page.inner_text("#total") == str(len(in_db))
    assert await page.inner_text("#last-seq") == str(db_state["last_seq"])

    # --- and the page is live again, not merely caught up ---
    extra = [{"name": "loss", "step": 10_000 + i, "value": float(i)} for i in range(5)]
    assert (await send_batch(client, project, run["id"], extra)).status_code == 200
    await wait_js(page, f"window.__state.last_seq === {db_state['last_seq'] + 5} && window.__state.status === 'connected'", timeout=15)
    assert page.errors == []
    return summary


# --- disruptions -----------------------------------------------------------------------------

WINDOW_SECONDS = 4.0


async def _watch_window(page, db, run_id) -> dict:
    """Sample the page and the database across the disruption window."""
    await asyncio.sleep(0.3)  # let bytes already in flight land
    page_from = (await read_page(page))["last_seq"]
    db_from = (await db_run_state(db, run_id))["last_seq"]
    statuses = []
    for _ in range(int((WINDOW_SECONDS - 0.3) / 0.25)):
        await asyncio.sleep(0.25)
        status = (await read_page(page))["status"]
        if not statuses or statuses[-1] != status:
            statuses.append(status)
    page_to = (await read_page(page))["last_seq"]
    db_to = (await db_run_state(db, run_id))["last_seq"]
    return {"window_page_seq": f"{page_from}->{page_to}", "window_db_seq": f"{db_from}->{db_to}", "window_statuses": "/".join(statuses)}


async def _time_to_recover(page) -> float:
    t0 = time.perf_counter()
    await wait_js(page, "window.__state.status === 'connected'", timeout=30)
    return round(time.perf_counter() - t0, 1)


async def network_down(*, page, client, db, proxy, run_id):
    """Every connection of the browser is reset and new ones are refused for 4 s."""
    await asyncio.sleep(3.0)
    proxy.mode = "refuse"
    cut = proxy.cut()
    seen = await _watch_window(page, db, run_id)
    proxy.mode = "pass"
    return {**seen, "connections_cut": cut, "refused_during_window": proxy.refused, "recover_seconds": await _time_to_recover(page)}


async def silent_stall(*, page, client, db, proxy, run_id):
    """The connection stays open but nothing flows any more, in either direction, and no error is
    ever delivered (half-open TCP, or a proxy that stopped forwarding). New connections hang too
    for 4 s. The stalled stream is never un-stalled: only the page can get itself out of this."""
    await asyncio.sleep(3.0)
    before = await debug_stats(client)
    proxy.mode = "blackhole"
    frozen = proxy.freeze()
    seen = await _watch_window(page, db, run_id)
    proxy.mode = "pass"
    recover = await _time_to_recover(page)
    await asyncio.sleep(3.0)  # longer than the server's stall timeout in test mode (2 s)
    after = await debug_stats(client)
    return {**seen, "connections_frozen": frozen, "recover_seconds": recover,
            # Observations about the SERVER side of the dead connection (reported, not asserted):
            "server_streams": f"{before['active_streams']}->{after['active_streams']}",
            "server_recycled": after["sse_recycled_total"] - before["sse_recycled_total"],
            "server_overflow_drops": after["sse_overflow_total"] - before["sse_overflow_total"]}


async def kill_twice(*, page, client, db, proxy, run_id):
    """The server drops the open stream abruptly, twice (as a crashing server or proxy would)."""
    killed = []
    for wait in (3.0, 3.0):
        await asyncio.sleep(wait)
        r = await client.post(f"/debug/kill-streams/{run_id}")
        killed.append(r.json()["killed"])
        await asyncio.sleep(0.3)
    return {"streams_killed": killed}


def _window(summary: dict, key: str) -> tuple[int, int]:
    low, high = summary[key].split("->")
    return int(low), int(high)


# --- the scenarios ---------------------------------------------------------------------------


async def test_e2e_network_down_mid_stream(client, db, page, proxy, make_project, make_run):
    s = await run_scenario(client, db, page, proxy, make_project, make_run, seed=101, label="network-down", disrupt=network_down)
    page_from, page_to = _window(s, "window_page_seq")
    db_from, db_to = _window(s, "window_db_seq")
    # The disruption really happened: the page was cut off and knew it, while the database moved on.
    assert s["connections_cut"] >= 1
    assert page_to == page_from, "page kept receiving events while the network was down"
    assert db_to > db_from + 50, "ingestion did not continue during the window"
    assert s["window_statuses"] == "reconnecting"
    assert s["reconnects"] >= 1 and len(s["stream_opens"]) >= 2
    assert all(cursor > 0 for _, cursor in s["stream_opens"][1:])


async def test_e2e_silent_stall_mid_stream(client, db, page, proxy, make_project, make_run, stream_lifetime):
    await stream_lifetime(5)  # production default is 300 s; see the end of this test
    s = await run_scenario(client, db, page, proxy, make_project, make_run, seed=404, label="silent-stall", disrupt=silent_stall)
    page_from, page_to = _window(s, "window_page_seq")
    db_from, db_to = _window(s, "window_db_seq")
    assert s["connections_frozen"] >= 1
    assert page_to == page_from, "page kept receiving events through a frozen connection"
    assert db_to > db_from + 50, "ingestion did not continue during the window"
    # No error ever reached the page; it noticed the silence by itself (missed heartbeats).
    assert s["window_statuses"] == "connected/reconnecting", s["window_statuses"]
    assert s["reconnects"] >= 1 and len(s["stream_opens"]) >= 2
    assert all(cursor > 0 for _, cursor in s["stream_opens"][1:])
    # The server cannot notice the dead stream by itself at this data rate: its only signal is a
    # write that blocks for SSE_STALL_SECONDS, and a few hundred small events fit in the kernel
    # socket buffers, so writes keep "succeeding" (server_overflow_drops stays 0). Before the
    # maximum stream lifetime existed this test reported server_streams=1->2: the dead stream
    # stayed registered. Now it is ended when its lifetime runs out.
    streams_before, streams_after = _window(s, "server_streams")
    assert s["server_overflow_drops"] == 0
    assert s["server_recycled"] >= 1
    assert streams_after <= streams_before, "the dead stream is still registered on the server"


async def recycled_by_server(*, page, client, db, proxy, run_id):
    """Nothing is broken: the server itself ends every stream after 3 s (maximum lifetime)."""
    before = await debug_stats(client)
    await asyncio.sleep(9.0)
    after = await debug_stats(client)
    return {"server_recycled": after["sse_recycled_total"] - before["sse_recycled_total"]}


async def test_e2e_streams_ended_by_max_lifetime_are_invisible_in_the_data(client, db, page, proxy, make_project, make_run, stream_lifetime):
    await stream_lifetime(3)
    s = await run_scenario(client, db, page, proxy, make_project, make_run, seed=505, label="max-lifetime", disrupt=recycled_by_server)
    assert s["server_recycled"] >= 2
    assert s["reconnects"] >= 2 and len(s["stream_opens"]) >= 3
    assert all(cursor > 0 for _, cursor in s["stream_opens"][1:])


async def test_e2e_server_kills_stream_and_browser_resumes_with_last_event_id(client, db, page, proxy, make_project, make_run):
    s = await run_scenario(client, db, page, proxy, make_project, make_run, seed=202, label="kill", disrupt=kill_twice)
    assert s["streams_killed"] == [1, 1]
    assert s["reconnects"] >= 2
    # The browser's own EventSource retry carried Last-Event-ID, and the server resumed from it.
    resumed = s["stream_opens"][1:]
    assert len(resumed) >= 2
    assert all(source == "last-event-id" and cursor > 0 for source, cursor in resumed), s["stream_opens"]
    assert [c for _, c in resumed] == sorted(c for _, c in resumed)


async def test_e2e_stream_killed_after_token_expired_page_resumes_with_fresh_token(client, db, page, proxy, make_project, make_run):
    """After a kill the browser retries with the token it has; here that token has expired, so the
    retry is refused (401), EventSource gives up, and the page itself must get a new token and
    resume from its own last_seq."""

    async def short_lived_tokens(route):  # TEST_MODE hook: ask for 1 s tokens instead of 60 s
        await route.continue_(headers={**route.request.headers, "x-test-token-ttl": "1"})

    await page.route("**/stream-token", short_lived_tokens)
    s = await run_scenario(client, db, page, proxy, make_project, make_run, seed=303, label="expired-token", disrupt=kill_twice)
    assert s["streams_killed"] == [1, 1]
    assert s["reconnects"] >= 2
    resumed = s["stream_opens"][1:]
    assert len(resumed) >= 2
    assert all(source == "since_seq" and cursor > 0 for source, cursor in resumed), s["stream_opens"]


async def test_playwright_set_offline_does_not_interrupt_an_open_stream(client, page, make_project, make_run):
    """Not a test of the app: a record of why the scenarios above use a TCP proxy. SPEC §7.1
    suggested `context.set_offline(True)` as the way to drop the connection. In this Chromium it
    blocks new requests but leaves an already-open SSE response flowing, so a consistency test
    built on it would pass without ever having disconnected anything. If this test starts failing,
    set_offline has begun to cut open streams and could be used as one more scenario."""
    a = await make_project()
    run = await make_run(a)
    await ui_login(page, a.key)
    await page.locator("#runs-body a").click()
    await wait_js(page, "window.__state && window.__state.status === 'connected'")

    await page.context.set_offline(True)
    try:
        await send_batch(client, a, run["id"], make_points(20))
        await asyncio.sleep(2.0)
        state = await read_page(page)
        new_request_blocked = await page.evaluate("fetch('/healthz').then(() => false, () => true)")
    finally:
        await page.context.set_offline(False)
    print(f"\n[e2e] set_offline(True): new fetch blocked={new_request_blocked}; open stream still delivered "
          f"{state['last_seq']} of 20 points; page status={state['status']}")
    assert new_request_blocked is True
    assert state["last_seq"] == 20 and state["status"] == "connected"
