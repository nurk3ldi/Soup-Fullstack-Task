"""Step 5: the two UI pages in a real (headless) browser."""
import uuid

from conftest import API_URL, bearer, db_points, make_points, send_batch, ui_login, wait_js


async def test_login_list_and_live_run_page(client, db, page, make_project, make_run):
    a = await make_project()
    run = await make_run(a, "train-ui")
    await send_batch(client, a, run["id"], make_points(15, "loss") + make_points(5, "acc"))

    # Wrong key first.
    await ui_login(page, "sk_definitely-wrong")
    await page.wait_for_selector("#login-error:not([hidden])")
    assert await page.inner_text("#login-error") == "That key was not accepted."
    assert await page.is_hidden("#runs-section")

    # Right key: the list shows the run with its counts.
    await page.fill("#key-input", a.key)
    await page.click("#login-form button")
    row = page.locator(f'#runs-body tr[data-run-id="{run["id"]}"]')
    await row.wait_for()
    assert await row.locator("td").all_inner_texts() == ["train-ui", await row.locator("td").nth(1).inner_text(), "20", "20"]
    assert await page.locator("#runs-body tr").count() == 1

    # Open the run page: catch-up.
    await row.locator("a").click()
    await wait_js(page, "window.__state && window.__state.last_seq === 20")
    await wait_js(page, "document.querySelectorAll('#metrics tbody tr').length === 20")
    assert await page.inner_text("#run-name") == "train-ui"
    assert await page.get_attribute("#status", "data-status") == "connected"
    assert await page.inner_text("#last-seq") == "20"
    assert await page.inner_text("#total") == "20"
    assert await page.locator("#metrics section").evaluate_all("els => els.map(e => e.dataset.metric)") == ["acc", "loss"]

    # Live: points sent after the page is open, deliberately out of step order.
    await send_batch(client, a, run["id"], [{"name": "loss", "step": s, "value": float(s)} for s in (40, 30, 17, 16)])
    await wait_js(page, "window.__state.last_seq === 24")
    await wait_js(page, "document.querySelectorAll('#metrics tbody tr').length === 24")

    state = await page.evaluate("window.__state")
    stored = await db_points(db, run["id"])
    assert sorted((p["name"], p["step"], p["value"], p["seq"]) for p in state["points"]) == sorted(stored)
    assert state["total"] == 24 and state["status"] == "connected"
    # The table is sorted by step, not by arrival.
    loss_steps = await page.locator('section[data-metric="loss"] tbody tr').evaluate_all("rows => rows.map(r => Number(r.dataset.step))")
    assert loss_steps == sorted(loss_steps) and len(loss_steps) == len(set(loss_steps)) == 19
    assert page.errors == []


async def test_names_from_other_users_are_rendered_as_text_never_as_html(client, page, make_project, make_run):
    a = await make_project()
    evil_run = '<img src=x onerror="window.__xss=1"><script>window.__xss=2</script>'
    evil_metric = '"><img src=x onerror=window.__xss=3><svg onload=window.__xss=4>'
    run = await make_run(a, evil_run)
    await send_batch(client, a, run["id"], [{"name": evil_metric, "step": 0, "value": 1.0}, {"name": evil_metric, "step": 1, "value": 2.0}])

    await ui_login(page, a.key)
    link = page.locator("#runs-body a")
    await link.wait_for()
    assert await link.inner_text() == evil_run
    await link.click()
    await wait_js(page, "window.__state && window.__state.last_seq === 2")
    await wait_js(page, "document.querySelectorAll('#metrics tbody tr').length === 2")

    assert await page.inner_text("#run-name") == evil_run
    assert await page.inner_text("#metrics h3") == evil_metric
    assert await page.evaluate("typeof window.__xss") == "undefined"
    assert await page.locator("img, script:not([src]), svg:not(.spark)").count() == 0
    assert page.errors == []


async def test_pages_are_static_and_carry_security_headers(client, make_project, make_run):
    a = await make_project()
    run = await make_run(a)
    existing = await client.get(f"/runs/{run['id']}/view")
    random_id = await client.get(f"/runs/{uuid.uuid4()}/view")
    index = await client.get("/")
    assert existing.status_code == random_id.status_code == index.status_code == 200
    # No lookup behind the page: an existing run and a made-up id give the very same bytes.
    assert existing.content == random_id.content
    assert run["id"].encode() not in existing.content
    for r in (existing, index):
        csp = r.headers["content-security-policy"]
        assert "default-src 'none'" in csp and "script-src 'self'" in csp and "unsafe-inline" not in csp
        assert r.headers["x-content-type-options"] == "nosniff"
        assert r.headers["cache-control"] == "no-store, private"
    assert (await client.get("/static/app.js")).status_code == 200
    r = await client.get("/static/../auth.py")
    assert r.status_code == 404


async def test_run_page_without_key_goes_to_login_and_back(client, page, make_project, make_run):
    a = await make_project()
    run = await make_run(a, "needs-login")
    await send_batch(client, a, run["id"], make_points(3))

    await page.goto(f"{API_URL}/runs/{run['id']}/view")
    await page.wait_for_url("**/?next=*")
    await page.fill("#key-input", a.key)
    await page.click("#login-form button")
    await page.wait_for_url(f"**/runs/{run['id']}/view")
    await wait_js(page, "window.__state && window.__state.last_seq === 3")


async def test_login_does_not_redirect_to_other_sites(page, make_project):
    a = await make_project()
    for bad in ("https://example.com/", "//example.com/runs/x/view", "/runs/../../static/app.js", "javascript:alert(1)"):
        await ui_login(page, a.key, path=f"/?next={bad}")
        await page.wait_for_selector("#runs-section:not([hidden])")
        assert page.url.startswith(API_URL + "/?next=")
        await page.click("#logout")


async def test_other_projects_run_page_shows_not_found_and_no_data(client, page, make_project, make_run):
    a, b = await make_project(), await make_project()
    rb = await make_run(b, "secret-run-b")
    await send_batch(client, b, rb["id"], make_points(5, "secret-metric"))

    await ui_login(page, a.key)
    await page.wait_for_selector("#runs-section:not([hidden])")
    await page.goto(f"{API_URL}/runs/{rb['id']}/view")
    await wait_js(page, "window.__state && window.__state.status === 'not_found'")
    state = await page.evaluate("window.__state")
    assert state["points"] == [] and state["last_seq"] == 0
    body = await page.inner_text("body")
    assert "secret-run-b" not in body and "secret-metric" not in body
    assert await page.inner_text("#message") == "Run not found."

    # Exactly the same page as for an id that does not exist at all.
    await page.goto(f"{API_URL}/runs/{uuid.uuid4()}/view")
    await wait_js(page, "window.__state && window.__state.status === 'not_found'")
    assert await page.inner_text("body") == body


async def test_open_run_page_stops_when_its_key_is_revoked(client, page, make_project, make_run):
    a = await make_project()
    run = await make_run(a)
    await send_batch(client, a, run["id"], make_points(4))
    key2 = (await client.post("/projects/me/keys", headers=a.headers)).json()

    await ui_login(page, a.key)
    await page.locator("#runs-body a").click()
    await wait_js(page, "window.__state && window.__state.last_seq === 4 && window.__state.status === 'connected'")

    assert (await client.post(f"/projects/me/keys/{a.key_id}/revoke", headers=bearer(key2["key"]))).status_code == 200
    await wait_js(page, "window.__state.status === 'unauthorized'", timeout=6)
    assert "revoked" in await page.inner_text("#message")

    # Data sent afterwards (with the new key) must not reach the page that holds the dead key.
    r = await client.post(f"/runs/{run['id']}/metrics", json={"batch_id": str(uuid.uuid4()), "points": make_points(3, start=50)}, headers=bearer(key2["key"]))
    assert r.status_code == 200
    await page.wait_for_timeout(2500)
    assert (await page.evaluate("window.__state"))["last_seq"] == 4
