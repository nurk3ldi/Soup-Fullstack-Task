"""Step 2: admin bootstrap, API keys, runs, uniform 401/404."""
import hashlib
import re
import uuid

from conftest import ADMIN_TOKEN, bearer

UNAUTHORIZED = b'{"error":"unauthorized"}'
NOT_FOUND = b'{"error":"not_found"}'
UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I)


# --- admin bootstrap -------------------------------------------------------------------------


async def test_create_project_requires_admin_token(client, db):
    before = await db.fetchval("SELECT count(*) FROM projects")
    for headers in ({}, bearer("wrong-token"), {"Authorization": "Bearer"}, {"Authorization": ADMIN_TOKEN}):
        r = await client.post("/projects", json={"name": "x"}, headers=headers)
        assert r.status_code == 401
        assert r.content == UNAUTHORIZED
    assert await db.fetchval("SELECT count(*) FROM projects") == before


async def test_api_key_cannot_create_project(client, make_project):
    a = await make_project()
    r = await client.post("/projects", json={"name": "x"}, headers=a.headers)
    assert r.status_code == 401
    assert r.content == UNAUTHORIZED


async def test_create_project_returns_key_and_stores_only_hash(make_project, db):
    a = await make_project()
    row = await db.fetchrow("SELECT key_hash, prefix, project_id, revoked_at FROM api_keys WHERE id = $1", uuid.UUID(a.key_id))
    assert row["key_hash"] == hashlib.sha256(a.key.encode()).digest()
    assert row["prefix"] == a.key[:8] == a.prefix
    assert str(row["project_id"]) == a.id
    assert row["revoked_at"] is None
    # The plaintext key is in no text/bytea column of api_keys.
    dump = await db.fetchval("SELECT string_agg(k::text, ' ') FROM api_keys k")
    assert a.key not in dump


# --- 401 is uniform --------------------------------------------------------------------------


async def test_unauthorized_is_identical_for_every_bad_key(client, make_project):
    a = await make_project()
    r = await client.post("/projects/me/keys", headers=a.headers)
    assert r.status_code == 201
    revoked = r.json()
    r = await client.post(f"/projects/me/keys/{revoked['id']}/revoke", headers=a.headers)
    assert r.status_code == 200

    bad = {
        "missing": {},
        "malformed scheme": {"Authorization": a.key},
        "basic scheme": {"Authorization": f"Basic {a.key}"},
        "empty bearer": {"Authorization": "Bearer"},  # "Bearer " with a trailing space is rejected by httpx itself
        "unknown key": bearer("sk_" + "x" * 43),
        "truncated key": bearer(a.key[:-1]),
        "revoked key": bearer(revoked["key"]),
        "admin token": bearer(ADMIN_TOKEN),
    }
    for label, headers in bad.items():
        for method, path in (("GET", "/runs"), ("POST", "/runs"), ("GET", f"/runs/{uuid.uuid4()}")):
            r = await client.request(method, path, headers=headers, json={"name": "x"} if method == "POST" else None)
            assert r.status_code == 401, (label, method, path)
            assert r.content == UNAUTHORIZED, (label, method, path)


# --- runs ------------------------------------------------------------------------------------


async def test_run_create_get_list(client, make_project):
    a = await make_project()
    r = await client.post("/runs", json={"name": "  train-1  "}, headers=a.headers)
    assert r.status_code == 201
    run = r.json()
    assert run["name"] == "train-1"
    assert run["last_seq"] == 0 and run["points"] == 0
    assert uuid.UUID(run["id"]).version == 4

    r = await client.get(f"/runs/{run['id']}", headers=a.headers)
    assert r.status_code == 200
    assert r.json() == run

    r = await client.get("/runs", headers=a.headers)
    assert r.status_code == 200
    assert r.json() == {"runs": [run]}


async def test_run_list_contains_only_own_runs(client, make_project, make_run):
    a, b = await make_project(), await make_project()
    ra1, ra2 = await make_run(a, "a1"), await make_run(a, "a2")
    rb = await make_run(b, "b1")

    ids_a = {r["id"] for r in (await client.get("/runs", headers=a.headers)).json()["runs"]}
    ids_b = {r["id"] for r in (await client.get("/runs", headers=b.headers)).json()["runs"]}
    assert ids_a == {ra1["id"], ra2["id"]}
    assert ids_b == {rb["id"]}


async def test_other_projects_run_is_indistinguishable_from_missing(client, make_project, make_run):
    a, b = await make_project(), await make_project()
    rb = await make_run(b)

    other = await client.get(f"/runs/{rb['id']}", headers=a.headers)
    missing = await client.get(f"/runs/{uuid.uuid4()}", headers=a.headers)
    malformed = await client.get("/runs/not-a-uuid", headers=a.headers)
    for r in (other, missing, malformed):
        assert r.status_code == 404
        assert r.content == NOT_FOUND
    assert other.headers["content-type"] == missing.headers["content-type"]
    # Sanity: the run really exists for its owner.
    assert (await client.get(f"/runs/{rb['id']}", headers=b.headers)).status_code == 200


async def test_run_name_validation_does_not_echo_input(client, make_project):
    a = await make_project()
    marker = "ECHO-" + uuid.uuid4().hex
    cases = [{"name": ""}, {"name": "   "}, {"name": marker * 20}, {"nam": marker}, {"name": 5}]
    for body in cases:
        r = await client.post("/runs", json=body, headers=a.headers)
        assert r.status_code == 422, body
        assert r.content == b'{"error":"invalid_request"}', body
    r = await client.post("/runs", content=b'{"name": "' + marker.encode(), headers={**a.headers, "Content-Type": "application/json"})
    assert r.status_code == 422
    assert marker.encode() not in r.content
    assert (await client.get("/runs", headers=a.headers)).json() == {"runs": []}


# --- key rotation ----------------------------------------------------------------------------


async def test_rotation_new_key_works_old_key_dies_immediately(client, make_project, make_run):
    a = await make_project()
    run = await make_run(a)

    r = await client.post("/projects/me/keys", headers=a.headers)
    assert r.status_code == 201
    key2 = r.json()
    assert key2["key"] != a.key and key2["key"].startswith(key2["prefix"])

    r = await client.post(f"/projects/me/keys/{a.key_id}/revoke", headers=bearer(key2["key"]))
    assert r.status_code == 200
    assert r.json() == {"id": a.key_id, "revoked": True}

    # Very next call with the old key.
    r = await client.get(f"/runs/{run['id']}", headers=a.headers)
    assert r.status_code == 401
    assert r.content == UNAUTHORIZED
    # Same project, new key still sees the run.
    r = await client.get(f"/runs/{run['id']}", headers=bearer(key2["key"]))
    assert r.status_code == 200


async def test_revoke_is_idempotent_and_keeps_first_timestamp(client, make_project, db):
    a = await make_project()
    key2 = (await client.post("/projects/me/keys", headers=a.headers)).json()
    url = f"/projects/me/keys/{key2['id']}/revoke"
    assert (await client.post(url, headers=a.headers)).status_code == 200
    first = await db.fetchval("SELECT revoked_at FROM api_keys WHERE id = $1", uuid.UUID(key2["id"]))
    assert first is not None
    assert (await client.post(url, headers=a.headers)).status_code == 200
    assert await db.fetchval("SELECT revoked_at FROM api_keys WHERE id = $1", uuid.UUID(key2["id"])) == first


async def test_cannot_revoke_another_projects_key(client, make_project, db):
    a, b = await make_project(), await make_project()
    other = await client.post(f"/projects/me/keys/{b.key_id}/revoke", headers=a.headers)
    missing = await client.post(f"/projects/me/keys/{uuid.uuid4()}/revoke", headers=a.headers)
    malformed = await client.post("/projects/me/keys/not-a-uuid/revoke", headers=a.headers)
    for r in (other, missing, malformed):
        assert r.status_code == 404
        assert r.content == NOT_FOUND
    assert await db.fetchval("SELECT revoked_at FROM api_keys WHERE id = $1", uuid.UUID(b.key_id)) is None
    assert (await client.get("/runs", headers=b.headers)).status_code == 200


async def test_new_key_belongs_to_callers_project(client, make_project, db):
    a, b = await make_project(), await make_project()
    key2 = (await client.post("/projects/me/keys", headers=a.headers)).json()
    assert str(await db.fetchval("SELECT project_id FROM api_keys WHERE id = $1", uuid.UUID(key2["id"]))) == a.id
    # A project_id smuggled in the body is ignored, not trusted.
    r = await client.post("/runs", json={"name": "smuggle", "project_id": b.id}, headers=a.headers)
    assert r.status_code == 201
    assert str(await db.fetchval("SELECT project_id FROM runs WHERE id = $1", uuid.UUID(r.json()["id"]))) == a.id
    assert (await client.get("/runs", headers=b.headers)).json() == {"runs": []}


# --- headers and error bodies ----------------------------------------------------------------


async def test_cache_headers_on_success_and_errors(client, make_project, make_run):
    a = await make_project()
    run = await make_run(a)
    responses = [
        await client.get("/runs", headers=a.headers),
        await client.get(f"/runs/{run['id']}", headers=a.headers),
        await client.get(f"/runs/{uuid.uuid4()}", headers=a.headers),
        await client.get("/runs"),
        await client.get("/no-such-route"),
    ]
    assert [r.status_code for r in responses] == [200, 200, 404, 401, 404]
    for r in responses:
        assert r.headers["cache-control"] == "no-store, private"
        assert "authorization" in r.headers["vary"].lower()
        assert re.fullmatch(r"[0-9a-f]{32}", r.headers["x-request-id"])


async def test_error_bodies_carry_no_ids_or_prefixes(client, make_project, make_run):
    a, b = await make_project(), await make_project()
    rb = await make_run(b)
    responses = [
        await client.get(f"/runs/{rb['id']}", headers=a.headers),
        await client.get(f"/runs/{rb['id']}", headers=bearer(a.key[:-1])),
        await client.post(f"/projects/me/keys/{b.key_id}/revoke", headers=a.headers),
        await client.get(f"/no-such-route/{rb['id']}"),
        await client.put(f"/runs/{rb['id']}", headers=a.headers),
    ]
    assert [r.status_code for r in responses] == [404, 401, 404, 404, 405]
    for r in responses:
        assert not UUID_RE.search(r.text), r.text
        assert a.prefix not in r.text and b.prefix not in r.text
        assert set(r.json().keys()) == {"error"}
