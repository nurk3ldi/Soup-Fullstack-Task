# CLAUDE.md — Mini Experiment Tracker (Soup take-home, Full-Stack)

**Read `SPEC.md` in full before writing code.** It is the source of truth; this file is a
summary plus the live project state. If they disagree, SPEC.md wins — and fix this file.

The user writes in Kazakh: reply in Kazakh. Code, comments, commit messages and the
deliverable docs (README, DESIGN, NOTE, AI_USAGE) are in English.

## What this is

A take-home assignment: a mini experiment tracker (projects + API keys, batched metric
ingestion over an unreliable network, live run page over SSE, run list). It must answer:
**does what the user sees always match what is stored, and do they see only what's theirs?**

Graders value **correctness, honesty and evidence** over features or UI polish.
Budget: ~6 h of work, ~20 h wall clock from 2026-11-10 evening. Prefer simple code.

## Non-negotiable rules (SPEC §9)

1. Never delete, skip or weaken a failing test to get a green suite. Report it instead.
2. Never claim a test passes without running it. Show real output after each step.
3. On a failing test: find and report the root cause **before** changing anything. Change a
   test only if the test itself is wrong, and explain why.
4. `TEST_MODE` hooks (fault injection, kill stream, `/debug/stats`) are off unless the env is set.
5. No secrets in logs. Structured JSON logs with request id and project id, never the key.
   `token=` is redacted from logged URLs.
6. Keep `AI_USAGE.md` updated with non-trivial suggestions: accepted / changed / rejected.
7. Do **not** write the final verdict or the "surprises / concerns" paragraph. Leave
   placeholders with measured numbers; the user writes the conclusions.

## Stack

Python 3.12 · FastAPI + uvicorn · PostgreSQL 16 via asyncpg (raw SQL, no ORM) · SSE with
Postgres LISTEN/NOTIFY as the fan-out bus · static HTML + vanilla JS (`EventSource`), no
build step · pytest, pytest-asyncio, httpx, Playwright (Python) · load test is a plain
async httpx script. Compose services: `db`, `api`, and `tests` (profile).

## Design invariants (do not break these)

- **Two orders, kept separate.** `step` = client logical order (charts sort by it).
  `seq` = server delivery order, gap-free and strictly increasing per run (SSE resume uses it).
- Dedup key is `(run_id, name, step)`, **first write wins** (`ON CONFLICT DO NOTHING`).
- Ingest is **one transaction**: lock run row `FOR UPDATE` → insert into `batches` ledger →
  insert points, only surviving rows consume a seq → update `runs.last_seq` → `NOTIFY` → commit.
  A batch is all-or-nothing. Same `batch_id` + same payload → `200 duplicate:true`;
  same `batch_id` + different payload → `409 batch_id_reused`.
- **DB is the source of truth; NOTIFY is only a wake-up**, never the payload. The stream
  registers its listener **before** the catch-up read, and re-reads `seq > cursor` on notify
  or every 2 s.
- Resume point: `Last-Event-ID` header, else `?since_seq=`, else 0.
- Slow client: bounded per-connection queue (1000) → `event: overflow` + close; client
  reconnects and catches up from DB. Count it (`sse_overflow_total`).
- **Every query filters by `project_id`** resolved from the API key. Never trust a
  project_id from URL or body.
- **Uniform not-found:** missing run and other project's run → identical
  `404 {"error":"not_found"}`, same code path, single query `WHERE id=$1 AND project_id=$2`.
  Never 403 for cross-project resources.
- 401 body is always `{"error":"unauthorized"}` (unknown, malformed or revoked key).
  Error bodies never echo IDs, key prefixes, SQL or stack traces.
- API keys stored as sha256 only. Stream token: HMAC, 60 s TTL, bound to
  project_id + run_id + key_id; revocation re-checked at connect and every 5 s.
- Responses carry `Cache-Control: no-store, private` and `Vary: Authorization`.
- Limits: ≤ 1000 points per batch, name ≤ 64 chars, finite floats only → else 422.
  Body > 2 MiB → 413. More than 100 open streams per project → 429. Over the rate limits → 429.
- 401 is the answer to every bad credential, except from an address that has used up its
  failure allowance: that address gets 429 for everything that needs a credential.
- The API container is unprivileged and its root filesystem is read-only: application code
  must never write to disk (use `/tmp`, a 16 MB tmpfs, if it ever has to).
- The API port is published on 127.0.0.1 only; do not change the default to 0.0.0.0.

## Commands

```bash
docker compose up --build                 # app at http://localhost:8000
docker compose run --rm tests             # full suite, inside docker
pytest -v 2>&1 | tee results/test_output.txt   # raw output, committed under results/
docker compose run --rm tests sh -c 'pytest -v -s 2>&1 | tee results/test_output.txt'
sh loadtest/run.sh                        # both load variants, ~3.5 min → results/load.txt
docker compose --profile tests up --build -d api api-test   # after changing app code
```

Update this section if the real commands end up different.

## Build order and current state

Update the checkboxes and the "State notes" below at the end of every working session.

- [x] 1. Schema + docker (`schema.sql`, `Dockerfile`, `docker-compose.yml`, `/healthz`)
- [x] 2. Auth / projects / keys / runs
- [x] 3. Ingest + tests 7.3 duplicates & partial writes
- [x] 4. SSE + catch-up + test 7.3 reconnect (then overflow, key rotation)
- [x] 5. UI (`/`, `/runs/{id}/view`, `window.__state`)
- [x] 6. Isolation tests (7.2)
- [x] 7. Consistency E2E (7.1, Playwright)
- [x] 8. Load test (7.4) → `results/load.txt`
- [x] 9. Docs: README, DESIGN.md (1 page), NOTE.md (≤2 pages), AI_USAGE.md (drafted; the user
      still writes the verdict and the surprises paragraph in NOTE.md)

### State notes

- 2026-11-10: Project started. `git init` done (branch `main`, nothing committed yet).
- 2026-11-10: Step 1 done and verified: `docker compose up --build -d` brings up `db` and
  `api` healthy, `/healthz` returns 200, five tables + indexes exist, and
  `docker compose run --rm tests pytest -v` → 2 passed (`tests/test_smoke.py`, raw output in
  `results/test_output.txt`). Headless Chromium launches inside the tests image.
- Images: `soup-api` 252 MB, `soup-tests` 2.05 GB. The tests image needs `libatomic1`
  (Playwright's node binary). One build stalled for 10+ min on a hung `pip` download; the
  Dockerfile now has separate layers and `pip --timeout 30 --retries 5`. If a build hangs
  again, stop it and rerun — do not wait.
- Docker access on this machine was granted with `setfacl` on `/var/run/docker.sock`; that
  is lost when the docker daemon restarts (group membership applies after re-login).
- Do not use `pkill -f` with a pattern that also appears in your own command line.
- Schema is applied by the API at startup (idempotent `IF NOT EXISTS`, under an advisory lock).
- The `db` port is not published: host port 5432 is already used by a local Postgres.
- 2026-11-10: Step 2 done and verified: 16 passed in docker (`tests/test_auth_runs.py` 14 +
  smoke 2). Code: `app/auth.py` (key → project, admin guard), `app/routes.py` (projects,
  keys, runs), `app/errors.py` (single `{"error": code}` shape; 422 never echoes input),
  `app/logging.py` (JSON logs, `token=` redaction), `RequestContextMiddleware` in
  `app/main.py` (request id, cache headers, one log line per request; pure ASGI so SSE
  passes through). Log scan after the suite: 193 lines, all JSON, 0 with a key or admin token.
- Conventions set in step 2: admin token goes in `Authorization: Bearer <ADMIN_TOKEN>`;
  keys look like `sk_<43 url-safe chars>`; malformed UUIDs in paths return the same 404 as
  missing ones (`routes.parse_id`); revoke is idempotent; uvicorn access log is disabled.
- Not covered yet: the middleware's 500 path; revoking a project's last active key is
  allowed and locks the project out (no recovery endpoint).
- Tests cannot read API logs yet. Step 6 (log redaction test) needs a way: e.g. a
  TEST_MODE-only ring buffer endpoint.
- 2026-11-10: Step 3 done and verified: 34 passed in docker, three runs in a row
  (`tests/test_ingest.py` 12, `tests/test_silent_failures.py` 6, plus earlier 16).
  `app/ingest.py` has `POST /runs/{id}/metrics` and `GET /runs/{id}/metrics`.
- TEST_MODE is solved with a second compose service `api-test` (profile `tests`,
  `TEST_MODE=1`, same image and DB, no host port). Tests use `API_URL` → `api-test` and
  `PLAIN_API_URL` → `api` to prove hooks are inert without TEST_MODE. After code changes
  rebuild both: `docker compose --profile tests up --build -d api api-test`.
- How ingest differs from the SPEC §4 wording (same guarantees, documented in AI_USAGE.md):
  the ledger is checked with a SELECT under the run lock and the ledger row is inserted last
  with the real `accepted`; new points are found with `NOT EXISTS` and numbered with
  `row_number()` in one statement, with no `ON CONFLICT` (a PK clash aborts loudly instead
  of leaving a seq gap). In-batch duplicate keys: first occurrence wins.
  NOTIFY is skipped when a batch adds 0 points.
- Fault injection: header `X-Test-Fault: mid-insert` (TEST_MODE only) inserts the first half
  of the batch, then raises `FaultInjected` → rollback → 500.
- Validation choices: points 1..1000 (empty batch is 422), step is a strict int 0..2^63-1,
  value must be a JSON number (no strings/bools), `ts` must carry a timezone, names reject
  control characters. No request body size limit yet.
- `GET /runs/{id}/metrics` returns `{points, last_seq, has_more}`; both reads share one
  REPEATABLE READ snapshot. `ingest.read_after()` and `ingest.point_out()` are meant to be
  reused by the SSE stream; NOTIFY channel is `run_events`, payload `<run_id>:<last_seq>`.
- 2026-11-10: Step 4 done and verified: 54 passed in docker (~41 s; one test waits 17 s for
  a heartbeat). `app/bus.py` (one LISTEN connection, auto-reconnect), `app/stream.py`
  (`POST /runs/{id}/stream-token`, `GET /runs/{id}/stream`, TEST_MODE-only `/debug/stats` and
  `POST /debug/kill-streams/{run_id}`), stream tokens in `app/auth.py`.
- **Stream design differs from SPEC §5 on purpose** (user has not confirmed yet — see
  AI_USAGE.md): there is no per-connection queue. The stream pulls one page (500 points)
  from the DB, writes it, and reads the next page only after the write was accepted. A
  client that cannot accept a write for `SSE_STALL_SECONDS` (default 10, `api-test` uses 2)
  is dropped and counted in `sse_overflow_total`; no `event: overflow` is sent because a
  stalled client cannot receive it. Dropping = returning without finishing the response, so
  uvicorn logs `ASGI callable returned without completing response` (expected on
  overflow and on the kill hook).
- Revocation is pushed: revoke does `pg_notify('key_revoked', key_id)` and the stream
  re-checks at once (~20 ms measured); the 5 s periodic check is the fallback (4.7 s measured
  with NOTIFY bypassed). On revoke the stream sends `event: revoked` and ends cleanly.
- Stream token: `project.run.key.exp.hmac` (hex), 60 s, checked only at connect. An open
  stream outlives its token; the key check is what keeps it honest. `STREAM_SECRET` env is
  optional; unset = random per process, so tokens from `api` do not work on `api-test`.
  The UI must mint a fresh token when a reconnect gets 401.
- SSE message: one per point, `id: <seq>`, `event: metric`,
  `data: {"name","step","value","seq","ts"}`. First line of a stream is `retry: 1000`.
- Test helpers in `tests/conftest.py`: `SSE`, `open_stream`, `stream_token`, `debug_stats`.
- Mutation checks done by hand (code broken on purpose, then restored): resume with
  `seq >=`, no NOTIFY, no key re-check, token not bound to run, no idempotency lookup —
  each made at least one test fail. Redo this kind of check when adding the E2E test.
- Known limits to report in NOTE.md: bus and stream registry are per process (one API
  instance only); a never-reading client's socket may linger after the drop until its
  kernel buffer drains or the peer goes away; heartbeat/poll intervals are not configurable
  per test, so the heartbeat test takes 17 s.
- 2026-11-10: Step 5 done and verified: 61 passed in docker (~53 s), incl. 7 browser tests
  in `tests/test_ui.py`. UI is `app/static/{index.html,run.html,app.js,style.css}`, served by
  `main.py` (`/`, `/runs/{id}/view`, `/static/*`) with a strict CSP (no inline, no eval).
  Screenshots: `results/screens/` (made by `python tests/_screens.py` in the tests container).
- **Playwright + CSP:** never use `page.wait_for_function` — it evals inside the page and
  the CSP blocks it. Use `wait_js(page, expr)` from conftest (polls via `page.evaluate`).
  Do not "fix" this with `bypass_csp=True`; the XSS test needs the CSP on.
- Run page contract for the E2E test: `window.__state` =
  `{run_id, status, last_seq, total, reconnects, points:[{name,step,value,seq}]}` sorted by
  (name, step); status ∈ connecting / connected / reconnecting / unauthorized / not_found;
  DOM rows are `#metrics section[data-metric] tbody tr[data-step]`; badge is
  `#status[data-status]`. Fixture `page`, helpers `ui_login`, `wait_js`.
- Reconnect logic in `app.js`: on error, if the browser is retrying itself (readyState
  CONNECTING) it is left alone (native retry sends Last-Event-ID); if EventSource is CLOSED
  (e.g. token expired → 401) or 8 s pass, the page mints a new token and reopens with
  `?since_seq=<lastSeq>`. `reconnects` counts connected → reconnecting transitions.
- UI limits to report: steps above 2^53 lose precision in JS; every point is a DOM row (no
  virtualization), so very large runs will be slow; no client-side detection of a half-open
  connection (EventSource hides heartbeats).
- 2026-11-11: Step 6 done and verified: 73 passed in docker (~63 s); `tests/test_isolation.py`
  has 12 tests covering every bullet of SPEC §7.2. Numbers for NOTE.md are printed by the
  tests with `[timing]`, `[logs]`, `[ids]` prefixes in `results/test_output.txt` (they vary
  per run; quote the committed file, do not retype from memory).
- Log hardening added in this step (`app/logging.py`): query values are masked except
  `since_seq` and `limit` (`safe_query`); every finished log line is also scrubbed of
  anything shaped like an API key or stream token. TEST_MODE-only `GET /debug/logs` returns
  the in-process copy of recent log lines (same formatter as stdout).
- Isolation mutation checks done by hand and restored: project filter removed from GET run,
  from ingest; raw query logged; 403 for foreign runs — each failed ≥1 isolation test.
- Timing result so far: foreign vs missing run differ by tens of µs in the median, far below
  the ~0.6 ms request-to-request noise; measured from inside the docker network only.
- 2026-11-11: Step 7 done and verified: 78 passed in docker (~108 s).
  `tests/test_consistency_e2e.py` has 4 scenarios (network-down, silent-stall, kill ×2 →
  Last-Event-ID, kill after token expiry → since_seq) plus one test recording a tool
  limitation. Each scenario prints an `[e2e]` summary line; quote those from
  `results/test_output.txt` in NOTE.md.
- **Finding 1 (tool):** Playwright `context.set_offline(True)` does NOT cut an open SSE
  response in this Chromium (new requests are blocked, the open stream keeps flowing). The
  SPEC's suggested method would have produced a test that never disconnects anything. The
  E2E browser therefore talks to the API through `tests/netproxy.py` (`FlakyProxy`: `cut()`,
  `freeze()`, modes pass / refuse / blackhole). Fixture `proxy`; `ui_login(..., base=proxy.url)`.
- **Finding 2 (app bug, fixed):** on a silently stalled connection the page stayed
  "connected" forever. Fix: heartbeats are now real SSE events (`event: ping`), the stream
  starts with `event: hello` `{"ping_seconds": N}`, and the page reconnects after 3 ping
  intervals of silence (min 3 s); `fetch` calls have timeouts. `api-test` runs with
  `SSE_PING_SECONDS=1` (production default 15 → detection after ~45 s).
- **Finding 3 (app limitation, NOT fixed — user to decide):** the server does not notice a
  silently dead client at low data rates: writes keep succeeding into kernel buffers, so the
  stall timeout never fires and the stream stays registered (`server_streams=1->2`,
  `server_overflow_drops=0` in the silent-stall summary). Candidate fixes: a maximum stream
  lifetime (client resumes transparently), or TCP keepalive / TCP_USER_TIMEOUT.
- `stream open` log line carries `cursor` and `resume_source` (start / since_seq /
  last-event-id); the E2E reads it via `/debug/logs` to prove which resume path ran.
- E2E mutation checks done by hand and restored: Last-Event-ID resume +3, page since_seq +1,
  table values rounded, heartbeat watchdog removed — each failed the relevant scenarios.
- Full suite duration is now ~1 m 50 s; the E2E file alone takes ~60 s.
- 2026-11-11: Step 8 done: `loadtest/load.py` + `loadtest/run.sh`; results in
  `results/load.txt` (1 worker, the default) and `results/load_workers4.txt` (experiment).
  Suite still 78 passed afterwards. **Quote numbers from those files; do not retype.**
- Load results in one line each (1 worker, everything on one 8-core machine):
  batch=10 (50 req/s): sustained, p99 ~11–14 ms, 0 errors, api ~24 % of a core.
  batch=1 (500 req/s target): NOT sustained, ~310–320 req/s achieved, api ~90 % of one core,
  backlog grows for as long as the load lasts; still 0 errors, 0 lost, 0 seq gaps.
  With 4 workers batch=1 reached ~414 req/s, still under 500.
- Bottlenecks found, in order: (1) the single Python API process (CPU); (2) Postgres
  commits — every ingest commit carries a NOTIFY, and such commits queue on a global lock
  (`Lock:object` on COMMIT seen in pg_stat_activity next to `IO:WALSync`). Not fixed; these
  are inputs for the user's verdict, which Claude must not write.
- More than one worker requires `STREAM_SECRET` (tokens are per-process otherwise). Scaling
  env vars on `api`: `WEB_CONCURRENCY`, `STREAM_SECRET`, `DB_POOL_MAX`. They must be
  exported for every compose command of a session: a later `docker compose run` without
  them silently recreates `api` with defaults (this invalidated two experiment runs before
  it was noticed; `run.sh` now prints the api config at the end too).
- Measurement traps hit in this step: httpx collapses as a load generator at 500 req/s (so
  ingest uses a raw per-run keep-alive connection); a 2 s python healthcheck showed up as
  10–40 % idle CPU in `docker stats` (healthchecks are now 30 s with a 1 s start interval,
  and CPU is taken from cgroup counters); connections opened in one burst all land on one
  uvicorn worker.
- **RESUME HERE (2026-11-11 afternoon).** Steps 1-9, the security review and rate limiting done, 106 tests passing. Left for the
  user: reviewing the
  "pending review" rows in AI_USAGE.md, and the final commit + push. The real deadline was
  never confirmed (estimate: 2026-11-11 16:13 +05).
- On resume, first check the stack: `docker compose --profile tests ps`. If docker says
  permission denied, the user must log out/in (they are in the `docker` group now) or re-run
  the `setfacl` command. Then run the suite once before changing anything.
- The user tried the UI by hand on 2026-11-11 (project "demo", run "demo-run"); it worked.
  They hit a 404 by using a new project's key on an older project's run — expected.
- 2026-11-11 (afternoon): hardening pass. New file `tests/test_hardening.py` (14 tests):
  auth-before-everything matrix, credentials only in the Authorization header, no CORS,
  36 malformed-input cases (never 5xx, error shape only), 10 MB body, bit-exact floats through
  DB / snapshot / stream, equal-but-differently-spelled retries, opaque metric names, UUID
  spellings, three race tests, snapshot consistency under writers, and SQL invariants over
  every run in the database (`[invariants]` line). Suite: 92 tests. New mutation checks were
  NOT done for this file (only the 401 test was seen failing against the unfixed app).
- Fixed a flaky test, not the app: `test_other_projects_run_page_shows_not_found_and_no_data`
  read the DOM right after `__state.status` flipped, but `app.js` renders up to 50 ms later
  (debounce). It now waits for `#status[data-status="not_found"]`. Failed 1 of 2 runs before,
  0 of 11 after. When reading text off the page, always wait on the DOM, not on `__state`.
- Recorded behaviour (tests assert it): a UTF-16 JSON body is parsed like UTF-8; `{uuid}`,
  `urn:uuid:`, upper-case and hyphen-less ids name the same run (`uuid.UUID()` accepts them)
  and still go through the project filter; a 10 MB body is parsed fully before the 422
  (~360 ms, there is still no body size limit).
- `results/test_output.txt` regenerated: 92 passed. `results/load.txt` NOT regenerated: an A/B
  load run on 2026-11-11 (without vs with the auth fix) gave batch=1 277 vs 281 req/s, both
  ~10 % below the committed 310; the machine was slower that day, the fix made no difference.
- `loadtest/run.sh` no longer hard-codes container names (they depend on the directory name;
  the old `soup-api-1` names stopped existing when the directory was renamed).
- The stack was down at the start of the day; `docker compose --profile tests up -d api api-test`
  worked without permission problems.

### Known failing tests / open problems

- No failing tests (106 passed, 2026-11-11; `results/test_output.txt` is from that run).
- Fixed 2026-11-11 with the user's approval: unauthenticated request + broken JSON body got
  422 instead of 401, because FastAPI decodes the body before dependencies run. Fix:
  `auth.AuthFirstRoute` (route class on all three routers) checks the admin token / key before
  the body is read and caches the principal on `request.state` (still one key query per
  request). New routers must use `route_class=AuthFirstRoute` or the bug comes back for them.
- Decided by the user on 2026-11-11: (1) keep the pull-based slow-client design and explain it
  in DESIGN.md; (2) zombie stream fixed with a maximum stream lifetime
  (`SSE_MAX_LIFETIME_SECONDS`, default 300, +-10 % jitter; TEST_MODE hook
  `POST /debug/stream-lifetime/{seconds}`, fixture `stream_lifetime`); (3) batch=1 at
  500 req/s stays as measured, for the verdict.
- 2026-11-11, security review (see SECURITY.md, raw output in `results/security/`). Fixed:
  API runs as uid 10001 on a read-only rootfs with all capabilities dropped (compose anchor
  `x-hardening`); port published on 127.0.0.1 (`API_BIND`); body limit 2 MiB → 413
  (`MAX_BODY_BYTES`, enforced in `RequestContextMiddleware.limited_receive`, only once the app
  reads the body, so auth still comes first); 100 open streams per project → 429
  (`SSE_MAX_STREAMS_PER_PROJECT`, `api-test` uses 10); security headers on every response,
  no `Server` header; dependencies pinned to exact versions; `client` field in request logs;
  startup warning while `ADMIN_TOKEN` is the default. Tests: `tests/test_security.py` (5).
- 2026-11-11, rate limiting added on the user's request: `app/ratelimit.py` (token buckets,
  per process). Per project `RATE_LIMIT_PROJECT_RPS` (600, burst 2x) → 429 `rate_limited`;
  per client address `RATE_LIMIT_AUTH_FAIL_BURST` (20) / `_RPS` (1) for failed credentials,
  after which every credentialed request from that address gets 429 unseen. Enforced in
  `auth.AuthFirstRoute` and in `stream.stream()`. **Off on `api-test`** (env set to 0) because
  the suite sends bursts and bad keys on purpose; tests switch it on with the fixture
  `rate_limits` (hook `POST /debug/rate-limits`). The plain `api` has it on: a test that sends
  more than ~20 bad credentials to `PLAIN_API_URL` will lock the tests container out for a
  while. Tests: `tests/test_ratelimit.py` (6).
- Still open (all listed in SECURITY.md): rate limits are per process only, no TLS, default credentials exist,
  keys never expire, limits are per process, `GET /runs` not paginated, 47 unfixable HIGH
  findings in the base image, revoking the last key locks a project out; no mutation checks
  for `test_hardening.py`, `test_security.py`, `test_ratelimit.py` and the lifetime tests; the verdict and
  surprises paragraph in NOTE.md were written by the user on 2026-11-11 (do not rewrite them).
- `results/load.txt` is from the final code (hardening + rate limits on): batch=10 sustained
  (p99 11.0 ms), batch=1 316.2 req/s, 0 errors. Five single runs of batch=1 over two days:
  309.8, 277, 281, 256.4, 316.2: the spread is the machine, not the code. Do not read a
  10 % difference between two single runs as a regression. `load_workers4.txt` is old code.
- 2026-11-11 (evening): UI restyle, CSS only (`app/static/style.css`); HTML, JS and test
  selectors untouched. Suite rerun on it: 106 passed; `results/test_output.txt` was kept at the
  committed run because NOTE.md quotes it. Screenshots regenerated. Keep the stylesheet free of
  `text-transform` (tests read text off the page) and of external assets (CSP).
- Project skills: 10 third-party skills in `.claude/skills/` (git-ignored; source and commit in
  AI_USAGE.md). They are generic checklists. Do not run their `scripts/` (not installed, not
  reviewed) and do not install the other 808.
- Scanners run through docker without mounting the docker socket: Trivy scans a
  `docker save` tarball (`--input`); commands are at the end of SECURITY.md. A gitleaks run
  with `-c /dev/null` has NO rules and always reports 0: use `.gitleaks.toml` or a config with
  `[extend] useDefault = true`.

## Deliverables checklist

Repo + `docker compose up` · tests + raw output in `results/` · `DESIGN.md` · `NOTE.md`
(parts 2–5, what the E2E test does NOT catch, timing numbers, measured revocation window) ·
surprises/concerns paragraph (user writes) · verdict SHIP / DON'T SHIP for 50 runs × 10
events/s (user writes) · `AI_USAGE.md`.
