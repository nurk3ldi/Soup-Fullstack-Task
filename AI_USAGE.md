# AI usage notes

Tool: Claude Code. Running list of non-trivial suggestions and what happened to them.
Status is one of: accepted / changed / rejected / pending review.

## Step 1 — schema + docker

| Suggestion by AI | Status | Verified how |
|---|---|---|
| One `Dockerfile` with two targets: `app` (small) and `tests` (adds pytest + Playwright Chromium), so the app image does not carry a browser. | pending review | both images build; app 252 MB, tests 2.05 GB |
| Apply `schema.sql` from the API at startup with `IF NOT EXISTS` under `pg_advisory_xact_lock`, instead of `docker-entrypoint-initdb.d` (which only runs on an empty volume). | pending review | `\dt` shows 5 tables after `compose up` |
| Do not publish the Postgres port to the host (host 5432 was already taken; only containers need the DB). | pending review | `docker compose config` |
| Added index `runs_project_created (project_id, created_at DESC)` for the run list; not in the original spec schema. | pending review | `\di` shows it; not yet used by a query |
| `/healthz` pings the DB (`SELECT 1`) and returns 503 `{"error":"unavailable"}` on failure, rather than a pure process-liveness check. | pending review | 200 path checked with curl; 503 path not tested |
| Smoke test asserting the five tables, the `metrics` and `batches` primary keys and the `metrics_run_seq` index exist. | pending review | 2 passed in docker, see `results/test_output.txt` |
| First `tests` image build failed (`libatomic.so.1` missing for Playwright's node). AI diagnosed from the build error and added `libatomic1`. | pending review | image builds; Chromium launches and loads `/healthz` |
| A build hung 10+ min inside `pip install`. AI checked host network (fast) and process state (idle), killed the build, split the `RUN` into layers and added `pip --timeout 30 --retries 5`. | pending review | rebuild finished in ~5 min |

## Step 2 — auth, projects, keys, runs

| Suggestion by AI | Status | Verified how |
|---|---|---|
| Missing, malformed, unknown and revoked keys all go through the same path (one sha256, one query) and return byte-identical 401. | pending review | `test_unauthorized_is_identical_for_every_bad_key` (8 kinds of bad key × 3 routes) |
| Run id is taken as a string and parsed by hand, so a malformed UUID returns the same 404 as a missing run instead of FastAPI's 422 that echoes the value. | pending review | `test_other_projects_run_is_indistinguishable_from_missing` |
| Replaced FastAPI's default 422 body (which echoes input) with `{"error":"invalid_request"}`. Trade-off: the client is not told which field is wrong. | pending review | `test_run_name_validation_does_not_echo_input` |
| Request logging as pure ASGI middleware instead of `BaseHTTPMiddleware`, to avoid buffering/disconnect problems with SSE later. | pending review | works for normal requests; SSE not built yet |
| Disabled uvicorn's access log (it prints full URLs incl. `?token=`); the app writes its own redacted JSON line. | pending review | log scan: 193 lines, all JSON, none contains a key or the admin token |
| Admin token sent as `Authorization: Bearer`, compared with `hmac.compare_digest`; no `ADMIN_TOKEN` set means nobody can create projects. | pending review | `test_create_project_requires_admin_token`; the unset-env case is not tested |
| Revoke made idempotent (`COALESCE(revoked_at, now())`). | pending review | `test_revoke_is_idempotent_and_keeps_first_timestamp` |
| OpenAPI/docs endpoints turned off (they are unauthenticated and not needed). | pending review | not tested |
| Two tests failed on first run: `bearer("")` builds `Bearer ` (trailing space), which httpx refuses to send. AI judged the test wrong, not the server, and changed the case to a bare `Bearer` header. | pending review | rerun: 16 passed |

## Step 3 — ingest

| Suggestion by AI | Status | Verified how |
|---|---|---|
| Second compose service `api-test` with `TEST_MODE=1` (tests profile) instead of toggling TEST_MODE on the main `api`; tests also hit the plain `api` to prove the hooks do nothing there. | pending review | `test_fault_header_is_ignored_without_test_mode`; `FaultInjected` appears only in the `api-test` log |
| Deviation from SPEC §4 step 2–3: idempotency ledger checked by SELECT under the run row lock (not `INSERT ... ON CONFLICT`), ledger row written last with the real `accepted`. | pending review | `test_same_batch_five_times_concurrently_is_stored_once`: 1 fresh + 4 `duplicate:true`, one ledger row |
| Deviation from SPEC §4 step 3: new points selected with `NOT EXISTS` and numbered with `row_number()` in a single statement, without `ON CONFLICT DO NOTHING`. Argument: every writer of a run holds the run row lock, so the check cannot race; if that is ever wrong the batch fails loudly rather than leaving a seq gap. | pending review | `test_concurrent_batches_to_one_run_get_unique_gap_free_seq` (30 concurrent batches → seq exactly 1..300); DB-wide check: 0 runs where last_seq ≠ count ≠ max(seq). The "fails loudly" branch itself is not exercised |
| Duplicate `(name, step)` inside one batch: first occurrence in request order wins (SPEC does not say). | pending review | `test_duplicate_key_inside_one_batch_keeps_first_occurrence` |
| `payload_sha` is computed over parsed points (float hex, UTC ts), not raw bytes, so a re-serialized retry is still "the same batch". Reordered points count as a different body → 409. | pending review | 409 cases tested; the re-serialized-retry case is not tested |
| Strict value/step types (no `"0.5"`, no booleans, no `1.5` steps), `ts` must have a timezone, empty batch rejected. | pending review | `test_invalid_batches_are_rejected_and_write_nothing` (22 bad bodies + boundary batch of exactly 1000) |
| Fault injection as a per-request header (`X-Test-Fault: mid-insert`) rather than a global flag, so tests cannot affect each other. Inserts half the points, then raises. | pending review | `test_failure_mid_transaction_leaves_nothing_and_retry_succeeds` |
| Snapshot read uses one REPEATABLE READ transaction so `last_seq` and the returned points agree. | pending review | paging tested; consistency under concurrent writes not tested |
| One test failed on first run: the middleware's 500 body was `{"error": "internal"}` (with a space), unlike every other error body. AI judged the server wrong and fixed the server; test unchanged. | pending review | rerun ×3: 34 passed |
| While writing this step AI noticed run names containing NUL would cause a 500 (Postgres rejects NUL in text) and added a no-control-characters pattern to run names. | pending review | covered for point names; **not** covered by a test for run names |

## Step 4 — SSE stream

| Suggestion by AI | Status | Verified how |
|---|---|---|
| **Deviation from SPEC §5 (slow client):** no per-connection queue of 1000 messages. The DB is the queue: the stream reads a page of ≤500 points, writes it, and reads the next page only after that write was accepted, so memory per connection is one page by construction. "Overflow" = a write not accepted for `SSE_STALL_SECONDS` → connection dropped, `sse_overflow_total` incremented, warning logged. Reason: with a push queue a healthy client catching up on a big run would overflow falsely; with pull there is nothing to overflow, only stalled clients to evict. | pending review | `test_stalled_client_is_dropped_and_catches_up_on_reconnect`: 20 000 points, overflow 0→1 after 2.3 s (timeout 2 s in test mode), `max_buffered_points`=500, RSS 59.5→60.7 MB, reconnect receives all 20 000 |
| **Deviation:** no `event: overflow` is sent — a client that is not reading cannot receive it; the connection is cut instead (the SPEC test allows "or connection is dropped"). | pending review | same test |
| **Addition:** revocation is pushed with `pg_notify('key_revoked', …)` so open streams close in milliseconds; the SPEC's 5 s periodic check is kept as the fallback. | pending review | pushed: stream closed +19.8 ms / +22.4 ms after revoke; fallback (revoked by direct SQL, no NOTIFY): 4.70 s |
| Custom ASGI response class instead of `StreamingResponse`, because each write needs a timeout. Dropping a connection = returning without completing the response. | pending review | kill-hook test sees the stream end as "dropped"; side effect: uvicorn logs an ERROR line for each drop |
| Stream token is checked only at connect; an open stream may outlive the 60 s token and is policed by the key check instead. | pending review | expiry at connect tested with a 1 s TTL test hook; "stream stays open past 60 s" is **not** tested |
| All stream auth failures (missing/forged/expired token, wrong run, malformed run id, revoked key) return the same 401, not 404. | pending review | `test_stream_rejects_bad_tokens_identically`, `test_token_for_one_run_does_not_open_another` |
| Safety-poll test and revocation-fallback test write to the DB directly (bypassing the API) to simulate a lost NOTIFY, instead of adding more test hooks. | pending review | delivered after 2.00 s; closed after 4.70 s |
| Mutation checks: AI broke the code five ways (`seq >=` on resume, no NOTIFY, no key re-check, token not bound to run, no idempotency lookup), ran the suite, restored the files (verified byte-identical). | pending review | each mutation failed ≥1 test; output not saved in `results/` |
| All 52 stream-step tests passed on the first run — no fixes were needed in this step. | — | 54 passed ×3 after adding the two fallback tests |

## Step 5 — UI

| Suggestion by AI | Status | Verified how |
|---|---|---|
| Pages are static files with no server-side data; `/runs/{id}/view` returns the same bytes for any id (no lookup), so the page itself cannot leak whether a run exists. | pending review | `test_pages_are_static_and_carry_security_headers` |
| Strict Content-Security-Policy on the pages (`default-src 'none'; script-src 'self'; …`), and all server-provided text placed with `textContent` only. Not in the SPEC; added because run and metric names are attacker-controlled text shown to other key holders of the project. | pending review | `test_names_from_other_users_are_rendered_as_text_never_as_html` (run and metric names containing `<script>`/`onerror` payloads) |
| Hybrid reconnect: leave the browser's native EventSource retry alone (it sends Last-Event-ID); take over with a fresh token + `since_seq` when it gives up (expired token → 401) or after 8 s. | pending review | revoke and catch-up paths tested here; drop/reconnect paths are **not** tested until the E2E step |
| After login, `?next=` only accepts `/runs/<uuid>/view` (no open redirect). | pending review | `test_login_does_not_redirect_to_other_sites` |
| Added a "Create run" form to the list page (not required by the SPEC). | pending review | used manually via screenshots script only; no test |
| Three UI tests failed on first run: Playwright's `wait_for_function` evals inside the page and the app's CSP blocked it. AI judged the app right and the test helper wrong; replaced it with `wait_js` (polls through `page.evaluate`) instead of disabling the CSP in tests. | pending review | rerun: 7 passed; full suite 61 passed |
| AI looked at screenshots (desktop light, mobile dark) and fixed one layout wrap on the run list form. | pending review | `results/screens/*.png` |

## Step 6 — isolation tests

| Suggestion by AI | Status | Verified how |
|---|---|---|
| One matrix test: for each run-scoped endpoint, A's key on B's run vs A's key on a random UUID must match in status, body bytes **and headers** (minus date/request id). | pending review | `test_every_endpoint_treats_foreign_run_exactly_like_a_missing_one` |
| Beyond the SPEC list: batch_id reuse across projects is not an oracle; A's open stream receives none of B's concurrent writes; forged tokens built from B's real ids with A's signature; project_id smuggled in body/query/header. | pending review | 4 tests in `test_isolation.py` |
| Logs are checked through a TEST_MODE-only in-process buffer (`/debug/logs`) because the test container cannot read the API container's stdout. Weakness: it is a copy, not stdout itself. AI also grepped the real `docker compose logs` by hand after the suite. | pending review | buffer: 0 lines with any of 6 secrets; real stdout of both containers: 2017 lines, 0 key-shaped, 0 token-shaped, 0 admin token |
| While writing the log test AI found a real gap: a key or token put in the URL by mistake (`?api_key=…`, or in the path) would have been logged. Fixed: query values masked except `since_seq`/`limit`, plus a final scrub of key- and token-shaped strings on every log line. | pending review | `test_logs_contain_neither_api_keys_nor_stream_tokens` sends secrets in query and path; mutation "log raw query" fails it |
| Timing is measured interleaved (foreign, missing, own in alternating order) and only loosely asserted (<5 ms); the result is the printed numbers. | pending review | see `[timing]` lines in `results/test_output.txt` |
| Mutation checks: removed the project filter from GET run and from ingest, logged the raw query, returned 403 for foreign runs; files restored and verified identical. | pending review | 5 / 2 / 1 / 2 isolation tests failed respectively; output not saved in `results/` |
| All 12 isolation tests passed on first run; no application fix was needed apart from the logging gap above. | — | full suite 73 passed |

## Step 7 — consistency E2E

| Suggestion by AI | Status | Verified how |
|---|---|---|
| Each scenario asserts that its disruption really happened (page frozen and aware of it while the DB advanced; streams actually killed; which resume path the server saw), not only that the end state matches. | pending review | this is what exposed the `set_offline` problem below |
| **SPEC §7.1 method did not work:** `context.set_offline(True)` leaves an open SSE response flowing in Chromium. First run: page advanced 183→370 while "offline", `reconnects=0`. AI replaced it with a TCP proxy written for the tests (`tests/netproxy.py`) and kept a small test that records the `set_offline` behaviour. | pending review | `test_playwright_set_offline_does_not_interrupt_an_open_stream`: "new fetch blocked=True; open stream still delivered 20 of 20 points" |
| Added a fourth scenario not in the SPEC: silent stall (connection open, nothing flows, no error). It exposed a real bug: the page showed "connected" forever. | pending review | mutation E4 (watchdog removed) fails the scenario |
| Fix for that bug: heartbeats as real SSE events + `hello` announcing the interval; page reconnects after 3 silent intervals; timeouts on `fetch`. Changes the stream format from step 4 (`: ping` comment → `event: ping`). | pending review | silent-stall scenario: status `connected/reconnecting`, recovered 3.7 s after the network came back, 500/500 points |
| AI's first version of the silent-stall test asserted that the server drops the stalled stream. That was a wrong assumption (not an app regression): at this data rate writes never block, so the server never notices. AI changed the assertion into reported observations and documented the limitation instead of hiding it. | pending review | summary line: `server_streams=1->2 server_overflow_drops=0`, same on 3 runs |
| Overlap batches carry different values for keys other batches also carry; the test does not predict the winner, it requires page == DB and exactly 500 rows. | pending review | all scenarios: `accepted_total=500 db_rows=500 page_rows=500 dom_rows=500 api_rows=500` |
| Four views compared, not two: `window.__state`, the rendered DOM table (text parsed back to floats), the API snapshot, and the DB. | pending review | mutation E3 (values rounded in the table only) fails all 4 scenarios |
| Mutation checks: Last-Event-ID resume skips 3; page resumes one event too far; table rounds values; watchdog removed. Files restored and verified identical. | pending review | 2 / 4 / 4 / 1 scenarios failed respectively; output not saved in `results/` |

## Step 8 — load test

| Suggestion by AI | Status | Verified how |
|---|---|---|
| Open-loop generator (batches sent on schedule regardless of responses) reporting both service time and latency from the scheduled time, so a saturated server cannot hide behind a slowed-down client. | pending review | batch=1: service p50 ~144 ms vs from-schedule p50 ~18.6 s — the second number is what shows the overload |
| **Deviation from SPEC §7.4 ("httpx"):** ingest requests use a hand-written keep-alive HTTP/1.1 client, one connection per run. With httpx the *generator* collapsed at 500 req/s (≈150 ms of CPU per request in its connection pool, shown by asyncio debug mode) while the API was idle; the first batch=1 run hung for 8 minutes because of it. httpx is still used for setup and SSE. | pending review | generator lateness p99 < 2.5 ms in all runs |
| Point value = send timestamp, so SSE lag is computed in one process with no clock skew and no timestamp parsing. | pending review | — |
| CPU measured from cgroup counters over the whole run instead of trusting `docker stats` snapshots; healthchecks slowed from 2 s to 30 s after they were found to cost 10–40 % CPU on an idle container. | pending review | idle api now shows 0.16 % in the snapshots |
| AI's first hypothesis for the batch=1 ceiling was Postgres NOTIFY commit serialization. Sampling `pg_stat_activity` showed it is real (`Lock:object` on COMMIT) but not the first limit: Postgres was mostly waiting for the client, and the API process sat at ~90 % of one core. | pending review | wait-event sample taken by hand; not saved in `results/` |
| Four-worker experiment to give the verdict a data point. **Two attempts were invalid and AI reported them as such:** `docker compose run` inside `run.sh` recreated the api with default settings, so the "4 workers" runs were 1 worker (caught by listing processes in the container, after the numbers looked identical to 1 worker). The wrong result file was deleted and the run repeated with the variables exported. | pending review | `results/load_workers4.txt`: per-process CPU seconds 36/35/42/61, 413.9 req/s; process list checked by hand afterwards |
| No performance fix was applied. The findings (single process CPU, then NOTIFY-in-transaction commits) are left for the verdict. | — | — |

## Hardening pass (2026-11-11)

| Suggestion by AI | Status | Verified how |
|---|---|---|
| New file `tests/test_hardening.py` (14 tests): auth-before-everything matrix, 36 malformed inputs, 10 MB body, bit-exact floats through DB / snapshot / stream, three race tests, snapshot consistency under writers, SQL invariants over every run in the database. | pending review | 92 passed twice in a row; raw output in `results/test_output.txt` (`[malformed]`, `[body]`, `[snapshot]`, `[invariants]` lines) |
| **Bug found by the new test, fixed on the user's approval:** FastAPI decodes the JSON body before it runs dependencies, so a caller with no key and a broken body got 422 instead of 401. Fix: `auth.AuthFirstRoute` runs the admin/key check before the body is read; the principal is cached on `request.state` so there is still one key query per request. | accepted | test failed in 5 full runs before the fix and passes after; load test A/B on the same day, without vs with the fix: batch=1 277.4 vs 280.5 req/s, batch=10 p99 14.5 vs 15.6 ms (one run each, outputs not saved in `results/`) |
| **AI's own expectation was wrong:** the first version of the malformed-input test required a UTF-16 JSON body to be rejected. Python's JSON parser detects UTF-16, the body was ingested (200). The test was changed, with the reason in a comment: such a body must mean the same as its UTF-8 twin. | changed | `test_retry_that_differs_only_in_spelling_is_still_the_same_batch` |
| **Flaky test fixed (test, not app):** `test_other_projects_run_page_shows_not_found_and_no_data` read the DOM right after `window.__state.status` changed, but the page renders up to 50 ms later (debounce). It now waits for the badge. The assertion itself is unchanged. | pending review | failed 1 of 2 full runs before; 0 of 8 UI-file runs and 0 of 5 full runs after |
| `loadtest/run.sh` had container names hard-coded (`soup-api-1`), which only exist if the directory is called `soup`. It now looks the containers up with `docker compose ps -q`. | pending review | script ran end to end in a directory with another name |
| Not done: mutation checks for the new test file (except the 401 test, which was seen failing against the unfixed app). `results/load.txt` was not regenerated: today's runs were ~10 % slower than the committed ones with and without the fix, so the difference is the machine, not the code. | — | — |

## Open decisions closed, and docs (2026-11-11)

| Suggestion by AI | Status | Verified how |
|---|---|---|
| Keep the pull-based slow-client design instead of the spec's 1000-message queue with `event: overflow`; explain the difference in DESIGN.md. | accepted | existing stalled-client test: dropped after 2.3 s, at most 500 points buffered |
| Maximum stream lifetime (`SSE_MAX_LIFETIME_SECONDS`, default 300, +-10 % jitter) so that a dead client the server cannot detect is released; live clients resume from their cursor. | accepted | silent-stall E2E: `server_streams=1->1` (was `1->2`); never-reading client with 3 points released after 2.1 s with 0 overflow drops; new `max-lifetime` E2E scenario: 3 reconnects, 500 = 500 = 500 = 500 |
| Leave batch=1 at 500 req/s unfixed and report it as measured. | accepted | `results/load.txt` |
| README.md, DESIGN.md and NOTE.md drafted by AI from the measured output. Verdict and the surprises paragraph are left as TODO for the author. | pending review | numbers copied from `results/test_output.txt` and `results/load.txt` |
