# Note: proofs, numbers, limits

All numbers are copied from `results/test_output.txt` (95 passed) and `results/load.txt`.
They vary a little per run; the committed files are the reference.

## 2. Consistency: page == database

`tests/test_consistency_e2e.py` drives a real headless Chromium against the real API. Each
scenario sends 500 unique points in 3 metrics as 50–61 shuffled, concurrent batches: 7–9 are
re-sent with the same `batch_id`, 5 overlap earlier batches with different values, 3–4 arrive
late. Mid-stream the connection is disrupted. At the end **four views** must be equal: the
database, the API snapshot, the page's `window.__state`, and the rendered table read back
from the DOM.

| Scenario | Disruption | Reconnects | Resumed from | Result |
|---|---|---|---|---|
| network-down | all connections reset, new ones refused 4 s | 1 | `Last-Event-ID` 176 | 500 = 500 = 500 = 500 |
| silent-stall | connection frozen, no error ever delivered | 1 | `since_seq` 179 | 500 = 500 = 500 = 500 |
| kill | server drops the stream twice | 2 | `Last-Event-ID` 139, 350 | 500 = 500 = 500 = 500 |
| expired-token | stream killed after its token expired | 2 | `since_seq` 105, 382 | 500 = 500 = 500 = 500 |
| max-lifetime | server ends every stream after 3 s | 3 | `Last-Event-ID` 176, 348 | 500 = 500 = 500 = 500 |

During the network-down window the database went from seq 183 to 389 while the page stayed at
176, then caught up in 0.7 s. In every scenario `seq` is exactly 1…500 and
`accepted_total` = 500.

**Two things the test found.** (1) Playwright's `set_offline(True)`, the method the spec
suggested, does not cut an open SSE response: the stream delivered 20 of 20 points while
"offline". A test built on it would never have disconnected anything, so the browser goes
through a small fault-injecting TCP proxy instead (`tests/netproxy.py`). (2) On a silently
stalled connection the page stayed "connected" forever. Heartbeats are now real events and the
page reconnects after 3 missed ones.

**What it does not catch.** One browser, one tab, one Chromium version. One API process:
`LISTEN/NOTIFY` across replicas is untested. Runs of 500 points; the page renders every point
as a DOM row and would be slow for very large runs. Steps above 2⁵³ lose precision in
JavaScript. Faults are injected at TCP level by a proxy on the same host: no real packet loss,
no buffering reverse proxy, no mobile network. No API or database crash mid-transaction (only
an injected exception). Clock skew is irrelevant by design (ordering never uses clocks) but is
also untested. Table correctness is checked, visual layout is not.

## 3. Isolation: A cannot reach B

`tests/test_isolation.py`, 12 tests. With A's key, every endpoint treats B's run exactly like a
random UUID: same status, same body bytes, and B's row counts unchanged. A's stream token does
not open B's stream; an open stream of A never receives B's events; a project id supplied by
the client is ignored.

- **Guessing ids.** Run ids are UUIDv4; the smallest gap between consecutively created ids was
  2¹²³. Other spellings of an id (`{…}`, `urn:uuid:`, upper case) reach the same run for its
  owner and the same 404 for anyone else.
- **Cache.** `Cache-Control: no-store, private` and `Vary: Authorization` on every response,
  errors included; the same URL with key A then key B returns different data.
- **Error messages.** Only `{"error":"<code>"}`. A regex check finds no UUIDs, key prefixes or
  internals in the error bodies. 36 malformed requests all returned 4xx with that shape and
  never a 500.
- **Logs.** 2048 JSON log lines scanned for two API keys, a new key, a stream token, its
  signature and the admin token: 0 lines contained any of them.
- **Timing** (200 requests each, interleaved, measured inside the Docker network only).
  Another project's run: median 5.269 ms. Nonexistent run: median 5.195 ms. Difference
  +74 µs, against a request-to-request standard deviation of 685 µs. Unknown key vs revoked
  key: 3.802 ms vs 3.836 ms, difference +33 µs. Both pairs run the same single query, so no
  difference is expected; these numbers do not rule out one below the noise.

**Bug found late.** A request with no valid key and a syntactically broken JSON body got 422
instead of 401, because the framework parses the body before authentication. Nothing leaked,
but unauthenticated input reached the parser. Fixed: the credential is checked first.

## 4. Silent failures

| Failure | How it was checked | Measured |
|---|---|---|
| Lost events on reconnect | Read to seq 40, disconnect, ingest 100, resume with `Last-Event-ID: 40` | received seq 41…140, no gaps |
| | 600 points ingested across 17 forced reconnects | 600 received, 600 unique, in order |
| Duplicates | Same batch sent 5 times concurrently | 1 fresh, 4 `duplicate:true`, stored once |
| | Same `batch_id`, different body; 12-way race of two payloads | 409; exactly one payload stored |
| Slow client | Client opens the stream and never reads; 20 000 points ingested | dropped after 2.3 s (limit 2.0 s in tests), at most 500 points buffered, RSS unchanged, full catch-up on reconnect |
| Partial writes | 999 valid points + 1 NaN | 422, 0 rows |
| | Exception injected after 50 of 100 inserts | 500, 0 new rows, `last_seq` unchanged, retry stores all 100 |
| Old key after rotation | Revoke, then use the old key and watch its open stream | API rejects at +15.3 ms, stream closed at +16.5 ms |
| | Same, with the revoke notification suppressed | stream closed after 4.70 s (5 s fallback check) |
| Lost notification | Point committed with no `NOTIFY` | delivered after 1.00 s (2 s safety poll) |
| Dead client, low rate | Client never reads, 3 points only (stall timeout cannot fire) | released after 2.1 s by the lifetime limit (2 s in tests) |

A final test checks every run ever written to the test database (1371 runs, 409 128 points,
71 971 batches, including load-test data): 0 runs with a `seq` gap or repeat, 0 ledger
mismatches, 0 non-finite values. Mutation checks (code broken on purpose, a test must fail)
were done by hand for the stream, isolation and E2E tests and are listed in `AI_USAGE.md`.
The newest files (`test_hardening.py`, the lifetime tests) have not had them.

**Known limits.** One API process only. The slow-client design differs from the spec (see
DESIGN.md). In production the lifetime limit is 300 s, so a dead client can stay registered
for up to 5 minutes. There is no request body size limit: a 10 MB body is parsed in full
before it is rejected (349 ms). Revoking a project's last key locks the project out.

## 5. Load: 50 runs × 10 events/s, 60 s, 50 SSE subscribers

One 8-core machine running the load generator, the API (1 process) and Postgres together.

| | batch = 10 (50 req/s) | batch = 1 (500 req/s target) |
|---|---|---|
| Achieved | 50.0 req/s, 500 points/s | **309.8 req/s** of 500 |
| Ingest latency p50 / p95 / p99 | 8.5 / 10.0 / 11.1 ms | 144 / 290 / 379 ms |
| Latency from scheduled send time, p50 | 9.8 ms | **18.6 s** (backlog grows for as long as the load lasts) |
| SSE end-to-end lag p50 / p99 | 9.8 / 12.6 ms | 182 / 450 ms |
| Errors, lost points, seq gaps | 0, 0, 0 | 0, 0, 0 |
| CPU (100 % = one core) | API 23.8 %, DB 22.0 % | API 89.1 %, DB 119.2 % |

With 4 workers batch = 1 reached 413.9 req/s (`results/load_workers4.txt`), still under 500.
A rerun on 2026-11-11 gave 277–281 req/s for batch = 1 on the same machine. Bottlenecks in the
order found: the single Python process (CPU), then Postgres commits, each of which carries a
`NOTIFY` and queues on a global lock. Neither was changed.

### Verdict

> **TODO (author):** SHIP / DON'T SHIP for 50 concurrent runs × 10 events/s, and what to
> change first. Inputs: the table above; batch = 10 is sustained with 0 errors and p99 11.1 ms;
> batch = 1 is not sustained (309.8 of 500 req/s) but loses nothing; single-process limit.

## What surprised me / what still concerns me

> **TODO (author):** one paragraph. Candidate facts: `set_offline` not cutting an open stream;
> the page staying "connected" on a stalled connection; the server being unable to see a dead
> client at low data rates; 401 vs 422 ordering in the framework; httpx collapsing as a load
> generator at 500 req/s; a 2 s healthcheck costing 10–40 % CPU on an idle container.
