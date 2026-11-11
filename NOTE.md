# Note: proofs, numbers, limits

All numbers are copied from `results/test_output.txt` (106 passed) and `results/load.txt`.
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
| max-lifetime | server ends every stream after 3 s | 3 | `Last-Event-ID` 176, 332 | 500 = 500 = 500 = 500 |

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
- **Logs.** 4248 JSON log lines scanned for two API keys, a new key, a stream token, its
  signature and the admin token: 0 lines contained any of them.
- **Timing** (200 requests each, interleaved, measured inside the Docker network only).
  Another project's run: median 6.459 ms. Nonexistent run: median 6.354 ms. Difference
  +105 µs, against a request-to-request standard deviation of 999 µs. Unknown key vs revoked
  key: 5.315 ms vs 5.268 ms, difference −47 µs. Both pairs run the same single query, so no
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
| Slow client | Client opens the stream and never reads; 20 000 points ingested | dropped after 2.4 s (limit 2.0 s in tests), at most 500 points buffered, RSS unchanged, full catch-up on reconnect |
| Partial writes | 999 valid points + 1 NaN | 422, 0 rows |
| | Exception injected after 50 of 100 inserts | 500, 0 new rows, `last_seq` unchanged, retry stores all 100 |
| Old key after rotation | Revoke, then use the old key and watch its open stream | API rejects at +32.4 ms, stream closed at +37.1 ms |
| | Same, with the revoke notification suppressed | stream closed after 4.70 s (5 s fallback check) |
| Lost notification | Point committed with no `NOTIFY` | delivered after 1.00 s (2 s safety poll) |
| Dead client, low rate | Client never reads, 3 points only (stall timeout cannot fire) | released after 2.0 s by the lifetime limit (2 s in tests) |

A final test checks every run ever written to the test database (1836 runs, 570 564 points,
114 621 batches, including load-test data): 0 runs with a `seq` gap or repeat, 0 ledger
mismatches, 0 non-finite values. Mutation checks (code broken on purpose, a test must fail)
were done by hand for the stream, isolation and E2E tests and are listed in `AI_USAGE.md`.
The newest files (`test_hardening.py`, `test_security.py`, `test_ratelimit.py`, the lifetime
tests) have not had them.

**Known limits.** One API process only. The slow-client design differs from the spec (see
DESIGN.md). In production the lifetime limit is 300 s, so a dead client can stay registered
for up to 5 minutes. Revoking a project's last key locks the project out. Rate limits
exist but only within one process; there is no TLS, which is left to a proxy in front.

**Security review.** Done last, recorded in `SECURITY.md` with scanner output in
`results/security/`. Eleven findings, all fixed or mitigated: the API container ran as root (now
uid 10001, read-only filesystem, no capabilities); the port was published on all interfaces
with public default credentials (now localhost only); no body size limit (now 413 above
2 MiB: the 10 MB request went from 349 ms to 10 ms); no cap on open streams (now 100 per
project, then 429); no rate limiting (now 600 requests/s per project, and 20 failed
credentials per address, then 1 per second: 992 guesses sent in 3 s, 10 evaluated); security
headers missing on API responses. Scanners: gitleaks found no
real secret, pip-audit no known vulnerability, Trivy 0 CRITICAL and 47 HIGH in Debian system
packages with no fix available.

## 5. Load: 50 runs × 10 events/s, 60 s, 50 SSE subscribers

One 8-core machine running the load generator, the API (1 process) and Postgres together.

| | batch = 10 (50 req/s) | batch = 1 (500 req/s target) |
|---|---|---|
| Achieved | 50.0 req/s, 500 points/s | **316.2 req/s** of 500 |
| Ingest latency p50 / p95 / p99 | 8.6 / 9.8 / 11.0 ms | 141 / 283 / 377 ms |
| Latency from scheduled send time, p50 | 9.9 ms | **17.7 s** (backlog grows for as long as the load lasts) |
| SSE end-to-end lag p50 / p99 | 9.9 / 12.5 ms | 176 / 447 ms |
| Errors, lost points, seq gaps | 0, 0, 0 | 0, 0, 0 |
| CPU (100 % = one core) | API 23.5 %, DB 22.0 % | API 89.0 %, DB 114.5 % |
| Peak memory | API 82 MiB (limit 512), DB 565 MiB | same |

This is the final code: hardened container and rate limits included (the limit of 600
requests/s per project was never reached). Five single runs of batch = 1 on this machine over
two days gave 309.8, 277, 281, 256.4 and 316.2 req/s; the lowest and the highest are both from
the last afternoon, on nearly the same code, so the spread is the machine and not the changes.
None of them reached 500. With 4 workers older code reached 413.9 req/s
(`results/load_workers4.txt`, not re-measured). Bottlenecks in the order found: the single
Python process (CPU), then Postgres commits, each of which carries a `NOTIFY` and queues on a
global lock. Neither was changed.

### Verdict

**SHIP, with a condition.** If clients send points in batches of 10, the system carries this
load easily: p99 latency 11 ms, no errors, and not one point lost in five load runs. If every
point is sent on its own (500 requests/s), the server does not get past 256–316 requests/s
and the backlog keeps growing. So the first thing I would change is to require clients to
batch, and the second is to put a proxy with TLS in front.

## What surprised me / what still concerns me

I did not expect the security review to find as many problems as it did in a system whose
tests were all passing. The other surprise was that a passing test does not mean the thing
was actually checked: tests should not be trusted too much, and have to be checked again and
again. What still concerns me is that the whole system was written by AI, so a bug can still
come from anywhere, including a place nobody thought of, and I am not fully confident in it.
If real people used it, I think the first errors would show up in the metrics.
