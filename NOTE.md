# Note: proofs, numbers, limits

Numbers come from `results/test_output.txt` (106 passed) and `results/load.txt`.

## 2. Consistency: page == database

`tests/test_consistency_e2e.py` drives headless Chromium against the real API. Each scenario
sends 500 unique points as 50–61 shuffled, concurrent batches (7–9 re-sent, 5 overlapping,
3–4 late) and disrupts the connection mid-stream. Four views must end equal: database, API
snapshot, `window.__state`, and the DOM table.

| Scenario | Disruption | Reconnects | Resumed from | All four views |
|---|---|---|---|---|
| network-down | reset, refused 4 s | 1 | `Last-Event-ID` 176 | 500 |
| silent-stall | frozen, no error | 1 | `since_seq` 179 | 500 |
| kill | stream dropped twice | 2 | `Last-Event-ID` 139, 350 | 500 |
| expired-token | killed after token expiry | 2 | `since_seq` 105, 382 | 500 |
| max-lifetime | streams ended after 3 s | 3 | `Last-Event-ID` 176, 348 | 500 |

Every scenario ends with `seq` exactly 1…500.

**Found by the test.** (1) Playwright's `set_offline(True)` does not cut an open SSE response
(20 of 20 points delivered "offline"), so the browser goes through a fault-injecting TCP proxy.
(2) On a silent stall the page stayed "connected" forever; it now reconnects after 3 missed
heartbeats.

**Not caught; known limits.** One browser tab. One API process: `LISTEN/NOTIFY` across
replicas is untested; every limit is per process. Runs of 500 points only. Steps above 2⁵³
lose precision in JavaScript. Faults are TCP-level on one host: no packet loss, buffering
proxy or database crash. The slow-client design differs from the spec (DESIGN.md). In
production a dead client stays registered up to 300 s. Revoking a project's last key locks it
out. No TLS.

## 3. Isolation: A cannot reach B

`tests/test_isolation.py`, 12 tests. With A's key every endpoint treats B's run like a random
UUID: same status, same bytes, B's rows unchanged.

- **Ids.** UUIDv4; smallest gap between consecutive ids: 2¹²².
- **Errors.** Only `{"error":"<code>"}`; 36 malformed requests gave 4xx, never 500.
- **Logs.** 6356 lines scanned for keys, tokens and the admin token: 0 hits.
- **Timing** (200 requests each, inside the Docker network). Another project's run: median
  5.323 ms; nonexistent run: 5.295 ms; difference +28.3 µs, standard deviation 515.6 µs.
  Unknown vs revoked key: 3.952 vs 3.980 ms. A difference below the noise is not ruled out.

**Bug found late, fixed.** No key plus broken JSON got 422, not 401.

## 4. Silent failures

| Failure | Check | Measured |
|---|---|---|
| Lost events | Disconnect at seq 40, ingest 100, resume | seq 41…140, no gaps |
| | 600 points, 17 reconnects | 600 received, 600 unique, in order |
| Duplicates | Same batch sent 5 times concurrently | 1 fresh, 4 `duplicate:true` |
| Slow client | Never reads; 20 000 points | dropped after 2.3 s (limit 2.0 s), ≤ 500 buffered |
| Partial writes | 999 valid points + 1 NaN | 422, 0 rows |
| | Exception after 50 of 100 inserts | 500, 0 new rows |
| Old key | Revoke with a stream open | API rejects +15.5 ms, stream closed +17.2 ms |
| | Same, no notification | closed after 4.70 s (5 s fallback) |
| Lost notification | Point committed with no `NOTIFY` | delivered after 1.00 s (2 s poll) |
| Dead client | Never reads, 3 points | released after 2.1 s (lifetime 2 s) |

Whole test database (2096 runs, 636 054 points, 115 917 batches): 0 `seq` gaps or
repeats, 0 ledger mismatches, 0 non-finite values.

**Security.** Eleven findings, all fixed or mitigated; Trivy reports 0 CRITICAL and 47 HIGH
with no fix available. Details in `SECURITY.md`.

## 5. Load: 50 runs × 10 events/s, 60 s, 50 SSE subscribers

Everything on one 8-core machine; 1 API process.

| | batch = 10 (50 req/s) | batch = 1 (500 req/s target) |
|---|---|---|
| Achieved | 50.0 req/s | **316.2 req/s** |
| Ingest p50 / p95 / p99 | 8.6 / 9.8 / 11.0 ms | 141 / 283 / 377 ms |
| From schedule, p50 | 9.9 ms | **17.7 s**, backlog grows |
| SSE lag p50 / p99 | 9.9 / 12.5 ms | 176 / 447 ms |
| Errors, lost points, seq gaps | 0, 0, 0 | 0, 0, 0 |
| CPU (100 % = one core) | API 23.5 %, DB 22.0 % | API 89.0 %, DB 114.5 % |

Five batch = 1 runs ranged 256–316 req/s (see `results/`); none reached 500. Four workers,
older code: 413.9 req/s.

## Questions we expected in the live discussion

- **Client offline 30 s.** The badge shows "reconnecting" and the page keeps what it had
  (network-down scenario: 176 while the database went from 183 to 389). On reconnect
  `Last-Event-ID` makes the server read `seq > 176` from the database; caught up in 0.7 s.
- **Weakest point.** One API process; `NOTIFY` serialized on a global lock at commit;
  `LISTEN/NOTIFY` across replicas untested.
- **Noticing a lost event in production.** Today we would not: the page does not check for
  `seq` gaps and `sse_overflow_total` is exposed only with `TEST_MODE`. We would have the
  client report gaps, export that counter, and run the existing seq-gap and ledger query
  periodically.

### Verdict

**SHIP, if clients batch.** If clients send points in batches of 10, the system carries this
load easily: p99 latency 11.0 ms, no errors, and not one point lost. If every point is sent on
its own (500 requests/s), the server does not get past 256–316 requests/s and the backlog keeps
growing (17.7 s behind at p50), although still no point is lost. So the first thing I would
change is to require clients to batch (make it the SDK default), and the second is to run
several API workers behind a proxy with TLS.

## What surprised me / what still concerns me

I did not expect the security review to find as many problems as it did (eleven) in a
system whose tests were all passing. The other surprise was that a passing test does not
mean the thing was actually checked: `set_offline`, the method the spec suggested, never
cut the stream, so that test would have passed while checking nothing. Tests should not be
trusted too much, and have to be checked again and again. What still concerns me is that
the whole system was written by AI, so a bug can still come from anywhere, including a place
nobody thought of, and I am not fully confident in it. The newest tests have not been
broken on purpose yet, and several API processes have never been tested together. If real
people used it, I think the first errors would show up in the metrics.
