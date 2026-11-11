# Design

One API process (FastAPI, asyncpg, raw SQL) and PostgreSQL. The database is the only source of
truth; everything the browser shows can be rebuilt from it at any moment.

## Event format and ordering

A point is `{name, step, value, ts?}`. There are **two orders, kept apart on purpose**:

- `step` is the client's logical order. Tables on the page sort by it.
- `seq` is the server's delivery order: assigned at commit, per run, strictly increasing and
  gap-free (1, 2, 3, …). The stream and its resume point use only `seq`.

Late or shuffled batches therefore need no special handling: a late point gets the next `seq`
and the page places it by `step`. An SSE message is one point:
`id: <seq>`, `event: metric`, `data: {"name","step","value","seq","ts"}`.

## Re-sent batches

A client on a bad network cannot know whether a batch arrived, so it must be able to resend.

- A point's identity is `(run_id, name, step)`, the primary key. **First write wins**; a later
  point with the same key is counted as a duplicate and consumes no `seq`.
- Each batch carries a client-generated `batch_id`, recorded in a ledger with a hash of the
  parsed payload. Same id and same payload → `200 {"duplicate": true}` with the original
  result. Same id and a different payload → `409 batch_id_reused`.
- Ingest is **one transaction**: lock the run row → check the ledger → insert the new points
  and number them → write the ledger row → advance `runs.last_seq` → `NOTIFY` → commit. A batch
  is stored whole or not at all. The row lock serializes writers of one run only.

## Catch-up after a disconnect

`NOTIFY` is only a wake-up, never the payload. Each stream:

1. subscribes to the run on the in-process bus (one `LISTEN` connection for the whole API),
2. **then** reads `seq > cursor ORDER BY seq` from the database and writes it,
3. repeats step 2 on every wake-up, and every 2 s even without one.

Subscribing before reading closes the gap between "read" and "listening". Lost or duplicated
notifications are harmless because the read is always "everything after my cursor". The
cursor on connect is `Last-Event-ID`, else `?since_seq=`, else 0, so a reconnect is the same
code path as a first connect. The page drops any event with `seq` ≤ the last one it applied.

Three ways a stream ends, and all of them resume the same way:

- **Slow client.** There is no per-connection queue. The stream holds at most one page
  (500 points) and reads the next only after the previous write was accepted, so memory per
  connection is bounded by construction. A client that accepts no write for 10 s is dropped
  and counted (`sse_overflow_total`). *This differs from the spec's 1000-message queue with an
  `overflow` event:* a client that cannot read cannot receive that event either.
- **Silent client.** The page treats 3 missed heartbeats (`event: ping`, every 15 s) as a dead
  connection and reconnects on its own.
- **Dead client the server cannot see.** Small writes to a vanished peer keep succeeding into
  kernel buffers, so every stream is ended after a maximum lifetime (300 s) and live clients
  reconnect by themselves.

## Project isolation

- The project comes **only** from the API key. No endpoint accepts a project id; every query
  carries `AND project_id = $key_project`.
- A run that belongs to another project and a run that does not exist are the same query
  (`WHERE id = $1 AND project_id = $2`), the same code path and the same bytes:
  `404 {"error":"not_found"}`. There is no 403 anywhere.
- Every bad credential (missing, malformed, unknown, revoked) gives the same
  `401 {"error":"unauthorized"}`, decided before the request body is read.
- Keys are stored as SHA-256 only. Run ids are random UUIDv4.
- `EventSource` cannot send headers, so the stream takes a token: HMAC-signed, 60 s, bound to
  project + run + key. The key is re-checked at connect, on revocation (pushed by `NOTIFY`,
  tens of milliseconds) and every 5 s as a fallback; a revoked key closes its open streams.
- Responses carry `Cache-Control: no-store, private` and `Vary: Authorization`. There is no
  in-process cache. Pages are static files with a strict CSP; they contain no data.
- Logs are JSON with request id and project id. Query values are masked, `token=` is
  redacted, and every line is scrubbed of anything shaped like a key or a token.

## Limits

1–1000 points per batch · name 1–64 characters, no control characters · finite doubles only ·
`step` 0…2⁶³−1 · request body ≤ 2 MiB · ≤ 100 open streams per project · one API process
(the bus, stream registry and stream cap are per process). No rate limiting and no TLS: both
belong to a proxy in front. The security review is in SECURITY.md.
