# Design

One API process (FastAPI, asyncpg, raw SQL) and PostgreSQL. The database is the only source of
truth; everything the browser shows can be rebuilt from it.

## Event format and ordering

A point is `{name, step, value, ts?}`. There are **two orders, kept apart on purpose**:

- `step`: the client's logical order. Tables on the page sort by it.
- `seq`: the server's delivery order, assigned at commit, per run, gap-free (1, 2, 3, …). The
  stream and its resume point use only `seq`.

A late or shuffled point simply gets the next `seq`, and the page places it by `step`. One SSE
message is one point: `id: <seq>`, `event: metric`, `data: {"name","step","value","seq","ts"}`.

## Re-sent batches

- A point's identity is `(run_id, name, step)`. **First write wins**; a repeat consumes no `seq`.
- Each batch carries a client-generated `batch_id`, stored in a ledger with a hash of the
  payload. Same id, same payload → `200 {"duplicate": true}`; same id, different payload →
  `409 batch_id_reused`.
- Ingest is **one transaction**: lock the run row → check the ledger → insert and number the new
  points → write the ledger row → advance `runs.last_seq` → `NOTIFY` → commit. A batch is
  stored whole or not at all.

## Catch-up after a disconnect

`NOTIFY` is only a wake-up, never the payload. A stream subscribes to the run **first**, then
reads `seq > cursor ORDER BY seq` from the database, and repeats that read on every wake-up and
every 2 s. Subscribing before reading closes the gap between the two; lost or duplicated
notifications are harmless. The cursor is `Last-Event-ID`, else `?since_seq=`, else 0, so a
reconnect is the same code path as a first connect. The page drops any event with `seq` ≤ the
last one it applied.

Three ways a stream ends; all resume the same way:

- **Slow client.** No per-connection queue: the stream holds one page (500 points) and reads
  the next only after the previous write was accepted. A client that accepts no write for 10 s
  is dropped and counted (`sse_overflow_total`). *This differs from the spec's 1000-message
  queue with an `overflow` event:* a client that cannot read cannot receive that event either.
- **Silent client.** The page treats 3 missed heartbeats (`event: ping`, every 15 s) as a dead
  connection and reconnects.
- **Dead client the server cannot see.** Writes to a vanished peer keep succeeding into kernel
  buffers, so every stream is ended after 300 s; live clients reconnect by themselves.

## Project isolation

- The project comes **only** from the API key. No endpoint accepts a project id; every query
  carries `AND project_id = $key_project`.
- Another project's run and a missing run are the same query and the same bytes:
  `404 {"error":"not_found"}`. There is no 403 anywhere.
- Every bad credential gives the same `401 {"error":"unauthorized"}`, decided before the body
  is read. An address that keeps failing gets `429`, valid credential or not.
- Keys are stored as SHA-256 only. Run ids are random UUIDv4.
- `EventSource` cannot send headers, so the stream takes a token: HMAC-signed, 60 s, bound to
  project + run + key. The key is re-checked at connect, on revocation (pushed by `NOTIFY`)
  and every 5 s; a revoked key closes its open streams.
- Responses carry `Cache-Control: no-store, private` and `Vary: Authorization`. No cache.
- Logs are JSON with request id and project id; keys and tokens are scrubbed.

## Limits

1–1000 points per batch · name ≤ 64 characters · finite doubles · body ≤ 2 MiB · 100 streams
and 600 requests/s per project · one API process (bus, registry and limits are per process) ·
no TLS (see SECURITY.md).
