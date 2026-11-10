# SPEC — Mini Experiment Tracker (Soup take-home, Full-Stack)

You are helping build a take-home assignment. Read this whole file before writing code.
The graders value **correctness, honesty and evidence** over features or UI polish.
A clearly reported failing test beats a hidden one. Never delete, skip or weaken a failing
test to make the suite green — report it instead.

Deadline is tight (~20 h of wall clock, ~6 h of work). Prefer simple, well-reasoned code.

---

## 0. Assignment (verbatim intent)

Build a mini experiment tracker and answer two questions:
**does what the user sees always match what is stored, and do they see only what's theirs?**

Required: projects + API keys; metric ingestion via `POST /runs/{id}/metrics` in batches from a
client on an unreliable network; storage; a run web page that updates live (SSE); a run list.
Must start with `docker compose up`.

Graded parts:
1. Design doc (1 page) — event format & ordering, re-sent batches, catch-up after disconnect, project isolation.
2. Consistency proof — E2E test (UI + API): out-of-order, duplicated, late metrics, connection drop/recover mid-stream. Final page state == DB. Include raw output + what the test would NOT catch.
3. Isolation proof — project A cannot read, write, subscribe to SSE of, or guess IDs of project B (incl. SSE and cache). Discuss logs, error messages, timing.
4. Silent failures — lost events on reconnect, duplicates, queue overflow for slow client, partial writes, old API key working after rotation. How each was checked.
5. Verdict — SHIP / DON'T SHIP for 50 concurrent runs × 10 events/s, and what to change first.

Deliverables: repo + docker compose · tests + raw output · NOTE.md (≤2 pages) · one paragraph on
what surprised / still concerns · AI usage notes (`AI_USAGE.md`).

---

## 1. Stack

- Python 3.12, **FastAPI**, **uvicorn**
- **PostgreSQL 16**, **asyncpg** (raw SQL, no ORM — keeps transactions and constraints explicit)
- Live updates: **SSE** (`text/event-stream`), Postgres **LISTEN/NOTIFY** as the fan-out bus
- Frontend: server-served static HTML + vanilla JS (`EventSource`) — no build step
- Tests: **pytest**, **pytest-asyncio**, **httpx**, **Playwright (Python)** for UI E2E
- Load test: small async Python script (httpx) — no extra tools
- `docker-compose.yml`: services `db`, `api`, and a `tests` profile/service that runs the suite

Why SSE over WebSocket (put in DESIGN.md): data flows one way (server → browser); SSE has
built-in `id:` + `Last-Event-ID` resume and auto-reconnect; plain HTTP for auth/proxies/tests.

---

## 2. Data model

```sql
CREATE TABLE projects (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name        TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE api_keys (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id  UUID NOT NULL REFERENCES projects(id),
  key_hash    BYTEA NOT NULL UNIQUE,          -- sha256(secret); plaintext never stored
  prefix      TEXT NOT NULL,                  -- first 8 chars, for display/logs only
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  revoked_at  TIMESTAMPTZ                     -- NULL = active
);

CREATE TABLE runs (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id  UUID NOT NULL REFERENCES projects(id),
  name        TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seq    BIGINT NOT NULL DEFAULT 0       -- server-assigned event counter per run
);

CREATE TABLE batches (                         -- idempotency ledger
  run_id      UUID NOT NULL REFERENCES runs(id),
  batch_id    UUID NOT NULL,                   -- client-generated
  payload_sha BYTEA NOT NULL,                  -- detect same id + different body
  accepted    INT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, batch_id)
);

CREATE TABLE metrics (                         -- one row per data point
  run_id      UUID NOT NULL REFERENCES runs(id),
  name        TEXT NOT NULL,                   -- e.g. "loss"
  step        BIGINT NOT NULL,                 -- client training step (logical order)
  value       DOUBLE PRECISION NOT NULL,
  client_ts   TIMESTAMPTZ,
  seq         BIGINT NOT NULL,                 -- server event seq (delivery order)
  batch_id    UUID NOT NULL,
  PRIMARY KEY (run_id, name, step)             -- natural dedup key
);
CREATE INDEX metrics_run_seq ON metrics (run_id, seq);
```

Rules:
- **Two orders, kept separate.** `step` = logical order chosen by the client (charts sort by it).
  `seq` = server delivery order, strictly increasing per run (SSE resume uses it).
- `(run_id, name, step)` is the dedup key. Conflict policy: **first write wins**
  (`ON CONFLICT DO NOTHING`). Document this choice; mention last-write-wins as the alternative.
- All IDs are UUIDv4 — not guessable or enumerable.

---

## 3. API

Auth: `Authorization: Bearer <api_key>` on every API call. Resolve key → project_id once per
request; **every query filters by project_id**. Never trust a project_id from the URL/body.

| Method | Path | Notes |
|---|---|---|
| POST | `/projects` | Admin bootstrap (guarded by `ADMIN_TOKEN` env). Returns project + first API key (plaintext shown once). |
| POST | `/projects/me/keys` | Create new key for caller's project. |
| POST | `/projects/me/keys/{key_id}/revoke` | Revoke immediately (rotation = create new + revoke old). |
| POST | `/runs` | Create run in caller's project. |
| GET | `/runs` | List caller's runs (id, name, created_at, last_seq, point count). |
| GET | `/runs/{id}` | Run detail. |
| POST | `/runs/{id}/metrics` | Batch ingest (see §4). |
| GET | `/runs/{id}/metrics?since_seq=N&limit=M` | Snapshot / catch-up page, ordered by seq. |
| POST | `/runs/{id}/stream-token` | Short-lived (60 s) signed token bound to (project_id, run_id). |
| GET | `/runs/{id}/stream?token=...` | SSE stream (see §5). |
| GET | `/` , `/runs/{id}/view` | Static UI pages. |
| GET | `/healthz` | Liveness. |

**Uniform not-found:** a run that doesn't exist and a run in another project both return
`404 {"error":"not_found"}` — same body, same code path, same DB query shape (single query with
`WHERE id=$1 AND project_id=$2`). Never 403 for cross-project resources.

Error bodies never echo IDs, key prefixes, SQL, or stack traces. 401 body is always
`{"error":"unauthorized"}` whether key is unknown, malformed, or revoked.

---

## 4. Ingestion: `POST /runs/{id}/metrics`

Request:
```json
{
  "batch_id": "uuid-v4 generated by client, reused on retry",
  "points": [ {"name":"loss","step":12,"value":0.41,"ts":"2026-11-10T15:00:00Z"} ]
}
```
Limits: ≤ 1000 points per batch, name ≤ 64 chars, finite floats only (reject NaN/Inf) → 422.

Algorithm — **single transaction**:
1. `SELECT ... FROM runs WHERE id=$1 AND project_id=$2 FOR UPDATE` → 404 if missing.
   (Row lock serializes seq assignment per run; different runs don't block each other.)
2. `INSERT INTO batches ... ON CONFLICT DO NOTHING`.
   - If it already existed: compare `payload_sha`. Same → return `200 {"duplicate":true,"accepted":<original>}`.
     Different → `409 {"error":"batch_id_reused"}`.
3. Sort points deterministically (name, step). For each new point assign `seq = last_seq + k`;
   insert with `ON CONFLICT (run_id,name,step) DO NOTHING RETURNING seq` — only returned rows
   consume a seq (no gaps from dropped duplicates; renumber after insert if needed, or insert
   into a temp table first then number the survivors).
4. `UPDATE runs SET last_seq = <new max>`.
5. `NOTIFY run_events, '<run_id>:<new last_seq>'` (inside tx → delivered only on commit).
6. Commit. Return `200 {"accepted":n,"duplicates":d,"last_seq":S}`.

Guarantees to state: a batch is all-or-nothing (no partial writes); resending is safe;
seq is gap-free and monotonic per run; events become visible to SSE only after commit.

---

## 5. Live stream: SSE

- `EventSource` can't send headers → browser first calls `POST /runs/{id}/stream-token` with
  the API key (UI page gets the key via a login form stored in `sessionStorage`), then opens
  `/runs/{id}/stream?token=...`. Token: HMAC-signed, 60 s TTL, bound to project_id + run_id +
  key_id. Stream re-checks key not revoked at connect **and periodically (every 5 s)**; closes
  the stream if revoked. Document: token in URL may appear in access logs → short TTL + log
  redaction middleware strips `token=` from logged URLs.
- On connect, resume point = `Last-Event-ID` header, else `?since_seq=`, else 0.
- Server loop per connection:
  1. Register a listener for this run on the LISTEN/NOTIFY bus.
  2. **Then** read from DB `WHERE seq > cursor ORDER BY seq` and send (catch-up).
     Register-before-read avoids the gap between read and subscribe.
  3. On notify (or every 2 s as a safety poll), read again from DB `seq > cursor`. The DB is
     the source of truth; NOTIFY is only a wake-up signal, never the payload. This makes
     duplicate/lost notifies harmless.
  4. Each SSE message: `id: <seq>`, `event: metric`, `data: {"name","step","value","seq"}`.
     Send batches of points as one message with `id` = max seq to cut overhead (optional).
  5. Heartbeat comment `: ping` every 15 s.
- Slow client: per-connection bounded queue (e.g. 1000 messages). If full → send
  `event: overflow` and close. Client reconnects with Last-Event-ID and catches up from DB.
  Log + count overflows (metric `sse_overflow_total`).
- Client JS: keeps state keyed by `(name, step)`; ignores seq ≤ last seen; renders a table
  (and a simple chart — optional, canvas/inline SVG) sorted by step. Shows connection status
  and `last_seq`. Exposes `window.__state` (JSON of points + last_seq) for tests.

---

## 6. UI

- `/` — enter API key → list runs (name, created, points, last_seq), links to run page.
- `/runs/{id}/view` — live table per metric (sorted by step), status badge
  (connected / reconnecting), `last_seq`, total points. Minimal clean CSS. No framework.

---

## 7. Tests (all must run inside docker: `docker compose run --rm tests`)

Save raw output: `pytest -v 2>&1 | tee results/test_output.txt`. Commit `results/`.

### 7.1 Consistency E2E (`tests/test_consistency_e2e.py`) — Playwright + httpx
1. Create project, run; open run page in headless Chromium.
2. Generate 500 points (3 metrics). Split into batches; send **shuffled**, with ~20% batches
   **re-sent** (same batch_id), some **delayed** (sent after later batches), concurrently.
3. Mid-stream, **drop the connection**: use a toxiproxy-like approach — simplest reliable
   option: Playwright `context.set_offline(True)` for 3–5 s while ingestion continues, then
   `set_offline(False)`. Also a second variant: server-side kill of the SSE connection
   (test-only endpoint enabled by `TEST_MODE=1`) to exercise Last-Event-ID resume.
4. Wait until page `last_seq` == DB `runs.last_seq`.
5. Assert page `window.__state` points == `SELECT name,step,value FROM metrics WHERE run_id`
   (exact set equality) and counts match and no duplicates in DOM.
6. Print a summary line: sent, re-sent, unique, db_rows, page_rows, reconnects.

Write in NOTE.md what it does NOT catch, e.g.: only one browser/tab; network drop simulated
in browser, not real TCP half-open/proxy buffering; single API instance (LISTEN/NOTIFY across
replicas untested); clock skew; process crash mid-transaction only indirectly; large runs /
memory; render correctness beyond the state object.

### 7.2 Isolation (`tests/test_isolation.py`) — projects A and B, runs ra (A) and rb (B)
- A's key: GET `/runs/rb` → 404 identical to GET `/runs/<random uuid>` (compare body bytes).
- A's key: POST `/runs/rb/metrics` → 404, and rb's DB row count unchanged.
- A's key: GET `/runs` contains no B runs; GET `/runs/rb/metrics` → 404.
- A's key: POST `/runs/rb/stream-token` → 404.
- A's stream-token for ra used on `/runs/rb/stream` → 401/404, no bytes of rb data.
- Tampered / expired token → 401.
- ID guessing: run IDs are UUIDv4 (assert version), sequential creation yields non-adjacent IDs.
- Cache: responses carry `Cache-Control: no-store, private` and `Vary: Authorization`; test
  asserts headers; test that same URL with key A then key B returns different results (no
  in-process cache keyed only by URL).
- Errors: 401/404 bodies contain no UUIDs/prefixes (regex check).
- Logs: run a request with a key and a stream token, then scan captured logs — assert neither
  the full key nor the token appears (redaction).
- Timing (measured, not asserted strictly): median latency for "other project's run" vs
  "nonexistent run" over 200 requests each; print both numbers and report in NOTE.md.

### 7.3 Silent failures (`tests/test_silent_failures.py`)
- **Lost events on reconnect:** open stream, read to seq K, disconnect, ingest 100 more,
  reconnect with `Last-Event-ID: K` → receive exactly seq K+1..K+100, no gaps.
- **Duplicates:** same batch 5× concurrently (asyncio.gather) → DB has points once; all
  responses consistent; one `accepted>0`, the rest `duplicate:true`. Same batch_id + different
  body → 409.
- **Queue overflow:** test client opens stream and does not read; ingest a lot → server sends
  overflow + closes (or connection is dropped); reconnect catches up fully; server memory /
  queue size bounded (assert via `/debug/stats` in TEST_MODE).
- **Partial writes:** batch with 999 valid points + 1 invalid (NaN) → 422 and **zero** rows
  written. Also inject failure mid-transaction (TEST_MODE fault flag raising after half the
  inserts) → zero rows, last_seq unchanged.
- **Key rotation:** create key2, revoke key1 → key1 gets 401 on next API call immediately;
  an SSE stream opened with key1 is closed within ≤ 5 s; stream token minted with key1 is
  rejected after revoke. Measure and report the actual window.

### 7.4 Load (`loadtest/load.py`, not part of pytest; output saved to `results/load.txt`)
50 concurrent runs × 10 events/s for 60 s, batches of 10 (1 request/s/run) and a variant with
batch size 1 (10 req/s/run = 500 req/s). Also 50 SSE subscribers (one per run).
Report: p50/p95/p99 ingest latency, error count, end-to-end lag (point ts → received on SSE),
final DB count vs sent count, CPU/mem from `docker stats` snapshot. This feeds the verdict.

---

## 8. Repo layout

```
.
├── docker-compose.yml
├── Dockerfile
├── README.md          # how to run app, tests, load test
├── DESIGN.md          # 1 page (part 1)
├── NOTE.md            # ≤2 pages: parts 2–5, surprises/concerns paragraph
├── AI_USAGE.md        # where AI was used, accepted/changed/verified
├── app/
│   ├── main.py  db.py  auth.py  ingest.py  stream.py  bus.py  logging.py
│   ├── schema.sql
│   └── static/ index.html  run.html  app.js  style.css
├── tests/  conftest.py  test_consistency_e2e.py  test_isolation.py  test_silent_failures.py
├── loadtest/ load.py
└── results/  test_output.txt  load.txt
```

---

## 9. Working rules for Claude Code

1. Build in this order: schema + docker → auth/projects/runs → ingest (with tests 7.3 duplicates
   & partial writes) → SSE + catch-up (7.3 reconnect) → UI → isolation tests → E2E → load test → docs.
2. After each step run the relevant tests and show me real output. Do not claim a test passes
   without running it.
3. If a test fails, investigate and tell me the root cause before changing anything. Do not
   modify a test to make it pass unless the test itself is wrong — and then explain why.
4. Keep `TEST_MODE` hooks (fault injection, kill stream, debug stats) disabled unless env set.
5. No secrets in logs. Structured JSON logs with request id, project id (not key).
6. Keep a running list in `AI_USAGE.md` of non-trivial suggestions you made that I accepted,
   changed, or rejected — I will edit it.
7. Don't write the final verdict or "surprises" paragraph yourself — draft placeholders with
   the measured numbers; I write the conclusions.
