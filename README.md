# Mini Experiment Tracker

Projects with API keys, batched metric ingestion over an unreliable network, a run page that
updates live over SSE, and a run list. Built to answer one question with evidence:
**does what the user sees always match what is stored, and do they see only what is theirs?**

| Document | What is in it |
|---|---|
| [DESIGN.md](DESIGN.md) | Event format and ordering, re-sent batches, catch-up, isolation (1 page) |
| [NOTE.md](NOTE.md) | Proofs, measured numbers, what the tests do not catch, verdict |
| [AI_USAGE.md](AI_USAGE.md) | Where AI was used; what was accepted, changed or rejected |
| [results/](results/) | Raw test output, load test output, screenshots |
| [SPEC.md](SPEC.md) | The working specification the code was built from |

## Run it

Requires Docker with the compose plugin. Nothing is installed on the host.

```bash
docker compose up --build -d      # db + api; the UI is at http://localhost:8000
```

Create a project (the admin token defaults to `dev-admin-token`; set `ADMIN_TOKEN` to change it):

```bash
curl -s -X POST localhost:8000/projects \
  -H 'Authorization: Bearer dev-admin-token' -H 'Content-Type: application/json' \
  -d '{"name":"demo"}'
# → {"project": {...}, "api_key": {"key": "sk_...", ...}}   the key is shown once
```

Paste the `sk_...` key into the UI, create a run, then send points:

```bash
KEY=sk_...; RUN=<run id>
curl -s -X POST localhost:8000/runs/$RUN/metrics \
  -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
  -d '{"batch_id":"11111111-1111-4111-8111-111111111111",
       "points":[{"name":"loss","step":0,"value":0.9},{"name":"loss","step":1,"value":0.7}]}'
```

The run page (`/runs/<id>/view`) shows them as they arrive.

## API

All endpoints except `POST /projects` and the stream take `Authorization: Bearer <api key>`.

| Method and path | Purpose |
|---|---|
| `POST /projects` | Create a project and its first key (admin token) |
| `POST /projects/me/keys` | Create another key for the caller's project |
| `POST /projects/me/keys/{key_id}/revoke` | Revoke a key (idempotent) |
| `POST /runs`, `GET /runs`, `GET /runs/{id}` | Create, list, read runs |
| `POST /runs/{id}/metrics` | Ingest a batch: `{batch_id, points: [{name, step, value, ts?}]}`, 1–1000 points |
| `GET /runs/{id}/metrics?since_seq=&limit=` | Snapshot read, ordered by `seq` |
| `POST /runs/{id}/stream-token` | Mint a 60 s token for the stream |
| `GET /runs/{id}/stream?token=` | SSE stream; resumes from `Last-Event-ID` or `?since_seq=` |
| `GET /healthz` | Liveness and database check |

Errors are always `{"error": "<code>"}`: `unauthorized` (401), `not_found` (404),
`batch_id_reused` (409), `invalid_request` (422).

## Tests

```bash
docker compose --profile tests up --build -d api api-test
docker compose run --rm tests                 # 95 tests, about 2 minutes
docker compose run --rm tests sh -c 'pytest -v -s 2>&1 | tee results/test_output.txt'
```

`api-test` is the same image with `TEST_MODE=1` (fault injection, stream kill, `/debug/*`). The
plain `api` service never has these hooks; tests check that too. After changing application
code, rebuild both services with the first command.

| File | Covers |
|---|---|
| `tests/test_consistency_e2e.py` | Browser page == database after shuffled, re-sent, late batches and four kinds of disruption |
| `tests/test_isolation.py` | Project A cannot read, write, subscribe to or guess anything of project B |
| `tests/test_silent_failures.py` | Lost events, duplicates, slow client, partial writes, key rotation, dead clients |
| `tests/test_hardening.py` | Malformed input, races, bit-exact values, invariants over the whole database |
| `tests/test_auth_runs.py`, `test_ingest.py`, `test_stream.py`, `test_ui.py` | Feature tests per component |

## Load test

```bash
sh loadtest/run.sh        # 50 runs × 10 events/s, batch 10 then batch 1, ~3.5 min → results/load.txt
```

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `ADMIN_TOKEN` | `dev-admin-token` | Token for `POST /projects`. Change it outside local use. |
| `STREAM_SECRET` | random per process | HMAC secret for stream tokens. Required with more than one worker. |
| `WEB_CONCURRENCY` | 1 | uvicorn workers |
| `DB_POOL_MAX` | 20 | Database connections per worker |
| `SSE_PING_SECONDS` | 15 | Heartbeat interval; the page reconnects after 3 missed heartbeats |
| `SSE_STALL_SECONDS` | 10 | A client that accepts no write for this long is dropped |
| `SSE_MAX_LIFETIME_SECONDS` | 300 | Every stream is ended after this long; clients resume by themselves |
| `TEST_MODE` | unset | Enables test hooks. Never set it on a real deployment. |

## Layout

```
app/        main.py auth.py routes.py ingest.py stream.py bus.py logging.py errors.py schema.sql static/
tests/      pytest suite, netproxy.py (fault-injecting TCP proxy for the browser tests)
loadtest/   load.py, run.sh
results/    test_output.txt, load.txt, load_workers4.txt, screens/
```
