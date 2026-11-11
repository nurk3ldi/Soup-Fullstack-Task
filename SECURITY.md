# Security review

A review of the service against the OWASP API Security Top 10 (2023), plus the container, the
dependencies and the repository. Done on 2026-11-11, after the functional work. Raw evidence
is in [`results/security/`](results/security/); every fix has a test in
`tests/test_security.py`, `tests/test_ratelimit.py` or `tests/test_hardening.py`.

**Scope.** The API, its static pages, the Docker image and compose file, the Python
dependencies, the git history. **Not in scope:** the host, the Docker daemon, TLS (the service
speaks plain HTTP and expects a TLS-terminating proxy in front), and anything at network level.

**Method.** Checklists from ten skills of the community repository
`mukul975/Anthropic-Cybersecurity-Skills` (not an Anthropic product; details in `AI_USAGE.md`),
applied by an AI assistant and checked by tests and by four scanners: gitleaks, Trivy,
pip-audit, hadolint. This is a review, not a penetration test by an independent party.

## Findings and fixes

| # | Finding | Severity | Status | Evidence |
|---|---|---|---|---|
| F1 | The API container ran as root, with a writable filesystem and Docker's default capabilities | High | **Fixed.** Runs as uid 10001, read-only root filesystem, all capabilities dropped, `no-new-privileges`, 256 processes and 512 MiB at most | `baseline.txt` → `after.txt`; test `…is_unprivileged_and_cannot_write…` |
| F2 | The API port was published on all interfaces (`0.0.0.0:8000`) | High, together with F3 | **Fixed.** Published on `127.0.0.1` only; `API_BIND` opts in to more | `after.txt` |
| F3 | Public default credentials: admin token `dev-admin-token`, database password `tracker` | High if exposed | **Mitigated.** Still the defaults so that `docker compose up` works, but reachable only from localhost (F2); both overridable (`ADMIN_TOKEN`, `POSTGRES_PASSWORD`); the API logs a warning at startup while the default token is in use | `after.txt`, `gitleaks.txt` |
| F4 | No request body size limit: a 10 MB body was parsed in full before being rejected (349 ms) | Medium | **Fixed.** Bodies over 2 MiB get `413 payload_too_large`, on the `Content-Length` header alone (10 ms for the same request) or, for chunked bodies, as soon as the limit is passed. Callers without a key still get 401 and their body is never read | tests `…body_over_the_limit…`, `…oversized_body…` |
| F5 | No limit on open SSE streams: one key could open any number, each costing a task and a database read every 2 s | Medium | **Fixed.** At most 100 streams per project (`SSE_MAX_STREAMS_PER_PROJECT`); the next gets `429 too_many_streams` with `Retry-After` | test `…streams_per_project_are_capped` |
| F6 | Security headers only on the two HTML pages; scripts, styles and all API responses had none; `Server: uvicorn` was sent | Low | **Fixed.** Every response carries `X-Content-Type-Options`, `X-Frame-Options`, `Referrer-Policy`, `Cross-Origin-Resource-Policy`, `Permissions-Policy` and a CSP; no `Server` header | test `…headers_on_every_kind_of_response` (26 responses on two services) |
| F7 | Dependencies were version ranges (`fastapi>=0.115,<1`), so two builds could differ | Low | **Fixed.** All 21 runtime and 4 test packages pinned to the exact versions that were tested | `requirements*.txt` |
| F8 | Two fixable HIGH vulnerabilities in the base image (zlib1g) | Low (not reachable) | **Fixed.** The image applies pending Debian security updates at build time | `trivy.txt`: 49 → 47 findings, 0 with a fix available |
| F9 | Requests were logged without the client address, so abuse could not be traced | Low | **Fixed.** Each request log line has a `client` field; still no keys or tokens | test `…records_the_client_address…` |
| F10 | A caller with no key and a broken JSON body got 422, not 401: input reached the parser before authentication | Low | **Fixed** earlier the same day: the credential is checked before the body is read | test `…401_no_matter_what_else_is_wrong` |
| F11 | No rate limiting: nothing slowed a flood of requests or of credential guesses | Medium | **Fixed, within one process.** Per project: 600 requests/s, burst 1200, then `429 rate_limited` with `Retry-After`, before the body is read. Per client address: 20 failed credentials, then 1 per second; while the allowance is used up, every request from that address that needs a credential is refused unseen | `tests/test_ratelimit.py` (6 tests) |

## Scanner results

| Tool | What it checked | Result |
|---|---|---|
| gitleaks 8.30.1 | Full git history and the working tree | 1 finding with default rules: the documented dev admin token in a README example (F3). 0 with the allowlist in `.gitleaks.toml`. No real secret was ever committed |
| pip-audit | The 21 Python packages installed in the image | No known vulnerabilities |
| Trivy | The API image, severities HIGH and CRITICAL | 0 CRITICAL. 0 in Python packages. 47 HIGH in Debian system packages (11 CVEs, mostly `util-linux` mount helpers, `ncurses`, `sqlite`, `perl-base`), **none with a fix available** |
| hadolint | The Dockerfile | No findings; three rules are disabled inline, each with its reason |

The 47 remaining image findings are in programs the API never runs (`mount`, `login`,
`nsenter`, terminal libraries). They need local code execution to be useful, and F1 removes
what they would escalate to: the process has no capabilities and cannot gain new privileges.
Moving to a distroless base image would remove most of these packages; that was not done.

## OWASP API Security Top 10

| Risk | Result | Why |
|---|---|---|
| API1 Broken object level authorization | Pass | The project comes only from the key; every query filters by it; a foreign run and a missing run are the same query and the same bytes. 12 isolation tests, five id spellings |
| API2 Broken authentication | Pass, with limits below | Keys are 256 random bits, stored as SHA-256; one 401 for every failure; guessing slowed to 1 attempt per second per address (F11); revocation reaches open streams in tens of milliseconds; stream tokens are HMAC-signed, 60 s, bound to project, run and key |
| API3 Broken object property level authorization | Pass | Responses are built field by field; request bodies are parsed into fixed models; a `project_id` sent by the client is ignored |
| API4 Unrestricted resource consumption | Pass, with limits below | Bounded: batch size, name length, page size, body size (F4), streams per project (F5), stream memory, stream lifetime, request rate per project and failed credentials per address (F11). Not bounded: stored data per project, unauthenticated requests |
| API5 Broken function level authorization | Pass | One admin function (`POST /projects`) behind its own token; an API key cannot call it and the admin token is not a key. Debug endpoints exist only with `TEST_MODE=1` |
| API6 Unrestricted access to sensitive business flows | Not applicable | No such flow (no sign-up, payment, messaging) |
| API7 Server-side request forgery | Not applicable | The server never fetches a URL |
| API8 Security misconfiguration | Pass after F1, F2, F6 | No CORS grants, no API docs endpoint, no verbose errors (36 malformed requests: 4xx, fixed error shape, never a 500), strict CSP |
| API9 Improper inventory management | Pass | One version; every route is listed in the README; test hooks return 404 on the normal service |
| API10 Unsafe consumption of APIs | Not applicable | No third-party API is called |

## What is still open

These are known and were left as they are. None is hidden by a test.

1. **Rate limiting is basic and per process.** (F11.) The limits live in the memory of one API
   process, so N workers allow N times as much, and a restart forgets them. Requests that
   need no credential (pages, `/healthz`, unknown paths) are not limited at all. The address
   is the TCP peer: behind a proxy every client shares the proxy's address unless uvicorn is
   told which proxy to trust. An address that used up its failures is refused even with a
   valid key, so one guesser can lock out everyone behind the same address while it keeps
   guessing. The default of 600 requests/s per project is above what one process can serve
   (256–316/s measured for single-point ingest), so it stops a runaway client from starving other
   projects only partly. A shared limit at the proxy is still the right long-term place.
2. **No TLS.** Keys and tokens travel in clear text unless a TLS proxy is put in front. That
   proxy should also add `Strict-Transport-Security`, which the app does not send.
3. **Default credentials still exist** (F3). They are safe only while the port stays on
   localhost. There is no check that refuses to start with them.
4. **The stream token is in the URL.** It is short-lived, bound to one run and one key, and
   redacted from the app's logs, but a proxy in front may log it. It can be replayed until it
   expires (60 s).
5. **The API key lives in the browser's `sessionStorage`.** A script injected into the page
   could read it. The CSP (no inline script, no other origins) and text-only rendering are
   what stand in the way; both are tested.
6. **Keys never expire and have no scopes.** A key is full access to its project until
   revoked. Revoking a project's last key locks the project out.
7. **Limits are per process.** The stream cap, like the stream registry, counts within one API
   process; with N workers a project can hold N × 100 streams.
8. **`GET /runs` is not paginated** and no quota limits how much one project can store.
9. **47 unfixed HIGH findings in the base image** (see above).
10. **The test service (`api-test`) has unauthenticated debug endpoints**, including recent
    log lines. It is never published to the host and only starts with the `tests` profile.
11. **The tests image runs as root** and is not hardened. It is a development tool.

## Reproducing the scans

```bash
docker run --rm -v "$PWD":/repo:ro -w /repo ghcr.io/gitleaks/gitleaks:latest git . --redact
docker run --rm -i hadolint/hadolint < Dockerfile
docker save soup-fullstack-task-api:latest -o /tmp/api.tar
docker run --rm -v /tmp:/in:ro aquasec/trivy:latest image --severity HIGH,CRITICAL --input /in/api.tar
```

The image name depends on the directory name (`docker compose images` shows it).
