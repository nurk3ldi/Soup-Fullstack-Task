-- Applied on every API start, so every statement must be idempotent.

CREATE TABLE IF NOT EXISTS projects (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name        TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS api_keys (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id  UUID NOT NULL REFERENCES projects(id),
  key_hash    BYTEA NOT NULL UNIQUE,          -- sha256(secret); plaintext never stored
  prefix      TEXT NOT NULL,                  -- first 8 chars, for display/logs only
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  revoked_at  TIMESTAMPTZ                     -- NULL = active
);

CREATE TABLE IF NOT EXISTS runs (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id  UUID NOT NULL REFERENCES projects(id),
  name        TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seq    BIGINT NOT NULL DEFAULT 0       -- server-assigned event counter per run
);
CREATE INDEX IF NOT EXISTS runs_project_created ON runs (project_id, created_at DESC);

CREATE TABLE IF NOT EXISTS batches (           -- idempotency ledger
  run_id      UUID NOT NULL REFERENCES runs(id),
  batch_id    UUID NOT NULL,                   -- client-generated
  payload_sha BYTEA NOT NULL,                  -- detect same id + different body
  accepted    INT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, batch_id)
);

CREATE TABLE IF NOT EXISTS metrics (           -- one row per data point
  run_id      UUID NOT NULL REFERENCES runs(id),
  name        TEXT NOT NULL,                   -- e.g. "loss"
  step        BIGINT NOT NULL,                 -- client training step (logical order)
  value       DOUBLE PRECISION NOT NULL,
  client_ts   TIMESTAMPTZ,
  seq         BIGINT NOT NULL,                 -- server event seq (delivery order)
  batch_id    UUID NOT NULL,
  PRIMARY KEY (run_id, name, step)             -- natural dedup key
);
CREATE INDEX IF NOT EXISTS metrics_run_seq ON metrics (run_id, seq);
