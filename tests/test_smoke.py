"""Step 1 smoke checks: the stack is up and the schema is what SPEC §2 says."""
import os

import asyncpg
import httpx

API_URL = os.environ.get("API_URL", "http://localhost:8000")
DATABASE_URL = os.environ["DATABASE_URL"]


async def test_healthz():
    async with httpx.AsyncClient(base_url=API_URL) as client:
        r = await client.get("/healthz")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


async def test_schema_tables_and_keys():
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        tables = {
            r["table_name"]
            for r in await conn.fetch(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
            )
        }
        assert {"projects", "api_keys", "runs", "batches", "metrics"} <= tables

        async def pk_columns(table: str) -> list[str]:
            rows = await conn.fetch(
                """
                SELECT a.attname
                FROM pg_index i
                JOIN unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
                WHERE i.indrelid = $1::regclass AND i.indisprimary
                ORDER BY k.ord
                """,
                table,
            )
            return [r["attname"] for r in rows]

        # The dedup key and the idempotency key are what the consistency guarantees rest on.
        assert await pk_columns("metrics") == ["run_id", "name", "step"]
        assert await pk_columns("batches") == ["run_id", "batch_id"]

        indexes = {
            r["indexname"]
            for r in await conn.fetch("SELECT indexname FROM pg_indexes WHERE schemaname = 'public'")
        }
        assert "metrics_run_seq" in indexes
    finally:
        await conn.close()
