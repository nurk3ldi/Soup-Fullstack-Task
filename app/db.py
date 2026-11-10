import os
from pathlib import Path

import asyncpg

SCHEMA_PATH = Path(__file__).with_name("schema.sql")
# Arbitrary constant: serializes schema application if several API processes start at once.
SCHEMA_LOCK_ID = 7301001


async def create_pool() -> asyncpg.Pool:
    return await asyncpg.create_pool(
        os.environ["DATABASE_URL"],
        min_size=2,
        max_size=int(os.environ.get("DB_POOL_MAX", "20")),
    )


async def apply_schema(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock($1)", SCHEMA_LOCK_ID)
            await conn.execute(SCHEMA_PATH.read_text())
