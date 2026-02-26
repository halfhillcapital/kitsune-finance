from __future__ import annotations

import logging
import os
from pathlib import Path

import asyncpg

log = logging.getLogger(__name__)

pool: asyncpg.Pool | None = None


def get_pool() -> asyncpg.Pool:
    assert pool is not None, "Database not initialized — call init_db() first"
    return pool


async def init_db() -> None:
    global pool
    dsn = os.environ.get("DATABASE_URL", "postgresql://kitsune:kitsune@localhost:5432/kitsune")
    pool = await asyncpg.create_pool(dsn, min_size=2, max_size=10)
    schema = Path(__file__).with_name("schema.sql").read_text()
    async with pool.acquire() as conn:
        await conn.execute(schema)
    log.info("Database initialized")


async def close_db() -> None:
    global pool
    if pool:
        await pool.close()
        pool = None
        log.info("Database pool closed")
