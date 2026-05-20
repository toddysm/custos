"""Shared test fixtures.

Integration tests are skipped unless either:
  - `CUSTOS_PG_DSN` is set (we use that), or
  - `testcontainers` can spin up a Postgres container locally.

Unit tests do not need either and run unconditionally.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

import pytest
import pytest_asyncio

if TYPE_CHECKING:
    from asyncpg.pool import Pool


@pytest_asyncio.fixture()
async def pg_pool() -> AsyncIterator[Pool]:
    """A connected `asyncpg.Pool` for integration tests.

    Resolution order:
      1. `CUSTOS_PG_DSN` env var (preferred for CI with an external Postgres).
      2. `testcontainers[postgres]` container (local dev).

    The fixture truncates known schemas before each test so adapter
    integration tests are isolated.
    """
    import asyncpg

    dsn = os.environ.get("CUSTOS_PG_DSN")
    container = None
    if not dsn:
        try:
            from testcontainers.postgres import PostgresContainer
        except ImportError:  # pragma: no cover - testcontainers missing
            pytest.skip("CUSTOS_PG_DSN not set and testcontainers not installed")
        try:
            container = PostgresContainer("postgres:16-alpine")
            container.start()
        except Exception as exc:  # pragma: no cover - docker not available
            pytest.skip(f"could not start postgres testcontainer: {exc}")
        dsn = container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")

    pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=4)
    assert pool is not None
    try:
        # Reset any prior state. Adapters create the schemas they need
        # on `apply_pending`, so dropping is the simplest clean slate.
        async with pool.acquire() as conn:
            await conn.execute("DROP SCHEMA IF EXISTS definition CASCADE")
            await conn.execute("DROP SCHEMA IF EXISTS catalog CASCADE")
            await conn.execute("DROP SCHEMA IF EXISTS custos_meta CASCADE")
        yield pool
    finally:
        await pool.close()
        if container is not None:
            container.stop()
