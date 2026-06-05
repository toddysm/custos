"""Fixtures for the Postgres-backed observability-audit-service integration suite.

These fixtures prove the audit-outbox **drain → store → retention** path runs
against a *real* ``custos_pg`` Postgres adapter, mirroring the trigger-service
and workflow-service integration conftests so every service shares one
operational story.

Resolution order for the database:

1. ``CUSTOS_PG_DSN`` env var — set this in CI (the
   ``observability-audit-service-integration`` GitHub Actions job attaches a
   ``postgres:15-alpine`` service container and passes the DSN).
2. ``testcontainers[postgres]`` — local-dev fallback. The suite *skips* (never
   fails) when neither the env var nor Docker is available, so ``pytest`` stays
   green on a laptop with no Postgres.

``asyncpg`` pools are event-loop bound. Each async test owns its own loop
(``asyncio_mode = "auto"``), so the per-test schema reset runs in a throwaway
loop via ``asyncio.run`` and the live pool is created *inside* the test loop
through the async ``pg_pool`` fixture — avoiding the classic "another operation
is in progress" cross-loop hazard.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator, Iterator
from typing import cast

import pytest
from custos_spl.interfaces.metadata_store import MetadataStoreProvider

# ---------------------------------------------------------------------------
# DSN fixture (testcontainers / CI service container)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def _postgres_dsn() -> Iterator[str]:
    """Session-wide Postgres DSN. Started once, cleaned per-test elsewhere."""
    dsn = os.environ.get("CUSTOS_PG_DSN")
    if dsn:
        yield dsn
        return

    try:
        from testcontainers.postgres import PostgresContainer
    except ImportError:  # pragma: no cover - dev-dep gate
        pytest.skip("CUSTOS_PG_DSN not set and testcontainers not installed")

    # Pinned to match the service container used by the
    # ``observability-audit-service-integration`` GitHub Actions job so
    # version-specific behaviour cannot slip through the local pre-merge gate.
    try:
        container = PostgresContainer("postgres:15-alpine")
        container.start()
    except Exception as exc:  # pragma: no cover - docker not available
        pytest.skip(f"could not start postgres testcontainer: {exc}")

    try:
        dsn = container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")
        yield dsn
    finally:  # pragma: no cover - container teardown
        container.stop()


async def _reset_and_migrate(dsn: str) -> None:
    """Drop the audit-touched schemas, then re-apply adapter migrations.

    Runs once per test from a single asyncpg connection so every test starts
    from an empty ``custos_state`` (audit_event, audit_outbox, cursor ledger)
    and a freshly-migrated revision table.
    """
    import asyncpg
    from custos_pg import PgMetadataAdapter

    pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=2)
    assert pool is not None
    try:
        async with pool.acquire() as conn:
            await conn.execute("DROP SCHEMA IF EXISTS custos_state CASCADE")
            await conn.execute("DROP SCHEMA IF EXISTS custos_meta CASCADE")
        await PgMetadataAdapter(pool=pool).apply_pending()
    finally:
        await pool.close()


@pytest.fixture
def pg_dsn(_postgres_dsn: str) -> str:
    """Per-test clean Postgres: schemas dropped + migrations re-applied."""
    asyncio.run(_reset_and_migrate(_postgres_dsn))
    return _postgres_dsn


# ---------------------------------------------------------------------------
# Loop-local pool + metadata-store adapter
# ---------------------------------------------------------------------------


@pytest.fixture
async def pg_pool(pg_dsn: str) -> AsyncIterator[object]:
    """A loop-local ``asyncpg`` pool, opened inside the test's event loop."""
    import asyncpg

    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=4)
    assert pool is not None
    try:
        yield pool
    finally:
        await pool.close()


@pytest.fixture
async def metadata_store(pg_pool: object) -> MetadataStoreProvider:
    """A ``PgMetadataAdapter`` over the loop-local pool, as the SPL surface."""
    from custos_pg import PgMetadataAdapter

    return cast(MetadataStoreProvider, PgMetadataAdapter(pool=pg_pool))
