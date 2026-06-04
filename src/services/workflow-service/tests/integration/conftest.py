"""Fixtures for the Postgres-backed workflow-service integration suite.

WF-IMPL-118 (issue #621) proves the durable wiring from WF-IMPL-114 /
116 / 117 actually persists and dedups against a *real* Postgres. The
fixtures here mirror the catalog-service integration conftest so the
two services share one operational story:

Resolution order for the database:

1. ``CUSTOS_PG_DSN`` env var — set this in CI (the
   ``workflow-service-integration`` GitHub Actions job attaches a
   ``postgres:15-alpine`` service container and passes the DSN).
2. ``testcontainers[postgres]`` — local dev fallback. The suite skips
   (never fails) when neither the env var nor Docker is available, so
   ``pytest`` stays green on a laptop with no Postgres.

``asyncpg`` pools are event-loop bound. Each async test owns its own
loop (``asyncio_mode = "auto"``), so the per-test cleanup runs in a
throwaway loop via ``asyncio.run`` and the live pools are created
*inside* the test loop through the ``metadata_provider_factory``
fixture — this avoids the classic "another operation is in progress"
cross-loop hazard.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
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
    # ``workflow-service-integration`` GitHub Actions job so
    # version-specific behaviour cannot slip through the local
    # pre-merge gate.
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
    """Drop the workflow-touched schemas, then re-apply adapter migrations.

    Runs once per test from a single asyncpg connection so every test
    starts from an empty ``custos_state`` (runs + idempotency ledger)
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
# Metadata-store provider factory
# ---------------------------------------------------------------------------


ProviderFactory = Callable[[], Awaitable[MetadataStoreProvider]]


@pytest.fixture
async def metadata_provider_factory(pg_dsn: str) -> AsyncIterator[ProviderFactory]:
    """Yield a factory that mints fresh ``PgMetadataAdapter`` providers.

    Each call opens a brand-new ``asyncpg`` pool + ``PgMetadataAdapter``
    over the same DSN — calling it twice simulates a process restart /
    HA failover, letting a test prove that a run (or idempotency entry)
    written by one provider is recovered by a completely independent
    one. The factory runs inside the test's event loop so every pool is
    loop-local; all pools are closed at teardown.
    """
    import asyncpg
    from custos_pg import PgMetadataAdapter

    pools: list[asyncpg.Pool] = []

    async def _make() -> MetadataStoreProvider:
        pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
        assert pool is not None
        pools.append(pool)
        return cast(MetadataStoreProvider, PgMetadataAdapter(pool=pool))

    try:
        yield _make
    finally:
        for pool in pools:
            await pool.close()
