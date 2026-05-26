"""Fixtures for connector-service integration tests (CONN-IMPL-003).

Provides a real ``custos_pg`` adapter stack wired into the connector
FastAPI app so each test exercises the schema-revision startup gate
and ``/healthz`` + ``/readyz`` probes against Postgres. Schemas are
dropped before every test for isolation.

Resolution order for the database:

1. ``CUSTOS_PG_DSN`` env var — set this in CI (the workflow attaches
   a ``postgres:15-alpine`` service container and passes the DSN).
2. ``testcontainers[postgres]`` — local dev fallback. Skipped if
   Docker is unavailable.

Each test runs in its own asyncio loop owned by ``TestClient``; the
provider pool is opened lazily inside that loop via ``LazyPool`` so we
avoid the classic "another operation is in progress" cross-loop hazard
with ``asyncpg``.
"""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Iterable, Iterator

import pytest
from fastapi.testclient import TestClient

from custos_connector import create_app
from custos_connector.providers import Providers, load_providers
from custos_connector.settings import load_settings

# All tests in this package require a live Postgres backend. Per-module
# ``pytestmark = pytest.mark.integration`` is the supported pytest pattern
# (module-level marks propagate to every test; package ``__init__`` marks
# do not, so we set the mark explicitly here for the fixtures as well as
# in each test module).
pytestmark = pytest.mark.integration


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
    # ``connector-service-integration`` GitHub Actions job so version-specific
    # behaviour cannot slip through the local pre-merge gate.
    try:
        container = PostgresContainer("postgres:15-alpine")
        container.start()
    except Exception as exc:  # pragma: no cover - docker not available
        pytest.skip(f"could not start postgres testcontainer: {exc}")

    try:
        dsn = container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")
        yield dsn
    finally:
        container.stop()


async def _reset_and_migrate(dsn: str) -> None:
    """Drop connector-touched schemas, then run every adapter's migrations.

    Runs once per test from a single asyncpg connection. The migration
    step ensures the platform startup gate finds the required SPL
    revisions when the FastAPI app's lifespan opens its own pool.
    """
    import asyncpg
    from custos_pg import (
        PgCatalogAdapter,
        PgConnectorInstanceAdapter,
        PgLeaseAdapter,
        PgMetadataAdapter,
    )

    pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=2)
    assert pool is not None
    try:
        async with pool.acquire() as conn:
            # Drop all schemas connector-service touches plus the SPL
            # ledger so the next ``apply_pending`` re-creates everything
            # cleanly. ``catalog`` (connector-type rows) +
            # ``connector_instance`` (workspace-scoped instance rows) +
            # ``lease`` (activity-token leases) +
            # ``custos_state`` (cursors) + ``custos_meta`` (revision
            # ledger).
            await conn.execute("DROP SCHEMA IF EXISTS catalog CASCADE")
            await conn.execute("DROP SCHEMA IF EXISTS connector_instance CASCADE")
            await conn.execute("DROP SCHEMA IF EXISTS lease CASCADE")
            await conn.execute("DROP SCHEMA IF EXISTS custos_state CASCADE")
            await conn.execute("DROP SCHEMA IF EXISTS custos_meta CASCADE")
        # Migrations: each adapter records its declared revisions into
        # ``custos_meta.adapter_revisions`` which the startup gate reads
        # via ``refresh_declared``.
        await PgCatalogAdapter(pool=pool).apply_pending()
        await PgConnectorInstanceAdapter(pool=pool).apply_pending()
        await PgLeaseAdapter(pool=pool).apply_pending()
        await PgMetadataAdapter(pool=pool).apply_pending()
    finally:
        await pool.close()


@pytest.fixture
def pg_dsn(_postgres_dsn: str) -> str:
    """Per-test clean Postgres: schemas dropped + migrations re-applied."""
    asyncio.run(_reset_and_migrate(_postgres_dsn))
    return _postgres_dsn


# ---------------------------------------------------------------------------
# Providers / client
# ---------------------------------------------------------------------------


@pytest.fixture
def client(pg_dsn: str) -> Iterator[TestClient]:
    """A :class:`TestClient` mounted on a Postgres-backed connector app.

    The provider pool is opened lazily inside the FastAPI lifespan
    (TestClient's own asyncio loop) so it lives and dies with the
    same loop the routes execute on.
    """
    env: dict[str, str] = {
        "CONN_CATALOG_STORE": pg_dsn,
        "CONN_METADATA_STORE": pg_dsn,
        "CONN_CATALOG_ENDPOINT": "http://catalog-service:8080",
        # CONN_AUTHZ_ENDPOINT intentionally unset — exercises the dev shim.
    }
    settings = load_settings(env)
    providers: Providers = load_providers(settings)
    app = create_app(settings=settings, providers=providers)
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def stale_client(_postgres_dsn: str) -> Iterator[TestClient]:
    """A :class:`TestClient` mounted on a Postgres-backed connector app
    where the migration ledger has been wiped *after* the schemas were
    re-applied, so the startup gate raises ``MigrationRequired`` and
    ``/readyz`` returns 503.

    Used to assert the operator-actionable schema-gate explainer surfaces
    end-to-end through the real adapters, not just the unit-tested code
    path with fakes.
    """
    # Run migrations first so the schemas exist...
    asyncio.run(_reset_and_migrate(_postgres_dsn))
    # ...then wipe the ledger so the gate fails.
    asyncio.run(_clear_ledger(_postgres_dsn))

    env: dict[str, str] = {
        "CONN_CATALOG_STORE": _postgres_dsn,
        "CONN_METADATA_STORE": _postgres_dsn,
        "CONN_CATALOG_ENDPOINT": "http://catalog-service:8080",
    }
    settings = load_settings(env)
    providers: Providers = load_providers(settings)
    app = create_app(settings=settings, providers=providers)
    with TestClient(app) as test_client:
        yield test_client


async def _clear_ledger(dsn: str) -> None:
    import asyncpg

    conn = await asyncpg.connect(dsn=dsn)
    try:
        await conn.execute("TRUNCATE TABLE custos_meta.adapter_revisions")
    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# CallContext header helpers (mirror tests/test_callctx.py)
# ---------------------------------------------------------------------------


def callctx_header(
    *,
    workspace_id: str = "ws-1",
    principal_id: str = "alice",
    permissions: Iterable[str] = (),
) -> dict[str, str]:
    payload = {
        "workspace_id": workspace_id,
        "principal_id": principal_id,
        "permissions": sorted(set(permissions)),
    }
    return {"x-custos-callctx": json.dumps(payload)}
