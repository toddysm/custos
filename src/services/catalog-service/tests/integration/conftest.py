"""Fixtures for catalog-service integration tests.

Provides a real ``custos_pg`` adapter stack wired into the catalog
FastAPI app so each test exercises publish / extract / materialize /
registry / RPC against Postgres. Schemas are dropped before every
test for isolation.

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
from typing import Any

import pytest
from fastapi.testclient import TestClient

from custos_catalog import create_app
from custos_catalog.providers import Providers, load_providers
from custos_catalog.settings import load_settings

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

    try:
        container = PostgresContainer("postgres:16-alpine")
        container.start()
    except Exception as exc:  # pragma: no cover - docker not available
        pytest.skip(f"could not start postgres testcontainer: {exc}")

    try:
        dsn = container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")
        yield dsn
    finally:
        container.stop()


async def _reset_and_migrate(dsn: str) -> None:
    """Drop catalog-touched schemas, then run every adapter's migrations.

    Runs once per test from a single asyncpg connection. The migration
    step ensures the platform startup gate finds the required SPL
    revisions when the FastAPI app's lifespan opens its own pool.
    """
    import asyncpg
    from custos_pg import (
        PgCatalogAdapter,
        PgDefinitionAdapter,
        PgMetadataAdapter,
    )

    pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=2)
    assert pool is not None
    try:
        async with pool.acquire() as conn:
            await conn.execute("DROP SCHEMA IF EXISTS definition CASCADE")
            await conn.execute("DROP SCHEMA IF EXISTS catalog CASCADE")
            await conn.execute("DROP SCHEMA IF EXISTS custos_state CASCADE")
            await conn.execute("DROP SCHEMA IF EXISTS custos_meta CASCADE")
        # Migrations: each adapter records its declared revisions
        # into ``custos_meta.adapter_revisions`` which the startup
        # gate reads via ``refresh_declared``.
        await PgDefinitionAdapter(pool=pool).apply_pending()
        await PgCatalogAdapter(pool=pool).apply_pending()
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
    """A :class:`TestClient` mounted on a Postgres-backed catalog app.

    The provider pool is opened lazily inside the FastAPI lifespan
    (TestClient's own asyncio loop) so it lives and dies with the
    same loop the routes execute on.
    """
    env: dict[str, str] = {
        "CAT_DEFINITION_STORE": pg_dsn,
        "CAT_CATALOG_STORE": pg_dsn,
        "CAT_METADATA_STORE": pg_dsn,
        "CAT_CONNECTOR_ENDPOINT": "http://connector-service:8080",
        # CAT_AUTHZ_ENDPOINT intentionally unset — exercises the dev shim.
    }
    settings = load_settings(env)
    providers: Providers = load_providers(settings)
    app = create_app(settings=settings, providers=providers)
    with TestClient(app) as test_client:
        yield test_client


# ---------------------------------------------------------------------------
# CallContext + permission helpers (mirror tests/api/conftest.py)
# ---------------------------------------------------------------------------


ALL_PERMISSIONS: tuple[str, ...] = (
    "catalog:workflows:read",
    "catalog:workflows:write",
    "catalog:templates:read",
    "catalog:templates:write",
    "catalog:activity-types:read",
    "catalog:activity-types:write",
    "catalog:connector-types:read",
    "catalog:connector-types:write",
    "catalog:rpc:read",
)


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


def admin_header(ws: str = "ws-1") -> dict[str, str]:
    return callctx_header(
        workspace_id=ws,
        principal_id="alice",
        permissions=ALL_PERMISSIONS,
    )


# ---------------------------------------------------------------------------
# Document factories
# ---------------------------------------------------------------------------


def minimal_activity_manifest(
    namespace: str = "ws-1",
    type: str = "echo",
    version: str = "1.0.0",
) -> dict[str, Any]:
    return {
        "apiVersion": "custos.dev/v1",
        "kind": "ActivityManifest",
        "metadata": {"namespace": namespace, "type": type, "version": version},
        "spec": {
            "contractVersion": "1",
            "runtime": {
                "kind": "oci-container",
                "image": "ghcr.io/x:v1",
                "digest": "sha256:abc",
            },
        },
    }


def minimal_connector_manifest(
    type: str = "oci-registry",
    version: str = "1.0.0",
) -> dict[str, Any]:
    return {
        "apiVersion": "custos.dev/connector-manifest/v1",
        "kind": "ConnectorManifest",
        "metadata": {"type": type, "version": version},
        "spec": {
            "capabilities": ["oci.pull"],
            "target": {"kind": "oci-registry", "endpoint": "https://ghcr.io"},
            "credentials": {"authenticationType": "oidc"},
        },
    }


def minimal_workflow(
    name: str = "orders",
    ws: str = "ws-1",
    *,
    activity_ref: str = "ws-1/echo@1",
) -> dict[str, Any]:
    """Smallest workflow that passes every publish gate."""
    return {
        "apiVersion": "custos.dev/v1",
        "kind": "Workflow",
        "metadata": {"name": name, "workspace": ws},
        "spec": {
            "inputs": {"image": {"type": "string"}},
            "steps": [
                {
                    "id": "say-hi",
                    "activity": activity_ref,
                    "with": {"message": "hello"},
                },
            ],
        },
    }


def minimal_template(
    name: str = "orders-tmpl",
    ws: str = "ws-1",
    *,
    activity_ref: str = "ws-1/echo@1",
) -> dict[str, Any]:
    """Smallest template that passes every publish gate."""
    return {
        "apiVersion": "custos.dev/v1",
        "kind": "WorkflowTemplate",
        "metadata": {"name": name, "workspace": ws},
        "spec": {
            "placeholders": [
                {
                    "name": "topic",
                    "type": "string",
                    "required": False,
                    "default": "default-topic",
                },
            ],
            "workflow": {
                "inputs": {"image": {"type": "string"}},
                "steps": [
                    {
                        "id": "say-hi",
                        "activity": activity_ref,
                        "with": {"message": "hello"},
                    },
                ],
            },
        },
    }


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------


def seed_echo_activity(
    client: TestClient,
    *,
    workspace_id: str = "ws-1",
    version: str = "1.0.0",
) -> dict[str, str]:
    """Register ``<ws>/echo@<version>`` via the REST API and return the ref."""
    resp = client.post(
        f"/v1/workspaces/{workspace_id}/activity-types",
        json={"manifest": minimal_activity_manifest(namespace=workspace_id, version=version)},
        headers=admin_header(workspace_id),
    )
    assert resp.status_code == 201, resp.text
    body: dict[str, str] = resp.json()
    return body
