"""Fixtures for auth-service integration tests (AS-IMPL-028).

Wires :func:`custos_auth.create_app` to a live Postgres adapter stack
so each test exercises the FastAPI surface end-to-end (mint → verify,
grant → authorize → revoke, sign-callctx → verify-callctx, JWKS
rotation overlap, token-revoked cache eviction).

Resolution order for the database, mirroring catalog-service so
operators have one mental model for "where does the integration suite
get its Postgres":

1. ``CUSTOS_AUTH_PG_DSN`` env var — set this in CI. The
   ``auth-service-integration`` GitHub Actions job attaches a
   ``postgres:15-alpine`` service container and passes the DSN.
2. ``testcontainers[postgres]`` — local-dev fallback. The session
   skips if Docker is unavailable so contributors without a local
   Docker daemon still get a clean ``pytest -q`` run (which excludes
   ``-m integration`` by default).

Each test runs in its own asyncio loop owned by ``TestClient``; the
provider pools are opened lazily inside that loop via the adapters'
:class:`LazyPool` so we avoid the classic "another operation is in
progress" cross-loop hazard with ``asyncpg``.

Schema reset
------------

Every test drops the auth-service-touched schemas (``auth``,
``custos_state``, ``custos_meta``) and re-applies both adapters'
migrations from scratch via :func:`asyncio.run`. That gives each test
a single source of truth for its starting state — no fixture-ordering
hazards, no leaked role bindings from a prior test, no audit-outbox
rows bleeding into the next assertion. The cost is one fresh DDL pass
per test (~10ms against ``postgres:15-alpine``), which is acceptable
given the suite size.
"""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Iterable, Iterator

import pytest
from fastapi.testclient import TestClient

from custos_auth import create_app
from custos_auth.middleware.callctx import CALLCTX_HEADER
from custos_auth.providers import Providers, load_providers
from custos_auth.settings import load_settings

# ---------------------------------------------------------------------------
# DSN fixture (testcontainers / CI service container)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def _postgres_dsn() -> Iterator[str]:
    """Session-wide Postgres DSN. Per-test schema cleanup happens elsewhere."""
    dsn = os.environ.get("CUSTOS_AUTH_PG_DSN")
    if dsn:
        yield dsn
        return

    try:
        from testcontainers.postgres import PostgresContainer  # type: ignore[import-untyped]
    except ImportError:  # pragma: no cover - dev-dep gate
        pytest.skip("CUSTOS_AUTH_PG_DSN not set and testcontainers not installed")

    # Pinned to match the service container used by the
    # ``auth-service-integration`` GitHub Actions job so version-specific
    # Postgres behaviour cannot slip through the local pre-merge gate.
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
    """Drop auth-service-touched schemas and re-apply every adapter's migrations.

    Runs once per test from a fresh asyncpg connection so the FastAPI
    lifespan's schema-revision startup gate finds the required SPL
    revisions when its own pool comes up.
    """
    import asyncpg
    from custos_pg import PgAuthAdapter, PgMetadataAdapter

    pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=2)
    assert pool is not None
    try:
        async with pool.acquire() as conn:
            # `auth` — owned by PgAuthAdapter.
            # `custos_state` — owned by PgMetadataAdapter (runs, triggers,
            # connector cursors, idempotency, device-code sessions, audit
            # outbox). Auth-service writes the audit-outbox subset only.
            # `custos_meta` — the SPL revisions ledger; rebuilt by
            # `ensure_ledger` inside `apply_pending`.
            await conn.execute("DROP SCHEMA IF EXISTS auth CASCADE")
            await conn.execute("DROP SCHEMA IF EXISTS custos_state CASCADE")
            await conn.execute("DROP SCHEMA IF EXISTS custos_meta CASCADE")
        # Migrations: each adapter records its declared revisions into
        # ``custos_meta.adapter_revisions`` which the startup gate reads
        # via ``refresh_declared``.
        await PgAuthAdapter(pool=pool).apply_pending()
        await PgMetadataAdapter(pool=pool).apply_pending()

        # Seed the dev-shim caller principal so foreign-key constraints
        # on ``role_binding.bound_by`` and ``service_token.revoked_by``
        # are satisfied when the integration tests POST/DELETE from
        # ``principal_id="user-1"``. The middleware accepts the
        # ``X-Custos-Callctx`` header at face value (no callctx-verifier
        # URL wired) so the API surface trusts the header's
        # principal_id verbatim; Postgres still enforces referential
        # integrity at write time.
        async with pool.acquire() as conn:
            from datetime import UTC, datetime

            now = datetime.now(UTC)
            await conn.execute(
                """
                INSERT INTO auth.principal (
                    principal_id, kind, tenant_id, workspace_id,
                    display_name, email, disabled_at, disabled_reason, created_at
                ) VALUES ($1, 'user', NULL, NULL, $2, NULL, NULL, NULL, $3)
                ON CONFLICT (principal_id) DO NOTHING
                """,
                "user-1",
                "Integration Test Caller",
                now,
            )
    finally:
        await pool.close()


@pytest.fixture
def pg_dsn(_postgres_dsn: str) -> str:
    """Per-test clean Postgres: schemas dropped + migrations re-applied."""
    asyncio.run(_reset_and_migrate(_postgres_dsn))
    return _postgres_dsn


# ---------------------------------------------------------------------------
# Settings + providers + client
# ---------------------------------------------------------------------------


def _integration_env(pg_dsn: str) -> dict[str, str]:
    """Env map for an integration-test run of :func:`create_app`.

    * Both DSN env vars point at the same Postgres (single-database
      deployment, matching the dev defaults in
      ``deploy/helm/charts/auth-service/values.yaml``).
    * Sweeper interval ``0`` keeps the background loop out of the test
      runtime — the sweeper's behaviour is exercised by the unit suite.
    * Rotation period ``0`` switches the lifespan to the "static" JWKS
      mode (no background key rotation). The JWKS-rotation-overlap test
      calls :meth:`KeyRing.rotate` directly so it can assert the
      overlap window without racing the loop.
    * OIDC is off — the integration tests target the service-token /
      call-context contract, not the external-issuer surface.
    """
    return {
        "CUSTOS_AUTH_STORE_DSN": pg_dsn,
        "CUSTOS_AUTH_METADATA_STORE_DSN": pg_dsn,
        "CUSTOS_AUTH_TOKEN_SWEEPER_INTERVAL_SECONDS": "0",
        "CUSTOS_AUTH_CALL_CONTEXT_KEY_ROTATION": "0",
        "CUSTOS_AUTH_OIDC_ENABLED": "false",
    }


@pytest.fixture
def client(pg_dsn: str) -> Iterator[TestClient]:
    """A :class:`TestClient` mounted on a Postgres-backed auth-service app.

    The provider pools are opened lazily inside the FastAPI lifespan
    (TestClient's own asyncio loop) so they live and die with the same
    loop the routes execute on.
    """
    settings = load_settings(_integration_env(pg_dsn))
    providers: Providers = load_providers(settings)
    app = create_app(settings=settings, providers=providers)
    with TestClient(app) as test_client:
        yield test_client


# ---------------------------------------------------------------------------
# Call-context helpers (mirror tests/conftest.py — kept local so the
# integration suite is self-contained)
# ---------------------------------------------------------------------------


def callctx_header(
    *,
    principal_id: str = "user-1",
    tenant_id: str | None = None,
    workspace_id: str | None = None,
    permissions: Iterable[str] = (),
) -> dict[str, str]:
    """Build a dev-shim call-context header for integration tests.

    ``permissions`` accepts any iterable; an empty iterable produces no
    ``permissions`` key in the JSON payload (callers gate on
    ``ctx.has_permission(...)`` which treats a missing key as the empty
    set). ``tenant_id`` / ``workspace_id`` are similarly opt-in so a
    test can shape the request exactly the way an inbound call from
    the API gateway would.
    """
    payload: dict[str, object] = {"principal_id": principal_id}
    if tenant_id is not None:
        payload["tenant_id"] = tenant_id
    if workspace_id is not None:
        payload["workspace_id"] = workspace_id
    perms = sorted(set(permissions))
    if perms:
        payload["permissions"] = perms
    return {CALLCTX_HEADER: json.dumps(payload)}


def platform_admin_header() -> dict[str, str]:
    """Call-context that carries the platform-admin short-circuit."""
    return callctx_header(permissions=["platform.admin"])


def workspace_admin_header(workspace_id: str, *, principal_id: str = "user-1") -> dict[str, str]:
    """Caller scoped to a workspace with the full admin permission set."""
    return callctx_header(
        principal_id=principal_id,
        workspace_id=workspace_id,
        permissions=[
            "admin:service-account",
            "admin:role-binding",
        ],
    )
