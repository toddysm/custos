"""Integration tests for the cursor admin REST surface (CONN-IMPL-024, #307).

Drives ``GET /v1/workspaces/{ws}/connectors/{id}/cursor`` and the three
operator POST endpoints (``cursor:rewind``, ``pull-loop:pause``,
``pull-loop:resume``) end-to-end through ``create_app`` with the real
``custos_pg`` adapter stack so the SPL row state + the audit outbox
are exercised against Postgres.

What this covers that the unit suite cannot:

* The SPL ``rewind_cursor`` write actually persists through asyncpg
  and is observable via a follow-up ``read_cursor`` on a fresh adapter.
* The ``cursor.rewound`` audit row lands in
  ``custos_state.audit_event`` + ``custos_state.audit_outbox`` so
  Observability Service drains it (the unit suite asserts the
  ``append_audit`` call shape against the fake adapter, not the real
  table rows).
* The pull-loop pause/resume audit rows likewise hit the outbox.

The fixture builds a custom :class:`Providers` that wires a
:class:`CursorService` + :class:`PullLoopScheduler` on top of the
standard provider stack — the default ``client`` fixture in
``conftest.py`` does not wire those (full lifespan wiring of the
Listen Manager lands in CONN-IMPL-025).
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
from collections.abc import Iterator, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import asyncpg
import pytest
from custos_pg import (
    PgCatalogAdapter,
    PgConnectorInstanceAdapter,
    PgMetadataAdapter,
)
from custos_spl.ids import ConnectorInstanceId, WorkspaceId
from custos_spl.interfaces.connector_instance_store import ConnectorInstance
from custos_spl.interfaces.metadata_store import AuditFilter
from fastapi.testclient import TestClient

from custos_connector import create_app
from custos_connector.cursor import CursorService
from custos_connector.middleware import CALLCTX_HEADER
from custos_connector.permissions import ADMIN_CONNECTOR, CONNECTOR_READ
from custos_connector.providers import Providers, load_providers
from custos_connector.runtime import (
    CursorEnvelope,
    ListenMode,
    ListenResult,
)
from custos_connector.scheduler import PullLoopScheduler
from custos_connector.settings import load_settings

pytestmark = pytest.mark.integration


_WORKSPACE = "ws-admin-int"
_TYPE = "http"
_VERSION = "1.0.0"
_ENCODING = "my-encoding-v1"


# ---------------------------------------------------------------------------
# Stub invoker/publisher (the admin surface never ticks)
# ---------------------------------------------------------------------------


class _NoopInvoker:
    async def listen(
        self,
        *,
        connector: Any,
        instance: ConnectorInstance,
        mode: ListenMode,
        cursor: CursorEnvelope | None,
    ) -> ListenResult:  # pragma: no cover - admin surface never ticks
        raise AssertionError("admin surface unexpectedly invoked the plugin")


async def _noop_publisher(
    workspace_id: WorkspaceId,
    instance: ConnectorInstance,
    events: Sequence[Mapping[str, Any]],
) -> None:  # pragma: no cover - admin surface never ticks
    raise AssertionError("admin surface unexpectedly invoked the publisher")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ctx_header(
    *,
    workspace_id: str = _WORKSPACE,
    permissions: list[str] | None = None,
) -> dict[str, str]:
    perms = permissions if permissions is not None else [ADMIN_CONNECTOR, CONNECTOR_READ]
    return {
        CALLCTX_HEADER: json.dumps(
            {
                "workspace_id": workspace_id,
                "principal_id": "op:alice",
                "permissions": perms,
            }
        )
    }


def _fingerprint_json(value: Any) -> str:
    persisted = json.dumps(value, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(persisted.encode("utf-8")).hexdigest()


async def _seed_catalog_and_instance(
    *,
    pool: asyncpg.Pool,
    instance_id: str,
) -> ConnectorInstance:
    catalog = PgCatalogAdapter(pool=pool)
    instances = PgConnectorInstanceAdapter(pool=pool)

    manifest: dict[str, Any] = {
        "metadata": {"type": _TYPE, "version": _VERSION},
        "spec": {
            "capabilities": ["events.delivery"],
            "events": {"pull": {"cursorEncoding": _ENCODING}},
        },
    }
    await catalog.put_connector_type_version(
        _TYPE,
        _VERSION,
        "sha256:fake",
        f"example.test/{_TYPE}@sha256:fake",
        manifest,
    )

    now = datetime.now(UTC)
    instance = ConnectorInstance(
        workspace_id=WorkspaceId(_WORKSPACE),
        instance_id=ConnectorInstanceId(instance_id),
        type=_TYPE,
        version=_VERSION,
        name="prod",
        lease_ttl_seconds=3600,
        enabled=True,
        status="active",
        health_status="healthy",
        target_config={},
        credentials_authentication={"type": "oidc"},
        used_capabilities=("events.delivery",),
        created_at=now,
        updated_at=now,
    )
    await instances.put_connector_instance(instance.workspace_id, instance)
    return instance


async def _seed_cursor_row(
    *,
    pool: asyncpg.Pool,
    instance_id: str,
    value: Any,
) -> None:
    """Write an initial cursor row via SPL acquire/commit primitives."""
    metadata = PgMetadataAdapter(pool=pool)
    _row, lease = await metadata.acquire_cursor_lease(
        WorkspaceId(_WORKSPACE),
        ConnectorInstanceId(instance_id),
        "seed-holder",
        60,
    )
    persisted = json.dumps(value, sort_keys=True, separators=(",", ":"))
    advanced_at = datetime.now(UTC)
    await metadata.commit_cursor(WorkspaceId(_WORKSPACE), lease, persisted, advanced_at)


# ---------------------------------------------------------------------------
# Client fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def admin_client(pg_dsn: str) -> Iterator[tuple[TestClient, Providers]]:
    """A TestClient on a Postgres-backed app with CursorService + scheduler wired.

    Mirrors the standard ``client`` fixture but post-mutates the loaded
    providers to attach a :class:`CursorService` + :class:`PullLoopScheduler`
    so the cursor admin REST surface has the dependencies the four
    endpoints require.
    """
    env: dict[str, str] = {
        "CONN_CATALOG_STORE": pg_dsn,
        "CONN_METADATA_STORE": pg_dsn,
        "CONN_CATALOG_ENDPOINT": "http://catalog-service:8080",
    }
    settings = load_settings(env)
    base = load_providers(settings)
    cursor_service = CursorService(
        metadata_store=base.metadata_store,
        catalog_store=base.catalog_store,
        instance_store=base.instance_store,
        plugin_invoker=_NoopInvoker(),  # type: ignore[arg-type]
        publisher=_noop_publisher,
        holder_id="test-holder",
        lease_ttl_seconds=60,
    )
    scheduler = PullLoopScheduler(cursor_service=cursor_service)
    providers = dataclasses.replace(
        base, cursor_service=cursor_service, pull_loop_scheduler=scheduler
    )
    app = create_app(settings=settings, providers=providers)
    with TestClient(app) as client:
        yield client, providers


# ---------------------------------------------------------------------------
# GET /cursor
# ---------------------------------------------------------------------------


def test_get_cursor_returns_redacted_envelope(
    admin_client: tuple[TestClient, Providers],
    pg_dsn: str,
) -> None:
    client, _providers = admin_client
    instance_id = str(uuid4())

    async def _seed() -> None:
        pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
        assert pool is not None
        try:
            await _seed_catalog_and_instance(pool=pool, instance_id=instance_id)
            await _seed_cursor_row(pool=pool, instance_id=instance_id, value={"offset": 42})
        finally:
            await pool.close()

    import asyncio

    asyncio.run(_seed())

    resp = client.get(
        f"/v1/workspaces/{_WORKSPACE}/connectors/{instance_id}/cursor",
        headers=_ctx_header(),
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["workspaceId"] == _WORKSPACE
    assert body["instanceId"] == instance_id
    assert body["encoding"] == _ENCODING
    assert body["valueFingerprint"] == _fingerprint_json({"offset": 42})
    assert body["valueLength"] == len(
        json.dumps({"offset": 42}, sort_keys=True, separators=(",", ":")).encode("utf-8")
    )
    assert "value" not in body  # raw value MUST NOT be exposed


def test_get_cursor_returns_404_when_no_row(
    admin_client: tuple[TestClient, Providers],
    pg_dsn: str,
) -> None:
    client, _providers = admin_client
    instance_id = str(uuid4())

    async def _seed() -> None:
        pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
        assert pool is not None
        try:
            await _seed_catalog_and_instance(pool=pool, instance_id=instance_id)
        finally:
            await pool.close()

    import asyncio

    asyncio.run(_seed())

    resp = client.get(
        f"/v1/workspaces/{_WORKSPACE}/connectors/{instance_id}/cursor",
        headers=_ctx_header(),
    )
    assert resp.status_code == 404, resp.text
    assert resp.json()["detail"]["error"]["code"] == "connector.cursor.not_found"


# ---------------------------------------------------------------------------
# POST /cursor:rewind
# ---------------------------------------------------------------------------


def test_rewind_to_beginning_writes_empty_sentinel_and_audits(
    admin_client: tuple[TestClient, Providers],
    pg_dsn: str,
) -> None:
    client, _providers = admin_client
    instance_id = str(uuid4())

    async def _seed() -> None:
        pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
        assert pool is not None
        try:
            await _seed_catalog_and_instance(pool=pool, instance_id=instance_id)
            await _seed_cursor_row(pool=pool, instance_id=instance_id, value={"offset": 100})
        finally:
            await pool.close()

    import asyncio

    asyncio.run(_seed())

    resp = client.post(
        f"/v1/workspaces/{_WORKSPACE}/connectors/{instance_id}/cursor:rewind",
        headers=_ctx_header(),
        json={"to": "beginning", "reason": "replay from oldest"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    # After "beginning" the SPL row holds the empty-string sentinel,
    # which ``CursorService.read_envelope`` decodes to ``value=None``
    # (= uninitialized). The redacted wire surfaces this as a null
    # fingerprint + null length so audit consumers can distinguish
    # "never written" from "written and emptied".
    assert body["valueFingerprint"] is None
    assert body["valueLength"] is None

    # Verify the SPL row actually holds the empty sentinel by reading
    # it back through a brand-new adapter (not via the app's pool).
    async def _verify() -> None:
        pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
        assert pool is not None
        try:
            row = await PgMetadataAdapter(pool=pool).read_cursor(
                WorkspaceId(_WORKSPACE), ConnectorInstanceId(instance_id)
            )
            assert row is not None
            assert row.value == ""

            # Audit row landed in the outbox.
            page = await PgMetadataAdapter(pool=pool).query_audit(
                WorkspaceId(_WORKSPACE),
                filter=AuditFilter(event_type="cursor.rewound"),
            )
            assert len(page.items) == 1
            evt = page.items[0]
            assert evt.actor == "op:alice"
            assert evt.subject["instance_id"] == instance_id
            assert evt.payload["reason"] == "replay from oldest"
            assert evt.payload["from"]["valueFingerprint"] == _fingerprint_json({"offset": 100})
            assert evt.payload["to"]["valueFingerprint"] is None
            assert evt.payload["to"]["valueLength"] is None
            # Raw value MUST NOT appear in the audit payload.
            assert "value" not in evt.payload["from"]
            assert "value" not in evt.payload["to"]
        finally:
            await pool.close()

    asyncio.run(_verify())


def test_rewind_to_explicit_envelope_writes_json_value(
    admin_client: tuple[TestClient, Providers],
    pg_dsn: str,
) -> None:
    client, _providers = admin_client
    instance_id = str(uuid4())

    async def _seed() -> None:
        pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
        assert pool is not None
        try:
            await _seed_catalog_and_instance(pool=pool, instance_id=instance_id)
            await _seed_cursor_row(pool=pool, instance_id=instance_id, value={"offset": 100})
        finally:
            await pool.close()

    import asyncio

    asyncio.run(_seed())

    resp = client.post(
        f"/v1/workspaces/{_WORKSPACE}/connectors/{instance_id}/cursor:rewind",
        headers=_ctx_header(),
        json={
            "to": {"encoding": _ENCODING, "value": {"offset": 7}},
            "reason": "test rewind to explicit position",
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["valueFingerprint"] == _fingerprint_json({"offset": 7})

    async def _verify() -> None:
        pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
        assert pool is not None
        try:
            row = await PgMetadataAdapter(pool=pool).read_cursor(
                WorkspaceId(_WORKSPACE), ConnectorInstanceId(instance_id)
            )
            assert row is not None
            assert json.loads(row.value) == {"offset": 7}
        finally:
            await pool.close()

    asyncio.run(_verify())


def test_rewind_rejects_encoding_mismatch(
    admin_client: tuple[TestClient, Providers],
    pg_dsn: str,
) -> None:
    client, _providers = admin_client
    instance_id = str(uuid4())

    async def _seed() -> None:
        pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
        assert pool is not None
        try:
            await _seed_catalog_and_instance(pool=pool, instance_id=instance_id)
            await _seed_cursor_row(pool=pool, instance_id=instance_id, value={"offset": 100})
        finally:
            await pool.close()

    import asyncio

    asyncio.run(_seed())

    resp = client.post(
        f"/v1/workspaces/{_WORKSPACE}/connectors/{instance_id}/cursor:rewind",
        headers=_ctx_header(),
        json={
            "to": {"encoding": "wrong-encoding-v2", "value": {"offset": 7}},
            "reason": "should be rejected",
        },
    )
    assert resp.status_code == 400, resp.text
    assert resp.json()["detail"]["error"]["code"] == "connector.cursor.encoding_mismatch"


# ---------------------------------------------------------------------------
# POST /pull-loop:pause + :resume
# ---------------------------------------------------------------------------


def test_pause_and_resume_audit_rows_land(
    admin_client: tuple[TestClient, Providers],
    pg_dsn: str,
) -> None:
    client, providers = admin_client
    instance_id = str(uuid4())

    async def _seed() -> None:
        pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
        assert pool is not None
        try:
            await _seed_catalog_and_instance(pool=pool, instance_id=instance_id)
        finally:
            await pool.close()

    import asyncio

    asyncio.run(_seed())

    # Register so pause/resume actually find the schedule entry.
    scheduler = providers.pull_loop_scheduler
    assert scheduler is not None
    scheduler.register(_WORKSPACE, instance_id)

    pause_resp = client.post(
        f"/v1/workspaces/{_WORKSPACE}/connectors/{instance_id}/pull-loop:pause",
        headers=_ctx_header(),
        json={"reason": "operator pause"},
    )
    assert pause_resp.status_code == 200, pause_resp.text

    resume_resp = client.post(
        f"/v1/workspaces/{_WORKSPACE}/connectors/{instance_id}/pull-loop:resume",
        headers=_ctx_header(),
    )
    assert resume_resp.status_code == 200, resume_resp.text

    async def _verify() -> None:
        pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
        assert pool is not None
        try:
            metadata = PgMetadataAdapter(pool=pool)
            paused = await metadata.query_audit(
                WorkspaceId(_WORKSPACE),
                filter=AuditFilter(event_type="connector.pull-loop.paused"),
            )
            assert len(paused.items) == 1
            assert paused.items[0].subject["instance_id"] == instance_id
            assert paused.items[0].payload["reason"] == "operator pause"
            assert paused.items[0].actor == "op:alice"

            resumed = await metadata.query_audit(
                WorkspaceId(_WORKSPACE),
                filter=AuditFilter(event_type="connector.pull-loop.resumed"),
            )
            assert len(resumed.items) == 1
            assert resumed.items[0].subject["instance_id"] == instance_id
            assert resumed.items[0].actor == "op:alice"
        finally:
            await pool.close()

    asyncio.run(_verify())


# ---------------------------------------------------------------------------
# AuthZ gating (smoke — full coverage in unit suite)
# ---------------------------------------------------------------------------


def test_rewind_403_on_workspace_mismatch(
    admin_client: tuple[TestClient, Providers],
    pg_dsn: str,
) -> None:
    client, _providers = admin_client
    instance_id = str(uuid4())

    resp = client.post(
        f"/v1/workspaces/{_WORKSPACE}/connectors/{instance_id}/cursor:rewind",
        headers=_ctx_header(workspace_id="ws-other"),
        json={"to": "beginning", "reason": "x"},
    )
    assert resp.status_code == 403, resp.text
    assert resp.json()["detail"]["error"]["code"] == "connector.workspace_mismatch"
