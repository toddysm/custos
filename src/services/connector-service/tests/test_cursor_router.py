"""Unit tests for the cursor admin REST surface (CONN-IMPL-024, #307).

Exercises ``GET /v1/workspaces/{ws}/connectors/{id}/cursor`` and the
three POST operator endpoints (``cursor:rewind``, ``pull-loop:pause``,
``pull-loop:resume``) end-to-end through ``create_app`` so the dev-shim
call-context middleware, the permission gates, the wire-model
translation, and the audit emission are all under test.

No Postgres: the SPL providers are :class:`FakeMetadataAdapter` /
:class:`FakeCatalogAdapter` / :class:`FakeConnectorInstanceAdapter` and
:class:`CursorService` is wired with a stub plugin invoker + publisher
(the admin surface never ticks, so they are never invoked).
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest
from custos_spl.ids import ConnectorInstanceId, WorkspaceId
from custos_spl.interfaces.catalog_store import ConnectorTypeVersion
from custos_spl.interfaces.connector_instance_store import ConnectorInstance
from fastapi.testclient import TestClient

from custos_connector import create_app
from custos_connector.audit import (
    EVENT_CURSOR_REWOUND,
    EVENT_PULL_LOOP_PAUSED,
    EVENT_PULL_LOOP_RESUMED,
)
from custos_connector.cursor.service import CursorService
from custos_connector.identity import IdentityResolverRegistry
from custos_connector.middleware import CALLCTX_HEADER
from custos_connector.permissions import (
    ADMIN_CONNECTOR,
    CONNECTOR_LEASE_MINT,
    CONNECTOR_READ,
)
from custos_connector.providers import Providers
from custos_connector.runtime import (
    CursorEnvelope,
    ListenMode,
    ListenResult,
)
from custos_connector.scheduler import PullLoopScheduler
from custos_connector.settings import Settings
from tests._fakes import (
    FakeCatalogAdapter,
    FakeConnectorInstanceAdapter,
    FakeLeaseAdapter,
    FakeMetadataAdapter,
    build_bind_for_step_service,
    build_lease_manager,
)

_BASE_SETTINGS = Settings(
    catalog_store_dsn="postgresql://u:p@h:5432/cat",
    metadata_store_dsn="postgresql://u:p@h:5432/meta",
    catalog_endpoint="http://catalog-service:8080",
    authz_endpoint="",  # dev shim
    oci_referrers_timeout_ms=5000,
    publish_max_body_mb=4,
    sidecar_default_ttl_sec=600,
    lease_max_concurrent=16,
    pull_loop_min_interval_sec=10,
    health_cache_ttl_s=60,
    sidecar_mtls_issuer=None,
    environment="development",
)

_WORKSPACE = "ws-1"
_TYPE = "http"
_VERSION = "1.0.0"
_ENCODING = "my-encoding-v1"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ctx_header(
    *,
    workspace_id: str = _WORKSPACE,
    permissions: list[str] | None = None,
) -> dict[str, str]:
    """Build a dev-shim call-context header.

    Defaults to the operator persona (`admin:connector` + `connector:read`)
    so most tests can call any endpoint without overriding permissions.
    """
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


def _make_connector_type(
    *,
    cursor_encoding: str = _ENCODING,
) -> ConnectorTypeVersion:
    return ConnectorTypeVersion(
        type=_TYPE,
        version=_VERSION,
        digest="sha256:fake",
        image_ref=f"example.test/{_TYPE}@sha256:fake",
        normalized_manifest={
            "metadata": {"type": _TYPE, "version": _VERSION},
            "spec": {
                "capabilities": ["events.delivery"],
                "events": {"pull": {"cursorEncoding": cursor_encoding}},
            },
        },
        parent_deprecated=False,
        published_at=datetime.now(UTC),
    )


def _make_instance(
    *,
    workspace_id: str = _WORKSPACE,
    instance_id: str | None = None,
) -> ConnectorInstance:
    now = datetime.now(UTC)
    return ConnectorInstance(
        workspace_id=WorkspaceId(workspace_id),
        instance_id=ConnectorInstanceId(instance_id or str(uuid4())),
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


class _NoopInvoker:
    """Plugin invoker stub. The admin surface never ticks, so any
    invocation indicates a wiring bug."""

    async def listen(
        self,
        *,
        connector: ConnectorTypeVersion,
        instance: ConnectorInstance,
        mode: ListenMode,
        cursor: CursorEnvelope | None,
    ) -> ListenResult:  # pragma: no cover - intentionally unreachable
        raise AssertionError("admin surface unexpectedly invoked the plugin")


async def _noop_publisher(
    workspace_id: WorkspaceId,
    instance: ConnectorInstance,
    events: Sequence[Mapping[str, Any]],
) -> None:  # pragma: no cover - intentionally unreachable
    raise AssertionError("admin surface unexpectedly invoked the publisher")


def _build_providers(
    *,
    metadata: FakeMetadataAdapter | None = None,
    catalog: FakeCatalogAdapter | None = None,
    instances: FakeConnectorInstanceAdapter | None = None,
    cursor_service: CursorService | None = None,
    pull_loop_scheduler: PullLoopScheduler | None = None,
) -> Providers:
    """Wire :class:`Providers` for the admin-router tests.

    Builds a full :class:`CursorService` + :class:`PullLoopScheduler`
    by default so each test only overrides the bits it cares about.
    """
    catalog_store = catalog or FakeCatalogAdapter(applied_revisions={1, 2})
    instance_store = instances or FakeConnectorInstanceAdapter(applied_revisions={1})
    metadata_store = metadata or FakeMetadataAdapter(applied_revisions={1, 2, 3, 4})
    lease_store = FakeLeaseAdapter(applied_revisions={1})
    registry = IdentityResolverRegistry(resolvers=[])
    bind_service = build_bind_for_step_service(
        catalog_store=catalog_store,
        instance_store=instance_store,
        metadata_store=metadata_store,
        identity_registry=registry,
    )
    cursor = cursor_service or CursorService(
        metadata_store=metadata_store,  # type: ignore[arg-type]
        catalog_store=catalog_store,  # type: ignore[arg-type]
        instance_store=instance_store,  # type: ignore[arg-type]
        plugin_invoker=_NoopInvoker(),  # type: ignore[arg-type]
        publisher=_noop_publisher,
        holder_id="test-holder",
        lease_ttl_seconds=60,
    )
    scheduler = pull_loop_scheduler or PullLoopScheduler(cursor_service=cursor)
    return Providers(
        catalog_store=catalog_store,  # type: ignore[arg-type]
        instance_store=instance_store,  # type: ignore[arg-type]
        lease_store=lease_store,  # type: ignore[arg-type]
        metadata_store=metadata_store,  # type: ignore[arg-type]
        identity_registry=registry,
        bind_for_step_service=bind_service,
        lease_manager=build_lease_manager(
            lease_store=lease_store,
            metadata_store=metadata_store,
        ),
        cursor_service=cursor,
        pull_loop_scheduler=scheduler,
    )


@contextmanager
def _make_client(
    *,
    providers: Providers | None = None,
    settings: Settings = _BASE_SETTINGS,
) -> Iterator[tuple[TestClient, Providers]]:
    effective = providers or _build_providers()
    app = create_app(settings=settings, providers=effective)
    with TestClient(app) as client:
        yield client, effective


async def _seed_catalog_and_instance(
    *,
    catalog: FakeCatalogAdapter,
    instances: FakeConnectorInstanceAdapter,
    cursor_encoding: str = _ENCODING,
    instance_id: str | None = None,
) -> ConnectorInstance:
    ctype = _make_connector_type(cursor_encoding=cursor_encoding)
    instance = _make_instance(instance_id=instance_id)
    await catalog.put_connector_type_version(
        ctype.type,
        ctype.version,
        ctype.digest,
        ctype.image_ref,
        ctype.normalized_manifest,
    )
    await instances.put_connector_instance(instance.workspace_id, instance)
    return instance


async def _seed_cursor_row(
    *,
    metadata: FakeMetadataAdapter,
    workspace_id: str,
    instance_id: str,
    value: Any,
) -> None:
    """Write an initial cursor row by driving the lease/commit primitive.

    ``value`` is the plugin-side JSON value; it is JSON-encoded the
    same way :class:`CursorService.tick` would encode it on the next
    commit.
    """
    _row, lease = await metadata.acquire_cursor_lease(workspace_id, instance_id, "seed-holder", 60)
    persisted = json.dumps(value, sort_keys=True, separators=(",", ":"))
    advanced_at = datetime.now(UTC) - timedelta(minutes=5)
    await metadata.commit_cursor(workspace_id, lease, persisted, advanced_at)


def _fingerprint_json(value: Any) -> str:
    persisted = json.dumps(value, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(persisted.encode("utf-8")).hexdigest()


def _events_of(metadata: FakeMetadataAdapter, event_type: str) -> list[Any]:
    return [evt for _ws, evt in metadata.append_audit_calls if evt.event_type == event_type]


# ---------------------------------------------------------------------------
# GET /cursor
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_cursor_returns_redacted_envelope() -> None:
    providers = _build_providers()
    instance = await _seed_catalog_and_instance(
        catalog=providers.catalog_store,  # type: ignore[arg-type]
        instances=providers.instance_store,  # type: ignore[arg-type]
    )
    await _seed_cursor_row(
        metadata=providers.metadata_store,  # type: ignore[arg-type]
        workspace_id=_WORKSPACE,
        instance_id=str(instance.instance_id),
        value={"offset": 42},
    )

    with _make_client(providers=providers) as (client, _):
        resp = client.get(
            f"/v1/workspaces/{_WORKSPACE}/connectors/{instance.instance_id}/cursor",
            headers=_ctx_header(permissions=[CONNECTOR_READ]),
        )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["workspaceId"] == _WORKSPACE
    assert body["instanceId"] == str(instance.instance_id)
    assert body["encoding"] == _ENCODING
    assert body["valueFingerprint"] == _fingerprint_json({"offset": 42})
    # Byte length matches the canonical JSON form.
    assert body["valueLength"] == len(
        json.dumps({"offset": 42}, sort_keys=True, separators=(",", ":")).encode("utf-8")
    )
    # Redaction: the raw value MUST NOT appear anywhere in the response.
    assert "value" not in body
    assert "42" not in resp.text
    assert "offset" not in resp.text


@pytest.mark.asyncio
async def test_get_cursor_404_when_no_row_yet() -> None:
    providers = _build_providers()
    instance = await _seed_catalog_and_instance(
        catalog=providers.catalog_store,  # type: ignore[arg-type]
        instances=providers.instance_store,  # type: ignore[arg-type]
    )
    with _make_client(providers=providers) as (client, _):
        resp = client.get(
            f"/v1/workspaces/{_WORKSPACE}/connectors/{instance.instance_id}/cursor",
            headers=_ctx_header(permissions=[CONNECTOR_READ]),
        )
    assert resp.status_code == 404, resp.text
    assert resp.json()["detail"]["error"]["code"] == "connector.cursor.not_found"


@pytest.mark.asyncio
async def test_get_cursor_403_without_read_permission() -> None:
    providers = _build_providers()
    instance = await _seed_catalog_and_instance(
        catalog=providers.catalog_store,  # type: ignore[arg-type]
        instances=providers.instance_store,  # type: ignore[arg-type]
    )
    with _make_client(providers=providers) as (client, _):
        resp = client.get(
            f"/v1/workspaces/{_WORKSPACE}/connectors/{instance.instance_id}/cursor",
            headers=_ctx_header(permissions=[CONNECTOR_LEASE_MINT]),
        )
    assert resp.status_code == 403, resp.text


@pytest.mark.asyncio
async def test_get_cursor_403_when_path_workspace_does_not_match_ctx() -> None:
    providers = _build_providers()
    instance = await _seed_catalog_and_instance(
        catalog=providers.catalog_store,  # type: ignore[arg-type]
        instances=providers.instance_store,  # type: ignore[arg-type]
    )
    with _make_client(providers=providers) as (client, _):
        resp = client.get(
            f"/v1/workspaces/other-ws/connectors/{instance.instance_id}/cursor",
            headers=_ctx_header(workspace_id=_WORKSPACE, permissions=[CONNECTOR_READ]),
        )
    assert resp.status_code == 403, resp.text
    assert resp.json()["detail"]["error"]["code"] == "connector.workspace_mismatch"


# ---------------------------------------------------------------------------
# POST /cursor:rewind
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rewind_to_beginning_writes_empty_sentinel() -> None:
    providers = _build_providers()
    instance = await _seed_catalog_and_instance(
        catalog=providers.catalog_store,  # type: ignore[arg-type]
        instances=providers.instance_store,  # type: ignore[arg-type]
    )
    await _seed_cursor_row(
        metadata=providers.metadata_store,  # type: ignore[arg-type]
        workspace_id=_WORKSPACE,
        instance_id=str(instance.instance_id),
        value={"offset": 42},
    )

    with _make_client(providers=providers) as (client, _):
        resp = client.post(
            f"/v1/workspaces/{_WORKSPACE}/connectors/{instance.instance_id}/cursor:rewind",
            json={"to": "beginning", "reason": "replay after upstream backfill"},
            headers=_ctx_header(),
        )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["valueFingerprint"] is None
    assert body["valueLength"] is None

    # SPL row was rewound to the empty sentinel.
    metadata = providers.metadata_store
    row = await metadata.read_cursor(
        WorkspaceId(_WORKSPACE), ConnectorInstanceId(str(instance.instance_id))
    )
    assert row is not None
    assert row.value == ""
    assert row.lease_holder is None

    # ``cursor.rewound`` audit event emitted with redacted envelopes.
    rewound = _events_of(providers.metadata_store, EVENT_CURSOR_REWOUND)  # type: ignore[arg-type]
    assert len(rewound) == 1
    evt = rewound[0]
    assert evt.actor == "op:alice"
    assert evt.payload["reason"] == "replay after upstream backfill"
    assert evt.payload["from"]["encoding"] == _ENCODING
    assert evt.payload["from"]["valueFingerprint"] == _fingerprint_json({"offset": 42})
    assert evt.payload["to"]["encoding"] == _ENCODING
    assert evt.payload["to"]["valueFingerprint"] is None
    # No raw ``value`` field anywhere in the payload.
    assert "value" not in evt.payload["from"]
    assert "value" not in evt.payload["to"]


@pytest.mark.asyncio
async def test_rewind_to_now_writes_iso_timestamp() -> None:
    providers = _build_providers()
    instance = await _seed_catalog_and_instance(
        catalog=providers.catalog_store,  # type: ignore[arg-type]
        instances=providers.instance_store,  # type: ignore[arg-type]
    )
    await _seed_cursor_row(
        metadata=providers.metadata_store,  # type: ignore[arg-type]
        workspace_id=_WORKSPACE,
        instance_id=str(instance.instance_id),
        value={"offset": 1},
    )

    with _make_client(providers=providers) as (client, _):
        resp = client.post(
            f"/v1/workspaces/{_WORKSPACE}/connectors/{instance.instance_id}/cursor:rewind",
            json={"to": "now", "reason": "skip forward past backfill"},
            headers=_ctx_header(),
        )
    assert resp.status_code == 200, resp.text

    # Persisted value is a JSON-encoded ISO timestamp string.
    row = await providers.metadata_store.read_cursor(
        WorkspaceId(_WORKSPACE), ConnectorInstanceId(str(instance.instance_id))
    )
    assert row is not None
    decoded = json.loads(row.value)
    assert isinstance(decoded, str)
    # Round-trip: the value is a valid UTC RFC 3339 timestamp.
    parsed = datetime.fromisoformat(decoded)
    assert parsed.tzinfo is not None


@pytest.mark.asyncio
async def test_rewind_to_explicit_envelope_writes_value() -> None:
    providers = _build_providers()
    instance = await _seed_catalog_and_instance(
        catalog=providers.catalog_store,  # type: ignore[arg-type]
        instances=providers.instance_store,  # type: ignore[arg-type]
    )
    await _seed_cursor_row(
        metadata=providers.metadata_store,  # type: ignore[arg-type]
        workspace_id=_WORKSPACE,
        instance_id=str(instance.instance_id),
        value={"offset": 42},
    )

    with _make_client(providers=providers) as (client, _):
        resp = client.post(
            f"/v1/workspaces/{_WORKSPACE}/connectors/{instance.instance_id}/cursor:rewind",
            json={
                "to": {"encoding": _ENCODING, "value": {"offset": 100}},
                "reason": "operator-driven manual replay",
            },
            headers=_ctx_header(),
        )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["valueFingerprint"] == _fingerprint_json({"offset": 100})

    row = await providers.metadata_store.read_cursor(
        WorkspaceId(_WORKSPACE), ConnectorInstanceId(str(instance.instance_id))
    )
    assert row is not None
    assert json.loads(row.value) == {"offset": 100}


@pytest.mark.asyncio
async def test_rewind_rejects_encoding_mismatch_400() -> None:
    providers = _build_providers()
    instance = await _seed_catalog_and_instance(
        catalog=providers.catalog_store,  # type: ignore[arg-type]
        instances=providers.instance_store,  # type: ignore[arg-type]
    )
    await _seed_cursor_row(
        metadata=providers.metadata_store,  # type: ignore[arg-type]
        workspace_id=_WORKSPACE,
        instance_id=str(instance.instance_id),
        value={"offset": 1},
    )

    with _make_client(providers=providers) as (client, _):
        resp = client.post(
            f"/v1/workspaces/{_WORKSPACE}/connectors/{instance.instance_id}/cursor:rewind",
            json={
                "to": {"encoding": "different-encoding-v2", "value": {"offset": 9}},
                "reason": "bad envelope",
            },
            headers=_ctx_header(),
        )
    assert resp.status_code == 400, resp.text
    assert resp.json()["detail"]["error"]["code"] == "connector.cursor.encoding_mismatch"
    # No ``cursor.rewound`` audit emitted on the rejected path.
    assert _events_of(providers.metadata_store, EVENT_CURSOR_REWOUND) == []  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_rewind_missing_reason_returns_422() -> None:
    providers = _build_providers()
    instance = await _seed_catalog_and_instance(
        catalog=providers.catalog_store,  # type: ignore[arg-type]
        instances=providers.instance_store,  # type: ignore[arg-type]
    )
    with _make_client(providers=providers) as (client, _):
        resp = client.post(
            f"/v1/workspaces/{_WORKSPACE}/connectors/{instance.instance_id}/cursor:rewind",
            json={"to": "beginning"},
            headers=_ctx_header(),
        )
    assert resp.status_code == 422, resp.text
    assert resp.json()["error"]["code"] == "invalid-request"


@pytest.mark.asyncio
async def test_rewind_404_when_no_cursor_row_yet() -> None:
    providers = _build_providers()
    instance = await _seed_catalog_and_instance(
        catalog=providers.catalog_store,  # type: ignore[arg-type]
        instances=providers.instance_store,  # type: ignore[arg-type]
    )
    with _make_client(providers=providers) as (client, _):
        resp = client.post(
            f"/v1/workspaces/{_WORKSPACE}/connectors/{instance.instance_id}/cursor:rewind",
            json={"to": "beginning", "reason": "no-op"},
            headers=_ctx_header(),
        )
    assert resp.status_code == 404, resp.text
    assert resp.json()["detail"]["error"]["code"] == "connector.cursor.not_found"


@pytest.mark.asyncio
async def test_rewind_403_without_admin_permission() -> None:
    providers = _build_providers()
    instance = await _seed_catalog_and_instance(
        catalog=providers.catalog_store,  # type: ignore[arg-type]
        instances=providers.instance_store,  # type: ignore[arg-type]
    )
    with _make_client(providers=providers) as (client, _):
        resp = client.post(
            f"/v1/workspaces/{_WORKSPACE}/connectors/{instance.instance_id}/cursor:rewind",
            json={"to": "beginning", "reason": "no-op"},
            headers=_ctx_header(permissions=[CONNECTOR_READ]),
        )
    assert resp.status_code == 403, resp.text


@pytest.mark.asyncio
async def test_rewind_403_when_workspace_path_mismatch() -> None:
    providers = _build_providers()
    instance = await _seed_catalog_and_instance(
        catalog=providers.catalog_store,  # type: ignore[arg-type]
        instances=providers.instance_store,  # type: ignore[arg-type]
    )
    with _make_client(providers=providers) as (client, _):
        resp = client.post(
            f"/v1/workspaces/other-ws/connectors/{instance.instance_id}/cursor:rewind",
            json={"to": "beginning", "reason": "no-op"},
            headers=_ctx_header(workspace_id=_WORKSPACE),
        )
    assert resp.status_code == 403, resp.text
    assert resp.json()["detail"]["error"]["code"] == "connector.workspace_mismatch"


# ---------------------------------------------------------------------------
# POST /pull-loop:pause and :resume
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pause_pull_loop_marks_registered_instance_paused() -> None:
    providers = _build_providers()
    instance = await _seed_catalog_and_instance(
        catalog=providers.catalog_store,  # type: ignore[arg-type]
        instances=providers.instance_store,  # type: ignore[arg-type]
    )
    scheduler = providers.pull_loop_scheduler
    assert scheduler is not None
    scheduler.register(_WORKSPACE, str(instance.instance_id))

    with _make_client(providers=providers) as (client, _):
        resp = client.post(
            f"/v1/workspaces/{_WORKSPACE}/connectors/{instance.instance_id}/pull-loop:pause",
            json={"reason": "investigating duplicate emission"},
            headers=_ctx_header(),
        )
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"status": "paused"}

    sched = scheduler.schedules()[(_WORKSPACE, str(instance.instance_id))]
    assert sched.paused is True

    paused_events = _events_of(providers.metadata_store, EVENT_PULL_LOOP_PAUSED)  # type: ignore[arg-type]
    assert len(paused_events) == 1
    evt = paused_events[0]
    assert evt.actor == "op:alice"
    assert evt.payload["reason"] == "investigating duplicate emission"


@pytest.mark.asyncio
async def test_pause_pull_loop_without_body_emits_audit_with_null_reason() -> None:
    providers = _build_providers()
    instance = await _seed_catalog_and_instance(
        catalog=providers.catalog_store,  # type: ignore[arg-type]
        instances=providers.instance_store,  # type: ignore[arg-type]
    )
    scheduler = providers.pull_loop_scheduler
    assert scheduler is not None
    scheduler.register(_WORKSPACE, str(instance.instance_id))

    with _make_client(providers=providers) as (client, _):
        resp = client.post(
            f"/v1/workspaces/{_WORKSPACE}/connectors/{instance.instance_id}/pull-loop:pause",
            headers=_ctx_header(),
        )
    assert resp.status_code == 200, resp.text
    paused_events = _events_of(providers.metadata_store, EVENT_PULL_LOOP_PAUSED)  # type: ignore[arg-type]
    assert len(paused_events) == 1
    assert paused_events[0].payload["reason"] is None


@pytest.mark.asyncio
async def test_pause_pull_loop_idempotent_on_unregistered_instance() -> None:
    """Per design the endpoint MUST be idempotent: pausing an instance
    the scheduler has never seen still emits a ``connector.pull-loop.paused``
    audit event so the operator action is recorded.
    """
    providers = _build_providers()
    instance = await _seed_catalog_and_instance(
        catalog=providers.catalog_store,  # type: ignore[arg-type]
        instances=providers.instance_store,  # type: ignore[arg-type]
    )
    with _make_client(providers=providers) as (client, _):
        resp = client.post(
            f"/v1/workspaces/{_WORKSPACE}/connectors/{instance.instance_id}/pull-loop:pause",
            json={"reason": "preemptive pause before first tick"},
            headers=_ctx_header(),
        )
    assert resp.status_code == 200, resp.text
    paused_events = _events_of(providers.metadata_store, EVENT_PULL_LOOP_PAUSED)  # type: ignore[arg-type]
    assert len(paused_events) == 1


@pytest.mark.asyncio
async def test_resume_pull_loop_clears_paused_and_halt_state() -> None:
    providers = _build_providers()
    instance = await _seed_catalog_and_instance(
        catalog=providers.catalog_store,  # type: ignore[arg-type]
        instances=providers.instance_store,  # type: ignore[arg-type]
    )
    scheduler = providers.pull_loop_scheduler
    assert scheduler is not None
    scheduler.register(_WORKSPACE, str(instance.instance_id))
    scheduler.pause(_WORKSPACE, str(instance.instance_id))

    with _make_client(providers=providers) as (client, _):
        resp = client.post(
            f"/v1/workspaces/{_WORKSPACE}/connectors/{instance.instance_id}/pull-loop:resume",
            headers=_ctx_header(),
        )
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"status": "resumed"}

    sched = scheduler.schedules()[(_WORKSPACE, str(instance.instance_id))]
    assert sched.paused is False

    resumed_events = _events_of(providers.metadata_store, EVENT_PULL_LOOP_RESUMED)  # type: ignore[arg-type]
    assert len(resumed_events) == 1
    assert resumed_events[0].actor == "op:alice"
    # Resume payload carries no operator-supplied reason field.
    assert resumed_events[0].payload == {}


@pytest.mark.asyncio
async def test_resume_pull_loop_idempotent_on_unregistered_instance() -> None:
    providers = _build_providers()
    instance = await _seed_catalog_and_instance(
        catalog=providers.catalog_store,  # type: ignore[arg-type]
        instances=providers.instance_store,  # type: ignore[arg-type]
    )
    with _make_client(providers=providers) as (client, _):
        resp = client.post(
            f"/v1/workspaces/{_WORKSPACE}/connectors/{instance.instance_id}/pull-loop:resume",
            headers=_ctx_header(),
        )
    assert resp.status_code == 200, resp.text
    resumed_events = _events_of(providers.metadata_store, EVENT_PULL_LOOP_RESUMED)  # type: ignore[arg-type]
    assert len(resumed_events) == 1


@pytest.mark.asyncio
async def test_pause_403_without_admin_permission() -> None:
    providers = _build_providers()
    instance = await _seed_catalog_and_instance(
        catalog=providers.catalog_store,  # type: ignore[arg-type]
        instances=providers.instance_store,  # type: ignore[arg-type]
    )
    with _make_client(providers=providers) as (client, _):
        resp = client.post(
            f"/v1/workspaces/{_WORKSPACE}/connectors/{instance.instance_id}/pull-loop:pause",
            headers=_ctx_header(permissions=[CONNECTOR_READ]),
        )
    assert resp.status_code == 403, resp.text


@pytest.mark.asyncio
async def test_resume_403_when_workspace_path_mismatch() -> None:
    providers = _build_providers()
    instance = await _seed_catalog_and_instance(
        catalog=providers.catalog_store,  # type: ignore[arg-type]
        instances=providers.instance_store,  # type: ignore[arg-type]
    )
    with _make_client(providers=providers) as (client, _):
        resp = client.post(
            f"/v1/workspaces/other-ws/connectors/{instance.instance_id}/pull-loop:resume",
            headers=_ctx_header(workspace_id=_WORKSPACE),
        )
    assert resp.status_code == 403, resp.text
    assert resp.json()["detail"]["error"]["code"] == "connector.workspace_mismatch"
