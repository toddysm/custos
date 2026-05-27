"""Unit tests for the push receiver router (CONN-IMPL-025, #308).

Drives ``POST /v1/webhooks/workspaces/{workspace_id}/connectors/{instance_id}/events`` end-to-end
through :func:`custos_connector.create_app` so the
:class:`CallContextMiddleware` bypass, the signature verifier, the
JSON parser, and the :class:`PublisherBridge` are all exercised.

The :class:`ListenManager` is wired with a fake
:class:`PullLoopScheduler` (the push tests do not tick), an
:class:`AllowAllSignatureVerifier(test_only=True)` for the happy
path, and the default :class:`RejectAllSignatureVerifier` for the
401 path.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

import pytest
from custos_spl.ids import ConnectorInstanceId, WorkspaceId
from custos_spl.interfaces.connector_instance_store import ConnectorInstance
from fastapi.testclient import TestClient

from custos_connector import create_app
from custos_connector.audit import (
    EVENT_NORMALIZED,
    EVENT_RECEIVED,
    EVENT_REJECTED,
)
from custos_connector.identity import IdentityResolverRegistry
from custos_connector.listen import (
    AllowAllSignatureVerifier,
    ListenManager,
    RecordingEventPublisher,
)
from custos_connector.providers import Providers
from custos_connector.settings import Settings
from tests._fakes import (
    FakeCatalogAdapter,
    FakeConnectorInstanceAdapter,
    FakeLeaseAdapter,
    FakeMetadataAdapter,
    build_bind_for_step_service,
    build_lease_manager,
)

pytestmark = pytest.mark.asyncio


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
_INSTANCE_ID = "inst-1"
_TYPE = "oci-registry"
_VERSION = "1.0.0"


class _FakeScheduler:
    """Minimal :class:`PullLoopScheduler` stand-in for push router tests."""

    def register(
        self,
        workspace_id: str,
        instance_id: str,
        *,
        interval_seconds: int | None = None,
    ) -> object:
        return object()

    def unregister(self, workspace_id: str, instance_id: str) -> bool:
        return True


async def _seed_type(catalog: FakeCatalogAdapter, *, delivery: list[str]) -> None:
    spec = {
        "description": "Test connector",
        "capabilities": ["c.read"],
        "target": {
            "kind": "oci-registry",
            "endpoint": "https://example.com",
            "config": {},
        },
        "credentials": {"authenticationType": "none"},
        "events": {
            "delivery": delivery,
            "produced": ["oci.image.pushed"],
        },
    }
    if "pull" in delivery:
        spec["events"]["pull"] = {  # type: ignore[index]
            "cursorEncoding": "my-enc",
            "initialCursorBehavior": "now",
        }
    await catalog.put_connector_type_version(
        type=_TYPE,
        version=_VERSION,
        digest="sha256:deadbeef",
        image_ref="ghcr.io/acme/c@sha256:deadbeef",
        normalized_manifest={"spec": spec},
    )


def _make_instance(*, instance_id: str = _INSTANCE_ID) -> ConnectorInstance:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    return ConnectorInstance(
        workspace_id=WorkspaceId(_WORKSPACE),
        instance_id=ConnectorInstanceId(instance_id),
        type=_TYPE,
        version=_VERSION,
        name="acme",
        lease_ttl_seconds=300,
        enabled=True,
        status="enabled",
        health_status="healthy",
        target_config={},
        credentials_authentication={},
        used_capabilities=(),
        created_at=now,
        updated_at=now,
    )


def _build_providers(
    *,
    catalog: FakeCatalogAdapter,
    metadata: FakeMetadataAdapter,
    listen_manager: ListenManager,
    publisher: RecordingEventPublisher,
) -> Providers:
    instances = FakeConnectorInstanceAdapter(applied_revisions={1})
    lease_store = FakeLeaseAdapter(applied_revisions={1})
    registry = IdentityResolverRegistry(resolvers=[])
    bind_service = build_bind_for_step_service(
        catalog_store=catalog,
        instance_store=instances,
        metadata_store=metadata,
        identity_registry=registry,
    )
    return Providers(
        catalog_store=catalog,  # type: ignore[arg-type]
        instance_store=instances,  # type: ignore[arg-type]
        lease_store=lease_store,  # type: ignore[arg-type]
        metadata_store=metadata,  # type: ignore[arg-type]
        identity_registry=registry,
        bind_for_step_service=bind_service,
        lease_manager=build_lease_manager(lease_store=lease_store, metadata_store=metadata),
        listen_manager=listen_manager,
        event_publisher=publisher,
    )


@contextmanager
def _make_client(providers: Providers) -> Iterator[TestClient]:
    app = create_app(settings=_BASE_SETTINGS, providers=providers)
    with TestClient(app) as client:
        yield client


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def _build_active_manager(
    *,
    catalog: FakeCatalogAdapter,
    verifier: object,
) -> ListenManager:
    manager = ListenManager(
        catalog_store=catalog,  # type: ignore[arg-type]
        scheduler=_FakeScheduler(),  # type: ignore[arg-type]
        default_signature_verifier=verifier,  # type: ignore[arg-type]
    )
    await manager.activate(_make_instance())
    return manager


async def test_post_events_happy_path_accepts_and_publishes() -> None:
    catalog = FakeCatalogAdapter()
    metadata = FakeMetadataAdapter()
    await _seed_type(catalog, delivery=["push"])
    publisher = RecordingEventPublisher()
    manager = await _build_active_manager(
        catalog=catalog,
        verifier=AllowAllSignatureVerifier(test_only=True),
    )
    providers = _build_providers(
        catalog=catalog,
        metadata=metadata,
        listen_manager=manager,
        publisher=publisher,
    )
    body: dict[str, Any] = {
        "events": [
            {"eventId": "e1", "eventType": "oci.image.pushed", "k": 1},
            {"eventId": "e2", "eventType": "oci.image.pushed", "k": 2},
        ]
    }

    with _make_client(providers) as client:
        resp = client.post(
            f"/v1/webhooks/workspaces/{_WORKSPACE}/connectors/{_INSTANCE_ID}/events",
            json=body,
        )

    assert resp.status_code == 200, resp.text
    assert resp.json() == {"accepted": 2, "rejected": 0}
    assert [e.event_id for e in publisher.published] == ["e1", "e2"]

    received = [e for _, e in metadata.append_audit_calls if e.event_type == EVENT_RECEIVED]
    assert len(received) == 1
    assert received[0].payload["eventCount"] == 2
    normalized = [e for _, e in metadata.append_audit_calls if e.event_type == EVENT_NORMALIZED]
    assert len(normalized) == 2


async def test_post_events_missing_event_id_audits_rejected() -> None:
    """Issue #308 acceptance criterion: missing eventId → event.rejected."""
    catalog = FakeCatalogAdapter()
    metadata = FakeMetadataAdapter()
    await _seed_type(catalog, delivery=["push"])
    publisher = RecordingEventPublisher()
    manager = await _build_active_manager(
        catalog=catalog,
        verifier=AllowAllSignatureVerifier(test_only=True),
    )
    providers = _build_providers(
        catalog=catalog,
        metadata=metadata,
        listen_manager=manager,
        publisher=publisher,
    )

    with _make_client(providers) as client:
        resp = client.post(
            f"/v1/webhooks/workspaces/{_WORKSPACE}/connectors/{_INSTANCE_ID}/events",
            json={
                "events": [
                    {"eventType": "oci.image.pushed"},  # no eventId
                    {"eventId": "e2", "eventType": "oci.image.pushed"},
                ]
            },
        )

    assert resp.status_code == 200
    assert resp.json() == {"accepted": 1, "rejected": 1}
    rejected = [e for _, e in metadata.append_audit_calls if e.event_type == EVENT_REJECTED]
    assert len(rejected) == 1
    assert rejected[0].payload["reason"] == "missing-event-id"


async def test_post_events_signature_rejected_returns_401() -> None:
    """Default ``RejectAllSignatureVerifier`` denies every request."""
    catalog = FakeCatalogAdapter()
    metadata = FakeMetadataAdapter()
    await _seed_type(catalog, delivery=["push"])
    publisher = RecordingEventPublisher()
    # Build the manager with the production-safe default.
    manager = ListenManager(
        catalog_store=catalog,  # type: ignore[arg-type]
        scheduler=_FakeScheduler(),  # type: ignore[arg-type]
    )
    await manager.activate(_make_instance())
    providers = _build_providers(
        catalog=catalog,
        metadata=metadata,
        listen_manager=manager,
        publisher=publisher,
    )

    with _make_client(providers) as client:
        resp = client.post(
            f"/v1/webhooks/workspaces/{_WORKSPACE}/connectors/{_INSTANCE_ID}/events",
            json={"events": [{"eventId": "e1", "eventType": "oci.image.pushed"}]},
        )

    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "webhook.signature_invalid"
    assert publisher.published == []


async def test_post_events_unknown_instance_returns_404() -> None:
    catalog = FakeCatalogAdapter()
    metadata = FakeMetadataAdapter()
    await _seed_type(catalog, delivery=["push"])
    publisher = RecordingEventPublisher()
    manager = ListenManager(
        catalog_store=catalog,  # type: ignore[arg-type]
        scheduler=_FakeScheduler(),  # type: ignore[arg-type]
        default_signature_verifier=AllowAllSignatureVerifier(test_only=True),
    )
    # NOT activating any instance.
    providers = _build_providers(
        catalog=catalog,
        metadata=metadata,
        listen_manager=manager,
        publisher=publisher,
    )

    with _make_client(providers) as client:
        resp = client.post(
            f"/v1/webhooks/workspaces/{_WORKSPACE}/connectors/nope/events",
            json={"events": [{"eventId": "e1", "eventType": "oci.image.pushed"}]},
        )

    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "connector.instance_not_listening"


async def test_post_events_pull_only_instance_returns_404() -> None:
    """Pull-only instances are not in the push registry → 404."""
    catalog = FakeCatalogAdapter()
    metadata = FakeMetadataAdapter()
    await _seed_type(catalog, delivery=["pull"])
    publisher = RecordingEventPublisher()
    manager = await _build_active_manager(
        catalog=catalog,
        verifier=AllowAllSignatureVerifier(test_only=True),
    )
    providers = _build_providers(
        catalog=catalog,
        metadata=metadata,
        listen_manager=manager,
        publisher=publisher,
    )

    with _make_client(providers) as client:
        resp = client.post(
            f"/v1/webhooks/workspaces/{_WORKSPACE}/connectors/{_INSTANCE_ID}/events",
            json={"events": [{"eventId": "e1", "eventType": "oci.image.pushed"}]},
        )

    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "connector.instance_not_listening"


async def test_post_events_malformed_json_returns_400() -> None:
    catalog = FakeCatalogAdapter()
    metadata = FakeMetadataAdapter()
    await _seed_type(catalog, delivery=["push"])
    publisher = RecordingEventPublisher()
    manager = await _build_active_manager(
        catalog=catalog,
        verifier=AllowAllSignatureVerifier(test_only=True),
    )
    providers = _build_providers(
        catalog=catalog,
        metadata=metadata,
        listen_manager=manager,
        publisher=publisher,
    )

    with _make_client(providers) as client:
        resp = client.post(
            f"/v1/webhooks/workspaces/{_WORKSPACE}/connectors/{_INSTANCE_ID}/events",
            content=b"not json {",
            headers={"content-type": "application/json"},
        )

    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "webhook.malformed"


async def test_post_events_missing_events_array_returns_400() -> None:
    catalog = FakeCatalogAdapter()
    metadata = FakeMetadataAdapter()
    await _seed_type(catalog, delivery=["push"])
    publisher = RecordingEventPublisher()
    manager = await _build_active_manager(
        catalog=catalog,
        verifier=AllowAllSignatureVerifier(test_only=True),
    )
    providers = _build_providers(
        catalog=catalog,
        metadata=metadata,
        listen_manager=manager,
        publisher=publisher,
    )

    with _make_client(providers) as client:
        resp = client.post(
            f"/v1/webhooks/workspaces/{_WORKSPACE}/connectors/{_INSTANCE_ID}/events",
            json={"not_events": []},
        )

    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "webhook.malformed"


async def test_post_events_workspace_mismatch_returns_404() -> None:
    """Routing by ``(workspace_id, instance_id)`` prevents cross-tenant delivery.

    Two workspaces that happen to reuse the same instance id (allowed
    by :class:`custos_spl.ids.ConnectorInstanceId` — opaque string,
    not globally unique) MUST NOT cross-route on the push path. The
    workspace segment in the URL is the discriminator.
    """
    catalog = FakeCatalogAdapter()
    metadata = FakeMetadataAdapter()
    await _seed_type(catalog, delivery=["push"])
    publisher = RecordingEventPublisher()
    manager = await _build_active_manager(
        catalog=catalog,
        verifier=AllowAllSignatureVerifier(test_only=True),
    )
    providers = _build_providers(
        catalog=catalog,
        metadata=metadata,
        listen_manager=manager,
        publisher=publisher,
    )

    with _make_client(providers) as client:
        # Same instance id, different workspace → 404, NOT cross-routed.
        resp = client.post(
            f"/v1/webhooks/workspaces/different-ws/connectors/{_INSTANCE_ID}/events",
            json={"events": [{"eventId": "e1", "eventType": "oci.image.pushed"}]},
        )

    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "connector.instance_not_listening"
    assert publisher.published == []


async def test_post_events_body_over_cap_returns_413() -> None:
    """The webhook path is unauthenticated until the signature step;
    a body over ``publish_max_body_mb`` MUST be refused before being
    buffered / parsed."""
    catalog = FakeCatalogAdapter()
    metadata = FakeMetadataAdapter()
    await _seed_type(catalog, delivery=["push"])
    publisher = RecordingEventPublisher()
    manager = await _build_active_manager(
        catalog=catalog,
        verifier=AllowAllSignatureVerifier(test_only=True),
    )
    providers = _build_providers(
        catalog=catalog,
        metadata=metadata,
        listen_manager=manager,
        publisher=publisher,
    )

    # Build a body just over the 4 MB cap on _BASE_SETTINGS.
    over_cap_bytes = (_BASE_SETTINGS.publish_max_body_mb * 1024 * 1024) + 1
    big = b"x" * over_cap_bytes

    with _make_client(providers) as client:
        resp = client.post(
            f"/v1/webhooks/workspaces/{_WORKSPACE}/connectors/{_INSTANCE_ID}/events",
            content=big,
            headers={"content-type": "application/octet-stream"},
        )

    assert resp.status_code == 413
    assert resp.json()["error"]["code"] == "webhook.body_too_large"
    assert publisher.published == []
    # No audit row should have been written — the cap fires before
    # we know whether the instance even exists.
    assert metadata.append_audit_calls == []
