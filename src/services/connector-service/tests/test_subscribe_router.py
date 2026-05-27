"""Tests for the ``SubscribeEvents`` HTTP router (CONN-IMPL-027, Phase J).

Exercises ``POST /internal/v1/events:subscribe`` end-to-end through
:func:`create_app`. The route only reads from :class:`Settings` and the
call context — no database state — so the test harness wires the
minimum :class:`Providers` bundle the lifespan hook + middleware
require.
"""

from __future__ import annotations

import json
from dataclasses import replace

from fastapi.testclient import TestClient

from custos_connector import create_app
from custos_connector.identity import IdentityResolverRegistry
from custos_connector.middleware import CALLCTX_HEADER
from custos_connector.permissions import CONNECTOR_BIND, EVENTS_SUBSCRIBE
from custos_connector.providers import Providers
from custos_connector.settings import Settings
from tests._fakes import (
    FakeCatalogAdapter,
    FakeConnectorInstanceAdapter,
    FakeLeaseAdapter,
    FakeMetadataAdapter,
    StubPluginBinder,
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

_DAPR_SETTINGS = replace(
    _BASE_SETTINGS,
    dapr_http_endpoint="http://localhost:3500",
    dapr_pubsub_name="custos-pubsub",
    dapr_event_topic="custos.connector.events",
)

_WORKSPACE = "ws-1"


def _ctx_header(
    *,
    workspace: str = _WORKSPACE,
    permissions: list[str] | None = None,
) -> dict[str, str]:
    perms = permissions if permissions is not None else [EVENTS_SUBSCRIBE]
    return {
        CALLCTX_HEADER: json.dumps(
            {
                "workspace_id": workspace,
                "principal_id": "svc:trigger-service",
                "permissions": perms,
            }
        )
    }


def _build_providers() -> Providers:
    catalog = FakeCatalogAdapter(applied_revisions={1, 2})
    instances = FakeConnectorInstanceAdapter(applied_revisions={1})
    metadata = FakeMetadataAdapter(applied_revisions={1, 2, 3, 4})
    leases = FakeLeaseAdapter(applied_revisions={1})
    registry = IdentityResolverRegistry(resolvers=[])
    return Providers(
        catalog_store=catalog,  # type: ignore[arg-type]
        instance_store=instances,  # type: ignore[arg-type]
        lease_store=leases,  # type: ignore[arg-type]
        metadata_store=metadata,  # type: ignore[arg-type]
        identity_registry=registry,
        bind_for_step_service=build_bind_for_step_service(
            catalog_store=catalog,
            instance_store=instances,
            metadata_store=metadata,
            identity_registry=registry,
            plugin_binder=StubPluginBinder(),
        ),
        lease_manager=build_lease_manager(lease_store=leases, metadata_store=metadata),
    )


def test_subscribe_returns_metadata_when_dapr_configured() -> None:
    app = create_app(settings=_DAPR_SETTINGS, providers=_build_providers())
    with TestClient(app) as client:
        response = client.post(
            "/internal/v1/events:subscribe",
            json={"connectorInstanceIds": ["inst-a", "inst-b"]},
            headers=_ctx_header(),
        )
    assert response.status_code == 200
    body = response.json()
    assert body == {
        "pubsubName": "custos-pubsub",
        "topic": "custos.connector.events",
        "workspaceId": _WORKSPACE,
        "instanceFilter": ["inst-a", "inst-b"],
    }


def test_subscribe_returns_metadata_with_null_filter_when_omitted() -> None:
    app = create_app(settings=_DAPR_SETTINGS, providers=_build_providers())
    with TestClient(app) as client:
        response = client.post(
            "/internal/v1/events:subscribe",
            json={},
            headers=_ctx_header(),
        )
    assert response.status_code == 200
    body = response.json()
    assert body["instanceFilter"] is None
    assert body["pubsubName"] == "custos-pubsub"
    assert body["topic"] == "custos.connector.events"


def test_subscribe_returns_503_when_dapr_unconfigured() -> None:
    # The default _BASE_SETTINGS has an empty dapr_http_endpoint
    # (dev-mode :class:`NoOpEventPublisher`).
    app = create_app(settings=_BASE_SETTINGS, providers=_build_providers())
    with TestClient(app) as client:
        response = client.post(
            "/internal/v1/events:subscribe",
            json={},
            headers=_ctx_header(),
        )
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "connector.pubsub_unconfigured"


def test_subscribe_requires_call_context_header() -> None:
    app = create_app(settings=_DAPR_SETTINGS, providers=_build_providers())
    with TestClient(app) as client:
        response = client.post("/internal/v1/events:subscribe", json={})
    assert response.status_code == 401


def test_subscribe_denies_without_events_subscribe_permission() -> None:
    app = create_app(settings=_DAPR_SETTINGS, providers=_build_providers())
    with TestClient(app) as client:
        response = client.post(
            "/internal/v1/events:subscribe",
            json={},
            headers=_ctx_header(permissions=[CONNECTOR_BIND]),
        )
    assert response.status_code == 403
