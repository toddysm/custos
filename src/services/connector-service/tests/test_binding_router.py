"""Tests for the ``BindForStep`` HTTP router (CONN-IMPL-016, Phase G).

Exercises the FastAPI route ``POST /internal/v1/bind-for-step`` end-to-end
through ``create_app`` so the dev-shim call-context middleware, the
``connector:bind`` permission gate, and the wire-model translation
between camelCase JSON and the service's snake-case dataclasses are
all under test.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, ClassVar
from uuid import uuid4

from custos_spl.ids import ConnectorInstanceId, WorkspaceId
from custos_spl.interfaces.catalog_store import ConnectorTypeVersion
from custos_spl.interfaces.connector_instance_store import ConnectorInstance
from fastapi.testclient import TestClient

from custos_connector import create_app
from custos_connector.identity import (
    IdentityResolverRegistry,
    ResolvedIdentity,
)
from custos_connector.identity.protocols import IdentityResolverContext
from custos_connector.loader.identity import IdentityCategory
from custos_connector.middleware import CALLCTX_HEADER
from custos_connector.permissions import CONNECTOR_BIND, CONNECTOR_READ
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


_WORKSPACE = "ws-1"


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


class _StubResolver:
    """Always-succeeds oidc resolver for router tests."""

    authentication_type: ClassVar[str] = "oidc"
    category: ClassVar[IdentityCategory] = IdentityCategory.FEDERATED

    async def resolve(
        self,
        *,
        credentials_authentication: Mapping[str, Any],
        context: IdentityResolverContext,
    ) -> ResolvedIdentity:
        del credentials_authentication, context
        now = datetime.now(UTC)
        return ResolvedIdentity.build(
            authentication_type=self.authentication_type,
            category=self.category,
            material={"token": "stub", "type": "Bearer"},
            descriptor="oidc:stub",
            issued_at=now,
            expires_at=None,
        )


def _make_connector_type(
    *,
    capabilities: list[str | dict[str, Any]] | None = None,
) -> ConnectorTypeVersion:
    caps = capabilities if capabilities is not None else ["events.delivery", "objects.read"]
    return ConnectorTypeVersion(
        type="http",
        version="1.0.0",
        digest="sha256:fake",
        image_ref="example.test/http@sha256:fake",
        normalized_manifest={
            "metadata": {"type": "http", "version": "1.0.0"},
            "spec": {"capabilities": caps},
        },
        parent_deprecated=False,
        published_at=datetime.now(UTC),
    )


def _make_instance(
    *,
    used_capabilities: tuple[str, ...] = ("events.delivery",),
    enabled: bool = True,
    health_status: str | None = "healthy",
) -> ConnectorInstance:
    now = datetime.now(UTC)
    return ConnectorInstance(
        workspace_id=WorkspaceId(_WORKSPACE),
        instance_id=ConnectorInstanceId(str(uuid4())),
        type="http",
        version="1.0.0",
        name="prod",
        lease_ttl_seconds=3600,
        enabled=enabled,
        status="active",
        health_status=health_status,
        target_config={},
        credentials_authentication={"type": "oidc"},
        used_capabilities=used_capabilities,
        created_at=now,
        updated_at=now,
    )


def _ctx_header(*, permissions: list[str] | None = None) -> dict[str, str]:
    perms = permissions if permissions is not None else [CONNECTOR_BIND]
    return {
        CALLCTX_HEADER: json.dumps(
            {
                "workspace_id": _WORKSPACE,
                "principal_id": "svc:workflow-service",
                "permissions": perms,
            }
        )
    }


def _build_providers(
    *,
    catalog: FakeCatalogAdapter | None = None,
    instances: FakeConnectorInstanceAdapter | None = None,
    metadata: FakeMetadataAdapter | None = None,
    plugin_binder: StubPluginBinder | None = None,
) -> Providers:
    catalog = catalog or FakeCatalogAdapter(applied_revisions={1, 2})
    instances = instances or FakeConnectorInstanceAdapter(applied_revisions={1})
    metadata = metadata or FakeMetadataAdapter(applied_revisions={1, 2, 3, 4})
    leases = FakeLeaseAdapter(applied_revisions={1})
    registry = IdentityResolverRegistry(resolvers=[_StubResolver()])
    binder = plugin_binder or StubPluginBinder()
    service = build_bind_for_step_service(
        catalog_store=catalog,
        instance_store=instances,
        metadata_store=metadata,
        identity_registry=registry,
        plugin_binder=binder,
    )
    return Providers(
        catalog_store=catalog,  # type: ignore[arg-type]
        instance_store=instances,  # type: ignore[arg-type]
        lease_store=leases,  # type: ignore[arg-type]
        metadata_store=metadata,  # type: ignore[arg-type]
        identity_registry=registry,
        bind_for_step_service=service,
        lease_manager=build_lease_manager(lease_store=leases, metadata_store=metadata),
    )


def _payload(
    instance: ConnectorInstance,
    *,
    slot_name: str = "source",
    required_capabilities: list[str] | None = None,
    run_id: str = "run-1",
    step_id: str = "step-1",
    attempt: int = 1,
) -> dict[str, Any]:
    caps = required_capabilities if required_capabilities is not None else ["events.delivery"]
    return {
        "runId": run_id,
        "stepId": step_id,
        "attempt": attempt,
        "stepKey": "copy.v1",
        "slots": [
            {
                "name": slot_name,
                "instanceId": str(instance.instance_id),
                "requiredCapabilities": caps,
            }
        ],
    }


async def _seed_catalog(catalog: FakeCatalogAdapter) -> None:
    await catalog.put_connector_type_version(
        "http",
        "1.0.0",
        "sha256:fake",
        "example.test/http@sha256:fake",
        {
            "metadata": {"type": "http", "version": "1.0.0"},
            "spec": {"capabilities": ["events.delivery", "objects.read"]},
        },
    )


async def _seed(
    catalog: FakeCatalogAdapter,
    instances: FakeConnectorInstanceAdapter,
    instance: ConnectorInstance,
) -> None:
    await _seed_catalog(catalog)
    await instances.put_connector_instance(instance.workspace_id, instance)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_bind_for_step_returns_200_with_contexts() -> None:
    catalog = FakeCatalogAdapter(applied_revisions={1, 2})
    instances = FakeConnectorInstanceAdapter(applied_revisions={1})
    metadata = FakeMetadataAdapter(applied_revisions={1, 2, 3, 4})
    instance = _make_instance(used_capabilities=("events.delivery",))
    asyncio.run(_seed(catalog, instances, instance))

    providers = _build_providers(
        catalog=catalog,
        instances=instances,
        metadata=metadata,
    )

    app = create_app(settings=_BASE_SETTINGS, providers=providers)
    with TestClient(app) as client:
        resp = client.post(
            "/internal/v1/bind-for-step",
            json=_payload(instance),
            headers=_ctx_header(),
        )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "contexts" in body
    assert "source" in body["contexts"]
    ctx = body["contexts"]["source"]
    assert ctx["endpoint"] == "stub://source"
    assert ctx["handle"]["slot"] == "source"


# ---------------------------------------------------------------------------
# Auth / call-context paths
# ---------------------------------------------------------------------------


def test_bind_for_step_requires_call_context_header() -> None:
    app = create_app(settings=_BASE_SETTINGS, providers=_build_providers())
    instance = _make_instance()
    with TestClient(app) as client:
        resp = client.post(
            "/internal/v1/bind-for-step",
            json=_payload(instance),
        )
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "callctx_missing"


def test_bind_for_step_denies_without_connector_bind_permission() -> None:
    app = create_app(settings=_BASE_SETTINGS, providers=_build_providers())
    instance = _make_instance()
    with TestClient(app) as client:
        resp = client.post(
            "/internal/v1/bind-for-step",
            json=_payload(instance),
            headers=_ctx_header(permissions=[CONNECTOR_READ]),
        )
    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "permission_denied"
    assert CONNECTOR_BIND in resp.json()["error"]["detail"]


# ---------------------------------------------------------------------------
# Error mapping
# ---------------------------------------------------------------------------


def test_bind_for_step_returns_404_when_instance_unknown() -> None:
    app = create_app(settings=_BASE_SETTINGS, providers=_build_providers())
    # Instance not seeded into store → instance-not-found.
    placeholder = _make_instance()
    with TestClient(app) as client:
        resp = client.post(
            "/internal/v1/bind-for-step",
            json=_payload(placeholder),
            headers=_ctx_header(),
        )
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "instance-not-found"


def test_bind_for_step_returns_503_when_instance_disabled() -> None:
    catalog = FakeCatalogAdapter(applied_revisions={1, 2})
    instances = FakeConnectorInstanceAdapter(applied_revisions={1})
    instance = _make_instance(enabled=False)
    asyncio.run(_seed(catalog, instances, instance))
    providers = _build_providers(catalog=catalog, instances=instances)
    app = create_app(settings=_BASE_SETTINGS, providers=providers)
    with TestClient(app) as client:
        resp = client.post(
            "/internal/v1/bind-for-step",
            json=_payload(instance),
            headers=_ctx_header(),
        )
    assert resp.status_code == 503
    assert resp.json()["error"]["code"] == "instance-disabled"


def test_bind_for_step_returns_412_on_capability_shortfall() -> None:
    catalog = FakeCatalogAdapter(applied_revisions={1, 2})
    instances = FakeConnectorInstanceAdapter(applied_revisions={1})
    # Instance lacks objects.read in used_capabilities.
    instance = _make_instance(used_capabilities=("events.delivery",))
    asyncio.run(_seed(catalog, instances, instance))
    providers = _build_providers(catalog=catalog, instances=instances)
    app = create_app(settings=_BASE_SETTINGS, providers=providers)
    with TestClient(app) as client:
        resp = client.post(
            "/internal/v1/bind-for-step",
            json=_payload(instance, required_capabilities=["objects.read"]),
            headers=_ctx_header(),
        )
    assert resp.status_code == 412
    assert resp.json()["error"]["code"] == "capability-shortfall"


def test_bind_for_step_rejects_unknown_fields() -> None:
    """Pydantic ``extra=forbid`` makes typos fail with 422 before the
    request reaches the service.

    The 422 must carry the service's canonical
    ``{"error": {"code", "detail"}}`` envelope (rendered by the
    :class:`fastapi.exceptions.RequestValidationError` handler in
    :mod:`custos_connector`) rather than FastAPI's default
    ``{"detail": [...]}`` body — clients only know how to parse the
    former.
    """
    app = create_app(settings=_BASE_SETTINGS, providers=_build_providers())
    instance = _make_instance()
    body = _payload(instance)
    body["unknownField"] = "boom"
    with TestClient(app) as client:
        resp = client.post(
            "/internal/v1/bind-for-step",
            json=body,
            headers=_ctx_header(),
        )
    assert resp.status_code == 422
    body_json = resp.json()
    assert body_json.keys() == {"error"}, "validation errors must render the standard envelope"
    assert body_json["error"]["code"] == "invalid-request"
    assert isinstance(body_json["error"]["detail"], str)
    assert body_json["error"]["detail"], "detail must be a non-empty string"
