"""Tests for the ``ValidateConnector`` HTTP router (CONN-IMPL-027, Phase J).

Exercises ``POST /internal/v1/connectors:validate`` end-to-end through
:func:`create_app` so the dev-shim call-context middleware, the
:data:`CONNECTOR_VALIDATE` permission gate, and the wire-model dispatch
between the two preflight modes (``instance`` / ``manifest``) are all
under test.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from custos_spl.ids import ConnectorInstanceId, WorkspaceId
from custos_spl.interfaces.connector_instance_store import ConnectorInstance
from fastapi.testclient import TestClient

from custos_connector import create_app
from custos_connector.identity import IdentityResolverRegistry
from custos_connector.middleware import CALLCTX_HEADER
from custos_connector.permissions import CONNECTOR_BIND, CONNECTOR_VALIDATE
from custos_connector.providers import Providers
from custos_connector.settings import Settings
from custos_connector.validate import ValidateConnectorService
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

_TYPE = "azure-blob"
_VERSION = "1.0.0"


# ---------------------------------------------------------------------------
# Manifest + instance helpers
# ---------------------------------------------------------------------------


def _manifest(
    *,
    target_config: Mapping[str, Any] | None = None,
    auth_type: str = "azure-key-vault",
    auth_fields: Mapping[str, Any] | None = None,
    capabilities: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "metadata": {"type": _TYPE, "version": _VERSION},
        "spec": {
            "target": {
                "kind": "azure-blob-storage",
                "config": dict(target_config or {}),
            },
            "credentials": {
                "authenticationType": auth_type,
                "authentication": dict(auth_fields or {}),
            },
            "capabilities": capabilities if capabilities is not None else ["events.delivery"],
        },
    }


def _make_instance(
    *,
    target_config: Mapping[str, Any] | None = None,
    credentials_authentication: Mapping[str, Any] | None = None,
    used_capabilities: tuple[str, ...] | None = ("events.delivery",),
) -> ConnectorInstance:
    now = datetime.now(UTC)
    return ConnectorInstance(
        workspace_id=WorkspaceId(_WORKSPACE),
        instance_id=ConnectorInstanceId(str(uuid4())),
        type=_TYPE,
        version=_VERSION,
        name="primary",
        lease_ttl_seconds=3600,
        enabled=True,
        status="active",
        health_status="healthy",
        target_config=dict(target_config or {"storageAccount": "acc", "container": "bucket"}),
        credentials_authentication=dict(
            credentials_authentication or {"vaultUri": "https://vault", "secretName": "secret"}
        ),
        used_capabilities=used_capabilities,
        created_at=now,
        updated_at=now,
    )


def _ctx_header(
    *,
    workspace: str = _WORKSPACE,
    permissions: list[str] | None = None,
) -> dict[str, str]:
    perms = permissions if permissions is not None else [CONNECTOR_VALIDATE]
    return {
        CALLCTX_HEADER: json.dumps(
            {
                "workspace_id": workspace,
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
) -> Providers:
    catalog = catalog or FakeCatalogAdapter(applied_revisions={1, 2})
    instances = instances or FakeConnectorInstanceAdapter(applied_revisions={1})
    metadata = metadata or FakeMetadataAdapter(applied_revisions={1, 2, 3, 4})
    leases = FakeLeaseAdapter(applied_revisions={1})
    registry = IdentityResolverRegistry(resolvers=[])
    bind_service = build_bind_for_step_service(
        catalog_store=catalog,
        instance_store=instances,
        metadata_store=metadata,
        identity_registry=registry,
        plugin_binder=StubPluginBinder(),
    )
    validate_service = ValidateConnectorService(
        catalog_store=catalog,  # type: ignore[arg-type]
        instance_store=instances,  # type: ignore[arg-type]
    )
    return Providers(
        catalog_store=catalog,  # type: ignore[arg-type]
        instance_store=instances,  # type: ignore[arg-type]
        lease_store=leases,  # type: ignore[arg-type]
        metadata_store=metadata,  # type: ignore[arg-type]
        identity_registry=registry,
        bind_for_step_service=bind_service,
        lease_manager=build_lease_manager(lease_store=leases, metadata_store=metadata),
        validate_service=validate_service,
    )


async def _seed_catalog(
    catalog: FakeCatalogAdapter,
    *,
    manifest: Mapping[str, Any] | None = None,
) -> None:
    await catalog.put_connector_type_version(
        _TYPE,
        _VERSION,
        "sha256:fake",
        "example.test/blob@sha256:fake",
        manifest
        if manifest is not None
        else _manifest(
            target_config={"storageAccount": "acc", "container": "bucket"},
            auth_fields={"vaultUri": "https://vault", "secretName": "secret"},
            capabilities=["events.delivery"],
        ),
    )


# ---------------------------------------------------------------------------
# Manifest-mode tests
# ---------------------------------------------------------------------------


def test_validate_manifest_mode_returns_ok_on_valid_payload() -> None:
    catalog = FakeCatalogAdapter(applied_revisions={1, 2})
    asyncio.run(_seed_catalog(catalog))
    providers = _build_providers(catalog=catalog)
    app = create_app(settings=_BASE_SETTINGS, providers=providers)
    with TestClient(app) as client:
        response = client.post(
            "/internal/v1/connectors:validate",
            json={
                "mode": "manifest",
                "type": _TYPE,
                "version": _VERSION,
                "targetConfig": {"storageAccount": "acc", "container": "bucket"},
                "credentialsAuthentication": {
                    "vaultUri": "https://vault",
                    "secretName": "secret",
                },
                "usedCapabilities": ["events.delivery"],
            },
            headers=_ctx_header(),
        )
    assert response.status_code == 200
    body = response.json()
    assert body == {"ok": True, "type": _TYPE, "version": _VERSION}


def test_validate_manifest_mode_returns_400_on_missing_target_field() -> None:
    catalog = FakeCatalogAdapter(applied_revisions={1, 2})
    # Seed a manifest with empty target.config so the per-kind
    # required-field check has to read everything from the caller's
    # bag — otherwise the validator's merge-on-defaults short-circuits
    # the missing-field detection.
    asyncio.run(
        _seed_catalog(
            catalog,
            manifest=_manifest(
                target_config={},
                auth_fields={"vaultUri": "https://vault", "secretName": "secret"},
                capabilities=["events.delivery"],
            ),
        )
    )
    providers = _build_providers(catalog=catalog)
    app = create_app(settings=_BASE_SETTINGS, providers=providers)
    with TestClient(app) as client:
        response = client.post(
            "/internal/v1/connectors:validate",
            json={
                "mode": "manifest",
                "type": _TYPE,
                "version": _VERSION,
                "targetConfig": {"storageAccount": "acc"},  # missing 'container'
                "credentialsAuthentication": {
                    "vaultUri": "https://vault",
                    "secretName": "secret",
                },
            },
            headers=_ctx_header(),
        )
    assert response.status_code == 400
    body = response.json()
    assert body["error"]["code"] == "connector.instance_config_invalid"
    codes = {issue["code"] for issue in body["error"]["issues"]}
    assert "missing-target-config-field" in codes


def test_validate_manifest_mode_returns_404_for_unknown_type() -> None:
    providers = _build_providers()
    app = create_app(settings=_BASE_SETTINGS, providers=providers)
    with TestClient(app) as client:
        response = client.post(
            "/internal/v1/connectors:validate",
            json={
                "mode": "manifest",
                "type": "no-such-type",
                "version": "9.9.9",
                "targetConfig": {},
                "credentialsAuthentication": {},
            },
            headers=_ctx_header(),
        )
    assert response.status_code == 404
    body = response.json()
    assert body["error"]["code"] == "connector.instance_type_not_registered"


# ---------------------------------------------------------------------------
# Instance-mode tests
# ---------------------------------------------------------------------------


def test_validate_instance_mode_returns_ok_for_valid_instance() -> None:
    catalog = FakeCatalogAdapter(applied_revisions={1, 2})
    instances = FakeConnectorInstanceAdapter(applied_revisions={1})
    asyncio.run(_seed_catalog(catalog))
    instance = _make_instance()
    asyncio.run(instances.put_connector_instance(instance.workspace_id, instance))
    providers = _build_providers(catalog=catalog, instances=instances)
    app = create_app(settings=_BASE_SETTINGS, providers=providers)
    with TestClient(app) as client:
        response = client.post(
            "/internal/v1/connectors:validate",
            json={
                "mode": "instance",
                "connectorInstanceId": str(instance.instance_id),
                "requiredCapabilities": ["events.delivery"],
            },
            headers=_ctx_header(),
        )
    assert response.status_code == 200
    assert response.json() == {"ok": True, "type": _TYPE, "version": _VERSION}


def test_validate_instance_mode_returns_404_when_instance_missing() -> None:
    catalog = FakeCatalogAdapter(applied_revisions={1, 2})
    asyncio.run(_seed_catalog(catalog))
    providers = _build_providers(catalog=catalog)
    app = create_app(settings=_BASE_SETTINGS, providers=providers)
    with TestClient(app) as client:
        response = client.post(
            "/internal/v1/connectors:validate",
            json={
                "mode": "instance",
                "connectorInstanceId": "no-such-id",
            },
            headers=_ctx_header(),
        )
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "connector.instance_not_found"


def test_validate_instance_mode_returns_400_on_manifest_drift() -> None:
    # Manifest drift: catalog now requires an extra capability the
    # instance's pinned capabilities don't claim. The validator only
    # rejects capability tokens the operator asked for that aren't on
    # the manifest; here we drift by removing a capability the
    # instance still pins so the precheck fires.
    catalog = FakeCatalogAdapter(applied_revisions={1, 2})
    instances = FakeConnectorInstanceAdapter(applied_revisions={1})
    asyncio.run(
        _seed_catalog(
            catalog,
            manifest=_manifest(
                target_config={"storageAccount": "acc", "container": "bucket"},
                auth_fields={"vaultUri": "https://vault", "secretName": "secret"},
                capabilities=[],  # drift: catalog dropped events.delivery
            ),
        )
    )
    instance = _make_instance(used_capabilities=("events.delivery",))
    asyncio.run(instances.put_connector_instance(instance.workspace_id, instance))
    providers = _build_providers(catalog=catalog, instances=instances)
    app = create_app(settings=_BASE_SETTINGS, providers=providers)
    with TestClient(app) as client:
        response = client.post(
            "/internal/v1/connectors:validate",
            json={
                "mode": "instance",
                "connectorInstanceId": str(instance.instance_id),
            },
            headers=_ctx_header(),
        )
    assert response.status_code == 400
    body = response.json()
    assert body["error"]["code"] == "connector.instance_config_invalid"
    codes = {issue["code"] for issue in body["error"]["issues"]}
    assert "unknown-capability-on-instance" in codes


# ---------------------------------------------------------------------------
# Auth + envelope tests
# ---------------------------------------------------------------------------


def test_validate_requires_call_context_header() -> None:
    providers = _build_providers()
    app = create_app(settings=_BASE_SETTINGS, providers=providers)
    with TestClient(app) as client:
        response = client.post(
            "/internal/v1/connectors:validate",
            json={
                "mode": "manifest",
                "type": _TYPE,
                "version": _VERSION,
                "targetConfig": {},
                "credentialsAuthentication": {},
            },
        )
    assert response.status_code == 401
    body = response.json()
    assert "error" in body


def test_validate_denies_without_connector_validate_permission() -> None:
    providers = _build_providers()
    app = create_app(settings=_BASE_SETTINGS, providers=providers)
    with TestClient(app) as client:
        response = client.post(
            "/internal/v1/connectors:validate",
            json={
                "mode": "manifest",
                "type": _TYPE,
                "version": _VERSION,
                "targetConfig": {},
                "credentialsAuthentication": {},
            },
            # Hand in a non-empty permission set that doesn't include
            # CONNECTOR_VALIDATE so the gate returns 403 rather than
            # the "unauthenticated" code path.
            headers=_ctx_header(permissions=[CONNECTOR_BIND]),
        )
    assert response.status_code == 403
    assert response.json()["error"]["code"]  # any non-empty error code


def test_validate_rejects_unknown_mode() -> None:
    providers = _build_providers()
    app = create_app(settings=_BASE_SETTINGS, providers=providers)
    with TestClient(app) as client:
        response = client.post(
            "/internal/v1/connectors:validate",
            json={"mode": "bogus"},
            headers=_ctx_header(),
        )
    assert response.status_code == 422
    body = response.json()
    assert body["error"]["code"] == "invalid-request"
