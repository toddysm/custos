"""End-to-end tests for the public REST surface (CONN-IMPL-026, #309).

Exercises ``custos_connector.api`` through :func:`create_app` so every
route in
:mod:`custos_connector.api.{connector_types, instances, lease_admin, audit}`
is hit with the dev-shim call-context middleware + permission gates +
wire-model translation under test.

Postgres is not used: SPL is wired with the in-memory fakes from
:mod:`tests._fakes`; the instance routes' health probe is shorted via a
``_StubPluginInvoker`` so ``:enable`` / ``:force-health-check`` can be
asserted without Docker.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from custos_spl.ids import ConnectorInstanceId, RunId, StepId, WorkspaceId
from custos_spl.interfaces.connector_instance_store import ConnectorInstance
from custos_spl.interfaces.lease_store import Lease
from custos_spl.interfaces.metadata_store import AuditEvent
from fastapi.testclient import TestClient

from custos_connector import create_app
from custos_connector.audit import EVENT_LEASE_REVOKE_REQUESTED, EVENT_LEASE_REVOKED
from custos_connector.identity import IdentityResolverRegistry
from custos_connector.instances.service import InstanceService
from custos_connector.middleware import CALLCTX_HEADER
from custos_connector.permissions import (
    ADMIN_CONNECTOR,
    AUDIT_READ,
    CONNECTOR_READ,
)
from custos_connector.providers import Providers
from custos_connector.runtime import HealthResult
from custos_connector.settings import Settings
from custos_connector.sidecar_admin import (
    InMemorySidecarRegistry,
    SidecarAdminClient,
    SidecarRegistry,
)
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


# ---------------------------------------------------------------------------
# Fakes + helpers
# ---------------------------------------------------------------------------


@dataclass
class _StubPluginInvoker:
    """Plugin invoker stub used by the instances routes.

    ``next_result`` flips between healthy / unhealthy without driving
    a Docker hook. Defaults to a healthy probe so ``:enable`` is happy
    in the common case.
    """

    next_result: HealthResult | Exception = field(
        default_factory=lambda: HealthResult(
            healthy=True, detail=None, checked_at=datetime.now(UTC), extras={}
        )
    )
    calls: int = 0

    async def health(self, *, connector: object, instance: object) -> HealthResult:
        del connector, instance
        self.calls += 1
        if isinstance(self.next_result, Exception):
            raise self.next_result
        return self.next_result


def _ctx_header(
    *,
    workspace_id: str = _WORKSPACE,
    permissions: list[str] | None = None,
    principal_id: str = "op:alice",
) -> dict[str, str]:
    perms = (
        permissions if permissions is not None else [ADMIN_CONNECTOR, CONNECTOR_READ, AUDIT_READ]
    )
    return {
        CALLCTX_HEADER: json.dumps(
            {
                "workspace_id": workspace_id,
                "principal_id": principal_id,
                "permissions": perms,
            }
        )
    }


def _make_manifest() -> dict[str, Any]:
    """Minimal manifest the instance-config validator accepts.

    Mirrors the production catalog shape used in
    :mod:`tests.test_instance_service`: ``target.kind=oci-registry``
    with ``repositoryNamespace`` filled in, plus
    ``credentials.authenticationType=oidc`` with ``issuerUri`` and
    ``audience`` filled in. Capabilities listed here also gate which
    ``usedCapabilities`` operator requests may declare.
    """
    return {
        "metadata": {"type": _TYPE, "version": _VERSION},
        "spec": {
            "target": {
                "kind": "oci-registry",
                "config": {"repositoryNamespace": "tests/fake"},
            },
            "credentials": {
                "authenticationType": "oidc",
                "authentication": {
                    "issuerUri": "https://oidc.example.com",
                    "audience": "test-audience",
                },
            },
            "capabilities": ["oci.registry.read", "oci.referrers.list"],
        },
    }


def _build_providers(
    *,
    metadata: FakeMetadataAdapter | None = None,
    catalog: FakeCatalogAdapter | None = None,
    instances: FakeConnectorInstanceAdapter | None = None,
    leases: FakeLeaseAdapter | None = None,
    plugin_invoker: _StubPluginInvoker | None = None,
    sidecar_admin_client: SidecarAdminClient | None = None,
    sidecar_registry: SidecarRegistry | None = None,
) -> Providers:
    catalog_store = catalog or FakeCatalogAdapter(applied_revisions={1, 2})
    instance_store = instances or FakeConnectorInstanceAdapter(applied_revisions={1})
    metadata_store = metadata or FakeMetadataAdapter(applied_revisions={1, 2, 3, 4})
    lease_store = leases or FakeLeaseAdapter(applied_revisions={1})
    registry = IdentityResolverRegistry(resolvers=[])
    bind_service = build_bind_for_step_service(
        catalog_store=catalog_store,
        instance_store=instance_store,
        metadata_store=metadata_store,
        identity_registry=registry,
    )
    invoker = plugin_invoker or _StubPluginInvoker()
    instance_service = InstanceService(
        instance_store=instance_store,  # type: ignore[arg-type]
        catalog_store=catalog_store,  # type: ignore[arg-type]
        metadata_store=metadata_store,  # type: ignore[arg-type]
        plugin_invoker=invoker,  # type: ignore[arg-type]
        health_cache_ttl_seconds=60,
    )
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
        instance_service=instance_service,
        sidecar_admin_client=sidecar_admin_client,
        sidecar_registry=sidecar_registry or InMemorySidecarRegistry(),
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


async def _seed_connector_type(catalog: FakeCatalogAdapter) -> None:
    await catalog.put_connector_type_version(
        _TYPE,
        _VERSION,
        "sha256:fake",
        f"example.test/{_TYPE}@sha256:fake",
        _make_manifest(),
    )


def _build_instance(
    *,
    instance_id: str,
    workspace_id: str = _WORKSPACE,
    enabled: bool = True,
) -> ConnectorInstance:
    now = datetime.now(UTC)
    return ConnectorInstance(
        workspace_id=WorkspaceId(workspace_id),
        instance_id=ConnectorInstanceId(instance_id),
        type=_TYPE,
        version=_VERSION,
        name="prod",
        lease_ttl_seconds=3600,
        enabled=enabled,
        status="active" if enabled else "disabled",
        health_status="healthy",
        target_config={"repositoryNamespace": "tests/fake"},
        credentials_authentication={
            "issuerUri": "https://oidc.example.com",
            "audience": "test-audience",
        },
        used_capabilities=("oci.registry.read",),
        created_at=now,
        updated_at=now,
    )


def _build_lease(
    *,
    lease_id: str,
    workspace_id: str = _WORKSPACE,
    instance_id: str = "instance-1",
    run_id: str = "run-1",
    step_id: str = "step-1",
    attempt: int = 1,
    issued_at: datetime | None = None,
    expires_in: timedelta = timedelta(hours=1),
) -> Lease:
    now = issued_at or datetime.now(UTC)
    return Lease(
        workspace_id=WorkspaceId(workspace_id),
        lease_id=lease_id,
        run_id=RunId(run_id),
        step_id=StepId(step_id),
        attempt=attempt,
        slot="primary",
        capability="oci.registry.read",
        connector_instance_id=ConnectorInstanceId(instance_id),
        token_type="bearer",
        issued_at=now,
        expires_at=now + expires_in,
        released_at=None,
        revoked_at=None,
        revoke_reason=None,
        created_at=now,
        updated_at=now,
    )


# ===========================================================================
# Connector-types route
# ===========================================================================


def test_list_connector_types_returns_versions_for_a_type() -> None:
    catalog = FakeCatalogAdapter(applied_revisions={1, 2})
    providers = _build_providers(catalog=catalog)

    async def _seed() -> None:
        await _seed_connector_type(catalog)

    import asyncio

    asyncio.run(_seed())
    with _make_client(providers=providers) as (client, _):
        resp = client.get(
            f"/v1/workspaces/{_WORKSPACE}/connector-types",
            params={"type": _TYPE},
            headers=_ctx_header(),
        )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["items"] == [
        {
            "type": _TYPE,
            "version": _VERSION,
            "digest": "sha256:fake",
            "imageRef": f"example.test/{_TYPE}@sha256:fake",
            "deprecated": False,
        }
    ]
    assert body["nextCursor"] is None


def test_list_connector_types_rejects_workspace_mismatch() -> None:
    with _make_client() as (client, _):
        resp = client.get(
            f"/v1/workspaces/{_WORKSPACE}/connector-types",
            params={"type": _TYPE},
            headers=_ctx_header(workspace_id="ws-other"),
        )
    assert resp.status_code == 403
    body = resp.json()
    assert body["error"]["code"] == "connector.workspace_mismatch"


def test_list_connector_types_requires_connector_read_permission() -> None:
    with _make_client() as (client, _):
        resp = client.get(
            f"/v1/workspaces/{_WORKSPACE}/connector-types",
            params={"type": _TYPE},
            headers=_ctx_header(permissions=[ADMIN_CONNECTOR]),
        )
    assert resp.status_code == 403


# ===========================================================================
# Instances routes — create / read / list / patch
# ===========================================================================


def test_create_connector_returns_201_with_canonical_body() -> None:
    catalog = FakeCatalogAdapter(applied_revisions={1, 2})
    providers = _build_providers(catalog=catalog)

    import asyncio

    asyncio.run(_seed_connector_type(catalog))
    with _make_client(providers=providers) as (client, _):
        resp = client.post(
            f"/v1/workspaces/{_WORKSPACE}/connectors",
            json={
                "type": _TYPE,
                "version": _VERSION,
                "name": "prod-1",
                "leaseTtlSeconds": 600,
                "enabled": True,
            },
            headers=_ctx_header(),
        )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["workspaceId"] == _WORKSPACE
    assert body["type"] == _TYPE
    assert body["version"] == _VERSION
    assert body["name"] == "prod-1"
    assert body["leaseTtlSeconds"] == 600
    assert body["enabled"] is True
    assert body.get("instanceId")
    assert "createdAt" in body and "updatedAt" in body


def test_create_connector_rejects_unregistered_type_with_400() -> None:
    providers = _build_providers()
    with _make_client(providers=providers) as (client, _):
        resp = client.post(
            f"/v1/workspaces/{_WORKSPACE}/connectors",
            json={"type": "ghost", "version": "9.9.9"},
            headers=_ctx_header(),
        )
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "connector.instance_type_not_registered"


def test_create_connector_requires_admin_permission() -> None:
    catalog = FakeCatalogAdapter(applied_revisions={1, 2})
    providers = _build_providers(catalog=catalog)

    import asyncio

    asyncio.run(_seed_connector_type(catalog))
    with _make_client(providers=providers) as (client, _):
        resp = client.post(
            f"/v1/workspaces/{_WORKSPACE}/connectors",
            json={"type": _TYPE, "version": _VERSION},
            headers=_ctx_header(permissions=[CONNECTOR_READ]),
        )
    assert resp.status_code == 403


def test_get_connector_returns_200_and_404_appropriately() -> None:
    catalog = FakeCatalogAdapter(applied_revisions={1, 2})
    instances = FakeConnectorInstanceAdapter(applied_revisions={1})
    providers = _build_providers(catalog=catalog, instances=instances)

    import asyncio

    async def _seed() -> str:
        await _seed_connector_type(catalog)
        row = _build_instance(instance_id="instance-1")
        await instances.put_connector_instance(row.workspace_id, row)
        return str(row.instance_id)

    instance_id = asyncio.run(_seed())
    with _make_client(providers=providers) as (client, _):
        ok = client.get(
            f"/v1/workspaces/{_WORKSPACE}/connectors/{instance_id}",
            headers=_ctx_header(),
        )
        missing = client.get(
            f"/v1/workspaces/{_WORKSPACE}/connectors/no-such",
            headers=_ctx_header(),
        )
    assert ok.status_code == 200
    assert ok.json()["instanceId"] == instance_id
    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "connector.instance_not_found"


def test_list_connectors_filters_by_type() -> None:
    catalog = FakeCatalogAdapter(applied_revisions={1, 2})
    instances = FakeConnectorInstanceAdapter(applied_revisions={1})
    providers = _build_providers(catalog=catalog, instances=instances)

    import asyncio

    async def _seed() -> None:
        await _seed_connector_type(catalog)
        for i in range(3):
            row = _build_instance(instance_id=f"instance-{i}")
            await instances.put_connector_instance(row.workspace_id, row)

    asyncio.run(_seed())
    with _make_client(providers=providers) as (client, _):
        resp = client.get(
            f"/v1/workspaces/{_WORKSPACE}/connectors",
            params={"type": _TYPE},
            headers=_ctx_header(),
        )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["items"]) == 3
    assert {item["instanceId"] for item in body["items"]} == {
        "instance-0",
        "instance-1",
        "instance-2",
    }


def test_patch_connector_updates_name_and_lease_ttl() -> None:
    catalog = FakeCatalogAdapter(applied_revisions={1, 2})
    instances = FakeConnectorInstanceAdapter(applied_revisions={1})
    providers = _build_providers(catalog=catalog, instances=instances)

    import asyncio

    async def _seed() -> str:
        await _seed_connector_type(catalog)
        row = _build_instance(instance_id="instance-1")
        await instances.put_connector_instance(row.workspace_id, row)
        return str(row.instance_id)

    instance_id = asyncio.run(_seed())
    with _make_client(providers=providers) as (client, _):
        resp = client.patch(
            f"/v1/workspaces/{_WORKSPACE}/connectors/{instance_id}",
            json={"name": "prod-renamed", "leaseTtlSeconds": 1200},
            headers=_ctx_header(),
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["name"] == "prod-renamed"
    assert body["leaseTtlSeconds"] == 1200


def test_patch_connector_rejects_unknown_field_at_parse_time() -> None:
    providers = _build_providers()
    with _make_client(providers=providers) as (client, _):
        resp = client.patch(
            f"/v1/workspaces/{_WORKSPACE}/connectors/instance-1",
            json={"type": "different-type"},
            headers=_ctx_header(),
        )
    # Pydantic's extra="forbid" produces a 422 wire-shape rejection.
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "invalid-request"


# ===========================================================================
# Instances routes — enable / disable / health / force-health-check
# ===========================================================================


def test_enable_connector_runs_health_probe_and_returns_200() -> None:
    catalog = FakeCatalogAdapter(applied_revisions={1, 2})
    instances = FakeConnectorInstanceAdapter(applied_revisions={1})
    invoker = _StubPluginInvoker(
        next_result=HealthResult(
            healthy=True, detail=None, checked_at=datetime.now(UTC), extras={}
        ),
    )
    providers = _build_providers(catalog=catalog, instances=instances, plugin_invoker=invoker)

    import asyncio

    async def _seed() -> str:
        await _seed_connector_type(catalog)
        row = _build_instance(instance_id="instance-1", enabled=False)
        await instances.put_connector_instance(row.workspace_id, row)
        return str(row.instance_id)

    instance_id = asyncio.run(_seed())
    with _make_client(providers=providers) as (client, _):
        resp = client.post(
            f"/v1/workspaces/{_WORKSPACE}/connectors/{instance_id}:enable",
            headers=_ctx_header(),
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["enabled"] is True
    assert body["status"] == "active"
    assert invoker.calls == 1


def test_enable_connector_returns_503_when_probe_fails() -> None:
    catalog = FakeCatalogAdapter(applied_revisions={1, 2})
    instances = FakeConnectorInstanceAdapter(applied_revisions={1})
    invoker = _StubPluginInvoker(
        next_result=HealthResult(
            healthy=False,
            detail="upstream timeout",
            checked_at=datetime.now(UTC),
            extras={},
        ),
    )
    providers = _build_providers(catalog=catalog, instances=instances, plugin_invoker=invoker)

    import asyncio

    async def _seed() -> str:
        await _seed_connector_type(catalog)
        row = _build_instance(instance_id="instance-1", enabled=False)
        await instances.put_connector_instance(row.workspace_id, row)
        return str(row.instance_id)

    instance_id = asyncio.run(_seed())
    with _make_client(providers=providers) as (client, _):
        resp = client.post(
            f"/v1/workspaces/{_WORKSPACE}/connectors/{instance_id}:enable",
            headers=_ctx_header(),
        )
    assert resp.status_code == 503
    assert resp.json()["error"]["code"] == "connector.instance_activation_probe_failed"


def test_disable_connector_returns_200_and_disabled_row() -> None:
    catalog = FakeCatalogAdapter(applied_revisions={1, 2})
    instances = FakeConnectorInstanceAdapter(applied_revisions={1})
    providers = _build_providers(catalog=catalog, instances=instances)

    import asyncio

    async def _seed() -> str:
        await _seed_connector_type(catalog)
        row = _build_instance(instance_id="instance-1", enabled=True)
        await instances.put_connector_instance(row.workspace_id, row)
        return str(row.instance_id)

    instance_id = asyncio.run(_seed())
    with _make_client(providers=providers) as (client, _):
        resp = client.post(
            f"/v1/workspaces/{_WORKSPACE}/connectors/{instance_id}:disable",
            headers=_ctx_header(),
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["enabled"] is False
    assert body["status"] == "disabled"


def test_read_connector_health_probes_and_returns_snapshot() -> None:
    catalog = FakeCatalogAdapter(applied_revisions={1, 2})
    instances = FakeConnectorInstanceAdapter(applied_revisions={1})
    invoker = _StubPluginInvoker(
        next_result=HealthResult(
            healthy=True, detail=None, checked_at=datetime.now(UTC), extras={}
        ),
    )
    providers = _build_providers(catalog=catalog, instances=instances, plugin_invoker=invoker)

    import asyncio

    async def _seed() -> str:
        await _seed_connector_type(catalog)
        row = _build_instance(instance_id="instance-1")
        await instances.put_connector_instance(row.workspace_id, row)
        return str(row.instance_id)

    instance_id = asyncio.run(_seed())
    with _make_client(providers=providers) as (client, _):
        resp = client.get(
            f"/v1/workspaces/{_WORKSPACE}/connectors/{instance_id}/health",
            headers=_ctx_header(),
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["healthy"] is True
    assert body["source"] in {"probe", "cache"}


def test_force_health_check_bypasses_cache() -> None:
    catalog = FakeCatalogAdapter(applied_revisions={1, 2})
    instances = FakeConnectorInstanceAdapter(applied_revisions={1})
    invoker = _StubPluginInvoker(
        next_result=HealthResult(
            healthy=True, detail=None, checked_at=datetime.now(UTC), extras={}
        ),
    )
    providers = _build_providers(catalog=catalog, instances=instances, plugin_invoker=invoker)

    import asyncio

    async def _seed() -> str:
        await _seed_connector_type(catalog)
        row = _build_instance(instance_id="instance-1")
        await instances.put_connector_instance(row.workspace_id, row)
        return str(row.instance_id)

    instance_id = asyncio.run(_seed())
    with _make_client(providers=providers) as (client, _):
        first = client.get(
            f"/v1/workspaces/{_WORKSPACE}/connectors/{instance_id}/health",
            headers=_ctx_header(),
        )
        forced = client.post(
            f"/v1/workspaces/{_WORKSPACE}/connectors/{instance_id}:force-health-check",
            headers=_ctx_header(),
        )
    assert first.status_code == 200
    assert forced.status_code == 200
    # GET /health probes once on a cold cache; :force-health-check probes
    # again regardless. The exact call count is "at least two".
    assert invoker.calls >= 2
    assert forced.json()["source"] == "probe"


# ===========================================================================
# Lease admin routes
# ===========================================================================


def test_list_leases_for_instance_returns_active_rows() -> None:
    leases = FakeLeaseAdapter(applied_revisions={1})
    providers = _build_providers(leases=leases)

    import asyncio

    async def _seed() -> None:
        for i in range(2):
            await leases.put_lease(
                WorkspaceId(_WORKSPACE),
                _build_lease(lease_id=f"lease-{i}", instance_id="instance-1"),
            )

    asyncio.run(_seed())
    with _make_client(providers=providers) as (client, _):
        resp = client.get(
            f"/v1/workspaces/{_WORKSPACE}/connectors/instance-1/leases",
            headers=_ctx_header(),
        )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["items"]) == 2
    assert {item["leaseId"] for item in body["items"]} == {"lease-0", "lease-1"}


def test_list_leases_for_run_filters_by_run_id() -> None:
    leases = FakeLeaseAdapter(applied_revisions={1})
    providers = _build_providers(leases=leases)

    import asyncio

    async def _seed() -> None:
        await leases.put_lease(
            WorkspaceId(_WORKSPACE),
            _build_lease(lease_id="a", run_id="run-a"),
        )
        await leases.put_lease(
            WorkspaceId(_WORKSPACE),
            _build_lease(lease_id="b", run_id="run-b"),
        )

    asyncio.run(_seed())
    with _make_client(providers=providers) as (client, _):
        resp = client.get(
            f"/v1/workspaces/{_WORKSPACE}/runs/run-a/leases",
            headers=_ctx_header(),
        )
    assert resp.status_code == 200
    body = resp.json()
    assert [item["leaseId"] for item in body["items"]] == ["a"]


def test_revoke_single_lease_returns_revoked_status() -> None:
    leases = FakeLeaseAdapter(applied_revisions={1})
    providers = _build_providers(leases=leases)

    import asyncio

    async def _seed() -> None:
        await leases.put_lease(
            WorkspaceId(_WORKSPACE),
            _build_lease(lease_id="lease-x"),
        )

    asyncio.run(_seed())
    with _make_client(providers=providers) as (client, _):
        resp = client.post(
            f"/v1/workspaces/{_WORKSPACE}/leases/lease-x:revoke",
            json={"reason": "ops-incident-1234"},
            headers=_ctx_header(),
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body == {"leaseId": "lease-x", "status": "revoked"}


def test_revoke_single_lease_returns_404_for_unknown_id() -> None:
    providers = _build_providers()
    with _make_client(providers=providers) as (client, _):
        resp = client.post(
            f"/v1/workspaces/{_WORKSPACE}/leases/ghost:revoke",
            json={"reason": "ghost-hunt"},
            headers=_ctx_header(),
        )
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "connector.lease_not_found"


def test_revoke_all_for_instance_revokes_each_active_lease() -> None:
    leases = FakeLeaseAdapter(applied_revisions={1})
    providers = _build_providers(leases=leases)

    import asyncio

    async def _seed() -> None:
        for i in range(3):
            await leases.put_lease(
                WorkspaceId(_WORKSPACE),
                _build_lease(lease_id=f"x-{i}", instance_id="instance-1"),
            )

    asyncio.run(_seed())
    with _make_client(providers=providers) as (client, _):
        resp = client.post(
            f"/v1/workspaces/{_WORKSPACE}/connectors/instance-1/leases:revoke-all",
            json={"reason": "rotate-credentials"},
            headers=_ctx_header(),
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 3
    assert body["revoked"] == 3
    assert all(entry["status"] == "revoked" for entry in body["results"])


def test_revoke_all_for_run_revokes_only_matching_run() -> None:
    leases = FakeLeaseAdapter(applied_revisions={1})
    providers = _build_providers(leases=leases)

    import asyncio

    async def _seed() -> None:
        await leases.put_lease(
            WorkspaceId(_WORKSPACE),
            _build_lease(lease_id="a", run_id="run-a"),
        )
        await leases.put_lease(
            WorkspaceId(_WORKSPACE),
            _build_lease(lease_id="b", run_id="run-b"),
        )

    asyncio.run(_seed())
    with _make_client(providers=providers) as (client, _):
        resp = client.post(
            f"/v1/workspaces/{_WORKSPACE}/runs/run-a/leases:revoke-all",
            json={"reason": "cancel"},
            headers=_ctx_header(),
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert body["revoked"] == 1
    assert body["results"][0]["leaseId"] == "a"


# ---------------------------------------------------------------------------
# Revoke flows — CONN-IMPL-028
# ---------------------------------------------------------------------------
#
# These tests exercise the four behaviors added in CONN-IMPL-028:
#   1. canonical ``connector.reason_required`` 400 on missing/empty reason
#      (the design refuses pydantic's default 422 detail-array shape).
#   2. ``lease.revoke-requested`` is emitted exactly once per operator
#      action and ``lease.revoked`` once per actually-revoked lease.
#   3. the audit ``actor`` on both event types is the operator's
#      call-context ``principal_id`` (not the static lease-manager actor).
#   4. best-effort sidecar fan-out per the design § Sidecar revoke
#      control-channel API: leases bucketed by registered endpoint, one
#      POST per bucket, transport errors and 503-shutting-down swallowed.


def _audit_event_types(
    metadata: FakeMetadataAdapter, *, workspace_id: str = _WORKSPACE
) -> list[str]:
    return [evt.event_type for ws, evt in metadata.append_audit_calls if ws == workspace_id]


def _audit_events(
    metadata: FakeMetadataAdapter, *, workspace_id: str = _WORKSPACE
) -> list[AuditEvent]:
    return [evt for ws, evt in metadata.append_audit_calls if ws == workspace_id]


def test_revoke_single_missing_reason_returns_reason_required_400() -> None:
    leases = FakeLeaseAdapter(applied_revisions={1})
    providers = _build_providers(leases=leases)

    import asyncio

    async def _seed() -> None:
        await leases.put_lease(WorkspaceId(_WORKSPACE), _build_lease(lease_id="lx"))

    asyncio.run(_seed())
    with _make_client(providers=providers) as (client, _):
        # No body at all.
        resp = client.post(
            f"/v1/workspaces/{_WORKSPACE}/leases/lx:revoke",
            headers=_ctx_header(),
        )
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "connector.reason_required"


@pytest.mark.parametrize("body", [{"reason": ""}, {"reason": "   "}, {}])
def test_revoke_single_empty_reason_returns_reason_required_400(
    body: dict[str, str],
) -> None:
    leases = FakeLeaseAdapter(applied_revisions={1})
    providers = _build_providers(leases=leases)

    import asyncio

    async def _seed() -> None:
        await leases.put_lease(WorkspaceId(_WORKSPACE), _build_lease(lease_id="lx"))

    asyncio.run(_seed())
    with _make_client(providers=providers) as (client, _):
        resp = client.post(
            f"/v1/workspaces/{_WORKSPACE}/leases/lx:revoke",
            json=body,
            headers=_ctx_header(),
        )
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "connector.reason_required"


def test_revoke_all_for_instance_missing_reason_returns_reason_required_400() -> None:
    providers = _build_providers()
    with _make_client(providers=providers) as (client, _):
        resp = client.post(
            f"/v1/workspaces/{_WORKSPACE}/connectors/instance-1/leases:revoke-all",
            headers=_ctx_header(),
        )
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "connector.reason_required"


def test_revoke_all_for_run_empty_reason_returns_reason_required_400() -> None:
    providers = _build_providers()
    with _make_client(providers=providers) as (client, _):
        resp = client.post(
            f"/v1/workspaces/{_WORKSPACE}/runs/run-a/leases:revoke-all",
            json={"reason": "   "},
            headers=_ctx_header(),
        )
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "connector.reason_required"


def test_revoke_single_emits_requested_and_revoked_with_operator_actor() -> None:
    """Single revoke emits one `lease.revoke-requested` + one `lease.revoked`,
    both carrying the operator's call-context ``principal_id`` as ``actor``.
    """
    metadata = FakeMetadataAdapter(applied_revisions={1, 2, 3, 4})
    leases = FakeLeaseAdapter(applied_revisions={1})
    providers = _build_providers(metadata=metadata, leases=leases)

    import asyncio

    async def _seed() -> None:
        await leases.put_lease(WorkspaceId(_WORKSPACE), _build_lease(lease_id="lx"))

    asyncio.run(_seed())
    with _make_client(providers=providers) as (client, _):
        resp = client.post(
            f"/v1/workspaces/{_WORKSPACE}/leases/lx:revoke",
            json={"reason": "ops-incident"},
            headers=_ctx_header(principal_id="op:carol"),
        )
    assert resp.status_code == 200
    assert _audit_event_types(metadata) == [
        EVENT_LEASE_REVOKE_REQUESTED,
        EVENT_LEASE_REVOKED,
    ]
    events = _audit_events(metadata)
    assert events[0].actor == "op:carol"
    assert events[0].payload["operator"] == "op:carol"
    assert events[0].payload["lease_ids"] == ["lx"]
    assert events[0].subject == {"selector_type": "lease", "selector_value": "lx"}
    assert events[1].actor == "op:carol"
    assert events[1].payload["revoke_reason"] == "ops-incident"


def test_revoke_all_for_instance_emits_one_requested_and_n_revoked() -> None:
    metadata = FakeMetadataAdapter(applied_revisions={1, 2, 3, 4})
    leases = FakeLeaseAdapter(applied_revisions={1})
    providers = _build_providers(metadata=metadata, leases=leases)

    import asyncio

    async def _seed() -> None:
        for i in range(3):
            await leases.put_lease(
                WorkspaceId(_WORKSPACE),
                _build_lease(lease_id=f"r-{i}", instance_id="instance-1"),
            )

    asyncio.run(_seed())
    with _make_client(providers=providers) as (client, _):
        resp = client.post(
            f"/v1/workspaces/{_WORKSPACE}/connectors/instance-1/leases:revoke-all",
            json={"reason": "rotate"},
            headers=_ctx_header(principal_id="op:dave"),
        )
    assert resp.status_code == 200
    types = _audit_event_types(metadata)
    assert types.count(EVENT_LEASE_REVOKE_REQUESTED) == 1
    assert types.count(EVENT_LEASE_REVOKED) == 3
    requested = next(
        evt for evt in _audit_events(metadata) if evt.event_type == EVENT_LEASE_REVOKE_REQUESTED
    )
    assert requested.actor == "op:dave"
    assert requested.payload["operator"] == "op:dave"
    assert sorted(requested.payload["lease_ids"]) == ["r-0", "r-1", "r-2"]
    assert requested.subject == {"selector_type": "instance", "selector_value": "instance-1"}
    for evt in _audit_events(metadata):
        if evt.event_type == EVENT_LEASE_REVOKED:
            assert evt.actor == "op:dave"


def test_revoke_all_for_run_emits_one_requested_and_only_matching_revoked() -> None:
    metadata = FakeMetadataAdapter(applied_revisions={1, 2, 3, 4})
    leases = FakeLeaseAdapter(applied_revisions={1})
    providers = _build_providers(metadata=metadata, leases=leases)

    import asyncio

    async def _seed() -> None:
        await leases.put_lease(
            WorkspaceId(_WORKSPACE),
            _build_lease(lease_id="ra", run_id="run-a"),
        )
        await leases.put_lease(
            WorkspaceId(_WORKSPACE),
            _build_lease(lease_id="rb", run_id="run-b"),
        )

    asyncio.run(_seed())
    with _make_client(providers=providers) as (client, _):
        resp = client.post(
            f"/v1/workspaces/{_WORKSPACE}/runs/run-a/leases:revoke-all",
            json={"reason": "cancel"},
            headers=_ctx_header(principal_id="op:erin"),
        )
    assert resp.status_code == 200
    types = _audit_event_types(metadata)
    assert types.count(EVENT_LEASE_REVOKE_REQUESTED) == 1
    assert types.count(EVENT_LEASE_REVOKED) == 1
    requested = next(
        evt for evt in _audit_events(metadata) if evt.event_type == EVENT_LEASE_REVOKE_REQUESTED
    )
    assert requested.payload["lease_ids"] == ["ra"]
    assert requested.subject == {"selector_type": "run", "selector_value": "run-a"}


def _make_sidecar_admin_client(
    handler: Any,
) -> SidecarAdminClient:
    """Build a SidecarAdminClient backed by an in-process httpx MockTransport.

    Tests pass a single ``handler(httpx.Request) -> httpx.Response`` that
    captures the incoming POSTs for later assertion.
    """
    import httpx

    transport = httpx.MockTransport(handler)
    http_client = httpx.AsyncClient(transport=transport)
    return SidecarAdminClient(http_client=http_client)


def test_revoke_all_for_run_fans_out_to_each_registered_sidecar() -> None:
    """Two leases on two different sidecars → two endpoint-scoped POSTs."""
    import httpx

    metadata = FakeMetadataAdapter(applied_revisions={1, 2, 3, 4})
    leases = FakeLeaseAdapter(applied_revisions={1})

    captured: list[tuple[str, dict[str, Any]]] = []

    def _handler(request: httpx.Request) -> httpx.Response:
        captured.append((str(request.url), json.loads(request.content.decode())))
        # Echo every lease as ``revoked`` for whichever bucket called us.
        body = json.loads(request.content.decode())
        return httpx.Response(
            status_code=200,
            json={"results": [{"leaseId": lid, "status": "revoked"} for lid in body["leaseIds"]]},
        )

    sidecar_client = _make_sidecar_admin_client(_handler)
    registry = InMemorySidecarRegistry()
    registry.register(lease_id="la", endpoint="http://sidecar-a:9443")
    registry.register(lease_id="lb", endpoint="http://sidecar-b:9443")

    providers = _build_providers(
        metadata=metadata,
        leases=leases,
        sidecar_admin_client=sidecar_client,
        sidecar_registry=registry,
    )

    import asyncio

    async def _seed() -> None:
        await leases.put_lease(
            WorkspaceId(_WORKSPACE),
            _build_lease(lease_id="la", run_id="run-x"),
        )
        await leases.put_lease(
            WorkspaceId(_WORKSPACE),
            _build_lease(lease_id="lb", run_id="run-x"),
        )

    asyncio.run(_seed())
    with _make_client(providers=providers) as (client, _):
        resp = client.post(
            f"/v1/workspaces/{_WORKSPACE}/runs/run-x/leases:revoke-all",
            json={"reason": "fan-out-test"},
            headers=_ctx_header(),
        )
    assert resp.status_code == 200
    # Exactly one call per registered endpoint, each carrying only its own
    # lease id. The bucket-by-endpoint reduction is the key contract.
    urls = sorted(url for url, _ in captured)
    assert urls == [
        "http://sidecar-a:9443/sidecar-admin/v1/revoke",
        "http://sidecar-b:9443/sidecar-admin/v1/revoke",
    ]
    by_url = dict(captured)
    assert by_url["http://sidecar-a:9443/sidecar-admin/v1/revoke"]["leaseIds"] == ["la"]
    assert by_url["http://sidecar-b:9443/sidecar-admin/v1/revoke"]["leaseIds"] == ["lb"]


def test_revoke_succeeds_with_no_sidecar_registered() -> None:
    """Empty registry → DB revoke only, no fan-out, operator gets 200."""
    import httpx

    metadata = FakeMetadataAdapter(applied_revisions={1, 2, 3, 4})
    leases = FakeLeaseAdapter(applied_revisions={1})

    calls: list[httpx.Request] = []

    def _handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(status_code=200, json={"results": []})

    providers = _build_providers(
        metadata=metadata,
        leases=leases,
        sidecar_admin_client=_make_sidecar_admin_client(_handler),
        sidecar_registry=InMemorySidecarRegistry(),
    )

    import asyncio

    async def _seed() -> None:
        await leases.put_lease(WorkspaceId(_WORKSPACE), _build_lease(lease_id="lx"))

    asyncio.run(_seed())
    with _make_client(providers=providers) as (client, _):
        resp = client.post(
            f"/v1/workspaces/{_WORKSPACE}/leases/lx:revoke",
            json={"reason": "no-sidecar"},
            headers=_ctx_header(),
        )
    assert resp.status_code == 200
    assert resp.json() == {"leaseId": "lx", "status": "revoked"}
    assert calls == []  # zero fan-out attempts


def test_revoke_succeeds_when_sidecar_returns_503_shutting_down() -> None:
    """Sidecar 503 (shutting down) is treated as terminal-revoke success."""
    import httpx

    metadata = FakeMetadataAdapter(applied_revisions={1, 2, 3, 4})
    leases = FakeLeaseAdapter(applied_revisions={1})

    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code=503)

    registry = InMemorySidecarRegistry()
    registry.register(lease_id="lx", endpoint="http://sidecar:9443")

    providers = _build_providers(
        metadata=metadata,
        leases=leases,
        sidecar_admin_client=_make_sidecar_admin_client(_handler),
        sidecar_registry=registry,
    )

    import asyncio

    async def _seed() -> None:
        await leases.put_lease(WorkspaceId(_WORKSPACE), _build_lease(lease_id="lx"))

    asyncio.run(_seed())
    with _make_client(providers=providers) as (client, _):
        resp = client.post(
            f"/v1/workspaces/{_WORKSPACE}/leases/lx:revoke",
            json={"reason": "shutting-down"},
            headers=_ctx_header(),
        )
    assert resp.status_code == 200
    assert resp.json() == {"leaseId": "lx", "status": "revoked"}


def test_revoke_succeeds_when_sidecar_transport_error() -> None:
    """Transport failure → swallowed; operator gets 200 from DB revoke."""
    import httpx

    metadata = FakeMetadataAdapter(applied_revisions={1, 2, 3, 4})
    leases = FakeLeaseAdapter(applied_revisions={1})

    def _handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    registry = InMemorySidecarRegistry()
    registry.register(lease_id="lx", endpoint="http://sidecar:9443")

    providers = _build_providers(
        metadata=metadata,
        leases=leases,
        sidecar_admin_client=_make_sidecar_admin_client(_handler),
        sidecar_registry=registry,
    )

    import asyncio

    async def _seed() -> None:
        await leases.put_lease(WorkspaceId(_WORKSPACE), _build_lease(lease_id="lx"))

    asyncio.run(_seed())
    with _make_client(providers=providers) as (client, _):
        resp = client.post(
            f"/v1/workspaces/{_WORKSPACE}/leases/lx:revoke",
            json={"reason": "transport-down"},
            headers=_ctx_header(),
        )
    assert resp.status_code == 200
    assert resp.json() == {"leaseId": "lx", "status": "revoked"}


def test_revoke_succeeds_when_sidecar_returns_non_object_json() -> None:
    """Malformed 200 (e.g. JSON array instead of object) is swallowed.

    Regression test for the case where the sidecar control listener
    returns ``200 OK`` with a payload that is not a mapping. Indexing
    into a list with ``["results"]`` raises ``TypeError``, which the
    client now catches alongside ``KeyError`` / ``ValueError``.
    """
    import httpx

    metadata = FakeMetadataAdapter(applied_revisions={1, 2, 3, 4})
    leases = FakeLeaseAdapter(applied_revisions={1})

    def _handler(request: httpx.Request) -> httpx.Response:
        # 200, but the body is a JSON array, not the expected object.
        return httpx.Response(status_code=200, json=[])

    registry = InMemorySidecarRegistry()
    registry.register(lease_id="lx", endpoint="http://sidecar:9443")

    providers = _build_providers(
        metadata=metadata,
        leases=leases,
        sidecar_admin_client=_make_sidecar_admin_client(_handler),
        sidecar_registry=registry,
    )

    import asyncio

    async def _seed() -> None:
        await leases.put_lease(WorkspaceId(_WORKSPACE), _build_lease(lease_id="lx"))

    asyncio.run(_seed())
    with _make_client(providers=providers) as (client, _):
        resp = client.post(
            f"/v1/workspaces/{_WORKSPACE}/leases/lx:revoke",
            json={"reason": "malformed"},
            headers=_ctx_header(),
        )
    assert resp.status_code == 200
    assert resp.json() == {"leaseId": "lx", "status": "revoked"}


def test_revoke_all_fans_out_to_sidecar_buckets_concurrently() -> None:
    """Concurrent fan-out caps wall time at one sidecar round-trip.

    Two sidecars each artificially delay their ack by 200ms. Sequential
    dispatch would take ~400ms; with :func:`asyncio.gather` the operator
    request should complete in ~200ms. We leave a generous safety margin
    in the assertion to avoid CI flake while still catching a regression
    to the linear path.
    """
    import asyncio
    import time

    import httpx

    metadata = FakeMetadataAdapter(applied_revisions={1, 2, 3, 4})
    leases = FakeLeaseAdapter(applied_revisions={1})

    async def _handler(request: httpx.Request) -> httpx.Response:
        # The MockTransport supports either sync or async handlers.
        await asyncio.sleep(0.2)
        body = json.loads(request.content.decode())
        return httpx.Response(
            status_code=200,
            json={"results": [{"leaseId": lid, "status": "revoked"} for lid in body["leaseIds"]]},
        )

    transport = httpx.MockTransport(_handler)
    http_client = httpx.AsyncClient(transport=transport)
    sidecar_client = SidecarAdminClient(http_client=http_client)
    registry = InMemorySidecarRegistry()
    registry.register(lease_id="la", endpoint="http://sidecar-a:9443")
    registry.register(lease_id="lb", endpoint="http://sidecar-b:9443")

    providers = _build_providers(
        metadata=metadata,
        leases=leases,
        sidecar_admin_client=sidecar_client,
        sidecar_registry=registry,
    )

    async def _seed() -> None:
        await leases.put_lease(
            WorkspaceId(_WORKSPACE),
            _build_lease(lease_id="la", run_id="run-x"),
        )
        await leases.put_lease(
            WorkspaceId(_WORKSPACE),
            _build_lease(lease_id="lb", run_id="run-x"),
        )

    asyncio.run(_seed())
    with _make_client(providers=providers) as (client, _):
        started = time.perf_counter()
        resp = client.post(
            f"/v1/workspaces/{_WORKSPACE}/runs/run-x/leases:revoke-all",
            json={"reason": "concurrent"},
            headers=_ctx_header(),
        )
        elapsed = time.perf_counter() - started
    assert resp.status_code == 200
    # Two 0.2s sleeps run concurrently → expect ~0.2s; the linear path
    # would be ~0.4s. The 0.35s budget catches the regression without
    # flaking on slow CI executors.
    assert elapsed < 0.35, f"fan-out appears sequential: took {elapsed:.3f}s"


def test_list_leases_aggregates_active_set_via_cs_store() -> None:
    """Authoritative live-state listing reads the CS lease store, not sidecars.

    The design § Operator Admin Surface contract is that ``GET .../leases``
    returns every active lease across every registered sidecar. The store
    is the canonical aggregate so a single ``list_active_leases`` covers
    it — no per-sidecar fan-in. This regression-tests that three leases
    nominally hosted across two sidecars (modeled by two distinct
    ``runId``s under the same instance) all surface in a single response.
    """
    leases = FakeLeaseAdapter(applied_revisions={1})
    providers = _build_providers(leases=leases)

    import asyncio

    async def _seed() -> None:
        # Two on sidecar A (run-a), one on sidecar B (run-b) — same instance.
        await leases.put_lease(
            WorkspaceId(_WORKSPACE),
            _build_lease(lease_id="agg-1", instance_id="instance-1", run_id="run-a"),
        )
        await leases.put_lease(
            WorkspaceId(_WORKSPACE),
            _build_lease(lease_id="agg-2", instance_id="instance-1", run_id="run-a"),
        )
        await leases.put_lease(
            WorkspaceId(_WORKSPACE),
            _build_lease(lease_id="agg-3", instance_id="instance-1", run_id="run-b"),
        )

    asyncio.run(_seed())
    with _make_client(providers=providers) as (client, _):
        resp = client.get(
            f"/v1/workspaces/{_WORKSPACE}/connectors/instance-1/leases",
            headers=_ctx_header(),
        )
    assert resp.status_code == 200
    items = resp.json()["items"]
    assert sorted(item["leaseId"] for item in items) == ["agg-1", "agg-2", "agg-3"]


# ===========================================================================
# Audit route
# ===========================================================================


def _audit_event(
    *,
    event_type: str,
    actor: str = "op:alice",
    occurred_at: datetime | None = None,
    event_id: str | None = None,
) -> AuditEvent:
    return AuditEvent(
        workspace_id=WorkspaceId(_WORKSPACE),
        event_id=event_id or f"evt-{event_type}",
        event_type=event_type,
        actor=actor,
        subject={},
        payload={},
        occurred_at=occurred_at or datetime.now(UTC),
    )


def test_audit_leases_returns_only_lease_events() -> None:
    metadata = FakeMetadataAdapter(applied_revisions={1, 2, 3, 4})
    providers = _build_providers(metadata=metadata)

    import asyncio

    async def _seed() -> None:
        await metadata.append_audit(
            WorkspaceId(_WORKSPACE),
            _audit_event(event_type="lease.issued", event_id="e1"),
        )
        await metadata.append_audit(
            WorkspaceId(_WORKSPACE),
            _audit_event(event_type="connector.instance.created", event_id="e2"),
        )
        await metadata.append_audit(
            WorkspaceId(_WORKSPACE),
            _audit_event(event_type="lease.revoked", event_id="e3"),
        )

    asyncio.run(_seed())
    with _make_client(providers=providers) as (client, _):
        resp = client.get(
            f"/v1/workspaces/{_WORKSPACE}/audit/leases",
            headers=_ctx_header(),
        )
    assert resp.status_code == 200
    body = resp.json()
    event_types = sorted(item["eventType"] for item in body["items"])
    assert event_types == ["lease.issued", "lease.revoked"]


def test_audit_leases_rejects_non_lease_event_type_filter() -> None:
    providers = _build_providers()
    with _make_client(providers=providers) as (client, _):
        resp = client.get(
            f"/v1/workspaces/{_WORKSPACE}/audit/leases",
            params={"eventType": "connector.instance.created"},
            headers=_ctx_header(),
        )
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "connector.audit_event_type_invalid"


def test_audit_leases_requires_audit_read_permission() -> None:
    providers = _build_providers()
    with _make_client(providers=providers) as (client, _):
        resp = client.get(
            f"/v1/workspaces/{_WORKSPACE}/audit/leases",
            headers=_ctx_header(permissions=[CONNECTOR_READ]),
        )
    assert resp.status_code == 403


# ===========================================================================
# OpenAPI snapshot
# ===========================================================================


def test_openapi_advertises_every_new_route() -> None:
    """Pin the CONN-IMPL-026 route table on /openapi.json.

    Mirrors the design's "Public Interface" table for the public REST
    surface; if a route is listed there but missing here, either the
    design or the impl has drifted.
    """
    with _make_client() as (client, _):
        resp = client.get("/openapi.json")
    assert resp.status_code == 200
    paths = set(resp.json().get("paths", {}).keys())
    expected = {
        "/v1/workspaces/{ws}/connector-types",
        "/v1/workspaces/{ws}/connectors",
        "/v1/workspaces/{ws}/connectors/{instance_id}",
        "/v1/workspaces/{ws}/connectors/{instance_id}:enable",
        "/v1/workspaces/{ws}/connectors/{instance_id}:disable",
        "/v1/workspaces/{ws}/connectors/{instance_id}/health",
        "/v1/workspaces/{ws}/connectors/{instance_id}:force-health-check",
        "/v1/workspaces/{ws}/connectors/{instance_id}/leases",
        "/v1/workspaces/{ws}/runs/{run_id}/leases",
        "/v1/workspaces/{ws}/leases/{lease_id}:revoke",
        "/v1/workspaces/{ws}/connectors/{instance_id}/leases:revoke-all",
        "/v1/workspaces/{ws}/runs/{run_id}/leases:revoke-all",
        "/v1/workspaces/{ws}/audit/leases",
    }
    missing = expected - paths
    assert not missing, f"missing routes in OpenAPI spec: {sorted(missing)}"


# Silence the "unused import" check on pytest — the file is collected
# directly by pytest's discovery without needing an explicit
# pytestmark, but linters still want a sentinel reference.
_ = pytest
