"""Unit tests for :class:`InstanceService`.

The service is exercised with in-memory fakes — no Postgres. The
catalog precedent uses the same pattern (see
``tests/test_loader_registry.py``).
"""

from __future__ import annotations

import pytest

from custos_connector.audit import EVENT_INSTANCE_CREATED, EVENT_INSTANCE_UPDATED
from custos_connector.instances import (
    ConnectorInstanceNotFound,
    ConnectorTypeNotRegistered,
    ImmutableFieldUpdate,
    InstanceService,
    InvalidInstancePayload,
    InvalidLeaseTtl,
)
from custos_connector.instances.validator import (
    InstanceConfigCode,
    InstanceConfigValidationError,
)
from tests._fakes import (
    FakeCatalogAdapter,
    FakeConnectorInstanceAdapter,
    FakeMetadataAdapter,
)

pytestmark = pytest.mark.asyncio


def _build_service() -> tuple[
    InstanceService,
    FakeConnectorInstanceAdapter,
    FakeCatalogAdapter,
    FakeMetadataAdapter,
]:
    catalog = FakeCatalogAdapter()
    instances = FakeConnectorInstanceAdapter()
    metadata = FakeMetadataAdapter()
    service = InstanceService(
        instance_store=instances,  # type: ignore[arg-type]
        catalog_store=catalog,  # type: ignore[arg-type]
        metadata_store=metadata,  # type: ignore[arg-type]
    )
    return service, instances, catalog, metadata


async def _register_type(catalog: FakeCatalogAdapter, type: str, version: str) -> None:
    """Register a connector-type-version with a minimal valid manifest.

    The manifest mirrors the production catalog shape by using
    top-level ``metadata`` / ``spec`` keys. The ``spec`` payload
    satisfies CONN-IMPL-012's instance config validator:
    ``target.kind=oci-registry`` with ``repositoryNamespace``
    already filled in by the manifest, plus
    ``credentials.authenticationType=oidc`` with the required
    ``issuerUri`` / ``audience`` already filled in. Tests that don't
    pass per-instance overrides therefore validate cleanly.
    """
    manifest: dict[str, object] = {
        "metadata": {
            "type": type,
            "version": version,
        },
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
    await catalog.put_connector_type_version(type, version, "sha256:fake", manifest)


# ---------------------------------------------------------------------------
# Create
# ---------------------------------------------------------------------------


async def test_create_persists_row_and_emits_audit() -> None:
    service, _instances, catalog, metadata = _build_service()
    await _register_type(catalog, "http", "1.0.0")

    row = await service.create(
        "ws-1",
        type="http",
        version="1.0.0",
        actor="user:alice",
        name="prod-http",
        lease_ttl_seconds=3600,
    )

    assert row.workspace_id == "ws-1"
    assert row.type == "http"
    assert row.version == "1.0.0"
    assert row.name == "prod-http"
    assert row.lease_ttl_seconds == 3600
    assert row.enabled is True
    assert row.status == "active"
    # Server-generated ID is a non-empty UUIDv4 string.
    assert len(str(row.instance_id)) >= 32

    # Audit emission happened with the right event_type + actor.
    assert len(metadata.append_audit_calls) == 1
    _ws, event = metadata.append_audit_calls[0]
    assert event.event_type == EVENT_INSTANCE_CREATED
    assert event.actor == "user:alice"
    assert event.subject["instance_id"] == str(row.instance_id)


async def test_create_rejects_unknown_connector_type() -> None:
    service, *_ = _build_service()
    with pytest.raises(ConnectorTypeNotRegistered) as exc_info:
        await service.create(
            "ws-1",
            type="bogus",
            version="9.9.9",
            actor="user:alice",
        )
    assert exc_info.value.type == "bogus"
    assert exc_info.value.version == "9.9.9"


async def test_create_persists_operator_supplied_config() -> None:
    """End-to-end: operator's target_config / credentials_authentication /
    used_capabilities flow through validation and reach the row."""
    service, _instances, catalog, _metadata = _build_service()
    await _register_type(catalog, "http", "1.0.0")

    row = await service.create(
        "ws-1",
        type="http",
        version="1.0.0",
        actor="user:alice",
        target_config={"repositoryNamespace": "ws-1/prod"},
        credentials_authentication={"issuerUri": "https://oidc.ws-1.example.com"},
        used_capabilities=("oci.registry.read",),
    )

    assert row.target_config == {"repositoryNamespace": "ws-1/prod"}
    assert row.credentials_authentication == {"issuerUri": "https://oidc.ws-1.example.com"}
    assert row.used_capabilities == ("oci.registry.read",)


async def test_create_rejects_unknown_capability_via_validator() -> None:
    service, _instances, catalog, _metadata = _build_service()
    await _register_type(catalog, "http", "1.0.0")

    with pytest.raises(InstanceConfigValidationError) as excinfo:
        await service.create(
            "ws-1",
            type="http",
            version="1.0.0",
            actor="user:alice",
            used_capabilities=("oci.image.push",),
        )
    codes = [i.code for i in excinfo.value.issues]
    assert InstanceConfigCode.UNKNOWN_CAPABILITY_ON_INSTANCE in codes


async def test_create_rejects_invalid_lease_ttl() -> None:
    service, _instances, catalog, _metadata = _build_service()
    await _register_type(catalog, "http", "1.0.0")
    with pytest.raises(InvalidLeaseTtl):
        await service.create(
            "ws-1",
            type="http",
            version="1.0.0",
            actor="user:alice",
            lease_ttl_seconds=0,
        )
    with pytest.raises(InvalidLeaseTtl):
        await service.create(
            "ws-1",
            type="http",
            version="1.0.0",
            actor="user:alice",
            lease_ttl_seconds=10**9,  # well above the 30-day ceiling
        )


async def test_create_rejects_empty_name() -> None:
    service, _instances, catalog, _metadata = _build_service()
    await _register_type(catalog, "http", "1.0.0")
    with pytest.raises(InvalidInstancePayload):
        await service.create(
            "ws-1",
            type="http",
            version="1.0.0",
            actor="user:alice",
            name="   ",
        )


# ---------------------------------------------------------------------------
# Get
# ---------------------------------------------------------------------------


async def test_get_returns_row_and_404_on_absent() -> None:
    service, _instances, catalog, _metadata = _build_service()
    await _register_type(catalog, "http", "1.0.0")
    created = await service.create("ws-1", type="http", version="1.0.0", actor="user:alice")

    got = await service.get("ws-1", str(created.instance_id))
    assert got.instance_id == created.instance_id

    with pytest.raises(ConnectorInstanceNotFound):
        await service.get("ws-1", "non-existent-id")


async def test_get_enforces_workspace_isolation() -> None:
    """A row in ws-A MUST NOT be visible from ws-B."""
    service, _instances, catalog, _metadata = _build_service()
    await _register_type(catalog, "http", "1.0.0")
    created = await service.create("ws-A", type="http", version="1.0.0", actor="user:alice")
    with pytest.raises(ConnectorInstanceNotFound):
        await service.get("ws-B", str(created.instance_id))


# ---------------------------------------------------------------------------
# Patch
# ---------------------------------------------------------------------------


async def test_patch_updates_allowed_fields_and_emits_audit() -> None:
    service, _instances, catalog, metadata = _build_service()
    await _register_type(catalog, "http", "1.0.0")
    created = await service.create(
        "ws-1",
        type="http",
        version="1.0.0",
        actor="user:alice",
        name="orig",
        enabled=True,
    )
    # Drop the create-emission so we can inspect only the patch event.
    metadata.append_audit_calls.clear()

    patched = await service.patch(
        "ws-1",
        str(created.instance_id),
        actor="user:bob",
        updates={"name": "renamed", "enabled": False, "lease_ttl_seconds": 7200},
    )
    assert patched.name == "renamed"
    assert patched.enabled is False
    assert patched.lease_ttl_seconds == 7200
    # Immutable fields untouched.
    assert patched.type == "http"
    assert patched.version == "1.0.0"
    assert patched.instance_id == created.instance_id

    assert len(metadata.append_audit_calls) == 1
    _ws, event = metadata.append_audit_calls[0]
    assert event.event_type == EVENT_INSTANCE_UPDATED
    assert event.actor == "user:bob"
    # Diff payload contains old + new for each touched field.
    changes = event.payload["changes"]
    assert changes["name"] == {"from": "orig", "to": "renamed"}
    assert changes["enabled"] == {"from": True, "to": False}
    assert changes["lease_ttl_seconds"] == {"from": None, "to": 7200}


async def test_patch_rejects_immutable_fields() -> None:
    service, _instances, catalog, _metadata = _build_service()
    await _register_type(catalog, "http", "1.0.0")
    created = await service.create("ws-1", type="http", version="1.0.0", actor="user:alice")
    with pytest.raises(ImmutableFieldUpdate) as exc_info:
        await service.patch(
            "ws-1",
            str(created.instance_id),
            actor="user:bob",
            updates={"type": "smtp"},
        )
    assert "type" in exc_info.value.fields


async def test_patch_rejects_server_only_fields() -> None:
    """The PATCH surface excludes ``status`` / ``health_status``.

    Those are server-mutated soft state set by CONN-IMPL-013; operator
    PATCH must not be able to forge an "active" status while
    bypassing the activation controller.
    """
    service, _instances, catalog, _metadata = _build_service()
    await _register_type(catalog, "http", "1.0.0")
    created = await service.create("ws-1", type="http", version="1.0.0", actor="user:alice")
    with pytest.raises(ImmutableFieldUpdate):
        await service.patch(
            "ws-1",
            str(created.instance_id),
            actor="user:bob",
            updates={"status": "disabled"},
        )


async def test_patch_404_when_absent() -> None:
    service, *_ = _build_service()
    with pytest.raises(ConnectorInstanceNotFound):
        await service.patch(
            "ws-1",
            "ghost-id",
            actor="user:bob",
            updates={"enabled": False},
        )


async def test_patch_workspace_isolation() -> None:
    """Patching from the wrong workspace MUST NOT mutate the row."""
    service, _instances, catalog, _metadata = _build_service()
    await _register_type(catalog, "http", "1.0.0")
    created = await service.create(
        "ws-A", type="http", version="1.0.0", actor="user:alice", name="A-name"
    )
    with pytest.raises(ConnectorInstanceNotFound):
        await service.patch(
            "ws-B",
            str(created.instance_id),
            actor="user:hijacker",
            updates={"name": "hijacked"},
        )
    # ws-A row is untouched.
    intact = await service.get("ws-A", str(created.instance_id))
    assert intact.name == "A-name"


async def test_patch_empty_updates_is_noop() -> None:
    service, _instances, catalog, metadata = _build_service()
    await _register_type(catalog, "http", "1.0.0")
    created = await service.create(
        "ws-1", type="http", version="1.0.0", actor="user:alice", name="orig"
    )
    metadata.append_audit_calls.clear()

    patched = await service.patch("ws-1", str(created.instance_id), actor="user:bob", updates={})
    # State is identical.
    assert patched.name == created.name
    assert patched.enabled == created.enabled
    # Audit still emitted but with an empty change-set.
    assert len(metadata.append_audit_calls) == 1
    _ws, event = metadata.append_audit_calls[0]
    assert event.payload["changes"] == {}


async def test_patch_rejects_invalid_lease_ttl() -> None:
    service, _instances, catalog, _metadata = _build_service()
    await _register_type(catalog, "http", "1.0.0")
    created = await service.create("ws-1", type="http", version="1.0.0", actor="user:alice")
    with pytest.raises(InvalidLeaseTtl):
        await service.patch(
            "ws-1",
            str(created.instance_id),
            actor="user:bob",
            updates={"lease_ttl_seconds": 0},
        )


# ---------------------------------------------------------------------------
# List
# ---------------------------------------------------------------------------


async def test_list_filters_by_type_and_enabled() -> None:
    service, _instances, catalog, _metadata = _build_service()
    await _register_type(catalog, "http", "1.0.0")
    await _register_type(catalog, "smtp", "1.0.0")
    a = await service.create("ws-1", type="http", version="1.0.0", actor="alice", name="a")
    b = await service.create(
        "ws-1", type="http", version="1.0.0", actor="alice", name="b", enabled=False
    )
    c = await service.create("ws-1", type="smtp", version="1.0.0", actor="alice", name="c")

    http_only = await service.list("ws-1", type="http")
    assert {i.instance_id for i in http_only.items} == {a.instance_id, b.instance_id}

    enabled_only = await service.list("ws-1", enabled=True)
    assert {i.instance_id for i in enabled_only.items} == {a.instance_id, c.instance_id}

    enabled_http = await service.list("ws-1", type="http", enabled=True)
    assert {i.instance_id for i in enabled_http.items} == {a.instance_id}


async def test_list_enforces_workspace_isolation() -> None:
    service, _instances, catalog, _metadata = _build_service()
    await _register_type(catalog, "http", "1.0.0")
    await service.create("ws-A", type="http", version="1.0.0", actor="alice")
    await service.create("ws-A", type="http", version="1.0.0", actor="alice")
    await service.create("ws-B", type="http", version="1.0.0", actor="alice")

    page_a = await service.list("ws-A")
    page_b = await service.list("ws-B")
    assert len(page_a.items) == 2
    assert len(page_b.items) == 1
    a_ids = {i.instance_id for i in page_a.items}
    b_ids = {i.instance_id for i in page_b.items}
    assert a_ids.isdisjoint(b_ids)
