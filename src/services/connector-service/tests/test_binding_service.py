"""Unit tests for :class:`BindForStepService` (CONN-IMPL-016, Phase G).

The service is exercised against the in-memory fakes from
``tests/_fakes.py``: the call sites construct ``ConnectorTypeVersion``
+ ``ConnectorInstance`` rows directly, register a stub identity
resolver per :mod:`custos_connector.identity` conventions, and use a
:class:`StubPluginBinder` so no Docker invocation happens.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, ClassVar
from uuid import uuid4

import pytest
from custos_spl.ids import ConnectorInstanceId, WorkspaceId
from custos_spl.interfaces.catalog_store import ConnectorTypeVersion
from custos_spl.interfaces.connector_instance_store import ConnectorInstance

from custos_connector.audit import (
    EVENT_BINDING_CREATED,
    EVENT_BINDING_REJECTED,
    EVENT_CAPABILITY_DEPRECATED,
)
from custos_connector.binding import (
    BindError,
    BindErrorCode,
    BindForStepRequest,
    BindSlotRequest,
)
from custos_connector.identity import (
    IdentityResolver,
    IdentityResolverError,
    IdentityResolverErrorCode,
    IdentityResolverRegistry,
    ResolvedIdentity,
)
from custos_connector.identity.protocols import IdentityResolverContext
from custos_connector.loader.identity import IdentityCategory
from custos_connector.runtime import (
    PluginErrorCode,
    PluginProtocolError,
    UpstreamUnreachable,
)
from tests._fakes import (
    FakeCatalogAdapter,
    FakeConnectorInstanceAdapter,
    FakeMetadataAdapter,
    StubPluginBinder,
    build_bind_for_step_service,
)

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_WORKSPACE = "ws-1"
_ACTOR = "workflow-service"


class _StubResolver:
    """Always-succeeds resolver returning a deterministic envelope."""

    authentication_type: ClassVar[str] = "oidc"
    category: ClassVar[IdentityCategory] = IdentityCategory.FEDERATED

    def __init__(self, *, material: Mapping[str, Any] | None = None) -> None:
        self._material = dict(material or {"token": "stub-token", "type": "Bearer"})

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
            material=self._material,
            descriptor="oidc:stub",
            issued_at=now,
            expires_at=None,
        )


class _FailingResolver:
    """Resolver that always raises :class:`IdentityResolverError`."""

    authentication_type: ClassVar[str] = "oidc"
    category: ClassVar[IdentityCategory] = IdentityCategory.FEDERATED

    async def resolve(
        self,
        *,
        credentials_authentication: Mapping[str, Any],
        context: IdentityResolverContext,
    ) -> ResolvedIdentity:
        del credentials_authentication, context
        raise IdentityResolverError(
            "upstream KMS unreachable",
            code=IdentityResolverErrorCode.UPSTREAM_UNAVAILABLE,
        )


def _make_connector_type(
    *,
    type_: str = "http",
    version: str = "1.0.0",
    capabilities: list[str | dict[str, Any]] | None = None,
) -> ConnectorTypeVersion:
    caps = capabilities if capabilities is not None else ["events.delivery", "objects.read"]
    return ConnectorTypeVersion(
        type=type_,
        version=version,
        digest="sha256:fake",
        image_ref=f"example.test/{type_}@sha256:fake",
        normalized_manifest={
            "metadata": {"type": type_, "version": version},
            "spec": {
                "capabilities": caps,
            },
        },
        parent_deprecated=False,
        published_at=datetime.now(UTC),
    )


def _make_instance(
    *,
    workspace_id: str = _WORKSPACE,
    type_: str = "http",
    version: str = "1.0.0",
    used_capabilities: tuple[str, ...] | None = ("events.delivery", "objects.read"),
    enabled: bool = True,
    health_status: str | None = "healthy",
    credentials_authentication: Mapping[str, Any] | None = None,
    lease_ttl_seconds: int = 3600,
) -> ConnectorInstance:
    now = datetime.now(UTC)
    creds = dict(credentials_authentication or {"type": "oidc"})
    return ConnectorInstance(
        workspace_id=WorkspaceId(workspace_id),
        instance_id=ConnectorInstanceId(str(uuid4())),
        type=type_,
        version=version,
        name="prod",
        lease_ttl_seconds=lease_ttl_seconds,
        enabled=enabled,
        status="active",
        health_status=health_status,
        target_config={},
        credentials_authentication=creds,
        used_capabilities=used_capabilities,
        created_at=now,
        updated_at=now,
    )


async def _seed(
    *,
    catalog: FakeCatalogAdapter,
    instance_store: FakeConnectorInstanceAdapter,
    instance: ConnectorInstance,
    connector_type: ConnectorTypeVersion,
) -> None:
    await catalog.put_connector_type_version(
        connector_type.type,
        connector_type.version,
        connector_type.digest,
        connector_type.image_ref,
        connector_type.normalized_manifest,
    )
    await instance_store.put_connector_instance(instance.workspace_id, instance)


def _registry_with(resolver: IdentityResolver) -> IdentityResolverRegistry:
    """Build a registry pre-loaded with ``resolver`` for the ``oidc`` token."""
    return IdentityResolverRegistry(resolvers=[resolver])


def _make_request(
    instance: ConnectorInstance,
    *,
    run_id: str = "run-1",
    step_id: str = "step-1",
    attempt: int = 1,
    step_key: str = "copy.v1",
    slot_name: str = "source",
    required_capabilities: tuple[str, ...] = ("events.delivery",),
) -> BindForStepRequest:
    return BindForStepRequest(
        run_id=run_id,
        step_id=step_id,
        attempt=attempt,
        step_key=step_key,
        slots=(
            BindSlotRequest(
                name=slot_name,
                instance_id=str(instance.instance_id),
                required_capabilities=required_capabilities,
            ),
        ),
        actor=_ACTOR,
    )


# ---------------------------------------------------------------------------
# Happy paths
# ---------------------------------------------------------------------------


async def test_bind_for_step_returns_one_context_per_slot() -> None:
    catalog = FakeCatalogAdapter()
    instances = FakeConnectorInstanceAdapter()
    metadata = FakeMetadataAdapter()
    src = _make_instance(used_capabilities=("events.delivery",))
    dst = _make_instance(used_capabilities=("objects.read",))
    connector_type = _make_connector_type()
    await _seed(
        catalog=catalog,
        instance_store=instances,
        instance=src,
        connector_type=connector_type,
    )
    await instances.put_connector_instance(dst.workspace_id, dst)

    stub_binder = StubPluginBinder()
    service = build_bind_for_step_service(
        catalog_store=catalog,
        instance_store=instances,
        metadata_store=metadata,
        identity_registry=_registry_with(_StubResolver()),
        plugin_binder=stub_binder,
    )

    request = BindForStepRequest(
        run_id="run-1",
        step_id="step-1",
        attempt=1,
        step_key="copy.v1",
        slots=(
            BindSlotRequest(
                name="source",
                instance_id=str(src.instance_id),
                required_capabilities=("events.delivery",),
            ),
            BindSlotRequest(
                name="destination",
                instance_id=str(dst.instance_id),
                required_capabilities=("objects.read",),
            ),
        ),
        actor=_ACTOR,
    )

    response = await service.bind_for_step(workspace_id=_WORKSPACE, request=request)

    # Slot order is preserved and every slot has a context.
    assert tuple(response.contexts.keys()) == ("source", "destination")
    assert response.contexts["source"].handle["slot"] == "source"
    assert response.contexts["destination"].handle["slot"] == "destination"
    # Plugin binder called once per slot with the primary capability.
    assert {call["slot"] for call in stub_binder.calls} == {"source", "destination"}
    assert all(
        call["capability"] in {"events.delivery", "objects.read"} for call in stub_binder.calls
    )
    # Audit: single binding.created carrying the slot → instance map.
    created = [e for _, e in metadata.append_audit_calls if e.event_type == EVENT_BINDING_CREATED]
    assert len(created) == 1
    assert set(created[0].payload["slots"].keys()) == {"source", "destination"}


async def test_bind_for_step_is_idempotent_for_same_step_attempt() -> None:
    catalog = FakeCatalogAdapter()
    instances = FakeConnectorInstanceAdapter()
    metadata = FakeMetadataAdapter()
    instance = _make_instance(used_capabilities=("events.delivery",))
    await _seed(
        catalog=catalog,
        instance_store=instances,
        instance=instance,
        connector_type=_make_connector_type(),
    )
    stub_binder = StubPluginBinder()
    service = build_bind_for_step_service(
        catalog_store=catalog,
        instance_store=instances,
        metadata_store=metadata,
        identity_registry=_registry_with(_StubResolver()),
        plugin_binder=stub_binder,
    )

    request = _make_request(instance)
    first = await service.bind_for_step(workspace_id=_WORKSPACE, request=request)
    second = await service.bind_for_step(workspace_id=_WORKSPACE, request=request)

    # Same response object — cache returns the exact ConnectorContext.
    assert first is second
    assert first.contexts["source"] is second.contexts["source"]
    # Plugin binder invoked exactly once across both calls.
    assert len(stub_binder.calls) == 1
    # Audit emitted exactly once.
    created = [e for _, e in metadata.append_audit_calls if e.event_type == EVENT_BINDING_CREATED]
    assert len(created) == 1


async def test_bind_for_step_emits_capability_deprecated_event() -> None:
    catalog = FakeCatalogAdapter()
    instances = FakeConnectorInstanceAdapter()
    metadata = FakeMetadataAdapter()
    instance = _make_instance(used_capabilities=("legacy.feed",))
    connector_type = _make_connector_type(
        capabilities=[
            {"name": "legacy.feed", "deprecated": True, "since": "0.9.0"},
        ],
    )
    await _seed(
        catalog=catalog,
        instance_store=instances,
        instance=instance,
        connector_type=connector_type,
    )
    service = build_bind_for_step_service(
        catalog_store=catalog,
        instance_store=instances,
        metadata_store=metadata,
        identity_registry=_registry_with(_StubResolver()),
        plugin_binder=StubPluginBinder(),
    )

    await service.bind_for_step(
        workspace_id=_WORKSPACE,
        request=_make_request(instance, required_capabilities=("legacy.feed",)),
    )

    deprecated_events = [
        e for _, e in metadata.append_audit_calls if e.event_type == EVENT_CAPABILITY_DEPRECATED
    ]
    assert len(deprecated_events) == 1
    assert deprecated_events[0].subject["capability"] == "legacy.feed"


# ---------------------------------------------------------------------------
# Rejection paths
# ---------------------------------------------------------------------------


async def test_bind_for_step_rejects_when_instance_missing() -> None:
    catalog = FakeCatalogAdapter()
    instances = FakeConnectorInstanceAdapter()
    metadata = FakeMetadataAdapter()
    service = build_bind_for_step_service(
        catalog_store=catalog,
        instance_store=instances,
        metadata_store=metadata,
        identity_registry=_registry_with(_StubResolver()),
        plugin_binder=StubPluginBinder(),
    )

    request = BindForStepRequest(
        run_id="run-1",
        step_id="step-1",
        attempt=1,
        step_key="copy.v1",
        slots=(
            BindSlotRequest(
                name="source",
                instance_id=str(uuid4()),
                required_capabilities=("events.delivery",),
            ),
        ),
        actor=_ACTOR,
    )

    with pytest.raises(BindError) as exc_info:
        await service.bind_for_step(workspace_id=_WORKSPACE, request=request)
    assert exc_info.value.code is BindErrorCode.INSTANCE_NOT_FOUND

    rejected = [e for _, e in metadata.append_audit_calls if e.event_type == EVENT_BINDING_REJECTED]
    assert len(rejected) == 1
    assert rejected[0].payload["reason_code"] == BindErrorCode.INSTANCE_NOT_FOUND.value


async def test_bind_for_step_treats_cross_workspace_as_not_found() -> None:
    catalog = FakeCatalogAdapter()
    instances = FakeConnectorInstanceAdapter()
    metadata = FakeMetadataAdapter()
    other = _make_instance(workspace_id="ws-other", used_capabilities=("events.delivery",))
    await _seed(
        catalog=catalog,
        instance_store=instances,
        instance=other,
        connector_type=_make_connector_type(),
    )
    service = build_bind_for_step_service(
        catalog_store=catalog,
        instance_store=instances,
        metadata_store=metadata,
        identity_registry=_registry_with(_StubResolver()),
        plugin_binder=StubPluginBinder(),
    )

    with pytest.raises(BindError) as exc_info:
        await service.bind_for_step(
            workspace_id=_WORKSPACE,
            request=_make_request(other),
        )
    assert exc_info.value.code is BindErrorCode.INSTANCE_NOT_FOUND


async def test_bind_for_step_rejects_disabled_instance() -> None:
    catalog = FakeCatalogAdapter()
    instances = FakeConnectorInstanceAdapter()
    metadata = FakeMetadataAdapter()
    instance = _make_instance(enabled=False, used_capabilities=("events.delivery",))
    await _seed(
        catalog=catalog,
        instance_store=instances,
        instance=instance,
        connector_type=_make_connector_type(),
    )
    service = build_bind_for_step_service(
        catalog_store=catalog,
        instance_store=instances,
        metadata_store=metadata,
        identity_registry=_registry_with(_StubResolver()),
        plugin_binder=StubPluginBinder(),
    )

    with pytest.raises(BindError) as exc_info:
        await service.bind_for_step(workspace_id=_WORKSPACE, request=_make_request(instance))
    assert exc_info.value.code is BindErrorCode.INSTANCE_DISABLED


async def test_bind_for_step_rejects_unhealthy_instance() -> None:
    catalog = FakeCatalogAdapter()
    instances = FakeConnectorInstanceAdapter()
    metadata = FakeMetadataAdapter()
    instance = _make_instance(
        health_status="unhealthy",
        used_capabilities=("events.delivery",),
    )
    await _seed(
        catalog=catalog,
        instance_store=instances,
        instance=instance,
        connector_type=_make_connector_type(),
    )
    service = build_bind_for_step_service(
        catalog_store=catalog,
        instance_store=instances,
        metadata_store=metadata,
        identity_registry=_registry_with(_StubResolver()),
        plugin_binder=StubPluginBinder(),
    )

    with pytest.raises(BindError) as exc_info:
        await service.bind_for_step(workspace_id=_WORKSPACE, request=_make_request(instance))
    assert exc_info.value.code is BindErrorCode.INSTANCE_UNHEALTHY


async def test_bind_for_step_rejects_on_capability_shortfall() -> None:
    catalog = FakeCatalogAdapter()
    instances = FakeConnectorInstanceAdapter()
    metadata = FakeMetadataAdapter()
    # Instance only carries events.delivery; request asks for objects.read.
    instance = _make_instance(used_capabilities=("events.delivery",))
    await _seed(
        catalog=catalog,
        instance_store=instances,
        instance=instance,
        connector_type=_make_connector_type(),
    )
    service = build_bind_for_step_service(
        catalog_store=catalog,
        instance_store=instances,
        metadata_store=metadata,
        identity_registry=_registry_with(_StubResolver()),
        plugin_binder=StubPluginBinder(),
    )

    with pytest.raises(BindError) as exc_info:
        await service.bind_for_step(
            workspace_id=_WORKSPACE,
            request=_make_request(instance, required_capabilities=("objects.read",)),
        )
    assert exc_info.value.code is BindErrorCode.CAPABILITY_SHORTFALL
    assert "objects.read" in exc_info.value.data["missing_capabilities"]


async def test_bind_for_step_folds_resolver_failure_into_bind_error() -> None:
    catalog = FakeCatalogAdapter()
    instances = FakeConnectorInstanceAdapter()
    metadata = FakeMetadataAdapter()
    instance = _make_instance(used_capabilities=("events.delivery",))
    await _seed(
        catalog=catalog,
        instance_store=instances,
        instance=instance,
        connector_type=_make_connector_type(),
    )
    service = build_bind_for_step_service(
        catalog_store=catalog,
        instance_store=instances,
        metadata_store=metadata,
        identity_registry=_registry_with(_FailingResolver()),
        plugin_binder=StubPluginBinder(),
    )

    with pytest.raises(BindError) as exc_info:
        await service.bind_for_step(workspace_id=_WORKSPACE, request=_make_request(instance))
    assert exc_info.value.code is BindErrorCode.IDENTITY_FAILED


async def test_bind_for_step_folds_plugin_failure_into_bind_error() -> None:
    catalog = FakeCatalogAdapter()
    instances = FakeConnectorInstanceAdapter()
    metadata = FakeMetadataAdapter()
    instance = _make_instance(used_capabilities=("events.delivery",))
    await _seed(
        catalog=catalog,
        instance_store=instances,
        instance=instance,
        connector_type=_make_connector_type(),
    )
    failing_binder = StubPluginBinder(
        raise_for_slot={"source": UpstreamUnreachable("plugin can't reach upstream")},
    )
    service = build_bind_for_step_service(
        catalog_store=catalog,
        instance_store=instances,
        metadata_store=metadata,
        identity_registry=_registry_with(_StubResolver()),
        plugin_binder=failing_binder,
    )

    with pytest.raises(BindError) as exc_info:
        await service.bind_for_step(workspace_id=_WORKSPACE, request=_make_request(instance))
    assert exc_info.value.code is BindErrorCode.UPSTREAM_BIND_FAILED
    assert exc_info.value.data["plugin_code"] == PluginErrorCode.UPSTREAM_UNREACHABLE.value


# ---------------------------------------------------------------------------
# Request-shape validation
# ---------------------------------------------------------------------------


async def test_bind_for_step_rejects_empty_slots() -> None:
    service = build_bind_for_step_service()
    request = BindForStepRequest(
        run_id="run-1",
        step_id="step-1",
        attempt=1,
        step_key="copy.v1",
        slots=(),
        actor=_ACTOR,
    )
    with pytest.raises(BindError) as exc_info:
        await service.bind_for_step(workspace_id=_WORKSPACE, request=request)
    assert exc_info.value.code is BindErrorCode.INVALID_REQUEST


async def test_bind_for_step_rejects_duplicate_slot_names() -> None:
    service = build_bind_for_step_service()
    request = BindForStepRequest(
        run_id="run-1",
        step_id="step-1",
        attempt=1,
        step_key="copy.v1",
        slots=(
            BindSlotRequest(
                name="source",
                instance_id=str(uuid4()),
                required_capabilities=("events.delivery",),
            ),
            BindSlotRequest(
                name="source",
                instance_id=str(uuid4()),
                required_capabilities=("events.delivery",),
            ),
        ),
        actor=_ACTOR,
    )
    with pytest.raises(BindError) as exc_info:
        await service.bind_for_step(workspace_id=_WORKSPACE, request=request)
    assert exc_info.value.code is BindErrorCode.INVALID_REQUEST


async def test_bind_for_step_rejects_slot_with_empty_required_capabilities() -> None:
    service = build_bind_for_step_service()
    request = BindForStepRequest(
        run_id="run-1",
        step_id="step-1",
        attempt=1,
        step_key="copy.v1",
        slots=(
            BindSlotRequest(
                name="source",
                instance_id=str(uuid4()),
                required_capabilities=(),
            ),
        ),
        actor=_ACTOR,
    )
    with pytest.raises(BindError) as exc_info:
        await service.bind_for_step(workspace_id=_WORKSPACE, request=request)
    assert exc_info.value.code is BindErrorCode.INVALID_REQUEST


async def test_protocol_error_from_plugin_is_bind_error() -> None:
    """A :class:`PluginProtocolError` (e.g. plugin returned bad payload)
    should fold into ``upstream-bind-failed`` like other plugin failures.
    """
    catalog = FakeCatalogAdapter()
    instances = FakeConnectorInstanceAdapter()
    metadata = FakeMetadataAdapter()
    instance = _make_instance(used_capabilities=("events.delivery",))
    await _seed(
        catalog=catalog,
        instance_store=instances,
        instance=instance,
        connector_type=_make_connector_type(),
    )
    binder = StubPluginBinder(
        raise_for_slot={"source": PluginProtocolError("malformed response")},
    )
    service = build_bind_for_step_service(
        catalog_store=catalog,
        instance_store=instances,
        metadata_store=metadata,
        identity_registry=_registry_with(_StubResolver()),
        plugin_binder=binder,
    )

    with pytest.raises(BindError) as exc_info:
        await service.bind_for_step(workspace_id=_WORKSPACE, request=_make_request(instance))
    assert exc_info.value.code is BindErrorCode.UPSTREAM_BIND_FAILED


# ---------------------------------------------------------------------------
# Bounded idempotency cache (TTL + LRU eviction)
# ---------------------------------------------------------------------------


class _FakeClock:
    """Manually-advanced clock for deterministic cache-expiry assertions."""

    def __init__(self, start: datetime | None = None) -> None:
        self._now = start or datetime(2026, 1, 1, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self._now

    def advance(self, seconds: int) -> None:
        self._now = self._now + timedelta(seconds=seconds)


async def test_bind_cache_evicts_expired_entry_and_re_resolves() -> None:
    """An entry whose TTL has elapsed must trigger a fresh resolve."""
    catalog = FakeCatalogAdapter()
    instances = FakeConnectorInstanceAdapter()
    metadata = FakeMetadataAdapter()
    instance = _make_instance(
        used_capabilities=("events.delivery",),
        lease_ttl_seconds=60,
    )
    await _seed(
        catalog=catalog,
        instance_store=instances,
        instance=instance,
        connector_type=_make_connector_type(),
    )
    binder = StubPluginBinder()
    clock = _FakeClock()
    service = build_bind_for_step_service(
        catalog_store=catalog,
        instance_store=instances,
        metadata_store=metadata,
        identity_registry=_registry_with(_StubResolver()),
        plugin_binder=binder,
        clock=clock,
    )

    request = _make_request(instance)
    await service.bind_for_step(workspace_id=_WORKSPACE, request=request)
    assert len(binder.calls) == 1, "first bind should hit the plugin"

    # Same idempotency key, well inside the TTL: cache hit.
    clock.advance(30)
    await service.bind_for_step(workspace_id=_WORKSPACE, request=request)
    assert len(binder.calls) == 1, "second bind inside TTL should be cached"

    # Cross the TTL boundary: the next bind must re-resolve.
    clock.advance(120)
    await service.bind_for_step(workspace_id=_WORKSPACE, request=request)
    assert len(binder.calls) == 2, "bind past TTL should bypass expired cache"


async def test_bind_cache_lru_evicts_when_full() -> None:
    """When the LRU cap is exceeded, the oldest entry is dropped."""
    catalog = FakeCatalogAdapter()
    instances = FakeConnectorInstanceAdapter()
    metadata = FakeMetadataAdapter()
    instance = _make_instance(
        used_capabilities=("events.delivery",),
        lease_ttl_seconds=3600,
    )
    await _seed(
        catalog=catalog,
        instance_store=instances,
        instance=instance,
        connector_type=_make_connector_type(),
    )
    binder = StubPluginBinder()
    service = build_bind_for_step_service(
        catalog_store=catalog,
        instance_store=instances,
        metadata_store=metadata,
        identity_registry=_registry_with(_StubResolver()),
        plugin_binder=binder,
        max_cache_entries=2,
    )

    # Three distinct idempotency keys: differing step_id forces unique
    # cache rows even though everything else matches.
    req_a = _make_request(instance, step_id="step-a")
    req_b = _make_request(instance, step_id="step-b")
    req_c = _make_request(instance, step_id="step-c")

    await service.bind_for_step(workspace_id=_WORKSPACE, request=req_a)
    await service.bind_for_step(workspace_id=_WORKSPACE, request=req_b)
    await service.bind_for_step(workspace_id=_WORKSPACE, request=req_c)
    assert len(binder.calls) == 3, "three distinct keys → three plugin calls"

    # The oldest (req_a) should have been evicted by req_c, so
    # re-issuing req_a must re-invoke the plugin while req_b/req_c
    # stay cached.
    await service.bind_for_step(workspace_id=_WORKSPACE, request=req_b)
    await service.bind_for_step(workspace_id=_WORKSPACE, request=req_c)
    assert len(binder.calls) == 3, "req_b and req_c should remain cached"

    await service.bind_for_step(workspace_id=_WORKSPACE, request=req_a)
    assert len(binder.calls) == 4, "evicted req_a must re-resolve"


async def test_bind_cache_skipped_when_lease_ttl_is_zero() -> None:
    """A non-positive lease TTL must keep the cache empty (fail-closed)."""
    catalog = FakeCatalogAdapter()
    instances = FakeConnectorInstanceAdapter()
    metadata = FakeMetadataAdapter()
    instance = _make_instance(
        used_capabilities=("events.delivery",),
        lease_ttl_seconds=0,
    )
    await _seed(
        catalog=catalog,
        instance_store=instances,
        instance=instance,
        connector_type=_make_connector_type(),
    )
    binder = StubPluginBinder()
    service = build_bind_for_step_service(
        catalog_store=catalog,
        instance_store=instances,
        metadata_store=metadata,
        identity_registry=_registry_with(_StubResolver()),
        plugin_binder=binder,
    )

    request = _make_request(instance)
    await service.bind_for_step(workspace_id=_WORKSPACE, request=request)
    await service.bind_for_step(workspace_id=_WORKSPACE, request=request)
    assert len(binder.calls) == 2, "zero lease TTL must not be cached"


async def test_bind_cache_ttl_cap_clamps_long_leases() -> None:
    """A lease TTL exceeding the cap must expire after the cap, not the lease."""
    catalog = FakeCatalogAdapter()
    instances = FakeConnectorInstanceAdapter()
    metadata = FakeMetadataAdapter()
    instance = _make_instance(
        used_capabilities=("events.delivery",),
        lease_ttl_seconds=24 * 3600,
    )
    await _seed(
        catalog=catalog,
        instance_store=instances,
        instance=instance,
        connector_type=_make_connector_type(),
    )
    binder = StubPluginBinder()
    clock = _FakeClock()
    service = build_bind_for_step_service(
        catalog_store=catalog,
        instance_store=instances,
        metadata_store=metadata,
        identity_registry=_registry_with(_StubResolver()),
        plugin_binder=binder,
        cache_ttl_cap_seconds=60,
        clock=clock,
    )

    request = _make_request(instance)
    await service.bind_for_step(workspace_id=_WORKSPACE, request=request)
    assert len(binder.calls) == 1

    # Advance past the cap (60s) but well within the lease TTL (24h).
    clock.advance(90)
    await service.bind_for_step(workspace_id=_WORKSPACE, request=request)
    assert len(binder.calls) == 2, "cap must override the lease TTL"


async def test_bind_cache_ttl_cap_zero_disables_caching() -> None:
    """Setting the TTL cap to zero must bypass the cache entirely."""
    catalog = FakeCatalogAdapter()
    instances = FakeConnectorInstanceAdapter()
    metadata = FakeMetadataAdapter()
    instance = _make_instance(used_capabilities=("events.delivery",))
    await _seed(
        catalog=catalog,
        instance_store=instances,
        instance=instance,
        connector_type=_make_connector_type(),
    )
    binder = StubPluginBinder()
    service = build_bind_for_step_service(
        catalog_store=catalog,
        instance_store=instances,
        metadata_store=metadata,
        identity_registry=_registry_with(_StubResolver()),
        plugin_binder=binder,
        cache_ttl_cap_seconds=0,
    )

    request = _make_request(instance)
    await service.bind_for_step(workspace_id=_WORKSPACE, request=request)
    await service.bind_for_step(workspace_id=_WORKSPACE, request=request)
    assert len(binder.calls) == 2, "ttl cap = 0 must disable caching"


async def test_bind_service_rejects_invalid_cache_configuration() -> None:
    """Constructor validation must reject nonsensical cache bounds."""
    with pytest.raises(ValueError):
        build_bind_for_step_service(max_cache_entries=0)
    with pytest.raises(ValueError):
        build_bind_for_step_service(cache_ttl_cap_seconds=-1)
