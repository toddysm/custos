"""``BindForStep`` orchestration service (CONN-IMPL-016, Phase G).

Per the design § "Operation: Bind Multi-Connector Step", Workflow
Service calls :meth:`BindForStepService.bind_for_step` once per step
before scheduling the activity. The binder:

1. Looks up each slot's :class:`ConnectorInstance` in the caller's
   workspace and the backing :class:`ConnectorTypeVersion` in the
   catalog. Workspace-mismatched or absent instances surface as
   :attr:`BindErrorCode.INSTANCE_NOT_FOUND`.
2. Validates each slot's ``required_capabilities ⊆
   instance.used_capabilities``. Shortfalls map to
   :attr:`BindErrorCode.CAPABILITY_SHORTFALL`.
3. Emits one :data:`~custos_connector.audit.EVENT_CAPABILITY_DEPRECATED`
   event per deprecated capability the slot consumes (advisory only —
   the bind still proceeds).
4. Resolves identity for each slot through the shared
   :class:`IdentityResolverRegistry`. Resolver failures surface as
   :attr:`BindErrorCode.IDENTITY_FAILED`.
5. Invokes the plugin ``bind`` hook for each slot, producing one
   :class:`ConnectorContext` per slot. Plugin failures surface as
   :attr:`BindErrorCode.UPSTREAM_BIND_FAILED`.
6. Emits a single :data:`~custos_connector.audit.EVENT_BINDING_CREATED`
   audit event on success or
   :data:`~custos_connector.audit.EVENT_BINDING_REJECTED` on any
   rejection.

Idempotency
-----------

The binder maintains an in-memory cache keyed by
``(workspace_id, run_id, step_id, attempt)``. A re-bind for the same
key returns the cached :class:`BindForStepResponse` without
re-resolving identity, re-invoking the plugin, or re-emitting audit
events.

The cache is **process-local**: it does not survive a service restart
and is not shared across replicas. For M1 (single-replica deployments)
this matches the design's acceptance criterion ("re-bind for same
``(runId, stepId, attempt)`` returns identical handles") under normal
operation. A durable Postgres-backed binding table is a follow-up
ticket and will replace this implementation transparently.

Concurrent re-binds for the same key collapse onto a single upstream
resolve via the per-key lock pattern introduced in CONN-IMPL-015 (see
:class:`IdentityResolverRegistry`).
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from custos_spl.ids import ConnectorInstanceId, WorkspaceId
from custos_spl.interfaces.catalog_store import (
    CatalogStoreProvider,
    ConnectorTypeVersion,
)
from custos_spl.interfaces.connector_instance_store import (
    ConnectorInstance,
    ConnectorInstanceStoreProvider,
)
from custos_spl.interfaces.metadata_store import MetadataStoreProvider

from custos_connector.audit import (
    audit_binding_created,
    audit_binding_rejected,
    audit_capability_deprecated,
)
from custos_connector.binding.errors import BindError, BindErrorCode
from custos_connector.binding.models import (
    BindForStepRequest,
    BindForStepResponse,
    BindSlotRequest,
)
from custos_connector.identity import (
    IdentityResolverError,
    IdentityResolverRegistry,
    ResolvedIdentity,
)
from custos_connector.runtime import (
    ConnectorContext,
    PluginRuntimeError,
)

# ---------------------------------------------------------------------------
# Plugin-bind transport boundary
# ---------------------------------------------------------------------------


class PluginBinder(Protocol):
    """Structural type for the plugin's ``bind`` hook invoker.

    :class:`custos_connector.runtime.PluginInvoker` already satisfies
    this signature; introducing the Protocol lets unit tests inject a
    stub without depending on the Docker runtime.
    """

    async def bind(
        self,
        *,
        connector: ConnectorTypeVersion,
        instance: ConnectorInstance,
        slot: str,
        capability: str,
        identity_material: Mapping[str, Any],
    ) -> ConnectorContext: ...


# ---------------------------------------------------------------------------
# Idempotency primitives
# ---------------------------------------------------------------------------

#: Idempotency cache key: (workspace_id, run_id, step_id, attempt).
_CacheKey = tuple[str, str, str, int]


@dataclass(slots=True)
class _KeyLock:
    """Per-cache-key serialization primitive.

    Mirrors :class:`custos_connector.identity.registry._KeyLock`: at
    most one :class:`asyncio.Lock` per *active* cache key so concurrent
    callers for the same key collapse onto a single resolve while
    callers for different keys run in parallel. ``waiters`` is a
    refcount used to evict the entry once nobody is using it.
    """

    lock: asyncio.Lock
    waiters: int = 0


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


class BindForStepService:
    """Orchestrates the ``BindForStep`` RPC.

    Constructed once per FastAPI app and held on
    :attr:`custos_connector.providers.Providers.bind_for_step_service`.
    Each request flows through :meth:`bind_for_step`, which fans out
    over the supplied slots while enforcing the validation +
    idempotency contract above.

    Args:
        catalog_store: SPL provider used to read
            :class:`ConnectorTypeVersion` rows (capabilities,
            deprecation envelope, manifest).
        instance_store: SPL provider used to read
            :class:`ConnectorInstance` rows (workspace-scoped).
        metadata_store: SPL provider used by the audit-emission
            helpers.
        identity_registry: Shared identity resolver registry from
            CONN-IMPL-015.
        plugin_binder: Plugin-bind hook invoker. Production wiring
            uses :class:`PluginInvoker` from
            :mod:`custos_connector.runtime`; tests substitute a stub.
    """

    def __init__(
        self,
        *,
        catalog_store: CatalogStoreProvider,
        instance_store: ConnectorInstanceStoreProvider,
        metadata_store: MetadataStoreProvider,
        identity_registry: IdentityResolverRegistry,
        plugin_binder: PluginBinder,
    ) -> None:
        self._catalog = catalog_store
        self._instances = instance_store
        self._metadata = metadata_store
        self._identity = identity_registry
        self._plugin = plugin_binder
        self._cache: dict[_CacheKey, BindForStepResponse] = {}
        self._key_locks: dict[_CacheKey, _KeyLock] = {}
        self._key_locks_mutex = asyncio.Lock()

    # ------------------------------------------------------------------
    # Public surface
    # ------------------------------------------------------------------

    async def bind_for_step(
        self,
        *,
        workspace_id: str,
        request: BindForStepRequest,
    ) -> BindForStepResponse:
        """Resolve every slot in ``request`` and return their contexts.

        Re-binding the same ``(workspace_id, run_id, step_id,
        attempt)`` returns the cached :class:`BindForStepResponse`
        without re-resolving identity, re-invoking the plugin, or
        re-emitting audit events.

        Raises:
            BindError: On any rejection (request shape, missing or
                disabled instance, capability shortfall, identity
                failure, plugin failure). The corresponding
                ``connector.binding.rejected`` audit event is emitted
                before the exception is re-raised.
        """
        self._validate_request_shape(request)

        cache_key: _CacheKey = (
            workspace_id,
            request.run_id,
            request.step_id,
            request.attempt,
        )

        # Lock-free cache check (cache hits don't need serialization).
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached

        key_lock = await self._acquire_key_lock(cache_key)
        try:
            async with key_lock.lock:
                # Re-check inside the per-key lock so concurrent callers
                # for the same step-attempt collapse onto a single
                # resolve.
                cached = self._cache.get(cache_key)
                if cached is not None:
                    return cached

                response = await self._resolve_uncached(
                    workspace_id=workspace_id,
                    request=request,
                )
                self._cache[cache_key] = response
                return response
        finally:
            await self._release_key_lock(cache_key)

    # ------------------------------------------------------------------
    # Internals: request shape
    # ------------------------------------------------------------------

    def _validate_request_shape(self, request: BindForStepRequest) -> None:
        """Cheap, sync request validation — fails fast before any I/O."""
        if not request.run_id.strip():
            raise BindError(BindErrorCode.INVALID_REQUEST, "run_id must be non-empty")
        if not request.step_id.strip():
            raise BindError(BindErrorCode.INVALID_REQUEST, "step_id must be non-empty")
        if not request.step_key.strip():
            raise BindError(BindErrorCode.INVALID_REQUEST, "step_key must be non-empty")
        if request.attempt < 1:
            raise BindError(BindErrorCode.INVALID_REQUEST, "attempt must be >= 1")
        if not request.slots:
            raise BindError(BindErrorCode.INVALID_REQUEST, "slots must be non-empty")

        seen_names: set[str] = set()
        for slot in request.slots:
            if not slot.name.strip():
                raise BindError(BindErrorCode.INVALID_REQUEST, "slot.name must be non-empty")
            if slot.name in seen_names:
                raise BindError(
                    BindErrorCode.INVALID_REQUEST,
                    f"duplicate slot name {slot.name!r}",
                    slot=slot.name,
                )
            seen_names.add(slot.name)
            if not slot.instance_id.strip():
                raise BindError(
                    BindErrorCode.INVALID_REQUEST,
                    f"slot {slot.name!r} instance_id must be non-empty",
                    slot=slot.name,
                )
            if not slot.required_capabilities:
                raise BindError(
                    BindErrorCode.INVALID_REQUEST,
                    f"slot {slot.name!r} required_capabilities must be non-empty",
                    slot=slot.name,
                    instance_id=slot.instance_id,
                )

            seen_capabilities: set[str] = set()
            for capability in slot.required_capabilities:
                if not capability.strip():
                    raise BindError(
                        BindErrorCode.INVALID_REQUEST,
                        f"slot {slot.name!r} required_capabilities must not contain empty entries",
                        slot=slot.name,
                        instance_id=slot.instance_id,
                    )
                if capability != capability.strip():
                    raise BindError(
                        BindErrorCode.INVALID_REQUEST,
                        f"slot {slot.name!r} capability {capability!r} must not contain surrounding whitespace",
                        slot=slot.name,
                        instance_id=slot.instance_id,
                    )
                if capability in seen_capabilities:
                    raise BindError(
                        BindErrorCode.INVALID_REQUEST,
                        f"slot {slot.name!r} has duplicate required capability {capability!r}",
                        slot=slot.name,
                        instance_id=slot.instance_id,
                    )
                seen_capabilities.add(capability)

    # ------------------------------------------------------------------
    # Internals: orchestration
    # ------------------------------------------------------------------

    async def _resolve_uncached(
        self,
        *,
        workspace_id: str,
        request: BindForStepRequest,
    ) -> BindForStepResponse:
        """Run validation + resolve + plugin-bind for every slot.

        Wraps the entire fan-out in a try/except so any
        :class:`BindError` raised by a slot's resolution path triggers
        a :data:`EVENT_BINDING_REJECTED` audit event before being
        re-raised. The success path emits a single
        :data:`EVENT_BINDING_CREATED` once every slot has resolved.
        """
        try:
            resolved: list[tuple[str, ConnectorContext]] = []
            slot_instance_map: dict[str, str] = {}
            for slot in request.slots:
                ctx, instance = await self._resolve_slot(
                    workspace_id=workspace_id,
                    request=request,
                    slot=slot,
                )
                resolved.append((slot.name, ctx))
                slot_instance_map[slot.name] = str(instance.instance_id)
        except BindError as exc:
            await audit_binding_rejected(
                self._metadata,
                workspace_id=workspace_id,
                actor=request.actor,
                run_id=request.run_id,
                step_id=request.step_id,
                attempt=request.attempt,
                step_key=request.step_key,
                slot=exc.slot,
                instance_id=exc.instance_id,
                reason_code=str(exc.code),
                reason_detail=exc.detail,
            )
            raise

        await audit_binding_created(
            self._metadata,
            workspace_id=workspace_id,
            actor=request.actor,
            run_id=request.run_id,
            step_id=request.step_id,
            attempt=request.attempt,
            step_key=request.step_key,
            slots=slot_instance_map,
        )
        return BindForStepResponse.build(resolved)

    async def _resolve_slot(
        self,
        *,
        workspace_id: str,
        request: BindForStepRequest,
        slot: BindSlotRequest,
    ) -> tuple[ConnectorContext, ConnectorInstance]:
        """Resolve one slot end-to-end.

        Returns the :class:`ConnectorContext` together with the
        :class:`ConnectorInstance` so the caller can build the
        slot→instance map used by the success audit event.
        """
        instance = await self._load_instance(workspace_id, slot)
        if not instance.enabled:
            raise BindError(
                BindErrorCode.INSTANCE_DISABLED,
                f"connector instance {slot.instance_id!r} is disabled",
                slot=slot.name,
                instance_id=slot.instance_id,
            )
        if not _is_healthy(instance):
            raise BindError(
                BindErrorCode.INSTANCE_UNHEALTHY,
                f"connector instance {slot.instance_id!r} is unhealthy "
                f"(health_status={instance.health_status!r})",
                slot=slot.name,
                instance_id=slot.instance_id,
                data={"health_status": instance.health_status},
            )

        connector_type = await self._load_connector_type(slot, instance)
        capability_index = _capability_index(connector_type)
        self._check_capability_coverage(slot, instance, capability_index)
        await self._emit_deprecated_capability_events(
            workspace_id=workspace_id,
            request=request,
            slot=slot,
            instance=instance,
            capability_index=capability_index,
        )
        identity = await self._resolve_identity(
            workspace_id=workspace_id,
            request=request,
            slot=slot,
            instance=instance,
        )
        ctx = await self._invoke_plugin_bind(
            slot=slot,
            instance=instance,
            connector_type=connector_type,
            identity=identity,
        )
        return ctx, instance

    # ------------------------------------------------------------------
    # Internals: per-step helpers
    # ------------------------------------------------------------------

    async def _load_instance(
        self,
        workspace_id: str,
        slot: BindSlotRequest,
    ) -> ConnectorInstance:
        """Workspace-scoped instance lookup.

        Cross-workspace and absent instances are indistinguishable at
        the adapter surface (the SPL contract) — both return ``None``
        which we translate into :attr:`BindErrorCode.INSTANCE_NOT_FOUND`.
        """
        instance = await self._instances.get_connector_instance(
            WorkspaceId(workspace_id),
            ConnectorInstanceId(slot.instance_id),
        )
        if instance is None:
            raise BindError(
                BindErrorCode.INSTANCE_NOT_FOUND,
                f"connector instance {slot.instance_id!r} not found in workspace",
                slot=slot.name,
                instance_id=slot.instance_id,
            )
        return instance

    async def _load_connector_type(
        self,
        slot: BindSlotRequest,
        instance: ConnectorInstance,
    ) -> ConnectorTypeVersion:
        """Load the ``ConnectorTypeVersion`` backing ``instance``.

        Absence here is unusual (it implies the catalog row was
        deleted out from under a live instance), but we still surface
        it as ``INSTANCE_NOT_FOUND`` rather than crashing — the bind
        precondition (a live, executable connector type) is genuinely
        unmet.
        """
        connector_type = await self._catalog.get_connector_type_version(
            instance.type, instance.version
        )
        if connector_type is None:
            raise BindError(
                BindErrorCode.INSTANCE_NOT_FOUND,
                f"connector type {instance.type!r}@{instance.version!r} not found in catalog",
                slot=slot.name,
                instance_id=slot.instance_id,
                data={"type": instance.type, "version": instance.version},
            )
        return connector_type

    def _check_capability_coverage(
        self,
        slot: BindSlotRequest,
        instance: ConnectorInstance,
        capability_index: Mapping[str, bool],
    ) -> None:
        """Enforce ``required_capabilities ⊆ used_capabilities ⊆ type``.

        The manifest's capability set is the upper bound (a type
        cannot declare coverage it did not register), and the
        instance's ``used_capabilities`` is the operator-approved
        narrowing of that set. The request fails if any required
        capability is missing from *either* level.
        """
        used = set(instance.used_capabilities or ())
        manifest_caps = set(capability_index)
        missing: list[str] = []
        for capability in slot.required_capabilities:
            if capability not in manifest_caps or capability not in used:
                missing.append(capability)
        if missing:
            raise BindError(
                BindErrorCode.CAPABILITY_SHORTFALL,
                f"slot {slot.name!r} requires capabilities {sorted(missing)!r} "
                f"that are not in the instance's used_capabilities",
                slot=slot.name,
                instance_id=slot.instance_id,
                data={
                    "missing_capabilities": sorted(missing),
                    "required_capabilities": list(slot.required_capabilities),
                    "used_capabilities": sorted(used),
                },
            )

    async def _emit_deprecated_capability_events(
        self,
        *,
        workspace_id: str,
        request: BindForStepRequest,
        slot: BindSlotRequest,
        instance: ConnectorInstance,
        capability_index: Mapping[str, bool],
    ) -> None:
        """Fire ``connector.capability.deprecated`` once per deprecated cap."""
        for capability in slot.required_capabilities:
            if capability_index.get(capability, False):
                await audit_capability_deprecated(
                    self._metadata,
                    workspace_id=workspace_id,
                    actor=request.actor,
                    instance_id=str(instance.instance_id),
                    type_name=instance.type,
                    version=instance.version,
                    capability=capability,
                    run_id=request.run_id,
                    step_id=request.step_id,
                    attempt=request.attempt,
                )

    async def _resolve_identity(
        self,
        *,
        workspace_id: str,
        request: BindForStepRequest,
        slot: BindSlotRequest,
        instance: ConnectorInstance,
    ) -> ResolvedIdentity:
        """Route through :class:`IdentityResolverRegistry`.

        :class:`IdentityResolverError` (the resolver's own taxonomy)
        is folded into :attr:`BindErrorCode.IDENTITY_FAILED`; the
        registry has already emitted ``connector.identity.failed`` by
        this point so we don't double-audit.
        """
        try:
            return await self._identity.resolve(
                workspace_id=workspace_id,
                actor=request.actor,
                instance_id=str(instance.instance_id),
                authentication_type=_authentication_type(instance),
                credentials_authentication=instance.credentials_authentication,
                lease_ttl_seconds=instance.lease_ttl_seconds or 0,
            )
        except IdentityResolverError as exc:
            raise BindError(
                BindErrorCode.IDENTITY_FAILED,
                f"identity resolver failed for slot {slot.name!r}: {exc.detail}",
                slot=slot.name,
                instance_id=str(instance.instance_id),
                data={
                    "resolver_code": str(exc.code),
                    "resolver_detail": exc.detail,
                },
            ) from exc

    async def _invoke_plugin_bind(
        self,
        *,
        slot: BindSlotRequest,
        instance: ConnectorInstance,
        connector_type: ConnectorTypeVersion,
        identity: ResolvedIdentity,
    ) -> ConnectorContext:
        """Call the plugin's ``bind`` hook with the first required cap.

        The plugin bind hook is per-(slot, capability). For slots with
        multiple required capabilities the first element of
        ``required_capabilities`` is the primary; the slot's
        :class:`ConnectorContext` is reused across all the capabilities
        the step subsequently invokes through that slot.
        """
        primary_capability = slot.required_capabilities[0]
        try:
            return await self._plugin.bind(
                connector=connector_type,
                instance=instance,
                slot=slot.name,
                capability=primary_capability,
                identity_material=identity.material,
            )
        except PluginRuntimeError as exc:
            raise BindError(
                BindErrorCode.UPSTREAM_BIND_FAILED,
                f"plugin bind hook failed for slot {slot.name!r}: {exc.detail}",
                slot=slot.name,
                instance_id=str(instance.instance_id),
                data={
                    "plugin_code": str(exc.code),
                    "plugin_detail": exc.detail,
                },
            ) from exc

    # ------------------------------------------------------------------
    # Internals: per-key locking
    # ------------------------------------------------------------------

    async def _acquire_key_lock(self, cache_key: _CacheKey) -> _KeyLock:
        """Return the per-key lock, incrementing its waiter refcount."""
        async with self._key_locks_mutex:
            key_lock = self._key_locks.get(cache_key)
            if key_lock is None:
                key_lock = _KeyLock(lock=asyncio.Lock())
                self._key_locks[cache_key] = key_lock
            key_lock.waiters += 1
            return key_lock

    async def _release_key_lock(self, cache_key: _CacheKey) -> None:
        """Drop the waiter refcount; evict the entry when it hits zero."""
        async with self._key_locks_mutex:
            key_lock = self._key_locks.get(cache_key)
            if key_lock is None:  # defensive: paired with acquire
                return
            key_lock.waiters -= 1
            if key_lock.waiters <= 0:
                del self._key_locks[cache_key]


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _is_healthy(instance: ConnectorInstance) -> bool:
    """Return ``True`` if the instance is bindable based on health.

    ``health_status`` is server-mutated soft state populated by
    CONN-IMPL-013 (health probe pipeline). We treat the absence of a
    health status (``None``) as bindable — instances that have never
    been probed should not be blocked from their first bind.
    """
    if instance.health_status is None:
        return True
    return instance.health_status == "healthy"


def _capability_index(
    connector_type: ConnectorTypeVersion,
) -> Mapping[str, bool]:
    """Return a mapping of capability-name → ``deprecated`` flag.

    The manifest schema accepts capabilities as either plain strings
    (undeprecated) or ``{name, deprecated, since, removeIn}`` objects.
    This helper normalizes both forms so the caller can query a single
    surface.
    """
    spec = connector_type.normalized_manifest.get("spec")
    if not isinstance(spec, Mapping):
        return {}
    entries = spec.get("capabilities")
    if not isinstance(entries, Sequence) or isinstance(entries, (str, bytes)):
        return {}
    index: dict[str, bool] = {}
    for entry in entries:
        if isinstance(entry, str):
            index[entry] = False
        elif isinstance(entry, Mapping):
            name = entry.get("name")
            if isinstance(name, str):
                index[name] = bool(entry.get("deprecated", False))
    return index


def _authentication_type(instance: ConnectorInstance) -> str:
    """Read the manifest ``authentication`` token from the instance.

    ``credentials_authentication`` is a frozen mapping shaped by the
    catalog manifest's ``credentials.authentication`` schema; the
    convention used by every connector type is to carry the resolver
    token under the ``type`` key. We surface the absence as an
    invalid-request rather than a resolver failure so the operator
    sees the right diagnostic.
    """
    auth_type = instance.credentials_authentication.get("type")
    if not isinstance(auth_type, str) or not auth_type:
        raise BindError(
            BindErrorCode.INVALID_REQUEST,
            f"connector instance {instance.instance_id!r} "
            "credentials_authentication is missing 'type'",
            instance_id=str(instance.instance_id),
        )
    return auth_type


__all__ = [
    "BindForStepService",
    "PluginBinder",
]
