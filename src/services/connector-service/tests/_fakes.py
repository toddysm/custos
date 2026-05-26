"""In-memory fakes for the SPL provider Protocols used by connector-service tests.

Each fake implements the slice of the SPL Protocol surface that
connector-service Phase B and Phase D actually exercise:

* ``MigrationCapable``: ``declared_revisions`` + ``refresh_declared`` so the
  schema-revision startup gate can run.
* A minimal ``append_audit`` recorder on the metadata fake so audit-emission
  unit tests can assert event shape without standing up Postgres.
* The connector-type registry methods (``put_connector_type_version``,
  ``get_connector_type_version``, ``list_connector_type_versions``,
  ``set_connector_type_deprecated``) that the Plugin Loader (CONN-IMPL-008)
  drives. The fake enforces SPL's ``ConflictDigest`` contract on
  identical-key digest divergence.

The full SPL Protocol surface (cursors, leases, audit outbox, etc.) is
exercised by the conformance suite under ``src/libs/custos-postgres/tests/``
and the Phase B integration test that drives the real ``PgCatalogAdapter`` /
``PgMetadataAdapter`` against a live database.
"""

from __future__ import annotations

from collections.abc import Mapping
from collections.abc import Set as AbstractSet
from datetime import UTC, datetime
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, cast

from custos_spl import ConflictDigest, ImmutableViolation
from custos_spl.ids import ConnectorInstanceId, WorkspaceId
from custos_spl.interfaces.catalog_store import (
    CatalogStoreProvider,
    ConnectorTypeVersion,
)
from custos_spl.interfaces.connector_instance_store import (
    ConnectorInstance,
    ConnectorInstanceFilter,
    ConnectorInstanceStoreProvider,
)
from custos_spl.interfaces.metadata_store import MetadataStoreProvider
from custos_spl.pagination import Cursor, Page

from custos_connector.binding import BindForStepService
from custos_connector.identity import IdentityResolverRegistry
from custos_connector.runtime import ConnectorContext

if TYPE_CHECKING:
    from custos_spl.interfaces.metadata_store import AuditEvent


class FakeCatalogAdapter:
    """In-memory ``CatalogStoreProvider`` for connector-service tests.

    Implements the ``MigrationCapable`` surface plus the connector-type
    registry slice the Plugin Loader (CONN-IMPL-008) drives.
    """

    SCHEMA_REVISION = 2

    def __init__(self, *, applied_revisions: AbstractSet[int] | None = None) -> None:
        self._applied: set[int] = set({1, 2} if applied_revisions is None else applied_revisions)
        self.refresh_calls = 0
        # Per-(type, version) row store + parent-type deprecation flag.
        # The parent_deprecated flag is denormalised onto every returned
        # row to mirror SPL's read-side contract.
        self._rows: dict[tuple[str, str], ConnectorTypeVersion] = {}
        self._deprecated_types: set[str] = set()

    @property
    def declared_revisions(self) -> Mapping[str, AbstractSet[int]]:
        return MappingProxyType(
            {"CatalogStoreProvider": frozenset(self._applied)},
        )

    async def apply_pending(self) -> list[str]:  # pragma: no cover - not exercised
        return []

    async def refresh_declared(self) -> None:
        self.refresh_calls += 1

    def set_applied(self, revisions: AbstractSet[int]) -> None:
        self._applied = set(revisions)

    # ------------------------------------------------------------------
    # ConnectorTypeVersion surface (Plugin Loader, CONN-IMPL-008)
    # ------------------------------------------------------------------

    async def put_connector_type_version(
        self,
        type: str,
        version: str,
        digest: str,
        image_ref: str,
        normalized_manifest: Mapping[str, Any],
    ) -> ConnectorTypeVersion:
        """Idempotent on identical ``(type, version, digest)``; raises
        :class:`ConflictDigest` on digest divergence."""
        key = (type, version)
        existing = self._rows.get(key)
        if existing is not None and existing.digest != digest:
            raise ConflictDigest(
                f"connector_type=({type!r},{version!r}) already registered "
                f"with digest={existing.digest!r}; refused new digest={digest!r}"
            )
        if existing is not None:
            # Idempotent re-put: refresh the parent_deprecated read.
            row = ConnectorTypeVersion(
                type=existing.type,
                version=existing.version,
                digest=existing.digest,
                image_ref=existing.image_ref,
                normalized_manifest=existing.normalized_manifest,
                parent_deprecated=type in self._deprecated_types,
                published_at=existing.published_at,
            )
            self._rows[key] = row
            return row
        row = ConnectorTypeVersion(
            type=type,
            version=version,
            digest=digest,
            image_ref=image_ref,
            normalized_manifest=dict(normalized_manifest),
            parent_deprecated=type in self._deprecated_types,
            published_at=datetime.now(UTC),
        )
        self._rows[key] = row
        return row

    async def get_connector_type_version(
        self,
        type: str,
        version: str,
    ) -> ConnectorTypeVersion | None:
        row = self._rows.get((type, version))
        if row is None:
            return None
        # Refresh denormalised parent_deprecated on every read so a
        # deprecate-then-get returns the updated flag without needing
        # to re-put.
        return ConnectorTypeVersion(
            type=row.type,
            version=row.version,
            digest=row.digest,
            image_ref=row.image_ref,
            normalized_manifest=row.normalized_manifest,
            parent_deprecated=type in self._deprecated_types,
            published_at=row.published_at,
        )

    async def list_connector_type_versions(
        self,
        type: str,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[ConnectorTypeVersion]:
        # Deterministic order by version string ASC so callers can
        # assert order without a tie-breaker dance.
        all_rows = [
            ConnectorTypeVersion(
                type=row.type,
                version=row.version,
                digest=row.digest,
                image_ref=row.image_ref,
                normalized_manifest=row.normalized_manifest,
                parent_deprecated=type in self._deprecated_types,
                published_at=row.published_at,
            )
            for (t, _), row in sorted(self._rows.items())
            if t == type
        ]
        # Page through using a simple offset encoded in the cursor's
        # opaque token. Production SPL uses an opaque keyset cursor;
        # the loader's contract here only cares that next_cursor is
        # ``None`` once the walk completes.
        start = 0
        if cursor is not None:
            try:
                start = int(cursor.token)
            except ValueError:  # pragma: no cover - defensive
                start = 0
        end = len(all_rows) if limit is None else min(start + limit, len(all_rows))
        next_cursor = Cursor(token=str(end)) if end < len(all_rows) else None
        return Page(items=all_rows[start:end], next_cursor=next_cursor)

    async def set_connector_type_deprecated(
        self,
        type: str,
        deprecated: bool,
    ) -> None:
        if deprecated:
            self._deprecated_types.add(type)
        else:
            self._deprecated_types.discard(type)


class FakeMetadataAdapter:
    """In-memory ``MetadataStoreProvider`` for wiring tests.

    Covers the migration surface plus a minimal ``append_audit`` recorder
    so middleware audit emission can be asserted without Postgres.
    """

    SCHEMA_REVISION = 4

    def __init__(self, *, applied_revisions: AbstractSet[int] | None = None) -> None:
        self._applied: set[int] = set(
            {1, 2, 3, 4} if applied_revisions is None else applied_revisions,
        )
        self.refresh_calls = 0
        self.append_audit_calls: list[tuple[str, AuditEvent]] = []

    @property
    def declared_revisions(self) -> Mapping[str, AbstractSet[int]]:
        return MappingProxyType(
            {"MetadataStoreProvider": frozenset(self._applied)},
        )

    async def apply_pending(self) -> list[str]:  # pragma: no cover - not exercised
        return []

    async def refresh_declared(self) -> None:
        self.refresh_calls += 1

    def set_applied(self, revisions: AbstractSet[int]) -> None:
        self._applied = set(revisions)

    async def append_audit(
        self,
        workspace_id: object,
        event: AuditEvent,
        tx: object = None,
    ) -> None:
        self.append_audit_calls.append((str(workspace_id), event))


class FakeConnectorInstanceAdapter:
    """In-memory ``ConnectorInstanceStoreProvider`` for service-layer tests.

    Mirrors the contract of :class:`PgConnectorInstanceAdapter`:
    create-only put, ``None`` on absent reads, allowlist-validated
    patches, and workspace isolation.
    """

    SCHEMA_REVISION = 1

    #: Mirror of the adapter's patchable-column allowlist. Kept in
    #: sync by hand because the SPL Protocol does not export it
    #: (per-adapter implementation detail).
    _PATCHABLE: frozenset[str] = frozenset(
        {"name", "lease_ttl_seconds", "enabled", "status", "health_status"}
    )

    def __init__(self, *, applied_revisions: AbstractSet[int] | None = None) -> None:
        self._applied: set[int] = set(
            {1} if applied_revisions is None else applied_revisions,
        )
        self.refresh_calls = 0
        # Keyed on (workspace_id, instance_id).
        self._rows: dict[tuple[str, str], ConnectorInstance] = {}

    @property
    def declared_revisions(self) -> Mapping[str, AbstractSet[int]]:
        return MappingProxyType(
            {"ConnectorInstanceStoreProvider": frozenset(self._applied)},
        )

    async def apply_pending(self) -> list[str]:  # pragma: no cover - not exercised
        return []

    async def refresh_declared(self) -> None:
        self.refresh_calls += 1

    def set_applied(self, revisions: AbstractSet[int]) -> None:
        self._applied = set(revisions)

    async def put_connector_instance(
        self,
        workspace_id: WorkspaceId,
        instance: ConnectorInstance,
    ) -> ConnectorInstance:
        if instance.workspace_id != workspace_id:
            raise ValueError(
                f"instance.workspace_id {instance.workspace_id!r} does not match "
                f"workspace_id arg {workspace_id!r}"
            )
        key = (str(workspace_id), str(instance.instance_id))
        if key in self._rows:
            raise ImmutableViolation(
                f"connector_instance ({workspace_id}, {instance.instance_id}) "
                f"already exists; use patch_connector_instance to update"
            )
        self._rows[key] = instance
        return instance

    async def get_connector_instance(
        self,
        workspace_id: WorkspaceId,
        instance_id: ConnectorInstanceId,
    ) -> ConnectorInstance | None:
        return self._rows.get((str(workspace_id), str(instance_id)))

    async def patch_connector_instance(
        self,
        workspace_id: WorkspaceId,
        instance_id: ConnectorInstanceId,
        updates: Mapping[str, Any],
    ) -> ConnectorInstance | None:
        unknown = set(updates) - self._PATCHABLE
        if unknown:
            raise ValueError(
                f"unknown patch fields: {sorted(unknown)!r}; allowed: {sorted(self._PATCHABLE)!r}"
            )
        key = (str(workspace_id), str(instance_id))
        current = self._rows.get(key)
        if current is None:
            return None
        new_kwargs: dict[str, Any] = {
            "workspace_id": current.workspace_id,
            "instance_id": current.instance_id,
            "type": current.type,
            "version": current.version,
            "name": current.name,
            "lease_ttl_seconds": current.lease_ttl_seconds,
            "enabled": current.enabled,
            "status": current.status,
            "health_status": current.health_status,
            "target_config": current.target_config,
            "credentials_authentication": current.credentials_authentication,
            "used_capabilities": current.used_capabilities,
            "created_at": current.created_at,
            "updated_at": datetime.now(UTC),
        }
        for k, v in updates.items():
            new_kwargs[k] = v
        updated = ConnectorInstance(**new_kwargs)
        self._rows[key] = updated
        return updated

    async def list_connector_instances(
        self,
        workspace_id: WorkspaceId,
        filter: ConnectorInstanceFilter | None = None,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[ConnectorInstance]:
        ws_rows = [row for (ws, _), row in self._rows.items() if ws == str(workspace_id)]
        if filter is not None:
            if filter.type is not None:
                ws_rows = [r for r in ws_rows if r.type == filter.type]
            if filter.enabled is not None:
                ws_rows = [r for r in ws_rows if r.enabled is filter.enabled]
        # Deterministic order: created_at DESC, instance_id ASC.
        ws_rows.sort(key=lambda r: (-r.created_at.timestamp(), str(r.instance_id)))
        start = 0
        if cursor is not None:
            try:
                start = int(cursor.token)
            except ValueError:  # pragma: no cover - defensive
                start = 0
        end = len(ws_rows) if limit is None else min(start + limit, len(ws_rows))
        next_cursor = Cursor(token=str(end)) if end < len(ws_rows) else None
        return Page(items=ws_rows[start:end], next_cursor=next_cursor)


__all__ = [
    "FakeCatalogAdapter",
    "FakeConnectorInstanceAdapter",
    "FakeMetadataAdapter",
    "StubPluginBinder",
    "build_bind_for_step_service",
]


class StubPluginBinder:
    """In-memory :class:`PluginBinder` for binding-service unit tests.

    Records every ``bind`` invocation and returns a deterministic
    :class:`ConnectorContext` whose ``handle`` echoes the slot,
    capability, and identity envelope keys so assertions can detect
    cross-slot wiring bugs.
    """

    def __init__(
        self,
        *,
        context_factory: Any | None = None,
        raise_for_slot: Mapping[str, BaseException] | None = None,
    ) -> None:
        self._context_factory = context_factory
        self._raise_for_slot: dict[str, BaseException] = dict(raise_for_slot or {})
        self.calls: list[dict[str, Any]] = []

    async def bind(
        self,
        *,
        connector: ConnectorTypeVersion,
        instance: ConnectorInstance,
        slot: str,
        capability: str,
        identity_material: Mapping[str, Any],
    ) -> ConnectorContext:
        self.calls.append(
            {
                "connector_type": connector.type,
                "connector_version": connector.version,
                "instance_id": str(instance.instance_id),
                "slot": slot,
                "capability": capability,
                "identity_material": dict(identity_material),
            }
        )
        exc = self._raise_for_slot.get(slot)
        if exc is not None:
            raise exc
        if self._context_factory is not None:
            built = self._context_factory(slot, capability, instance)
            assert isinstance(built, ConnectorContext)
            return built
        return ConnectorContext(
            endpoint=f"stub://{slot}",
            token_type_hint=None,
            handle=MappingProxyType(
                {
                    "slot": slot,
                    "capability": capability,
                    "instance_id": str(instance.instance_id),
                }
            ),
            extras=MappingProxyType(
                {
                    "identity_envelope_keys": tuple(sorted(identity_material.keys())),
                }
            ),
        )


def build_bind_for_step_service(
    *,
    catalog_store: Any | None = None,
    instance_store: Any | None = None,
    metadata_store: Any | None = None,
    identity_registry: IdentityResolverRegistry | None = None,
    plugin_binder: Any | None = None,
) -> BindForStepService:
    """Build a :class:`BindForStepService` wired entirely to in-memory fakes.

    Defaults: ``FakeCatalogAdapter``, ``FakeConnectorInstanceAdapter``,
    ``FakeMetadataAdapter``, an empty :class:`IdentityResolverRegistry`,
    and a :class:`StubPluginBinder`. Tests override individual
    components by passing them explicitly.

    The store parameters are typed as :data:`Any` so tests can pass the
    in-process :class:`FakeCatalogAdapter` / :class:`FakeMetadataAdapter`
    / :class:`FakeConnectorInstanceAdapter` instances directly without
    needing per-call ``# type: ignore[arg-type]`` annotations to bridge
    the SPL Protocol's ``ClassVar SCHEMA_REVISION`` to the fakes'
    instance-attribute equivalent.
    """
    return BindForStepService(
        catalog_store=cast(
            "CatalogStoreProvider",
            catalog_store if catalog_store is not None else FakeCatalogAdapter(),
        ),
        instance_store=cast(
            "ConnectorInstanceStoreProvider",
            instance_store if instance_store is not None else FakeConnectorInstanceAdapter(),
        ),
        metadata_store=cast(
            "MetadataStoreProvider",
            metadata_store if metadata_store is not None else FakeMetadataAdapter(),
        ),
        identity_registry=identity_registry or IdentityResolverRegistry(),
        plugin_binder=plugin_binder or StubPluginBinder(),
    )
