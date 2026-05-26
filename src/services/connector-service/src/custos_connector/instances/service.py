"""`InstanceService` — workspace-scoped CRUD for connector instances.

The service is the only layer that talks to
:class:`ConnectorInstanceStoreProvider`. Routes (CONN-IMPL-012) build
on this; activation (CONN-IMPL-013) extends with status transitions.

Validation policy
-----------------

* **Create-time**: the supplied `(type, version)` MUST resolve to a
  catalog row. Looking up the catalog at create-time is the cheapest
  way to catch typos; a stale instance pointing at a deprecated
  catalog row is allowed (deprecation is a property of the catalog
  row, not the instance, and the connector keeps working on the
  pinned version until explicitly migrated).
* **PATCH**: only `name`, `lease_ttl_seconds`, and `enabled` are
  caller-mutable. Attempts to touch immutable fields (`type`,
  `version`, `instance_id`) or server-mutable soft state (`status`,
  `health_status`) raise :class:`ImmutableFieldUpdate`.
* **Lease TTL**: positive integer, ≤ :data:`_MAX_LEASE_TTL_SECONDS`.
  The catalog-manifest-derived per-type ceiling (`credentials.maxLeaseTtl`
  in `design.md § 8 Limits`) is enforced in CONN-IMPL-013 when the
  manifest carries that field — the v1 connector-manifest schema does
  not yet, so we keep a conservative platform-wide ceiling here.

Audit policy
------------

Every successful state mutation emits a typed audit event via
:mod:`custos_connector.audit`. Audit emission is **best-effort**:
failures are logged at WARNING + counted on
:data:`custos_connector.audit.EMIT_FAILURES_TOTAL` and never roll
back the state mutation. This matches the catalog-service
CS-IMPL-019 contract.

Workspace isolation
-------------------

Every method takes `workspace_id` as its first arg and forwards it
to the SPL provider. A row in workspace A is **invisible** from
workspace B — the SPL provider returns ``None`` rather than raising,
so this service maps ``None`` → :class:`ConnectorInstanceNotFound`.
The 404 is identical for "does not exist" and "exists in another
workspace" so cross-workspace existence cannot be probed.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Final
from uuid import uuid4

from custos_spl.ids import ConnectorInstanceId, WorkspaceId
from custos_spl.interfaces.connector_instance_store import (
    ConnectorInstance,
    ConnectorInstanceFilter,
    ConnectorInstanceStoreProvider,
)

from custos_connector.audit import audit_instance_created, audit_instance_updated
from custos_connector.instances.validator import (
    InstanceConfigValidationError,
    validate_instance_config,
)

if TYPE_CHECKING:
    from custos_spl.interfaces.catalog_store import CatalogStoreProvider
    from custos_spl.interfaces.metadata_store import MetadataStoreProvider
    from custos_spl.pagination import Cursor, Page


#: Hard ceiling on caller-supplied ``lease_ttl_seconds``. The
#: connector-manifest schema will eventually carry a per-type
#: ceiling (``credentials.maxLeaseTtl``) which CONN-IMPL-013 will
#: enforce on top of this. Until then this is the platform-wide
#: ceiling.
_MAX_LEASE_TTL_SECONDS: Final[int] = 30 * 24 * 60 * 60  # 30 days

#: Fields the PATCH surface accepts.
_PATCHABLE_FIELDS: Final[frozenset[str]] = frozenset({"name", "lease_ttl_seconds", "enabled"})

#: Fields that are immutable post-create. Listed explicitly so the
#: error message can name them.
_IMMUTABLE_FIELDS: Final[frozenset[str]] = frozenset(
    {"instance_id", "workspace_id", "type", "version", "created_at"}
)


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class InstanceServiceError(Exception):
    """Base class for InstanceService failures."""

    code: str = "connector.instance_service_failed"


class ConnectorInstanceNotFound(InstanceServiceError):
    """Raised when the (workspace, instance_id) row does not exist.

    The 404 is the same for "no such row anywhere" and "row exists
    in a different workspace" so cross-workspace existence cannot
    leak via this exception.
    """

    code: str = "connector.instance_not_found"

    def __init__(self, *, workspace_id: str, instance_id: str) -> None:
        self.workspace_id = workspace_id
        self.instance_id = instance_id
        super().__init__(
            f"connector instance {instance_id!r} not found in workspace {workspace_id!r}"
        )


class ConnectorTypeNotRegistered(InstanceServiceError):
    """Raised at create-time when (type, version) is not in the catalog."""

    code: str = "connector.instance_type_not_registered"

    def __init__(self, *, type: str, version: str) -> None:
        self.type = type
        self.version = version
        super().__init__(f"connector type {type}@{version} is not registered in the catalog")


class InvalidLeaseTtl(InstanceServiceError):
    """Raised when ``lease_ttl_seconds`` is non-positive or > ceiling."""

    code: str = "connector.instance_invalid_lease_ttl"

    def __init__(self, *, value: int, ceiling: int = _MAX_LEASE_TTL_SECONDS) -> None:
        self.value = value
        self.ceiling = ceiling
        super().__init__(f"lease_ttl_seconds={value!r} is out of range (must be 1..{ceiling})")


class ImmutableFieldUpdate(InstanceServiceError):
    """Raised when PATCH tries to mutate an immutable or server-only field."""

    code: str = "connector.instance_immutable_field_update"

    def __init__(self, *, fields: frozenset[str]) -> None:
        self.fields = fields
        rendered = ", ".join(sorted(fields))
        super().__init__(f"fields not mutable via PATCH: {rendered}")


class InvalidInstancePayload(InstanceServiceError):
    """Raised on malformed create-time input (empty name, etc.)."""

    code: str = "connector.instance_invalid_payload"


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


class InstanceService:
    """Workspace-scoped CRUD on `ConnectorInstance` rows.

    Args:
        instance_store: SPL provider that owns the instance rows.
        catalog_store: Used to verify the supplied ``(type, version)``
            exists at create-time.
        metadata_store: SPL provider that owns the audit outbox.
    """

    def __init__(
        self,
        *,
        instance_store: ConnectorInstanceStoreProvider,
        catalog_store: CatalogStoreProvider,
        metadata_store: MetadataStoreProvider,
    ) -> None:
        self._instances = instance_store
        self._catalog = catalog_store
        self._metadata = metadata_store

    # ------------------------------------------------------------------
    # Create
    # ------------------------------------------------------------------

    async def create(
        self,
        workspace_id: str,
        *,
        type: str,
        version: str,
        actor: str,
        name: str | None = None,
        lease_ttl_seconds: int | None = None,
        enabled: bool = True,
        target_config: Mapping[str, Any] | None = None,
        credentials_authentication: Mapping[str, Any] | None = None,
        used_capabilities: tuple[str, ...] | None = None,
    ) -> ConnectorInstance:
        """Create a new connector instance.

        Generates the ``instance_id`` server-side (UUIDv4) so callers
        never pick IDs — this matches the platform convention for
        run/workflow IDs.

        Operator-supplied config (``target_config``,
        ``credentials_authentication``, ``used_capabilities``) is
        validated against the referenced ``ConnectorTypeVersion``
        manifest via
        :func:`custos_connector.instances.validator.validate_instance_config`
        before persistence. Validation issues are aggregated into a
        single :class:`InstanceConfigValidationError` (collect-all-
        errors policy, mirrors the manifest-validator UX from
        CONN-IMPL-005).
        """
        # 1. Surface-level validation (cheap; fails fast).
        if name is not None and not name.strip():
            raise InvalidInstancePayload("name must be non-empty when supplied")
        if lease_ttl_seconds is not None:
            self._validate_lease_ttl(lease_ttl_seconds)

        effective_target_config: Mapping[str, Any] = (
            dict(target_config) if target_config is not None else {}
        )
        effective_credentials_auth: Mapping[str, Any] = (
            dict(credentials_authentication)
            if credentials_authentication is not None
            else {}
        )

        # 2. Catalog existence check. Stale-on-deprecation is fine
        #    (deprecation is a property of the catalog row); only
        #    "row not present at all" rejects the create.
        catalog_row = await self._catalog.get_connector_type_version(type, version)
        if catalog_row is None:
            raise ConnectorTypeNotRegistered(type=type, version=version)

        # 3. Manifest-driven config validation. Issues are collected
        #    and surfaced as a single typed error so the API layer
        #    can render one 400 response with the complete diff.
        validate_instance_config(
            manifest=catalog_row.normalized_manifest,
            target_config=effective_target_config,
            credentials_authentication=effective_credentials_auth,
            used_capabilities=used_capabilities,
        )

        # 4. Build the row and persist.
        now = datetime.now(UTC)
        instance = ConnectorInstance(
            workspace_id=WorkspaceId(workspace_id),
            instance_id=ConnectorInstanceId(str(uuid4())),
            type=type,
            version=version,
            name=name,
            lease_ttl_seconds=lease_ttl_seconds,
            enabled=enabled,
            status="active",
            health_status=None,
            target_config=effective_target_config,
            credentials_authentication=effective_credentials_auth,
            used_capabilities=used_capabilities,
            created_at=now,
            updated_at=now,
        )
        stored = await self._instances.put_connector_instance(WorkspaceId(workspace_id), instance)

        # 5. Best-effort audit (failures logged + counted, never
        #    roll back the state we just persisted).
        await audit_instance_created(
            self._metadata,
            workspace_id=workspace_id,
            actor=actor,
            instance_id=str(stored.instance_id),
            type_name=stored.type,
            version=stored.version,
            name=stored.name,
            enabled=stored.enabled,
            lease_ttl_seconds=stored.lease_ttl_seconds,
        )
        return stored

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    async def get(
        self,
        workspace_id: str,
        instance_id: str,
    ) -> ConnectorInstance:
        """Read a single instance; 404 if absent in this workspace."""
        row = await self._instances.get_connector_instance(
            WorkspaceId(workspace_id), ConnectorInstanceId(instance_id)
        )
        if row is None:
            raise ConnectorInstanceNotFound(workspace_id=workspace_id, instance_id=instance_id)
        return row

    async def list(
        self,
        workspace_id: str,
        *,
        type: str | None = None,
        enabled: bool | None = None,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[ConnectorInstance]:
        """Paginated listing within a single workspace."""
        return await self._instances.list_connector_instances(
            WorkspaceId(workspace_id),
            filter=ConnectorInstanceFilter(type=type, enabled=enabled),
            cursor=cursor,
            limit=limit,
        )

    # ------------------------------------------------------------------
    # Patch
    # ------------------------------------------------------------------

    async def patch(
        self,
        workspace_id: str,
        instance_id: str,
        *,
        actor: str,
        updates: Mapping[str, Any],
    ) -> ConnectorInstance:
        """Apply a partial update; emit ``connector.instance.updated``.

        ``updates`` is a mapping from field name to new value;
        unknown keys or attempts to touch immutable fields raise
        :class:`ImmutableFieldUpdate`. Empty mapping is a no-op patch
        that still bumps ``updated_at`` and emits an audit event with
        an empty change-set.
        """
        # Reject any field outside the PATCH-able allowlist. We catch
        # both "immutable" (type/version/...) and "unknown" in a
        # single typed error so the API surface speaks with one voice.
        bad = set(updates) - _PATCHABLE_FIELDS
        if bad:
            raise ImmutableFieldUpdate(fields=frozenset(bad))
        if "lease_ttl_seconds" in updates and updates["lease_ttl_seconds"] is not None:
            self._validate_lease_ttl(updates["lease_ttl_seconds"])
        if "name" in updates:
            new_name = updates["name"]
            if new_name is not None and not str(new_name).strip():
                raise InvalidInstancePayload("name must be non-empty when supplied")

        # Read the current state so we can compute a diff for the
        # audit payload. We do an explicit get rather than relying on
        # SPL's returned post-state because the audit "from" half of
        # each change pair has to come from the pre-state.
        before = await self.get(workspace_id, instance_id)

        after = await self._instances.patch_connector_instance(
            WorkspaceId(workspace_id),
            ConnectorInstanceId(instance_id),
            updates,
        )
        if after is None:
            # The row vanished between the read above and the patch
            # below — almost certainly a concurrent delete. Surface
            # as 404 so the caller can retry / give up cleanly.
            raise ConnectorInstanceNotFound(workspace_id=workspace_id, instance_id=instance_id)

        # Build a {field: {"from": old, "to": new}} payload over the
        # fields the caller actually touched. We compare after-vs-before
        # rather than just echoing `updates` so the audit log carries
        # the canonical post-state values (e.g. coerced types).
        changes: dict[str, dict[str, Any]] = {}
        for field in updates:
            old = getattr(before, field)
            new = getattr(after, field)
            if old != new:
                changes[field] = {"from": old, "to": new}
        await audit_instance_updated(
            self._metadata,
            workspace_id=workspace_id,
            actor=actor,
            instance_id=instance_id,
            changes=changes,
        )
        return after

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_lease_ttl(value: int) -> None:
        if value < 1 or value > _MAX_LEASE_TTL_SECONDS:
            raise InvalidLeaseTtl(value=value)


__all__ = [
    "ConnectorInstanceNotFound",
    "ConnectorTypeNotRegistered",
    "ImmutableFieldUpdate",
    "InstanceConfigValidationError",
    "InstanceService",
    "InstanceServiceError",
    "InvalidInstancePayload",
    "InvalidLeaseTtl",
]
