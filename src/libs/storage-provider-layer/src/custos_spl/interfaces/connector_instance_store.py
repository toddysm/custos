"""ConnectorInstanceStoreProvider — workspace-scoped connector instance rows.

A `ConnectorInstance` is the runtime row that ties a workspace to a
specific connector-type version with operator-supplied config (lease
TTL override, activation state, human-readable label). Every method
is workspace-scoped; `workspace_id` is the first arg on every call so
the SPL workspace-scoping middleware can enforce isolation in the
same way it does for `MetadataStoreProvider`.

Connector cursors (`ConnectorCursor`) still live on
`MetadataStoreProvider` — they belong with run/step state rather than
instance config. The two providers share the `ConnectorInstanceId`
namespace via :mod:`custos_spl.ids` so a cursor row can reference an
instance without a cross-provider join at the type level.

See `design/components/connector-service/design.md` § Data Models for
the ConnectorInstance entity and § Public Interface for the surface
that callers expose on top of these CRUD methods.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, ClassVar, Protocol, runtime_checkable

from custos_spl.ids import ConnectorInstanceId, WorkspaceId
from custos_spl.pagination import Cursor, Page


@dataclass(frozen=True, slots=True)
class ConnectorInstance:
    """A workspace-scoped configured connection.

    Primary key is `(workspace_id, instance_id)`. The `(type, version)`
    pair references a row in `CatalogStoreProvider.connector_type_version`
    but the SPL does not enforce that foreign key at the storage layer
    — instance create-time validation belongs in the connector-service
    domain layer where the catalog provider is available.

    Field categories
    ----------------

    * **Immutable after create**: `workspace_id`, `instance_id`,
      `type`, `version`, `created_at`.
    * **Operator-mutable via patch**: `name`, `lease_ttl_seconds`,
      `enabled`.
    * **Server-mutated soft state**: `status`, `health_status`,
      `updated_at`. Activation + health transition logic ships in
      CONN-IMPL-013; this row carries the columns now so the schema
      is stable when that ticket lands.
    """

    workspace_id: WorkspaceId
    instance_id: ConnectorInstanceId
    type: str
    version: str
    name: str | None
    lease_ttl_seconds: int | None
    enabled: bool
    status: str
    health_status: str | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class ConnectorInstanceFilter:
    """Optional filter for :meth:`ConnectorInstanceStoreProvider.list_connector_instances`.

    All fields are conjunctive (AND): supplying `type` and `enabled`
    returns only rows that match both. Unsupplied fields are wildcards.
    """

    type: str | None = None
    enabled: bool | None = None


@runtime_checkable
class ConnectorInstanceStoreProvider(Protocol):
    """Workspace-scoped CRUD for `ConnectorInstance` rows.

    Put semantics
    -------------

    * `put_connector_instance` is **create-only**: re-putting on an
      existing `(workspace_id, instance_id)` raises
      :class:`custos_spl.errors.ImmutableViolation`. Operator updates
      flow through :meth:`patch_connector_instance` so the mutation
      surface is bounded by an explicit field allowlist (the service
      layer's contract; the SPL accepts whatever mapping the caller
      passes and trusts it to be already-validated).

    * `patch_connector_instance` accepts a mapping of fields to set.
      Unknown keys are an adapter contract error
      (:class:`ValueError`); the service layer is expected to gate
      what reaches the adapter. Server timestamps (`updated_at`) are
      bumped by the adapter on every successful patch.

    Cross-workspace reads
    ---------------------

    `get_connector_instance` and `patch_connector_instance` both
    surface absent rows as `None`. An instance present in a
    different workspace is indistinguishable from non-existent at
    this surface — adapters MUST NOT raise to disclose
    cross-workspace existence.

    The schema revision required by this build is `SCHEMA_REVISION`.
    """

    SCHEMA_REVISION: ClassVar[int] = 1

    async def put_connector_instance(
        self,
        workspace_id: WorkspaceId,
        instance: ConnectorInstance,
    ) -> ConnectorInstance:
        """Create a new connector instance row.

        Raises :class:`custos_spl.errors.ImmutableViolation` if an
        instance with the same `(workspace_id, instance_id)` already
        exists, even with identical contents. Callers wanting to
        update must go through :meth:`patch_connector_instance`.
        """
        ...

    async def get_connector_instance(
        self,
        workspace_id: WorkspaceId,
        instance_id: ConnectorInstanceId,
    ) -> ConnectorInstance | None:
        """Read a single instance row.

        Returns `None` when no row exists for `(workspace_id,
        instance_id)`. An instance in a different workspace is
        indistinguishable from absent at this surface — the adapter
        MUST NOT raise to disclose cross-workspace existence.
        """
        ...

    async def patch_connector_instance(
        self,
        workspace_id: WorkspaceId,
        instance_id: ConnectorInstanceId,
        updates: Mapping[str, Any],
    ) -> ConnectorInstance | None:
        """Apply a partial update to mutable fields and bump `updated_at`.

        `updates` may contain any subset of the operator-mutable
        fields (`name`, `lease_ttl_seconds`, `enabled`) or the
        server-mutable soft-state fields (`status`, `health_status`).
        Empty mapping is permitted and only refreshes `updated_at`.

        Returns the row state **after** the update, or `None` when
        no row exists for `(workspace_id, instance_id)` — both for
        "row does not exist anywhere" and "row exists in another
        workspace". Adapters MUST NOT disclose cross-workspace
        existence, so absent maps to a single `None` signal here
        identically to :meth:`get_connector_instance`.
        """
        ...

    async def list_connector_instances(
        self,
        workspace_id: WorkspaceId,
        filter: ConnectorInstanceFilter | None = None,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[ConnectorInstance]:
        """Paginated listing of instances in a single workspace.

        The default ordering is `(created_at DESC, instance_id ASC)`
        so newest-first paging is stable. `filter` fields are
        conjunctive; `cursor` is opaque to callers and produced by
        prior page tails.
        """
        ...


__all__ = [
    "ConnectorInstance",
    "ConnectorInstanceFilter",
    "ConnectorInstanceStoreProvider",
]
