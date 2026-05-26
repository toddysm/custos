"""LeaseStoreProvider — workspace-scoped activity-token lease rows.

A `Lease` represents the durable side of a single activity-facing
credential lease minted by the Connector Service Lease Manager
(CONN-IMPL-017). The in-memory state machine that owns issuance,
refresh, release, and the concurrent-lease cap lives in
``custos_connector.lease``; this Protocol is the persistence seam so
the service can survive process restart without losing track of
currently-outstanding leases.

The store is workspace-scoped: `workspace_id` is the first arg on every
method, and the composite primary key includes it so the SPL
workspace-scoping middleware can enforce isolation the same way it does
for :class:`ConnectorInstanceStoreProvider`. Cross-workspace reads
return ``None`` rather than disclose existence.

Field semantics
---------------

* `lease_id` — stable ULID minted by the Lease Manager at ``issue``
  time. Preserved across refreshes (the unit of audit and revocation
  per design § Lease lifecycle).
* `run_id`, `step_id`, `attempt`, `slot`, `capability` — the scope
  the lease was issued for. The Lease Manager enforces a 16-lease cap
  per ``(run_id, step_id, attempt)``; the store carries the columns
  with a secondary index so the cap check is a constant-time count.
* `connector_instance_id` — the resolved connector instance backing
  this lease. Joined back to :class:`ConnectorInstance` rows out of
  band; the store does not enforce the foreign key (SPL § Atomicity).
* `token_type` — opaque per-connector type tag (e.g. ``bearer``,
  ``aws-sigv4``). Never the token material itself.
* `issued_at`, `expires_at` — RFC3339 UTC timestamps. The Lease
  Manager refreshes `expires_at` (and the underlying token material,
  which never lands here) on every successful refresh.
* `released_at` — populated by :meth:`release_lease`. Released rows
  are kept (not deleted) so the audit trail can join `lease.released`
  events to the original `lease.issued` row.
* `revoked_at`, `revoke_reason` — written by the operator revoke flow
  (CONN-IMPL-028) which lands in Phase L. Reserved as columns here so
  the schema is stable when that ticket ships without a follow-up
  migration.

Lifecycle
---------

* :meth:`put_lease` is **create-only**: re-puts on an existing
  ``(workspace_id, lease_id)`` raise
  :class:`custos_spl.errors.ImmutableViolation`. The Lease Manager
  mints a fresh ULID per issue, so a duplicate put is a service-layer
  bug worth surfacing loudly.
* :meth:`refresh_lease` updates only ``expires_at`` and bumps
  ``updated_at``. ``lease_id`` is stable by contract — callers cannot
  rotate it.
* :meth:`release_lease` marks the row released without deleting it.
  Returns the post-release row so the service can emit the audit
  event with the canonical timestamps.
* :meth:`count_active_for_step_attempt` is the cap-check primitive:
  the number of non-released, non-expired leases for a given
  ``(workspace_id, run_id, step_id, attempt)`` tuple at ``as_of``.
  ``as_of`` is supplied by the caller (rather than ``now()``) so the
  Lease Manager's clock seam carries through to the cap.
* :meth:`list_active_leases` reloads the live state at process start;
  the Lease Manager rehydrates its in-memory cap counters from this.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar, Protocol, runtime_checkable

from custos_spl.ids import ConnectorInstanceId, RunId, StepId, WorkspaceId
from custos_spl.pagination import Cursor, Page


@dataclass(frozen=True, slots=True)
class Lease:
    """A workspace-scoped activity-token lease row.

    Primary key is ``(workspace_id, lease_id)``. The `(run_id, step_id,
    attempt)` tuple is indexed for the cap-check; `connector_instance_id`
    is indexed for operator revoke flows (CONN-IMPL-028).
    """

    workspace_id: WorkspaceId
    lease_id: str
    run_id: RunId
    step_id: StepId
    attempt: int
    slot: str
    capability: str
    connector_instance_id: ConnectorInstanceId
    token_type: str
    issued_at: datetime
    expires_at: datetime
    released_at: datetime | None
    revoked_at: datetime | None
    revoke_reason: str | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class LeaseFilter:
    """Optional filter for :meth:`LeaseStoreProvider.list_active_leases`.

    All fields are conjunctive (AND). The default semantics of
    ``active`` are "not released and not yet expired at the moment of
    the SQL evaluation". Callers who need a pinned wall clock should
    instead use :meth:`LeaseStoreProvider.count_active_for_step_attempt`
    with an explicit ``as_of`` — listing is intended for startup
    rehydration where "as of now" is exactly what the Lease Manager
    wants.
    """

    run_id: RunId | None = None
    step_id: StepId | None = None
    attempt: int | None = None
    connector_instance_id: ConnectorInstanceId | None = None


@runtime_checkable
class LeaseStoreProvider(Protocol):
    """Workspace-scoped CRUD + cap-check for activity-token leases.

    Put semantics
    -------------

    * :meth:`put_lease` is **create-only**: re-puts on an existing
      ``(workspace_id, lease_id)`` raise
      :class:`custos_spl.errors.ImmutableViolation`, even when the
      row contents are byte-identical. The Lease Manager mints a
      fresh ULID per ``issue`` so a duplicate put is a service-layer
      bug worth surfacing loudly.

    * :meth:`refresh_lease` updates only ``expires_at`` and bumps
      ``updated_at``. ``lease_id`` is stable by contract.

    * :meth:`release_lease` marks the row released and returns the
      post-release row. Idempotent: releasing an already-released
      lease returns the existing row unchanged.

    Cross-workspace reads
    ---------------------

    :meth:`get_lease`, :meth:`refresh_lease`, and
    :meth:`release_lease` all surface absent rows as ``None``. A
    lease present in a different workspace is indistinguishable from
    non-existent at this surface — adapters MUST NOT raise to
    disclose cross-workspace existence.

    The schema revision required by this build is ``SCHEMA_REVISION``.
    """

    SCHEMA_REVISION: ClassVar[int] = 1

    async def put_lease(
        self,
        workspace_id: WorkspaceId,
        lease: Lease,
    ) -> Lease:
        """Create a new lease row.

        Raises :class:`custos_spl.errors.ImmutableViolation` if a row
        with the same ``(workspace_id, lease_id)`` already exists,
        even with identical contents. Callers wanting to extend a
        lease must go through :meth:`refresh_lease`.
        """
        ...

    async def get_lease(
        self,
        workspace_id: WorkspaceId,
        lease_id: str,
    ) -> Lease | None:
        """Read a single lease row.

        Returns ``None`` when no row exists for
        ``(workspace_id, lease_id)``. A lease in a different
        workspace is indistinguishable from absent at this surface —
        the adapter MUST NOT raise to disclose cross-workspace
        existence.
        """
        ...

    async def refresh_lease(
        self,
        workspace_id: WorkspaceId,
        lease_id: str,
        new_expires_at: datetime,
    ) -> Lease | None:
        """Extend a lease by overwriting ``expires_at``.

        Returns the post-refresh row, or ``None`` when no row exists
        for ``(workspace_id, lease_id)`` (both "does not exist
        anywhere" and "exists in another workspace"). Also returns
        ``None`` when the targeted row is already released —
        refreshing a released lease is a service-layer programming
        error and surfaces here as an absent-row result.

        ``new_expires_at`` is supplied by the Lease Manager after
        running TTL precedence; the store does not validate the
        magnitude.
        """
        ...

    async def release_lease(
        self,
        workspace_id: WorkspaceId,
        lease_id: str,
        released_at: datetime,
    ) -> Lease | None:
        """Mark a lease released without deleting the row.

        Returns the post-release row. Idempotent: releasing an
        already-released lease returns the existing row unchanged
        (``released_at`` is the original release timestamp, not the
        new one). Returns ``None`` when no row exists for
        ``(workspace_id, lease_id)``.
        """
        ...

    async def count_active_for_step_attempt(
        self,
        workspace_id: WorkspaceId,
        run_id: RunId,
        step_id: StepId,
        attempt: int,
        as_of: datetime,
    ) -> int:
        """Count non-released, non-expired leases for a step-attempt.

        ``as_of`` is the wall clock against which ``expires_at`` is
        compared — the Lease Manager's clock seam threads through to
        the cap check so tests can pin the boundary deterministically.

        Used by :class:`~custos_connector.lease.LeaseManager` to
        enforce the per-step cap (16 by default; see
        ``CONN_LEASE_MAX_CONCURRENT``).
        """
        ...

    async def list_active_leases(
        self,
        workspace_id: WorkspaceId,
        filter: LeaseFilter | None = None,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[Lease]:
        """Paginated listing of currently-active leases in a workspace.

        "Active" means not released and not yet expired at SQL
        evaluation time. Default ordering is
        ``(issued_at DESC, lease_id ASC)`` so newest-first paging is
        stable. The Lease Manager calls this once at process start to
        rehydrate its in-memory cap counters; routine traffic should
        prefer :meth:`count_active_for_step_attempt`.
        """
        ...


__all__ = [
    "Lease",
    "LeaseFilter",
    "LeaseStoreProvider",
]
