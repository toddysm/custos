"""``RunStore`` Protocol + in-process :class:`MetadataStoreProvider` adapter.

This module wires the Run Controller to the workflow-service's
persistence story. The :class:`RunStore` Protocol pins the
narrow surface every higher-level Run Controller sub-module
keys off; the :class:`InProcessRunStore` adapter delegates each
call to a :class:`custos_spl.interfaces.metadata_store.MetadataStoreProvider`,
so the workflow-service host can swap in any conformant SPL
adapter (Postgres in production, in-memory in tests) without
code change.

The Protocol surface intentionally mirrors the four locked
acceptance criteria from WF-IMPL-032 (#384):

* :meth:`put_run` — idempotent on ``(workspace_id, run_id)``;
  duplicate inserts with a byte-equal payload are a no-op,
  divergent payloads raise :class:`RunStateConflictError`.
* :meth:`update_run_status` — enforces the
  :data:`STATUS_TRANSITIONS` table; every illegal move raises
  :class:`RunStateConflictError`.
* :meth:`get_run` — returns the persisted :class:`RunRecord`
  or ``None`` when absent.
* :meth:`list_runs` — paginates via the SPL :class:`Cursor`
  opaque token.

The compiled-:class:`ExecutionGraph` round-trip through the
persistent store is the WF-IMPL-033 (#385) deliverable. The
in-process adapter here stashes :attr:`RunRecord.compiled_graph`
in an internal side-map so the WF-IMPL-032 acceptance criteria
(status-transition enforcement + idempotent re-put + pagination
+ status enum) can ship cleanly without WF-IMPL-033's
serialization concerns.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, cast, runtime_checkable

from custos_spl.errors import ImmutableViolation
from custos_spl.ids import RunId as SplRunId
from custos_spl.ids import WorkflowId as SplWorkflowId
from custos_spl.ids import WorkspaceId as SplWorkspaceId
from custos_spl.interfaces.metadata_store import (
    MetadataStoreProvider,
)
from custos_spl.interfaces.metadata_store import (
    Run as SplRun,
)
from custos_spl.pagination import Cursor, Page

from custos_workflow.runs.errors import RunStateConflictError
from custos_workflow.runs.ids import RunId
from custos_workflow.runs.model import STATUS_TRANSITIONS, RunRecord, RunStatus

if TYPE_CHECKING:
    from custos_workflow.graph.model import ExecutionGraph

__all__ = ["InProcessRunStore", "RunStore"]


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class RunStore(Protocol):
    """Narrow persistence surface the Run Controller drives.

    Every method is ``async`` to match the SPL provider surface
    and to keep the door open for a future remote / sharded
    backing store without rewriting callers.
    """

    async def put_run(self, run: RunRecord) -> RunRecord:
        """Insert *run*. Idempotent on ``(workspace_id, run_id)``.

        A duplicate insert with a byte-equal payload is a no-op
        and returns the already-persisted row. A divergent
        payload raises :class:`RunStateConflictError`.
        """
        ...

    async def update_run_status(
        self,
        workspace_id: str,
        run_id: RunId,
        status: RunStatus,
        *,
        reason: str | None = None,
    ) -> RunRecord:
        """Move ``(workspace_id, run_id)`` to *status*.

        Enforces the :data:`STATUS_TRANSITIONS` table. Illegal
        moves raise :class:`RunStateConflictError`. Missing rows
        raise :class:`RunStateConflictError` as well (the caller
        should use :meth:`get_run` for "exists?" checks).
        """
        ...

    async def get_run(self, workspace_id: str, run_id: RunId) -> RunRecord | None:
        """Return the row or ``None`` when absent."""
        ...

    async def list_runs(
        self,
        workspace_id: str,
        *,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[RunRecord]:
        """Return a paginated slice of the workspace's runs."""
        ...


# ---------------------------------------------------------------------------
# In-process adapter
# ---------------------------------------------------------------------------


class InProcessRunStore:
    """The canonical :class:`RunStore` adapter.

    Delegates to an injected
    :class:`custos_spl.interfaces.metadata_store.MetadataStoreProvider`
    for the persistent fields (everything :class:`SplRun` carries)
    and keeps :attr:`RunRecord.compiled_graph` in an internal
    side-map until WF-IMPL-033 (#385) lands the serialization
    layer.

    The adapter does not own any global state — instantiate one
    per process (or per test) and pass it through the dependency
    graph.
    """

    def __init__(self, provider: MetadataStoreProvider) -> None:
        self._provider: MetadataStoreProvider = provider
        # Side-map for the compiled graph until WF-IMPL-033 lands
        # the serialization round-trip into the SPL ``Run`` row.
        # Key: ``(workspace_id, run_id)``.
        self._graphs: dict[tuple[str, str], ExecutionGraph] = {}

    # ----- helpers ------------------------------------------------------

    @staticmethod
    def _to_spl_run(record: RunRecord) -> SplRun:
        return SplRun(
            workspace_id=cast(SplWorkspaceId, record.workspace_id),
            run_id=cast(SplRunId, record.run_id),
            workflow_id=cast(SplWorkflowId, record.workflow_id),
            workflow_version=record.workflow_version,
            status=record.status.value,
            reason=record.reason,
            started_at=record.started_at,
            updated_at=record.updated_at,
        )

    def _to_record(self, spl: SplRun) -> RunRecord:
        return RunRecord(
            workspace_id=str(spl.workspace_id),
            run_id=cast(RunId, str(spl.run_id)),
            workflow_id=str(spl.workflow_id),
            workflow_version=spl.workflow_version,
            status=RunStatus(spl.status),
            reason=spl.reason,
            started_at=spl.started_at,
            updated_at=spl.updated_at,
            compiled_graph=self._graphs.get((str(spl.workspace_id), str(spl.run_id))),
        )

    # ----- RunStore surface ---------------------------------------------

    async def put_run(self, run: RunRecord) -> RunRecord:
        """Persist *run*, treating same-payload re-puts as no-ops.

        The SPL contract is that :meth:`MetadataStoreProvider.put_run`
        raises :class:`ImmutableViolation` on any re-put. We catch
        that, fetch the existing row, and either:

        * return the existing :class:`RunRecord` unchanged when
          the incoming and existing payloads are byte-equal
          (idempotent re-put);
        * raise :class:`RunStateConflictError` when they diverge.

        The compiled-graph side-map is treated as part of the
        payload — divergent graphs for the same ``(workspace_id,
        run_id)`` are a conflict.
        """

        try:
            spl_persisted = await self._provider.put_run(
                cast(SplWorkspaceId, run.workspace_id),
                self._to_spl_run(run),
            )
        except ImmutableViolation:
            existing = await self.get_run(run.workspace_id, run.run_id)
            if existing is None:
                # Defensive: ImmutableViolation but no existing row
                # is the SPL contract violating itself; surface as
                # a state conflict so callers see a single failure
                # taxonomy.
                raise RunStateConflictError(
                    "put_run raised ImmutableViolation but no existing row was found",
                    run_id=run.run_id,
                ) from None
            if existing == run:
                return existing
            raise RunStateConflictError(
                "put_run for an existing run_id with a divergent payload",
                run_id=run.run_id,
                current_status=existing.status.value,
                attempted_status=run.status.value,
            ) from None

        if run.compiled_graph is not None:
            self._graphs[(run.workspace_id, str(run.run_id))] = run.compiled_graph
        return self._to_record(spl_persisted)

    async def update_run_status(
        self,
        workspace_id: str,
        run_id: RunId,
        status: RunStatus,
        *,
        reason: str | None = None,
    ) -> RunRecord:
        """Validate the transition then delegate to the SPL provider."""

        existing = await self.get_run(workspace_id, run_id)
        if existing is None:
            raise RunStateConflictError(
                "update_run_status called for an unknown run_id",
                run_id=run_id,
                attempted_status=status.value,
            )
        allowed = STATUS_TRANSITIONS[existing.status]
        if status not in allowed:
            raise RunStateConflictError(
                f"illegal status transition: {existing.status.value} -> {status.value}",
                run_id=run_id,
                current_status=existing.status.value,
                attempted_status=status.value,
            )
        spl = await self._provider.update_run_status(
            cast(SplWorkspaceId, workspace_id),
            cast(SplRunId, run_id),
            status.value,
            reason,
        )
        return self._to_record(spl)

    async def get_run(self, workspace_id: str, run_id: RunId) -> RunRecord | None:
        """Return the row or ``None`` when absent."""

        spl = await self._provider.get_run(
            cast(SplWorkspaceId, workspace_id),
            cast(SplRunId, run_id),
        )
        if spl is None:
            return None
        return self._to_record(spl)

    async def list_runs(
        self,
        workspace_id: str,
        *,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[RunRecord]:
        """Return a paginated slice of the workspace's runs.

        Filtering (by status, workflow_id, started_after, …) is
        intentionally not surfaced here: WF-IMPL-038 will add the
        :class:`RunFilter` builder + per-status query parameters
        once the HTTP surface lands.
        """

        spl_page = await self._provider.list_runs(
            cast(SplWorkspaceId, workspace_id),
            filter=None,
            cursor=cursor,
            limit=limit,
        )
        return Page(
            items=tuple(self._to_record(r) for r in spl_page.items),
            next_cursor=spl_page.next_cursor,
        )
