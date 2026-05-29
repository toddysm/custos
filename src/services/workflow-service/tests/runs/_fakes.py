"""Test fakes for the WF-IMPL-032 Run row CRUD surface.

A minimal in-memory :class:`custos_spl.MetadataStoreProvider`
fake covering the four methods the :class:`InProcessRunStore`
adapter calls today: ``put_run`` / ``update_run_status`` /
``get_run`` / ``list_runs``.

Mirrors the sibling pattern at
``src/services/auth-service/tests/_fakes.py``. Tests pass the
fake to :class:`InProcessRunStore` via a
``cast(MetadataStoreProvider, fake)`` to silence the Protocol's
"unimplemented method" complaints from strict mypy — every other
sub-module's fake follows the same convention.
"""

from __future__ import annotations

from custos_spl.errors import ImmutableViolation
from custos_spl.ids import RunId as SplRunId
from custos_spl.ids import WorkspaceId as SplWorkspaceId
from custos_spl.interfaces.metadata_store import (
    Run as SplRun,
)
from custos_spl.interfaces.metadata_store import (
    RunFilter,
)
from custos_spl.pagination import Cursor, Page


class FakeMetadataStoreProvider:
    """An in-memory subset of :class:`MetadataStoreProvider`.

    Only the Run-row methods are implemented. Pagination uses
    simple offset-encoded :class:`Cursor` tokens (``str(int)``)
    so tests can exercise the cursor round-trip without bringing
    the keyset machinery in.
    """

    def __init__(self) -> None:
        # Key: (workspace_id, run_id). Value: persisted Run row.
        self._runs: dict[tuple[str, str], SplRun] = {}

    async def put_run(self, workspace_id: SplWorkspaceId, run: SplRun) -> SplRun:
        key = (str(workspace_id), str(run.run_id))
        if key in self._runs:
            raise ImmutableViolation(
                f"run already exists: workspace_id={workspace_id!r} run_id={run.run_id!r}"
            )
        self._runs[key] = run
        return run

    async def update_run_status(
        self,
        workspace_id: SplWorkspaceId,
        run_id: SplRunId,
        status: str,
        reason: str | None = None,
    ) -> SplRun:
        key = (str(workspace_id), str(run_id))
        existing = self._runs.get(key)
        if existing is None:
            raise KeyError(key)
        updated = SplRun(
            workspace_id=existing.workspace_id,
            run_id=existing.run_id,
            workflow_id=existing.workflow_id,
            workflow_version=existing.workflow_version,
            status=status,
            reason=reason,
            started_at=existing.started_at,
            updated_at=existing.updated_at,
        )
        self._runs[key] = updated
        return updated

    async def get_run(self, workspace_id: SplWorkspaceId, run_id: SplRunId) -> SplRun | None:
        return self._runs.get((str(workspace_id), str(run_id)))

    async def list_runs(
        self,
        workspace_id: SplWorkspaceId,
        filter: RunFilter | None = None,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[SplRun]:
        ws = str(workspace_id)
        # Workspace-scoped slice; dict preserves insertion order.
        all_rows = [r for (w, _), r in self._runs.items() if w == ws]
        offset = int(cursor.token) if cursor is not None else 0
        if limit is None:
            window = all_rows[offset:]
            next_cursor = None
        else:
            window = all_rows[offset : offset + limit]
            next_offset = offset + len(window)
            next_cursor = Cursor(token=str(next_offset)) if next_offset < len(all_rows) else None
        return Page(items=window, next_cursor=next_cursor)
