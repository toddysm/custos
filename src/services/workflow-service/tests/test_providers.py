"""Tests for :mod:`custos_workflow.providers` (WF-IMPL-043).

The default in-process metadata-store provider must mirror the
SPL Postgres adapter's ``updated_at`` semantics so consumers see
a fresh timestamp on every status transition. Without this the
default in-memory wiring would silently report stale ``updated_at``
values, which is a hard pitfall to debug downstream (alerts /
observability dashboards that key off the freshness of the row).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from custos_spl.ids import RunId as SplRunId
from custos_spl.ids import WorkflowId as SplWorkflowId
from custos_spl.ids import WorkspaceId as SplWorkspaceId
from custos_spl.interfaces.metadata_store import Run as SplRun

from custos_workflow.providers import _InProcessMetadataStoreProvider


@pytest.mark.asyncio
async def test_update_run_status_refreshes_updated_at() -> None:
    """A status transition must bump ``updated_at`` to "now".

    Mirrors ``custos_pg/adapters/metadata.py``
    (``SET status = $3, reason = $4, updated_at = now()``).
    """
    provider = _InProcessMetadataStoreProvider()
    ws = SplWorkspaceId("ws-1")
    rid = SplRunId("run-1")
    initial_ts = datetime(2000, 1, 1, tzinfo=UTC)
    row = SplRun(
        workspace_id=ws,
        run_id=rid,
        workflow_id=SplWorkflowId("wf-1"),
        workflow_version="1",
        status="queued",
        reason=None,
        started_at=initial_ts,
        updated_at=initial_ts,
    )
    await provider.put_run(ws, row)

    before = datetime.now(UTC)
    updated = await provider.update_run_status(ws, rid, "running")
    after = datetime.now(UTC)

    assert updated.status == "running"
    assert updated.started_at == initial_ts  # immutable on status transitions
    assert before - timedelta(seconds=1) <= updated.updated_at <= after + timedelta(seconds=1)
    assert updated.updated_at > initial_ts
