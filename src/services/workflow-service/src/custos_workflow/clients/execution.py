"""Optional execution-aware client capabilities without changing legacy adapter protocols."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

from custos_callctx.execution import ExecutionIdentity

if TYPE_CHECKING:
    from custos_workflow.runs.controller import WorkflowVersion


@runtime_checkable
class ExecutionCatalogClient(Protocol):
    """Read a version under the run's pinned account, not an ambient or current default identity."""

    async def get_execution_workflow_version(
        self, workspace_id: str, workflow_version_id: str, identity: ExecutionIdentity
    ) -> WorkflowVersion:
        """Authorize this immutable version lookup with a fresh operation-scoped context."""
        ...
