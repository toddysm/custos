"""Workflow runtime adapter for the Run Controller (WF-IMPL-029).

This subpackage isolates every Dapr Workflow Python SDK import behind a
thin adapter. The Run Controller code in :mod:`custos_workflow` only
interacts with :class:`WorkflowRuntime` and :class:`WorkflowClient` from
this module; tests substitute :class:`FakeWorkflowRuntime` and
:class:`FakeWorkflowClient` so the suite runs without a live Dapr
sidecar.

The contract is intentionally narrow — only the methods the Run
Controller actually needs:

* :meth:`WorkflowRuntime.register_workflow` /
  :meth:`WorkflowRuntime.register_activity` /
  :meth:`WorkflowRuntime.start` / :meth:`WorkflowRuntime.shutdown` for
  the worker lifecycle (called from the FastAPI ``lifespan`` in
  WF-IMPL-043).
* :meth:`WorkflowClient.schedule_new_workflow` /
  :meth:`WorkflowClient.get_workflow_state` /
  :meth:`WorkflowClient.terminate_workflow` /
  :meth:`WorkflowClient.pause_workflow` /
  :meth:`WorkflowClient.resume_workflow` /
  :meth:`WorkflowClient.raise_workflow_event` for the public
  :class:`RunController` surface (WF-IMPL-037 ... WF-IMPL-040).

Every :class:`WorkflowClient` method takes a single
``@dataclass(frozen=True)`` request object and is declared ``async``;
the real adapter delegates to the sync Dapr SDK via
:func:`asyncio.to_thread` so the host event loop never blocks on a
sidecar round-trip.

See the implementation plan at
``design/components/workflow-service/implementation-plan.md`` § Phase A.
"""

from __future__ import annotations

from custos_workflow.runtime._common import (
    GetRunStateRequest,
    PauseRunRequest,
    RaiseRunEventRequest,
    ResumeRunRequest,
    RunState,
    RunStatus,
    ScheduleWorkflowRequest,
    TerminateRunRequest,
)
from custos_workflow.runtime.dapr import (
    DaprWorkflowContextAdapter,
    WorkflowClient,
    WorkflowRuntime,
)
from custos_workflow.runtime.fake import (
    ChildWorkflowError,
    FakeActivityContext,
    FakeWorkflowClient,
    FakeWorkflowContext,
    FakeWorkflowRuntime,
)

__all__ = [
    "ChildWorkflowError",
    "DaprWorkflowContextAdapter",
    "FakeActivityContext",
    "FakeWorkflowClient",
    "FakeWorkflowContext",
    "FakeWorkflowRuntime",
    "GetRunStateRequest",
    "PauseRunRequest",
    "RaiseRunEventRequest",
    "ResumeRunRequest",
    "RunState",
    "RunStatus",
    "ScheduleWorkflowRequest",
    "TerminateRunRequest",
    "WorkflowClient",
    "WorkflowRuntime",
]
