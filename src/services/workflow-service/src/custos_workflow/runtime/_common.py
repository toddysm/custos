"""Shared types and frozen request dataclasses for the runtime adapter.

This module is intentionally Dapr-free: nothing here imports
``dapr.ext.workflow``. The :class:`RunStatus` enum mirrors
:class:`dapr.ext.workflow.WorkflowStatus` so Run Controller callers
never need to import a Dapr symbol; the real :class:`WorkflowClient`
performs the mapping in :mod:`custos_workflow.runtime.dapr`, and the
fake constructs :class:`RunState` values directly.

Request dataclasses are all ``@dataclass(frozen=True)``: they cross
the async boundary into ``asyncio.to_thread`` worker threads in the
real adapter, so immutability matters. Optional fields default to
``None`` to keep the surface ergonomic at call sites while staying
explicit about every argument the Dapr SDK accepts.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Final, TypeAlias


class RunStatus(Enum):
    """Run lifecycle status, mirroring :class:`dapr.ext.workflow.WorkflowStatus`.

    The string values are the canonical lowercase names that surface
    in audit / lifecycle event payloads emitted by the Run Controller
    (WF-IMPL-041). They are stable contract and must not change.
    """

    UNKNOWN = "unknown"
    PENDING = "pending"
    RUNNING = "running"
    SUSPENDED = "suspended"
    COMPLETED = "completed"
    FAILED = "failed"
    TERMINATED = "terminated"
    STALLED = "stalled"


#: Sentinel set of terminal :class:`RunStatus` values. A run in any of
#: these states will not transition further; the Run Controller uses
#: this in :meth:`RunController.get_run` (WF-IMPL-040) to short-circuit
#: status polling.
TERMINAL_STATUSES: Final[frozenset[RunStatus]] = frozenset(
    {RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.TERMINATED}
)


@dataclass(frozen=True)
class RunState:
    """Snapshot of a Run instance returned by :meth:`WorkflowClient.get_workflow_state`.

    Mirrors :class:`dapr.ext.workflow.WorkflowState` but exposes only
    the fields the Run Controller actually persists or surfaces. All
    payload fields are kept as raw JSON strings (``serialized_input``
    / ``serialized_output``) so callers can decide whether to parse;
    Dapr serializes its own payloads as JSON and we preserve that.
    """

    instance_id: str
    name: str
    status: RunStatus
    created_at: datetime | None
    last_updated_at: datetime | None
    serialized_input: str | None
    serialized_output: str | None
    failure_message: str | None = None
    failure_type: str | None = None


#: Accepted shapes for the ``workflow`` argument to
#: :meth:`WorkflowClient.schedule_new_workflow`. The Dapr SDK accepts
#: either the registered workflow function (it derives the name from
#: ``__name__`` / the ``name=`` kwarg used at registration) or the
#: string name itself. Run Controller call sites (WF-IMPL-037) always
#: pass the string name; the callable form is supported here for
#: parity and to keep the surface symmetric with the SDK.
WorkflowRef: TypeAlias = Callable[..., Any] | str


@dataclass(frozen=True)
class ScheduleWorkflowRequest:
    """Request payload for :meth:`WorkflowClient.schedule_new_workflow`."""

    workflow: WorkflowRef
    input: Any = None
    instance_id: str | None = None
    start_at: datetime | None = None


@dataclass(frozen=True)
class GetRunStateRequest:
    """Request payload for :meth:`WorkflowClient.get_workflow_state`."""

    instance_id: str
    fetch_payloads: bool = True


@dataclass(frozen=True)
class TerminateRunRequest:
    """Request payload for :meth:`WorkflowClient.terminate_workflow`."""

    instance_id: str
    output: Any = None
    recursive: bool = True


@dataclass(frozen=True)
class PauseRunRequest:
    """Request payload for :meth:`WorkflowClient.pause_workflow`."""

    instance_id: str


@dataclass(frozen=True)
class ResumeRunRequest:
    """Request payload for :meth:`WorkflowClient.resume_workflow`."""

    instance_id: str


@dataclass(frozen=True)
class RaiseRunEventRequest:
    """Request payload for :meth:`WorkflowClient.raise_workflow_event`."""

    instance_id: str
    event_name: str
    data: Any = None


__all__ = [
    "TERMINAL_STATUSES",
    "GetRunStateRequest",
    "PauseRunRequest",
    "RaiseRunEventRequest",
    "ResumeRunRequest",
    "RunState",
    "RunStatus",
    "ScheduleWorkflowRequest",
    "TerminateRunRequest",
    "WorkflowRef",
]
