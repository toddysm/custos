"""``RunRecord`` projection + locked status enum + transition table.

This module pins three things that every other Run Controller
sub-module keys off:

1. :class:`RunStatus` — the **locked** 8-value lifecycle enum
   (``queued | running | pausing | paused | cancelling | cancelled
   | succeeded | failed``). The string values are the wire-stable
   names persisted in :class:`custos_spl.Run.status`.

2. :data:`STATUS_TRANSITIONS` — the canonical "allowed move"
   table. :meth:`RunStore.update_run_status` enforces this, so
   adding a status without updating the table is a contract
   break the test suite is wired to fail loudly on.

3. :class:`RunRecord` — the workflow-service-internal projection
   over :class:`custos_spl.Run`. It layers a typed
   :class:`RunStatus` over the SPL's plain ``status: str`` and
   carries the optional compiled :class:`ExecutionGraph` that
   the Step Coordinator drives off of.

The compiled-graph round-trip through the persistent store is
landed by WF-IMPL-033 (#385). The WF-IMPL-032 adapter stashes
the graph in an internal side-map so the WF-IMPL-032 acceptance
criteria (status-transition enforcement + idempotent re-put) can
ship without WF-IMPL-033's serialization concerns.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Final

from custos_workflow.runs.ids import RunId

if TYPE_CHECKING:
    from custos_workflow.graph.model import ExecutionGraph

__all__ = [
    "STATUS_TRANSITIONS",
    "TERMINAL_STATUSES",
    "RunRecord",
    "RunStatus",
    "is_terminal",
]


# ---------------------------------------------------------------------------
# Locked status enum
# ---------------------------------------------------------------------------


class RunStatus(StrEnum):
    """Locked run-lifecycle status values.

    The string values are part of the public wire contract — they
    are persisted verbatim in :attr:`custos_spl.Run.status` and
    surfaced over the Run Controller HTTP / gRPC APIs (WF-IMPL-037
    onwards). Adding, renaming, or removing a member here is a
    downstream contract break.

    The 8 values mirror the design's ``status`` field in
    ``design/components/workflow-service/design.md`` § Data Models.
    """

    QUEUED = "queued"
    RUNNING = "running"
    PAUSING = "pausing"
    PAUSED = "paused"
    CANCELLING = "cancelling"
    CANCELLED = "cancelled"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


#: Terminal statuses have no out-edges in :data:`STATUS_TRANSITIONS`.
#: Computed once at import time so callers can fast-check
#: ``status in TERMINAL_STATUSES`` without re-walking the table.
TERMINAL_STATUSES: Final[frozenset[RunStatus]] = frozenset(
    {RunStatus.CANCELLED, RunStatus.SUCCEEDED, RunStatus.FAILED}
)


#: The canonical allowed-transition table for the run lifecycle.
#:
#: ``STATUS_TRANSITIONS[from_status]`` is the frozenset of statuses
#: ``from_status`` may move to. Terminal statuses
#: (:data:`TERMINAL_STATUSES`) map to the empty frozenset — they
#: are absorbing.
#:
#: Every illegal transition raises
#: :class:`custos_workflow.runs.errors.RunStateConflictError` from
#: :meth:`RunStore.update_run_status`. The transition table is
#: pinned by the test suite as a single source of truth: adding a
#: :class:`RunStatus` member without updating this dict fails the
#: build.
#:
#: Topology summary:
#:
#: * ``queued``     → ``running``, ``cancelling``, ``failed``
#: * ``running``    → ``pausing``, ``cancelling``, ``succeeded``, ``failed``
#: * ``pausing``    → ``paused``, ``cancelling``, ``failed``
#: * ``paused``     → ``running``, ``cancelling``, ``failed``
#: * ``cancelling`` → ``cancelled``, ``failed``
#: * ``cancelled``  → ø (terminal)
#: * ``succeeded``  → ø (terminal)
#: * ``failed``     → ø (terminal)
STATUS_TRANSITIONS: Final[dict[RunStatus, frozenset[RunStatus]]] = {
    RunStatus.QUEUED: frozenset({RunStatus.RUNNING, RunStatus.CANCELLING, RunStatus.FAILED}),
    RunStatus.RUNNING: frozenset(
        {
            RunStatus.PAUSING,
            RunStatus.CANCELLING,
            RunStatus.SUCCEEDED,
            RunStatus.FAILED,
        }
    ),
    RunStatus.PAUSING: frozenset({RunStatus.PAUSED, RunStatus.CANCELLING, RunStatus.FAILED}),
    RunStatus.PAUSED: frozenset({RunStatus.RUNNING, RunStatus.CANCELLING, RunStatus.FAILED}),
    RunStatus.CANCELLING: frozenset({RunStatus.CANCELLED, RunStatus.FAILED}),
    RunStatus.CANCELLED: frozenset(),
    RunStatus.SUCCEEDED: frozenset(),
    RunStatus.FAILED: frozenset(),
}


def is_terminal(status: RunStatus) -> bool:
    """Return ``True`` iff *status* is a terminal status."""

    return status in TERMINAL_STATUSES


# ---------------------------------------------------------------------------
# RunRecord
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RunRecord:
    """Workflow-service-internal projection over :class:`custos_spl.Run`.

    Layers a typed :class:`RunStatus` over the SPL's plain ``status:
    str`` and carries the optional compiled
    :class:`ExecutionGraph` that the Step Coordinator drives off
    of. Frozen / hashable so dict-keying + dedup of in-flight
    snapshots is safe.

    The :attr:`compiled_graph` field is locked here so the
    Step Coordinator (WF-IMPL-036+) can rely on the field's
    presence and type, but its round-trip through the persistent
    store is the WF-IMPL-033 (#385) deliverable. The WF-IMPL-032
    adapter stores the graph in an internal side-map.

    Attributes:
        workspace_id: The owning workspace.
        run_id: The deterministic-or-random identifier from
            :func:`custos_workflow.runs.derive_run_id`.
        workflow_id: The workflow this run instantiates.
        workflow_version: The workflow version string (the SPL
            persists this as a plain ``str``).
        status: The locked :class:`RunStatus`.
        reason: Optional human-readable explanation attached to
            the current status (e.g. failure reason on
            ``failed``, caller-supplied cancellation reason on
            ``cancelled``). ``None`` when no reason was provided.
        started_at: When the row was first persisted.
        updated_at: When the row was last updated (status change
            or graph mutation).
        compiled_graph: The compiled execution plan (WF-IMPL-018
            output). ``None`` until the compile step has run.
            Persistence round-trip is the WF-IMPL-033 deliverable.
    """

    workspace_id: str
    run_id: RunId
    workflow_id: str
    workflow_version: str
    status: RunStatus
    reason: str | None
    started_at: datetime
    updated_at: datetime
    compiled_graph: ExecutionGraph | None = None
