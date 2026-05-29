"""Replay reconciler hook (WF-IMPL-042).

The Dapr Workflow runtime re-drives an orchestrator function from
the top whenever a worker re-loads its state (pod restart, host
migration, lease loss, etc.). The Step Coordinator (Phase E) needs
to re-register resume subscriptions with the Trigger Service every
time that happens, because the Trigger Service's in-memory
subscription map is itself lost on a Trigger Service pod restart
and there is no other authoritative source for "what runs are
currently waiting on what event".

design.md § Resume Subscription Replay Protocol describes the
contract:

* The orchestrator MUST fire a single reconciler hook **before** the
  first node dispatch on every orchestrator entry.
* The reconciler hook MUST be idempotent — re-registering an already
  registered subscription is a no-op on the Trigger Service side
  (it dedups on ``(runId, stepId, eventKey)`` per
  design.md § Resume Subscription Replay Protocol).
* The hook MUST fire even when the graph has zero waiting steps, so
  the reconciler gets a chance to sweep stale subscriptions that
  are no longer needed.

This module ships only the **wiring** — the Protocol surface every
production reconciler implements, plus a :class:`NoopReplayReconciler`
default the Run Controller uses until WF-IMPL-046 plugs the real
Step Coordinator implementation in. The orchestrator's existing
:data:`~custos_workflow.runs.orchestrator.ReplayHook` callable is
satisfied by binding ``reconciler.on_replay`` to the hook slot.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from custos_workflow.graph.model import ExecutionGraph
from custos_workflow.runs.step_handler import StepExecutionContext

__all__ = [
    "NoopReplayReconciler",
    "ReplayReconciler",
]


@runtime_checkable
class ReplayReconciler(Protocol):
    """Single-method hook the orchestrator calls on every entry.

    Implementations re-register resume subscriptions, sweep stale
    state, or otherwise reconcile external systems with the
    workflow's expected at-run-time state. Called exactly once per
    orchestrator entry (Dapr replay or first execution) BEFORE any
    step is dispatched, so the reconciler observes the same
    ``(run_id, workspace_id, outputs)`` snapshot the first dispatch
    will see — empty ``ctx.outputs`` on the first entry, populated
    from the prior partial bag on a replay-after-suspension.

    The hook MUST be idempotent: replays can fire it arbitrarily
    many times against the same reconstructed state, and the
    Trigger Service (the v1 reconcile target) dedups on
    ``(runId, stepId, eventKey)`` — the resume-subscription
    idempotency tuple locked by design.md § Resume Subscription
    Replay Protocol. Implementations MUST NOT raise; any failure
    must be logged and swallowed so a flapping reconcile
    dependency cannot wedge the orchestrator. The orchestrator
    does not catch exceptions thrown here — a raise will propagate
    into Dapr's non-determinism handling and abort the run.
    """

    def on_replay(self, ctx: StepExecutionContext, graph: ExecutionGraph) -> None:
        """Reconcile external state for the run *ctx* describes.

        ``graph`` is the compiled :class:`ExecutionGraph` the
        orchestrator is about to walk; implementations use it to
        discover which steps may suspend (e.g. nodes with
        ``await:`` kinds) and re-register subscriptions for those.
        """


# ---------------------------------------------------------------------------
# Default no-op
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class NoopReplayReconciler:
    """The default :class:`ReplayReconciler` — does nothing.

    Used by the Run Controller whenever no production reconciler
    has been wired in (development environments, unit tests that
    don't care about the reconcile path, and the pre-Phase-E
    production deployments that still have no Step Coordinator).

    Frozen + slots so two ``NoopReplayReconciler()`` instances
    compare equal and the dataclass hash works for set membership
    in dependency-injection containers.
    """

    def on_replay(self, ctx: StepExecutionContext, graph: ExecutionGraph) -> None:
        """Intentionally empty."""
