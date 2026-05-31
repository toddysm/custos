"""``StepHandler`` Protocol — the Run Controller ↔ Step Coordinator boundary (WF-IMPL-034).

The Run Controller orchestrator drives a Dapr Workflow over an
:class:`~custos_workflow.graph.model.ExecutionGraph`: it walks the
topological order, evaluates each node's gating expressions, and
dispatches every node that survives gating through a
:class:`StepHandler`. The handler returns one of four canonical
:data:`StepResult` shapes; the orchestrator dispatches on that
shape and updates the per-run output bag accordingly.

This module is the *contract surface only*. The real handler that
drives :class:`~custos_workflow.graph.model.StepKind.ACTIVITY` /
``WORKFLOW`` / future ``approval`` / ``for`` / ``parallel`` steps
is the Step Coordinator sub-module, which is out of scope for the
Run Controller plan and lands separately. To let WF-IMPL-035 ship
the orchestrator without dragging Step Coordinator scope in, this
module also exports a :class:`NoopStepHandler` test default that
handles the orchestrator-inline ``let:`` kind and explicitly
:class:`NotImplementedError`-s on every other kind.

Public surface:

* :class:`WorkflowContext` — narrow structural Protocol covering
  the subset of the Dapr Workflow context surface every Run
  Controller code path actually uses (``instance_id``,
  ``current_utc_datetime``, ``is_replaying``,
  ``set_custom_status``). Both
  :class:`~custos_workflow.runtime.FakeWorkflowContext` and the
  real :class:`dapr.ext.workflow.DaprWorkflowContext` structurally
  satisfy it, so the orchestrator never imports
  ``dapr.ext.workflow`` directly.
* :class:`StepExecutionContext` — the immutable bundle Run
  Controller passes to :meth:`StepHandler.execute` on every step
  dispatch. The frozen wrapper protects the bundle from
  accidental mutation while the per-run output bag remains a
  plain :class:`dict` so the orchestrator can collect outputs
  into it.
* :data:`StepResult` — the frozen four-variant union the
  orchestrator dispatches on: :class:`StepSucceeded`,
  :class:`StepFailed`, :class:`StepSkipped`,
  :class:`StepWaiting`.
* :data:`_STEP_RESULT_VARIANTS` — exhaustive tuple of the
  :data:`StepResult` variants. The orchestrator (WF-IMPL-035)
  imports this and pins a match-arms exhaustiveness assertion at
  module import time so adding a fifth variant in the future
  fails the build until every dispatch site is updated.
* :class:`StepHandler` — the runtime-checkable Protocol with one
  method, :meth:`execute`.
* :class:`NoopStepHandler` — concrete default referenced by
  WF-IMPL-035 tests; handles ``let:`` inline and raises
  :class:`NotImplementedError` on every other kind.

Acceptance criteria (mirrored from #386):

* Protocol is ``runtime_checkable``;
  ``isinstance(NoopStepHandler(), StepHandler)`` returns ``True``.
* Every :data:`StepResult` variant is a frozen dataclass — direct
  attribute mutation raises :class:`dataclasses.FrozenInstanceError`.
* :data:`_STEP_RESULT_VARIANTS` enumerates the union exhaustively
  (the test re-derives the union members from
  :data:`StepResult.__args__` and asserts byte-equal membership).
* 100 % line + branch coverage on this module.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Final, Protocol, runtime_checkable

if TYPE_CHECKING:
    from custos_cel.clock import Clock

    from custos_workflow.graph.model import ExecutionGraph
    from custos_workflow.runs.ids import RunId

__all__ = [
    "NoopStepHandler",
    "StepExecutionContext",
    "StepFailed",
    "StepHandler",
    "StepResult",
    "StepSkipped",
    "StepSucceeded",
    "StepWaiting",
    "WorkflowContext",
]


# ---------------------------------------------------------------------------
# Workflow-context Protocol (narrow structural surface)
# ---------------------------------------------------------------------------


@runtime_checkable
class WorkflowContext(Protocol):
    """Structural surface every Run Controller code path keys off.

    Covers two slices of the Dapr Workflow context that Run
    Controller code drives:

    1. **Observability / lifecycle properties** — ``instance_id``,
       ``current_utc_datetime``, ``is_replaying``, plus the
       :meth:`set_custom_status` mutator.
    2. **Durable I/O yield-targets** — :meth:`call_activity`,
       :meth:`wait_for_external_event`, :meth:`create_timer`.
       Step Coordinator handlers (WF-IMPL-035+) drive these to
       request external work; the orchestrator yields the returned
       opaque task token back to the Dapr runtime, which suspends
       the workflow until the corresponding result arrives.

    Both :class:`~custos_workflow.runtime.FakeWorkflowContext` and
    the real :class:`dapr.ext.workflow.DaprWorkflowContext`
    structurally satisfy this surface, so the orchestrator and
    Step Coordinator handlers can target a single type and tests
    can drop the fake in interchangeably. Yield-target return
    values are typed as :class:`typing.Any` — they are opaque
    tokens whose concrete type differs between the Dapr SDK
    (``Task``) and the fake (private dataclasses), and they are
    never inspected by Run Controller code, only re-yielded.
    """

    @property
    def instance_id(self) -> str: ...

    @property
    def current_utc_datetime(self) -> datetime: ...

    @property
    def is_replaying(self) -> bool: ...

    def set_custom_status(self, custom_status: str) -> None: ...

    def call_activity(
        self,
        activity: Callable[..., Any] | str,
        *,
        input: Any = None,
    ) -> Any: ...

    def wait_for_external_event(self, name: str) -> Any: ...

    def create_timer(self, fire_at: datetime | timedelta) -> Any: ...


# ---------------------------------------------------------------------------
# Per-dispatch context bundle
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class StepExecutionContext:
    """Immutable bundle Run Controller passes to :meth:`StepHandler.execute`.

    The bundle itself is frozen; the :attr:`outputs` bag remains a
    plain :class:`dict` because the orchestrator collects outputs
    *into* it across the topological walk. Handlers MUST treat
    :attr:`outputs` as read-only — write-back is the orchestrator's
    job after the handler returns :class:`StepSucceeded`.

    Attributes:
        run_id: The Run instance id.
        workspace_id: The owning workspace.
        workflow_version_id: The Catalog Workflow Version id this
            run was started against (frozen at start time). Surfaced
            so handlers can build a :class:`custos_cel.BindingScope`
            whose ``workflow.version`` resolves to the same string
            the orchestrator's gate evaluator uses (WF-IMPL-052
            consistency fix).
        inputs: User-supplied run inputs — the values keyed off
            ``inputs.*`` in CEL expressions. Wrapped in
            :class:`MappingProxyType` by :func:`_step_ctx` so
            handlers cannot mutate the orchestrator's snapshot.
        workflow_context: Typed view of the Dapr Workflow context
            (see :class:`WorkflowContext`). Both the real
            :class:`dapr.ext.workflow.DaprWorkflowContext` and the
            test :class:`~custos_workflow.runtime.FakeWorkflowContext`
            structurally satisfy this Protocol.
        outputs: Per-run output bag, keyed by ``step_id``. Read-only
            from the handler's perspective; the orchestrator
            populates it post-:meth:`execute`.
        clock: Replay-deterministic clock used by CEL evaluation
            (typically :class:`custos_cel.DaprWorkflowClock` in
            production, :class:`custos_cel.FixedClock` in tests).
    """

    run_id: RunId
    workspace_id: str
    workflow_version_id: str
    inputs: Mapping[str, Any]
    workflow_context: WorkflowContext
    outputs: Mapping[str, Mapping[str, Any]]
    clock: Clock


# ---------------------------------------------------------------------------
# StepResult union
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class StepSucceeded:
    """The step ran to completion and produced :attr:`outputs`."""

    outputs: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class StepFailed:
    """The step terminated with an error envelope.

    :attr:`envelope` is the canonical Custos error envelope
    (``{"kind": ..., "message": ..., ...}``) that the audit /
    lifecycle event emitter (WF-IMPL-041) ships verbatim.
    """

    envelope: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class StepSkipped:
    """The step's ``if:`` / ``when:`` / ``unless:`` gate excluded it.

    :attr:`reason` is a short, log-safe summary of the gate decision
    (e.g. ``"if=false"``); it surfaces in the lifecycle event
    payload so operators can audit branch decisions.
    """

    reason: str


@dataclass(frozen=True, slots=True)
class StepWaiting:
    """The step paused on an external signal (durable timer / event).

    Surfaced when ``wait:`` / ``waitFor`` lands; today's
    :class:`NoopStepHandler` never returns this shape, but the
    orchestrator (WF-IMPL-035) must already handle it so the
    landing path is type-safe.
    """

    reason: str


#: The four canonical :class:`StepHandler.execute` return shapes.
#:
#: The orchestrator's match arms MUST cover every member of this
#: union. The exhaustive tuple below is the build-time guard that
#: catches a future fifth variant before it ships.
StepResult = StepSucceeded | StepFailed | StepSkipped | StepWaiting


#: Exhaustive enumeration of the :data:`StepResult` union members.
#:
#: Imported by WF-IMPL-035 to pin a one-line assertion at module
#: import time so adding a variant without extending the
#: orchestrator's match arms fails the build. Kept private (``_``
#: prefix) because callers should match on the union, not iterate
#: this tuple.
_STEP_RESULT_VARIANTS: Final[tuple[type, ...]] = (
    StepSucceeded,
    StepFailed,
    StepSkipped,
    StepWaiting,
)


# ---------------------------------------------------------------------------
# StepHandler Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class StepHandler(Protocol):
    """The Run Controller ↔ Step Coordinator dispatch surface.

    A single method, :meth:`execute`, takes the per-dispatch
    :class:`StepExecutionContext`, the compiled
    :class:`~custos_workflow.graph.model.ExecutionGraph`, and the
    target ``step_id``; returns one of the four
    :data:`StepResult` variants.

    The method is intentionally synchronous: it runs inside the
    Dapr Workflow orchestrator function, which is itself a Dapr
    generator. Any I/O the Step Coordinator needs (calling an
    activity, raising a sub-workflow, opening a timer) happens by
    yielding the appropriate token through the
    :attr:`StepExecutionContext.workflow_context` — not by
    awaiting inside :meth:`execute`.
    """

    def execute(
        self,
        ctx: StepExecutionContext,
        graph: ExecutionGraph,
        step_id: str,
    ) -> StepResult: ...


# ---------------------------------------------------------------------------
# NoopStepHandler — test/landing default
# ---------------------------------------------------------------------------


class NoopStepHandler:
    """Default :class:`StepHandler` until the Step Coordinator ships.

    Handles exactly one step kind inline:

    * :class:`~custos_workflow.graph.model.StepKind.LET` — delegates
      to :class:`custos_workflow.steps.LetStepHandler`, the dedicated
      WF-IMPL-052 handler that evaluates the step's ``let:`` bindings
      against the current per-run scope. The handler ships behind a
      module-local import so the
      :mod:`custos_workflow.runs.step_handler` module stays free of
      any inbound dependency from
      :mod:`custos_workflow.steps` — that subpackage is allowed to
      import from :mod:`custos_workflow.runs`, but not the other way
      round.

    Every other :class:`~custos_workflow.graph.model.StepKind`
    raises :class:`NotImplementedError`. Tests that need a real
    handler must substitute their own implementation.

    Unknown ``step_id`` raises :class:`KeyError`.
    """

    def execute(
        self,
        ctx: StepExecutionContext,
        graph: ExecutionGraph,
        step_id: str,
    ) -> StepResult:
        from custos_workflow.graph.model import StepKind
        from custos_workflow.steps.let_step import LetStepHandler

        node = next((n for n in graph.nodes if n.step_id == step_id), None)
        if node is None:
            raise KeyError(step_id)
        if node.kind is StepKind.LET:
            return LetStepHandler().execute(ctx, graph, step_id)
        raise NotImplementedError("StepHandler.execute")
