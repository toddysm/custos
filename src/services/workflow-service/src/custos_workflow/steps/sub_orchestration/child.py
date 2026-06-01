"""Child sub-workflow orchestrator function (WF-IMPL-088).

The Sub-Orchestration Manager (ADR-007) spawns one **child Dapr
Workflow instance** per loop iteration (``forEach``) and per
sub-workflow invocation (``workflow:``). Every such child runs a
single inner-step body — an ``activity:`` or ``let:`` step — and
hands its outputs back to the parent, which merges them into
``steps.<stepId>.outputs`` in its own expression scope.

This module ships that child workflow function. It is intentionally
*thin*: it does **not** re-implement step dispatch. It rebuilds a
:class:`~custos_workflow.runs.StepExecutionContext` for the single
inner step and delegates to the same
:class:`~custos_workflow.runs.StepHandler` the parent orchestrator
uses (typically a
:class:`~custos_workflow.steps.step_coordinator.StepCoordinator`),
so the inner step type-checks and executes exactly as it would in a
top-level run.

Item scoping
------------

For a loop body the per-iteration item must be visible to the inner
step's CEL expressions. The CEL :class:`custos_cel.BindingScope`
exposes a fixed set of roots (``inputs`` / ``steps`` / ``run`` /
``workflow`` / ``let`` / ``now``); the loop variable is therefore
surfaced under the ``inputs`` namespace, keyed by
:attr:`ChildStepInput.loop_var` (default :data:`DEFAULT_LOOP_VAR`).
A loop body referencing ``${{ inputs.item.image }}`` resolves the
spawned item. Sub-workflow invocation (WF-IMPL-091) carries no
per-iteration item, so it passes an empty :attr:`~ChildStepInput.loop_var`
to skip the injection and expose only the parent-supplied inputs.

Determinism & replay
--------------------

The function is a Dapr generator workflow. It performs no wall-clock
reads of its own — the inner handler observes time only through the
replay-safe :class:`custos_cel.DaprWorkflowClock` built from the
child context — and it derives no nondeterministic state, so Dapr
replay reproduces an identical result. It is runnable under
:class:`~custos_workflow.runtime.FakeWorkflowRuntime` (schedule it
directly or spawn it via :meth:`call_child_workflow`).

Failure surface
---------------

Whatever the inner handler returns is normalised into a
:class:`ChildStepResult` envelope the parent can dispatch on:

* :class:`~custos_workflow.runs.StepSucceeded` → ``status="succeeded"``
  carrying the step outputs.
* :class:`~custos_workflow.runs.StepSkipped` → ``status="succeeded"``
  with empty outputs (a gated-out iteration contributes nothing,
  mirroring the top-level orchestrator).
* :class:`~custos_workflow.runs.StepFailed` → ``status="failed"``
  carrying the inner step's canonical error envelope, so the
  parent loop can raise ``step.sub_workflow_failed`` (WF-IMPL-089).
"""

from __future__ import annotations

from collections.abc import Callable, Generator, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Final, cast

from custos_cel import DaprWorkflowClock

from custos_workflow.graph.serialize import from_json
from custos_workflow.runs import (
    RunId,
    StepExecutionContext,
    StepFailed,
    StepHandler,
    StepResult,
    StepSkipped,
    StepSucceeded,
    WorkflowContext,
)

__all__ = [
    "CHILD_STEP_WORKFLOW_NAME",
    "DEFAULT_LOOP_VAR",
    "ChildStepInput",
    "ChildStepResult",
    "make_child_step_orchestrator",
]


#: Wire-stable Dapr workflow name the child orchestrator registers
#: under. The parent's :meth:`call_child_workflow` call site
#: (WF-IMPL-089 / WF-IMPL-091) passes this constant; production
#: wiring (WF-IMPL-094) registers the function the factory below
#: returns under this name.
CHILD_STEP_WORKFLOW_NAME: Final[str] = "custos.workflow.child_step"


#: Default name the per-iteration loop item is surfaced under inside
#: the inner step's ``inputs`` namespace (``inputs.item.*``).
DEFAULT_LOOP_VAR: Final[str] = "item"


@dataclass(frozen=True, slots=True)
class ChildStepInput:
    """JSON-serialisable input envelope the child workflow consumes.

    Carries everything the child needs to rebuild the inner step's
    :class:`~custos_workflow.runs.StepExecutionContext` without
    consulting any external service — the parent freezes it at spawn
    time so Dapr replay re-hydrates an identical context.

    Attributes:
        workspace_id: The owning workspace (surfaces as ``run.workspace``).
        workflow_version_id: The Catalog Workflow Version id the run
            was started against (surfaces as ``workflow.version``).
        compiled_graph_json: The byte-stable JSON envelope (see
            :func:`custos_workflow.graph.serialize.to_json`) of the
            single-node graph carrying the inner step.
        step_id: The id of the inner step to execute within
            ``compiled_graph_json``.
        inputs: The parent run's ``inputs.*`` namespace, propagated so
            the inner body resolves run inputs identically.
        parent_outputs: Snapshot of preceding step outputs the inner
            body may reference via ``steps.<id>.outputs.*``.
        loop_var: Name the per-iteration :attr:`item` is surfaced
            under inside ``inputs`` (``inputs.<loop_var>.*``). Empty
            string skips item injection (sub-workflow invocation).
        item: The per-iteration loop item. Ignored when
            :attr:`loop_var` is empty.
    """

    workspace_id: str
    workflow_version_id: str
    compiled_graph_json: str
    step_id: str
    inputs: Mapping[str, Any] = field(default_factory=dict)
    parent_outputs: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    loop_var: str = DEFAULT_LOOP_VAR
    item: Any = None

    def to_dict(self) -> dict[str, Any]:
        """Render to a JSON-serialisable dict for the Dapr boundary."""

        return {
            "workspace_id": self.workspace_id,
            "workflow_version_id": self.workflow_version_id,
            "compiled_graph_json": self.compiled_graph_json,
            "step_id": self.step_id,
            "inputs": dict(self.inputs),
            "parent_outputs": {sid: dict(out) for sid, out in self.parent_outputs.items()},
            "loop_var": self.loop_var,
            "item": self.item,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> ChildStepInput:
        """Reconstruct from a JSON-decoded mapping."""

        return cls(
            workspace_id=str(payload["workspace_id"]),
            workflow_version_id=str(payload["workflow_version_id"]),
            compiled_graph_json=str(payload["compiled_graph_json"]),
            step_id=str(payload["step_id"]),
            inputs=dict(payload.get("inputs") or {}),
            parent_outputs={
                str(sid): dict(out) for sid, out in (payload.get("parent_outputs") or {}).items()
            },
            loop_var=str(payload.get("loop_var", DEFAULT_LOOP_VAR)),
            item=payload.get("item"),
        )


@dataclass(frozen=True, slots=True)
class ChildStepResult:
    """JSON-serialisable output envelope the child workflow returns.

    Normalises the inner :class:`~custos_workflow.runs.StepResult`
    into a shape the parent can dispatch on without importing the Run
    Controller's result union across the Dapr boundary.

    Attributes:
        status: ``"succeeded"`` or ``"failed"``.
        outputs: The inner step's outputs on success; empty otherwise.
        failure_envelope: The inner step's canonical error envelope on
            failure; ``None`` on success.
    """

    status: str
    outputs: Mapping[str, Any] = field(default_factory=dict)
    failure_envelope: Mapping[str, Any] | None = None

    @classmethod
    def succeeded(cls, outputs: Mapping[str, Any]) -> ChildStepResult:
        """Build a success envelope carrying ``outputs``."""

        return cls(status="succeeded", outputs=dict(outputs))

    @classmethod
    def failed(cls, envelope: Mapping[str, Any]) -> ChildStepResult:
        """Build a failure envelope carrying the inner error ``envelope``."""

        return cls(status="failed", failure_envelope=dict(envelope))

    def to_dict(self) -> dict[str, Any]:
        """Render to a JSON-serialisable dict for the Dapr boundary."""

        return {
            "status": self.status,
            "outputs": dict(self.outputs),
            "failure_envelope": (
                dict(self.failure_envelope) if self.failure_envelope is not None else None
            ),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> ChildStepResult:
        """Reconstruct from a JSON-decoded mapping."""

        envelope = payload.get("failure_envelope")
        return cls(
            status=str(payload["status"]),
            outputs=dict(payload.get("outputs") or {}),
            failure_envelope=dict(envelope) if envelope is not None else None,
        )


def make_child_step_orchestrator(
    handler: StepHandler,
) -> Callable[[WorkflowContext, Any], Generator[Any, Any, dict[str, Any]]]:
    """Build the child workflow function bound to a concrete handler.

    The returned callable matches the shape Dapr's Python SDK expects
    from a generator-style workflow function
    (``Callable[[ctx, input], Generator[Any, Any, Any]]``). Tests
    register it on a
    :class:`~custos_workflow.runtime.FakeWorkflowRuntime` under
    :data:`CHILD_STEP_WORKFLOW_NAME`; production wiring (WF-IMPL-094)
    registers it on the real
    :class:`~custos_workflow.runtime.WorkflowRuntime`.

    Args:
        handler: The :class:`~custos_workflow.runs.StepHandler` the
            inner ``activity:`` / ``let:`` step is dispatched through
            — the same handler the top-level orchestrator binds.

    Returns:
        The child workflow function. Its ``__name__`` is
        :data:`CHILD_STEP_WORKFLOW_NAME` so
        :meth:`WorkflowRuntime.register_workflow` picks up the Dapr
        workflow name automatically.
    """

    def run_child_step(ctx: WorkflowContext, raw: Any) -> Generator[Any, Any, dict[str, Any]]:
        # The child body is a single ``activity:`` / ``let:`` step
        # whose handler resolves synchronously, so this generator
        # never opens a durable yield. The unreachable ``yield``
        # keeps Python treating the function as a generator (the
        # shape the runtime drives), matching the convention the
        # Sub-Orchestration runtime primitives use for childless
        # workflow bodies.
        if False:  # pragma: no cover - generator shape, never executed
            yield None

        child_input = raw if isinstance(raw, ChildStepInput) else ChildStepInput.from_dict(raw)
        graph = from_json(child_input.compiled_graph_json)
        clock = DaprWorkflowClock(ctx)
        step_ctx = _child_step_ctx(ctx, child_input, clock)

        result = handler.execute(step_ctx, graph, child_input.step_id)
        return _normalise(result).to_dict()

    run_child_step.__name__ = CHILD_STEP_WORKFLOW_NAME
    return run_child_step


def _child_step_ctx(
    ctx: WorkflowContext,
    child_input: ChildStepInput,
    clock: DaprWorkflowClock,
) -> StepExecutionContext:
    """Build the inner step's frozen :class:`StepExecutionContext`.

    The per-iteration item (when :attr:`~ChildStepInput.loop_var` is
    non-empty) is overlaid onto the parent inputs under that name, so
    the inner body sees it at ``inputs.<loop_var>.*`` while still
    resolving every parent ``inputs.*`` reference unchanged.
    """

    merged_inputs: dict[str, Any] = dict(child_input.inputs)
    if child_input.loop_var:
        merged_inputs[child_input.loop_var] = child_input.item

    outputs_snapshot: Mapping[str, Mapping[str, Any]] = MappingProxyType(
        {sid: MappingProxyType(dict(out)) for sid, out in child_input.parent_outputs.items()}
    )
    return StepExecutionContext(
        run_id=cast(RunId, ctx.instance_id),
        workspace_id=child_input.workspace_id,
        workflow_version_id=child_input.workflow_version_id,
        inputs=MappingProxyType(merged_inputs),
        workflow_context=ctx,
        outputs=outputs_snapshot,
        clock=clock,
    )


def _normalise(result: StepResult) -> ChildStepResult:
    """Collapse the inner :class:`StepResult` into a child envelope.

    A skipped inner step contributes empty outputs — the same way the
    top-level orchestrator records ``{}`` for a gated-out step — so
    the iteration succeeds with nothing to merge. ``StepWaiting`` is
    unreachable for an ``activity:`` / ``let:`` body (neither opens a
    durable wait); the approval gate (WF-IMPL-092) owns the suspending
    path through its own child function.
    """

    if isinstance(result, StepSucceeded):
        return ChildStepResult.succeeded(result.outputs)
    if isinstance(result, StepSkipped):
        return ChildStepResult.succeeded({})
    if isinstance(result, StepFailed):
        return ChildStepResult.failed(result.envelope)
    raise NotImplementedError(  # pragma: no cover - defensive; see docstring
        f"child step body returned an unsupported result variant: {type(result).__name__}"
    )
