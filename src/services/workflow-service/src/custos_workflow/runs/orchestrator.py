"""WF-IMPL-035 — ``run_orchestrator`` Dapr Workflow function.

The Python function the Workflow Service registers with the Dapr
Workflow runtime under the workflow name ``"custos.workflow.run"``.
Every Custos run is an instance of this function: it walks the
topologically-sorted compiled :class:`~custos_workflow.graph.model.ExecutionGraph`,
evaluates each node's ``if:`` / ``when:`` / ``unless:`` guards through
:func:`custos_cel.evaluate` against a :class:`~custos_cel.BindingScope`
derived from the per-run output bag + the workflow context's
``current_utc_datetime`` clock (Dapr-replay-safe per WF-IMPL-006),
dispatches surviving nodes through :meth:`StepHandler.execute`, and
collects outputs.

Replay safety
-------------

The Dapr Workflow runtime re-drives an orchestrator function from the
top whenever a worker re-loads its state (pod restart, host migration,
etc.). The orchestrator body MUST produce an identical dispatch
sequence on every replay or the runtime aborts the instance with a
non-determinism error. Three pieces of input pin that contract:

* The compiled graph is supplied as a JSON envelope on
  :attr:`RunInput.compiled_graph_json` and re-hydrated through
  :func:`custos_workflow.graph.serialize.from_json` on every entry —
  the orchestrator NEVER consults the Catalog Service at run time
  (design.md § Pod Restart / Dapr Replay).
* Wall-clock time is sourced exclusively from
  :class:`custos_cel.DaprWorkflowClock` (which wraps
  :attr:`WorkflowContext.current_utc_datetime`) — a guarantee that
  carries through both the CEL evaluator and any handler that
  resolves a ``now()`` call.
* Iteration uses
  :attr:`~custos_workflow.graph.model.ExecutionGraph.topological_order`,
  a tuple of step ids the compiler emits in a deterministic order
  (WF-IMPL-019).

Step-result dispatch
--------------------

The orchestrator dispatches on the four :data:`StepResult` variants:

* :class:`StepSucceeded` — write outputs into the per-run bag, advance.
* :class:`StepSkipped` — write an empty entry, advance.
* :class:`StepWaiting` — return a partial :class:`RunOutput` with
  ``status="waiting"`` so the Run Controller can suspend the workflow;
  the Step Coordinator (Phase E) owns the resume path.
* :class:`StepFailed` — short-circuit and return
  :class:`RunOutput` ``(status="failed", failed_step=..., failure_envelope=...)``.

The module-level :data:`_STEP_RESULT_VARIANTS` assertion pins the
match arms below against the union: adding a fifth variant in
:mod:`custos_workflow.runs.step_handler` without extending the
dispatch sites here trips the assertion at import time.

Replay hook
-----------

The orchestrator fires an optional :data:`ReplayHook` callback
exactly once per orchestrator entry, BEFORE the first node is
dispatched. Phase E (WF-IMPL-046) plugs the ``ReplayReconciler``
into this hook so the Step Coordinator can re-register resume
subscriptions on Dapr replay (design.md § Resume Subscription Replay
Protocol).
"""

from __future__ import annotations

import copy
from collections.abc import Callable, Generator, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Final, cast, get_args

import custos_cel
from custos_cel import (
    BindingScope,
    DaprWorkflowClock,
    RunInfo,
    StepBinding,
    WorkflowInfo,
)
from custos_cel.clock import Clock

from custos_workflow._telemetry import observe_run_replay
from custos_workflow.graph.model import ExecutionGraph, ExecutionNode, StepKind
from custos_workflow.graph.serialize import from_json
from custos_workflow.runs.ids import RunId
from custos_workflow.runs.model import RunStatus
from custos_workflow.runs.step_handler import (
    _STEP_RESULT_VARIANTS,
    StepExecutionContext,
    StepFailed,
    StepHandler,
    StepResult,
    StepSkipped,
    StepSucceeded,
    StepWaiting,
    WorkflowContext,
)
from custos_workflow.runs.wait import WaitStepHandler

if TYPE_CHECKING:
    # ``ActivityStepHandler`` lives under ``custos_workflow.steps``,
    # which itself transitively imports ``runs.orchestrator`` via
    # ``runs.controller`` during package initialisation. Defer the
    # import to ``TYPE_CHECKING`` so the runtime hint resolves
    # without closing the cycle; the orchestrator never instantiates
    # an :class:`ActivityStepHandler` itself — callers inject a
    # constructed instance via the ``activity_handler`` kwarg — so
    # the runtime only ever duck-types ``iter_calls`` on the
    # supplied object.
    from custos_workflow.steps.activity_step import ActivityStepHandler

__all__ = [
    "WORKFLOW_NAME",
    "ReplayHook",
    "RunInput",
    "RunOutput",
    "make_run_orchestrator",
]


# Build-time exhaustiveness guard.
#
# Adding a fifth variant to :data:`StepResult` without extending
# :data:`_STEP_RESULT_VARIANTS` AND the dispatch in
# :func:`make_run_orchestrator` trips this assertion the moment the
# module is imported — i.e. before any orchestrator instance starts.
assert get_args(StepResult) == _STEP_RESULT_VARIANTS, (
    "StepResult union changed without updating _STEP_RESULT_VARIANTS;"
    " update run_orchestrator dispatch arms too."
)


#: Wire-stable Dapr workflow name. The Run Controller's
#: :meth:`WorkflowClient.schedule_new_workflow` call site
#: (WF-IMPL-037) passes this constant; production wiring registers
#: the function the factory below returns under this name.
WORKFLOW_NAME: Final[str] = "custos.workflow.run"


# ---------------------------------------------------------------------------
# Workflow inputs / outputs
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RunInput:
    """Inputs the ``run_orchestrator`` workflow consumes.

    All five fields are required-by-shape; ``inputs`` and
    ``idempotency_key`` carry sensible defaults so tests that don't
    care about either can construct a minimal instance.

    Attributes:
        workspace_id: The owning workspace.
        workflow_version_id: The Catalog Workflow Version id this
            run was started against (frozen at start time).
        compiled_graph_json: The byte-stable JSON envelope produced
            by :func:`custos_workflow.graph.serialize.to_json`.
            Re-hydrated on every orchestrator entry — the
            orchestrator never consults the Catalog Service post-start.
        inputs: User-supplied run inputs (e.g. the values keyed off
            ``inputs.*`` in CEL expressions).
        idempotency_key: Optional client-supplied de-duplication
            key; surfaced in audit envelopes downstream. Empty
            string when not supplied.
    """

    workspace_id: str
    workflow_version_id: str
    compiled_graph_json: str
    inputs: Mapping[str, Any] = field(default_factory=dict)
    idempotency_key: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Render to a JSON-serializable dict for the Dapr boundary."""

        return {
            "workspace_id": self.workspace_id,
            "workflow_version_id": self.workflow_version_id,
            "compiled_graph_json": self.compiled_graph_json,
            "inputs": dict(self.inputs),
            "idempotency_key": self.idempotency_key,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> RunInput:
        """Reconstruct from a JSON-decoded mapping."""

        return cls(
            workspace_id=str(payload["workspace_id"]),
            workflow_version_id=str(payload["workflow_version_id"]),
            compiled_graph_json=str(payload["compiled_graph_json"]),
            inputs=dict(payload.get("inputs") or {}),
            idempotency_key=str(payload.get("idempotency_key") or ""),
        )


@dataclass(frozen=True, slots=True)
class RunOutput:
    """Output of one ``run_orchestrator`` invocation.

    :attr:`status` is the wire-stable string from
    :class:`~custos_workflow.runs.model.RunStatus` (``"succeeded"``
    or ``"failed"``), plus the transitional ``"waiting"`` sentinel
    the orchestrator returns when a :class:`StepWaiting` arm fires
    (Step Coordinator owns the resume path; full Dapr suspension
    integration lands in WF-IMPL-036+).

    Attributes:
        status: ``"succeeded"`` | ``"failed"`` | ``"waiting"``.
        outputs: Per-step output bag, keyed by step id. Each value
            is the outputs mapping the corresponding step's
            :class:`StepSucceeded` reported (or an empty mapping
            for skipped / let / waiting steps).
        failed_step: The step id that returned :class:`StepFailed`
            and short-circuited the run. ``None`` unless
            ``status == "failed"``.
        failure_envelope: The canonical error envelope the failed
            step reported (``{"kind": ..., "message": ..., ...}``).
            ``None`` unless ``status == "failed"``.
        waiting_step: The step id that returned :class:`StepWaiting`
            and suspended the run. ``None`` unless
            ``status == "waiting"``.
        waiting_reason: A short, log-safe summary of the wait
            condition (e.g. ``"timer"``, ``"event:approval"``).
            ``None`` unless ``status == "waiting"``.
    """

    status: str
    outputs: Mapping[str, Mapping[str, Any]]
    failed_step: str | None = None
    failure_envelope: Mapping[str, Any] | None = None
    waiting_step: str | None = None
    waiting_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Render to a JSON-serializable dict for the Dapr boundary."""

        return {
            "status": self.status,
            "outputs": {sid: dict(out) for sid, out in self.outputs.items()},
            "failed_step": self.failed_step,
            "failure_envelope": (
                dict(self.failure_envelope) if self.failure_envelope is not None else None
            ),
            "waiting_step": self.waiting_step,
            "waiting_reason": self.waiting_reason,
        }


# ---------------------------------------------------------------------------
# Replay hook
# ---------------------------------------------------------------------------


#: Callable fired exactly once per orchestrator entry, BEFORE the
#: first step is dispatched. Phase E (WF-IMPL-046) plugs the
#: ``ReplayReconciler`` Protocol into this hook so the Step
#: Coordinator can re-register resume subscriptions on Dapr replay.
#: ``None`` (the default) skips the hook entirely.
ReplayHook = Callable[[StepExecutionContext, ExecutionGraph], None]


# ---------------------------------------------------------------------------
# Gate kinds
# ---------------------------------------------------------------------------


#: ``(slot_label, skip_if_truthy, skip_reason)`` for every guard
#: kind the orchestrator evaluates. ``skip_if_truthy`` flips the
#: comparison for ``unless:`` (skip when the expression is true)
#: relative to ``if:`` / ``when:`` (skip when false).
_GATE_SLOTS: Final[tuple[tuple[str, bool, str], ...]] = (
    ("if", False, "if=false"),
    ("when", False, "when=false"),
    ("unless", True, "unless=true"),
)


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def make_run_orchestrator(
    handler: StepHandler,
    *,
    on_replay: ReplayHook | None = None,
    expression_timeout_ms: int | None = None,
    wait_handler: WaitStepHandler | None = None,
    activity_handler: ActivityStepHandler | None = None,
) -> Callable[[WorkflowContext, Any], Generator[Any, Any, RunOutput]]:
    """Build the workflow function bound to a concrete :class:`StepHandler`.

    The returned callable matches the shape Dapr's Python SDK
    expects from a generator-style workflow function:
    ``Callable[[ctx, input], Generator[Any, Any, Any]]``. Tests
    register it on a :class:`~custos_workflow.runtime.FakeWorkflowRuntime`;
    production wiring (WF-IMPL-037) registers it on the real
    :class:`~custos_workflow.runtime.WorkflowRuntime` under
    :data:`WORKFLOW_NAME`.

    The orchestrator is a Python generator function: every
    invocation returns a generator, which the Dapr runtime drives
    forward by sending it the resolved value of each task token
    the body yields. Today the body only yields when a
    :attr:`~custos_workflow.graph.model.StepKind.WAIT` node opens
    a durable timer through :class:`WaitStepHandler` (WF-IMPL-036);
    every other path returns via :class:`StopIteration` immediately
    after the topological walk completes. The generator shape is
    fixed so future kinds the Run Controller adopts inline
    (``approval:``) can land without changing the dispatch surface.

    Args:
        handler: The :class:`StepHandler` every surviving non-wait
            node is dispatched through.
        on_replay: Optional callback fired exactly once per
            orchestrator entry, BEFORE the first dispatch. Phase E
            uses this for resume-subscription re-registration.
        expression_timeout_ms: Per-call CEL evaluation budget. Defaults
            to whatever :func:`custos_cel.evaluate` picks
            (``WF_EXPR_TIMEOUT_MS`` env var or
            :data:`custos_cel.DEFAULT_TIMEOUT_MS`).
        wait_handler: Optional :class:`WaitStepHandler` override.
            Tests inject a stub to assert dispatch routing; the
            default is a stateless module-level instance that opens
            a Dapr durable timer per :attr:`WaitStep.wait`.
        activity_handler: Optional
            :class:`~custos_workflow.steps.activity_step.ActivityStepHandler`
            override. When supplied, every
            :attr:`~custos_workflow.graph.model.StepKind.ACTIVITY`
            node is dispatched via ``yield from
            activity_handler.iter_calls(...)`` (the WF-IMPL-074
            yield protocol), bypassing the generic ``handler``
            (typically :class:`StepCoordinator`) for that kind so
            each ``bind_for_step`` /
            ``schedule_activity`` call is a separately-yielded
            :data:`~custos_workflow.runtime.dapr_activities.ActivityCallToken`
            the runtime resolves as a durable Dapr activity (the
            production resolver lands in WF-IMPL-079). When
            ``None`` (the default), ACTIVITY nodes flow through
            the legacy synchronous ``handler.execute`` adapter
            and bind / schedule happen inline within the
            orchestrator generator — fine for tests but not
            durable enough for production.

    Returns:
        The workflow function. Its ``__name__`` is :data:`WORKFLOW_NAME`,
        so :meth:`WorkflowRuntime.register_workflow` picks up the
        Dapr workflow name automatically.
    """
    wait_dispatcher: WaitStepHandler = (
        wait_handler if wait_handler is not None else WaitStepHandler()
    )

    def run_orchestrator(ctx: WorkflowContext, raw: Any) -> Generator[Any, Any, RunOutput]:
        run_input = raw if isinstance(raw, RunInput) else RunInput.from_dict(raw)
        graph = from_json(run_input.compiled_graph_json)
        clock = DaprWorkflowClock(ctx)

        # Per-run output bag. One entry per visited node; empty
        # mapping for skipped / waiting / let nodes that produced no
        # outputs. The CEL scope re-reads this on every gate
        # evaluation, so downstream ``steps.<id>.outputs.*``
        # references always resolve.
        output_bag: dict[str, dict[str, Any]] = {}

        run_info = RunInfo(id=ctx.instance_id, workspace=run_input.workspace_id)
        workflow_info = WorkflowInfo(
            name=graph.metadata.workflow_name,
            version=run_input.workflow_version_id,
        )

        # Phase E hook — fire once, BEFORE the first dispatch, even
        # when there is zero work to do (so a stale-state sweep can
        # run on an empty graph). Wrapped in the WF-IMPL-044
        # ``observe_run_replay`` span so the replay-path latency is
        # observable alongside the user-facing lifecycle operations
        # in the same ``custos_workflow_run_lifecycle_call_duration_ms``
        # histogram (operation label ``replay``). The reconciler
        # MUST NOT raise per its Protocol contract, so in practice
        # this wrapper only emits ``outcome=ok`` samples; a buggy
        # reconciler that does raise still surfaces on the
        # ``internal_error`` outcome via the shared error path.
        if on_replay is not None:
            with observe_run_replay():
                on_replay(_step_ctx(ctx, run_input, output_bag, clock), graph)

        nodes_by_id: dict[str, ExecutionNode] = {node.step_id: node for node in graph.nodes}

        for step_id in graph.topological_order:
            node = nodes_by_id[step_id]

            skip_reason = _evaluate_gates(
                node=node,
                run_input=run_input,
                run_info=run_info,
                workflow_info=workflow_info,
                clock=clock,
                output_bag=output_bag,
                timeout_ms=expression_timeout_ms,
            )
            if skip_reason is not None:
                # A gate excluded the step. Record an empty entry so
                # downstream references still resolve, then advance.
                output_bag[step_id] = {}
                continue

            if node.kind is StepKind.WAIT:
                # Run Controller owns ``wait:`` inline (design.md §
                # Workflow Schema: Step Kinds Handled — Wait /
                # sleep → Run Controller → Durable timer). The
                # WaitStepHandler is a generator that yields a Dapr
                # durable timer token; we re-yield it via
                # ``yield from`` so the runtime suspends the workflow
                # instance until the timer fires.
                wait_result = yield from wait_dispatcher.execute(ctx, node)
                output_bag[step_id] = copy.deepcopy(dict(wait_result.outputs))
                continue

            if node.kind is StepKind.ACTIVITY and activity_handler is not None:
                # WF-IMPL-074 yield protocol: drive the activity
                # handler's generator via ``yield from`` so every
                # ``bind_for_step`` / ``schedule_activity`` call
                # surfaces as its own yielded
                # :data:`~custos_workflow.runtime.dapr_activities.ActivityCallToken`,
                # which the runtime resolves as a durable Dapr
                # activity (production resolver: WF-IMPL-079;
                # test resolver:
                # :class:`~custos_workflow.runtime.dapr_activities.FakeDaprActivityDispatcher`
                # wired into :class:`FakeWorkflowRuntime`). The
                # generator's terminal ``return StepResult`` lands
                # as ``result`` here, so the existing exhaustive
                # dispatch arms below cover it without change.
                result = yield from activity_handler.iter_calls(
                    _step_ctx(ctx, run_input, output_bag, clock),
                    graph,
                    step_id,
                )
            else:
                result = handler.execute(
                    _step_ctx(ctx, run_input, output_bag, clock),
                    graph,
                    step_id,
                )

            # Exhaustive dispatch over StepResult. The module-level
            # ``get_args(StepResult) == _STEP_RESULT_VARIANTS``
            # assertion guarantees the four arms below cover the
            # whole union — a future fifth variant trips that guard
            # at import time so this match-block must be updated
            # before the new variant can ship.
            if isinstance(result, StepSucceeded):
                # Deep-copy at the ingest boundary so handlers that
                # retain references to the mappings/lists they emit
                # cannot mutate ``output_bag`` after the fact. This
                # is the replay-determinism guarantee for the Step
                # Coordinator boundary: every dispatch must observe
                # outputs as they were at the moment the producing
                # step completed.
                output_bag[step_id] = copy.deepcopy(dict(result.outputs))
                continue
            if isinstance(result, StepSkipped):
                output_bag[step_id] = {}
                continue
            if isinstance(result, StepWaiting):
                output_bag[step_id] = {}
                return RunOutput(
                    status="waiting",
                    outputs=_frozen_output_bag(output_bag),
                    waiting_step=step_id,
                    waiting_reason=result.reason,
                )
            if isinstance(result, StepFailed):
                return RunOutput(
                    status=RunStatus.FAILED.value,
                    outputs=_frozen_output_bag(output_bag),
                    failed_step=step_id,
                    failure_envelope=MappingProxyType(dict(result.envelope)),
                )
            # Unreachable per the module-level exhaustiveness guard,
            # but defensive in case the guard is bypassed (e.g. a
            # caller subclasses one of the variants).
            raise TypeError(  # pragma: no cover
                f"unknown StepResult variant: {type(result).__name__}"
            )

        return RunOutput(
            status=RunStatus.SUCCEEDED.value,
            outputs=_frozen_output_bag(output_bag),
        )

    run_orchestrator.__name__ = WORKFLOW_NAME
    # Surface the bound :class:`StepHandler` for introspection
    # (e.g. the WF-IMPL-057 lifespan-wiring test asserts the
    # registered orchestrator is bound to a :class:`StepCoordinator`,
    # not the default :class:`NoopStepHandler`). The attribute is a
    # one-liner read-only handle — the orchestrator function itself
    # still closes over ``handler`` so removing this attribute would
    # not change runtime behaviour.
    run_orchestrator.step_handler = handler  # type: ignore[attr-defined]
    return run_orchestrator


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _step_ctx(
    ctx: WorkflowContext,
    run_input: RunInput,
    output_bag: Mapping[str, Mapping[str, Any]],
    clock: Clock,
) -> StepExecutionContext:
    """Build a frozen :class:`StepExecutionContext` snapshot.

    A fresh snapshot is produced before every handler dispatch so
    the handler observes the outputs of every preceding step.

    The snapshot's outer surface is read-only: the top-level
    mapping and every per-step mapping are wrapped in
    :class:`MappingProxyType`, so handlers cannot rebind step ids
    or output keys through this view. Nested values are not deeply
    frozen, but they are deep-copies of what handlers originally
    returned (see the :class:`StepSucceeded` ingest path in
    ``run_orchestrator``), so any in-place mutation a handler
    performs on its own retained references — or on values it
    fishes out of ``ctx.outputs`` — cannot affect the orchestrator's
    ``output_bag`` or subsequent gate evaluation.
    """

    snapshot: dict[str, Mapping[str, Any]] = {
        sid: MappingProxyType(dict(out)) for sid, out in output_bag.items()
    }
    return StepExecutionContext(
        run_id=cast(RunId, ctx.instance_id),
        workspace_id=run_input.workspace_id,
        workflow_version_id=run_input.workflow_version_id,
        inputs=MappingProxyType(dict(run_input.inputs)),
        workflow_context=ctx,
        outputs=MappingProxyType(snapshot),
        clock=clock,
    )


def _evaluate_gates(
    *,
    node: ExecutionNode,
    run_input: RunInput,
    run_info: RunInfo,
    workflow_info: WorkflowInfo,
    clock: Clock,
    output_bag: Mapping[str, Mapping[str, Any]],
    timeout_ms: int | None,
) -> str | None:
    """Evaluate every ``if:`` / ``when:`` / ``unless:`` gate on ``node``.

    Returns the skip reason string (``"if=false"``,
    ``"when=false"``, or ``"unless=true"``) the moment any gate
    excludes the node; ``None`` when every gate passes (or when
    the node has no gates at all).
    """

    # Fast path: most nodes have no gates. Skip BindingScope
    # construction entirely in that case — it's the hot path.
    if not any(slot in node.call_sites for slot, _, _ in _GATE_SLOTS):
        return None

    scope = BindingScope(
        run=run_info,
        workflow=workflow_info,
        now=clock.now,
        inputs=run_input.inputs,
        steps={sid: StepBinding(out, sealed=True) for sid, out in output_bag.items()},
    )

    for slot, skip_if_truthy, reason in _GATE_SLOTS:
        call_site = node.call_sites.get(slot)
        if call_site is None:
            continue
        value = custos_cel.evaluate(
            call_site.typed_ast,
            scope,
            clock,
            timeout_ms=timeout_ms,
        )
        if bool(value) == skip_if_truthy:
            return reason
    return None


def _frozen_output_bag(
    output_bag: Mapping[str, Mapping[str, Any]],
) -> Mapping[str, Mapping[str, Any]]:
    """Wrap the orchestrator's mutable output bag for return."""

    return MappingProxyType({sid: MappingProxyType(dict(out)) for sid, out in output_bag.items()})
