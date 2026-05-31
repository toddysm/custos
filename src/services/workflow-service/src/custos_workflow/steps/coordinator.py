"""``StepCoordinator`` — concrete :class:`StepHandler` dispatcher (WF-IMPL-055).

The Step Coordinator is the public dispatch surface the Run
Controller orchestrator (WF-IMPL-035) drives every step through.
It routes execution strictly by the compiled
:class:`~custos_workflow.graph.model.PrimitiveHandler` tag on each
:class:`~custos_workflow.graph.model.ExecutionNode` — the
resolution happens at compile time, so every replay reaches the
same handler for the same node.

Dispatch table (mirrors implementation-plan.md § WF-IMPL-055):

+----------------------------------------------+-----------------------------+
| :class:`PrimitiveHandler`                    | Routed to                   |
+==============================================+=============================+
| ``EXPRESSION_INLINE`` (``let:``)             | :class:`LetStepHandler`     |
+----------------------------------------------+-----------------------------+
| ``ACTIVITY_RUNTIME`` (``activity:``)         | :class:`ActivityStepHandler`|
+----------------------------------------------+-----------------------------+
| ``RUN_CONTROLLER_TIMER`` (``wait:``)         | Defensive raise — the Run   |
|                                              | Controller orchestrator     |
|                                              | dispatches ``wait:`` nodes  |
|                                              | inline via                  |
|                                              | ``ctx.create_timer``; the   |
|                                              | dispatcher should never see |
|                                              | one.                        |
+----------------------------------------------+-----------------------------+
| ``SUB_ORCHESTRATION``                        | :class:`StepFailed` carrying|
| (``for:`` / ``approval:`` / ``workflow:``)   | a                           |
|                                              | ``step.kind_not_implemented``|
|                                              | envelope. The Sub-          |
|                                              | Orchestration Manager sub-  |
|                                              | module owns the real        |
|                                              | implementation.             |
+----------------------------------------------+-----------------------------+

Exhaustiveness guard
====================

A module-level assertion compares
:data:`_EXPECTED_PRIMITIVE_HANDLERS` against
``set(PrimitiveHandler)`` so adding a new tag without extending
the dispatch table fails the build (mirroring WF-IMPL-035's
``_STEP_RESULT_VARIANTS`` pattern). A companion unit test in
:mod:`tests.steps.test_coordinator` re-derives the set
independently so the guard is exercised on every run.

Lifecycle events
================

This module emits **no** ``step.*`` lifecycle events.
:class:`StepCoordinator` is a pure dispatcher — event emission
(``step.started`` / ``step.completed`` / etc.) is owned by
WF-IMPL-056, which wraps this dispatcher with a publisher.
Keeping the surfaces separate lets WF-IMPL-056 land without
re-opening the dispatch table.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import TYPE_CHECKING, Final

from custos_workflow.graph.model import PrimitiveHandler
from custos_workflow.runs.step_handler import (
    StepExecutionContext,
    StepFailed,
    StepHandler,
    StepResult,
)
from custos_workflow.steps.errors import LOCKED_STEP_KINDS, StepKindNotImplementedError
from custos_workflow.steps.let_step import LetStepHandler

# NOTE: ``custos_workflow._telemetry`` is imported lazily inside
# :meth:`StepCoordinator.execute` rather than at module level.
# ``_telemetry`` pulls in ``custos_workflow.runs.errors`` which
# transitively triggers ``custos_workflow.runs.__init__`` — and
# this module is itself loaded during ``custos_workflow.steps``
# package initialisation (via
# ``clients.activity_runtime → steps.idempotency``). Eager loading
# would close the cycle. A function-scope import keeps the
# dispatcher import surface flat.

if TYPE_CHECKING:
    from custos_workflow.graph.model import ExecutionGraph


__all__ = ["StepCoordinator"]


# ---------------------------------------------------------------------------
# Build-time exhaustiveness guard
# ---------------------------------------------------------------------------


#: Every :class:`PrimitiveHandler` member the dispatch table below
#: must handle. The module-level :keyword:`assert` immediately
#: after this constant pins the equality with
#: ``set(PrimitiveHandler)`` so the build fails if a new tag is
#: added without extending the dispatcher.
_EXPECTED_PRIMITIVE_HANDLERS: Final[frozenset[PrimitiveHandler]] = frozenset(
    {
        PrimitiveHandler.EXPRESSION_INLINE,
        PrimitiveHandler.ACTIVITY_RUNTIME,
        PrimitiveHandler.SUB_ORCHESTRATION,
        PrimitiveHandler.RUN_CONTROLLER_TIMER,
    }
)
assert set(PrimitiveHandler) == _EXPECTED_PRIMITIVE_HANDLERS, (
    "PrimitiveHandler enum changed — extend StepCoordinator dispatch "
    "in custos_workflow.steps.coordinator and the unit-test guard "
    "in tests/steps/test_coordinator.py."
)


# ---------------------------------------------------------------------------
# StepCoordinator
# ---------------------------------------------------------------------------


class StepCoordinator:
    """Concrete :class:`StepHandler` that routes by ``primitive_handler``.

    Stateless aside from the per-kind sub-handlers it composes. A
    single instance is safe to share across runs.

    Constructor:

    :param activity_handler: The :class:`StepHandler` instance used
        for every ``ACTIVITY_RUNTIME`` node. Conventionally a
        :class:`~custos_workflow.steps.activity_step.ActivityStepHandler`
        but the dispatcher only uses the :class:`StepHandler`
        surface, so any conforming implementation works (notably:
        tests can substitute a recording fake without needing to
        construct real activity / connector clients).
    :param let_handler: The :class:`StepHandler` instance used for
        every ``EXPRESSION_INLINE`` node. Defaults to a fresh
        :class:`LetStepHandler` — that handler is stateless so
        the default is correct for production callers who do not
        want to construct one explicitly.
    """

    __slots__ = ("_activity_handler", "_let_handler")

    def __init__(
        self,
        activity_handler: StepHandler,
        let_handler: StepHandler | None = None,
    ) -> None:
        self._activity_handler: Final[StepHandler] = activity_handler
        self._let_handler: Final[StepHandler] = (
            let_handler if let_handler is not None else LetStepHandler()
        )

    def execute(
        self,
        ctx: StepExecutionContext,
        graph: ExecutionGraph,
        step_id: str,
    ) -> StepResult:
        """Dispatch ``step_id`` to the handler resolved at compile time.

        :raises KeyError: ``step_id`` is not present in
            ``graph.nodes``.
        :raises StepKindNotImplementedError: The node's
            ``primitive_handler`` is
            :attr:`PrimitiveHandler.RUN_CONTROLLER_TIMER`. That
            kind belongs to the Run Controller orchestrator —
            reaching the dispatcher with one is a compile-time
            bug, so we raise loudly rather than emit a
            :class:`StepFailed`.
        """
        node = next((n for n in graph.nodes if n.step_id == step_id), None)
        if node is None:
            raise KeyError(step_id)

        # Lazy import — see module-level NOTE on the
        # ``_telemetry`` ⇄ ``runs`` ⇄ ``steps`` circular import.
        from custos_workflow._telemetry import (
            observe_step_execute,
            record_step_error,
        )

        primitive = node.primitive_handler

        # WF-IMPL-058: wrap the dispatch in
        # ``custos_workflow.step.execute`` so every primitive (let,
        # activity, sub-orchestration stub) records exactly one
        # span + one ``custos_workflow_step_execute_duration_ms``
        # sample. The wrapper observes raised exceptions only; the
        # ``StepFailed``-envelope path (which is *returned*, not
        # raised, by the activity / sub-orchestration handlers)
        # bumps :data:`STEP_ERRORS_TOTAL` explicitly below so the
        # error counter stays in sync with the StepFailed wire
        # surface.
        with observe_step_execute(node.kind.value):
            if primitive is PrimitiveHandler.EXPRESSION_INLINE:
                result = self._let_handler.execute(ctx, graph, step_id)
            elif primitive is PrimitiveHandler.ACTIVITY_RUNTIME:
                result = self._activity_handler.execute(ctx, graph, step_id)
            elif primitive is PrimitiveHandler.SUB_ORCHESTRATION:
                err = StepKindNotImplementedError(
                    f"step {step_id!r} kind={node.kind.value!r} "
                    f"primitive_handler={primitive.value!r} is deferred "
                    "to the Sub-Orchestration Manager sub-module",
                    run_id=str(ctx.run_id),
                    step_id=step_id,
                    step_kind=node.kind.value,
                    primitive_handler=primitive.value,
                )
                result = StepFailed(envelope=MappingProxyType(err.to_dict()))
            else:
                # PrimitiveHandler.RUN_CONTROLLER_TIMER — wait: nodes are
                # dispatched inline by the Run Controller orchestrator via
                # ``ctx.create_timer`` (see WF-IMPL-035 / the Resume
                # Subscription Manager sub-module for the durable-timer
                # path). The dispatcher should never see one; if it does,
                # there's a compile-time mis-tag and we want to fail
                # loudly. Raising propagates through
                # ``observe_step_execute`` so the histogram sample
                # carries ``outcome=kind_not_implemented`` and the
                # ``custos_workflow_step_errors_total`` counter is
                # bumped via the standard error path below.
                raise StepKindNotImplementedError(
                    f"step {step_id!r} kind={node.kind.value!r} "
                    f"primitive_handler={primitive.value!r} is handled inline "
                    "by the Run Controller orchestrator, not by the Step "
                    "Coordinator dispatcher",
                    run_id=str(ctx.run_id),
                    step_id=step_id,
                    step_kind=node.kind.value,
                    primitive_handler=primitive.value,
                )

        # ``StepFailed``-envelope path bumps the error counter. The
        # envelope ``kind`` is the structured error string from the
        # locked LOCKED_STEP_KINDS taxonomy; anything outside that
        # set is ignored (defensive — a sub-handler returning a
        # malformed envelope should never silently pollute the
        # error histogram with an unknown kind).
        if isinstance(result, StepFailed):
            envelope_kind = result.envelope.get("kind")
            if isinstance(envelope_kind, str) and envelope_kind in LOCKED_STEP_KINDS:
                record_step_error(envelope_kind)
        return result
