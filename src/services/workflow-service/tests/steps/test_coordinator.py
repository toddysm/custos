"""Tests for the ``StepCoordinator`` dispatcher (WF-IMPL-055)."""

from __future__ import annotations

from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any

import pytest
from custos_cel import FixedClock

from custos_workflow.document import ActivityStep, LetStep, WaitForStep, WaitStep, WorkflowStep
from custos_workflow.graph import (
    ExecutionGraph,
    ExecutionNode,
    GraphMetadata,
    PrimitiveHandler,
    StepKind,
)
from custos_workflow.runs import (
    RunId,
    StepExecutionContext,
    StepFailed,
    StepHandler,
    StepResult,
    StepSkipped,
    StepSucceeded,
)
from custos_workflow.runtime import FakeWorkflowContext
from custos_workflow.steps.activity_step import ActivityStepHandler
from custos_workflow.steps.coordinator import (
    _EXPECTED_PRIMITIVE_HANDLERS,
    StepCoordinator,
)
from custos_workflow.steps.errors import StepKindNotImplementedError
from custos_workflow.steps.let_step import LetStepHandler

# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------


_CLOCK_NOW = datetime(2026, 1, 1, tzinfo=UTC)
_CLOCK = FixedClock(_CLOCK_NOW)


def _activity_node(step_id: str = "scan") -> ExecutionNode:
    return ExecutionNode(
        step_id=step_id,
        kind=StepKind.ACTIVITY,
        primitive_handler=PrimitiveHandler.ACTIVITY_RUNTIME,
        retry_policy=None,
        on_error_routes=(),
        call_sites={},
        step_source=ActivityStep.model_validate(
            {"id": step_id, "activity": "scanners/trivy@1"},
        ),
    )


def _let_node(step_id: str = "derive") -> ExecutionNode:
    return ExecutionNode(
        step_id=step_id,
        kind=StepKind.LET,
        primitive_handler=PrimitiveHandler.EXPRESSION_INLINE,
        retry_policy=None,
        on_error_routes=(),
        call_sites={},
        step_source=LetStep.model_validate({"id": step_id, "let": {"x": 1}}),
    )


def _workflow_node(step_id: str = "child") -> ExecutionNode:
    return ExecutionNode(
        step_id=step_id,
        kind=StepKind.WORKFLOW,
        primitive_handler=PrimitiveHandler.SUB_ORCHESTRATION,
        retry_policy=None,
        on_error_routes=(),
        call_sites={},
        step_source=WorkflowStep.model_validate(
            {"id": step_id, "workflow": "ws/sub@1"},
        ),
    )


def _wait_node(step_id: str = "pause") -> ExecutionNode:
    return ExecutionNode(
        step_id=step_id,
        kind=StepKind.WAIT,
        primitive_handler=PrimitiveHandler.RUN_CONTROLLER_TIMER,
        retry_policy=None,
        on_error_routes=(),
        call_sites={},
        step_source=WaitStep.model_validate({"id": step_id, "wait": "PT5M"}),
    )


def _resume_node(step_id: str = "await-event") -> ExecutionNode:
    return ExecutionNode(
        step_id=step_id,
        kind=StepKind.WAIT_FOR,
        primitive_handler=PrimitiveHandler.RESUME_SUBSCRIPTION,
        retry_policy=None,
        on_error_routes=(),
        call_sites={},
        step_source=WaitForStep.model_validate(
            {"id": step_id, "waitFor": {"eventKey": "${{ inputs.key }}"}},
        ),
    )


def _graph(*nodes: ExecutionNode) -> ExecutionGraph:
    return ExecutionGraph(
        nodes=tuple(nodes),
        edges=(),
        topological_order=tuple(n.step_id for n in nodes),
        metadata=GraphMetadata(
            workflow_name="pipeline",
            workflow_workspace="ws",
            document_api_version="custos.dev/v1",
        ),
    )


def _ctx(run_id: str = "run-1") -> StepExecutionContext:
    return StepExecutionContext(
        run_id=RunId(run_id),
        workspace_id="ws-1",
        workflow_version_id="wf-1",
        inputs=MappingProxyType({}),
        workflow_context=FakeWorkflowContext(instance_id=run_id, now=_CLOCK_NOW),
        outputs=MappingProxyType({}),
        clock=_CLOCK,
    )


class _RecordingLetHandler:
    """``LetStepHandler`` stand-in that records every dispatch."""

    def __init__(self, result: StepResult | None = None) -> None:
        self.calls: list[tuple[StepExecutionContext, ExecutionGraph, str]] = []
        self._result: StepResult = (
            result
            if result is not None
            else StepSucceeded(
                outputs=MappingProxyType({"let": "ran"}),
            )
        )

    def execute(
        self,
        ctx: StepExecutionContext,
        graph: ExecutionGraph,
        step_id: str,
    ) -> StepResult:
        self.calls.append((ctx, graph, step_id))
        return self._result


class _RecordingActivityHandler:
    """``ActivityStepHandler`` stand-in that records every dispatch.

    Structurally compatible with :class:`ActivityStepHandler` for
    the surface :class:`StepCoordinator` calls (``execute`` only).
    """

    def __init__(self, result: StepResult | None = None) -> None:
        self.calls: list[tuple[StepExecutionContext, ExecutionGraph, str]] = []
        self._result: StepResult = (
            result
            if result is not None
            else StepSucceeded(
                outputs=MappingProxyType({"activity": "ran"}),
            )
        )

    def execute(
        self,
        ctx: StepExecutionContext,
        graph: ExecutionGraph,
        step_id: str,
    ) -> StepResult:
        self.calls.append((ctx, graph, step_id))
        return self._result


# ---------------------------------------------------------------------------
# StepHandler Protocol conformance
# ---------------------------------------------------------------------------


class TestProtocolConformance:
    def test_step_coordinator_satisfies_step_handler_protocol(self) -> None:
        coord = StepCoordinator(_RecordingActivityHandler())
        assert isinstance(coord, StepHandler)


# ---------------------------------------------------------------------------
# Dispatch arms
# ---------------------------------------------------------------------------


class TestDispatchExpressionInline:
    def test_let_step_dispatches_to_let_handler(self) -> None:
        let = _RecordingLetHandler()
        activity = _RecordingActivityHandler()
        coord = StepCoordinator(activity, let_handler=let)
        graph = _graph(_let_node())
        ctx = _ctx()

        result = coord.execute(ctx, graph, "derive")

        assert isinstance(result, StepSucceeded)
        assert dict(result.outputs) == {"let": "ran"}
        assert len(let.calls) == 1
        # ActivityHandler was NOT called.
        assert activity.calls == []
        # The same ctx / graph / step_id are forwarded verbatim.
        (forwarded_ctx, forwarded_graph, forwarded_step_id) = let.calls[0]
        assert forwarded_ctx is ctx
        assert forwarded_graph is graph
        assert forwarded_step_id == "derive"

    def test_let_handler_step_failed_passes_through(self) -> None:
        # The dispatcher must NOT post-process the sub-handler's
        # result — it just hands it back to the orchestrator.
        envelope = MappingProxyType({"kind": "step.with_input_resolution_error"})
        let = _RecordingLetHandler(result=StepFailed(envelope=envelope))
        coord = StepCoordinator(_RecordingActivityHandler(), let_handler=let)

        result = coord.execute(_ctx(), _graph(_let_node()), "derive")

        assert isinstance(result, StepFailed)
        assert dict(result.envelope) == {"kind": "step.with_input_resolution_error"}


class TestDispatchActivityRuntime:
    def test_activity_step_dispatches_to_activity_handler(self) -> None:
        activity = _RecordingActivityHandler()
        let = _RecordingLetHandler()
        coord = StepCoordinator(activity, let_handler=let)
        graph = _graph(_activity_node())
        ctx = _ctx()

        result = coord.execute(ctx, graph, "scan")

        assert isinstance(result, StepSucceeded)
        assert dict(result.outputs) == {"activity": "ran"}
        assert len(activity.calls) == 1
        # LetHandler was NOT called.
        assert let.calls == []

    def test_activity_handler_step_skipped_passes_through(self) -> None:
        activity = _RecordingActivityHandler(result=StepSkipped(reason="skipped"))
        coord = StepCoordinator(activity)

        result = coord.execute(_ctx(), _graph(_activity_node()), "scan")

        assert isinstance(result, StepSkipped)
        assert result.reason == "skipped"


class TestDispatchSubOrchestration:
    def test_sub_orchestration_step_raises_step_kind_not_implemented_error(
        self,
    ) -> None:
        """``forEach`` / ``workflow:`` / ``approval:`` are dispatched inline.

        The Run Controller orchestrator drives SUB_ORCHESTRATION nodes
        through the Sub-Orchestration Manager (WF-IMPL-093), so reaching
        the Step Coordinator dispatcher with one is a compile-time
        routing bug. The dispatcher raises (instead of returning a
        :class:`StepFailed` envelope) to surface it loudly, mirroring the
        ``run_controller_timer`` arm. The exception subclasses
        :class:`NotImplementedError`.
        """
        coord = StepCoordinator(_RecordingActivityHandler())
        graph = _graph(_workflow_node("child"))

        with pytest.raises(StepKindNotImplementedError) as excinfo:
            coord.execute(_ctx("run-A"), graph, "child")

        err = excinfo.value
        assert err.KIND == "step.kind_not_implemented"
        assert err.step_id == "child"
        assert err.run_id == "run-A"
        assert err.step_kind == "workflow"
        assert err.primitive_handler == "sub_orchestration"
        assert "Sub-Orchestration Manager" in err.message
        # Subclass of NotImplementedError (taxonomy guarantee).
        assert isinstance(err, NotImplementedError)


class TestDispatchRunControllerTimer:
    def test_wait_step_raises_step_kind_not_implemented_error(self) -> None:
        """``wait:`` belongs to the Run Controller, not the dispatcher.

        Reaching the dispatcher with one is a compile-time bug, so
        we raise (instead of returning :class:`StepFailed`) to
        surface it loudly. The exception subclasses
        :class:`NotImplementedError`, so existing ``except
        NotImplementedError:`` callers still catch it.
        """
        coord = StepCoordinator(_RecordingActivityHandler())
        graph = _graph(_wait_node("pause"))

        with pytest.raises(StepKindNotImplementedError) as excinfo:
            coord.execute(_ctx("run-B"), graph, "pause")

        err = excinfo.value
        assert err.KIND == "step.kind_not_implemented"
        assert err.step_id == "pause"
        assert err.run_id == "run-B"
        assert err.step_kind == "wait"
        assert err.primitive_handler == "run_controller_timer"
        # Subclass of NotImplementedError (taxonomy guarantee).
        assert isinstance(err, NotImplementedError)


class TestDispatchResumeSubscription:
    def test_wait_for_step_raises_step_kind_not_implemented_error(self) -> None:
        """``waitFor:`` is dispatched inline by the Run Controller.

        The Resume Subscription Manager drives RESUME_SUBSCRIPTION
        nodes through the Run Controller orchestrator (REQ-081), so
        reaching the Step Coordinator dispatcher with one is a
        compile-time routing bug. The dispatcher raises (instead of
        returning a :class:`StepFailed` envelope) to surface it
        loudly, mirroring the ``run_controller_timer`` arm. The
        exception subclasses :class:`NotImplementedError`.
        """
        coord = StepCoordinator(_RecordingActivityHandler())
        graph = _graph(_resume_node("await-event"))

        with pytest.raises(StepKindNotImplementedError) as excinfo:
            coord.execute(_ctx("run-C"), graph, "await-event")

        err = excinfo.value
        assert err.KIND == "step.kind_not_implemented"
        assert err.step_id == "await-event"
        assert err.run_id == "run-C"
        assert err.step_kind == "wait_for"
        assert err.primitive_handler == "resume_subscription"
        assert "Resume Subscription Manager" in err.message
        # Subclass of NotImplementedError (taxonomy guarantee).
        assert isinstance(err, NotImplementedError)


# ---------------------------------------------------------------------------
# Defensive guards
# ---------------------------------------------------------------------------


class TestDefensiveGuards:
    def test_unknown_step_id_raises_key_error(self) -> None:
        coord = StepCoordinator(_RecordingActivityHandler())
        graph = _graph(_activity_node("scan"))

        with pytest.raises(KeyError):
            coord.execute(_ctx(), graph, "missing")

    def test_default_let_handler_is_let_step_handler(self) -> None:
        """The default ``let_handler`` is a fresh ``LetStepHandler``."""
        coord = StepCoordinator(_RecordingActivityHandler())
        # Verify the default sub-handler is a LetStepHandler — read
        # the private slot to confirm wiring without exposing it.
        assert isinstance(coord._let_handler, LetStepHandler)

    def test_explicit_let_handler_overrides_default(self) -> None:
        custom = LetStepHandler()
        coord = StepCoordinator(_RecordingActivityHandler(), let_handler=custom)
        assert coord._let_handler is custom

    def test_real_activity_handler_type_round_trip(self) -> None:
        """``StepCoordinator`` accepts a real ``ActivityStepHandler``.

        Construction-only check: confirms the static type signature
        of the constructor accepts the real handler (no
        ``# type: ignore`` needed here).
        """

        class _Stub:
            def schedule_activity(self, request: Any) -> Any:  # pragma: no cover
                raise NotImplementedError

            def cancel_activity(self, run_id: str, step_id: str) -> None:  # pragma: no cover
                raise NotImplementedError

        class _StubConn:
            def bind_for_step(self, request: Any) -> Any:  # pragma: no cover
                raise NotImplementedError

        real = ActivityStepHandler(_Stub(), _StubConn())
        coord = StepCoordinator(real)
        assert isinstance(coord, StepHandler)


# ---------------------------------------------------------------------------
# Exhaustiveness guard (mirrors WF-IMPL-035 _STEP_RESULT_VARIANTS pattern)
# ---------------------------------------------------------------------------


class TestExhaustivenessGuard:
    def test_expected_primitive_handlers_covers_every_enum_member(self) -> None:
        """Adding a ``PrimitiveHandler`` member without extending the
        dispatcher's expected-set MUST fail this test.

        Re-derives the set independently from ``PrimitiveHandler``
        so the dispatcher's module-level :keyword:`assert` is
        exercised by the test suite (and the failure surfaces here
        first if the assert is ever weakened).
        """
        assert set(PrimitiveHandler) == _EXPECTED_PRIMITIVE_HANDLERS

    def test_every_primitive_handler_member_is_dispatched(self) -> None:
        """Sanity: the five PrimitiveHandler members are all the ones
        the dispatcher's table handles.

        If a new member is added (e.g. ``PARALLEL_FAN_OUT``), this
        test fails on the assert above AND the dispatcher's
        module-level assert fails at import time. Either failure
        directs the maintainer to extend the dispatch table.
        """
        members = {member.value for member in PrimitiveHandler}
        assert members == {
            "activity_runtime",
            "expression_inline",
            "sub_orchestration",
            "run_controller_timer",
            "resume_subscription",
        }
