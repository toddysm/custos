"""WF-IMPL-034 — :class:`StepHandler` Protocol + :class:`StepResult` union.

Covers the four acceptance criteria from #386:

1. Protocol is ``runtime_checkable`` and the
   :class:`NoopStepHandler` instance passes
   ``isinstance(h, StepHandler)``.
2. :class:`StepResult` variants are exhaustively enumerated in
   :data:`_STEP_RESULT_VARIANTS` — the test cross-checks the tuple
   against :data:`StepResult.__args__` so adding a fifth variant
   without updating the tuple fails the build.
3. Every variant is a frozen dataclass — attribute assignment
   raises :class:`dataclasses.FrozenInstanceError`.
4. :class:`NoopStepHandler` returns
   :class:`StepSucceeded` for :class:`StepKind.LET` and raises
   :class:`NotImplementedError` for every other kind; unknown
   ``step_id`` raises :class:`KeyError`.

Also pins :class:`WorkflowContext` Protocol conformance against
the existing :class:`~custos_workflow.runtime.FakeWorkflowContext`,
so the orchestrator (WF-IMPL-035) can drop the fake in without
ceremony.
"""

from __future__ import annotations

import dataclasses
from datetime import UTC, datetime
from typing import Any, get_args

import pytest
from custos_cel import FixedClock

from custos_workflow.document.models import ActivityStep, LetStep
from custos_workflow.graph.model import (
    ExecutionGraph,
    ExecutionNode,
    GraphMetadata,
    PrimitiveHandler,
    StepKind,
)
from custos_workflow.runs import (
    NoopStepHandler,
    RunId,
    StepExecutionContext,
    StepFailed,
    StepHandler,
    StepResult,
    StepSkipped,
    StepSucceeded,
    StepWaiting,
    WorkflowContext,
)
from custos_workflow.runs.step_handler import _STEP_RESULT_VARIANTS
from custos_workflow.runtime import FakeWorkflowContext

# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------


def _let_node(step_id: str) -> ExecutionNode:
    return ExecutionNode(
        step_id=step_id,
        kind=StepKind.LET,
        primitive_handler=PrimitiveHandler.EXPRESSION_INLINE,
        retry_policy=None,
        on_error_routes=(),
        call_sites={},
        step_source=LetStep.model_validate({"id": step_id, "let": {"v": "${{ true }}"}}),
    )


def _activity_node(step_id: str) -> ExecutionNode:
    return ExecutionNode(
        step_id=step_id,
        kind=StepKind.ACTIVITY,
        primitive_handler=PrimitiveHandler.ACTIVITY_RUNTIME,
        retry_policy=None,
        on_error_routes=(),
        call_sites={},
        step_source=ActivityStep.model_validate(
            {"id": step_id, "activity": "x/y@1", "connector": "primary"}
        ),
    )


def _graph(*nodes: ExecutionNode) -> ExecutionGraph:
    return ExecutionGraph(
        nodes=tuple(nodes),
        edges=(),
        topological_order=tuple(n.step_id for n in nodes),
        metadata=GraphMetadata(
            workflow_name="t",
            workflow_workspace="ws",
            document_api_version="custos.dev/v1",
        ),
    )


def _ctx() -> StepExecutionContext:
    return StepExecutionContext(
        run_id=RunId("run-1"),
        workspace_id="ws-1",
        workflow_context=FakeWorkflowContext(
            instance_id="run-1", now=datetime(2026, 1, 1, tzinfo=UTC)
        ),
        outputs={},
        clock=FixedClock(datetime(2026, 1, 1, tzinfo=UTC)),
    )


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


class TestProtocolConformance:
    def test_step_handler_is_runtime_checkable(self) -> None:
        assert isinstance(NoopStepHandler(), StepHandler)

    def test_workflow_context_protocol_is_satisfied_by_fake(self) -> None:
        ctx = FakeWorkflowContext(instance_id="x", now=datetime(2026, 1, 1, tzinfo=UTC))
        assert isinstance(ctx, WorkflowContext)

    def test_non_handler_object_does_not_pass_isinstance(self) -> None:
        class _NotAHandler:
            pass

        assert not isinstance(_NotAHandler(), StepHandler)


# ---------------------------------------------------------------------------
# StepResult union exhaustiveness + immutability
# ---------------------------------------------------------------------------


class TestStepResultUnion:
    def test_step_result_variants_tuple_matches_union_members(self) -> None:
        # ``get_args(StepResult)`` returns the union members in
        # declaration order. The exhaustive tuple must match
        # byte-for-byte; a new variant added to the union but
        # missing from the tuple fails this test.
        assert get_args(StepResult) == _STEP_RESULT_VARIANTS

    @pytest.mark.parametrize(
        ("variant", "kwargs"),
        [
            (StepSucceeded, {"outputs": {"x": 1}}),
            (StepFailed, {"envelope": {"kind": "e", "message": "boom"}}),
            (StepSkipped, {"reason": "if=false"}),
            (StepWaiting, {"reason": "timer"}),
        ],
    )
    def test_each_variant_is_frozen(self, variant: type, kwargs: dict[str, Any]) -> None:
        instance = variant(**kwargs)
        # Pick any attribute the dataclass declares and verify
        # assignment is rejected.
        (field_name, _) = next(iter(kwargs.items()))
        with pytest.raises(dataclasses.FrozenInstanceError):
            setattr(instance, field_name, "mutated")


# ---------------------------------------------------------------------------
# StepExecutionContext immutability
# ---------------------------------------------------------------------------


class TestStepExecutionContext:
    def test_context_is_frozen(self) -> None:
        ctx = _ctx()
        with pytest.raises(dataclasses.FrozenInstanceError):
            ctx.workspace_id = "other"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# NoopStepHandler behaviour
# ---------------------------------------------------------------------------


class TestNoopStepHandler:
    def test_let_kind_returns_succeeded_with_empty_outputs(self) -> None:
        handler = NoopStepHandler()
        graph = _graph(_let_node("a"))

        result = handler.execute(_ctx(), graph, "a")

        assert isinstance(result, StepSucceeded)
        assert dict(result.outputs) == {}

    def test_non_let_kind_raises_not_implemented(self) -> None:
        handler = NoopStepHandler()
        graph = _graph(_activity_node("a"))

        with pytest.raises(NotImplementedError, match=r"StepHandler\.execute"):
            handler.execute(_ctx(), graph, "a")

    def test_unknown_step_id_raises_key_error(self) -> None:
        handler = NoopStepHandler()
        graph = _graph(_let_node("a"))

        with pytest.raises(KeyError, match="zzz"):
            handler.execute(_ctx(), graph, "zzz")
