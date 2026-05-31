"""Tests for the ``LetStepHandler`` (WF-IMPL-052)."""

from __future__ import annotations

from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any
from unittest.mock import patch

import pytest
from custos_cel import (
    FixedClock,
    IntType,
    SchemaBindings,
    parse,
    type_check,
)
from custos_cel.errors import (
    EvaluationError,
    UnboundNameError,
)
from custos_cel.errors import (
    TypeError as CelTypeError,
)

from custos_workflow.document import LetStep
from custos_workflow.graph import (
    CallSiteKind,
    ExecutionGraph,
    ExecutionNode,
    GraphMetadata,
    PrimitiveHandler,
    StepKind,
    TypedCallSite,
)
from custos_workflow.runs import (
    RunId,
    StepExecutionContext,
    StepFailed,
    StepSucceeded,
)
from custos_workflow.runtime import FakeWorkflowContext
from custos_workflow.steps import LetStepHandler

# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------

_CLOCK_NOW = datetime(2026, 1, 1, tzinfo=UTC)
_CLOCK = FixedClock(_CLOCK_NOW)


def _typed(source: str, *, let: dict[str, Any] | None = None) -> Any:
    ast = parse(source)
    bindings = (
        SchemaBindings(inputs={"type": "object", "properties": {}, "required": []})
        if let is None
        else SchemaBindings(
            inputs={"type": "object", "properties": {}, "required": []},
            let=let,
        )
    )
    return type_check(ast, bindings)


def _cs(
    source_cel: str,
    *,
    binding_name: str,
    let_types: dict[str, Any] | None = None,
) -> TypedCallSite:
    return TypedCallSite(
        source=f"${{{{ {source_cel} }}}}",
        typed_ast=_typed(source_cel, let=let_types),
        kind=CallSiteKind.LET,
        document_path=f"spec.steps[0].let.{binding_name}",
    )


def _let_node(
    *,
    step_id: str = "derive",
    let_block: dict[str, Any],
    call_sites: dict[str, TypedCallSite] | None = None,
) -> ExecutionNode:
    return ExecutionNode(
        step_id=step_id,
        kind=StepKind.LET,
        primitive_handler=PrimitiveHandler.EXPRESSION_INLINE,
        retry_policy=None,
        on_error_routes=(),
        call_sites=call_sites or {},
        step_source=LetStep.model_validate({"id": step_id, "let": let_block}),
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


def _ctx(
    *,
    outputs: dict[str, dict[str, Any]] | None = None,
    inputs: dict[str, Any] | None = None,
    run_id: str = "run-1",
    workspace_id: str = "ws-1",
    workflow_version_id: str = "wf-version-1",
) -> StepExecutionContext:
    return StepExecutionContext(
        run_id=RunId(run_id),
        workspace_id=workspace_id,
        workflow_version_id=workflow_version_id,
        inputs=MappingProxyType(dict(inputs or {})),
        workflow_context=FakeWorkflowContext(instance_id=run_id, now=_CLOCK_NOW),
        outputs=MappingProxyType(
            {sid: MappingProxyType(dict(out)) for sid, out in (outputs or {}).items()}
        ),
        clock=_CLOCK,
    )


# ---------------------------------------------------------------------------
# Happy paths
# ---------------------------------------------------------------------------


class TestLetStepHandlerHappyPath:
    def test_single_string_binding_evaluates_cel_expression(self) -> None:
        node = _let_node(
            let_block={"sum": "${{ 1 + 2 }}"},
            call_sites={"let.sum": _cs("1 + 2", binding_name="sum")},
        )
        graph = _graph(node)

        result = LetStepHandler().execute(_ctx(), graph, "derive")

        assert isinstance(result, StepSucceeded)
        assert dict(result.outputs) == {"sum": 3}

    def test_non_string_value_passes_through_unchanged(self) -> None:
        node = _let_node(
            let_block={
                "count": 42,
                "flag": True,
                "labels": ["a", "b"],
                "config": {"k": "v"},
                "nothing": None,
            },
        )
        graph = _graph(node)

        result = LetStepHandler().execute(_ctx(), graph, "derive")

        assert isinstance(result, StepSucceeded)
        assert dict(result.outputs) == {
            "count": 42,
            "flag": True,
            "labels": ["a", "b"],
            "config": {"k": "v"},
            "nothing": None,
        }

    def test_literal_string_with_no_placeholder_passes_through(self) -> None:
        node = _let_node(
            let_block={"label": "critical"},
        )
        graph = _graph(node)

        result = LetStepHandler().execute(_ctx(), graph, "derive")

        assert isinstance(result, StepSucceeded)
        assert dict(result.outputs) == {"label": "critical"}

    def test_multi_binding_cross_reference_reads_from_let_overlay(self) -> None:
        # ``let.b`` references ``let.a`` from the same step's overlay,
        # NOT from ``steps.derive.outputs.a`` — the step hasn't
        # completed yet at evaluation time.
        node = _let_node(
            let_block={
                "a": "${{ 1 + 1 }}",
                "b": "${{ let.a * 10 }}",
            },
            call_sites={
                "let.a": _cs("1 + 1", binding_name="a"),
                "let.b": _cs("let.a * 10", binding_name="b", let_types={"a": IntType()}),
            },
        )
        graph = _graph(node)

        result = LetStepHandler().execute(_ctx(), graph, "derive")

        assert isinstance(result, StepSucceeded)
        assert dict(result.outputs) == {"a": 2, "b": 20}

    def test_binding_can_reference_prior_step_outputs(self) -> None:
        node = _let_node(
            let_block={"echo": "${{ steps.upstream.outputs.value + 1 }}"},
            call_sites={
                "let.echo": TypedCallSite(
                    source="${{ steps.upstream.outputs.value + 1 }}",
                    typed_ast=type_check(
                        parse("steps.upstream.outputs.value + 1"),
                        SchemaBindings(
                            inputs={"type": "object", "properties": {}, "required": []},
                            prior_steps=(
                                (
                                    "upstream",
                                    {
                                        "type": "object",
                                        "properties": {"value": {"type": "integer"}},
                                        "required": ["value"],
                                    },
                                ),
                            ),
                        ),
                    ),
                    kind=CallSiteKind.LET,
                    document_path="spec.steps[1].let.echo",
                )
            },
        )
        graph = _graph(node)

        result = LetStepHandler().execute(
            _ctx(outputs={"upstream": {"value": 41}}),
            graph,
            "derive",
        )

        assert isinstance(result, StepSucceeded)
        assert dict(result.outputs) == {"echo": 42}

    def test_binding_can_reference_run_inputs(self) -> None:
        # WF-IMPL-052: ``StepExecutionContext.inputs`` was widened so
        # ``let:`` expressions observe the same ``inputs.*`` namespace
        # as the orchestrator's gate evaluator. This regression test
        # pins that ``${{ inputs.x }}`` resolves end-to-end.
        node = _let_node(
            let_block={"echo": "${{ inputs.threshold + 1 }}"},
            call_sites={
                "let.echo": TypedCallSite(
                    source="${{ inputs.threshold + 1 }}",
                    typed_ast=type_check(
                        parse("inputs.threshold + 1"),
                        SchemaBindings(
                            inputs={
                                "type": "object",
                                "properties": {"threshold": {"type": "integer"}},
                                "required": ["threshold"],
                            }
                        ),
                    ),
                    kind=CallSiteKind.LET,
                    document_path="spec.steps[0].let.echo",
                )
            },
        )
        graph = _graph(node)

        result = LetStepHandler().execute(
            _ctx(inputs={"threshold": 5}),
            graph,
            "derive",
        )

        assert isinstance(result, StepSucceeded)
        assert dict(result.outputs) == {"echo": 6}

    def test_workflow_version_matches_step_execution_context(self) -> None:
        # WF-IMPL-052 consistency fix: ``workflow.version`` MUST
        # resolve to ``ctx.workflow_version_id`` (the same string the
        # orchestrator's gate evaluator uses via
        # ``run_input.workflow_version_id``) — NOT to the graph
        # metadata's ``document_api_version``. This regression test
        # pins that contract.
        node = _let_node(
            let_block={"v": "${{ workflow.version }}"},
            call_sites={
                "let.v": TypedCallSite(
                    source="${{ workflow.version }}",
                    typed_ast=type_check(
                        parse("workflow.version"),
                        SchemaBindings(inputs={"type": "object", "properties": {}, "required": []}),
                    ),
                    kind=CallSiteKind.LET,
                    document_path="spec.steps[0].let.v",
                )
            },
        )
        graph = _graph(node)

        result = LetStepHandler().execute(
            _ctx(workflow_version_id="wf-version-uuid-7"),
            graph,
            "derive",
        )

        assert isinstance(result, StepSucceeded)
        assert dict(result.outputs) == {"v": "wf-version-uuid-7"}

    def test_outputs_mapping_is_immutable(self) -> None:
        node = _let_node(
            let_block={"v": "${{ 1 + 1 }}"},
            call_sites={"let.v": _cs("1 + 1", binding_name="v")},
        )
        graph = _graph(node)

        result = LetStepHandler().execute(_ctx(), graph, "derive")

        assert isinstance(result, StepSucceeded)
        assert isinstance(result.outputs, MappingProxyType)
        with pytest.raises(TypeError):
            result.outputs["mutated"] = 1  # type: ignore[index]

    def test_replay_determinism_byte_equal_under_fixed_clock(self) -> None:
        # Same graph, same context, same FixedClock — two calls must
        # produce byte-equal outputs. This is the Dapr Workflow
        # replay-determinism guarantee.
        node = _let_node(
            let_block={
                "a": "${{ 1 + 1 }}",
                "b": "${{ let.a + 3 }}",
            },
            call_sites={
                "let.a": _cs("1 + 1", binding_name="a"),
                "let.b": _cs("let.a + 3", binding_name="b", let_types={"a": IntType()}),
            },
        )
        graph = _graph(node)

        first = LetStepHandler().execute(_ctx(), graph, "derive")
        second = LetStepHandler().execute(_ctx(), graph, "derive")

        assert isinstance(first, StepSucceeded)
        assert isinstance(second, StepSucceeded)
        assert dict(first.outputs) == dict(second.outputs)


# ---------------------------------------------------------------------------
# Error wrapping
# ---------------------------------------------------------------------------


class TestLetStepHandlerErrorWrapping:
    def test_unbound_name_returns_step_failed_envelope(self) -> None:
        # Trigger a real CEL ``expression.unbound_name`` error at
        # evaluation time by referencing an ``inputs.*`` field that
        # the test context's ``inputs`` snapshot does not carry. The
        # call site type-checks fine against the declared schema,
        # but the runtime BindingScope (built from
        # :attr:`StepExecutionContext.inputs`) is empty, so
        # :func:`custos_cel.evaluate` raises ``UnboundNameError``.
        node = _let_node(
            let_block={"broken": "${{ inputs.absent }}"},
            call_sites={
                "let.broken": TypedCallSite(
                    source="${{ inputs.absent }}",
                    typed_ast=type_check(
                        parse("inputs.absent"),
                        SchemaBindings(
                            inputs={
                                "type": "object",
                                "properties": {"absent": {"type": "string"}},
                                "required": [],
                            }
                        ),
                    ),
                    kind=CallSiteKind.LET,
                    document_path="spec.steps[0].let.broken",
                )
            },
        )
        graph = _graph(node)

        # Empty ``inputs`` on the context → ``inputs.absent`` raises
        # UnboundNameError at evaluate time → wrapped as
        # ``step.with_input_resolution_error``.
        result = LetStepHandler().execute(_ctx(), graph, "derive")

        assert isinstance(result, StepFailed)
        env = dict(result.envelope)
        assert env["kind"] == "step.with_input_resolution_error"
        assert env["binding_name"] == "broken"
        assert env["step_id"] == "derive"
        assert env["run_id"] == "run-1"
        assert env["attempt"] is None
        assert env["cause_kind"] == "expression.unbound_name"
        assert env["source"] == "${{ inputs.absent }}"

    @pytest.mark.parametrize(
        ("exc", "expected_cause_kind"),
        [
            (UnboundNameError("nope"), "expression.unbound_name"),
            (CelTypeError("bad type"), "expression.type_error"),
            (EvaluationError("boom"), "expression.evaluation_error"),
        ],
    )
    def test_each_cel_error_kind_propagates_to_cause_kind(
        self, exc: Exception, expected_cause_kind: str
    ) -> None:
        # Drive every CelError subclass through the wrapper to prove
        # the ``cause_kind`` carries the underlying ``kind`` verbatim.
        node = _let_node(
            let_block={"v": "${{ 1 + 1 }}"},
            call_sites={"let.v": _cs("1 + 1", binding_name="v")},
        )
        graph = _graph(node)

        with patch("custos_workflow.steps.let_step.custos_cel.evaluate", side_effect=exc):
            result = LetStepHandler().execute(_ctx(), graph, "derive")

        assert isinstance(result, StepFailed)
        env = dict(result.envelope)
        assert env["kind"] == "step.with_input_resolution_error"
        assert env["cause_kind"] == expected_cause_kind
        assert env["binding_name"] == "v"
        assert env["source"] == "${{ 1 + 1 }}"

    def test_first_failing_binding_short_circuits_subsequent_bindings(self) -> None:
        # When ``a`` fails, ``b`` must not be evaluated. Patch
        # ``evaluate`` to raise on the first call; if ``b`` were
        # evaluated, the patch would consume a second side_effect.
        node = _let_node(
            let_block={
                "a": "${{ 1 + 1 }}",
                "b": "${{ let.a + 1 }}",
            },
            call_sites={
                "let.a": _cs("1 + 1", binding_name="a"),
                "let.b": _cs("let.a + 1", binding_name="b", let_types={"a": IntType()}),
            },
        )
        graph = _graph(node)

        with patch(
            "custos_workflow.steps.let_step.custos_cel.evaluate",
            side_effect=[EvaluationError("nope")],
        ) as evaluate_mock:
            result = LetStepHandler().execute(_ctx(), graph, "derive")

        assert isinstance(result, StepFailed)
        assert evaluate_mock.call_count == 1
        env = dict(result.envelope)
        assert env["binding_name"] == "a"


# ---------------------------------------------------------------------------
# Defensive guards
# ---------------------------------------------------------------------------


class TestLetStepHandlerDefensiveGuards:
    def test_unknown_step_id_raises_key_error(self) -> None:
        node = _let_node(
            let_block={"v": 1},
        )
        graph = _graph(node)

        with pytest.raises(KeyError, match="missing"):
            LetStepHandler().execute(_ctx(), graph, "missing")

    def test_non_let_step_raises_not_implemented_error(self) -> None:
        from custos_workflow.document import ActivityStep

        node = ExecutionNode(
            step_id="act",
            kind=StepKind.ACTIVITY,
            primitive_handler=PrimitiveHandler.ACTIVITY_RUNTIME,
            retry_policy=None,
            on_error_routes=(),
            call_sites={},
            step_source=ActivityStep.model_validate(
                {"id": "act", "activity": "x/y@1", "connector": "primary"}
            ),
        )
        graph = _graph(node)

        with pytest.raises(NotImplementedError, match=r"only StepKind\.LET is supported"):
            LetStepHandler().execute(_ctx(), graph, "act")
