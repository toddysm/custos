"""Tests for the ``WithInputResolver`` (WF-IMPL-051)."""

from __future__ import annotations

from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any
from unittest.mock import patch

import pytest
from custos_cel import (
    BindingScope,
    FixedClock,
    RunInfo,
    SchemaBindings,
    StepBinding,
    WorkflowInfo,
    parse,
    type_check,
)
from custos_cel.errors import (
    EvaluationError,
    ParseError,
    UnboundNameError,
)
from custos_cel.errors import (
    TimeoutError as CelTimeoutError,
)
from custos_cel.errors import (
    TypeError as CelTypeError,
)

from custos_workflow.document import ActivityStep
from custos_workflow.graph import (
    CallSiteKind,
    ExecutionNode,
    PrimitiveHandler,
    StepKind,
    TypedCallSite,
)
from custos_workflow.steps import WithInputResolutionError, WithInputResolver

# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------

_INPUTS_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "target": {"type": "string"},
        "count": {"type": "integer"},
        "flag": {"type": "boolean"},
        "tags": {"type": "array", "items": {"type": "string"}},
    },
    "required": [],
}


def _typed(source: str) -> Any:
    ast = parse(source)
    return type_check(ast, SchemaBindings(inputs=_INPUTS_SCHEMA))


def _typed_with_steps(source: str, prior_steps: tuple[tuple[str, dict[str, Any]], ...]) -> Any:
    ast = parse(source)
    return type_check(ast, SchemaBindings(inputs=_INPUTS_SCHEMA, prior_steps=prior_steps))


def _activity_step(step_id: str, with_block: dict[str, Any] | None = None) -> ActivityStep:
    payload: dict[str, Any] = {"id": step_id, "activity": "x/y@1", "connector": "primary"}
    if with_block is not None:
        payload["with"] = with_block
    return ActivityStep.model_validate(payload)


def _node(
    step_id: str = "scan",
    *,
    with_block: dict[str, Any] | None = None,
    call_sites: dict[str, TypedCallSite] | None = None,
) -> ExecutionNode:
    return ExecutionNode(
        step_id=step_id,
        kind=StepKind.ACTIVITY,
        primitive_handler=PrimitiveHandler.ACTIVITY_RUNTIME,
        retry_policy=None,
        on_error_routes=(),
        call_sites=call_sites or {},
        step_source=_activity_step(step_id, with_block),
    )


def _cs(source_cel: str, *, token: str | None = None) -> TypedCallSite:
    return TypedCallSite(
        source=token or f"${{{{ {source_cel} }}}}",
        typed_ast=_typed(source_cel),
        kind=CallSiteKind.WITH,
        document_path="spec.steps[0].with.x",
    )


def _scope(
    *,
    inputs: dict[str, Any] | None = None,
    steps: dict[str, StepBinding] | None = None,
) -> BindingScope:
    return BindingScope(
        run=RunInfo(id="run-1", workspace="ws-1"),
        workflow=WorkflowInfo(name="wf", version="v1"),
        now=lambda: datetime(2026, 1, 1, tzinfo=UTC),
        inputs=inputs or {},
        steps=steps or {},
    )


_CLOCK = FixedClock(datetime(2026, 1, 1, tzinfo=UTC))


# ---------------------------------------------------------------------------
# Happy paths
# ---------------------------------------------------------------------------


class TestWithInputResolverHappyPath:
    def test_node_without_with_block_returns_empty_mapping(self) -> None:
        # An ActivityStep with no ``with:`` field yields an empty
        # input mapping that is still a MappingProxyType snapshot.
        node = _node(with_block=None)
        result = WithInputResolver().resolve(node, _scope(), _CLOCK)
        assert dict(result) == {}
        assert isinstance(result, MappingProxyType)

    def test_node_with_empty_with_block_returns_empty_mapping(self) -> None:
        node = _node(with_block={})
        result = WithInputResolver().resolve(node, _scope(), _CLOCK)
        assert dict(result) == {}
        assert isinstance(result, MappingProxyType)

    def test_non_string_values_pass_through(self) -> None:
        # Numbers, bools, lists, dicts, None bypass CEL entirely.
        node = _node(
            with_block={
                "count": 42,
                "flag": True,
                "ratio": 3.14,
                "tags": ["a", "b"],
                "nested": {"k": 1},
                "absent": None,
            }
        )
        result = WithInputResolver().resolve(node, _scope(), _CLOCK)
        assert result["count"] == 42
        assert result["flag"] is True
        assert result["ratio"] == 3.14
        assert result["tags"] == ["a", "b"]
        assert result["nested"] == {"k": 1}
        assert result["absent"] is None

    def test_string_without_placeholders_passes_through(self) -> None:
        # A plain literal string never invokes the CEL evaluator.
        node = _node(with_block={"image": "ghcr.io/example/scan:1.2.3"})
        result = WithInputResolver().resolve(node, _scope(), _CLOCK)
        assert result["image"] == "ghcr.io/example/scan:1.2.3"

    def test_single_placeholder_preserves_raw_type_int(self) -> None:
        # ``${{ inputs.count }}`` returns the int unchanged — not "42".
        node = _node(
            with_block={"count": "${{ inputs.count }}"},
            call_sites={"with.count": _cs("inputs.count")},
        )
        scope = _scope(inputs={"count": 42})
        result = WithInputResolver().resolve(node, scope, _CLOCK)
        assert result["count"] == 42
        assert isinstance(result["count"], int)

    def test_single_placeholder_preserves_raw_type_list(self) -> None:
        node = _node(
            with_block={"tags": "${{ inputs.tags }}"},
            call_sites={"with.tags": _cs("inputs.tags")},
        )
        scope = _scope(inputs={"tags": ["x", "y"]})
        result = WithInputResolver().resolve(node, scope, _CLOCK)
        assert result["tags"] == ["x", "y"]

    def test_single_placeholder_with_surrounding_whitespace(self) -> None:
        # Whitespace outside the placeholder still counts as a
        # single-placeholder string per the design predicate.
        node = _node(
            with_block={"target": "  ${{ inputs.target }}  "},
            call_sites={"with.target": _cs("inputs.target")},
        )
        scope = _scope(inputs={"target": "host-a"})
        result = WithInputResolver().resolve(node, scope, _CLOCK)
        assert result["target"] == "host-a"
        assert isinstance(result["target"], str)

    def test_mixed_string_interpolates_via_str_conversion(self) -> None:
        # ``prefix-${{ inputs.target }}-${{ inputs.count }}`` should
        # concatenate the literal segments and ``str()``-cast each
        # placeholder value in source order.
        node = _node(
            with_block={"image": "img-${{ inputs.target }}-${{ inputs.count }}-v1"},
            call_sites={
                "with.image[0]": _cs("inputs.target", token="${{ inputs.target }}"),
                "with.image[1]": _cs("inputs.count", token="${{ inputs.count }}"),
            },
        )
        scope = _scope(inputs={"target": "host", "count": 42})
        result = WithInputResolver().resolve(node, scope, _CLOCK)
        assert result["image"] == "img-host-42-v1"

    def test_binding_scope_sees_prior_step_outputs(self) -> None:
        # Steps in the scope are visible as ``steps.<id>.outputs.<key>``.
        upstream_schema = {
            "type": "object",
            "properties": {"host": {"type": "string"}},
            "required": ["host"],
        }
        cs = TypedCallSite(
            source="${{ steps.upstream.outputs.host }}",
            typed_ast=_typed_with_steps(
                "steps.upstream.outputs.host",
                (("upstream", upstream_schema),),
            ),
            kind=CallSiteKind.WITH,
            document_path="spec.steps[0].with.target",
        )
        node = _node(
            with_block={"target": "${{ steps.upstream.outputs.host }}"},
            call_sites={"with.target": cs},
        )
        scope = _scope(steps={"upstream": StepBinding({"host": "host-a"}, sealed=True)})
        result = WithInputResolver().resolve(node, scope, _CLOCK)
        assert result["target"] == "host-a"

    def test_resolved_mapping_is_a_mappingproxytype_snapshot(self) -> None:
        node = _node(
            with_block={"target": "${{ inputs.target }}"},
            call_sites={"with.target": _cs("inputs.target")},
        )
        scope = _scope(inputs={"target": "host-a"})
        result = WithInputResolver().resolve(node, scope, _CLOCK)
        assert isinstance(result, MappingProxyType)
        with pytest.raises(TypeError):
            result["other"] = "x"  # type: ignore[index]

    def test_determinism_two_calls_byte_equal(self) -> None:
        # Same node + scope + clock → same output, twice.
        node = _node(
            with_block={
                "a": "${{ inputs.count }}",
                "b": "x-${{ inputs.count }}-y",
            },
            call_sites={
                "with.a": _cs("inputs.count"),
                "with.b[0]": _cs("inputs.count", token="${{ inputs.count }}"),
            },
        )
        scope = _scope(inputs={"count": 3})
        out1 = WithInputResolver().resolve(node, scope, _CLOCK)
        out2 = WithInputResolver().resolve(node, scope, _CLOCK)
        assert dict(out1) == dict(out2)

    def test_run_id_and_attempt_are_optional(self) -> None:
        # Both default to None; passing them is purely for richer
        # error envelopes if something fails.
        node = _node(with_block={"k": 1})
        result = WithInputResolver().resolve(node, _scope(), _CLOCK, run_id="r1", attempt=1)
        assert result["k"] == 1


# ---------------------------------------------------------------------------
# Error wrapping — every locked CEL kind must surface on cause_kind
# ---------------------------------------------------------------------------


class TestWithInputResolverErrorWrapping:
    def _node_with_slot(self) -> ExecutionNode:
        return _node(
            with_block={"target": "${{ inputs.target }}"},
            call_sites={"with.target": _cs("inputs.target")},
        )

    def test_evaluation_error_wraps_with_cause_kind(self) -> None:
        node = self._node_with_slot()
        scope = _scope(inputs={"target": "ok"})
        with (
            patch(
                "custos_workflow.steps.with_inputs.evaluate",
                side_effect=EvaluationError("boom"),
            ),
            pytest.raises(WithInputResolutionError) as ei,
        ):
            WithInputResolver().resolve(node, scope, _CLOCK)
        assert ei.value.cause_kind == "expression.evaluation_error"
        assert ei.value.binding_name == "target"
        assert ei.value.step_id == "scan"
        assert ei.value.source == "${{ inputs.target }}"

    def test_parse_error_wraps_with_cause_kind(self) -> None:
        node = self._node_with_slot()
        scope = _scope(inputs={"target": "ok"})
        with (
            patch(
                "custos_workflow.steps.with_inputs.evaluate",
                side_effect=ParseError("bad token"),
            ),
            pytest.raises(WithInputResolutionError) as ei,
        ):
            WithInputResolver().resolve(node, scope, _CLOCK)
        assert ei.value.cause_kind == "expression.parse_error"

    def test_type_error_wraps_with_cause_kind(self) -> None:
        node = self._node_with_slot()
        scope = _scope(inputs={"target": "ok"})
        with (
            patch(
                "custos_workflow.steps.with_inputs.evaluate",
                side_effect=CelTypeError("type mismatch"),
            ),
            pytest.raises(WithInputResolutionError) as ei,
        ):
            WithInputResolver().resolve(node, scope, _CLOCK)
        assert ei.value.cause_kind == "expression.type_error"

    def test_unbound_name_wraps_with_cause_kind(self) -> None:
        # Real call — ``inputs.target`` is not in the empty scope.
        node = self._node_with_slot()
        scope = _scope(inputs={})
        with pytest.raises(WithInputResolutionError) as ei:
            WithInputResolver().resolve(node, scope, _CLOCK)
        assert ei.value.cause_kind == "expression.unbound_name"

    def test_unbound_name_via_patched_evaluator(self) -> None:
        node = self._node_with_slot()
        scope = _scope(inputs={"target": "ok"})
        with (
            patch(
                "custos_workflow.steps.with_inputs.evaluate",
                side_effect=UnboundNameError("no such name"),
            ),
            pytest.raises(WithInputResolutionError) as ei,
        ):
            WithInputResolver().resolve(node, scope, _CLOCK)
        assert ei.value.cause_kind == "expression.unbound_name"

    def test_timeout_wraps_with_cause_kind(self) -> None:
        node = self._node_with_slot()
        scope = _scope(inputs={"target": "ok"})
        with (
            patch(
                "custos_workflow.steps.with_inputs.evaluate",
                side_effect=CelTimeoutError("deadline exceeded", elapsed_ms=100, timeout_ms=50),
            ),
            pytest.raises(WithInputResolutionError) as ei,
        ):
            WithInputResolver().resolve(node, scope, _CLOCK)
        assert ei.value.cause_kind == "expression.timeout"

    def test_run_id_and_attempt_attached_to_error_envelope(self) -> None:
        node = self._node_with_slot()
        scope = _scope(inputs={})
        with pytest.raises(WithInputResolutionError) as ei:
            WithInputResolver().resolve(node, scope, _CLOCK, run_id="r1", attempt=2)
        assert ei.value.run_id == "r1"
        assert ei.value.attempt == 2

    def test_error_in_mixed_string_records_binding_name(self) -> None:
        # Failure on the second placeholder of a multi-placeholder
        # value still records the parent binding name (``image``),
        # not the slot label.
        node = _node(
            with_block={"image": "${{ inputs.target }}-${{ inputs.count }}"},
            call_sites={
                "with.image[0]": _cs("inputs.target", token="${{ inputs.target }}"),
                "with.image[1]": _cs("inputs.count", token="${{ inputs.count }}"),
            },
        )
        scope = _scope(inputs={"target": "ok"})  # ``count`` missing → unbound.
        with pytest.raises(WithInputResolutionError) as ei:
            WithInputResolver().resolve(node, scope, _CLOCK)
        assert ei.value.binding_name == "image"
        assert ei.value.cause_kind == "expression.unbound_name"


# ---------------------------------------------------------------------------
# Defensive guards — missing or mis-kinded slots
# ---------------------------------------------------------------------------


class TestWithInputResolverDefensiveGuards:
    def test_missing_call_site_raises_wrapped_error(self) -> None:
        # ``with.target`` should be in ``call_sites`` per the
        # compiler contract; if it's missing, we raise a structured
        # error instead of a bare ``KeyError`` so the audit envelope
        # still carries the slot context.
        node = _node(
            with_block={"target": "${{ inputs.target }}"},
            call_sites={},  # intentionally empty
        )
        with pytest.raises(WithInputResolutionError) as ei:
            WithInputResolver().resolve(node, _scope(), _CLOCK)
        assert "missing TypedAST" in str(ei.value)
        assert ei.value.binding_name == "target"
        assert ei.value.step_id == "scan"

    def test_wrong_kind_slot_rejected(self) -> None:
        # A defensive guard: if a slot label happens to collide
        # with a different ``CallSiteKind`` (impossible today, but
        # we still defend), the resolver refuses to evaluate it.
        wrong = TypedCallSite(
            source="${{ inputs.target }}",
            typed_ast=_typed("inputs.target"),
            kind=CallSiteKind.IF,  # not WITH
            document_path="spec.steps[0].with.target",
        )
        node = _node(
            with_block={"target": "${{ inputs.target }}"},
            call_sites={"with.target": wrong},
        )
        with pytest.raises(WithInputResolutionError) as ei:
            WithInputResolver().resolve(node, _scope(inputs={"target": "x"}), _CLOCK)
        assert "is not a with: call site" in str(ei.value)

    def test_missing_indexed_slot_raises_wrapped_error(self) -> None:
        # Multi-placeholder string missing one of the indexed slots
        # is still surfaced cleanly.
        node = _node(
            with_block={"image": "${{ inputs.target }}-${{ inputs.count }}"},
            call_sites={
                "with.image[0]": _cs("inputs.target", token="${{ inputs.target }}"),
                # "with.image[1]" intentionally omitted
            },
        )
        with pytest.raises(WithInputResolutionError) as ei:
            WithInputResolver().resolve(node, _scope(inputs={"target": "ok", "count": 3}), _CLOCK)
        assert "missing TypedAST" in str(ei.value)
        assert ei.value.binding_name == "image"
