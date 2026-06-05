"""Tests for the Custos CEL evaluator (WF-IMPL-006).

Covers literals, identifier resolution through the binding scope,
member / index chains, every operator family, ternary, list/map
literals, the function allow-list (``now``, ``size``, ``has``,
``type``), and the sandbox / determinism acceptance criteria.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from custos_cel import (
    BindingScope,
    DoubleType,
    EvalError,
    FixedClock,
    IntType,
    RunInfo,
    SchemaBindings,
    StepBinding,
    StringType,
    UnboundNameError,
    WorkflowInfo,
    evaluate,
    parse,
    type_check,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FIXED_TS = datetime(2026, 5, 22, 12, 0, 0, tzinfo=UTC)


def _clock() -> FixedClock:
    return FixedClock(_FIXED_TS)


def _scope(
    *,
    inputs: dict[str, Any] | None = None,
    steps: dict[str, StepBinding] | None = None,
    let: dict[str, Any] | None = None,
    event: dict[str, Any] | None = None,
    run_id: str = "run-123",
    workspace: str = "ws-1",
    workflow_name: str = "wf",
    workflow_version: str = "v1",
) -> BindingScope:
    return BindingScope(
        run=RunInfo(id=run_id, workspace=workspace),
        workflow=WorkflowInfo(name=workflow_name, version=workflow_version),
        now=_clock().now,
        inputs=inputs or {},
        steps=steps or {},
        let=let or {},
        event=event or {},
    )


_INPUTS_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "image": {"type": "string"},
        "count": {"type": "integer"},
        "ratio": {"type": "number"},
        "enabled": {"type": "boolean"},
        "tags": {"type": "array", "items": {"type": "string"}},
        "labels": {"type": "object", "additionalProperties": {"type": "string"}},
        "targets": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {"image": {"type": "string"}, "tag": {"type": "string"}},
            },
        },
    },
}


def _bindings(**overrides: Any) -> SchemaBindings:
    defaults: dict[str, Any] = {
        "inputs": _INPUTS_SCHEMA,
        "prior_steps": (
            (
                "scan",
                {"type": "object", "properties": {"critical": {"type": "integer"}}},
            ),
            (
                "scan-alt",
                {"type": "object", "properties": {"critical": {"type": "integer"}}},
            ),
        ),
        "let": {"totalCritical": IntType()},
    }
    defaults.update(overrides)
    return SchemaBindings(**defaults)


def _eval(
    src: str,
    *,
    scope: BindingScope | None = None,
    clock: FixedClock | None = None,
    bindings: SchemaBindings | None = None,
) -> Any:
    ast = parse(src)
    typed = type_check(ast, bindings or _bindings())
    return evaluate(typed, scope or _scope(), clock or _clock())


# ---------------------------------------------------------------------------
# Public surface guards
# ---------------------------------------------------------------------------


def test_evaluate_rejects_non_scope() -> None:
    typed = type_check(parse("1 + 1"), _bindings())
    with pytest.raises(TypeError, match="BindingScope"):
        evaluate(typed, None, _clock())  # type: ignore[arg-type]


def test_evaluate_rejects_non_clock() -> None:
    typed = type_check(parse("1 + 1"), _bindings())
    with pytest.raises(TypeError, match="Clock"):
        evaluate(typed, _scope(), object())  # type: ignore[arg-type]


def test_evaluate_rejects_non_node_ast() -> None:
    with pytest.raises(TypeError, match="'typed_ast' must be a Node"):
        evaluate("1 + 1", _scope(), _clock())  # type: ignore[arg-type]


def test_evaluate_rejects_untyped_ast() -> None:
    # ``parse()`` returns an AST whose nodes all have ``cel_type=None``.
    # ``evaluate`` is documented to require a TypedAST; the wrapper
    # enforces that invariant at the root level.
    untyped = parse("1 + 1")
    assert untyped.cel_type is None
    with pytest.raises(TypeError, match="untyped"):
        evaluate(untyped, _scope(), _clock())


# ---------------------------------------------------------------------------
# Literals
# ---------------------------------------------------------------------------


def test_int_literal() -> None:
    assert _eval("42") == 42


def test_string_literal() -> None:
    assert _eval('"hello"') == "hello"


def test_bool_literals() -> None:
    assert _eval("true") is True
    assert _eval("false") is False


def test_null_literal() -> None:
    assert _eval("null") is None


def test_double_literal() -> None:
    assert _eval("3.14") == pytest.approx(3.14)


def test_negative_literal() -> None:
    assert _eval("-1") == -1


# ---------------------------------------------------------------------------
# Identifier resolution / Member / Index
# ---------------------------------------------------------------------------


def test_inputs_member_access() -> None:
    scope = _scope(inputs={"image": "alpine:3.19"})
    assert _eval("inputs.image", scope=scope) == "alpine:3.19"


def test_inputs_nested_via_homogeneous_map() -> None:
    scope = _scope(inputs={"labels": {"app": "web"}})
    assert _eval('inputs.labels["app"]', scope=scope) == "web"


def test_inputs_array_runtime_index() -> None:
    scope = _scope(inputs={"tags": ["alpha", "beta", "gamma"]})
    assert _eval("inputs.tags[1]", scope=scope) == "beta"


def test_inputs_array_of_objects_member_after_index() -> None:
    scope = _scope(
        inputs={
            "targets": [
                {"image": "a", "tag": "1"},
                {"image": "b", "tag": "2"},
            ]
        }
    )
    assert _eval("inputs.targets[0].image", scope=scope) == "a"
    assert _eval("inputs.targets[1].tag", scope=scope) == "2"


def test_steps_outputs_canonical_design_example() -> None:
    scope = _scope(
        steps={
            "scan": StepBinding(outputs={"critical": 7}, sealed=True),
            "scan-alt": StepBinding(outputs={"critical": 5}, sealed=True),
        }
    )
    assert (
        _eval('steps.scan.outputs.critical + steps["scan-alt"].outputs.critical', scope=scope) == 12
    )


def test_run_and_workflow_fields() -> None:
    scope = _scope(run_id="abc", workspace="ws-x", workflow_name="my-wf", workflow_version="v9")
    assert _eval("run.id", scope=scope) == "abc"
    assert _eval("run.workspace", scope=scope) == "ws-x"
    assert _eval("workflow.name", scope=scope) == "my-wf"
    assert _eval("workflow.version", scope=scope) == "v9"


# ---------------------------------------------------------------------------
# event root (TS-IMPL-005)
# ---------------------------------------------------------------------------


def _event_scope() -> BindingScope:
    return _scope(
        event={
            "kind": "workflow.completed",
            "subject": "run/abc",
            "source": {"type": "workflow", "vendor": "custos"},
            "data": {"status": "succeeded", "repository": "ghcr.io/acme/app"},
            "raw": {"headers": {"x-id": "1"}, "body": "{}"},
        }
    )


def test_event_scalar_and_subtree_eval() -> None:
    scope = _event_scope()
    assert _eval("event.kind", scope=scope) == "workflow.completed"
    assert _eval("event.subject", scope=scope) == "run/abc"
    assert _eval("event.source.vendor", scope=scope) == "custos"
    assert _eval("event.data.status", scope=scope) == "succeeded"
    assert _eval("event.raw.body", scope=scope) == "{}"


def test_event_canonical_selector_eval_true() -> None:
    scope = _event_scope()
    expr = 'event.kind == "workflow.completed" && event.data.status == "succeeded"'
    assert _eval(expr, scope=scope) is True


def test_event_canonical_selector_eval_false() -> None:
    scope = _scope(event={"kind": "workflow.completed", "data": {"status": "failed"}})
    expr = 'event.kind == "workflow.completed" && event.data.status == "succeeded"'
    assert _eval(expr, scope=scope) is False


def test_event_desugared_prefix_selector_eval() -> None:
    scope = _event_scope()
    expr = 'event.data.status in ["succeeded", "failed"]'
    assert _eval(expr, scope=scope) is True


def test_event_has_macro_on_subtree() -> None:
    scope = _event_scope()
    assert _eval("has(event.data.status)", scope=scope) is True
    assert _eval("has(event.data.missing)", scope=scope) is False


def test_let_binding_resolves_through_scope() -> None:
    scope = _scope(let={"totalCritical": 99})
    assert _eval("let.totalCritical", scope=scope) == 99


def test_index_out_of_range_raises_eval_error() -> None:
    scope = _scope(inputs={"tags": ["a"]})
    with pytest.raises(EvalError, match="out of range"):
        _eval("inputs.tags[5]", scope=scope)


# ---------------------------------------------------------------------------
# Arithmetic — integer (CEL truncation toward zero)
# ---------------------------------------------------------------------------


def test_integer_addition() -> None:
    assert _eval("2 + 3") == 5


def test_integer_subtraction() -> None:
    assert _eval("10 - 4") == 6


def test_integer_multiplication() -> None:
    assert _eval("6 * 7") == 42


def test_integer_division_positive() -> None:
    assert _eval("7 / 2") == 3


def test_integer_division_truncates_toward_zero_for_negative() -> None:
    # Python's `//` floors (`-7 // 2 == -4`); CEL truncates (`-3`).
    assert _eval("-7 / 2") == -3


def test_integer_modulo_positive() -> None:
    assert _eval("10 % 3") == 1


def test_integer_modulo_truncated_remainder_for_negative() -> None:
    # CEL: `(-7) - trunc(-7/2)*2 == -7 - (-3)*2 == -1`
    assert _eval("-7 % 2") == -1


def test_integer_division_by_zero_raises() -> None:
    with pytest.raises(EvalError, match="division by zero"):
        _eval("1 / 0")


def test_integer_modulo_by_zero_raises() -> None:
    with pytest.raises(EvalError, match="modulo by zero"):
        _eval("1 % 0")


# ---------------------------------------------------------------------------
# Arithmetic — double
# ---------------------------------------------------------------------------


def test_double_addition() -> None:
    assert _eval("1.5 + 2.25") == pytest.approx(3.75)


def test_double_division() -> None:
    assert _eval("5.0 / 2.0") == pytest.approx(2.5)


def test_double_division_by_zero_raises() -> None:
    with pytest.raises(EvalError, match="division by zero"):
        _eval("1.0 / 0.0")


# ---------------------------------------------------------------------------
# Concatenation
# ---------------------------------------------------------------------------


def test_string_concatenation() -> None:
    assert _eval('"hello" + " " + "world"') == "hello world"


def test_list_concatenation() -> None:
    assert _eval("[1, 2] + [3, 4]") == [1, 2, 3, 4]


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------


def test_integer_comparisons() -> None:
    assert _eval("1 < 2") is True
    assert _eval("2 <= 2") is True
    assert _eval("3 > 2") is True
    assert _eval("3 >= 3") is True


def test_string_comparisons() -> None:
    assert _eval('"a" < "b"') is True
    assert _eval('"b" >= "a"') is True


# ---------------------------------------------------------------------------
# Equality (CEL: strict types, null-aware)
# ---------------------------------------------------------------------------


def test_equality_same_type() -> None:
    assert _eval("1 == 1") is True
    assert _eval('"x" == "x"') is True
    assert _eval("1 != 2") is True


def test_null_equality_is_total() -> None:
    assert _eval("null == null") is True


def test_equality_against_null_is_false_for_non_null() -> None:
    scope = _scope(inputs={"image": "a"})
    assert _eval("inputs.image == null", scope=scope) is False


# ---------------------------------------------------------------------------
# Logical (short-circuit)
# ---------------------------------------------------------------------------


def test_logical_and() -> None:
    assert _eval("true && true") is True
    assert _eval("true && false") is False
    assert _eval("false && false") is False


def test_logical_or() -> None:
    assert _eval("true || false") is True
    assert _eval("false || false") is False


def test_logical_short_circuit_avoids_rhs_evaluation() -> None:
    # The right-hand side would raise EvalError (division by zero)
    # if executed; the short-circuit guarantees it isn't.
    assert _eval("false && (1 / 0 == 0)") is False
    assert _eval("true || (1 / 0 == 0)") is True


# ---------------------------------------------------------------------------
# Unary
# ---------------------------------------------------------------------------


def test_unary_neg_int() -> None:
    scope = _scope(inputs={"count": 5})
    assert _eval("-inputs.count", scope=scope) == -5


def test_unary_not() -> None:
    assert _eval("!true") is False
    assert _eval("!false") is True


# ---------------------------------------------------------------------------
# IN
# ---------------------------------------------------------------------------


def test_in_list() -> None:
    assert _eval('"b" in ["a", "b", "c"]') is True
    assert _eval('"z" in ["a", "b"]') is False


def test_in_map_checks_keys() -> None:
    scope = _scope(inputs={"labels": {"x": "1", "y": "2"}})
    assert _eval('"x" in inputs.labels', scope=scope) is True
    assert _eval('"z" in inputs.labels', scope=scope) is False


# ---------------------------------------------------------------------------
# Ternary
# ---------------------------------------------------------------------------


def test_ternary_then() -> None:
    assert _eval('true ? "a" : "b"') == "a"


def test_ternary_else() -> None:
    assert _eval('false ? "a" : "b"') == "b"


def test_ternary_lazy_branch_not_evaluated() -> None:
    # The non-selected branch would raise EvalError if executed.
    assert _eval('true ? "yes" : (1 / 0 == 0 ? "a" : "b")') == "yes"
    assert _eval('false ? (1 / 0 == 0 ? "a" : "b") : "no"') == "no"


# ---------------------------------------------------------------------------
# List / Map literals
# ---------------------------------------------------------------------------


def test_list_literal() -> None:
    assert _eval("[1, 2, 3]") == [1, 2, 3]


def test_map_literal() -> None:
    assert _eval('{"a": 1, "b": 2}') == {"a": 1, "b": 2}


# ---------------------------------------------------------------------------
# now() / Clock
# ---------------------------------------------------------------------------


def test_now_returns_clock_value() -> None:
    assert _eval("now()") == _FIXED_TS


def test_now_with_different_fixed_clock_returns_that_value() -> None:
    other_ts = datetime(2030, 1, 1, tzinfo=UTC)
    clock = FixedClock(other_ts)
    scope = BindingScope(
        run=RunInfo(id="r", workspace="w"),
        workflow=WorkflowInfo(name="n", version="v"),
        now=clock.now,
    )
    typed = type_check(parse("now()"), _bindings())
    assert evaluate(typed, scope, clock) == other_ts


# ---------------------------------------------------------------------------
# size() / has() / type()
# ---------------------------------------------------------------------------


def test_size_of_string() -> None:
    scope = _scope(inputs={"image": "alpine:3.19"})
    assert _eval("size(inputs.image)", scope=scope) == len("alpine:3.19")


def test_size_of_list() -> None:
    scope = _scope(inputs={"tags": ["a", "b", "c"]})
    assert _eval("size(inputs.tags)", scope=scope) == 3


def test_size_of_map() -> None:
    scope = _scope(inputs={"labels": {"a": "1", "b": "2"}})
    assert _eval("size(inputs.labels)", scope=scope) == 2


def test_has_present_field() -> None:
    scope = _scope(inputs={"image": "alpine"})
    assert _eval("has(inputs.image)", scope=scope) is True


def test_has_absent_field() -> None:
    scope = _scope(inputs={})
    # ``image`` is declared in the schema; the runtime binding lacks it.
    assert _eval("has(inputs.image)", scope=scope) is False


def test_has_string_literal_index_form() -> None:
    scope = _scope(inputs={"labels": {"app": "web"}})
    assert _eval('has(inputs.labels["app"])', scope=scope) is True
    assert _eval('has(inputs.labels["missing"])', scope=scope) is False


def test_has_steps_dotted_outputs_field() -> None:
    # ``has(steps.<id>.outputs.<key>)`` — the dotted form. Regression
    # guard for the WF-IMPL-010 evaluator fix: the type checker
    # accepts the chain but ``BindingScope.resolve`` rejects
    # ``steps.<id>.outputs`` as "not a value", so ``_eval_has`` must
    # special-case the chain to probe the outputs mapping directly.
    scope = _scope(
        steps={
            "scan": StepBinding(outputs={"critical": 7}, sealed=True),
        }
    )
    assert _eval("has(steps.scan.outputs.critical)", scope=scope) is True
    assert _eval("has(steps.scan.outputs.absent)", scope=scope) is False


def test_has_steps_bracket_outputs_field() -> None:
    # ``has(steps["<id>"].outputs.<key>)`` — the bracket form,
    # required for step ids that aren't valid CEL identifiers (e.g.
    # ``scan-alt`` contains a ``-``). The evaluator must recognize
    # the ``Index(Ident("steps"), Literal("..."))`` target shape the
    # same way it recognizes the dotted ``Member`` chain.
    scope = _scope(
        steps={
            "scan-alt": StepBinding(outputs={"critical": 5}, sealed=True),
        }
    )
    assert _eval('has(steps["scan-alt"].outputs.critical)', scope=scope) is True
    assert _eval('has(steps["scan-alt"].outputs.absent)', scope=scope) is False


def test_has_steps_unknown_step_id_raises() -> None:
    # An unknown step id reaching ``has`` is a typo, not a missing
    # field — both the dotted and bracket forms must raise
    # ``UnboundNameError`` rather than silently returning False.
    bindings = SchemaBindings(
        inputs=_INPUTS_SCHEMA,
        prior_steps=(
            (
                "scan-alt",
                {"type": "object", "properties": {"critical": {"type": "integer"}}},
            ),
        ),
    )
    scope = _scope(steps={})  # type-checker sees scan-alt; runtime doesn't have it
    with pytest.raises(UnboundNameError):
        _eval('has(steps["scan-alt"].outputs.critical)', scope=scope, bindings=bindings)


def test_has_unbound_target_still_raises() -> None:
    # ``bogus`` is not a binding root, so the *target* itself is
    # unbound — that's a typo, not a missing field, and ``has`` is
    # not a free pass for those.
    scope = _scope()
    with pytest.raises(UnboundNameError):
        _eval("has(bogus.x)", scope=scope)


def test_type_of_int_literal() -> None:
    assert _eval("type(1)") == "int"


def test_type_of_string_literal() -> None:
    assert _eval('type("hello")') == "string"


def test_type_of_list_literal() -> None:
    assert _eval("type([1, 2])") == "list"


def test_type_of_now() -> None:
    assert _eval("type(now())") == "google.protobuf.Timestamp"


# ---------------------------------------------------------------------------
# Allow-list / sandbox
# ---------------------------------------------------------------------------


def test_unknown_function_call_raises_unbound() -> None:
    # The type checker rejects this at compile time — verify the
    # error type is structurally the same as evaluate's runtime
    # allow-list error.
    from custos_cel import TypeCheckError

    with pytest.raises(TypeCheckError, match="unknown function"):
        type_check(parse("open()"), _bindings())


def test_unknown_root_identifier_raises_unbound() -> None:
    # The type checker handles this at compile time, but bypassing
    # the type check (by hand-crafting an AST) is part of WF-IMPL-005;
    # here we just confirm that the eval-time call path goes through
    # ``scope.resolve`` which enforces the root allow-list.
    from custos_cel import Ident, IntType, SourcePosition

    # ``cel_type`` is a placeholder — the root-level guard requires it
    # to be non-``None``; this test exercises ill-typed escape, not
    # untyped escape.
    ast = Ident(pos=SourcePosition(line=1, column=1), cel_type=IntType(), name="os")
    with pytest.raises(UnboundNameError):
        evaluate(ast, _scope(), _clock())


def test_eval_module_does_not_import_dangerous_stdlib() -> None:
    # Static audit: a literal source-text scan over *code lines* (not
    # docstrings / comments) asserts the sandbox acceptance criterion
    # in the issue. Failing this test means someone added a dangerous
    # import / call to ``custos_cel/eval.py``.
    import ast as _pyast
    import importlib.util
    import pathlib

    spec = importlib.util.find_spec("custos_cel.eval")
    assert spec is not None and spec.origin is not None
    source = pathlib.Path(spec.origin).read_text()
    tree = _pyast.parse(source)
    forbidden_modules = {"os", "sys", "subprocess", "socket", "importlib"}
    forbidden_names = {"open", "__import__", "eval", "exec", "compile"}

    for node in _pyast.walk(tree):
        if isinstance(node, _pyast.Import):
            for alias in node.names:
                root = alias.name.split(".")[0]
                assert root not in forbidden_modules, (
                    f"forbidden import {alias.name!r} found in custos_cel/eval.py"
                )
        elif isinstance(node, _pyast.ImportFrom):
            assert node.module is not None
            root = node.module.split(".")[0]
            assert root not in forbidden_modules, (
                f"forbidden import-from {node.module!r} found in custos_cel/eval.py"
            )
        elif isinstance(node, _pyast.Call) and isinstance(node.func, _pyast.Name):
            assert node.func.id not in forbidden_names, (
                f"forbidden call to {node.func.id!r} found in custos_cel/eval.py"
            )
        elif isinstance(node, _pyast.Name):
            # Bare reference to a builtin like ``__import__`` (without
            # a call) is also forbidden — callers could still bind it
            # to a local and invoke through the binding.
            assert node.id not in forbidden_names, (
                f"forbidden bare reference to {node.id!r} found in custos_cel/eval.py"
            )


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


def test_two_evaluations_with_same_fixed_clock_byte_equal() -> None:
    scope = _scope(inputs={"image": "alpine:3.19", "count": 5}, let={"totalCritical": 99})
    src = (
        'inputs.image + " count=" + (inputs.count + let.totalCritical > 100 ? "high" : "low")'
        ' + " now=" + type(now())'
    )
    a = _eval(src, scope=scope)
    b = _eval(src, scope=scope)
    assert a == b
    assert isinstance(a, str)


def test_now_invariant_across_calls_in_same_evaluation() -> None:
    # Two ``now()`` references in the same expression resolve to the
    # same value (the clock is queried each time but a FixedClock
    # returns the same instant).
    assert _eval("now() == now()") is True


# ---------------------------------------------------------------------------
# Position propagation on runtime failures
# ---------------------------------------------------------------------------


def test_eval_error_carries_source_position() -> None:
    with pytest.raises(EvalError) as excinfo:
        _eval("1 / 0")
    assert excinfo.value.source_position is not None
    assert excinfo.value.kind == "expression.evaluation_error"


# ---------------------------------------------------------------------------
# Member access on plain values
# ---------------------------------------------------------------------------


def test_index_on_non_collection_runtime_value_raises() -> None:
    scope = _scope(inputs={"count": 5})
    # Type checker rejects this at compile time, but if a caller
    # bypasses it (e.g. by hand-building an AST), the evaluator
    # surfaces a clean EvalError rather than leaking the host
    # ``__getitem__`` machinery.
    from custos_cel import Ident, Index, Literal, LiteralKind, Member, SourcePosition

    pos = SourcePosition(line=1, column=1)
    ast = Index(
        pos=pos,
        cel_type=IntType(),
        target=Member(
            pos=pos,
            cel_type=IntType(),
            target=Ident(pos=pos, cel_type=None, name="inputs"),
            name="count",
        ),
        index=Literal(pos=pos, cel_type=IntType(), kind=LiteralKind.INT, value=0),
    )
    with pytest.raises(EvalError):
        evaluate(ast, scope, _clock())


# ---------------------------------------------------------------------------
# Type-error paths the evaluator should surface (defense in depth)
# ---------------------------------------------------------------------------


def test_unary_neg_on_non_numeric_via_hand_crafted_ast() -> None:
    from custos_cel import Literal, LiteralKind, SourcePosition, Unary, UnaryOp

    pos = SourcePosition(line=1, column=1)
    ast = Unary(
        pos=pos,
        cel_type=StringType(),
        op=UnaryOp.NEG,
        operand=Literal(pos=pos, cel_type=StringType(), kind=LiteralKind.STRING, value="x"),
    )
    with pytest.raises(EvalError, match="numeric"):
        evaluate(ast, _scope(), _clock())


def test_division_double_int_mix_via_hand_crafted_ast() -> None:
    from custos_cel import Binary, BinaryOp, Literal, LiteralKind, SourcePosition

    pos = SourcePosition(line=1, column=1)
    ast = Binary(
        pos=pos,
        cel_type=DoubleType(),
        op=BinaryOp.ADD,
        left=Literal(pos=pos, cel_type=IntType(), kind=LiteralKind.INT, value=1),
        right=Literal(pos=pos, cel_type=DoubleType(), kind=LiteralKind.DOUBLE, value=1.0),
    )
    with pytest.raises(EvalError, match="undefined"):
        evaluate(ast, _scope(), _clock())
