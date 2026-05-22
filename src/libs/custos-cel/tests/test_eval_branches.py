"""Defensive-path coverage for the evaluator.

The tests in ``test_eval.py`` exercise the source-language surface
(parse → type_check → evaluate). Many evaluator branches are *only*
reachable via hand-crafted ASTs that bypass the type checker —
they exist as defense-in-depth: if the type checker were ever to
admit an ill-typed tree (a bug), the evaluator must still degrade
to a structured ``EvalError`` rather than leak host semantics.

These tests build those ill-typed trees directly and call
``evaluate`` on them, asserting the evaluator surfaces a clean
``EvalError`` (or, for unknown identifiers, ``UnboundNameError``).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from custos_cel import (
    Binary,
    BinaryOp,
    BindingScope,
    BoolType,
    BytesType,
    Call,
    Conditional,
    DoubleType,
    EvalError,
    FixedClock,
    Ident,
    Index,
    IntType,
    ListLit,
    ListType,
    Literal,
    LiteralKind,
    MapLit,
    MapType,
    Member,
    NullType,
    RunInfo,
    SchemaBindings,
    SourcePosition,
    StepBinding,
    StringType,
    TimestampType,
    UintType,
    Unary,
    UnaryOp,
    UnboundNameError,
    WorkflowInfo,
    evaluate,
    parse,
    type_check,
)

# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

_POS = SourcePosition(line=1, column=1, offset=0)
_TS = datetime(2026, 5, 22, tzinfo=UTC)


def _make_scope(
    *,
    inputs: dict[str, Any] | None = None,
    steps: dict[str, StepBinding] | None = None,
    let: dict[str, Any] | None = None,
) -> BindingScope:
    return BindingScope(
        run=RunInfo(id="r1", workspace="w1"),
        workflow=WorkflowInfo(name="wf", version="v1"),
        now=lambda: _TS,
        inputs=inputs or {},
        steps=steps or {},
        let=let or {},
    )


def _clock() -> FixedClock:
    return FixedClock(_TS)


def _ilit(n: int) -> Literal:
    return Literal(pos=_POS, cel_type=IntType(), kind=LiteralKind.INT, value=n)


def _slit(s: str) -> Literal:
    return Literal(pos=_POS, cel_type=StringType(), kind=LiteralKind.STRING, value=s)


def _blit(b: bool) -> Literal:
    return Literal(pos=_POS, cel_type=BoolType(), kind=LiteralKind.BOOL, value=b)


# ---------------------------------------------------------------------------
# Chain-collapse: root not an Ident
# ---------------------------------------------------------------------------


def test_member_chain_with_non_ident_root() -> None:
    # ``[1, 2, 3][0]`` — the chain root is a ListLit, so the
    # evaluator falls through to the "evaluate root, then apply
    # accessors" arm of ``_eval_member_or_index``.
    ast = Index(
        pos=_POS,
        cel_type=IntType(),
        target=ListLit(
            pos=_POS,
            cel_type=ListType(element=IntType()),
            elements=(
                _ilit(10),
                _ilit(20),
                _ilit(30),
            ),
        ),
        index=_ilit(1),
    )
    assert evaluate(ast, _make_scope(), _clock()) == 20


# ---------------------------------------------------------------------------
# _runtime_access: missing key + non-int index + non-collection target
# ---------------------------------------------------------------------------


def test_missing_key_on_runtime_homogeneous_map() -> None:
    # ``inputs.labels[let.k]`` — the index is a runtime value (not
    # a string literal in the source), so the chain-collapse logic
    # routes the final accessor through ``_runtime_access`` rather
    # than ``scope.resolve``. The key is absent at runtime.
    schema = SchemaBindings(
        inputs={
            "type": "object",
            "properties": {
                "labels": {
                    "type": "object",
                    "additionalProperties": {"type": "string"},
                },
            },
        },
        let={"k": StringType()},
    )
    typed = type_check(parse("inputs.labels[let.k]"), schema)
    scope = _make_scope(inputs={"labels": {"a": "1"}}, let={"k": "missing"})
    with pytest.raises(EvalError, match="missing key"):
        evaluate(typed, scope, _clock())


def test_runtime_access_list_with_bool_index() -> None:
    # Hand-crafted: ``inputs.tags[true]`` — type checker would reject
    # this, but the evaluator must surface a clean error if it ever
    # arrives. Construct the AST directly.
    ast = Index(
        pos=_POS,
        cel_type=StringType(),
        target=Member(
            pos=_POS,
            cel_type=ListType(element=StringType()),
            target=Ident(pos=_POS, cel_type=None, name="inputs"),
            name="tags",
        ),
        index=_blit(True),
    )
    scope = _make_scope(inputs={"tags": ["a", "b"]})
    with pytest.raises(EvalError, match="list index must be int"):
        evaluate(ast, scope, _clock())


def test_runtime_access_on_string_value_is_rejected() -> None:
    # Hand-crafted: ``inputs.image[0]`` — strings are not indexable
    # in the CEL subset.
    ast = Index(
        pos=_POS,
        cel_type=StringType(),
        target=Member(
            pos=_POS,
            cel_type=StringType(),
            target=Ident(pos=_POS, cel_type=None, name="inputs"),
            name="image",
        ),
        index=_ilit(0),
    )
    scope = _make_scope(inputs={"image": "alpine"})
    with pytest.raises(EvalError, match="cannot access"):
        evaluate(ast, scope, _clock())


# ---------------------------------------------------------------------------
# Call dispatch: unknown function + wrong arg counts
# ---------------------------------------------------------------------------


def test_call_unknown_function_raises_unbound() -> None:
    # The type checker rejects this at compile time, so the only
    # way to hit the evaluator's allow-list is a hand-crafted AST.
    ast = Call(pos=_POS, cel_type=None, function="open", args=(_slit("/etc/passwd"),))
    with pytest.raises(UnboundNameError, match="allow-list"):
        evaluate(ast, _make_scope(), _clock())


def test_now_with_args_rejected() -> None:
    ast = Call(pos=_POS, cel_type=TimestampType(), function="now", args=(_ilit(1),))
    with pytest.raises(EvalError, match="no arguments"):
        evaluate(ast, _make_scope(), _clock())


def test_size_with_zero_args_rejected() -> None:
    ast = Call(pos=_POS, cel_type=IntType(), function="size", args=())
    with pytest.raises(EvalError, match="exactly one"):
        evaluate(ast, _make_scope(), _clock())


def test_size_on_unsupported_type_rejected() -> None:
    ast = Call(pos=_POS, cel_type=IntType(), function="size", args=(_ilit(42),))
    with pytest.raises(EvalError, match="size"):
        evaluate(ast, _make_scope(), _clock())


def test_type_with_zero_args_rejected() -> None:
    ast = Call(pos=_POS, cel_type=StringType(), function="type", args=())
    with pytest.raises(EvalError, match="exactly one"):
        evaluate(ast, _make_scope(), _clock())


def test_type_falls_back_to_runtime_when_cel_type_absent() -> None:
    # ``type(x)`` where ``x`` has ``cel_type=None`` — the evaluator
    # evaluates the runtime value and probes its Python type.
    untyped_arg = ListLit(pos=_POS, cel_type=None, elements=(_ilit(1),))
    ast = Call(pos=_POS, cel_type=StringType(), function="type", args=(untyped_arg,))
    assert evaluate(ast, _make_scope(), _clock()) == "list"


def test_has_zero_args_rejected() -> None:
    ast = Call(pos=_POS, cel_type=BoolType(), function="has", args=())
    with pytest.raises(EvalError, match="exactly one"):
        evaluate(ast, _make_scope(), _clock())


def test_has_with_call_argument_rejected() -> None:
    # ``has(now())`` is not a valid macro shape (must be member or
    # string-literal index).
    ast = Call(
        pos=_POS,
        cel_type=BoolType(),
        function="has",
        args=(Call(pos=_POS, cel_type=TimestampType(), function="now", args=()),),
    )
    with pytest.raises(EvalError, match="dotted member"):
        evaluate(ast, _make_scope(), _clock())


# ---------------------------------------------------------------------------
# has() across all binding roots + non-mapping targets
# ---------------------------------------------------------------------------


def test_has_on_let_root() -> None:
    ast = Call(
        pos=_POS,
        cel_type=BoolType(),
        function="has",
        args=(
            Member(
                pos=_POS,
                cel_type=None,
                target=Ident(pos=_POS, cel_type=None, name="let"),
                name="x",
            ),
        ),
    )
    scope = _make_scope(let={"x": 1})
    assert evaluate(ast, scope, _clock()) is True
    scope2 = _make_scope(let={})
    assert evaluate(ast, scope2, _clock()) is False


def test_has_on_steps_root() -> None:
    ast = Call(
        pos=_POS,
        cel_type=BoolType(),
        function="has",
        args=(
            Member(
                pos=_POS,
                cel_type=None,
                target=Ident(pos=_POS, cel_type=None, name="steps"),
                name="scan",
            ),
        ),
    )
    scope = _make_scope(steps={"scan": StepBinding(outputs={"c": 1}, sealed=True)})
    assert evaluate(ast, scope, _clock()) is True
    assert evaluate(ast, _make_scope(), _clock()) is False


def test_has_on_run_root_returns_false_for_non_mapping_value() -> None:
    # ``run`` resolves to a frozen dataclass, not a Mapping — the
    # macro returns False (the evaluator stays total without leaking
    # host attribute semantics).
    ast = Call(
        pos=_POS,
        cel_type=BoolType(),
        function="has",
        args=(
            Member(
                pos=_POS,
                cel_type=None,
                target=Ident(pos=_POS, cel_type=None, name="run"),
                name="id",
            ),
        ),
    )
    assert evaluate(ast, _make_scope(), _clock()) is False


def test_has_on_workflow_root_returns_false_for_non_mapping_value() -> None:
    ast = Call(
        pos=_POS,
        cel_type=BoolType(),
        function="has",
        args=(
            Member(
                pos=_POS,
                cel_type=None,
                target=Ident(pos=_POS, cel_type=None, name="workflow"),
                name="name",
            ),
        ),
    )
    assert evaluate(ast, _make_scope(), _clock()) is False


def test_has_on_now_root_returns_false() -> None:
    # ``has(now.x)`` — ``now`` resolves to a callable (not a
    # mapping), so the macro returns False rather than raising.
    ast = Call(
        pos=_POS,
        cel_type=BoolType(),
        function="has",
        args=(
            Member(
                pos=_POS,
                cel_type=None,
                target=Ident(pos=_POS, cel_type=None, name="now"),
                name="x",
            ),
        ),
    )
    assert evaluate(ast, _make_scope(), _clock()) is False


def test_has_on_list_value_returns_false() -> None:
    # ``has(some_list.x)`` — the target value is a list at runtime;
    # the macro returns False (lists have no fields).
    ast = Call(
        pos=_POS,
        cel_type=BoolType(),
        function="has",
        args=(
            Member(
                pos=_POS,
                cel_type=None,
                target=ListLit(
                    pos=_POS,
                    cel_type=None,
                    elements=(
                        _ilit(1),
                        _ilit(2),
                    ),
                ),
                name="x",
            ),
        ),
    )
    assert evaluate(ast, _make_scope(), _clock()) is False


def test_has_on_scalar_value_returns_false() -> None:
    ast = Call(
        pos=_POS,
        cel_type=BoolType(),
        function="has",
        args=(
            Member(
                pos=_POS,
                cel_type=None,
                target=_ilit(42),
                name="x",
            ),
        ),
    )
    assert evaluate(ast, _make_scope(), _clock()) is False


def test_has_propagates_unbound_target_error() -> None:
    # ``has(bogus.x)`` \u2014 type_check rejects unknown identifiers at
    # compile time, so the only way to exercise the evaluator's
    # ``except UnboundNameError: raise`` arm in ``_eval_has`` is a
    # hand-crafted AST.
    ast = Call(
        pos=_POS,
        cel_type=BoolType(),
        function="has",
        args=(
            Member(
                pos=_POS,
                cel_type=None,
                target=Ident(pos=_POS, cel_type=None, name="bogus"),
                name="x",
            ),
        ),
    )
    with pytest.raises(UnboundNameError):
        evaluate(ast, _make_scope(), _clock())


# ---------------------------------------------------------------------------
# _celtype_name: each declared CEL type
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("cel_type", "expected_name"),
    [
        (IntType(), "int"),
        (UintType(), "uint"),
        (DoubleType(), "double"),
        (BoolType(), "bool"),
        (StringType(), "string"),
        (BytesType(), "bytes"),
        (NullType(), "null_type"),
        (TimestampType(), "google.protobuf.Timestamp"),
        (ListType(element=IntType()), "list"),
        (MapType(key=StringType(), value=StringType()), "map"),
    ],
)
def test_type_returns_celtype_name(cel_type: Any, expected_name: str) -> None:
    arg = Literal(pos=_POS, cel_type=cel_type, kind=LiteralKind.INT, value=0)
    ast = Call(pos=_POS, cel_type=StringType(), function="type", args=(arg,))
    assert evaluate(ast, _make_scope(), _clock()) == expected_name


# ---------------------------------------------------------------------------
# _runtime_type_name: each value shape via cel_type=None fallback
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected_name", "kind"),
    [
        (True, "bool", LiteralKind.BOOL),
        (7, "int", LiteralKind.INT),
        (1.5, "double", LiteralKind.DOUBLE),
        ("x", "string", LiteralKind.STRING),
        (b"x", "bytes", LiteralKind.BYTES),
        (None, "null_type", LiteralKind.NULL),
    ],
)
def test_type_runtime_fallback_per_value_shape(
    value: Any, expected_name: str, kind: LiteralKind
) -> None:
    arg = Literal(pos=_POS, cel_type=None, kind=kind, value=value)
    ast = Call(pos=_POS, cel_type=StringType(), function="type", args=(arg,))
    assert evaluate(ast, _make_scope(), _clock()) == expected_name


def test_type_runtime_fallback_for_map_literal_via_call_arg() -> None:
    arg = MapLit(pos=_POS, cel_type=None, entries=((_slit("a"), _ilit(1)),))
    ast = Call(pos=_POS, cel_type=StringType(), function="type", args=(arg,))
    assert evaluate(ast, _make_scope(), _clock()) == "map"


def test_type_runtime_fallback_for_datetime_value_via_now_call() -> None:
    # Reach the datetime branch in ``_runtime_type_name`` by handing
    # ``type`` a ``now()`` call whose ``cel_type`` is None.
    arg = Call(pos=_POS, cel_type=None, function="now", args=())
    ast = Call(pos=_POS, cel_type=StringType(), function="type", args=(arg,))
    assert evaluate(ast, _make_scope(), _clock()) == "google.protobuf.Timestamp"


# ---------------------------------------------------------------------------
# Conditional / Unary defensive paths
# ---------------------------------------------------------------------------


def test_ternary_condition_must_be_bool() -> None:
    ast = Conditional(
        pos=_POS,
        cel_type=IntType(),
        cond=_ilit(1),
        then_branch=_ilit(2),
        else_branch=_ilit(3),
    )
    with pytest.raises(EvalError, match="ternary condition must be bool"):
        evaluate(ast, _make_scope(), _clock())


def test_unary_not_requires_bool_operand() -> None:
    ast = Unary(pos=_POS, cel_type=BoolType(), op=UnaryOp.NOT, operand=_ilit(1))
    with pytest.raises(EvalError, match="bool operand"):
        evaluate(ast, _make_scope(), _clock())


def test_unknown_unary_op_should_be_exhaustive() -> None:
    # The pragma-no-cover branch — covered defensively to ensure
    # nobody widens UnaryOp without updating the evaluator.
    assert {op for op in UnaryOp} == {UnaryOp.NEG, UnaryOp.NOT}


# ---------------------------------------------------------------------------
# Binary operator defensive paths
# ---------------------------------------------------------------------------


def test_and_right_side_must_be_bool_when_left_is_true() -> None:
    ast = Binary(
        pos=_POS,
        cel_type=BoolType(),
        op=BinaryOp.AND,
        left=_blit(True),
        right=_ilit(1),
    )
    with pytest.raises(EvalError, match="'&&' requires bool"):
        evaluate(ast, _make_scope(), _clock())


def test_and_left_side_must_be_bool() -> None:
    ast = Binary(
        pos=_POS,
        cel_type=BoolType(),
        op=BinaryOp.AND,
        left=_ilit(1),
        right=_blit(True),
    )
    with pytest.raises(EvalError, match="'&&' requires bool"):
        evaluate(ast, _make_scope(), _clock())


def test_or_right_side_must_be_bool_when_left_is_false() -> None:
    ast = Binary(
        pos=_POS,
        cel_type=BoolType(),
        op=BinaryOp.OR,
        left=_blit(False),
        right=_ilit(1),
    )
    with pytest.raises(EvalError, match=r"'\|\|' requires bool"):
        evaluate(ast, _make_scope(), _clock())


def test_or_left_side_must_be_bool() -> None:
    ast = Binary(
        pos=_POS,
        cel_type=BoolType(),
        op=BinaryOp.OR,
        left=_ilit(1),
        right=_blit(True),
    )
    with pytest.raises(EvalError, match=r"'\|\|' requires bool"):
        evaluate(ast, _make_scope(), _clock())


def test_subtract_mixed_types_rejected() -> None:
    ast = Binary(
        pos=_POS,
        cel_type=IntType(),
        op=BinaryOp.SUB,
        left=_slit("a"),
        right=_slit("b"),
    )
    with pytest.raises(EvalError, match="'-' undefined"):
        evaluate(ast, _make_scope(), _clock())


def test_multiply_mixed_types_rejected() -> None:
    ast = Binary(
        pos=_POS,
        cel_type=IntType(),
        op=BinaryOp.MUL,
        left=_slit("a"),
        right=_ilit(2),
    )
    with pytest.raises(EvalError, match="'\\*' undefined"):
        evaluate(ast, _make_scope(), _clock())


def test_divide_mixed_types_rejected() -> None:
    ast = Binary(
        pos=_POS,
        cel_type=IntType(),
        op=BinaryOp.DIV,
        left=_slit("a"),
        right=_ilit(2),
    )
    with pytest.raises(EvalError, match="'/' undefined"):
        evaluate(ast, _make_scope(), _clock())


def test_modulo_mixed_types_rejected() -> None:
    ast = Binary(
        pos=_POS,
        cel_type=IntType(),
        op=BinaryOp.MOD,
        left=_slit("a"),
        right=_ilit(2),
    )
    with pytest.raises(EvalError, match="'%' undefined"):
        evaluate(ast, _make_scope(), _clock())


def test_bytes_concatenation_via_handcrafted_ast() -> None:
    # Reaches the bytes branch in ``_binary_add``.
    blit = Literal(pos=_POS, cel_type=BytesType(), kind=LiteralKind.BYTES, value=b"foo")
    blit2 = Literal(pos=_POS, cel_type=BytesType(), kind=LiteralKind.BYTES, value=b"bar")
    ast = Binary(pos=_POS, cel_type=BytesType(), op=BinaryOp.ADD, left=blit, right=blit2)
    assert evaluate(ast, _make_scope(), _clock()) == b"foobar"


def test_negative_integer_divmod_truncates_toward_zero() -> None:
    # Direct hand-crafted ASTs guarantee we reach ``_trunc_div``
    # with mixed-sign operands, exercising the ``q += 1`` branch.
    div_ast = Binary(
        pos=_POS,
        cel_type=IntType(),
        op=BinaryOp.DIV,
        left=_ilit(-7),
        right=_ilit(2),
    )
    assert evaluate(div_ast, _make_scope(), _clock()) == -3

    mod_ast = Binary(
        pos=_POS,
        cel_type=IntType(),
        op=BinaryOp.MOD,
        left=_ilit(-7),
        right=_ilit(2),
    )
    assert evaluate(mod_ast, _make_scope(), _clock()) == -1


def test_float_modulo_via_handcrafted_ast() -> None:
    flit = Literal(pos=_POS, cel_type=DoubleType(), kind=LiteralKind.DOUBLE, value=5.5)
    flit2 = Literal(pos=_POS, cel_type=DoubleType(), kind=LiteralKind.DOUBLE, value=2.0)
    ast = Binary(pos=_POS, cel_type=DoubleType(), op=BinaryOp.MOD, left=flit, right=flit2)
    assert evaluate(ast, _make_scope(), _clock()) == pytest.approx(1.5)


def test_float_modulo_by_zero() -> None:
    flit = Literal(pos=_POS, cel_type=DoubleType(), kind=LiteralKind.DOUBLE, value=5.5)
    zero = Literal(pos=_POS, cel_type=DoubleType(), kind=LiteralKind.DOUBLE, value=0.0)
    ast = Binary(pos=_POS, cel_type=DoubleType(), op=BinaryOp.MOD, left=flit, right=zero)
    with pytest.raises(EvalError, match="double modulo by zero"):
        evaluate(ast, _make_scope(), _clock())


# ---------------------------------------------------------------------------
# Equality / comparison defensive paths
# ---------------------------------------------------------------------------


def test_bool_equality_only_against_bool() -> None:
    # CEL: ``true == 1`` is false (strict typing) even though
    # Python evaluates ``True == 1`` to True.
    ast = Binary(
        pos=_POS,
        cel_type=BoolType(),
        op=BinaryOp.EQ,
        left=_blit(True),
        right=_ilit(1),
    )
    assert evaluate(ast, _make_scope(), _clock()) is False


def test_string_equals_string_returns_true() -> None:
    ast = Binary(
        pos=_POS,
        cel_type=BoolType(),
        op=BinaryOp.EQ,
        left=_slit("x"),
        right=_slit("x"),
    )
    assert evaluate(ast, _make_scope(), _clock()) is True


def test_string_vs_bytes_equality_false() -> None:
    blit = Literal(pos=_POS, cel_type=BytesType(), kind=LiteralKind.BYTES, value=b"x")
    ast = Binary(
        pos=_POS,
        cel_type=BoolType(),
        op=BinaryOp.EQ,
        left=_slit("x"),
        right=blit,
    )
    assert evaluate(ast, _make_scope(), _clock()) is False


def test_compare_mismatched_types_rejected() -> None:
    ast = Binary(
        pos=_POS,
        cel_type=BoolType(),
        op=BinaryOp.LT,
        left=_ilit(1),
        right=_slit("x"),
    )
    with pytest.raises(EvalError, match="same type"):
        evaluate(ast, _make_scope(), _clock())


def test_compare_uncomparable_type_rejected() -> None:
    # Two lists — same Python type, but lists are not in the
    # ``(int, float, str, bytes, datetime)`` comparable allow-list.
    list_lit = ListLit(
        pos=_POS,
        cel_type=ListType(element=IntType()),
        elements=(_ilit(1),),
    )
    ast = Binary(
        pos=_POS,
        cel_type=BoolType(),
        op=BinaryOp.LT,
        left=list_lit,
        right=list_lit,
    )
    with pytest.raises(EvalError, match="not defined for"):
        evaluate(ast, _make_scope(), _clock())


def test_in_with_non_collection_right_rejected() -> None:
    ast = Binary(
        pos=_POS,
        cel_type=BoolType(),
        op=BinaryOp.IN,
        left=_ilit(1),
        right=_ilit(2),
    )
    with pytest.raises(EvalError, match="'in' requires"):
        evaluate(ast, _make_scope(), _clock())
