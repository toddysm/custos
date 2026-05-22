"""Tests for :func:`custos_cel.parse` end-to-end.

These exercise the celpy -> internal AST converter on the design.md
canonical examples and on the operator categories called out by the
WF-IMPL-003 acceptance criteria (literal, ident, member chain, function
call, ternary, arithmetic, comparison, logical-and/or).
"""

from __future__ import annotations

import pytest

import custos_cel
from custos_cel import (
    Binary,
    BinaryOp,
    Call,
    CelConvertError,
    Conditional,
    Ident,
    Index,
    ListLit,
    Literal,
    LiteralKind,
    MapLit,
    Member,
    Unary,
    UnaryOp,
    from_json,
    to_json,
)


def test_parse_int_literal() -> None:
    n = custos_cel.parse("42")
    assert isinstance(n, Literal)
    assert n.kind is LiteralKind.INT
    assert n.value == 42


def test_parse_string_literal() -> None:
    n = custos_cel.parse('"hello"')
    assert isinstance(n, Literal)
    assert n.kind is LiteralKind.STRING
    assert n.value == "hello"


def test_parse_bool_literal() -> None:
    n = custos_cel.parse("true")
    assert isinstance(n, Literal)
    assert n.kind is LiteralKind.BOOL
    assert n.value is True


def test_parse_null_literal() -> None:
    n = custos_cel.parse("null")
    assert isinstance(n, Literal)
    assert n.kind is LiteralKind.NULL
    assert n.value is None


def test_parse_double_literal() -> None:
    n = custos_cel.parse("1.5")
    assert isinstance(n, Literal)
    assert n.kind is LiteralKind.DOUBLE
    assert n.value == 1.5


def test_parse_uint_literal_strips_suffix() -> None:
    n = custos_cel.parse("7u")
    assert isinstance(n, Literal)
    assert n.kind is LiteralKind.UINT
    assert n.value == 7


def test_parse_bare_ident() -> None:
    n = custos_cel.parse("foo")
    assert isinstance(n, Ident)
    assert n.name == "foo"


def test_parse_member_chain() -> None:
    n = custos_cel.parse("inputs.foo.bar")
    assert isinstance(n, Member)
    assert n.name == "bar"
    assert isinstance(n.target, Member)
    assert n.target.name == "foo"
    assert isinstance(n.target.target, Ident)
    assert n.target.target.name == "inputs"


def test_parse_index_with_string_key() -> None:
    # bracket form for hyphenated step ids (per change record 005)
    n = custos_cel.parse('steps["scan-alt"]')
    assert isinstance(n, Index)
    assert isinstance(n.target, Ident)
    assert n.target.name == "steps"
    assert isinstance(n.index, Literal)
    assert n.index.kind is LiteralKind.STRING
    assert n.index.value == "scan-alt"


def test_parse_canonical_design_example_uses_bracket_form() -> None:
    # The canonical design.md example with hyphenated step id, in the
    # bracket form Custos workflow definitions must use.
    src = 'steps["scan"].outputs.critical + steps["scan-alt"].outputs.critical'
    n = custos_cel.parse(src)
    assert isinstance(n, Binary)
    assert n.op is BinaryOp.ADD
    # Both operands are Member chains rooted in Index nodes.
    for side in (n.left, n.right):
        assert isinstance(side, Member)
        assert side.name == "critical"
        assert isinstance(side.target, Member)
        assert side.target.name == "outputs"
        assert isinstance(side.target.target, Index)


def test_parse_function_call_zero_args() -> None:
    n = custos_cel.parse("now()")
    assert isinstance(n, Call)
    assert n.function == "now"
    assert n.args == ()


def test_parse_function_call_with_args() -> None:
    n = custos_cel.parse("size(x)")
    assert isinstance(n, Call)
    assert n.function == "size"
    assert len(n.args) == 1
    assert isinstance(n.args[0], Ident)


def test_parse_has_macro_shape_is_call_node() -> None:
    # `has(...)` is parsed as a Call by celpy; the type checker
    # (WF-IMPL-005) is responsible for rejecting macros from the subset.
    n = custos_cel.parse("has(x)")
    assert isinstance(n, Call)
    assert n.function == "has"


def test_parse_ternary() -> None:
    n = custos_cel.parse("cond ? x : y")
    assert isinstance(n, Conditional)
    assert isinstance(n.cond, Ident) and n.cond.name == "cond"
    assert isinstance(n.then_branch, Ident) and n.then_branch.name == "x"
    assert isinstance(n.else_branch, Ident) and n.else_branch.name == "y"


def test_parse_arithmetic_precedence() -> None:
    # 1 + 2 * 3  ->  1 + (2 * 3)
    n = custos_cel.parse("1 + 2 * 3")
    assert isinstance(n, Binary)
    assert n.op is BinaryOp.ADD
    assert isinstance(n.left, Literal) and n.left.value == 1
    assert isinstance(n.right, Binary) and n.right.op is BinaryOp.MUL


def test_parse_subtraction_and_division() -> None:
    # 10 - 4 / 2  ->  10 - (4 / 2)
    n = custos_cel.parse("10 - 4 / 2")
    assert isinstance(n, Binary)
    assert n.op is BinaryOp.SUB
    assert isinstance(n.right, Binary) and n.right.op is BinaryOp.DIV


@pytest.mark.parametrize(
    ("source", "op"),
    [
        ("a == b", BinaryOp.EQ),
        ("a != b", BinaryOp.NE),
        ("a < b", BinaryOp.LT),
        ("a <= b", BinaryOp.LE),
        ("a > b", BinaryOp.GT),
        ("a >= b", BinaryOp.GE),
        ("a in b", BinaryOp.IN),
    ],
)
def test_parse_comparison(source: str, op: BinaryOp) -> None:
    n = custos_cel.parse(source)
    assert isinstance(n, Binary)
    assert n.op is op


def test_parse_logical_and_or_left_associative() -> None:
    # a && b || c  ->  (a && b) || c
    n = custos_cel.parse("a && b || c")
    assert isinstance(n, Binary)
    assert n.op is BinaryOp.OR
    assert isinstance(n.left, Binary) and n.left.op is BinaryOp.AND


def test_parse_unary_not() -> None:
    n = custos_cel.parse("!flag")
    assert isinstance(n, Unary)
    assert n.op is UnaryOp.NOT
    assert isinstance(n.operand, Ident) and n.operand.name == "flag"


def test_parse_unary_neg() -> None:
    n = custos_cel.parse("-x")
    assert isinstance(n, Unary)
    assert n.op is UnaryOp.NEG


def test_parse_list_literal() -> None:
    n = custos_cel.parse("[1, 2, 3]")
    assert isinstance(n, ListLit)
    assert len(n.elements) == 3
    assert all(isinstance(e, Literal) for e in n.elements)


def test_parse_map_literal() -> None:
    n = custos_cel.parse('{"k": 1, "j": 2}')
    assert isinstance(n, MapLit)
    assert len(n.entries) == 2
    for key, value in n.entries:
        assert isinstance(key, Literal) and key.kind is LiteralKind.STRING
        assert isinstance(value, Literal) and value.kind is LiteralKind.INT


def test_parse_paren_expression_collapses() -> None:
    # Parentheses are purely grouping; the AST should not carry a
    # paren node.
    n = custos_cel.parse("(1 + 2) * 3")
    assert isinstance(n, Binary)
    assert n.op is BinaryOp.MUL
    assert isinstance(n.left, Binary) and n.left.op is BinaryOp.ADD


def test_parse_attaches_source_positions() -> None:
    n = custos_cel.parse("inputs.foo + bar")
    assert isinstance(n, Binary)
    # The Binary node inherits position from its left child; we only
    # assert that *some* node in the tree carries a non-None line.
    found_line = False

    def walk(node: object) -> None:
        nonlocal found_line
        if hasattr(node, "pos") and getattr(node.pos, "line", None) is not None:
            found_line = True
        # walk known child fields without depending on Node internals
        for attr in (
            "left",
            "right",
            "target",
            "index",
            "operand",
            "cond",
            "then_branch",
            "else_branch",
        ):
            child = getattr(node, attr, None)
            if child is not None:
                walk(child)
        for collection_attr in ("args", "elements"):
            children = getattr(node, collection_attr, None)
            if children is not None:
                for c in children:
                    walk(c)

    walk(n)
    assert found_line, "expected at least one node to carry a source position"


def test_parse_round_trips_through_json() -> None:
    # Acceptance criterion: parse -> to_json -> from_json yields an equal tree.
    src = 'steps["scan"].outputs.critical + steps["scan-alt"].outputs.critical'
    n = custos_cel.parse(src)
    text = to_json(n)
    assert to_json(n) == text  # byte-stable across two invocations
    assert from_json(text) == n


def test_parse_rejects_method_call_syntax() -> None:
    # Custos's CEL subset disallows receiver.method(args). The converter
    # surfaces this as CelConvertError so workflow publish-time validation
    # has a typed signal to reject the expression.
    with pytest.raises(CelConvertError, match="method-call syntax"):
        custos_cel.parse('"abc".size()')
