"""Unit tests for the :mod:`custos_cel.ast` data model.

Covers:
* Direct construction of every node type.
* :class:`CelType` hierarchy construction and serialization.
* Byte-stable JSON round-trip via :func:`custos_cel.to_json` /
  :func:`custos_cel.from_json` (acceptance criterion for WF-IMPL-003).
* Schema-version envelope.
"""

from __future__ import annotations

import json

import pytest

from custos_cel.ast import (
    AST_SCHEMA_VERSION,
    Binary,
    BinaryOp,
    BoolType,
    BytesType,
    Call,
    CelType,
    Conditional,
    DoubleType,
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
    SourcePosition,
    StringType,
    UintType,
    Unary,
    UnaryOp,
    from_dict,
    from_json,
    node_from_dict,
    to_dict_envelope,
    to_json,
)

# ---------------------------------------------------------------------------
# Source positions
# ---------------------------------------------------------------------------


def test_source_position_round_trip() -> None:
    pos = SourcePosition(line=3, column=5, offset=42)
    assert SourcePosition.from_dict(pos.to_dict()) == pos


def test_source_position_allows_none_fields() -> None:
    pos = SourcePosition()
    d = pos.to_dict()
    assert d == {"column": None, "line": None, "offset": None}
    assert SourcePosition.from_dict(d) == pos


def test_source_position_rejects_bool() -> None:
    with pytest.raises(TypeError):
        SourcePosition.from_dict({"line": True, "column": 1, "offset": 0})


# ---------------------------------------------------------------------------
# CelType hierarchy
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "cel_type",
    [
        IntType(),
        UintType(),
        DoubleType(),
        BoolType(),
        StringType(),
        BytesType(),
        NullType(),
        ListType(element=IntType()),
        MapType(key=StringType(), value=ListType(element=IntType())),
    ],
)
def test_cel_type_round_trip(cel_type: CelType) -> None:
    assert CelType.from_dict(cel_type.to_dict()) == cel_type


def test_cel_type_from_dict_rejects_unknown_kind() -> None:
    with pytest.raises(ValueError, match="unknown type kind"):
        CelType.from_dict({"kind": "no_such_type"})


def test_cel_type_from_dict_rejects_missing_kind() -> None:
    with pytest.raises(ValueError, match="missing or non-string"):
        CelType.from_dict({})


# ---------------------------------------------------------------------------
# Node construction + dict round-trip
# ---------------------------------------------------------------------------

_POS = SourcePosition(line=1, column=1, offset=0)


def _ident(name: str) -> Ident:
    return Ident(pos=_POS, name=name)


def test_literal_int_round_trip() -> None:
    n = Literal(pos=_POS, kind=LiteralKind.INT, value=42)
    assert node_from_dict(n.to_dict()) == n


def test_literal_string_round_trip() -> None:
    n = Literal(pos=_POS, kind=LiteralKind.STRING, value="hello")
    assert node_from_dict(n.to_dict()) == n


def test_literal_bytes_round_trip_uses_hex() -> None:
    n = Literal(pos=_POS, kind=LiteralKind.BYTES, value=b"\x00\xff")
    d = n.to_dict()
    assert d["value"] == "00ff"  # hex encoding so the dict stays JSON-safe
    assert node_from_dict(d) == n


def test_literal_bool_round_trip() -> None:
    for value in (True, False):
        n = Literal(pos=_POS, kind=LiteralKind.BOOL, value=value)
        assert node_from_dict(n.to_dict()) == n


def test_literal_null_round_trip() -> None:
    n = Literal(pos=_POS, kind=LiteralKind.NULL, value=None)
    assert node_from_dict(n.to_dict()) == n


def test_literal_double_round_trip() -> None:
    n = Literal(pos=_POS, kind=LiteralKind.DOUBLE, value=1.5)
    assert node_from_dict(n.to_dict()) == n


def test_ident_round_trip() -> None:
    n = _ident("foo")
    assert node_from_dict(n.to_dict()) == n


def test_member_chain_round_trip() -> None:
    n = Member(
        pos=_POS,
        target=Member(pos=_POS, target=_ident("a"), name="b"),
        name="c",
    )
    assert node_from_dict(n.to_dict()) == n


def test_index_round_trip() -> None:
    n = Index(
        pos=_POS,
        target=_ident("a"),
        index=Literal(pos=_POS, kind=LiteralKind.STRING, value="k"),
    )
    assert node_from_dict(n.to_dict()) == n


def test_call_round_trip() -> None:
    n = Call(
        pos=_POS,
        function="now",
        args=(),
    )
    assert node_from_dict(n.to_dict()) == n

    n_with_args = Call(
        pos=_POS,
        function="size",
        args=(_ident("x"),),
    )
    assert node_from_dict(n_with_args.to_dict()) == n_with_args


def test_conditional_round_trip() -> None:
    n = Conditional(
        pos=_POS,
        cond=_ident("cond"),
        then_branch=_ident("x"),
        else_branch=_ident("y"),
    )
    assert node_from_dict(n.to_dict()) == n


def test_binary_round_trip_all_ops() -> None:
    for op in BinaryOp:
        n = Binary(pos=_POS, op=op, left=_ident("a"), right=_ident("b"))
        assert node_from_dict(n.to_dict()) == n


def test_unary_round_trip_all_ops() -> None:
    for op in UnaryOp:
        n = Unary(pos=_POS, op=op, operand=_ident("x"))
        assert node_from_dict(n.to_dict()) == n


def test_list_lit_round_trip() -> None:
    n = ListLit(
        pos=_POS,
        elements=(
            Literal(pos=_POS, kind=LiteralKind.INT, value=1),
            Literal(pos=_POS, kind=LiteralKind.INT, value=2),
        ),
    )
    assert node_from_dict(n.to_dict()) == n


def test_map_lit_round_trip() -> None:
    n = MapLit(
        pos=_POS,
        entries=(
            (
                Literal(pos=_POS, kind=LiteralKind.STRING, value="k"),
                Literal(pos=_POS, kind=LiteralKind.INT, value=1),
            ),
        ),
    )
    assert node_from_dict(n.to_dict()) == n


def test_node_with_cel_type_round_trip() -> None:
    n = Ident(pos=_POS, cel_type=IntType(), name="x")
    d = n.to_dict()
    assert d["cel_type"] == {"kind": "int"}
    assert node_from_dict(d) == n


# ---------------------------------------------------------------------------
# JSON envelope + byte-stable serialization
# ---------------------------------------------------------------------------


def test_to_dict_envelope_carries_schema_version() -> None:
    n = _ident("foo")
    env = to_dict_envelope(n)
    assert env["schema_version"] == AST_SCHEMA_VERSION
    assert env["root"] == n.to_dict()


def test_json_round_trip_is_byte_stable() -> None:
    # Build a tree that exercises every node shape.
    n = Conditional(
        pos=_POS,
        cond=Binary(
            pos=_POS,
            op=BinaryOp.AND,
            left=Binary(pos=_POS, op=BinaryOp.EQ, left=_ident("a"), right=_ident("b")),
            right=Unary(pos=_POS, op=UnaryOp.NOT, operand=_ident("flag")),
        ),
        then_branch=Member(
            pos=_POS,
            target=Index(
                pos=_POS,
                target=_ident("steps"),
                index=Literal(pos=_POS, kind=LiteralKind.STRING, value="scan-alt"),
            ),
            name="critical",
        ),
        else_branch=Call(
            pos=_POS,
            function="size",
            args=(
                ListLit(
                    pos=_POS,
                    elements=(
                        Literal(pos=_POS, kind=LiteralKind.INT, value=1),
                        Literal(pos=_POS, kind=LiteralKind.INT, value=2),
                    ),
                ),
            ),
        ),
    )

    s1 = to_json(n)
    s2 = to_json(n)
    assert s1 == s2  # byte-stable across two invocations

    # Round-trip preserves structural equality.
    n_back = from_json(s1)
    assert n_back == n

    # Re-serializing the round-tripped node yields the same bytes.
    assert to_json(n_back) == s1


def test_from_dict_rejects_wrong_schema_version() -> None:
    n = _ident("foo")
    env = to_dict_envelope(n)
    env["schema_version"] = 999
    with pytest.raises(ValueError, match="unsupported schema version"):
        from_dict(env)


def test_node_from_dict_rejects_unknown_kind() -> None:
    with pytest.raises(ValueError, match="unknown node kind"):
        node_from_dict({"node": "NotARealKind", "pos": _POS.to_dict()})


def test_node_from_dict_rejects_missing_pos() -> None:
    # A missing `pos` key must be a hard error so corrupted/partial cache
    # payloads cannot silently deserialize as SourcePosition(None, None, None).
    with pytest.raises(ValueError, match="missing 'pos'"):
        node_from_dict({"node": "Ident", "name": "x"})


def test_node_from_dict_rejects_non_mapping_pos() -> None:
    with pytest.raises(ValueError, match="'pos' must be a mapping"):
        node_from_dict({"node": "Ident", "name": "x", "pos": "not-a-mapping"})


def test_from_json_handles_canonical_form() -> None:
    n = _ident("foo")
    s = to_json(n)
    # canonical form: keys sorted, separators compact
    assert s == json.dumps(json.loads(s), sort_keys=True, separators=(",", ":"))
