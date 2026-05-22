"""Convert a celpy (cloud-custodian/cel-python) parse tree into our internal AST.

This module is private. The public entry point is :func:`custos_cel.parse`,
which calls :func:`convert_celpy_tree` on the output of
``celpy.Environment().compile(source)``.

The celpy grammar is left-recursive and full of pass-through productions
(``relation`` -> ``addition`` -> ``multiplication`` -> ``unary`` ->
``member`` -> ``primary``). The converter collapses those pass-throughs
and turns the binary-operator wrappers (``addition_add``,
``relation_eq``, ``member_dot``, etc.) into the corresponding
:class:`~custos_cel.ast.Binary` / :class:`~custos_cel.ast.Member` /
:class:`~custos_cel.ast.Index` / :class:`~custos_cel.ast.Unary` nodes.
"""

from __future__ import annotations

import ast as _py_ast
from collections.abc import Callable
from typing import Any

from custos_cel.ast import (
    Binary,
    BinaryOp,
    Call,
    Conditional,
    Ident,
    Index,
    ListLit,
    Literal,
    LiteralKind,
    MapLit,
    Member,
    Node,
    SourcePosition,
    Unary,
    UnaryOp,
)

# Mapping from celpy wrapper-rule names to our BinaryOp.
_BINARY_OP_BY_WRAPPER: dict[str, BinaryOp] = {
    "addition_add": BinaryOp.ADD,
    "addition_sub": BinaryOp.SUB,
    "multiplication_mul": BinaryOp.MUL,
    "multiplication_div": BinaryOp.DIV,
    "multiplication_mod": BinaryOp.MOD,
    "relation_eq": BinaryOp.EQ,
    "relation_ne": BinaryOp.NE,
    "relation_lt": BinaryOp.LT,
    "relation_le": BinaryOp.LE,
    "relation_gt": BinaryOp.GT,
    "relation_ge": BinaryOp.GE,
    "relation_in": BinaryOp.IN,
}

_UNARY_OP_BY_WRAPPER: dict[str, UnaryOp] = {
    "unary_not": UnaryOp.NOT,
    "unary_neg": UnaryOp.NEG,
}


class CelConvertError(ValueError):
    """Raised when the celpy parse tree contains a shape we do not handle."""


def convert_celpy_tree(tree: Any) -> Node:
    """Convert a celpy ``lark.Tree`` (output of ``Environment.compile``) to our AST."""
    return _convert(tree)


def _convert(node: Any) -> Node:
    if _is_token(node):
        raise CelConvertError(
            f"unexpected bare token at top of subtree: type={node.type!r} value={node.value!r}"
        )
    rule = node.data
    handler = _RULE_HANDLERS.get(rule)
    if handler is None:
        raise CelConvertError(f"unhandled celpy rule {rule!r}")
    return handler(node)


# ---------------------------------------------------------------------------
# Per-rule handlers
# ---------------------------------------------------------------------------


def _expr(node: Any) -> Node:
    # 1 child  -> pass through
    # 3 children -> ternary  cond ? then : else
    children = node.children
    if len(children) == 1:
        return _convert(children[0])
    if len(children) == 3:
        return Conditional(
            pos=_pos(node),
            cond=_convert(children[0]),
            then_branch=_convert(children[1]),
            else_branch=_convert(children[2]),
        )
    raise CelConvertError(f"expr: expected 1 or 3 children, got {len(children)}")


def _conditionalor(node: Any) -> Node:
    return _fold_flat_binary(node, BinaryOp.OR)


def _conditionaland(node: Any) -> Node:
    return _fold_flat_binary(node, BinaryOp.AND)


def _fold_flat_binary(node: Any, op: BinaryOp) -> Node:
    children = node.children
    if len(children) == 1:
        return _convert(children[0])
    # left-associative fold
    left = _convert(children[0])
    for rhs_tree in children[1:]:
        right = _convert(rhs_tree)
        left = Binary(pos=left.pos, op=op, left=left, right=right)
    return left


def _relation(node: Any) -> Node:
    return _wrapped_binary(node, "relation")


def _addition(node: Any) -> Node:
    return _wrapped_binary(node, "addition")


def _multiplication(node: Any) -> Node:
    return _wrapped_binary(node, "multiplication")


def _wrapped_binary(node: Any, rule: str) -> Node:
    """celpy shape for binary infix operators (relation, addition, multiplication):

      <rule>
        <op_wrapper>      # e.g. addition_add, relation_eq
          <inner>         # the LHS, same rule recursively
        <rhs>             # the RHS, the next tighter rule

    or single-child pass-through.
    """
    children = node.children
    if len(children) == 1:
        return _convert(children[0])
    if len(children) != 2:
        raise CelConvertError(f"{rule}: expected 1 or 2 children, got {len(children)}")
    op_wrapper, rhs_tree = children
    if _is_token(op_wrapper) or op_wrapper.data not in _BINARY_OP_BY_WRAPPER:
        raise CelConvertError(
            f"{rule}: expected operator wrapper as first child, got {_describe(op_wrapper)}"
        )
    op = _BINARY_OP_BY_WRAPPER[op_wrapper.data]
    inner_children = op_wrapper.children
    if len(inner_children) != 1:
        raise CelConvertError(
            f"{rule}: operator wrapper {op_wrapper.data!r} expected 1 child, "
            f"got {len(inner_children)}"
        )
    left = _convert(inner_children[0])
    right = _convert(rhs_tree)
    return Binary(pos=left.pos, op=op, left=left, right=right)


def _unary(node: Any) -> Node:
    """celpy shape for unary:
    single child -> pass through (no operator)
    two children -> first is unary_not / unary_neg marker (no children), second is operand
    """
    children = node.children
    if len(children) == 1:
        return _convert(children[0])
    if len(children) != 2:
        raise CelConvertError(f"unary: expected 1 or 2 children, got {len(children)}")
    marker, operand_tree = children
    if _is_token(marker) or marker.data not in _UNARY_OP_BY_WRAPPER:
        raise CelConvertError(
            f"unary: expected unary marker as first child, got {_describe(marker)}"
        )
    op = _UNARY_OP_BY_WRAPPER[marker.data]
    operand = _convert(operand_tree)
    return Unary(pos=_pos(node), op=op, operand=operand)


def _member(node: Any) -> Node:
    """``member`` is always a single-child wrapper around one of
    ``primary`` / ``member_dot`` / ``member_index`` / ``member_dot_arg`` /
    ``member_object``. The actual (target, name/index) pair lives inside
    those child rules — unlike ``addition`` / ``relation`` / ``multiplication``
    where the op-wrapper carries only the LHS and the RHS is a sibling.
    """
    children = node.children
    if len(children) != 1:
        raise CelConvertError(f"member: expected 1 child, got {len(children)}")
    return _convert(children[0])


def _member_dot(node: Any) -> Node:
    """``member . IDENT`` — two children: inner member tree and the IDENT token."""
    children = node.children
    if len(children) != 2:
        raise CelConvertError(f"member_dot: expected 2 children, got {len(children)}")
    target_tree, tail = children
    target = _convert(target_tree)
    if not _is_token(tail) or tail.type != "IDENT":
        raise CelConvertError(f"member_dot: expected IDENT tail, got {_describe(tail)}")
    return Member(pos=target.pos, target=target, name=str(tail.value))


def _member_index(node: Any) -> Node:
    """``member [ expr ]`` — two children: inner member tree and the index expr."""
    children = node.children
    if len(children) != 2:
        raise CelConvertError(f"member_index: expected 2 children, got {len(children)}")
    target_tree, index_tree = children
    target = _convert(target_tree)
    index = _convert(index_tree)
    return Index(pos=target.pos, target=target, index=index)


def _member_dot_arg(node: Any) -> Node:
    """Method-call form ``receiver.method(args)``. The Custos CEL subset
    disallows this construct; surface it as a typed converter error so
    publish-time validation has a structured signal to reject."""
    raise CelConvertError(
        "method-call syntax (receiver.method(...)) is not supported in the Custos CEL subset"
    )


def _member_object(node: Any) -> Node:
    """Protobuf message construction ``Type{...}``. Out of scope for Custos."""
    raise CelConvertError(
        "protobuf message construction (Type{...}) is not supported in the Custos CEL subset"
    )


def _expect_single(wrapper: Any, label: str) -> Any:
    if len(wrapper.children) != 1:
        raise CelConvertError(f"{label}: expected exactly 1 child, got {len(wrapper.children)}")
    return wrapper.children[0]


def _primary(node: Any) -> Node:
    children = node.children
    if len(children) != 1:
        raise CelConvertError(f"primary: expected 1 child, got {len(children)}")
    return _convert(children[0])


def _ident(node: Any) -> Node:
    tok = _expect_single(node, "ident")
    if not _is_token(tok) or tok.type != "IDENT":
        raise CelConvertError(f"ident: expected IDENT token, got {_describe(tok)}")
    return Ident(pos=_token_pos(tok), name=str(tok.value))


def _ident_arg(node: Any) -> Node:
    """``ident_arg`` is a bare function call: ``name(args...)``.

    celpy emits one or two children:
      - just the IDENT token for zero-arg calls (e.g. ``now()``);
      - the IDENT token plus an ``exprlist`` for one-or-more args.
    """
    children = node.children
    if len(children) == 0 or len(children) > 2:
        raise CelConvertError(f"ident_arg: expected 1 or 2 children, got {len(children)}")
    name_tok = children[0]
    if not _is_token(name_tok) or name_tok.type != "IDENT":
        raise CelConvertError(f"ident_arg: expected IDENT first child, got {_describe(name_tok)}")
    args: tuple[Node, ...] = ()
    if len(children) == 2:
        exprlist = children[1]
        if _is_token(exprlist) or exprlist.data != "exprlist":
            raise CelConvertError(
                f"ident_arg: expected exprlist second child, got {_describe(exprlist)}"
            )
        args = tuple(_convert(c) for c in exprlist.children)
    return Call(pos=_token_pos(name_tok), function=str(name_tok.value), args=args)


def _literal(node: Any) -> Node:
    tok = _expect_single(node, "literal")
    if not _is_token(tok):
        raise CelConvertError(f"literal: expected token child, got {_describe(tok)}")
    pos = _token_pos(tok)
    src = str(tok.value)
    if tok.type == "INT_LIT":
        return Literal(pos=pos, kind=LiteralKind.INT, value=_decode_int(src))
    if tok.type == "UINT_LIT":
        # Strip trailing u/U.
        return Literal(pos=pos, kind=LiteralKind.UINT, value=_decode_int(src.rstrip("uU")))
    if tok.type == "FLOAT_LIT":
        return Literal(pos=pos, kind=LiteralKind.DOUBLE, value=float(src))
    if tok.type == "STRING_LIT":
        return Literal(pos=pos, kind=LiteralKind.STRING, value=_decode_string(src))
    if tok.type == "BYTES_LIT":
        return Literal(pos=pos, kind=LiteralKind.BYTES, value=_decode_bytes(src))
    if tok.type == "BOOL_LIT":
        return Literal(pos=pos, kind=LiteralKind.BOOL, value=(src == "true"))
    if tok.type == "NULL_LIT":
        return Literal(pos=pos, kind=LiteralKind.NULL, value=None)
    raise CelConvertError(f"literal: unhandled token type {tok.type!r}")


def _decode_int(src: str) -> int:
    # int(..., 0) handles 0x / 0o / 0b prefixes as well as plain decimals.
    return int(src, 0)


def _decode_string(src: str) -> str:
    # CEL string escapes overlap with Python's; literal_eval is a safe
    # decoder (no code execution) that handles the common cases. CEL
    # peculiarities (raw strings with r"", triple-quoted strings) survive
    # because Python's literal_eval understands them too.
    value = _py_ast.literal_eval(src)
    if not isinstance(value, str):
        raise CelConvertError(f"STRING_LIT did not decode to str: {src!r}")
    return value


def _decode_bytes(src: str) -> bytes:
    value = _py_ast.literal_eval(src)
    if not isinstance(value, bytes):
        raise CelConvertError(f"BYTES_LIT did not decode to bytes: {src!r}")
    return value


def _list_lit(node: Any) -> Node:
    children = node.children
    if len(children) == 0:
        return ListLit(pos=_pos(node), elements=())
    if len(children) != 1:
        raise CelConvertError(f"list_lit: expected 0 or 1 children, got {len(children)}")
    exprlist = children[0]
    if _is_token(exprlist) or exprlist.data != "exprlist":
        raise CelConvertError(f"list_lit: expected exprlist, got {_describe(exprlist)}")
    return ListLit(
        pos=_pos(node),
        elements=tuple(_convert(c) for c in exprlist.children),
    )


def _map_lit(node: Any) -> Node:
    children = node.children
    if len(children) == 0:
        return MapLit(pos=_pos(node), entries=())
    if len(children) != 1:
        raise CelConvertError(f"map_lit: expected 0 or 1 children, got {len(children)}")
    mapinits = children[0]
    if _is_token(mapinits) or mapinits.data != "mapinits":
        raise CelConvertError(f"map_lit: expected mapinits, got {_describe(mapinits)}")
    if len(mapinits.children) % 2 != 0:
        raise CelConvertError(
            f"map_lit: expected even number of mapinits children, got {len(mapinits.children)}"
        )
    pairs: list[tuple[Node, Node]] = []
    for i in range(0, len(mapinits.children), 2):
        key = _convert(mapinits.children[i])
        value = _convert(mapinits.children[i + 1])
        pairs.append((key, value))
    return MapLit(pos=_pos(node), entries=tuple(pairs))


def _paren_expr(node: Any) -> Node:
    # celpy emits parenthesized exprs as a single-child tree.
    inner = _expect_single(node, "paren_expr")
    return _convert(inner)


_RULE_HANDLERS: dict[str, Callable[[Any], Node]] = {
    "expr": _expr,
    "conditionalor": _conditionalor,
    "conditionaland": _conditionaland,
    "relation": _relation,
    "addition": _addition,
    "multiplication": _multiplication,
    "unary": _unary,
    "member": _member,
    "member_dot": _member_dot,
    "member_index": _member_index,
    "member_dot_arg": _member_dot_arg,
    "member_object": _member_object,
    "primary": _primary,
    "ident": _ident,
    "ident_arg": _ident_arg,
    "literal": _literal,
    "list_lit": _list_lit,
    "map_lit": _map_lit,
    "paren_expr": _paren_expr,
}


# ---------------------------------------------------------------------------
# Position / inspection helpers
# ---------------------------------------------------------------------------


def _pos(node: Any) -> SourcePosition:
    meta = getattr(node, "meta", None)
    line = getattr(meta, "line", None) if meta is not None else None
    column = getattr(meta, "column", None) if meta is not None else None
    offset = getattr(meta, "start_pos", None) if meta is not None else None
    return SourcePosition(
        line=_coerce_pos(line),
        column=_coerce_pos(column),
        offset=_coerce_pos(offset),
    )


def _token_pos(tok: Any) -> SourcePosition:
    return SourcePosition(
        line=_coerce_pos(getattr(tok, "line", None)),
        column=_coerce_pos(getattr(tok, "column", None)),
        offset=_coerce_pos(getattr(tok, "start_pos", None)),
    )


def _coerce_pos(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    return None


def _is_token(node: Any) -> bool:
    # Lark Tokens carry a ``type`` attribute and no ``data`` / ``children``.
    return hasattr(node, "type") and not hasattr(node, "data")


def _describe(node: Any) -> str:
    if _is_token(node):
        return f"TOKEN(type={node.type!r}, value={node.value!r})"
    return f"TREE(data={getattr(node, 'data', '?')!r})"


# ``cast`` is only used implicitly via the registry above when subclasses are
# wired up; the public surface re-exports the converter under its real name.
__all__ = ["CelConvertError", "convert_celpy_tree"]
