"""Custos sandboxed CEL-like expression evaluator.

This package hosts the parser, type checker, and replay-deterministic runtime
for workflow expressions used by the Workflow Service Step Coordinator and by
the Catalog Service publish-time validator (parser half only).

See the design at:
https://github.com/toddysm/custos/blob/main/design/components/workflow-service/design.md
(§ Expression Evaluator / ADR-011).

The chosen parser/runtime is `cel-python <https://github.com/cloud-custodian/cel-python>`_
(import name ``celpy``). See change record
``design/components/workflow-service/changes/2026-05-21-005-cel-parser-choice.md``.

The AST data model lands in WF-IMPL-003 (this module re-exports the
node classes from :mod:`custos_cel.ast`). The type checker and evaluator
remain stubs until WF-IMPL-005 / WF-IMPL-006.
"""

from __future__ import annotations

from typing import Any

import celpy

from custos_cel._celpy_convert import CelConvertError, convert_celpy_tree
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
    Node,
    NullType,
    SourcePosition,
    StringType,
    UintType,
    Unary,
    UnaryOp,
    from_dict,
    from_json,
    node_from_dict,
    to_json,
)

__all__ = [
    "AST",
    "AST_SCHEMA_VERSION",
    "Binary",
    "BinaryOp",
    "BoolType",
    "BytesType",
    "Call",
    "CelConvertError",
    "CelType",
    "Conditional",
    "DoubleType",
    "Ident",
    "Index",
    "IntType",
    "ListLit",
    "ListType",
    "Literal",
    "LiteralKind",
    "MapLit",
    "MapType",
    "Member",
    "Node",
    "NullType",
    "SourcePosition",
    "StringType",
    "TypedAST",
    "UintType",
    "Unary",
    "UnaryOp",
    "__version__",
    "evaluate",
    "from_dict",
    "from_json",
    "node_from_dict",
    "parse",
    "to_json",
    "type_check",
]

__version__ = "0.1.0"

# Public type aliases.
#
# Both names resolve to :class:`custos_cel.ast.Node` today — the same Python
# class represents the structural (untyped) tree returned by :func:`parse`
# and the type-annotated tree produced by :func:`type_check`. The
# distinction is carried per-node in :attr:`Node.cel_type`: ``None`` for an
# untyped AST, populated everywhere for a TypedAST. The runtime invariant
# is asserted by the type checker and evaluator (WF-IMPL-005 / WF-IMPL-006).
AST = Node
TypedAST = Node


def parse(source: str) -> Node:
    """Parse a CEL expression into an untyped AST.

    Delegates the lexer/parser to ``celpy`` and converts its parse tree
    into the internal :class:`~custos_cel.ast.Node` shape. The returned
    tree carries source positions but **no** resolved types and **no**
    binding information — use :func:`type_check` to lift it to a
    :data:`TypedAST` before handing it to :func:`evaluate`.

    Args:
        source: The CEL expression source text.

    Returns:
        The root :class:`~custos_cel.ast.Node` of the parsed tree.

    Raises:
        celpy.celparser.CELParseError: If ``source`` is not syntactically
            valid CEL.
        CelConvertError: If the parse tree contains a CEL construct that
            is outside the Custos subset (e.g. method-call syntax or
            protobuf message construction).
    """
    env = celpy.Environment()
    tree = env.compile(source)
    return convert_celpy_tree(tree)


def type_check(ast: Node, bindings: Any) -> Node:
    """Type-check an :data:`AST` against JSON Schema bindings.

    Resolves every identifier against ``bindings`` and annotates each
    node with its inferred :class:`~custos_cel.ast.CelType`, producing a
    :data:`TypedAST`. The result is the only input shape accepted by
    :func:`evaluate`.

    Args:
        ast: An :data:`AST` produced by :func:`parse`. Must be untyped —
            i.e. ``ast.cel_type is None`` at the root. Re-checking a
            typed tree is a usage error and will be rejected once
            WF-IMPL-005 lands.
        bindings: The binding scope describing available identifiers and
            their JSON Schemas.

    Returns:
        A :data:`TypedAST` — a fresh tree with the same structure as the
        input but with :attr:`Node.cel_type` populated on every node.

    Raises:
        NotImplementedError: Always. Implementation lands in WF-IMPL-005.
    """
    raise NotImplementedError("custos_cel.type_check is not yet implemented; see WF-IMPL-005.")


def evaluate(ast: Node, bindings: Any) -> Any:
    """Evaluate a :data:`TypedAST` against a binding scope.

    Requires a :data:`TypedAST` (the output of :func:`type_check`).
    Passing an untyped :data:`AST` directly from :func:`parse` is a
    usage error and will be rejected once WF-IMPL-006 lands.

    Args:
        ast: A :data:`TypedAST` produced by :func:`type_check`.
        bindings: The binding scope providing concrete values.

    Returns:
        The evaluated result. Concrete type defined in WF-IMPL-006.

    Raises:
        NotImplementedError: Always. Implementation lands in WF-IMPL-006.
    """
    raise NotImplementedError("custos_cel.evaluate is not yet implemented; see WF-IMPL-006.")
