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
    TimestampType,
    UintType,
    Unary,
    UnaryOp,
    from_dict,
    from_json,
    node_from_dict,
    to_json,
)
from custos_cel.clock import Clock, DaprWorkflowClock, FixedClock
from custos_cel.eval import EvalError
from custos_cel.eval import evaluate as _evaluate_impl
from custos_cel.scope import (
    BindingScope,
    BindingValue,
    RunInfo,
    StepBinding,
    UnboundNameError,
    WorkflowInfo,
)
from custos_cel.types import (
    SchemaBindings,
    TypeCheckError,
)
from custos_cel.types import (
    type_check as _type_check_impl,
)

__all__ = [
    "AST",
    "AST_SCHEMA_VERSION",
    "Binary",
    "BinaryOp",
    "BindingScope",
    "BindingValue",
    "BoolType",
    "BytesType",
    "Call",
    "CelConvertError",
    "CelType",
    "Clock",
    "Conditional",
    "DaprWorkflowClock",
    "DoubleType",
    "EvalError",
    "FixedClock",
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
    "RunInfo",
    "SchemaBindings",
    "SourcePosition",
    "StepBinding",
    "StringType",
    "TimestampType",
    "TypeCheckError",
    "TypedAST",
    "UintType",
    "Unary",
    "UnaryOp",
    "UnboundNameError",
    "WorkflowInfo",
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


def type_check(ast: Node, bindings: SchemaBindings) -> Node:
    """Type-check an :data:`AST` against JSON Schema bindings.

    Resolves every identifier against ``bindings`` and annotates each
    node with its inferred :class:`~custos_cel.ast.CelType`, producing a
    :data:`TypedAST`. The result is the only input shape accepted by
    :func:`evaluate`.

    Args:
        ast: An :data:`AST` produced by :func:`parse`.
        bindings: A :class:`SchemaBindings` describing every binding
            root visible to the expression (run inputs schema, ordered
            prior-step output schemas, declared ``let`` types, and the
            static types of ``run``, ``workflow``, and ``now()``).

    Returns:
        A :data:`TypedAST` — a fresh tree with the same structure as
        the input but with :attr:`Node.cel_type` populated on every
        node.

    Raises:
        TypeCheckError: For any type mismatch (operator-arity violation,
            ternary branch divergence, schema/value-type mismatch,
            unsupported language construct). Subclasses Python's
            :class:`TypeError`.
        UnboundNameError: For any identifier, step id, or schema field
            not declared in ``bindings``.
    """
    return _type_check_impl(ast, bindings)


def evaluate(ast: Node, scope: BindingScope, clock: Clock) -> Any:
    """Evaluate a :data:`TypedAST` against a binding scope and clock.

    Walks the type-checked tree, resolving every identifier through
    ``scope`` (whose strict root allow-list keeps the host Python
    namespace structurally unreachable) and routing every ``now()``
    call through ``clock`` (whose :class:`Clock` protocol guarantees a
    replay-deterministic wall-clock source).

    Args:
        ast: A :data:`TypedAST` produced by :func:`type_check`. Passing
            an untyped :data:`AST` directly from :func:`parse` is a
            programmer error.
        scope: A :class:`BindingScope` providing concrete values for
            ``inputs``, ``steps``, ``run``, ``workflow``, and ``let``.
        clock: A :class:`Clock` adapter — typically
            :class:`DaprWorkflowClock` in production or
            :class:`FixedClock` in tests.

    Returns:
        The expression's value (``bool`` / ``int`` / ``float`` /
        ``str`` / ``bytes`` / ``None`` / ``list`` / ``dict`` /
        :class:`datetime.datetime`).

    Raises:
        UnboundNameError: For any unresolved identifier or non-allow-
            listed function name.
        EvalError: For value-level runtime failures (division by zero,
            missing key on a runtime mapping, out-of-range list
            index, type-shape mismatches that escaped the type
            checker).
    """
    return _evaluate_impl(ast, scope, clock)
