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

import os
from typing import Any

import celpy
from celpy.celparser import CELParseError

from custos_cel import errors
from custos_cel._celpy_convert import CelConvertError, convert_celpy_tree
from custos_cel._telemetry import (
    count_nodes as _count_nodes,
)
from custos_cel._telemetry import (
    observe_evaluate as _observe_evaluate,
)
from custos_cel._telemetry import (
    observe_parse as _observe_parse,
)
from custos_cel._telemetry import (
    observe_type_check as _observe_type_check,
)
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
from custos_cel.errors import (
    CelError,
    DivergenceError,
    EvaluationError,
    ParseError,
)
from custos_cel.eval import DEFAULT_TIMEOUT_MS, EvalError, EvalTimeoutError
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

#: Environment variable consulted by :func:`evaluate` when the caller
#: passes ``timeout_ms=None``. Matches design.md § Configuration.
TIMEOUT_ENV_VAR = "WF_EXPR_TIMEOUT_MS"

__all__ = [
    "AST",
    "AST_SCHEMA_VERSION",
    "DEFAULT_TIMEOUT_MS",
    "TIMEOUT_ENV_VAR",
    "Binary",
    "BinaryOp",
    "BindingScope",
    "BindingValue",
    "BoolType",
    "BytesType",
    "Call",
    "CelConvertError",
    "CelError",
    "CelType",
    "Clock",
    "Conditional",
    "DaprWorkflowClock",
    "DivergenceError",
    "DoubleType",
    "EvalError",
    "EvalTimeoutError",
    "EvaluationError",
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
    "ParseError",
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
    "errors",
    "evaluate",
    "from_dict",
    "from_json",
    "node_from_dict",
    "parse",
    "to_json",
    "type_check",
]

__version__ = "0.1.1"

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
        ParseError: If ``source`` is not syntactically valid CEL or
            if the parse tree contains a construct outside the Custos
            subset (e.g. method-call syntax or protobuf message
            construction). The error always carries
            ``kind="expression.parse_error"`` per the WF-IMPL-008
            locked taxonomy; the ``CelConvertError`` subclass surface
            is preserved for callers that key off the narrower type.
    """
    env = celpy.Environment()
    with _observe_parse() as _span:
        # Gate descriptive span attributes behind ``is_recording()`` so
        # the no-SDK hot path (proxy / no-op spans) pays nothing for
        # attribute computation. This is the recommended OTel idiom
        # for instrumentation libraries whose attribute values aren't
        # free to derive (here, ``_count_nodes`` is an O(N) walk).
        recording = _span.is_recording()
        if recording:
            _span.set_attribute("custos_cel.source_length", len(source))
        try:
            tree = env.compile(source)
        except CELParseError as exc:
            # celpy raises its own ``CELParseError`` for lexer/parser
            # failures. Re-raise as our taxonomy ``ParseError`` so the
            # public surface only ever raises one error type at the parse
            # boundary; ``__cause__`` retains the celpy traceback for
            # debugging.
            raise ParseError(str(exc)) from exc
        node = convert_celpy_tree(tree)
        if recording:
            _span.set_attribute("custos_cel.node_count", _count_nodes(node))
    return node


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
    with _observe_type_check() as _span:
        # See ``parse`` above for why ``_count_nodes`` is gated.
        if _span.is_recording():
            _span.set_attribute("custos_cel.node_count", _count_nodes(ast))
        return _type_check_impl(ast, bindings)


def evaluate(
    ast: Node,
    scope: BindingScope,
    clock: Clock,
    *,
    timeout_ms: int | None = None,
) -> Any:
    """Evaluate a :data:`TypedAST` against a binding scope and clock.

    Walks the type-checked tree, resolving every identifier through
    ``scope`` (whose strict root allow-list keeps the host Python
    namespace structurally unreachable) and routing every ``now()``
    call through ``clock`` (whose :class:`Clock` protocol guarantees a
    replay-deterministic wall-clock source).

    A per-evaluation wall-clock deadline bounds the walk; the budget
    is sourced from (in order) ``timeout_ms`` if supplied, the
    ``WF_EXPR_TIMEOUT_MS`` environment variable, or
    :data:`DEFAULT_TIMEOUT_MS` (100 ms).

    Args:
        ast: A :data:`TypedAST` produced by :func:`type_check`. Passing
            an untyped :data:`AST` directly from :func:`parse` raises
            :class:`TypeError` (the root-level ``cel_type`` is checked
            up front; deeper untyped nodes that escape that check
            surface as :class:`EvalError` during the walk).
        scope: A :class:`BindingScope` providing concrete values for
            ``inputs``, ``steps``, ``run``, ``workflow``, and ``let``.
        clock: A :class:`Clock` adapter — typically
            :class:`DaprWorkflowClock` in production or
            :class:`FixedClock` in tests.
        timeout_ms: Per-evaluation wall-clock budget in milliseconds.
            ``None`` (the default) consults ``WF_EXPR_TIMEOUT_MS``;
            ``0`` disables the gate; negative values are rejected.

    Returns:
        The expression's value (``bool`` / ``int`` / ``float`` /
        ``str`` / ``bytes`` / ``None`` / ``list`` / ``dict`` /
        :class:`datetime.datetime`).

    Raises:
        TypeError: If ``scope`` is not a :class:`BindingScope`, if
            ``clock`` does not satisfy the :class:`Clock` protocol, if
            ``ast`` is not a :class:`Node` whose root carries a
            populated ``cel_type``, or if the resolved ``timeout_ms``
            is not an :class:`int`.
        ValueError: If ``timeout_ms`` (after env-var resolution) is
            negative, or if ``WF_EXPR_TIMEOUT_MS`` is set to a value
            that cannot be parsed as an integer.
        UnboundNameError: For any unresolved identifier or non-allow-
            listed function name.
        EvalError: For value-level runtime failures (division by zero,
            missing key on a runtime mapping, out-of-range list
            index, type-shape mismatches that escaped the type
            checker).
        EvalTimeoutError: If the walk's wall-clock elapsed time
            exceeds the resolved budget.
    """
    if timeout_ms is None:
        timeout_ms = _resolve_timeout_from_env()
    with _observe_evaluate() as _span:
        # Gate attribute computation on ``is_recording()`` — no-op
        # spans see no attributes and pay no ``_count_nodes`` cost,
        # which dominates the per-call overhead on small ASTs.
        if _span.is_recording():
            if isinstance(ast, Node):
                # ``count_nodes`` walks the (already-validated) typed
                # AST, so it never raises on the values the public
                # API accepts; an untyped node here would surface
                # from ``_evaluate_impl`` as its usual ``TypeError``
                # and still be timed under the ``internal_error``
                # outcome bucket.
                _span.set_attribute("custos_cel.node_count", _count_nodes(ast))
            _span.set_attribute("custos_cel.timeout_ms", timeout_ms)
        return _evaluate_impl(ast, scope, clock, timeout_ms=timeout_ms)


def _resolve_timeout_from_env() -> int:
    """Resolve the default per-evaluation timeout from the environment.

    Returns ``WF_EXPR_TIMEOUT_MS`` as an :class:`int` if set; otherwise
    :data:`DEFAULT_TIMEOUT_MS`. A non-integer setting raises
    :class:`ValueError` so misconfiguration fails loudly at the first
    evaluation rather than silently falling back to the default.
    """
    raw = os.environ.get(TIMEOUT_ENV_VAR)
    if raw is None:
        return DEFAULT_TIMEOUT_MS
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{TIMEOUT_ENV_VAR}={raw!r} is not a valid integer") from exc
