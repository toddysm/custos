"""Sandboxed evaluator runtime for Custos CEL expressions (WF-IMPL-006).

This module implements :func:`evaluate`, which walks a TypedAST (the
output of :func:`custos_cel.type_check`) against a
:class:`~custos_cel.scope.BindingScope` and a
:class:`~custos_cel.clock.Clock`, and returns the expression's value.
The walk is pure-Python and side-effect-free aside from the single
:meth:`Clock.now <custos_cel.clock.Clock.now>` invocation that powers
the ``now()`` builtin.

Sandbox guarantees (per design.md § Expression Evaluator, ADR-011):

* Resolution of every identifier goes through the supplied
  :class:`BindingScope`, whose root allow-list (``inputs``, ``steps``,
  ``run``, ``workflow``, ``let``, ``now``) is enforced before any host
  attribute access occurs. The host Python namespace is structurally
  unreachable.
* No ``os``, ``sys``, ``subprocess``, ``socket``, ``importlib``,
  ``open``, ``__import__``, ``eval``, ``exec`` imports anywhere in
  this module. A ``grep`` for those names returns zero hits.
* Function dispatch uses a fixed allow-list (``now``, ``size``, ``has``,
  ``type``); any other ``Call.function`` raises
  :class:`~custos_cel.UnboundNameError`.
* The single clock observation is delegated to the
  :class:`Clock` adapter, so the production wiring (Dapr Workflow
  ``current_utc_datetime``) is replay-deterministic and the test
  wiring (:class:`~custos_cel.clock.FixedClock`) is byte-deterministic.

See the issue: https://github.com/toddysm/custos/issues/181
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import datetime
from typing import Any, Final

from custos_cel.ast import (
    Binary,
    BinaryOp,
    BoolType,
    BytesType,
    Call,
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
)
from custos_cel.clock import Clock
from custos_cel.scope import BindingScope, UnboundNameError

__all__ = ["EvalError", "evaluate"]


# Root identifiers resolvable via :meth:`BindingScope.resolve`. Kept in
# sync with the scope's own allow-list — duplicated here so the
# chain-collapse logic does not have to import the scope's private set.
_BINDING_ROOTS: Final[frozenset[str]] = frozenset(
    {"inputs", "steps", "run", "workflow", "let", "now"}
)


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class EvalError(RuntimeError):
    """Runtime evaluation error in a CEL expression.

    Raised for division-by-zero, modulo-by-zero, out-of-range index,
    missing map key on a runtime value, and similar value-level
    failures. Type-shape mismatches that should have been caught by the
    type checker are also surfaced as :class:`EvalError` if they reach
    the evaluator (which would indicate a bug in either the type
    checker or the caller's bindings).

    Carries the source position of the offending node so the Step
    Coordinator can emit a structured ``expression.eval_error`` audit
    event per the WF-IMPL-008 error taxonomy.
    """

    KIND: Final[str] = "expression.eval_error"

    def __init__(
        self,
        message: str,
        *,
        source_position: SourcePosition | None = None,
    ) -> None:
        super().__init__(message)
        self.kind: str = self.KIND
        self.message: str = message
        self.source_position: SourcePosition | None = source_position


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def evaluate(typed_ast: Node, scope: BindingScope, clock: Clock) -> Any:
    """Evaluate a TypedAST against a binding scope.

    Args:
        typed_ast: A TypedAST node — the output of
            :func:`custos_cel.type_check`. Every node must carry a
            :attr:`~custos_cel.ast.Node.cel_type`; passing an untyped
            AST is a programmer error.
        scope: The :class:`BindingScope` providing values for every
            binding root the expression references.
        clock: The :class:`Clock` adapter that powers ``now()``. Must
            satisfy the :class:`Clock` protocol; production uses
            :class:`~custos_cel.clock.DaprWorkflowClock`, tests use
            :class:`~custos_cel.clock.FixedClock`.

    Returns:
        The expression's value. Booleans, integers (positive and
        negative), floats, strings, bytes, ``None``, lists, dicts, and
        :class:`datetime.datetime` instances are the possible concrete
        types — matching the CEL value shapes inferred by the type
        checker.

    Raises:
        UnboundNameError: If the expression references an identifier
            (or function name) that is not resolvable in ``scope`` or
            in the function allow-list.
        EvalError: For any value-level runtime failure (division by
            zero, missing key on a runtime mapping, out-of-range list
            index, unsupported runtime type for an operator).
    """
    if not isinstance(scope, BindingScope):
        raise TypeError("evaluate: 'scope' must be a BindingScope; got " + type(scope).__name__)
    if not isinstance(clock, Clock):
        raise TypeError(
            "evaluate: 'clock' must satisfy the Clock protocol; got " + type(clock).__name__
        )
    return _eval(typed_ast, scope, clock)


# ---------------------------------------------------------------------------
# Recursive walker
# ---------------------------------------------------------------------------


def _eval(node: Node, scope: BindingScope, clock: Clock) -> Any:
    if isinstance(node, Literal):
        return _eval_literal(node)
    if isinstance(node, Ident):
        return _eval_ident(node, scope)
    if isinstance(node, Member):
        return _eval_member_or_index(node, scope, clock)
    if isinstance(node, Index):
        return _eval_member_or_index(node, scope, clock)
    if isinstance(node, Call):
        return _eval_call(node, scope, clock)
    if isinstance(node, Conditional):
        return _eval_conditional(node, scope, clock)
    if isinstance(node, Binary):
        return _eval_binary(node, scope, clock)
    if isinstance(node, Unary):
        return _eval_unary(node, scope, clock)
    if isinstance(node, ListLit):
        return [_eval(e, scope, clock) for e in node.elements]
    if isinstance(node, MapLit):
        return {_eval(k, scope, clock): _eval(v, scope, clock) for k, v in node.entries}
    raise EvalError(  # pragma: no cover - exhaustive
        f"internal: unhandled node type {type(node).__name__}",
        source_position=getattr(node, "pos", None),
    )


# ---------------------------------------------------------------------------
# Literal / Ident
# ---------------------------------------------------------------------------


def _eval_literal(node: Literal) -> Any:
    if node.kind is LiteralKind.NULL:
        return None
    return node.value


def _eval_ident(node: Ident, scope: BindingScope) -> Any:
    # Bare identifier: the only meaningful case is ``now`` (the bound
    # clock callable). Every other binding root is a record, not a
    # value, and is rejected by ``BindingScope.resolve`` — surface that
    # rejection unchanged so the error message stays consistent with
    # dotted-chain failures.
    return scope.resolve([node.name], pos=node.pos)


# ---------------------------------------------------------------------------
# Member / Index chains
# ---------------------------------------------------------------------------


def _eval_member_or_index(node: Node, scope: BindingScope, clock: Clock) -> Any:
    """Resolve a Member/Index chain.

    Walks down to the chain's root, accumulating accessors. Each
    accessor is either:

    * ``("static", name)`` — a ``Member.name`` or a string-literal
      ``Index.index``, i.e. a key known at compile time. These collapse
      into a single :meth:`BindingScope.resolve` call when the root is
      a binding identifier.
    * ``("dynamic", value)`` — an ``Index.index`` that evaluates to a
      runtime value (typically an int for list access or a non-literal
      string for map access).

    The strategy:

    1. If the chain's root is a binding identifier, hand the longest
       prefix of static accessors to :meth:`BindingScope.resolve` so
       the scope's strict allow-list runs against the static portion.
    2. Apply any remaining accessors (static *or* dynamic) at runtime
       against the resolved value.
    3. If the chain's root is some other expression (a literal, a
       call, an operator result), evaluate that root, then apply all
       accessors at runtime.
    """
    accessors: list[tuple[str, Any]] = []
    cur: Node = node
    while isinstance(cur, (Member, Index)):
        if isinstance(cur, Member):
            accessors.append(("static", cur.name))
            cur = cur.target
        else:  # Index
            if (
                isinstance(cur.index, Literal)
                and cur.index.kind is LiteralKind.STRING
                and isinstance(cur.index.value, str)
            ):
                accessors.append(("static", cur.index.value))
            else:
                accessors.append(("dynamic", _eval(cur.index, scope, clock)))
            cur = cur.target
    accessors.reverse()

    if isinstance(cur, Ident) and cur.name in _BINDING_ROOTS:
        static_count = 0
        for kind, _ in accessors:
            if kind == "static":
                static_count += 1
            else:
                break
        chain: list[str] = [cur.name, *(v for _, v in accessors[:static_count])]
        value = scope.resolve(chain, pos=node.pos)
        for kind, key in accessors[static_count:]:
            value = _runtime_access(value, key, kind, node.pos)
        return value

    value = _eval(cur, scope, clock)
    for kind, key in accessors:
        value = _runtime_access(value, key, kind, node.pos)
    return value


def _runtime_access(target: Any, key: Any, kind: str, pos: SourcePosition | None) -> Any:
    """Apply one accessor to a runtime value.

    ``kind`` is ``"static"`` (compile-time-known key, typically a
    string) or ``"dynamic"`` (computed key, typically an int for lists
    or a string for maps).
    """
    # Mapping covers both dict and MappingProxyType.
    if isinstance(target, Mapping):
        if key in target:
            return target[key]
        raise EvalError(
            f"missing key {key!r} in mapping",
            source_position=pos,
        )
    # Distinguish "sequence" (list, tuple) from "string-like" (str,
    # bytes) — both satisfy Sequence but member-style access on
    # strings is not part of the CEL subset.
    if isinstance(target, (list, tuple)) and not isinstance(target, (str, bytes)):
        if not isinstance(key, int) or isinstance(key, bool):
            raise EvalError(
                f"list index must be int, got {type(key).__name__}",
                source_position=pos,
            )
        if key < 0 or key >= len(target):
            raise EvalError(
                f"list index {key} out of range (size {len(target)})",
                source_position=pos,
            )
        return target[key]
    raise EvalError(
        f"cannot access {key!r} on value of type {type(target).__name__}",
        source_position=pos,
    )


# ---------------------------------------------------------------------------
# Call dispatch (fixed allow-list)
# ---------------------------------------------------------------------------


def _eval_call(node: Call, scope: BindingScope, clock: Clock) -> Any:
    name = node.function
    if name == "now":
        return _eval_now(node, clock)
    if name == "size":
        return _eval_size(node, scope, clock)
    if name == "type":
        return _eval_type(node, scope, clock)
    if name == "has":
        return _eval_has(node, scope, clock)
    # Anything else — including any host Python name the parser might
    # have surfaced — is rejected by the same UnboundNameError shape
    # the scope produces for unknown identifiers. This is what makes
    # ``open()``, ``__import__()``, ``eval()``, ``exec()`` structurally
    # unreachable from inside an expression.
    raise UnboundNameError(
        [name],
        pos=node.pos,
        reason=f"function {name!r} is not in the evaluator allow-list",
    )


def _eval_now(node: Call, clock: Clock) -> datetime:
    if node.args:
        raise EvalError(
            f"'now' takes no arguments, got {len(node.args)}",
            source_position=node.pos,
        )
    return clock.now()


def _eval_size(node: Call, scope: BindingScope, clock: Clock) -> int:
    if len(node.args) != 1:
        raise EvalError(
            f"'size' takes exactly one argument, got {len(node.args)}",
            source_position=node.pos,
        )
    value = _eval(node.args[0], scope, clock)
    if isinstance(value, (str, bytes)):
        return len(value)
    if isinstance(value, (list, tuple)):
        return len(value)
    if isinstance(value, Mapping):
        return len(value)
    raise EvalError(
        f"'size' is undefined for value of type {type(value).__name__}",
        source_position=node.pos,
    )


def _eval_type(node: Call, scope: BindingScope, clock: Clock) -> str:
    if len(node.args) != 1:
        raise EvalError(
            f"'type' takes exactly one argument, got {len(node.args)}",
            source_position=node.pos,
        )
    arg = node.args[0]
    # Prefer the static type from the TypedAST when present — that's
    # what CEL semantics require for ``type(x)`` (the *declared* type,
    # not a runtime ``isinstance`` probe).
    if arg.cel_type is not None:
        return _celtype_name(arg.cel_type)
    value = _eval(arg, scope, clock)
    return _runtime_type_name(value)


def _eval_has(node: Call, scope: BindingScope, clock: Clock) -> bool:
    if len(node.args) != 1:
        raise EvalError(
            f"'has' takes exactly one argument, got {len(node.args)}",
            source_position=node.pos,
        )
    arg = node.args[0]
    if isinstance(arg, Member):
        key = arg.name
        target_node: Node = arg.target
    elif (
        isinstance(arg, Index)
        and isinstance(arg.index, Literal)
        and arg.index.kind is LiteralKind.STRING
        and isinstance(arg.index.value, str)
    ):
        key = arg.index.value
        target_node = arg.target
    else:
        raise EvalError(
            "'has' argument must be a dotted member or string-literal index",
            source_position=node.pos,
        )
    # Use the same chain-collapse logic for the target, then probe.
    # If resolving the target itself raises UnboundNameError, ``has``
    # propagates the error — only the *final* accessor is probed.
    try:
        target_value = _resolve_has_target(target_node, scope, clock)
    except UnboundNameError:
        # The target itself is unbound — ``has(x.y)`` on a missing
        # ``x`` is false in CEL only if ``x`` is reachable but the
        # field is missing. A truly unknown ``x`` remains an error so
        # typos surface loudly. This matches the CEL macro contract.
        raise
    if isinstance(target_value, Mapping):
        return key in target_value
    if isinstance(target_value, (list, tuple)):
        # ``has`` on a list is meaningless in CEL; treat as false to
        # keep the macro total without leaking host semantics.
        return False
    # For any other runtime shape (a scalar, a datetime, etc.) the
    # field is, by construction, not present.
    return False


def _resolve_has_target(node: Node, scope: BindingScope, clock: Clock) -> Any:
    """Resolve the *target* of a ``has`` macro.

    Differs from the ordinary evaluator in one place only: a bare
    ``Ident`` that names a binding root (e.g. ``inputs`` in
    ``has(inputs.image)``) returns the root mapping itself rather than
    routing through :meth:`BindingScope.resolve`, which rejects bare
    roots as "not a value".

    Every other shape — dotted chains, indexed chains, calls — is
    evaluated identically to the rest of the expression, so the
    sandbox guarantees still hold.
    """
    if isinstance(node, Ident) and node.name in _BINDING_ROOTS:
        if node.name == "inputs":
            return scope.inputs
        if node.name == "let":
            return scope.let
        if node.name == "steps":
            return scope.steps
        if node.name == "run":
            return scope.run
        if node.name == "workflow":
            return scope.workflow
        # ``has(now)`` — ``now`` is a callable, not a record. Fall
        # through to the normal evaluator so the scope's error shape
        # (rejecting bare ``now``) is what surfaces.
    return _eval(node, scope, clock)


def _celtype_name(t: Any) -> str:
    if isinstance(t, IntType):
        return "int"
    if isinstance(t, UintType):
        return "uint"
    if isinstance(t, DoubleType):
        return "double"
    if isinstance(t, BoolType):
        return "bool"
    if isinstance(t, StringType):
        return "string"
    if isinstance(t, BytesType):
        return "bytes"
    if isinstance(t, NullType):
        return "null_type"
    if isinstance(t, TimestampType):
        return "google.protobuf.Timestamp"
    if isinstance(t, ListType):
        return "list"
    if isinstance(t, MapType):
        return "map"
    return type(t).__name__  # pragma: no cover - defensive


def _runtime_type_name(value: Any) -> str:
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "double"
    if isinstance(value, str):
        return "string"
    if isinstance(value, bytes):
        return "bytes"
    if value is None:
        return "null_type"
    if isinstance(value, datetime):
        return "google.protobuf.Timestamp"
    if isinstance(value, (list, tuple)):
        return "list"
    if isinstance(value, Mapping):
        return "map"
    return type(value).__name__  # pragma: no cover - defensive


# ---------------------------------------------------------------------------
# Conditional / Unary
# ---------------------------------------------------------------------------


def _eval_conditional(node: Conditional, scope: BindingScope, clock: Clock) -> Any:
    cond = _eval(node.cond, scope, clock)
    if not isinstance(cond, bool):
        raise EvalError(
            f"ternary condition must be bool, got {type(cond).__name__}",
            source_position=node.cond.pos,
        )
    return _eval(node.then_branch if cond else node.else_branch, scope, clock)


def _eval_unary(node: Unary, scope: BindingScope, clock: Clock) -> Any:
    value = _eval(node.operand, scope, clock)
    if node.op is UnaryOp.NEG:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise EvalError(
                f"unary '-' requires numeric operand, got {type(value).__name__}",
                source_position=node.pos,
            )
        return -value
    if node.op is UnaryOp.NOT:
        if not isinstance(value, bool):
            raise EvalError(
                f"unary '!' requires bool operand, got {type(value).__name__}",
                source_position=node.pos,
            )
        return not value
    raise EvalError(  # pragma: no cover - exhaustive
        f"internal: unknown unary op {node.op!r}",
        source_position=node.pos,
    )


# ---------------------------------------------------------------------------
# Binary operators
# ---------------------------------------------------------------------------


def _eval_binary(node: Binary, scope: BindingScope, clock: Clock) -> Any:
    op = node.op
    # Short-circuit logical operators evaluate the right-hand side only
    # when needed, matching CEL semantics.
    if op is BinaryOp.AND:
        left = _eval(node.left, scope, clock)
        if not isinstance(left, bool):
            raise EvalError(
                f"'&&' requires bool operands, got {type(left).__name__} on left",
                source_position=node.left.pos,
            )
        if not left:
            return False
        right = _eval(node.right, scope, clock)
        if not isinstance(right, bool):
            raise EvalError(
                f"'&&' requires bool operands, got {type(right).__name__} on right",
                source_position=node.right.pos,
            )
        return right
    if op is BinaryOp.OR:
        left = _eval(node.left, scope, clock)
        if not isinstance(left, bool):
            raise EvalError(
                f"'||' requires bool operands, got {type(left).__name__} on left",
                source_position=node.left.pos,
            )
        if left:
            return True
        right = _eval(node.right, scope, clock)
        if not isinstance(right, bool):
            raise EvalError(
                f"'||' requires bool operands, got {type(right).__name__} on right",
                source_position=node.right.pos,
            )
        return right

    left = _eval(node.left, scope, clock)
    right = _eval(node.right, scope, clock)

    if op is BinaryOp.ADD:
        return _binary_add(left, right, node.pos)
    if op is BinaryOp.SUB:
        return _binary_sub(left, right, node.pos)
    if op is BinaryOp.MUL:
        return _binary_mul(left, right, node.pos)
    if op is BinaryOp.DIV:
        return _binary_div(left, right, node.pos)
    if op is BinaryOp.MOD:
        return _binary_mod(left, right, node.pos)
    if op is BinaryOp.EQ:
        return _binary_eq(left, right)
    if op is BinaryOp.NE:
        return not _binary_eq(left, right)
    if op is BinaryOp.LT:
        _require_comparable(left, right, "<", node.pos)
        return left < right
    if op is BinaryOp.LE:
        _require_comparable(left, right, "<=", node.pos)
        return left <= right
    if op is BinaryOp.GT:
        _require_comparable(left, right, ">", node.pos)
        return left > right
    if op is BinaryOp.GE:
        _require_comparable(left, right, ">=", node.pos)
        return left >= right
    if op is BinaryOp.IN:
        return _binary_in(left, right, node.pos)
    raise EvalError(  # pragma: no cover - exhaustive
        f"internal: unknown binary op {op!r}",
        source_position=node.pos,
    )


def _is_numeric(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _binary_add(left: Any, right: Any, pos: SourcePosition | None) -> Any:
    if _is_numeric(left) and _is_numeric(right) and type(left) is type(right):
        return left + right
    if isinstance(left, str) and isinstance(right, str):
        return left + right
    if isinstance(left, bytes) and isinstance(right, bytes):
        return left + right
    if isinstance(left, list) and isinstance(right, list):
        return left + right
    raise EvalError(
        f"'+' undefined for ({type(left).__name__}, {type(right).__name__})",
        source_position=pos,
    )


def _binary_sub(left: Any, right: Any, pos: SourcePosition | None) -> Any:
    if _is_numeric(left) and _is_numeric(right) and type(left) is type(right):
        return left - right
    raise EvalError(
        f"'-' undefined for ({type(left).__name__}, {type(right).__name__})",
        source_position=pos,
    )


def _binary_mul(left: Any, right: Any, pos: SourcePosition | None) -> Any:
    if _is_numeric(left) and _is_numeric(right) and type(left) is type(right):
        return left * right
    raise EvalError(
        f"'*' undefined for ({type(left).__name__}, {type(right).__name__})",
        source_position=pos,
    )


def _binary_div(left: Any, right: Any, pos: SourcePosition | None) -> Any:
    if isinstance(left, int) and isinstance(right, int) and not isinstance(left, bool):
        if right == 0:
            raise EvalError("integer division by zero", source_position=pos)
        return _trunc_div(left, right)
    if isinstance(left, float) and isinstance(right, float):
        if right == 0.0:
            raise EvalError("double division by zero", source_position=pos)
        return left / right
    raise EvalError(
        f"'/' undefined for ({type(left).__name__}, {type(right).__name__})",
        source_position=pos,
    )


def _binary_mod(left: Any, right: Any, pos: SourcePosition | None) -> Any:
    if isinstance(left, int) and isinstance(right, int) and not isinstance(left, bool):
        if right == 0:
            raise EvalError("integer modulo by zero", source_position=pos)
        return left - _trunc_div(left, right) * right
    if isinstance(left, float) and isinstance(right, float):
        if right == 0.0:
            raise EvalError("double modulo by zero", source_position=pos)
        return math.fmod(left, right)
    raise EvalError(
        f"'%' undefined for ({type(left).__name__}, {type(right).__name__})",
        source_position=pos,
    )


def _trunc_div(a: int, b: int) -> int:
    """Integer division with truncation toward zero (CEL semantics).

    Python's ``//`` floors toward negative infinity, which diverges from
    CEL on mixed-sign operands. CEL specifies truncated division (C
    semantics): ``-7 / 2 == -3`` rather than ``-4``.
    """
    q, r = divmod(a, b)
    if r != 0 and (a < 0) != (b < 0):
        q += 1
    return q


def _binary_eq(left: Any, right: Any) -> bool:
    # CEL equality is total: ``null == null`` is true, ``null == 0`` is
    # false. Booleans compare only to booleans (in particular, ``True
    # == 1`` is false even though that holds in raw Python).
    if left is None or right is None:
        return left is None and right is None
    if isinstance(left, bool) or isinstance(right, bool):
        return type(left) is type(right) and left == right
    if type(left) is not type(right):
        # Numeric tower: CEL does not implicitly promote int<->double,
        # so mismatched numerics are not equal. Strings vs bytes are
        # likewise not equal. This matches the type checker's strict
        # equality rules.
        return False
    return bool(left == right)


def _require_comparable(left: Any, right: Any, op: str, pos: SourcePosition | None) -> None:
    if type(left) is not type(right):
        raise EvalError(
            f"'{op}' requires operands of the same type, got "
            f"{type(left).__name__} and {type(right).__name__}",
            source_position=pos,
        )
    if not isinstance(left, (int, float, str, bytes, datetime)):
        raise EvalError(
            f"'{op}' is not defined for {type(left).__name__}",
            source_position=pos,
        )


def _binary_in(left: Any, right: Any, pos: SourcePosition | None) -> bool:
    if isinstance(right, (list, tuple)) and not isinstance(right, (str, bytes)):
        return any(_binary_eq(left, item) for item in right)
    if isinstance(right, Mapping):
        try:
            return left in right
        except TypeError as exc:
            raise EvalError(
                f"'in' undefined for unhashable key type {type(left).__name__}",
                source_position=pos,
            ) from exc
    raise EvalError(
        f"'in' requires list or map on the right, got {type(right).__name__}",
        source_position=pos,
    )
