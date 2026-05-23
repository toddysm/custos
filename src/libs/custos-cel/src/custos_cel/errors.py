"""Locked structured error taxonomy for the Custos CEL evaluator.

This module implements WF-IMPL-008 (issue #183): a single, frozen
hierarchy of error classes that every public ``custos_cel`` entry
point raises. Each class carries a stable ``kind`` string that maps
1:1 to the Workflow Service audit ``kind`` field per
``design/components/workflow-service/design.md`` § Expression Evaluator
failure modes. The strings are intentionally narrow and stable —
audit consumers (Observability Service, Step Coordinator emission,
Definition Compiler) key off them, so any change here is a downstream
contract break.

The hierarchy:

* :class:`CelError` — abstract base. Subclasses Python's
  :class:`Exception`. Defines the shared ``kind`` / ``message`` /
  ``source_position`` attribute surface and the
  :meth:`CelError.to_dict` JSON-safe serializer used by audit emission.
* :class:`ParseError` — Catalog publish-time syntactic failure
  (``expression.parse_error``). Also subclasses :class:`ValueError`
  so existing parse-error catch blocks continue to fire.
* :class:`TypeError` — Definition-compiler type mismatch
  (``expression.type_error``). Also subclasses the built-in
  :class:`TypeError` so generic validation wrappers still see it.
* :class:`UnboundNameError` — Lookup of a name outside the
  allow-listed binding roots, missing step id, missing schema field,
  or non-allow-listed function (``expression.unbound_name``). Also
  subclasses :class:`LookupError`.
* :class:`TimeoutError` — Per-evaluation wall-clock budget exceeded
  (``expression.timeout``). Also subclasses the built-in
  :class:`TimeoutError`.
* :class:`EvaluationError` — Catch-all runtime evaluation failure
  not covered by the above (e.g. division by zero, out-of-range
  index, runtime type-shape mismatch that escaped the type checker)
  (``expression.evaluation_error``). Also subclasses
  :class:`RuntimeError`.
* :class:`DivergenceError` — Replay non-determinism detected by a
  higher-level component (``expression.divergence``). Lives in this
  taxonomy for completeness; ``custos_cel`` itself never raises it,
  but the Workflow Service Step Coordinator constructs and emits it
  when Dapr Workflow signals a replay divergence per design.md §
  Failure Modes.

Every class is hashable (Python ``Exception`` is hashable by identity
by default), has a structured ``__repr__``, and round-trips through
:meth:`to_dict` for audit emission. The ``kind`` string is a class-
level :data:`typing.Final` constant so ``cls.KIND`` and
``instance.kind`` are always identical and never accidentally
overridden by callers.

See the issue: https://github.com/toddysm/custos/issues/183
"""

from __future__ import annotations

import builtins
from typing import Any, ClassVar, Final

from custos_cel.ast import CelType, SourcePosition

__all__ = [
    "CelError",
    "DivergenceError",
    "EvaluationError",
    "ParseError",
    "TimeoutError",
    "TypeError",
    "UnboundNameError",
]


# ---------------------------------------------------------------------------
# Base
# ---------------------------------------------------------------------------


class CelError(Exception):
    """Base class for every structured CEL error.

    Concrete subclasses pin a stable :attr:`KIND` string and may add
    extra structured fields (e.g. :attr:`TypeError.expected_type`).
    The constructor signature is intentionally narrow: ``message`` is
    positional, every other field is keyword-only. Subclasses that add
    fields keep the same shape so callers and pattern-matching
    consumers see a uniform surface.

    Attributes:
        kind: The :attr:`KIND` of this error's concrete class. Always
            an ``"expression.*"`` string. Set in ``__init__`` so it
            survives ``copy.copy`` and pickling cleanly.
        message: Human-readable explanation. Mirrors
            ``str(exception)`` for the default formatter.
        source_position: Position in the original CEL source string
            where the offending node was parsed, when available.
            ``None`` for errors that arise outside an AST walk (e.g.
            an env-var misconfiguration would not raise a
            :class:`CelError`; only AST-level failures do).
    """

    #: Subclasses pin this to a concrete ``"expression.*"`` string.
    #: The base raises if instantiated directly because the empty
    #: kind would defeat the taxonomy.
    KIND: ClassVar[str] = ""

    def __init__(
        self,
        message: str,
        *,
        source_position: SourcePosition | None = None,
    ) -> None:
        if not self.KIND:
            raise builtins.TypeError(
                "CelError is abstract; instantiate a concrete subclass "
                "(ParseError, TypeError, UnboundNameError, TimeoutError, "
                "EvaluationError, DivergenceError) instead.",
            )
        super().__init__(message)
        self.kind: str = self.KIND
        self.message: str = message
        self.source_position: SourcePosition | None = source_position

    def _extra_fields(self) -> dict[str, Any]:
        """Hook for subclasses to contribute extra fields to
        :meth:`to_dict` and :meth:`__repr__`.

        The base returns an empty mapping. Subclasses override and
        return only JSON-safe primitives.
        """

        return {}

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-safe dict for audit-event emission.

        Shape:

        ``{"kind": str, "message": str, "source_position": {...} | None, ...}``

        Subclasses extend the result with their structured fields
        (see :meth:`_extra_fields`). The result is deterministic in
        key order: ``kind`` first, then ``message``, then
        ``source_position``, then any subclass extras in their
        declaration order — so byte-stable audit serialization is
        possible without an extra canonicalization step.
        """

        out: dict[str, Any] = {
            "kind": self.kind,
            "message": self.message,
            "source_position": _source_position_to_dict(self.source_position),
        }
        out.update(self._extra_fields())
        return out

    def __repr__(self) -> str:  # pragma: no cover - exercised via tests
        parts: list[str] = [
            f"kind={self.kind!r}",
            f"message={self.message!r}",
            f"source_position={self.source_position!r}",
        ]
        for name, value in self._extra_fields().items():
            parts.append(f"{name}={value!r}")
        return f"{type(self).__name__}({', '.join(parts)})"


def _source_position_to_dict(
    pos: SourcePosition | None,
) -> dict[str, int | None] | None:
    """Render a :class:`SourcePosition` for :meth:`CelError.to_dict`.

    Returns ``None`` when the originating site did not propagate a
    position. Otherwise defers to
    :meth:`SourcePosition.to_dict` so the JSON shape stays
    byte-identical to the AST's own serialization (``line`` /
    ``column`` / ``offset``, each ``int | None``).
    """

    if pos is None:
        return None
    return pos.to_dict()


# ---------------------------------------------------------------------------
# Concrete subclasses
# ---------------------------------------------------------------------------


class ParseError(CelError, ValueError):
    """Syntactic parse failure observed at parser surface.

    Raised by :func:`custos_cel.parse` when ``celpy`` rejects the
    source or when the parse-tree contains a construct outside the
    Custos CEL subset (e.g. method-call syntax, protobuf message
    construction). Per ``bundle-h``, this is a contract violation at
    ``StartRun`` time because Catalog Service has already gated the
    expression at publish time — Definition Compiler surfaces it as a
    permanent compile error.
    """

    KIND: Final[str] = "expression.parse_error"  # type: ignore[misc]


class TypeError(CelError, builtins.TypeError):
    """Type-check mismatch raised by :func:`custos_cel.type_check`.

    Also subclasses the built-in :class:`TypeError` so callers using
    generic validation idioms (``except TypeError``) still catch it.

    Attributes:
        expected_type: The :class:`~custos_cel.ast.CelType` the
            checker expected at this position, or a short
            human-readable label (e.g. ``"numeric"`` for the
            arithmetic operator family).
        actual_type: The :class:`~custos_cel.ast.CelType` actually
            inferred, or a short label. Same shape as
            ``expected_type``.
    """

    KIND: Final[str] = "expression.type_error"  # type: ignore[misc]

    def __init__(
        self,
        message: str,
        *,
        source_position: SourcePosition | None = None,
        expected_type: CelType | str | None = None,
        actual_type: CelType | str | None = None,
    ) -> None:
        super().__init__(message, source_position=source_position)
        self.expected_type: CelType | str | None = expected_type
        self.actual_type: CelType | str | None = actual_type

    def _extra_fields(self) -> dict[str, Any]:
        return {
            "expected_type": _type_to_str(self.expected_type),
            "actual_type": _type_to_str(self.actual_type),
        }


def _type_to_str(t: CelType | str | None) -> str | None:
    """Render a CelType (or a pre-rendered label) as a stable string.

    JSON-safe; ``None`` round-trips to ``None``. :class:`CelType`
    instances render via ``repr`` because their ``__repr__`` is the
    canonical short label (``int``, ``list<string>``, etc.) used by
    the type checker's error messages.
    """

    if t is None:
        return None
    if isinstance(t, str):
        return t
    return repr(t)


class UnboundNameError(CelError, LookupError):
    """A name (root, step id, schema field, function) is not bound.

    Raised by :class:`~custos_cel.BindingScope` and by the type
    checker / evaluator when an identifier chain cannot be resolved.
    Also subclasses :class:`LookupError` so generic catch blocks
    still see it.

    Attributes:
        name_chain: Tuple of identifiers as written, e.g.
            ``("inputs", "image")`` for ``inputs.image``.
        reason: Short, machine-readable description of *why* the
            chain failed to resolve (``"unknown root"``,
            ``"no such field"``, etc.). May be ``None`` for very
            shallow failures.

    Backwards-compatible aliases: :attr:`chain` mirrors
    :attr:`name_chain` (since WF-IMPL-004 callers used that name) and
    :attr:`pos` mirrors :attr:`source_position` for the same reason.
    """

    KIND: Final[str] = "expression.unbound_name"  # type: ignore[misc]

    def __init__(
        self,
        name_chain: object = (),
        *,
        pos: SourcePosition | None = None,
        source_position: SourcePosition | None = None,
        reason: str | None = None,
    ) -> None:
        # Accept either ``name_chain`` (positional, new) or ``pos`` /
        # ``source_position`` (keyword). Two positional aliases means
        # call sites from WF-IMPL-004 keep working unmodified.
        if isinstance(name_chain, str):
            # Defensive: callers historically passed an iterable of
            # str; a bare ``str`` would silently iterate by character.
            chain_tuple: tuple[str, ...] = (name_chain,)
        else:
            chain_tuple = tuple(name_chain)  # type: ignore[arg-type]
        rendered = ".".join(chain_tuple) if chain_tuple else "<empty>"
        suffix = f" ({reason})" if reason else ""
        message = f"unbound name: {rendered}{suffix}"
        resolved_pos = source_position if source_position is not None else pos
        super().__init__(message, source_position=resolved_pos)
        self.name_chain: tuple[str, ...] = chain_tuple
        self.reason: str | None = reason
        # Backwards-compat aliases used by WF-IMPL-004 / WF-IMPL-006.
        self.chain: tuple[str, ...] = chain_tuple
        self.pos: SourcePosition | None = resolved_pos

    def _extra_fields(self) -> dict[str, Any]:
        return {
            "name_chain": list(self.name_chain),
            "reason": self.reason,
        }


class TimeoutError(CelError, builtins.TimeoutError):
    """Per-evaluation wall-clock budget exceeded.

    Raised by :func:`custos_cel.evaluate` when the cooperative
    deadline check (every 32 nodes, see WF-IMPL-007) observes that
    :func:`time.monotonic` has passed the precomputed deadline. Also
    subclasses the built-in :class:`TimeoutError` so callers that
    catch the standard timeout shape still see it.

    Attributes:
        elapsed_ms: Wall-clock milliseconds elapsed before the
            deadline check fired. Always ``>= timeout_ms``.
        timeout_ms: The budget that was exceeded, as passed to
            :func:`evaluate` (or as resolved from
            ``WF_EXPR_TIMEOUT_MS``).
    """

    KIND: Final[str] = "expression.timeout"  # type: ignore[misc]

    def __init__(
        self,
        message: str,
        *,
        elapsed_ms: int,
        timeout_ms: int,
        source_position: SourcePosition | None = None,
    ) -> None:
        super().__init__(message, source_position=source_position)
        self.elapsed_ms: int = elapsed_ms
        self.timeout_ms: int = timeout_ms

    def _extra_fields(self) -> dict[str, Any]:
        return {"elapsed_ms": self.elapsed_ms, "timeout_ms": self.timeout_ms}


class EvaluationError(CelError, RuntimeError):
    """Catch-all runtime evaluation failure.

    Raised by :func:`custos_cel.evaluate` for value-level failures
    not covered by the more specific :class:`UnboundNameError` /
    :class:`TimeoutError` classes: division by zero, modulo by zero,
    out-of-range list index, missing key on a runtime map, and any
    runtime type-shape mismatch that escaped the type checker (which
    would indicate a bug in either the type checker or the caller's
    bindings).

    Also subclasses :class:`RuntimeError`.
    """

    KIND: Final[str] = "expression.evaluation_error"  # type: ignore[misc]


class DivergenceError(CelError, RuntimeError):
    """Replay-divergence detected by the Step Coordinator.

    ``custos_cel`` itself never raises :class:`DivergenceError` — the
    expression evaluator is replay-deterministic by construction (the
    injected :class:`~custos_cel.Clock` is the only externally
    observable input). The class lives in this taxonomy because the
    Workflow Service Step Coordinator constructs and emits it when
    Dapr Workflow signals a non-determinism error per design.md §
    Failure Modes; downstream audit consumers key off the same
    ``kind`` string regardless of which component raised it.

    Also subclasses :class:`RuntimeError`.
    """

    KIND: Final[str] = "expression.divergence"  # type: ignore[misc]
