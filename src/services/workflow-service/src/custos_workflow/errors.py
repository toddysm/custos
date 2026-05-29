"""Locked structured error taxonomy for the Workflow Definition Compiler.

This module implements WF-IMPL-024 (issue #358): a single, frozen
hierarchy of error classes that every public ``custos_workflow``
compile-time entry point raises. Each class carries a stable
:attr:`KIND` string that maps 1:1 to the audit ``kind`` field per
``design/components/workflow-service/design.md`` § Failure Modes.
The strings are intentionally narrow and stable — audit consumers
(Observability Service, Step Coordinator emission, the Catalog
Service publish flow) key off them, so any change here is a
downstream contract break.

The hierarchy:

* :class:`CompileError` — abstract base. Subclasses Python's
  :class:`RuntimeError`. Defines the shared ``kind`` / ``message`` /
  ``source_position`` attribute surface, hashable / equal-on-fields
  identity, and the :meth:`CompileError.to_dict` JSON-safe
  serializer used by audit emission.
* :class:`CompileParseError` — A CEL placeholder failed to parse
  (``compile.parse_error``). Wraps the underlying
  :class:`custos_cel.CelError` cause. Also subclasses
  :class:`ValueError`.
* :class:`CompileTypeError` — A CEL call site failed type-checking
  (``compile.type_error``). Carries ``step_id`` and
  ``call_site_path``. Also subclasses the built-in
  :class:`TypeError`.
* :class:`CompileTopologyError` — Explicit/implicit edges or
  topological sort rejected the graph
  (``compile.topology_error``). Carries the offending ``cycle``
  tuple when the rejection is a cycle. Also subclasses
  :class:`ValueError`.
* :class:`CompileRetryPolicyError` — A layered retry policy is
  invalid (``compile.retry_policy_error``). Carries the offending
  ``field`` and a short ``reason``. Also subclasses
  :class:`ValueError`.

Every class is hashable and equal-on-fields, has a deterministic
:meth:`__repr__`, and round-trips through :meth:`to_dict` for audit
emission. The :attr:`KIND` string is a class-level
:data:`typing.Final` constant so ``cls.KIND`` and ``instance.kind``
are always identical and never accidentally overridden by callers.

See the issue: https://github.com/toddysm/custos/issues/358
"""

from __future__ import annotations

import builtins
from typing import TYPE_CHECKING, Any, ClassVar, Final

if TYPE_CHECKING:
    from custos_cel import CelError
    from custos_cel.ast import SourcePosition

__all__ = [
    "CompileError",
    "CompileParseError",
    "CompileRetryPolicyError",
    "CompileTopologyError",
    "CompileTypeError",
]


# ---------------------------------------------------------------------------
# Base
# ---------------------------------------------------------------------------


class CompileError(RuntimeError):
    """Base class for every structured compile-time error.

    Concrete subclasses pin a stable :attr:`KIND` string and may
    add extra structured fields (e.g. :attr:`CompileTypeError.step_id`).
    The constructor signature is intentionally narrow: ``message``
    is positional, every other field is keyword-only. Subclasses
    keep the same shape so callers and pattern-matching consumers
    see a uniform surface.

    Attributes:
        kind: The :attr:`KIND` of this error's concrete class.
            Always a ``"compile.*"`` string. Set in ``__init__``
            so it survives ``copy.copy`` and pickling cleanly.
        message: Human-readable explanation. Mirrors
            ``str(exception)`` for the default formatter.
        source_position: Position in the original document /
            CEL source where the offending node was parsed, when
            available. ``None`` for errors that arise outside an
            AST walk (e.g. topology errors carry no position).
    """

    #: Subclasses pin this to a concrete ``"compile.*"`` string.
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
                "CompileError is abstract; instantiate a concrete "
                "subclass (CompileParseError, CompileTypeError, "
                "CompileTopologyError, CompileRetryPolicyError) instead.",
            )
        super().__init__(message)
        self.kind: str = self.KIND
        self.message: str = message
        self.source_position: SourcePosition | None = source_position

    def _extra_fields(self) -> dict[str, Any]:
        """Hook for subclasses to contribute extra fields to
        :meth:`to_dict` and :meth:`__repr__` / :meth:`__eq__` /
        :meth:`__hash__`.

        The base returns an empty mapping. Subclasses override
        and return only JSON-safe primitives. The mapping's
        iteration order is preserved by :meth:`to_dict` so audit
        serialization stays deterministic.
        """
        return {}

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-safe dict for audit-event emission.

        Shape (deterministic key order):

        ``{"kind": str, "message": str, "source_position": {...} | None, ...}``

        Subclasses extend the result with their structured fields
        (see :meth:`_extra_fields`). The result is deterministic
        in key order: ``kind`` first, then ``message``, then
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

    def __repr__(self) -> str:
        parts: list[str] = [
            f"kind={self.kind!r}",
            f"message={self.message!r}",
            f"source_position={self.source_position!r}",
        ]
        parts.extend(f"{name}={value!r}" for name, value in self._extra_fields().items())
        return f"{type(self).__name__}({', '.join(parts)})"

    def _identity(self) -> tuple[Any, ...]:
        """Hashable identity tuple used by :meth:`__eq__` and :meth:`__hash__`.

        Concrete instances of the same subclass with identical
        fields compare equal and hash identically — different
        from the default exception-by-identity semantics. This
        is intentional: audit consumers dedupe failures by
        structural identity rather than instance.
        """
        # ``type(self)`` discriminates so a CompileParseError and a
        # CompileTopologyError with the same message do NOT collide.
        # Subclass extras are appended via _extra_fields so each
        # subclass picks up its own field set without overriding
        # this method. ``_extra_fields`` may return JSON-list
        # values (e.g. ``cycle`` is rendered as a list for
        # byte-stable serialisation); we re-freeze each one via
        # :func:`_freeze` before hashing so the identity remains
        # hashable.
        frozen_extras = tuple(
            (name, _freeze(value)) for name, value in self._extra_fields().items()
        )
        return (
            type(self),
            self.kind,
            self.message,
            self.source_position,
            frozen_extras,
        )

    def __eq__(self, other: object) -> bool:
        if type(other) is not type(self):
            return NotImplemented
        return self._identity() == other._identity()

    def __hash__(self) -> int:
        return hash(self._identity())


def _freeze(value: Any) -> Any:
    """Recursively freeze a JSON-list/dict value into a hashable form.

    Used by :meth:`CompileError._identity` to convert the
    JSON-rendered shape from :meth:`CompileError._extra_fields`
    (which may contain ``list`` / ``dict`` values for byte-stable
    serialisation) back into a tuple-of-tuples that ``hash`` can
    consume. Primitive values pass through unchanged.
    """
    if isinstance(value, list):
        return tuple(_freeze(v) for v in value)
    if isinstance(value, dict):
        return tuple((k, _freeze(v)) for k, v in value.items())
    return value


def _source_position_to_dict(
    pos: SourcePosition | None,
) -> dict[str, int | None] | None:
    """Render a :class:`SourcePosition` for :meth:`CompileError.to_dict`.

    Returns ``None`` when the originating site did not propagate
    a position. Otherwise defers to
    :meth:`SourcePosition.to_dict` so the JSON shape stays
    byte-identical to the AST's own serialization (``line`` /
    ``column`` / ``offset``, each ``int | None``).
    """
    if pos is None:
        return None
    return pos.to_dict()


def _cause_to_dict(cause: CelError | None) -> dict[str, Any] | None:
    """Render a wrapped :class:`custos_cel.CelError` for ``to_dict``.

    Preserves the underlying ``kind`` / ``message`` so audit
    consumers can correlate a compile-time wrapping with the
    original evaluator failure. Returns ``None`` when there is
    no underlying cause (e.g. the compiler synthesised the error
    itself).
    """
    if cause is None:
        return None
    return {"kind": cause.kind, "message": cause.message}


# ---------------------------------------------------------------------------
# Concrete subclasses
# ---------------------------------------------------------------------------


class CompileParseError(CompileError, ValueError):
    """A ``${{ ... }}`` placeholder failed to parse.

    Wraps :class:`custos_cel.ParseError` (or a sibling
    :class:`custos_cel.CelError`) raised from a call-site parse.
    Also subclasses :class:`ValueError` so callers using generic
    ``except ValueError:`` blocks still catch it.

    Attributes:
        step_id: The step id whose call site failed parsing,
            when known. ``None`` for document-level parse failures
            outside a step body.
        call_site_path: The collector-assigned dict-key path
            inside the step (e.g. ``"if"``, ``"with.image"``,
            ``"let.severity"``), when known.
        cause: The underlying :class:`custos_cel.CelError`
            preserved verbatim for audit correlation; rendered
            into :meth:`to_dict` under the ``"cause"`` key.
    """

    KIND: Final[str] = "compile.parse_error"  # type: ignore[misc]

    def __init__(
        self,
        message: str,
        *,
        source_position: SourcePosition | None = None,
        step_id: str | None = None,
        call_site_path: str | None = None,
        cause: CelError | None = None,
    ) -> None:
        super().__init__(message, source_position=source_position)
        self.step_id: str | None = step_id
        self.call_site_path: str | None = call_site_path
        self.cause: CelError | None = cause

    def _extra_fields(self) -> dict[str, Any]:
        return {
            "step_id": self.step_id,
            "call_site_path": self.call_site_path,
            "cause": _cause_to_dict(self.cause),
        }


class CompileTypeError(CompileError, builtins.TypeError):
    """A CEL call site failed type-checking.

    Wraps :class:`custos_cel.TypeError` (or a sibling
    :class:`custos_cel.CelError`) raised from the type-check stage.
    Also subclasses the built-in :class:`TypeError` so callers
    using generic ``except TypeError:`` blocks still catch it.

    Attributes:
        step_id: The step id whose call site failed type-checking.
        call_site_path: The collector-assigned dict-key path
            inside the step (e.g. ``"with.image"``).
        cause: The underlying :class:`custos_cel.CelError`
            preserved verbatim for audit correlation; rendered
            into :meth:`to_dict` under the ``"cause"`` key.
    """

    KIND: Final[str] = "compile.type_error"  # type: ignore[misc]

    def __init__(
        self,
        message: str,
        *,
        source_position: SourcePosition | None = None,
        step_id: str | None = None,
        call_site_path: str | None = None,
        cause: CelError | None = None,
    ) -> None:
        super().__init__(message, source_position=source_position)
        self.step_id: str | None = step_id
        self.call_site_path: str | None = call_site_path
        self.cause: CelError | None = cause

    def _extra_fields(self) -> dict[str, Any]:
        return {
            "step_id": self.step_id,
            "call_site_path": self.call_site_path,
            "cause": _cause_to_dict(self.cause),
        }


class CompileTopologyError(CompileError, ValueError):
    """The graph's edge / topology stage rejected the document.

    Raised when an explicit edge points at an unknown step, a
    cycle is detected, or a topological sort cannot produce a
    deterministic order. Also subclasses :class:`ValueError`.

    Attributes:
        cycle: The offending cycle as a tuple of step ids in
            declaration order, when the rejection is a cycle.
            Empty for non-cycle topology failures (e.g.
            forward-reference rejection).
    """

    KIND: Final[str] = "compile.topology_error"  # type: ignore[misc]

    def __init__(
        self,
        message: str,
        *,
        source_position: SourcePosition | None = None,
        cycle: tuple[str, ...] = (),
    ) -> None:
        super().__init__(message, source_position=source_position)
        self.cycle: tuple[str, ...] = cycle

    def _extra_fields(self) -> dict[str, Any]:
        return {"cycle": list(self.cycle)}


class CompileRetryPolicyError(CompileError, ValueError):
    """A layered retry policy is invalid.

    Raised when the per-match → step → ``spec.defaults`` →
    platform overlay produces a policy that violates the
    design's invariants (malformed ISO-8601 duration,
    ``maxDelay < initialDelay`` after overlay, inline
    ``maxAttempts:`` shorthand conflicting with structured
    ``retry: { maxAttempts: ... }``, ``retry:`` or ``on_error:``
    on a disallowed step kind, ``do: retry`` on a
    ``class: permanent`` / ``class: cancelled`` arm, ``retry:``
    on a ``do: skip`` / ``do: fail`` arm). Also subclasses
    :class:`ValueError`.

    Attributes:
        field: The offending field path (e.g. ``"backoff.maxDelay"``,
            ``"on_error[0].retry.maxAttempts"``), when known.
        reason: A short machine-readable description of why the
            field was rejected (e.g. ``"malformed ISO-8601 duration"``,
            ``"max_delay < initial_delay"``).
    """

    KIND: Final[str] = "compile.retry_policy_error"  # type: ignore[misc]

    def __init__(
        self,
        message: str,
        *,
        source_position: SourcePosition | None = None,
        field: str | None = None,
        reason: str | None = None,
    ) -> None:
        super().__init__(message, source_position=source_position)
        self.field: str | None = field
        self.reason: str | None = reason

    def _extra_fields(self) -> dict[str, Any]:
        return {"field": self.field, "reason": self.reason}
