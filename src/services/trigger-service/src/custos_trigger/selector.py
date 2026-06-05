"""CEL selector evaluator (design § Selector Language, resolves TODO-002).

A subscription selector is a single CEL **boolean** expression evaluated by the
shared :mod:`custos_cel` sandbox against the ``event`` binding root that mirrors
the :class:`~custos_trigger.events.NormalizedEvent` envelope
(``event.kind``, ``event.subject``,
``event.source.{type,connectorInstanceId,subscriptionId,vendor,occurredAt}``,
``event.data.*``, ``event.raw.{headers,body}``). This is the canonical persisted
form; the legacy ``field: matchType:value`` sugar lowers to CEL before storage.

Lifecycle (design § Selector Language):

* **Compile-at-create.** :meth:`SelectorEvaluator.compile` runs
  :func:`custos_cel.parse` + :func:`custos_cel.type_check` against the ``event``
  :class:`~custos_cel.SchemaBindings`; a syntax error, unbound name, or type
  mismatch surfaces as :class:`SelectorInvalidError` (``trigger.selector_invalid``,
  HTTP 422) **before** the subscription is persisted. The typed AST is cached
  in-process keyed by ``(subscriptionId, exprHash)``.
* **Evaluate-at-match.** :meth:`SelectorEvaluator.evaluate` builds a
  :class:`~custos_cel.BindingScope` carrying the normalized event and evaluates
  under the per-evaluation timeout budget. A non-bool result raises
  :class:`SelectorTypeError` (``trigger.selector_type_error`` — the match layer
  audits and treats as no-match); a timeout or a runtime resolution failure
  (e.g. a selector referencing an event field absent from this particular event)
  is a no-match.

Legacy desugar — divergence from the design's documented form
-------------------------------------------------------------

The design and the selector-cel-parity change record desugar ``prefix`` to
``event.data.<field>.startsWith("…")``. The locked ``custos_cel`` subset
(ADR-011) **rejects method-call syntax** and ships no ``startsWith`` / ``matches``
/ regex function — its only string operations are ``==``, the ordering
comparisons, and ``in``. So this implementation lowers:

* ``eq`` → ``event.data.<field> == "<value>"``
* ``prefix`` → the semantically-identical lexicographic range
  ``event.data.<field> >= "<value>" && event.data.<field> < "<value⁺>"`` where
  ``<value⁺>`` increments the final code point — a true prefix test that the
  subset *can* express and evaluate.

``regex`` and ``jsonpath`` have no representation in the v1 subset (there is no
regex or path primitive), so they are rejected at desugar with
:class:`SelectorInvalidError`; they await a future ``custos_cel`` ``matches()`` /
jsonpath extension. Authors needing those today write an explicit CEL expression
over the available operators.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from custos_cel import (
    BindingScope,
    Clock,
    EvalError,
    EvalTimeoutError,
    FixedClock,
    Node,
    ParseError,
    RunInfo,
    SchemaBindings,
    TypeCheckError,
    UnboundNameError,
    WorkflowInfo,
)
from custos_cel import (
    evaluate as cel_evaluate,
)
from custos_cel import (
    parse as cel_parse,
)
from custos_cel import (
    type_check as cel_type_check,
)

from custos_trigger.errors import TriggerError, TriggerErrorKind
from custos_trigger.events import NormalizedEvent
from custos_trigger.models import SelectorMatchType

__all__ = [
    "EVENT_SCHEMA_BINDINGS",
    "CompiledSelector",
    "SelectorEvaluator",
    "SelectorInvalidError",
    "SelectorTypeError",
    "compute_expr_hash",
    "desugar_legacy_selector",
]

#: The ``SchemaBindings`` selectors type-check against. The default ``event``
#: root already mirrors the ``NormalizedEvent`` envelope; ``inputs`` is left
#: empty so a selector referencing anything but ``event.*`` (or the static
#: ``run`` / ``workflow`` / ``now()`` roots) fails fast at compile.
EVENT_SCHEMA_BINDINGS: SchemaBindings = SchemaBindings()

#: Placeholder identity roots. Selectors are documented to reference only
#: ``event.*``; these satisfy the required :class:`BindingScope` fields without
#: leaking real run/workflow identity into the match.
_PLACEHOLDER_RUN: RunInfo = RunInfo(id="", workspace="")
_PLACEHOLDER_WORKFLOW: WorkflowInfo = WorkflowInfo(name="", version="")

#: A legacy field path segment must be a bare CEL identifier so the desugared
#: ``event.data.<field>`` parses as member access (not, say, subtraction on a
#: hyphenated name).
_IDENT_RE: re.Pattern[str] = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class SelectorInvalidError(TriggerError):
    """A selector failed to parse / type-check (or cannot be desugared).

    Carries the locked ``trigger.selector_invalid`` kind; the REST layer
    surfaces it as HTTP 422 at subscription create / patch time.
    """

    def __init__(self, message: str, *, details: dict[str, Any] | None = None) -> None:
        super().__init__(TriggerErrorKind.SELECTOR_INVALID, message, details=details)


class SelectorTypeError(TriggerError):
    """A selector evaluated to a non-boolean value at match time.

    Carries the locked ``trigger.selector_type_error`` kind; the match layer
    audits it and treats the event as a no-match.
    """

    def __init__(self, message: str, *, details: dict[str, Any] | None = None) -> None:
        super().__init__(TriggerErrorKind.SELECTOR_TYPE_ERROR, message, details=details)


@dataclass(frozen=True, slots=True)
class CompiledSelector:
    """A parsed + type-checked selector, ready to evaluate.

    Cached in-process keyed by ``(subscription_id, expr_hash)`` so the hot match
    path skips the parse + type-check work.
    """

    subscription_id: str
    expr: str
    expr_hash: str
    typed_ast: Node


def compute_expr_hash(expr: str) -> str:
    """Return a stable hash of a selector expression (cache key component)."""
    return hashlib.sha256(expr.encode("utf-8")).hexdigest()


def _cel_string_literal(value: str) -> str:
    """Render ``value`` as a CEL double-quoted string literal.

    CEL string-literal escaping is a superset-compatible match for JSON string
    escaping for the characters that appear here (quotes, backslashes, control
    chars), so the encoder reuses JSON's.
    """
    return json.dumps(value)


def _increment_code_point(code_point: int) -> int | None:
    """Return the next valid Unicode scalar value after ``code_point``.

    Skips the UTF-16 surrogate range (``U+D800`` to ``U+DFFF``, not valid scalar
    values) and returns ``None`` when ``code_point`` is already the maximum
    (``U+10FFFF``) and so has no successor.
    """
    if code_point >= 0x10FFFF:
        return None
    nxt = code_point + 1
    if 0xD800 <= nxt <= 0xDFFF:
        return 0xE000
    return nxt


def _prefix_upper_bound(value: str) -> str:
    """Return the exclusive upper bound of the prefix range for ``value``.

    The smallest string that sorts strictly after every string beginning with
    ``value``: increment the final code point, carrying left past any code
    points already at the maximum. Raises :class:`SelectorInvalidError` when no
    successor exists (every code point is ``U+10FFFF``), which cannot bound a
    prefix range.
    """
    for index in range(len(value) - 1, -1, -1):
        nxt = _increment_code_point(ord(value[index]))
        if nxt is not None:
            return value[:index] + chr(nxt)
    raise SelectorInvalidError(
        "prefix value has no lexicographic successor and cannot bound a range",
        details={"value": value},
    )


def _legacy_field_to_cel_path(field: str) -> str:
    """Lower a legacy selector ``field`` to its ``event.data.<field>`` CEL path.

    The legacy tuple form addressed the vendor payload, so ``repository`` becomes
    ``event.data.repository`` and dotted ``a.b`` becomes ``event.data.a.b``. Each
    segment must be a bare identifier; anything else raises
    :class:`SelectorInvalidError`.
    """
    if not field:
        raise SelectorInvalidError("legacy selector field must be non-empty")
    segments = field.split(".")
    for segment in segments:
        if not _IDENT_RE.match(segment):
            raise SelectorInvalidError(
                f"legacy selector field segment {segment!r} is not a valid identifier",
                details={"field": field},
            )
    return "event.data." + ".".join(segments)


def desugar_legacy_selector(*, field: str, match_type: SelectorMatchType, value: str) -> str:
    """Lower a legacy ``field: matchType:value`` selector into a CEL expression.

    Supports ``eq`` (equality) and ``prefix`` (lexicographic range). ``regex``
    and ``jsonpath`` have no representation in the v1 ``custos_cel`` subset and
    raise :class:`SelectorInvalidError`. Calling with ``cel`` is a programming
    error (a CEL selector is already canonical) and raises :class:`ValueError`.
    """
    path = _legacy_field_to_cel_path(field)

    if match_type is SelectorMatchType.EQ:
        return f"{path} == {_cel_string_literal(value)}"

    if match_type is SelectorMatchType.PREFIX:
        if value == "":
            # An empty prefix matches any string-valued field.
            return f'{path} >= ""'
        lower = _cel_string_literal(value)
        upper = _cel_string_literal(_prefix_upper_bound(value))
        return f"{path} >= {lower} && {path} < {upper}"

    if match_type in (SelectorMatchType.REGEX, SelectorMatchType.JSONPATH):
        raise SelectorInvalidError(
            f"match type {match_type.value!r} is not supported by the v1 CEL subset "
            "(no regex / jsonpath primitive); author an explicit CEL selector instead",
            details={"field": field, "matchType": match_type.value},
        )

    raise ValueError(f"{match_type.value!r} is not a legacy match type")


def _event_to_mapping(event: NormalizedEvent | Mapping[str, Any]) -> Mapping[str, Any]:
    """Render an event as the camelCase mapping the ``event`` root resolves."""
    if isinstance(event, NormalizedEvent):
        return event.model_dump(by_alias=True)
    return event


class SelectorEvaluator:
    """Compile-at-create + evaluate-at-match CEL selector evaluator.

    One instance is held per Trigger Service process; its in-process cache is
    keyed by ``(subscription_id, expr_hash)`` so re-authoring a selector (a new
    hash) supersedes the old entry while a stable selector stays warm.
    """

    def __init__(
        self,
        *,
        bindings: SchemaBindings = EVENT_SCHEMA_BINDINGS,
        timeout_ms: int | None = None,
    ) -> None:
        self._bindings = bindings
        self._timeout_ms = timeout_ms
        self._cache: dict[tuple[str, str], CompiledSelector] = {}

    def compile(self, expr: str, *, subscription_id: str) -> CompiledSelector:
        """Parse + type-check ``expr`` and cache the typed AST.

        Raises:
            SelectorInvalidError: If ``expr`` fails to parse, references an
                unbound name, or fails type checking.
        """
        expr_hash = compute_expr_hash(expr)
        key = (subscription_id, expr_hash)
        cached = self._cache.get(key)
        if cached is not None:
            return cached

        try:
            ast = cel_parse(expr)
            typed_ast = cel_type_check(ast, self._bindings)
        except (ParseError, TypeCheckError, UnboundNameError) as exc:
            raise SelectorInvalidError(
                f"selector did not compile: {exc}",
                details={"subscriptionId": subscription_id},
            ) from exc

        compiled = CompiledSelector(
            subscription_id=subscription_id,
            expr=expr,
            expr_hash=expr_hash,
            typed_ast=typed_ast,
        )
        self._cache[key] = compiled
        return compiled

    def evaluate(
        self,
        compiled: CompiledSelector,
        event: NormalizedEvent | Mapping[str, Any],
        *,
        clock: Clock | None = None,
    ) -> bool:
        """Evaluate ``compiled`` against ``event`` and return the boolean result.

        A timeout or a runtime resolution failure (e.g. the selector references
        an event field this event omits) is a no-match (``False``). A well-typed
        non-bool result raises :class:`SelectorTypeError`.
        """
        active_clock: Clock = clock if clock is not None else FixedClock(datetime.now(UTC))
        scope = BindingScope(
            run=_PLACEHOLDER_RUN,
            workflow=_PLACEHOLDER_WORKFLOW,
            now=active_clock.now,
            event=_event_to_mapping(event),
        )

        try:
            result = cel_evaluate(
                compiled.typed_ast, scope, active_clock, timeout_ms=self._timeout_ms
            )
        except EvalTimeoutError:
            return False
        except (EvalError, UnboundNameError):
            # A selector that references a field absent from this particular
            # event is a no-match, not a pipeline failure.
            return False

        if not isinstance(result, bool):
            raise SelectorTypeError(
                "selector did not evaluate to a boolean",
                details={
                    "subscriptionId": compiled.subscription_id,
                    "resultType": type(result).__name__,
                },
            )
        return result

    def matches(
        self,
        expr: str,
        event: NormalizedEvent | Mapping[str, Any],
        *,
        subscription_id: str,
        clock: Clock | None = None,
    ) -> bool:
        """Compile (cached) + evaluate ``expr`` against ``event`` in one call."""
        compiled = self.compile(expr, subscription_id=subscription_id)
        return self.evaluate(compiled, event, clock=clock)

    def clear(self) -> None:
        """Drop all cached compiled selectors."""
        self._cache.clear()
