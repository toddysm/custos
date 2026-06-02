"""Definition-Compiler call-site collector (WF-IMPL-020).

Walks a parsed :class:`~custos_workflow.document.WorkflowDocument`
and emits a flat ``{step_id: [CallSite, ...]}`` mapping covering
every CEL call site documented in
``docs/developers/cel-expressions.md`` § *Where expressions are
used*. The walk is deterministic and structural — it dispatches
strictly off the step's Pydantic class and known field names — so
the collector's output is byte-stable for a given input document
(a precondition for Dapr Workflow replay).

Field handling
==============

Direct CEL slots (one site per field):

- ``if`` / ``when`` / ``unless`` / ``forEach`` / ``where`` — every
  one is a complete ``${{ ... }}`` token (enforced by the document
  model). Emits one :class:`CallSite` per non-``None`` slot with
  :attr:`SourcePosition.text_offset` set to ``0``.
- ``waitFor.eventKey`` / ``waitFor.selector`` (:class:`WaitForStep`
  only) — each present value is a complete ``${{ ... }}`` token
  (enforced by the document model). Emits one ``WAIT_FOR_EVENT_KEY``
  / ``WAIT_FOR_SELECTOR`` :class:`CallSite` with ``text_offset = 0``.
  The companion ``waitFor.ttl`` is a constant ISO-8601 duration,
  not a CEL slot, so it is never collected.
- ``let.<name>`` (:class:`LetStep` only) — only string values that
  are complete ``${{ ... }}`` tokens are treated as CEL bindings.
  Non-string values (literals, nested objects) and string values
  without the wrapper are passed through as data; the document
  model already rejects shapes that are not authoring-legal, so
  the collector remains permissive.

Embedded placeholders (zero or more sites per field):

- ``with.<key>`` (:class:`ActivityStep` and :class:`WorkflowStep`)
  — string values are scanned by :func:`extract_placeholders` and
  one ``WITH``-kind :class:`CallSite` is emitted per ``${{ ... }}``
  segment. Non-string values pass through unchanged.

Parse failures
==============

If :func:`custos_cel.parse` raises while compiling a placeholder's
inner source, the collector re-raises as
:class:`CallSiteParseError` so the failing step / path / offset is
preserved in the chained traceback (``__cause__`` retains the
original :class:`custos_cel.ParseError`).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from custos_cel import ParseError
from custos_cel import parse as cel_parse

from custos_workflow.callsites.model import CallSite, SourcePosition
from custos_workflow.callsites.placeholders import (
    PlaceholderSegment,
    extract_placeholders,
)
from custos_workflow.document import ActivityStep, LetStep, WaitForStep, WorkflowStep
from custos_workflow.graph.model import CallSiteKind

if TYPE_CHECKING:
    from collections.abc import Iterator

    from custos_workflow.document import Step, WorkflowDocument


_CEL_TOKEN_PREFIX = "${{"
_CEL_TOKEN_SUFFIX = "}}"


class CallSiteParseError(ValueError):
    """Raised when a collected ``${{ ... }}`` fails to parse.

    Carries the failing step id and the path within the step
    (e.g. ``"with.image"``) so the compiler can surface a
    user-friendly diagnostic. The underlying
    :class:`custos_cel.ParseError` is kept as ``__cause__`` so the
    full lexer/parser trace is available for debugging.
    """

    def __init__(self, step_id: str, path: str, source: str, message: str) -> None:
        self.step_id = step_id
        self.path = path
        self.source = source
        super().__init__(
            f"step {step_id!r}: failed to parse CEL at {path!r}: {message} (source: {source!r})"
        )


def collect_call_sites(doc: WorkflowDocument) -> dict[str, list[CallSite]]:
    """Walk ``doc`` and return every CEL call site grouped by step id.

    Args:
        doc: A parsed :class:`WorkflowDocument`. Step uniqueness is
            already enforced by the document model
            (:class:`WorkflowSpec._step_ids_unique`), so the returned
            mapping is unambiguous.

    Returns:
        A dict keyed by step id whose value is the list of
        :class:`CallSite` records for that step in source order.
        Steps with no CEL expressions appear in the dict with an
        empty list — downstream code can iterate
        ``doc.spec.steps`` and look each id up without a default.

    Raises:
        CallSiteParseError: If any collected ``${{ ... }}`` segment
            fails :func:`custos_cel.parse`. The chained cause
            preserves the original :class:`custos_cel.ParseError`.
    """
    out: dict[str, list[CallSite]] = {}
    for step_index, step in enumerate(doc.spec.steps):
        base_path = f"spec.steps[{step_index}]"
        sites: list[CallSite] = []
        sites.extend(_collect_common_slots(step, base_path))
        if isinstance(step, LetStep):
            sites.extend(_collect_let_bindings(step, base_path))
        elif isinstance(step, ActivityStep | WorkflowStep):
            sites.extend(_collect_with_block(step, base_path))
        elif isinstance(step, WaitForStep):
            sites.extend(_collect_wait_for(step, base_path))
        out[step.id] = sites
    return out


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


# Pairs of (CallSiteKind, attribute name on _StepCommon, wire-name used in
# document_path). The wire-name differs from the Python attribute for
# ``forEach`` / ``if``: the Pydantic model uses ``for_each`` / ``if_`` so
# the keyword/alias rules don't collide with Python syntax.
_COMMON_SLOTS: tuple[tuple[CallSiteKind, str, str], ...] = (
    (CallSiteKind.IF, "if_", "if"),
    (CallSiteKind.WHEN, "when", "when"),
    (CallSiteKind.UNLESS, "unless", "unless"),
    (CallSiteKind.FOR_EACH, "for_each", "forEach"),
    (CallSiteKind.WHERE, "where", "where"),
)


def _collect_common_slots(step: Step, base_path: str) -> Iterator[CallSite]:
    """Emit one :class:`CallSite` per non-``None`` ``_StepCommon`` CEL slot.

    The document model has already enforced that each value is a
    complete ``${{ ... }}`` token, so we strip the wrapper and parse
    the inner source directly. ``text_offset`` is always ``0`` since
    the entire field IS the call site.
    """
    for kind, attr, wire_name in _COMMON_SLOTS:
        value = getattr(step, attr, None)
        if value is None:
            continue
        path = wire_name
        document_path = f"{base_path}.{wire_name}"
        inner = _strip_wrapper(value)
        ast = _parse_or_raise(step.id, path, value, inner)
        yield CallSite(
            step_id=step.id,
            kind=kind,
            path=path,
            source=value,
            position=SourcePosition(document_path=document_path, text_offset=0),
            parsed_ast=ast,
        )


def _collect_let_bindings(step: LetStep, base_path: str) -> Iterator[CallSite]:
    """Emit one :class:`CallSite` per ``let.<name>`` string-CEL binding.

    ``LetStep.let`` is typed ``dict[str, Any]`` and the document
    model does NOT enforce the ``${{ ... }}`` wrapper for its
    values, so the discrimination happens here:

    - Non-string values (numbers, booleans, dicts, lists) are
      carried through as data and skipped.
    - Strings with no ``${{ ... }}`` segment are carried through
      as data and skipped.
    - Strings whose entire content is a single ``${{ ... }}``
      placeholder (allowing surrounding whitespace) become a
      ``LET``-kind call site.
    - Mixed-content strings (interleaved literals and
      placeholders, or multiple placeholders) are rejected as
      :class:`CallSiteParseError`. A ``let`` binding is a single
      expression by design, so a mixed string is almost always a
      typo; raising eagerly here gives a clean diagnostic instead
      of feeding wrapper-stripped garbage to ``custos_cel.parse``.
    """
    for name, value in step.let.items():
        if not isinstance(value, str):
            continue
        path = f"let.{name}"
        try:
            segments = extract_placeholders(value)
        except ValueError as exc:
            raise CallSiteParseError(step.id, path, value, str(exc)) from exc
        if not segments:
            # Plain literal data — carried through, no call site.
            continue
        if len(segments) == 1 and _segment_covers_value(value, segments[0]):
            segment = segments[0]
            document_path = f"{base_path}.let.{name}"
            ast = _parse_or_raise(step.id, path, value, segment.inner)
            yield CallSite(
                step_id=step.id,
                kind=CallSiteKind.LET,
                path=path,
                source=value,
                position=SourcePosition(
                    document_path=document_path,
                    text_offset=0,
                ),
                parsed_ast=ast,
            )
            continue
        # Multiple placeholders OR a single placeholder with
        # surrounding non-whitespace literal text. Either shape is
        # malformed for a ``let`` binding (which is a single
        # expression by design).
        raise CallSiteParseError(
            step.id,
            path,
            value,
            "let bindings must be either a literal value or a single "
            "'${{ ... }}' expression; mixed-content strings are not "
            "supported under let:",
        )


def _collect_wait_for(step: WaitForStep, base_path: str) -> Iterator[CallSite]:
    """Emit one :class:`CallSite` per CEL slot on a ``waitFor:`` block.

    A ``waitFor:`` step carries up to two CEL expressions on its
    :class:`~custos_workflow.document.WaitForSpec`:

    - ``eventKey`` (required) → :attr:`CallSiteKind.WAIT_FOR_EVENT_KEY`
    - ``selector`` (optional) → :attr:`CallSiteKind.WAIT_FOR_SELECTOR`

    The document model has already enforced that each present value
    is a complete ``${{ ... }}`` token, so we strip the wrapper and
    parse the inner source directly with ``text_offset = 0`` (the
    entire field IS the call site, like the common slots). The
    ``ttl`` field is a constant ISO-8601 duration, not a CEL
    expression, so it is never collected.
    """
    spec = step.wait_for
    slots: tuple[tuple[CallSiteKind, str, str, str | None], ...] = (
        (CallSiteKind.WAIT_FOR_EVENT_KEY, "waitFor.eventKey", "waitFor.eventKey", spec.event_key),
        (CallSiteKind.WAIT_FOR_SELECTOR, "waitFor.selector", "waitFor.selector", spec.selector),
    )
    for kind, path, wire_name, value in slots:
        if value is None:
            continue
        document_path = f"{base_path}.{wire_name}"
        inner = _strip_wrapper(value)
        ast = _parse_or_raise(step.id, path, value, inner)
        yield CallSite(
            step_id=step.id,
            kind=kind,
            path=path,
            source=value,
            position=SourcePosition(document_path=document_path, text_offset=0),
            parsed_ast=ast,
        )


def _collect_with_block(step: ActivityStep | WorkflowStep, base_path: str) -> Iterator[CallSite]:
    """Emit one :class:`CallSite` per ``${{ ... }}`` placeholder under ``with``.

    A single ``with:`` key may admit zero, one, or many embedded
    placeholders. The collector scans each string scalar with
    :func:`extract_placeholders` and emits a ``WITH``-kind call site
    per segment, in source order, each carrying the character
    offset of its opening ``${{``.
    """
    with_block = step.with_
    if with_block is None:
        return
    for key, value in with_block.items():
        if not isinstance(value, str):
            continue
        try:
            segments = extract_placeholders(value)
        except ValueError as exc:
            raise CallSiteParseError(step.id, f"with.{key}", value, str(exc)) from exc
        yield from _emit_with_segments(step.id, base_path, key, value, segments)


def _emit_with_segments(
    step_id: str,
    base_path: str,
    key: str,
    value: str,
    segments: list[PlaceholderSegment],
) -> Iterator[CallSite]:
    """Turn the placeholder segments of a single ``with`` value into CallSites."""
    multiple = len(segments) > 1
    for idx, segment in enumerate(segments):
        path = f"with.{key}[{idx}]" if multiple else f"with.{key}"
        document_path = f"{base_path}.with.{key}"
        ast = _parse_or_raise(step_id, path, segment.token, segment.inner)
        yield CallSite(
            step_id=step_id,
            kind=CallSiteKind.WITH,
            path=path,
            source=segment.token,
            position=SourcePosition(
                document_path=document_path,
                text_offset=segment.start,
            ),
            parsed_ast=ast,
        )


def _segment_covers_value(value: str, segment: PlaceholderSegment) -> bool:
    """Return ``True`` iff ``segment`` spans the whole of ``value``.

    Leading and trailing whitespace OUTSIDE the placeholder is
    tolerated so a YAML scalar like ``"  ${{ x }}  "`` still counts
    as a single CEL expression — whitespace is not significant in
    CEL and a workflow author legitimately may insert it for
    readability. Any non-whitespace literal text around the
    placeholder disqualifies the value as a single-CEL binding;
    this is what guards against the false positive on values like
    ``"${{ a }}-${{ b }}"`` (which both starts with ``${{`` and
    ends with ``}}`` but is actually two distinct placeholders).
    """
    before = value[: segment.start]
    after = value[segment.end :]
    return before.strip() == "" and after.strip() == ""


def _strip_wrapper(token: str) -> str:
    """Return the inner CEL expression text of a complete ``${{ ... }}`` token.

    Used only by :func:`_collect_common_slots` — each common slot
    has already been wrapper-validated by the document model
    (``_StepCommon._check_cel_wrappers``) so the assumption holds
    structurally. The leading / trailing whitespace of the token
    is preserved so the parser sees the same source the author
    wrote (CEL is whitespace-insensitive but the invariant aids
    debugging).
    """
    stripped = token.strip()
    return stripped[len(_CEL_TOKEN_PREFIX) : -len(_CEL_TOKEN_SUFFIX)]


def _parse_or_raise(step_id: str, path: str, source: str, inner: str) -> Any:
    """Parse ``inner`` via :func:`custos_cel.parse`, re-raising on failure.

    The chained :class:`CallSiteParseError` carries the locator so
    callers see ``step 'scan': failed to parse CEL at 'with.image':
    …`` rather than a bare CEL ``ParseError`` with no idea where in
    the document the offending source lives.
    """
    try:
        return cel_parse(inner)
    except ParseError as exc:
        raise CallSiteParseError(step_id, path, source, str(exc)) from exc
