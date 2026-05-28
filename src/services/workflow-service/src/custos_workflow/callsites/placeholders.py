"""Placeholder extractor for ``${{ ... }}`` segments (WF-IMPL-020).

A ``with:`` value (or any future free-text field that admits
embedded placeholders) is a *mixed string* — arbitrary literal text
interleaved with one or more ``${{ ... }}`` CEL segments. The
collector calls :func:`extract_placeholders` on every string scalar
under such fields and turns the returned segments into
:class:`~custos_workflow.callsites.CallSite` records.

The scanner is deliberately small and CEL-aware enough to be
correct on real workflows:

- ``${{`` opens a placeholder; the corresponding ``}}`` closes it.
- Curly braces inside string literals (``'...'`` / ``"..."``) do
  NOT participate in the brace-matching — that is how an author
  writes ``${{ {'a': '}}'}.a }}`` without truncating the placeholder
  at the first ``}}`` inside the string.
- Bare curly braces inside the expression (e.g. map literals like
  ``${{ {'a': 1, 'b': 2}.a }}``) raise and lower a depth counter so
  the closing ``}}`` only fires at depth zero.
- A backslash-escaped opener (``\\${{``) yields a literal ``${{``
  and is not treated as a call site, matching the standard escape
  convention used by templating engines.

The scanner does NOT attempt to validate the inner CEL — that is
:func:`custos_cel.parse`'s job. We only need to identify the
**boundaries** of each placeholder so the parser receives one
expression at a time.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class PlaceholderSegment:
    """One ``${{ ... }}`` occurrence inside a mixed string.

    Attributes:
        token: The verbatim segment INCLUDING the ``${{`` / ``}}``
            wrappers. Equal to ``text[start:end]``. Stored so the
            caller can reproduce the source byte-for-byte without
            re-slicing.
        inner: The CEL expression text WITHOUT the wrapper. This is
            what :func:`custos_cel.parse` expects.
        start: Character offset of the opening ``$`` within the
            input string. Used as
            :attr:`~custos_workflow.callsites.SourcePosition.text_offset`.
        end: Character offset of one past the final ``}`` within
            the input string. ``text[start:end] == token``.
    """

    token: str
    inner: str
    start: int
    end: int


def extract_placeholders(text: str) -> list[PlaceholderSegment]:
    """Return every ``${{ ... }}`` segment in ``text`` in source order.

    The scanner runs a single left-to-right pass. It is reentrant and
    side-effect free — calling it on the same string is byte-stable,
    which the determinism contract on the Definition Compiler
    requires (the resulting :class:`PlaceholderSegment` list is
    consumed in iteration order and the compiler hashes the result).

    Args:
        text: A string scalar from a ``with:`` value (or any field
            whose grammar allows interleaved CEL placeholders).

    Returns:
        A list of :class:`PlaceholderSegment` records, one per
        unescaped ``${{ ... }}`` occurrence, in source order. Empty
        for plain-literal strings.

    Raises:
        ValueError: If the scanner finds an unterminated ``${{``
            (no matching ``}}`` before end of string). This is a
            *structural* error — the document would fail Catalog
            validation in production — but we surface it cleanly
            here so the call-site collector can re-raise it with a
            :class:`~custos_workflow.callsites.CallSite` breadcrumb.
    """
    segments: list[PlaceholderSegment] = []
    i = 0
    n = len(text)
    while i < n:
        ch = text[i]
        # Backslash escape — consume the escape and the next char as
        # a literal. This matches the common templating escape
        # convention (``\${{`` is the canonical way to write a
        # literal ``${{`` in a mixed string).
        if ch == "\\" and i + 1 < n:
            i += 2
            continue
        # Opening ``${{``?
        if ch == "$" and text.startswith("${{", i):
            end = _find_closing_braces(text, i + 3)
            if end == -1:
                raise ValueError(
                    f"unterminated '${{{{' placeholder at offset {i}: no matching '}}}}' found"
                )
            inner = text[i + 3 : end]
            segments.append(
                PlaceholderSegment(
                    token=text[i : end + 2],
                    inner=inner,
                    start=i,
                    end=end + 2,
                ),
            )
            i = end + 2
            continue
        i += 1
    return segments


def _find_closing_braces(text: str, start: int) -> int:
    """Locate the ``}}`` that closes a placeholder opened at ``start - 3``.

    The scanner is CEL-string-aware (so a ``}}`` inside a quoted
    string literal does not close the placeholder) and tracks brace
    depth so map literals (``{'a': 1}``) inside the expression do
    not prematurely close it.

    Args:
        text: The full source string.
        start: Offset of the first character INSIDE the placeholder
            (i.e. one past the opening ``${{``).

    Returns:
        The offset of the FIRST ``}`` of the matching ``}}`` pair.
        Caller adds 2 to skip past the close. Returns ``-1`` if no
        matching close is found before end of string.
    """
    depth = 0
    # CEL string state: ``None`` outside strings; otherwise the
    # opening quote character (``'`` or ``"``).
    quote: str | None = None
    i = start
    n = len(text)
    while i < n:
        ch = text[i]
        if quote is not None:
            # Inside a string literal — only ``\`` and the matching
            # quote affect state.
            if ch == "\\" and i + 1 < n:
                i += 2
                continue
            if ch == quote:
                quote = None
            i += 1
            continue
        # Outside any string.
        if ch in ("'", '"'):
            quote = ch
            i += 1
            continue
        if ch == "{":
            depth += 1
            i += 1
            continue
        if ch == "}":
            if depth > 0:
                depth -= 1
                i += 1
                continue
            # depth == 0 — this ``}`` is a candidate for the closing
            # ``}}``. Only consumes if followed by another ``}``;
            # otherwise the expression is malformed and we let the
            # CEL parser surface the diagnostic.
            if i + 1 < n and text[i + 1] == "}":
                return i
            # A single ``}`` at depth 0 is structurally invalid in a
            # placeholder; treat it as not-yet-closing so the
            # eventual unterminated error fires at end of string.
            i += 1
            continue
        i += 1
    return -1
