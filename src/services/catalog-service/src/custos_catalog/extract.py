"""Template-from-Workflow Extractor (CS-IMPL-014).

The extractor is the inverse of the template engine: given a published
:class:`WorkflowVersion` and a list of :class:`Selector` records, it
rewrites the selected scalars with ``${{ placeholders.<name> }}``
tokens, emits the matching ``placeholders[]`` declaration block, and
verifies the round-trip property — re-materializing the extracted
template with the captured original values reproduces the source
workflow byte-for-byte after canonicalization (ADR-009).

The selector grammar is a deliberate subset of JSONPath:

* Dotted identifiers: ``spec.steps``
* Numeric indices: ``spec.steps[0]``
* Wildcard over list elements: ``spec.steps[*].activity``

The wildcard form matches every element of a list and requires every
matched scalar to be equal — the substitution is reversible only when
a single binding can be substituted back into every slot. Mixed
values reject as ``inhomogeneous_wildcard`` issues.

Round-trip enforcement lives in :func:`self_check_roundtrip`, called
by :meth:`TemplateManager.extract_from_workflow` immediately after
the rewrite. Any mismatch surfaces a :class:`RoundtripViolation`
carrying a unified diff against the canonicalized form so callers can
see exactly which fields drifted (per ADR-009 § Round-Trip Property).
"""

from __future__ import annotations

import copy
import difflib
import json
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Any, Final

from custos_catalog.normalize import (
    canonical_hash,
    normalize_workflow,
)
from custos_catalog.placeholders import PlaceholderType
from custos_catalog.template_engine import render

#: Sentinel for the ``[*]`` segment in a parsed selector path.
_WILDCARD: Final[object] = object()


@dataclass(frozen=True, slots=True)
class Selector:
    """A single extraction directive.

    Attributes:
        path: Dotted path into the workflow document (relative to the
            workflow root, e.g. ``"spec.steps[0].activity"``). Supports
            integer indices and the ``[*]`` wildcard.
        placeholder_name: Name of the resulting placeholder (e.g.
            ``"scanActivity"``). Must be unique across the selector set.
        placeholder_type: One of the seven placeholder types accepted
            by :class:`custos_catalog.placeholders.PlaceholderDeclaration`.
        required: When ``False`` and a ``default`` is supplied, the
            resulting declaration carries the default.
        default: Optional default for non-required placeholders.
        connector_type: Required when ``placeholder_type`` is
            ``"connectorRef"``.
        activity_type: Required when ``placeholder_type`` is
            ``"activityRef"``.
        description: Optional human-readable note carried through.
    """

    path: str
    placeholder_name: str
    placeholder_type: PlaceholderType
    required: bool = True
    default: Any | None = None
    connector_type: str | None = None
    activity_type: str | None = None
    description: str | None = None


@dataclass(frozen=True, slots=True)
class ExtractIssue:
    """One extraction-time issue.

    Attributes:
        path: Selector path (or ``""`` for cross-selector issues).
        code: One of ``"invalid_path"``, ``"no_match"``,
            ``"non_scalar_target"``, ``"inhomogeneous_wildcard"``,
            ``"duplicate_placeholder_name"``.
        message: Human-readable explanation.
    """

    path: str
    code: str
    message: str


class ExtractError(ValueError):
    """Raised when an extraction request cannot be fulfilled."""

    code: str = "catalog.template_extract_failed"

    def __init__(self, issues: list[ExtractIssue]) -> None:
        self.issues = list(issues)
        rendered = "; ".join(f"{i.path or '<root>'} -> {i.message}" for i in self.issues)
        super().__init__(f"{len(self.issues)} extract issue(s): {rendered}")


class RoundtripViolation(Exception):
    """Raised by :func:`self_check_roundtrip` when re-materialization drifts.

    Carries a unified diff of canonicalized JSON so the caller can
    pinpoint the offending field.
    """

    code: str = "catalog.template_roundtrip_violation"

    def __init__(self, *, diff: str) -> None:
        self.diff = diff
        super().__init__(f"extracted template does not round-trip:\n{diff}")


def extract(
    workflow_doc: Mapping[str, Any],
    selectors: list[Selector],
    *,
    template_name: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Extract a WorkflowTemplate from a workflow document.

    Args:
        workflow_doc: The published workflow document. Treated as
            immutable — the extractor deep-copies before rewriting.
        selectors: Ordered list of extraction directives. Selectors
            are applied in order; later selectors see the rewrites of
            earlier ones (though in practice selectors target
            disjoint paths).
        template_name: ``metadata.name`` for the resulting template.

    Returns:
        A ``(template_doc, captured_bindings)`` tuple. The
        ``captured_bindings`` dict is the inverse of the rewrite —
        feeding it back into :func:`custos_catalog.template_engine.render`
        reproduces the source workflow.

    Raises:
        ExtractError: With one issue per problematic selector. The
            extractor is collect-all: every selector is attempted
            even when earlier ones fail.
    """
    out = copy.deepcopy(dict(workflow_doc))
    captured: dict[str, Any] = {}
    declarations: list[dict[str, Any]] = []
    issues: list[ExtractIssue] = []

    for sel in selectors:
        try:
            segments = _split_path(sel.path)
        except ValueError as exc:
            issues.append(
                ExtractIssue(path=sel.path, code="invalid_path", message=str(exc)),
            )
            continue
        if not segments:
            issues.append(
                ExtractIssue(
                    path=sel.path,
                    code="invalid_path",
                    message="selector path must not be empty",
                ),
            )
            continue
        matches = list(_navigate(out, segments))
        if not matches:
            issues.append(
                ExtractIssue(
                    path=sel.path,
                    code="no_match",
                    message="selector matched no values",
                ),
            )
            continue
        values = [m[2] for m in matches]
        non_scalars = [v for v in values if not _is_scalar(v)]
        if non_scalars:
            issues.append(
                ExtractIssue(
                    path=sel.path,
                    code="non_scalar_target",
                    message=(
                        "selector targets non-scalar value(s); only "
                        "strings, numbers, booleans, and null are allowed"
                    ),
                ),
            )
            continue
        if _has_wildcard(segments):
            distinct = {_freeze(v) for v in values}
            if len(distinct) > 1:
                issues.append(
                    ExtractIssue(
                        path=sel.path,
                        code="inhomogeneous_wildcard",
                        message=(
                            "wildcard selector matched differing values "
                            f"({len(distinct)} distinct); a single binding "
                            "cannot round-trip into mixed slots"
                        ),
                    ),
                )
                continue
        token = f"${{{{ placeholders.{sel.placeholder_name} }}}}"
        for parent, key, _value in matches:
            parent[key] = token
        captured[sel.placeholder_name] = values[0]
        declarations.append(_build_declaration(sel))

    names = [d["name"] for d in declarations]
    if len(set(names)) != len(names):
        issues.append(
            ExtractIssue(
                path="",
                code="duplicate_placeholder_name",
                message="selectors produced duplicate placeholder names",
            ),
        )

    if issues:
        raise ExtractError(issues)

    template_doc: dict[str, Any] = {
        "apiVersion": "custos.dev/v1",
        "kind": "WorkflowTemplate",
        "metadata": _carry_metadata(workflow_doc, template_name),
        "spec": {
            "placeholders": declarations,
            "workflow": out.get("spec", {}),
        },
    }
    return template_doc, captured


def self_check_roundtrip(
    template_doc: Mapping[str, Any],
    original_workflow: Mapping[str, Any],
    captured_bindings: Mapping[str, Any],
) -> None:
    """Verify the extracted template round-trips into the original workflow.

    Re-materializes ``template_doc`` with ``captured_bindings`` via
    :func:`custos_catalog.template_engine.render`, normalizes both
    documents, and compares :func:`canonical_hash`. A mismatch raises
    :class:`RoundtripViolation` with a unified diff.

    The extractor's guarantee is byte-equality after canonicalization;
    cosmetic differences (key ordering, whitespace) are absorbed by
    :func:`normalize_workflow`.

    Args:
        template_doc: The just-extracted template.
        original_workflow: The source workflow document (raw, not
            yet normalized).
        captured_bindings: The mapping returned by :func:`extract`.
    """
    target_name = ""
    metadata = original_workflow.get("metadata", {})
    if isinstance(metadata, Mapping):
        raw_name = metadata.get("name")
        if isinstance(raw_name, str):
            target_name = raw_name
    rendered = render(template_doc, captured_bindings, target_workflow_name=target_name)
    rendered_normalized = normalize_workflow(rendered)
    original_normalized = normalize_workflow(dict(original_workflow))
    if canonical_hash(rendered_normalized.document) == canonical_hash(
        original_normalized.document,
    ):
        return
    diff = _unified_diff(
        original_normalized.document,
        rendered_normalized.document,
    )
    raise RoundtripViolation(diff=diff)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _split_path(path: str) -> list[Any]:
    """Tokenize a dotted selector path into a list of segments.

    Returns segments of three kinds:

    * ``str`` — dict key,
    * ``int`` — list index,
    * :data:`_WILDCARD` — ``[*]`` over a list.
    """
    if not isinstance(path, str):  # pragma: no cover - typing gate
        raise ValueError("selector path must be a string")
    segments: list[Any] = []
    i = 0
    n = len(path)
    while i < n:
        ch = path[i]
        if ch == ".":
            i += 1
            continue
        if ch == "[":
            close = path.find("]", i)
            if close == -1:
                raise ValueError(f"unterminated '[' in path {path!r}")
            inner = path[i + 1 : close]
            if inner == "*":
                segments.append(_WILDCARD)
            else:
                try:
                    segments.append(int(inner))
                except ValueError as exc:
                    raise ValueError(
                        f"invalid index {inner!r} in path {path!r}",
                    ) from exc
            i = close + 1
            continue
        # Identifier
        j = i
        while j < n and path[j] not in ".[":
            j += 1
        ident = path[i:j]
        if not ident:
            raise ValueError(f"empty identifier at position {i} in path {path!r}")
        segments.append(ident)
        i = j
    return segments


def _navigate(
    node: Any,
    segments: list[Any],
) -> Iterator[tuple[Any, Any, Any]]:
    """Yield ``(parent, last_key, value)`` for every selector match.

    The yielded tuple targets a *mutable* parent container (a list or
    dict) so the extractor can rewrite the slot in place.
    """
    if len(segments) == 1:
        seg = segments[0]
        if seg is _WILDCARD:
            if isinstance(node, list):
                for idx, value in enumerate(node):
                    yield (node, idx, value)
            return
        if isinstance(seg, int):
            if isinstance(node, list) and 0 <= seg < len(node):
                yield (node, seg, node[seg])
            return
        if isinstance(seg, str):
            if isinstance(node, dict) and seg in node:
                yield (node, seg, node[seg])
            return
        return
    head, *rest = segments
    if head is _WILDCARD:
        if isinstance(node, list):
            for value in node:
                yield from _navigate(value, rest)
        return
    if isinstance(head, int):
        if isinstance(node, list) and 0 <= head < len(node):
            yield from _navigate(node[head], rest)
        return
    if isinstance(head, str) and isinstance(node, dict) and head in node:
        yield from _navigate(node[head], rest)


def _has_wildcard(segments: list[Any]) -> bool:
    return any(s is _WILDCARD for s in segments)


def _is_scalar(value: Any) -> bool:
    if isinstance(value, bool):  # bool is a subclass of int; treat separately
        return True
    return isinstance(value, (str, int, float, type(None)))


def _freeze(value: Any) -> Any:
    """Deep-freeze a JSON value so it can live in a ``set``."""
    if isinstance(value, dict):
        return tuple(sorted((k, _freeze(v)) for k, v in value.items()))
    if isinstance(value, list):
        return tuple(_freeze(v) for v in value)
    return value


def _build_declaration(selector: Selector) -> dict[str, Any]:
    decl: dict[str, Any] = {
        "name": selector.placeholder_name,
        "type": selector.placeholder_type,
        "required": selector.required,
    }
    if selector.default is not None:
        decl["default"] = selector.default
    if selector.description is not None:
        decl["description"] = selector.description
    if selector.connector_type is not None:
        decl["connectorType"] = selector.connector_type
    if selector.activity_type is not None:
        decl["activityType"] = selector.activity_type
    return decl


def _carry_metadata(
    workflow_doc: Mapping[str, Any],
    template_name: str,
) -> dict[str, Any]:
    metadata: dict[str, Any] = {"name": template_name}
    source_metadata = workflow_doc.get("metadata", {})
    if isinstance(source_metadata, Mapping):
        workspace = source_metadata.get("workspace")
        if isinstance(workspace, str):
            metadata["workspace"] = workspace
    return metadata


def _unified_diff(left: Mapping[str, Any], right: Mapping[str, Any]) -> str:
    left_lines = json.dumps(dict(left), indent=2, sort_keys=True).splitlines(keepends=True)
    right_lines = json.dumps(dict(right), indent=2, sort_keys=True).splitlines(keepends=True)
    return "".join(
        difflib.unified_diff(
            left_lines,
            right_lines,
            fromfile="original (canonicalized)",
            tofile="re-materialized",
            n=3,
        ),
    )


__all__ = [
    "ExtractError",
    "ExtractIssue",
    "RoundtripViolation",
    "Selector",
    "extract",
    "self_check_roundtrip",
]
