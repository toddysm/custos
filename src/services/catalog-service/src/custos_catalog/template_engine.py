"""Template Engine (CS-IMPL-013).

Materialization takes a WorkflowTemplate document plus a mapping of
placeholder bindings and produces a concrete Workflow document. The
key invariants:

* Substitution is **textual at the document level**, not CEL
  evaluation. The engine walks the canonicalized template body and
  replaces every scalar string that is *exactly* a
  ``${{ placeholders.<name> }}`` token with the bound value. The
  bound value retains its native JSON type — an ``integer``
  placeholder produces an integer in the output, not a stringified
  integer.

* Tokens that appear inside a larger expression (e.g. ``"prefix-${{
  placeholders.foo }}"``) are **not** substituted. Compound
  expressions belong to CEL and are evaluated at workflow run time;
  the materializer never blends placeholder bindings into surrounding
  text.

* Non-placeholder ``${{ ... }}`` expressions — ``${{ inputs.x }}``,
  ``${{ steps.y.outputs.z }}``, etc. — are passed through unchanged.
  They are workflow expressions, not template placeholders, and they
  evaluate later in the workflow runtime.

The engine emits a Workflow document with::

    apiVersion: custos.dev/v1
    kind: Workflow
    metadata: {name: <target_name>, [workspace]: <inherited>}
    spec: <rendered spec.workflow body>

The publish pipeline runs against this document; a template that
materializes to an invalid workflow surfaces a structured error at
the materialize step, not at run time (design § Operation: Materialize
Workflow from Template).

CS-IMPL-014 (extractor) consumes the same data shapes in reverse.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final

#: Whole-string token: matches scalars that are *only* a
#: ``${{ placeholders.<name> }}`` reference (with optional whitespace
#: inside and around the ``${{ ... }}``). The captured group is the
#: placeholder name. We deliberately do not anchor with ``\A`` /
#: ``\Z`` since :meth:`re.Pattern.fullmatch` already enforces that.
_WHOLE_STRING_TOKEN: Final[re.Pattern[str]] = re.compile(
    r"\s*\$\{\{\s*placeholders\.([a-zA-Z][a-zA-Z0-9_]*)\s*\}\}\s*",
)

#: Embedded token: matches any ``${{ placeholders.<name> }}`` reference
#: anywhere in a string. Used only to detect (and reject) compound
#: expressions; the materializer never substitutes embedded tokens.
_EMBEDDED_TOKEN: Final[re.Pattern[str]] = re.compile(
    r"\$\{\{\s*placeholders\.([a-zA-Z][a-zA-Z0-9_]*)\s*\}\}",
)


class TemplateRenderError(ValueError):
    """Raised when a template cannot be rendered with the supplied bindings.

    The materializer wraps this in a higher-level
    :class:`MaterializationError` (CS-IMPL-013 manager surface), but
    direct callers of :func:`render` see the underlying issues.
    """

    code: str = "catalog.template_render_failed"

    def __init__(self, issues: list[TemplateRenderIssue]) -> None:
        self.issues = list(issues)
        rendered = "; ".join(f"{i.path or '<root>'} -> {i.message}" for i in self.issues)
        super().__init__(f"{len(self.issues)} render issue(s): {rendered}")


@dataclass(frozen=True, slots=True)
class TemplateRenderIssue:
    """One render-time issue.

    Attributes:
        code: Stable machine-readable code (one of
            ``"unbound_placeholder"``, ``"embedded_placeholder"``, or
            ``"invalid_template"``).
    """

    path: str
    code: str
    message: str


def render(
    template_doc: Mapping[str, Any],
    bindings: Mapping[str, Any],
    *,
    target_workflow_name: str,
) -> dict[str, Any]:
    """Materialize ``template_doc`` with ``bindings`` into a Workflow document.

    Args:
        template_doc: The (normalized) template document. The
            ``spec.workflow`` body is rendered; the ``spec.placeholders``
            block is dropped from the output.
        bindings: A mapping of placeholder name → concrete value.
            Defaults from the declarations are *not* applied here;
            callers should feed the output of
            :func:`custos_catalog.placeholders.effective_bindings`.
        target_workflow_name: ``metadata.name`` for the materialized
            workflow. The caller picks the name (the design's
            ``targetName`` argument on
            ``POST /v1/.../templates/{id}:materialize``).

    Returns:
        A dict suitable for :meth:`DefinitionManager.publish_workflow`.

    Raises:
        TemplateRenderError: When any token references an unbound
            placeholder, or when a placeholder token appears embedded
            inside a larger string expression.
    """
    spec = template_doc.get("spec", {})
    if not isinstance(spec, Mapping):  # pragma: no cover - schema gate
        raise TemplateRenderError(
            [
                TemplateRenderIssue(
                    path="spec",
                    code="invalid_template",
                    message="template spec must be an object",
                ),
            ],
        )
    inner = spec.get("workflow")
    if not isinstance(inner, Mapping):  # pragma: no cover - schema gate
        raise TemplateRenderError(
            [
                TemplateRenderIssue(
                    path="spec/workflow",
                    code="invalid_template",
                    message="template spec.workflow must be an object",
                ),
            ],
        )

    issues: list[TemplateRenderIssue] = []
    rendered_spec = _walk(inner, bindings, path=("spec", "workflow"), issues=issues)
    if issues:
        raise TemplateRenderError(issues)

    metadata: dict[str, Any] = {"name": target_workflow_name}
    template_metadata = template_doc.get("metadata", {})
    if isinstance(template_metadata, Mapping):
        workspace = template_metadata.get("workspace")
        if isinstance(workspace, str):
            metadata["workspace"] = workspace

    return {
        "apiVersion": "custos.dev/v1",
        "kind": "Workflow",
        "metadata": metadata,
        "spec": rendered_spec,
    }


def _walk(
    node: Any,
    bindings: Mapping[str, Any],
    *,
    path: tuple[str | int, ...],
    issues: list[TemplateRenderIssue],
) -> Any:
    if isinstance(node, dict):
        return {k: _walk(v, bindings, path=(*path, k), issues=issues) for k, v in node.items()}
    if isinstance(node, list):
        return [
            _walk(item, bindings, path=(*path, idx), issues=issues) for idx, item in enumerate(node)
        ]
    if isinstance(node, str):
        return _substitute_scalar(node, bindings, path=path, issues=issues)
    return node


def _substitute_scalar(
    value: str,
    bindings: Mapping[str, Any],
    *,
    path: tuple[str | int, ...],
    issues: list[TemplateRenderIssue],
) -> Any:
    whole = _WHOLE_STRING_TOKEN.fullmatch(value)
    if whole is not None:
        name = whole.group(1)
        if name not in bindings:
            issues.append(
                TemplateRenderIssue(
                    path=_render_path(path),
                    code="unbound_placeholder",
                    message=f"placeholder {name!r} has no binding",
                ),
            )
            return value
        return bindings[name]

    embedded = _EMBEDDED_TOKEN.search(value)
    if embedded is not None:
        # A placeholder token appears inside a compound expression
        # (e.g. ``"prefix-${{ placeholders.foo }}"``). Reject it —
        # combining placeholder bindings with surrounding text is the
        # job of CEL at run time, not the template engine.
        issues.append(
            TemplateRenderIssue(
                path=_render_path(path),
                code="embedded_placeholder",
                message=(
                    f"placeholder {embedded.group(1)!r} appears inside a compound "
                    "expression; materialization only substitutes whole-string tokens"
                ),
            ),
        )
    return value


def _render_path(path: tuple[str | int, ...]) -> str:
    return "/".join(str(p) for p in path)


__all__ = [
    "TemplateRenderError",
    "TemplateRenderIssue",
    "render",
]
