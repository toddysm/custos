"""``validate_workflow`` / ``validate_template`` and YAML/JSON loaders.

The validators wrap ``jsonschema.Draft202012Validator`` and collect
**every** error in one pass (no first-error short-circuit) so the API
gateway can return the full set in a single response. Each error
surfaces as a :class:`SchemaValidationIssue` carrying the JSON-Pointer
path to the offending field and the validator-supplied message.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import yaml
from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError

from custos_catalog.schema.template import TEMPLATE_SCHEMA
from custos_catalog.schema.workflow import WORKFLOW_SCHEMA


@dataclass(frozen=True, slots=True)
class SchemaValidationIssue:
    """One field-level schema violation.

    Attributes:
        path: JSON-Pointer-style path to the offending element
            (e.g. ``"spec/steps/2/activity"``). Empty string for
            errors at the document root.
        message: The validator-supplied human-readable message.
        validator: The JSON Schema keyword that produced the error
            (e.g. ``"required"``, ``"pattern"``, ``"oneOf"``). Useful
            for clients that want to map errors to remediations.
    """

    path: str
    message: str
    validator: str

    @classmethod
    def from_validation_error(cls, exc: ValidationError) -> SchemaValidationIssue:
        """Build a :class:`SchemaValidationIssue` from a ``jsonschema`` error."""
        # ``absolute_path`` is a deque of segments. Render as a
        # JSON-Pointer-like slash-joined string, escaping per RFC 6901.
        parts: list[str] = []
        for segment in exc.absolute_path:
            seg = str(segment)
            seg = seg.replace("~", "~0").replace("/", "~1")
            parts.append(seg)
        path = "/".join(parts)
        return cls(
            path=path,
            message=exc.message,
            validator=str(exc.validator),
        )


class SchemaValidationError(ValueError):
    """Base class for the workflow / template schema validators.

    Carries the full list of :class:`SchemaValidationIssue` objects so
    operators see every problem at once.
    """

    def __init__(self, kind: str, issues: list[SchemaValidationIssue]) -> None:
        self.kind = kind
        self.issues = issues
        # Render a single-line summary for ``str(exc)``: ``N issue(s):
        # path -> msg; path -> msg``. The full list lives on
        # :attr:`issues` for JSON serialisation by callers.
        rendered = "; ".join(f"{issue.path or '<root>'} -> {issue.message}" for issue in issues)
        super().__init__(f"{kind}: {len(issues)} issue(s): {rendered}")


class WorkflowSchemaError(SchemaValidationError):
    """Raised when ``validate_workflow`` finds at least one violation."""

    def __init__(self, issues: list[SchemaValidationIssue]) -> None:
        super().__init__("workflow schema validation failed", issues)


class TemplateSchemaError(SchemaValidationError):
    """Raised when ``validate_template`` finds at least one violation."""

    def __init__(self, issues: list[SchemaValidationIssue]) -> None:
        super().__init__("template schema validation failed", issues)


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------


class DocumentParseError(ValueError):
    """Raised when the supplied document is neither valid JSON nor YAML."""


def load_document(source: str | bytes) -> dict[str, Any]:
    """Parse ``source`` (JSON or YAML) into a Python ``dict``.

    The API gateway forwards the raw publish body verbatim. JSON is
    tried first (so structured posts skip the YAML parser entirely),
    falling back to ``yaml.safe_load`` which also accepts JSON as a
    superset.

    Raises:
        DocumentParseError: when the input is not parseable as JSON or
            YAML, or when it does not decode to a mapping (a list or
            scalar at the document root is rejected here so downstream
            validators always see a ``dict``).
    """
    if isinstance(source, bytes):
        try:
            text = source.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise DocumentParseError(f"document is not valid UTF-8: {exc}") from exc
    else:
        text = source
    try:
        parsed: Any = json.loads(text)
    except json.JSONDecodeError:
        try:
            parsed = yaml.safe_load(text)
        except yaml.YAMLError as exc:
            raise DocumentParseError(f"document is neither valid JSON nor YAML: {exc}") from exc
    if not isinstance(parsed, dict):
        raise DocumentParseError(
            "workflow / template document must decode to a JSON object at the root, "
            f"got {type(parsed).__name__}",
        )
    return parsed


# ---------------------------------------------------------------------------
# Validators
# ---------------------------------------------------------------------------


def _collect(validator: Draft202012Validator, doc: dict[str, Any]) -> list[SchemaValidationIssue]:
    return [
        SchemaValidationIssue.from_validation_error(err)
        for err in sorted(validator.iter_errors(doc), key=lambda e: list(e.absolute_path))
    ]


_workflow_validator: Draft202012Validator = Draft202012Validator(WORKFLOW_SCHEMA)
_template_validator: Draft202012Validator = Draft202012Validator(TEMPLATE_SCHEMA)


def validate_workflow(doc: dict[str, Any]) -> None:
    """Validate ``doc`` against :data:`WORKFLOW_SCHEMA`.

    Raises:
        WorkflowSchemaError: when at least one schema violation is
            detected. The exception's ``issues`` list carries every
            error in a single pass (no first-error short-circuit).
    """
    issues = _collect(_workflow_validator, doc)
    if issues:
        raise WorkflowSchemaError(issues)


def validate_template(doc: dict[str, Any]) -> None:
    """Validate ``doc`` against :data:`TEMPLATE_SCHEMA`.

    Per ADR-009 the template body is a ``Workflow`` ``spec`` (without
    its own ``placeholders``) wrapped under ``spec.workflow``; we
    validate the template wrapper here and rely on the wrapped spec
    being structurally compatible because it shares the helpers in
    :mod:`custos_catalog.schema.workflow`.
    """
    issues = _collect(_template_validator, doc)
    if issues:
        raise TemplateSchemaError(issues)
