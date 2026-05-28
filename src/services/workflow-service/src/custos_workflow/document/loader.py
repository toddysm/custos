"""YAML → :class:`WorkflowDocument` loader (WF-IMPL-016).

The loader is intentionally minimal:

* ``yaml.safe_load`` for parsing (rejects YAML tags / arbitrary
  Python objects — the workflow document is a closed schema).
* Pydantic v2 ``model_validate`` for structural validation.
* Both parse and validation failures are re-raised as
  :class:`DocumentParseError` so callers have one ``except`` site.

When WF-IMPL-024 lands the full Workflow Service error taxonomy
:class:`DocumentParseError` will be split into ``YamlSyntaxError``
and ``SchemaMismatchError`` subclasses; callers that catch the
broad type continue to work.
"""

from __future__ import annotations

from typing import Any

import yaml
from pydantic import ValidationError

from custos_workflow.document.models import WorkflowDocument


class DocumentParseError(ValueError):
    """Raised when YAML cannot be parsed or fails schema validation.

    The exception preserves the underlying ``yaml.YAMLError`` or
    Pydantic :class:`ValidationError` as ``__cause__`` so log
    formatters and the API error renderer (WF-IMPL-024) can extract
    structured details.
    """


def parse_document(yaml_text: str) -> WorkflowDocument:
    """Parse a YAML workflow document and return the typed model.

    Args:
        yaml_text: The raw YAML source. Must be a single document.

    Returns:
        A validated :class:`WorkflowDocument`.

    Raises:
        DocumentParseError: The YAML is syntactically invalid, is not
            a single mapping at the root, or fails schema validation.
    """

    try:
        data: Any = yaml.safe_load(yaml_text)
    except yaml.YAMLError as exc:
        raise DocumentParseError(f"invalid YAML: {exc}") from exc

    if not isinstance(data, dict):
        raise DocumentParseError(
            f"workflow document root must be a YAML mapping, got {type(data).__name__}"
        )

    try:
        return WorkflowDocument.model_validate(data)
    except ValidationError as exc:
        raise DocumentParseError(
            f"workflow document failed schema validation: {exc.error_count()} "
            f"error(s); first: {exc.errors()[0]['msg']}"
        ) from exc
