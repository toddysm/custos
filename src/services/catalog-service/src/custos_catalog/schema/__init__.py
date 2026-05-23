"""Publish-time JSON Schema validators (CS-IMPL-005).

This package owns the first validation gate in
``design/components/catalog-service/design.md`` § Publish-Time Validation
Scope: YAML/JSON Schema conformance against the canonical workflow and
template grammars defined in ``design/architecture/overview.md``
§ Workflow and Template Schema.

The validators accept either YAML or JSON input and surface structured
field-path errors so the API gateway can return them verbatim. They are
deliberately limited to **structural** checks; CEL expressions inside
``${{ ... }}`` slots are treated as opaque strings here and parsed by
CS-IMPL-007 against the normalized document.
"""

from custos_catalog.schema.template import TEMPLATE_SCHEMA
from custos_catalog.schema.validate import (
    DocumentParseError,
    SchemaValidationError,
    SchemaValidationIssue,
    TemplateSchemaError,
    WorkflowSchemaError,
    load_document,
    validate_template,
    validate_workflow,
)
from custos_catalog.schema.workflow import WORKFLOW_SCHEMA

__all__ = [
    "TEMPLATE_SCHEMA",
    "WORKFLOW_SCHEMA",
    "DocumentParseError",
    "SchemaValidationError",
    "SchemaValidationIssue",
    "TemplateSchemaError",
    "WorkflowSchemaError",
    "load_document",
    "validate_template",
    "validate_workflow",
]
