"""JSON Schema (Draft 2020-12) for the WorkflowTemplate document.

Per ADR-009 templates share the workflow grammar; the wrapping shape
adds a ``spec.placeholders[]`` block plus a nested ``spec.workflow``
that carries the same body as a Workflow's ``spec`` (without its own
``placeholders``). The wrapper is reified here so the validator can
attribute schema errors at the right level.

The placeholder schema is normative: every placeholder is one of
``connectorRef``, ``activityRef``, ``string``, ``integer``,
``number``, ``boolean``, or ``json``; some types carry extra
constraints (e.g. ``connectorRef`` requires ``connectorType``).
Materialization (CS-IMPL-013, deferred to Phase E) consumes this
shape directly.
"""

from __future__ import annotations

from typing import Any

from custos_catalog.schema.workflow import _expression_or_value, _spec

_PLACEHOLDER_NAME_PATTERN: str = r"^[a-zA-Z][a-zA-Z0-9_]{0,62}$"


def _placeholder() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["name", "type"],
        "properties": {
            "name": {"type": "string", "pattern": _PLACEHOLDER_NAME_PATTERN},
            "type": {
                "type": "string",
                "enum": [
                    "connectorRef",
                    "activityRef",
                    "string",
                    "integer",
                    "number",
                    "boolean",
                    "json",
                ],
            },
            "required": {"type": "boolean"},
            "description": {"type": "string"},
            "default": _expression_or_value(),
            # connectorRef-only: pins the placeholder to a concrete
            # connector type so the materializer can narrow at
            # binding time.
            "connectorType": {"type": "string", "minLength": 1},
            # activityRef-only: pins the placeholder to an activity
            # type (no version; the supplied binding picks the version).
            "activityType": {"type": "string", "minLength": 1},
        },
        # Branch on `type` so the cross-field constraints surface at
        # the placeholder level rather than the parent array.
        "allOf": [
            {
                "if": {"properties": {"type": {"const": "connectorRef"}}},
                "then": {"required": ["connectorType"]},
            },
            {
                "if": {"properties": {"type": {"const": "activityRef"}}},
                "then": {"required": ["activityType"]},
            },
        ],
    }


#: The WorkflowTemplate document JSON Schema (Draft 2020-12).
TEMPLATE_SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "https://custos.dev/schemas/workflow-template.v1.schema.json",
    "title": "Custos WorkflowTemplate",
    "type": "object",
    "additionalProperties": False,
    "required": ["apiVersion", "kind", "metadata", "spec"],
    "properties": {
        "apiVersion": {"const": "custos.dev/v1"},
        "kind": {"const": "WorkflowTemplate"},
        "metadata": {
            "type": "object",
            "additionalProperties": False,
            "required": ["name"],
            "properties": {
                "name": {
                    "type": "string",
                    "pattern": r"^[a-z][a-z0-9-]{0,62}$",
                },
                "workspace": {
                    "type": "string",
                    "pattern": r"^[a-z][a-z0-9-]{0,62}$",
                },
                "description": {"type": "string"},
                "labels": {
                    "type": "object",
                    "additionalProperties": {"type": "string"},
                },
            },
        },
        "spec": {
            "type": "object",
            "additionalProperties": False,
            "required": ["placeholders", "workflow"],
            "properties": {
                "placeholders": {
                    "type": "array",
                    "minItems": 1,
                    "items": _placeholder(),
                },
                "workflow": _spec(),
            },
        },
    },
}
