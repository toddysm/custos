"""JSON Schema (Draft 2020-12) for the Workflow document.

The schema is derived from ``design/architecture/overview.md``
§ Workflow and Template Schema; every example in that section MUST
validate against this schema. Updates here are normative — bump
``$id`` and record the change in a design changelog entry.

Design choices encoded here:

* CEL expression slots (``if``, ``when``, ``unless``, ``where``,
  ``forEach``, ``with`` values, ``let`` values, trigger ``connector``)
  are typed as the union ``string | number | boolean | object | array``.
  The structural validator does NOT inspect the CEL source; that is
  CS-IMPL-007's responsibility, which sees the normalized document
  emitted by CS-IMPL-006.
* The step union (``activity`` / ``let`` / ``workflow``) is enforced
  through ``oneOf`` so a single field-path error names the violating
  branch (``spec/steps/2 -> oneOf branch did not match``).
* ``connector:`` (singular, string) and ``connectors:`` (map of aliases)
  are mutually exclusive on activity steps (M1; see overview § Step
  forms). The schema enforces this via the ``activity`` branch's
  ``oneOf`` over the two binding forms.
* ``activity`` references are required to be fully-qualified
  ``<namespace>/<type>@<major-or-exact>`` strings (or
  ``${{ placeholders.<name> }}`` in templates). Short forms and
  ``@<major>.<minor>`` are rejected per design § Operation: Resolve
  Activity Reference at Workflow Publish.
"""

from __future__ import annotations

from typing import Any

#: A CEL placeholder string of the form ``${{ ... }}``. Used wherever
#: an interpolated expression is allowed (e.g. ``connector:`` values
#: in templates). Captures the whole token; CS-IMPL-007 inspects the
#: inside.
_CEL_TOKEN_PATTERN: str = r"^\$\{\{[\s\S]+\}\}$"

#: Fully-qualified activity reference: ``<ns>/<type>@<version>``.
#: ``<version>`` is either an integer (major-pinned) or a
#: ``MAJOR.MINOR.PATCH`` triple. ``MAJOR.MINOR`` is reserved and
#: rejected at the resolver layer with a stable error code
#: (CS-IMPL-008), so the JSON Schema regex accepts it to keep the
#: error attribution at the right layer.
_ACTIVITY_REF_PATTERN: str = (
    r"^[a-z][a-z0-9._-]*/[a-z][a-z0-9._-]*@"
    r"(?:[0-9]+|[0-9]+\.[0-9]+|[0-9]+\.[0-9]+\.[0-9]+)$"
)

#: Step id grammar: DNS-1123-like, with hyphens allowed but not leading.
#: Matches the cluster-resource convention used elsewhere in Custos.
_STEP_ID_PATTERN: str = r"^[a-z][a-z0-9-]{0,62}$"

#: Workspace / workflow / template name grammar. Same as step id.
_NAME_PATTERN: str = r"^[a-z][a-z0-9-]{0,62}$"


def _expression_or_value() -> dict[str, Any]:
    """Permits any JSON scalar/composite OR a `${{ ... }}` token.

    Used for ``with:`` values, ``let:`` values, and any other slot
    where the author can either inline a literal or interpolate a CEL
    expression. The structural validator does not parse the CEL — see
    CS-IMPL-007 for the syntactic + name-binding gate.
    """
    return {
        "type": ["string", "number", "integer", "boolean", "object", "array", "null"],
    }


def _expression_string() -> dict[str, Any]:
    """A required CEL expression token in the form ``${{ ... }}``.

    Applied to ``if``, ``when``, ``unless``, ``where``, ``forEach``
    where the field's whole value must be an expression. The
    structural validator only checks the wrapper shape; the inside
    is parsed by CS-IMPL-007.
    """
    return {
        "type": "string",
        "pattern": _CEL_TOKEN_PATTERN,
    }


def _input_definition() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["type"],
        "properties": {
            "type": {
                "type": "string",
                "enum": ["string", "integer", "number", "boolean", "object", "array"],
            },
            "required": {"type": "boolean"},
            "default": _expression_or_value(),
            "description": {"type": "string"},
        },
    }


def _retry_policy() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "maxAttempts": {"type": "integer", "minimum": 1},
            "backoff": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "strategy": {
                        "type": "string",
                        "enum": ["constant", "linear", "exponential"],
                    },
                    "initialDelay": {"type": "string"},
                    "maxDelay": {"type": "string"},
                    "multiplier": {"type": "number", "exclusiveMinimum": 0},
                },
            },
            "jitter": {
                "type": "string",
                "enum": ["none", "full", "equal", "decorrelated"],
            },
            "respectRetryAfter": {"type": "boolean"},
        },
    }


def _on_error_arm() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["match", "do"],
        "properties": {
            "match": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "code": {"type": "string"},
                    "codePrefix": {"type": "string"},
                    "class": {"type": "string"},
                },
                # Exactly one of code/codePrefix/class. Enforced via the
                # oneOf below so a missing match key surfaces the
                # branch that fired.
                "oneOf": [
                    {"required": ["code"]},
                    {"required": ["codePrefix"]},
                    {"required": ["class"]},
                ],
            },
            "do": {"type": "string", "enum": ["skip", "retry", "fail"]},
            "retry": _retry_policy(),
            # Shorthand for `retry: { maxAttempts: N }` on a `do: retry` arm.
            "maxAttempts": {"type": "integer", "minimum": 1},
        },
    }


def _trigger() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["type"],
        "properties": {
            "type": {"type": "string", "minLength": 1},
            "connector": {
                "anyOf": [
                    {"type": "string", "minLength": 1},
                    _expression_string(),
                ],
            },
        },
    }


def _step_common_properties() -> dict[str, Any]:
    """Properties shared by every step kind (activity / let / workflow)."""
    return {
        "id": {"type": "string", "pattern": _STEP_ID_PATTERN},
        "description": {"type": "string"},
        "if": _expression_string(),
        "when": _expression_string(),
        "unless": _expression_string(),
        "forEach": _expression_string(),
        "where": _expression_string(),
        "retry": _retry_policy(),
        "on_error": {
            "type": "array",
            "items": _on_error_arm(),
        },
    }


def _activity_step() -> dict[str, Any]:
    """Step kind: bind a containerized activity to one or more connectors."""
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["id", "activity"],
        "properties": {
            **_step_common_properties(),
            "activity": {
                "anyOf": [
                    {"type": "string", "pattern": _ACTIVITY_REF_PATTERN},
                    # Template path: `${{ placeholders.scanActivity }}`.
                    _expression_string(),
                ],
            },
            # Mutually exclusive: a singular connector name OR a map of
            # aliases (e.g. `image-promote@1` with source/destination).
            "connector": {
                "anyOf": [
                    {"type": "string", "minLength": 1},
                    _expression_string(),
                ],
            },
            "connectors": {
                "type": "object",
                "minProperties": 1,
                "additionalProperties": {
                    "anyOf": [
                        {"type": "string", "minLength": 1},
                        _expression_string(),
                    ],
                },
            },
            "with": {
                "type": "object",
                "additionalProperties": _expression_or_value(),
            },
        },
        # `connector` XOR `connectors` (both absent is allowed for
        # connectorless activities; both present is not).
        "not": {
            "type": "object",
            "required": ["connector", "connectors"],
        },
    }


def _let_step() -> dict[str, Any]:
    """Step kind: pure-data step computed by the WF Expression Evaluator."""
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["id", "let"],
        "properties": {
            **_step_common_properties(),
            "let": {
                "type": "object",
                "minProperties": 1,
                # `let` values are CEL expressions but may also be
                # literal scalars/objects — keep the open shape so
                # CS-IMPL-007 owns the per-value parse.
                "additionalProperties": _expression_or_value(),
            },
        },
    }


def _workflow_step() -> dict[str, Any]:
    """Step kind: invoke a sub-workflow by `workflowVersionId` or triple."""
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["id", "workflow"],
        "properties": {
            **_step_common_properties(),
            "workflow": {
                "anyOf": [
                    # `workflowVersionId` UUID.
                    {
                        "type": "string",
                        "pattern": (
                            r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-"
                            r"[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
                        ),
                    },
                    # `<workspace>/<name>@<version>` triple.
                    {
                        "type": "string",
                        "pattern": (
                            r"^[a-z][a-z0-9-]{0,62}/"
                            r"[a-z][a-z0-9-]{0,62}@"
                            r"[0-9]+(?:\.[0-9]+){0,2}$"
                        ),
                    },
                    _expression_string(),
                ],
            },
            "with": {
                "type": "object",
                "additionalProperties": _expression_or_value(),
            },
        },
    }


def _step() -> dict[str, Any]:
    return {
        "oneOf": [
            _activity_step(),
            _let_step(),
            _workflow_step(),
        ],
    }


def _spec(*, include_workflow_keys: bool = True) -> dict[str, Any]:
    """Returns the schema for the workflow ``spec`` block.

    When ``include_workflow_keys`` is True (the default) the schema is
    the standalone workflow ``spec``. When False, the schema is the
    inner ``spec.workflow`` block of a WorkflowTemplate, which carries
    the same shape but without ``placeholders`` (those live alongside
    ``workflow`` at the template's ``spec`` level).
    """
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["steps"],
        "properties": {
            "inputs": {
                "type": "object",
                "additionalProperties": _input_definition(),
            },
            "defaults": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "retry": _retry_policy(),
                },
            },
            "triggers": {
                "type": "array",
                "items": _trigger(),
            },
            "steps": {
                "type": "array",
                "minItems": 1,
                "items": _step(),
            },
            "on_error": {
                "type": "array",
                "items": _on_error_arm(),
            },
        },
    }


def _metadata(*, require_workspace: bool) -> dict[str, Any]:
    required = ["name"]
    if require_workspace:
        required = ["name", "workspace"]
    return {
        "type": "object",
        "additionalProperties": False,
        "required": required,
        "properties": {
            "name": {"type": "string", "pattern": _NAME_PATTERN},
            "workspace": {"type": "string", "pattern": _NAME_PATTERN},
            "description": {"type": "string"},
            "labels": {
                "type": "object",
                "additionalProperties": {"type": "string"},
            },
        },
    }


#: The Workflow document JSON Schema (Draft 2020-12).
#:
#: ``$id`` is the stable identifier the resolver layer (CS-IMPL-008)
#: uses for downstream references; bumping it is a normative change
#: and must be paired with a design changelog entry.
WORKFLOW_SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "https://custos.dev/schemas/workflow.v1.schema.json",
    "title": "Custos Workflow",
    "type": "object",
    "additionalProperties": False,
    "required": ["apiVersion", "kind", "metadata", "spec"],
    "properties": {
        "apiVersion": {"const": "custos.dev/v1"},
        "kind": {"const": "Workflow"},
        # Workspace is supplied by the API path at publish time, so the
        # in-document workspace key is optional. When present it must
        # match the URL workspace (enforced at the manager layer in
        # CS-IMPL-010, not here).
        "metadata": _metadata(require_workspace=False),
        "spec": _spec(),
    },
}
