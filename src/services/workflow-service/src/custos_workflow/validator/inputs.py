"""JSON Schema evaluation for the API Adapter Validator.

This module implements the schema-match leg of WF-IMPL-063 (issue
#449). The :class:`~custos_workflow.validator.service.StartRunValidator`
calls :func:`validate_inputs_against_schema` with the caller's
``inputs`` mapping and a JSON-Schema object derived from the
workflow version's ``spec.inputs`` declaration; failures are
collected into a structured rejection list and raised as
:class:`~custos_workflow.validator.errors.InputsSchemaError`.

Surface
=======

* :func:`derive_inputs_schema` — translate the typed
  :class:`~custos_workflow.document.models.InputDefinition` map on
  :class:`~custos_workflow.document.models.WorkflowSpec` into a
  Draft 2020-12 JSON Schema. The Validator owns this translation
  rather than the document model so the document parser stays
  byte-equal with the published Catalog schema.
* :func:`validate_inputs_against_schema` — evaluate a payload
  against a schema and raise
  :class:`~custos_workflow.validator.errors.InputsSchemaError`
  with structured ``validation`` entries on failure.

Each ``validation`` entry has the shape
``{"loc": "<json_pointer>", "code": "<jsonschema_validator>",
"message": "<human_readable>"}``. The ``loc`` field is the
RFC 6901 JSON Pointer to the failing input field — stable across
``jsonschema`` releases because we derive it from the validator
context's :attr:`absolute_path`.

See the issue: https://github.com/toddysm/custos/issues/449
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from jsonschema import Draft202012Validator

from custos_workflow.validator.errors import InputsSchemaError

if TYPE_CHECKING:
    from collections.abc import Iterable

    from custos_workflow.document.models import InputDefinition


__all__ = [
    "derive_inputs_schema",
    "validate_inputs_against_schema",
]


def derive_inputs_schema(
    inputs_def: Mapping[str, InputDefinition] | None,
) -> dict[str, Any]:
    """Translate ``WorkflowSpec.inputs`` into a JSON Schema object.

    The Workflow Document declares each input as a typed slot
    (``type`` + ``required``). The Validator derives an object
    schema from that declaration so the runtime check covers the
    minimum-viable contract:

    * ``type: "object"`` and ``additionalProperties: false`` so
      unknown input keys are rejected.
    * One ``properties[name]`` per declared input with the declared
      ``type``.
    * ``required`` lists every name whose
      :attr:`~custos_workflow.document.models.InputDefinition.required`
      flag is ``True``.

    The richer per-input validation surfaces (regex, enum, range,
    nested object shape) live in the document model itself and are
    enforced at Catalog publish time; this derived schema is a
    runtime backstop, not a re-implementation of the document
    grammar.

    Args:
        inputs_def: The :class:`InputDefinition` map from
            :attr:`WorkflowSpec.inputs`. May be ``None`` (the spec
            declared no ``inputs:`` block) or empty (declared but
            empty) — both cases produce the same closed schema
            that accepts only ``{}``.

    Returns:
        A JSON-safe ``dict`` ready to hand to
        :class:`Draft202012Validator`.
    """
    schema: dict[str, Any] = {
        "type": "object",
        "additionalProperties": False,
        "properties": {},
        "required": [],
    }
    if not inputs_def:
        return schema
    for name, definition in inputs_def.items():
        schema["properties"][name] = {"type": definition.type}
        if definition.required:
            schema["required"].append(name)
    return schema


def _format_loc(absolute_path: Iterable[Any]) -> str:
    """Render a ``jsonschema`` :attr:`absolute_path` as a JSON Pointer.

    ``absolute_path`` is a :class:`~collections.deque` of path
    segments (strings for object keys, ints for array indices). The
    output follows RFC 6901: the root document is the empty string
    ``""``; non-root pointers are ``/``-separated reference tokens
    with ``~`` escaped as ``~0`` and ``/`` escaped as ``~1`` per the
    spec.

    Args:
        absolute_path: The validator-context path attribute (an
            iterable of ``int`` or ``str`` segments, typed loosely
            because the upstream library types it as
            :class:`~collections.deque[Any]`).

    Returns:
        The JSON Pointer string (``""`` for the root document).
    """
    parts = list(absolute_path)
    if not parts:
        return ""
    out: list[str] = []
    for segment in parts:
        if isinstance(segment, int):
            out.append(str(segment))
            continue
        token = str(segment).replace("~", "~0").replace("/", "~1")
        out.append(token)
    return "/" + "/".join(out)


def validate_inputs_against_schema(
    inputs: Mapping[str, Any] | None,
    schema: Mapping[str, Any],
    *,
    workspace_id: str | None = None,
) -> None:
    """Validate ``inputs`` against ``schema``; raise on failure.

    Uses :class:`jsonschema.Draft202012Validator` so the dialect
    matches what the Workflow Document publishes. The iteration is
    deterministic: errors are sorted by JSON-pointer ``loc`` and
    then by ``code`` so the rejection list is stable across calls
    (the order is published to the caller via RFC 7807 so a Hypothesis
    determinism test pins it).

    Args:
        inputs: The caller-supplied payload, or ``None`` (treated
            as ``{}`` — mirrors
            :meth:`custos_workflow.runs.controller.RunController.start_run`).
        schema: A JSON Schema object, typically the result of
            :func:`derive_inputs_schema`.
        workspace_id: Forwarded to the raised
            :class:`InputsSchemaError` for audit emission. Optional
            because :func:`validate_inputs_against_schema` is also
            called from tests with no workspace context.

    Raises:
        InputsSchemaError: The payload failed the schema. The
            error's :attr:`validation` list carries one entry per
            failure; the human-readable :meth:`__str__` summarises
            the first failure's pointer and code.
    """
    payload: dict[str, Any] = dict(inputs or {})
    validator = Draft202012Validator(dict(schema))
    issues: list[dict[str, Any]] = []
    for error in validator.iter_errors(payload):
        # jsonschema reports ``additionalProperties`` failures with
        # ``absolute_path`` pointing at the *parent* object (root in
        # the typical workflow-inputs case). That hides which key was
        # unexpected, which is exactly the diagnostic the caller needs.
        # Recover the offending property names from ``error.message``
        # (the upstream message is the canonical source — see
        # ``jsonschema._keywords.additionalProperties``) and emit one
        # entry per offender pointing at ``<parent>/<key>`` so the
        # JSON-Pointer ``loc`` matches the field the caller sent.
        if error.validator == "additionalProperties" and error.validator_value is False:
            instance = error.instance if isinstance(error.instance, Mapping) else {}
            declared = set(error.schema.get("properties", {}))
            extras = sorted(set(instance) - declared)
            parent_loc = _format_loc(error.absolute_path)
            for extra in extras:
                token = str(extra).replace("~", "~0").replace("/", "~1")
                issues.append(
                    {
                        "loc": f"{parent_loc}/{token}",
                        "code": "additionalProperties",
                        "message": error.message,
                    },
                )
            continue
        issues.append(
            {
                "loc": _format_loc(error.absolute_path),
                "code": error.validator or "schema",
                "message": error.message,
            },
        )
    if not issues:
        return
    issues.sort(key=lambda item: (item["loc"], item["code"]))
    first = issues[0]
    summary = f"inputs failed schema validation at {first['loc'] or '/'}: {first['message']}"
    raise InputsSchemaError(
        summary,
        workspace_id=workspace_id,
        validation=issues,
    )
