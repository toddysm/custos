"""Tests for the validator inputs JSON-Schema evaluator (WF-IMPL-063).

Pins:

* :func:`derive_inputs_schema` produces a closed object schema with
  ``additionalProperties: false`` and one ``properties`` entry per
  declared :class:`InputDefinition`, with ``required: True`` inputs
  collected into ``required``.
* :func:`validate_inputs_against_schema` returns silently on the
  happy path.
* Type mismatch → :class:`InputsSchemaError` with the failing
  field's JSON Pointer.
* Missing required field → :class:`InputsSchemaError` with the
  ``required`` validator code and ``loc=""`` (the root).
* Unknown field → :class:`InputsSchemaError` with the
  ``additionalProperties`` code and a stable pointer.
* Nested object failures emit a multi-segment JSON Pointer.
* JSON Pointer escaping of ``~`` and ``/`` per RFC 6901.
* Hypothesis: nested-failure pointers stay deterministic across
  permutations of dict iteration order (200 examples).
"""

from __future__ import annotations

from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from custos_workflow.document.models import InputDefinition
from custos_workflow.validator.errors import InputsSchemaError
from custos_workflow.validator.inputs import (
    derive_inputs_schema,
    validate_inputs_against_schema,
)

# ---------------------------------------------------------------------------
# derive_inputs_schema
# ---------------------------------------------------------------------------


def test_derive_schema_for_none_inputs_block() -> None:
    """``spec.inputs`` absent → closed empty-object schema."""
    schema = derive_inputs_schema(None)
    assert schema == {
        "type": "object",
        "additionalProperties": False,
        "properties": {},
        "required": [],
    }


def test_derive_schema_for_empty_inputs_block() -> None:
    """``spec.inputs: {}`` → same closed empty-object schema."""
    assert derive_inputs_schema({}) == derive_inputs_schema(None)


def test_derive_schema_translates_each_input_definition() -> None:
    """One ``properties[name]`` per declared input; ``required`` collected."""
    schema = derive_inputs_schema(
        {
            "name": InputDefinition(type="string", required=True),
            "count": InputDefinition(type="integer"),
        }
    )
    assert schema["type"] == "object"
    assert schema["additionalProperties"] is False
    assert schema["properties"] == {
        "name": {"type": "string"},
        "count": {"type": "integer"},
    }
    assert schema["required"] == ["name"]


# ---------------------------------------------------------------------------
# validate_inputs_against_schema — happy path
# ---------------------------------------------------------------------------


def test_validate_returns_silently_on_happy_path() -> None:
    """Conforming payload → no exception."""
    schema = derive_inputs_schema({"name": InputDefinition(type="string", required=True)})
    validate_inputs_against_schema({"name": "alice"}, schema)


def test_validate_treats_none_payload_as_empty_object() -> None:
    """``None`` is normalised to ``{}`` and validated as such."""
    schema = derive_inputs_schema({})
    validate_inputs_against_schema(None, schema)


# ---------------------------------------------------------------------------
# validate_inputs_against_schema — error mapping
# ---------------------------------------------------------------------------


def test_validate_rejects_type_mismatch_with_field_pointer() -> None:
    """Wrong type at a top-level key → ``loc='/name'`` with ``type`` code."""
    schema = derive_inputs_schema({"name": InputDefinition(type="string", required=True)})
    with pytest.raises(InputsSchemaError) as info:
        validate_inputs_against_schema({"name": 42}, schema, workspace_id="ws-1")
    err = info.value
    assert err.workspace_id == "ws-1"
    assert err.validation
    issue = err.validation[0]
    assert issue["loc"] == "/name"
    assert issue["code"] == "type"


def test_validate_rejects_missing_required_with_root_pointer() -> None:
    """``required`` failures report at the root (no failing field path)."""
    schema = derive_inputs_schema({"name": InputDefinition(type="string", required=True)})
    with pytest.raises(InputsSchemaError) as info:
        validate_inputs_against_schema({}, schema)
    issue = info.value.validation[0]
    assert issue["loc"] == ""
    assert issue["code"] == "required"


def test_validate_rejects_unknown_field() -> None:
    """``additionalProperties: false`` rejects unknown keys.

    The pointer must locate the *offending field* (``/extra``), not
    the parent object, so the diagnostic the caller renders is
    actionable.
    """
    schema = derive_inputs_schema({"name": InputDefinition(type="string", required=True)})
    with pytest.raises(InputsSchemaError) as info:
        validate_inputs_against_schema({"name": "alice", "extra": True}, schema)
    by_loc = {issue["loc"]: issue for issue in info.value.validation}
    assert "/extra" in by_loc
    assert by_loc["/extra"]["code"] == "additionalProperties"


def test_validate_emits_one_entry_per_unexpected_field() -> None:
    """A single ``additionalProperties`` violation with N extras
    fans out into N pointer entries, sorted deterministically."""
    schema = derive_inputs_schema({"name": InputDefinition(type="string", required=True)})
    with pytest.raises(InputsSchemaError) as info:
        validate_inputs_against_schema({"name": "alice", "b": 1, "a": 2}, schema)
    locs = [issue["loc"] for issue in info.value.validation]
    assert locs == ["/a", "/b"]
    assert all(issue["code"] == "additionalProperties" for issue in info.value.validation)


def test_validate_emits_nested_additional_properties_pointer() -> None:
    """Nested ``additionalProperties`` failures point at
    ``/<parent>/<extra>`` so the caller can locate the field inside
    a nested object too."""
    schema: dict[str, Any] = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "config": {
                "type": "object",
                "additionalProperties": False,
                "properties": {"port": {"type": "integer"}},
            },
        },
    }
    with pytest.raises(InputsSchemaError) as info:
        validate_inputs_against_schema({"config": {"port": 8080, "host": "x"}}, schema)
    by_loc = {issue["loc"]: issue for issue in info.value.validation}
    assert "/config/host" in by_loc
    assert by_loc["/config/host"]["code"] == "additionalProperties"


def test_validate_emits_deterministic_issue_order() -> None:
    """Two failures sort by ``loc`` then ``code``."""
    schema = derive_inputs_schema(
        {
            "a": InputDefinition(type="string", required=True),
            "b": InputDefinition(type="integer", required=True),
        }
    )
    with pytest.raises(InputsSchemaError) as info:
        # Both fields fail type validation; "/a" must sort before "/b".
        validate_inputs_against_schema({"a": 1, "b": "x"}, schema)
    locs = [issue["loc"] for issue in info.value.validation]
    assert locs == ["/a", "/b"]


def test_validate_emits_nested_json_pointer() -> None:
    """Failure inside a nested object reports a multi-segment pointer."""
    schema: dict[str, Any] = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "config": {
                "type": "object",
                "additionalProperties": False,
                "properties": {"port": {"type": "integer"}},
                "required": ["port"],
            },
        },
        "required": ["config"],
    }
    with pytest.raises(InputsSchemaError) as info:
        validate_inputs_against_schema({"config": {"port": "eighty"}}, schema)
    issue = info.value.validation[0]
    assert issue["loc"] == "/config/port"
    assert issue["code"] == "type"


def test_validate_emits_array_index_pointer() -> None:
    """Array index path segments render as integer JSON Pointer tokens."""
    schema: dict[str, Any] = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "tags": {
                "type": "array",
                "items": {"type": "string"},
            },
        },
    }
    with pytest.raises(InputsSchemaError) as info:
        validate_inputs_against_schema({"tags": ["ok", 42, "ok"]}, schema)
    issue = info.value.validation[0]
    assert issue["loc"] == "/tags/1"
    assert issue["code"] == "type"


def test_validate_escapes_rfc6901_special_chars() -> None:
    """RFC 6901: ``~`` → ``~0`` and ``/`` → ``~1`` in object-key segments."""
    schema: dict[str, Any] = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "weird~name": {"type": "string"},
            "with/slash": {"type": "string"},
        },
        "required": ["weird~name", "with/slash"],
    }
    with pytest.raises(InputsSchemaError) as info:
        validate_inputs_against_schema({"weird~name": 1, "with/slash": 1}, schema)
    locs = sorted(issue["loc"] for issue in info.value.validation)
    assert locs == ["/weird~0name", "/with~1slash"]


def test_validate_summary_message_quotes_first_failure() -> None:
    """The exception's message names the first failure's pointer + reason."""
    schema = derive_inputs_schema({"name": InputDefinition(type="string", required=True)})
    with pytest.raises(InputsSchemaError, match=r"/name") as info:
        validate_inputs_against_schema({"name": 42}, schema)
    assert "/name" in info.value.message


# ---------------------------------------------------------------------------
# Hypothesis: pointer stability under dict-iteration permutations
# ---------------------------------------------------------------------------


@settings(max_examples=200, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    fields=st.lists(
        st.text(
            alphabet=st.characters(min_codepoint=ord("a"), max_codepoint=ord("z")),
            min_size=1,
            max_size=6,
        ),
        min_size=1,
        max_size=4,
        unique=True,
    )
)
def test_nested_pointer_is_stable_under_permutation(fields: list[str]) -> None:
    """Hypothesis: identical type-mismatch payloads yield identical sorted loc sets."""
    schema: dict[str, Any] = {
        "type": "object",
        "additionalProperties": False,
        "properties": {name: {"type": "integer"} for name in fields},
        "required": fields,
    }
    payload_a = {name: "x" for name in fields}
    payload_b = {name: "x" for name in reversed(fields)}
    with pytest.raises(InputsSchemaError) as info_a:
        validate_inputs_against_schema(payload_a, schema)
    with pytest.raises(InputsSchemaError) as info_b:
        validate_inputs_against_schema(payload_b, schema)
    locs_a = [issue["loc"] for issue in info_a.value.validation]
    locs_b = [issue["loc"] for issue in info_b.value.validation]
    assert locs_a == locs_b  # sorted ⇒ permutation-invariant
