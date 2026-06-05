"""Tests for :mod:`custos_cel.types` — the WF-IMPL-005 type checker.

Coverage targets:

* JSON Schema → :class:`CelType` translation for every supported leaf.
* Successful annotation of every AST node kind against well-typed
  expressions, including the design.md canonical example
  ``steps.scan.outputs.critical + steps.scan-alt.outputs.critical``.
* :class:`TypeCheckError` (and :class:`UnboundNameError`) raised with
  source-position metadata at every node kind where a mismatch is
  possible.
* ``≥90% line coverage`` per the WF-IMPL-005 acceptance criterion.
"""

from __future__ import annotations

from typing import Any

import pytest

import custos_cel
from custos_cel import (
    Binary,
    BoolType,
    BytesType,
    Call,
    CelType,
    Conditional,
    DoubleType,
    Ident,
    Index,
    IntType,
    ListLit,
    ListType,
    Literal,
    MapLit,
    MapType,
    Member,
    Node,
    NullType,
    SchemaBindings,
    SourcePosition,
    StringType,
    TimestampType,
    TypeCheckError,
    UintType,
    Unary,
    UnboundNameError,
    parse,
    type_check,
)

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


_INPUTS_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "image": {"type": "string"},
        "count": {"type": "integer"},
        "ratio": {"type": "number"},
        "enabled": {"type": "boolean"},
        "tags": {"type": "array", "items": {"type": "string"}},
        "profile": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "level": {"type": "integer"},
            },
        },
        "labels": {
            "type": "object",
            "additionalProperties": {"type": "string"},
        },
    },
}

_SCAN_OUTPUTS: dict[str, Any] = {
    "type": "object",
    "properties": {"critical": {"type": "integer"}, "verdict": {"type": "string"}},
}

_SCAN_ALT_OUTPUTS: dict[str, Any] = {
    "type": "object",
    "properties": {"critical": {"type": "integer"}, "verdict": {"type": "string"}},
}


def _bindings(**overrides: Any) -> SchemaBindings:
    defaults: dict[str, Any] = {
        "inputs": _INPUTS_SCHEMA,
        "prior_steps": (
            ("scan", _SCAN_OUTPUTS),
            ("scan-alt", _SCAN_ALT_OUTPUTS),
        ),
        "let": {"totalCritical": IntType()},
    }
    defaults.update(overrides)
    return SchemaBindings(**defaults)


def _typed(src: str, bindings: SchemaBindings | None = None) -> Node:
    return type_check(parse(src), bindings or _bindings())


# ---------------------------------------------------------------------------
# SchemaBindings construction
# ---------------------------------------------------------------------------


def test_schema_bindings_defaults_are_immutable_proxies() -> None:
    sb = SchemaBindings()
    assert sb.run["id"] == StringType()
    assert sb.run["workspace"] == StringType()
    assert sb.workflow["name"] == StringType()
    assert sb.workflow["version"] == StringType()
    assert isinstance(sb.now, TimestampType)
    with pytest.raises(TypeError):
        sb.inputs["x"] = "y"  # type: ignore[index]


def test_schema_bindings_external_mutation_does_not_leak() -> None:
    inputs: dict[str, Any] = {"type": "object", "properties": {}}
    sb = SchemaBindings(inputs=inputs)
    inputs["properties"] = {"image": {"type": "string"}}
    # Stored mapping is a snapshot — but mappings nest, so the snapshot
    # is at the top level only. Documented behavior.
    assert "properties" in sb.inputs
    # Top-level keys cannot be added via the proxy.
    with pytest.raises(TypeError):
        sb.inputs["other"] = {}  # type: ignore[index]


def test_schema_bindings_prior_steps_lookup() -> None:
    sb = _bindings()
    assert sb.step_outputs_schema("scan") is _SCAN_OUTPUTS
    assert sb.step_outputs_schema("scan-alt") is _SCAN_ALT_OUTPUTS
    assert sb.step_outputs_schema("unknown") is None


def test_type_check_rejects_non_schema_bindings_argument() -> None:
    with pytest.raises(TypeError, match="SchemaBindings"):
        type_check(parse("1"), bindings=object())  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Literals — every kind
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("src", "expected"),
    [
        ("42", IntType()),
        ("42u", UintType()),
        ("1.5", DoubleType()),
        ("true", BoolType()),
        ("false", BoolType()),
        ('"hi"', StringType()),
        ("null", NullType()),
        ("b'abcd'", BytesType()),
    ],
)
def test_literal_typing(src: str, expected: CelType) -> None:
    n = _typed(src)
    assert n.cel_type == expected


# ---------------------------------------------------------------------------
# Identifiers / drilling — happy path
# ---------------------------------------------------------------------------


def test_inputs_scalar_property() -> None:
    n = _typed("inputs.image")
    assert isinstance(n, Member)
    assert n.cel_type == StringType()
    # The Ident target is annotated with the placeholder root type.
    assert isinstance(n.target, Ident)
    assert isinstance(n.target.cel_type, MapType)


def test_inputs_integer_property() -> None:
    assert _typed("inputs.count").cel_type == IntType()


def test_inputs_number_property() -> None:
    assert _typed("inputs.ratio").cel_type == DoubleType()


def test_inputs_boolean_property() -> None:
    assert _typed("inputs.enabled").cel_type == BoolType()


def test_inputs_array_property() -> None:
    n = _typed("inputs.tags")
    assert n.cel_type == ListType(element=StringType())


def test_inputs_nested_object_member_access() -> None:
    n = _typed("inputs.profile.name")
    assert n.cel_type == StringType()


def test_inputs_nested_object_integer() -> None:
    assert _typed("inputs.profile.level").cel_type == IntType()


def test_inputs_additional_properties_drill() -> None:
    n = _typed('inputs.labels["env"]')
    # labels is map<string, string> — bracket access by literal string
    # drills via additionalProperties.
    assert n.cel_type == StringType()


# ---------------------------------------------------------------------------
# event root (TS-IMPL-005)
# ---------------------------------------------------------------------------


def test_event_scalar_properties() -> None:
    assert _typed("event.kind").cel_type == StringType()
    assert _typed("event.subject").cel_type == StringType()


def test_event_subtree_member_access() -> None:
    # source / data / raw are string-valued objects (additionalProperties),
    # so dotted access into them types as string.
    assert _typed("event.source.vendor").cel_type == StringType()
    assert _typed("event.data.status").cel_type == StringType()
    assert _typed("event.raw.body").cel_type == StringType()


def test_event_subtree_bracket_access() -> None:
    assert _typed('event.data["status"]').cel_type == StringType()


def test_event_root_placeholder_is_map() -> None:
    n = _typed("event.kind")
    assert isinstance(n, Member)
    assert isinstance(n.target, Ident)
    assert isinstance(n.target.cel_type, MapType)


def test_event_canonical_selector_type_checks() -> None:
    # design.md § Selector Language canonical example.
    n = _typed('event.kind == "workflow.completed" && event.data.status == "succeeded"')
    assert isinstance(n, Binary)
    assert n.cel_type == BoolType()


def test_event_desugared_prefix_selector_type_checks() -> None:
    # String-typed event.data members participate in membership tests.
    n = _typed('event.data.status in ["succeeded", "failed"]')
    assert n.cel_type == BoolType()


def test_event_unknown_top_level_field_is_unbound() -> None:
    with pytest.raises(UnboundNameError, match="no such field"):
        _typed("event.nope")


def test_event_default_schema_present_and_immutable() -> None:
    sb = SchemaBindings()
    assert sb.event["type"] == "object"
    assert "kind" in sb.event["properties"]
    with pytest.raises(TypeError):
        sb.event["x"] = "y"  # type: ignore[index]


def test_event_custom_schema_override() -> None:
    schema: dict[str, Any] = {
        "type": "object",
        "properties": {"data": {"type": "object", "properties": {"n": {"type": "integer"}}}},
    }
    n = _typed("event.data.n", _bindings(event=schema))
    assert n.cel_type == IntType()


def test_let_declared_type() -> None:
    n = _typed("let.totalCritical")
    assert n.cel_type == IntType()


def test_run_id_and_workspace() -> None:
    assert _typed("run.id").cel_type == StringType()
    assert _typed("run.workspace").cel_type == StringType()


def test_workflow_name_and_version() -> None:
    assert _typed("workflow.name").cel_type == StringType()
    assert _typed("workflow.version").cel_type == StringType()


def test_now_call_returns_timestamp() -> None:
    n = _typed("now()")
    assert isinstance(n, Call)
    assert n.function == "now"
    assert n.cel_type == TimestampType()


def test_now_with_args_is_a_type_error() -> None:
    with pytest.raises(TypeCheckError, match="takes no arguments"):
        _typed("now(1)")


def test_bare_now_ident_is_a_usage_error() -> None:
    with pytest.raises(TypeCheckError, match="must be called as 'now"):
        _typed("now")


def test_unknown_function_is_a_type_error() -> None:
    err: TypeCheckError
    with pytest.raises(TypeCheckError, match="unknown function") as excinfo:
        _typed("foo(1, 2)")
    err = excinfo.value
    assert err.kind == "expression.type_error"
    assert err.source_position is not None


# ---------------------------------------------------------------------------
# Steps drilling
# ---------------------------------------------------------------------------


def test_steps_dotted_outputs_chain() -> None:
    n = _typed("steps.scan.outputs.critical")
    assert isinstance(n, Member)
    assert n.cel_type == IntType()
    # Each Member in the chain is annotated.
    cur: Node = n
    while isinstance(cur, Member):
        assert cur.cel_type is not None
        cur = cur.target
    assert isinstance(cur, Ident) and cur.name == "steps"
    assert cur.cel_type is not None


def test_steps_bracket_id_then_outputs() -> None:
    n = _typed('steps["scan-alt"].outputs.critical')
    assert n.cel_type == IntType()


def test_canonical_addition_of_two_step_integers() -> None:
    # Acceptance: steps.scan.outputs.critical + steps.scan-alt.outputs.critical
    # types as int.
    n = _typed('steps.scan.outputs.critical + steps["scan-alt"].outputs.critical')
    assert isinstance(n, Binary)
    assert n.cel_type == IntType()
    assert n.left.cel_type == IntType()
    assert n.right.cel_type == IntType()


def test_unknown_step_is_unbound_name() -> None:
    with pytest.raises(UnboundNameError) as excinfo:
        _typed("steps.no_such_step.outputs.x")
    assert "no such step" in str(excinfo.value)


def test_step_member_other_than_outputs_is_unbound() -> None:
    with pytest.raises(UnboundNameError, match="must use 'outputs'"):
        _typed("steps.scan.not_outputs.x")


def test_unknown_output_field_is_unbound() -> None:
    with pytest.raises(UnboundNameError, match="no such field"):
        _typed("steps.scan.outputs.no_such_field")


# ---------------------------------------------------------------------------
# Unbound roots
# ---------------------------------------------------------------------------


def test_unknown_top_level_root_is_unbound() -> None:
    with pytest.raises(UnboundNameError, match="unknown root"):
        _typed("os.environ")


def test_unknown_run_field_is_unbound() -> None:
    with pytest.raises(UnboundNameError, match="unknown run field"):
        _typed("run.no_such_field")


def test_unknown_workflow_field_is_unbound() -> None:
    with pytest.raises(UnboundNameError, match="unknown workflow field"):
        _typed("workflow.unknown")


def test_unknown_let_binding_is_unbound() -> None:
    with pytest.raises(UnboundNameError, match="unknown let field"):
        _typed("let.undeclared")


def test_unknown_inputs_field_is_unbound() -> None:
    with pytest.raises(UnboundNameError, match="no such field"):
        _typed("inputs.no_such_field")


# ---------------------------------------------------------------------------
# Arithmetic operators
# ---------------------------------------------------------------------------


def test_int_addition() -> None:
    assert _typed("1 + 2").cel_type == IntType()


def test_double_addition() -> None:
    assert _typed("1.5 + 2.5").cel_type == DoubleType()


def test_uint_addition() -> None:
    assert _typed("1u + 2u").cel_type == UintType()


def test_string_concatenation() -> None:
    assert _typed('"a" + "b"').cel_type == StringType()


def test_bytes_concatenation() -> None:
    assert _typed("b'aa' + b'bb'").cel_type == BytesType()


def test_list_concatenation_homogeneous() -> None:
    assert _typed("[1, 2] + [3]").cel_type == ListType(element=IntType())


def test_list_concatenation_heterogeneous_fails() -> None:
    with pytest.raises(TypeCheckError, match="matching element types"):
        _typed('[1] + ["a"]')


def test_int_plus_double_is_type_error() -> None:
    with pytest.raises(TypeCheckError, match=r"not defined"):
        _typed("1 + 1.5")


def test_subtract_multiply_divide_modulo_int() -> None:
    for op in ("-", "*", "/", "%"):
        assert _typed(f"3 {op} 2").cel_type == IntType()


def test_subtract_strings_is_type_error() -> None:
    with pytest.raises(TypeCheckError, match="numeric"):
        _typed('"a" - "b"')


def test_string_addition_mixed_with_int_fails() -> None:
    with pytest.raises(TypeCheckError, match="not defined"):
        _typed('"a" + 1')


# ---------------------------------------------------------------------------
# Comparison and equality
# ---------------------------------------------------------------------------


def test_int_less_than() -> None:
    assert _typed("1 < 2").cel_type == BoolType()


def test_string_less_than() -> None:
    assert _typed('"a" < "b"').cel_type == BoolType()


def test_comparison_type_mismatch() -> None:
    with pytest.raises(TypeCheckError, match="comparable"):
        _typed('"a" < 1')


def test_equality_same_type() -> None:
    assert _typed("1 == 2").cel_type == BoolType()
    assert _typed('"a" != "b"').cel_type == BoolType()


def test_equality_with_null_either_side() -> None:
    assert _typed("inputs.image == null").cel_type == BoolType()
    assert _typed("null != inputs.count").cel_type == BoolType()


def test_equality_cross_type_is_type_error() -> None:
    with pytest.raises(TypeCheckError, match="matching operand"):
        _typed('"a" == 1')


# ---------------------------------------------------------------------------
# Logical
# ---------------------------------------------------------------------------


def test_logical_and_or() -> None:
    assert _typed("true && false").cel_type == BoolType()
    assert _typed("true || false").cel_type == BoolType()


def test_logical_requires_bool() -> None:
    with pytest.raises(TypeCheckError, match="requires bool"):
        _typed("1 && true")


# ---------------------------------------------------------------------------
# 'in' operator
# ---------------------------------------------------------------------------


def test_in_list_correct_type() -> None:
    assert _typed('"x" in inputs.tags').cel_type == BoolType()


def test_in_list_element_type_mismatch() -> None:
    with pytest.raises(TypeCheckError, match="element type mismatch"):
        _typed("1 in inputs.tags")


def test_in_map_correct_key_type() -> None:
    assert _typed('"env" in inputs.labels').cel_type == BoolType()


def test_in_map_key_type_mismatch() -> None:
    with pytest.raises(TypeCheckError, match="key type mismatch"):
        _typed("1 in inputs.labels")


def test_in_right_operand_must_be_list_or_map() -> None:
    with pytest.raises(TypeCheckError, match="list or map"):
        _typed('"a" in "abc"')


# ---------------------------------------------------------------------------
# Unary
# ---------------------------------------------------------------------------


def test_unary_negate_int_and_double() -> None:
    # ``-1`` is constant-folded by celpy into a Literal(int, -1); use a
    # non-foldable operand so the Unary node survives parsing.
    n_int = _typed("-inputs.count")
    assert n_int.cel_type == IntType()
    assert isinstance(n_int, Unary)
    n_dbl = _typed("-inputs.ratio")
    assert n_dbl.cel_type == DoubleType()
    assert isinstance(n_dbl, Unary)


def test_unary_negate_literal_folds_into_literal() -> None:
    # Sanity check the parser fold so the negate path is still
    # representative of real expressions.
    n = _typed("-1")
    assert n.cel_type == IntType()
    assert isinstance(n, Literal)


def test_unary_negate_string_is_type_error() -> None:
    with pytest.raises(TypeCheckError, match="int or double"):
        _typed('-"a"')


def test_unary_not_bool() -> None:
    assert _typed("!true").cel_type == BoolType()


def test_unary_not_non_bool_is_type_error() -> None:
    with pytest.raises(TypeCheckError, match="requires bool"):
        _typed("!1")


# ---------------------------------------------------------------------------
# Ternary
# ---------------------------------------------------------------------------


def test_ternary_branches_unify() -> None:
    n = _typed('inputs.count > 0 ? "block" : "allow"')
    assert isinstance(n, Conditional)
    assert n.cel_type == StringType()


def test_ternary_branches_with_null_promote() -> None:
    n = _typed("inputs.count > 0 ? inputs.image : null")
    assert n.cel_type == StringType()


def test_ternary_branch_divergence_is_type_error() -> None:
    with pytest.raises(TypeCheckError, match="incompatible types"):
        _typed('true ? 1 : "a"')


def test_ternary_non_bool_condition_is_type_error() -> None:
    with pytest.raises(TypeCheckError, match="condition must be bool"):
        _typed('1 ? "a" : "b"')


def test_ternary_recursive_list_branches() -> None:
    n = _typed("true ? [1, 2] : [3]")
    assert n.cel_type == ListType(element=IntType())


# ---------------------------------------------------------------------------
# Collection literals
# ---------------------------------------------------------------------------


def test_list_literal_homogeneous() -> None:
    assert _typed("[1, 2, 3]").cel_type == ListType(element=IntType())


def test_list_literal_with_null_promotes() -> None:
    assert _typed('["a", null]').cel_type == ListType(element=StringType())


def test_list_literal_heterogeneous_fails() -> None:
    with pytest.raises(TypeCheckError, match="heterogeneous element"):
        _typed('[1, "a"]')


def test_empty_list_literal_fails() -> None:
    with pytest.raises(TypeCheckError, match="empty list"):
        _typed("[]")


def test_map_literal_homogeneous() -> None:
    n = _typed('{"a": 1, "b": 2}')
    assert isinstance(n, MapLit)
    assert n.cel_type == MapType(key=StringType(), value=IntType())


def test_map_literal_heterogeneous_keys_fail() -> None:
    with pytest.raises(TypeCheckError, match="heterogeneous key"):
        _typed('{"a": 1, 2: 3}')


def test_map_literal_heterogeneous_values_fail() -> None:
    with pytest.raises(TypeCheckError, match="heterogeneous value"):
        _typed('{"a": 1, "b": "x"}')


def test_empty_map_literal_fails() -> None:
    with pytest.raises(TypeCheckError, match="empty map"):
        _typed("{}")


# ---------------------------------------------------------------------------
# Member / Index on plain values
# ---------------------------------------------------------------------------


def test_member_access_on_scalar_is_type_error() -> None:
    with pytest.raises(TypeCheckError, match="cannot access member"):
        _typed('"hello".length')


def test_index_into_list_with_int_returns_element() -> None:
    n = _typed("inputs.tags[0]")
    assert n.cel_type == StringType()
    assert isinstance(n, Index)


def test_index_into_list_with_wrong_index_type() -> None:
    with pytest.raises(TypeCheckError, match="list index must be int"):
        _typed('inputs.tags["x"]')


def test_index_into_scalar_is_type_error() -> None:
    with pytest.raises(TypeCheckError, match="cannot index"):
        _typed("inputs.count[0]")


def test_index_with_runtime_string_into_homogeneous_map() -> None:
    # inputs.labels is map<string, string>; indexing by a runtime
    # string value is fine. Use a binding that has a string value.
    n = _typed("inputs.labels[inputs.image]")
    assert n.cel_type == StringType()


def test_index_with_wrong_key_type_into_map() -> None:
    with pytest.raises(TypeCheckError, match="map index must be"):
        _typed("inputs.labels[1]")


# ---------------------------------------------------------------------------
# Member / Index drilling through JSON-Schema arrays (items sub-schema)
# ---------------------------------------------------------------------------


def _array_of_objects_bindings() -> SchemaBindings:
    return _bindings(
        inputs={
            "type": "object",
            "properties": {
                "targets": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "image": {"type": "string"},
                            "tag": {"type": "string"},
                            "meta": {
                                "type": "object",
                                "properties": {"score": {"type": "integer"}},
                            },
                        },
                    },
                },
                "matrix": {
                    "type": "array",
                    "items": {"type": "array", "items": {"type": "integer"}},
                },
                "tags": {"type": "array", "items": {"type": "string"}},
                "records": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": {"type": "string"},
                    },
                },
            },
        }
    )


def test_member_after_index_drills_into_items_object_schema() -> None:
    n = _typed("inputs.targets[0].image", _array_of_objects_bindings())
    assert n.cel_type == StringType()


def test_nested_member_after_index_drills_through_object_items() -> None:
    n = _typed("inputs.targets[0].meta.score", _array_of_objects_bindings())
    assert n.cel_type == IntType()


def test_double_index_into_array_of_arrays() -> None:
    n = _typed("inputs.matrix[0][1]", _array_of_objects_bindings())
    assert n.cel_type == IntType()


def test_index_into_array_of_homogeneous_maps_returns_map() -> None:
    n = _typed('inputs.records[0]["any-key"]', _array_of_objects_bindings())
    assert n.cel_type == StringType()


def test_member_access_on_array_value_without_index_is_type_error() -> None:
    with pytest.raises(TypeCheckError, match="member"):
        _typed("inputs.targets.image", _array_of_objects_bindings())


def test_string_bracket_on_array_value_is_int_index_error() -> None:
    with pytest.raises(TypeCheckError, match="list index must be int"):
        _typed('inputs.targets["image"]', _array_of_objects_bindings())


def test_unknown_field_on_indexed_array_element_is_unbound() -> None:
    from custos_cel import UnboundNameError

    with pytest.raises(UnboundNameError):
        _typed("inputs.targets[0].nope", _array_of_objects_bindings())


def test_index_into_scalar_array_does_not_carry_dotting_drill() -> None:
    # `inputs.tags[0]` is a string; accessing `.length` is invalid CEL
    # and must produce a "cannot access member" error rather than
    # silently succeeding.
    with pytest.raises(TypeCheckError, match="cannot access member"):
        _typed("inputs.tags[0].length", _array_of_objects_bindings())


# ---------------------------------------------------------------------------
# Member / Index drilling through JSON-Schema maps (additionalProperties)
# ---------------------------------------------------------------------------


def _map_of_objects_bindings() -> SchemaBindings:
    return _bindings(
        inputs={
            "type": "object",
            "properties": {
                "key": {"type": "string"},
                "records": {
                    "type": "object",
                    "additionalProperties": {
                        "type": "object",
                        "properties": {
                            "image": {"type": "string"},
                            "meta": {
                                "type": "object",
                                "properties": {"score": {"type": "integer"}},
                            },
                        },
                    },
                },
                "matrices": {
                    "type": "object",
                    "additionalProperties": {
                        "type": "array",
                        "items": {"type": "integer"},
                    },
                },
                "nestedMaps": {
                    "type": "object",
                    "additionalProperties": {
                        "type": "object",
                        "additionalProperties": {"type": "string"},
                    },
                },
                "scalars": {
                    "type": "object",
                    "additionalProperties": {"type": "string"},
                },
            },
        }
    )


def test_member_after_runtime_key_index_drills_into_additional_properties() -> None:
    n = _typed("inputs.records[inputs.key].image", _map_of_objects_bindings())
    assert n.cel_type == StringType()


def test_nested_member_after_runtime_key_index_drills_through_object_values() -> None:
    n = _typed("inputs.records[inputs.key].meta.score", _map_of_objects_bindings())
    assert n.cel_type == IntType()


def test_index_then_index_through_map_of_arrays() -> None:
    n = _typed("inputs.matrices[inputs.key][0]", _map_of_objects_bindings())
    assert n.cel_type == IntType()


def test_index_through_map_of_maps_with_runtime_keys() -> None:
    n = _typed("inputs.nestedMaps[inputs.key][inputs.key]", _map_of_objects_bindings())
    assert n.cel_type == StringType()


def test_member_access_after_runtime_key_on_scalar_map_is_type_error() -> None:
    with pytest.raises(TypeCheckError, match="cannot access member"):
        _typed("inputs.scalars[inputs.key].length", _map_of_objects_bindings())


def test_unknown_field_after_runtime_key_into_map_value_is_unbound() -> None:
    from custos_cel import UnboundNameError

    with pytest.raises(UnboundNameError):
        _typed("inputs.records[inputs.key].nope", _map_of_objects_bindings())


def test_string_literal_key_member_still_works_for_homogeneous_map() -> None:
    # String-literal key is routed through _drill_schema's
    # additionalProperties fallback (the pre-existing path) — this
    # test guards against the new runtime-key drill regressing the
    # literal-key behavior.
    n = _typed('inputs.records["any"].image', _map_of_objects_bindings())
    assert n.cel_type == StringType()


# ---------------------------------------------------------------------------
# Source position propagation
# ---------------------------------------------------------------------------


def test_source_position_carried_on_type_error() -> None:
    err: TypeCheckError
    with pytest.raises(TypeCheckError) as excinfo:
        _typed('"a" + 1')
    err = excinfo.value
    assert err.source_position is not None
    assert err.source_position.line == 1


def test_unbound_error_carries_position() -> None:
    with pytest.raises(UnboundNameError) as excinfo:
        _typed("foo")
    assert excinfo.value.pos is not None


# ---------------------------------------------------------------------------
# JSON-Schema → CelType failure paths
# ---------------------------------------------------------------------------


def test_unsupported_schema_type_at_inputs_property() -> None:
    sb = _bindings(
        inputs={"type": "object", "properties": {"x": {"type": "weird"}}},
    )
    with pytest.raises(TypeCheckError, match="unsupported JSON Schema"):
        type_check(parse("inputs.x"), sb)


def test_array_without_items_is_type_error() -> None:
    sb = _bindings(
        inputs={"type": "object", "properties": {"tags": {"type": "array"}}},
    )
    with pytest.raises(TypeCheckError, match="must declare an 'items'"):
        type_check(parse("inputs.tags"), sb)


def test_nullable_type_list_in_schema_is_accepted() -> None:
    sb = _bindings(
        inputs={
            "type": "object",
            "properties": {"image": {"type": ["string", "null"]}},
        },
    )
    n = type_check(parse("inputs.image"), sb)
    assert n.cel_type == StringType()


def test_unsupported_type_list_in_schema_is_rejected() -> None:
    sb = _bindings(
        inputs={
            "type": "object",
            "properties": {"x": {"type": ["string", "integer"]}},
        },
    )
    with pytest.raises(TypeCheckError, match="type' list"):
        type_check(parse("inputs.x"), sb)


def test_schema_property_not_a_mapping_is_type_error() -> None:
    sb = _bindings(inputs={"type": "object", "properties": {"x": "broken"}})
    with pytest.raises(TypeCheckError, match="not a JSON Schema mapping"):
        type_check(parse("inputs.x"), sb)


# ---------------------------------------------------------------------------
# Returned tree integrity
# ---------------------------------------------------------------------------


def test_every_node_in_typed_tree_carries_cel_type() -> None:
    n = _typed("inputs.count > 0 && steps.scan.outputs.critical + 1 == 5")
    _assert_typed(n)


def _assert_typed(node: Node) -> None:
    assert node.cel_type is not None, f"node {type(node).__name__} missing cel_type"
    for child in _children(node):
        _assert_typed(child)


def _children(n: Node) -> list[Node]:
    if isinstance(n, Member):
        return [n.target]
    if isinstance(n, Index):
        return [n.target, n.index]
    if isinstance(n, Call):
        return list(n.args)
    if isinstance(n, Conditional):
        return [n.cond, n.then_branch, n.else_branch]
    if isinstance(n, Binary):
        return [n.left, n.right]
    if isinstance(n, Unary):
        return [n.operand]
    if isinstance(n, ListLit):
        return list(n.elements)
    if isinstance(n, MapLit):
        out: list[Node] = []
        for k, v in n.entries:
            out.append(k)
            out.append(v)
        return out
    return []


def test_input_tree_is_unchanged_by_type_check() -> None:
    src = "inputs.image"
    parsed = parse(src)
    typed = type_check(parsed, _bindings())
    assert parsed.cel_type is None
    assert typed.cel_type == StringType()
    assert parsed is not typed


# ---------------------------------------------------------------------------
# TypeCheckError attributes
# ---------------------------------------------------------------------------


def test_type_check_error_default_attributes() -> None:
    err = TypeCheckError("oops")
    assert err.kind == "expression.type_error"
    assert err.message == "oops"
    assert err.source_position is None
    assert err.expected_type is None
    assert err.actual_type is None
    # Subclasses the builtin TypeError so callers can catch either.
    assert isinstance(err, TypeError)


def test_type_check_error_with_attributes() -> None:
    pos = SourcePosition(line=2, column=4, offset=10)
    err = TypeCheckError(
        "bad",
        source_position=pos,
        expected_type=IntType(),
        actual_type=StringType(),
    )
    assert err.source_position == pos
    assert err.expected_type == IntType()
    assert err.actual_type == StringType()


# ---------------------------------------------------------------------------
# Module exports
# ---------------------------------------------------------------------------


def test_public_exports() -> None:
    for name in ("SchemaBindings", "TypeCheckError", "type_check", "TimestampType"):
        assert hasattr(custos_cel, name)
        assert name in custos_cel.__all__
