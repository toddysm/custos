"""Tests for :mod:`custos_catalog.placeholders` (CS-IMPL-012)."""

from __future__ import annotations

import pytest

from custos_catalog.placeholders import (
    PlaceholderBindingError,
    PlaceholderDeclaration,
    PlaceholderDeclarationError,
    effective_bindings,
    parse_declarations,
    validate_placeholder_bindings,
    validate_placeholder_declarations,
)

# ---------------------------------------------------------------------------
# parse_declarations
# ---------------------------------------------------------------------------


def test_parse_declarations_minimal() -> None:
    decls = parse_declarations(
        [
            {"name": "topic", "type": "string"},
        ],
    )
    assert len(decls) == 1
    assert decls[0].name == "topic"
    assert decls[0].type == "string"
    assert decls[0].required is True  # default
    assert decls[0].default is None
    assert decls[0].has_default is False


def test_parse_declarations_optional_with_default() -> None:
    decls = parse_declarations(
        [
            {"name": "retries", "type": "integer", "required": False, "default": 3},
        ],
    )
    assert decls[0].required is False
    assert decls[0].default == 3
    assert decls[0].has_default is True


def test_parse_declarations_connector_ref_pins_type() -> None:
    decls = parse_declarations(
        [
            {
                "name": "reg",
                "type": "connectorRef",
                "connectorType": "oci-registry",
            },
        ],
    )
    assert decls[0].connector_type == "oci-registry"


def test_parse_declarations_activity_ref_pins_type() -> None:
    decls = parse_declarations(
        [
            {
                "name": "scan",
                "type": "activityRef",
                "activityType": "vuln-scan",
            },
        ],
    )
    assert decls[0].activity_type == "vuln-scan"


# ---------------------------------------------------------------------------
# validate_placeholder_declarations — well-formedness
# ---------------------------------------------------------------------------


def test_validate_declarations_accepts_unique_well_formed_set() -> None:
    decls = parse_declarations(
        [
            {"name": "topic", "type": "string"},
            {"name": "retries", "type": "integer", "default": 3},
            {"name": "verbose", "type": "boolean", "default": True},
            {"name": "extras", "type": "json", "default": {"a": 1}},
        ],
    )
    validate_placeholder_declarations(decls)


def test_validate_declarations_rejects_duplicate_names() -> None:
    decls = parse_declarations(
        [
            {"name": "topic", "type": "string"},
            {"name": "topic", "type": "integer"},
        ],
    )
    with pytest.raises(PlaceholderDeclarationError) as exc:
        validate_placeholder_declarations(decls)
    issues = exc.value.issues
    assert len(issues) == 1
    assert issues[0].code == "duplicate_name"
    assert "topic" in issues[0].message
    assert issues[0].path == "placeholders[1].name"


def test_validate_declarations_rejects_unknown_type() -> None:
    decls = [
        PlaceholderDeclaration(name="weird", type="bogus"),  # type: ignore[arg-type]
    ]
    with pytest.raises(PlaceholderDeclarationError) as exc:
        validate_placeholder_declarations(decls)
    codes = [i.code for i in exc.value.issues]
    assert "unknown_type" in codes


@pytest.mark.parametrize(
    ("type_", "bad_default", "type_name"),
    [
        ("string", 5, "int"),
        ("integer", "five", "str"),
        ("integer", True, "bool"),
        ("number", True, "bool"),
        ("boolean", 1, "int"),
        ("connectorRef", 42, "int"),
        ("activityRef", "", None),  # empty string
    ],
)
def test_validate_declarations_rejects_default_type_mismatch(
    type_: str,
    bad_default: object,
    type_name: str | None,
) -> None:
    decls = parse_declarations(
        [
            {"name": "p", "type": type_, "default": bad_default},
        ],
    )
    with pytest.raises(PlaceholderDeclarationError) as exc:
        validate_placeholder_declarations(decls)
    issue = exc.value.issues[0]
    assert issue.code == "default_type_mismatch"
    assert issue.path == "placeholders[0].default"
    if type_name is not None:
        assert type_name in issue.message


def test_validate_declarations_passes_expression_default_through() -> None:
    # Expression-form defaults are opaque (evaluated at runtime), so
    # type compatibility is not enforced.
    decls = parse_declarations(
        [
            {
                "name": "topic",
                "type": "integer",
                "default": "${{ inputs.count }}",
            },
        ],
    )
    validate_placeholder_declarations(decls)


def test_validate_declarations_accepts_number_for_integer_default_only_when_int() -> None:
    # number declared with int default is fine
    decls = parse_declarations([{"name": "p", "type": "number", "default": 3}])
    validate_placeholder_declarations(decls)
    # number declared with float default is fine
    decls = parse_declarations([{"name": "p", "type": "number", "default": 3.14}])
    validate_placeholder_declarations(decls)


def test_validate_declarations_json_default_recursively_checked() -> None:
    decls = parse_declarations(
        [
            {
                "name": "weird",
                "type": "json",
                "default": {"a": [1, 2, object()]},
            },
        ],
    )
    with pytest.raises(PlaceholderDeclarationError) as exc:
        validate_placeholder_declarations(decls)
    assert exc.value.issues[0].code == "default_type_mismatch"


# ---------------------------------------------------------------------------
# validate_placeholder_bindings
# ---------------------------------------------------------------------------


def _decls_for_bindings() -> list[PlaceholderDeclaration]:
    return parse_declarations(
        [
            {
                "name": "registryConnector",
                "type": "connectorRef",
                "connectorType": "oci-registry",
                "required": True,
            },
            {
                "name": "topic",
                "type": "string",
                "required": False,
                "default": "default-topic",
            },
            {
                "name": "retries",
                "type": "integer",
                "required": False,
            },
        ],
    )


def test_validate_bindings_accepts_required_with_default_optional_with_default() -> None:
    decls = _decls_for_bindings()
    validate_placeholder_bindings(
        decls,
        {"registryConnector": "my-workspace/my-registry"},
    )


def test_validate_bindings_rejects_missing_required_no_default() -> None:
    decls = _decls_for_bindings()
    with pytest.raises(PlaceholderBindingError) as exc:
        validate_placeholder_bindings(decls, {})
    issues = exc.value.issues
    assert len(issues) == 1
    assert issues[0].code == "required_binding_missing"
    assert issues[0].path == "bindings.registryConnector"


def test_validate_bindings_rejects_type_mismatch() -> None:
    decls = _decls_for_bindings()
    with pytest.raises(PlaceholderBindingError) as exc:
        validate_placeholder_bindings(
            decls,
            {"registryConnector": "my-workspace/my-registry", "retries": "lots"},
        )
    issue = exc.value.issues[0]
    assert issue.code == "binding_type_mismatch"
    assert issue.path == "bindings.retries"


def test_validate_bindings_rejects_unknown_placeholder() -> None:
    decls = _decls_for_bindings()
    with pytest.raises(PlaceholderBindingError) as exc:
        validate_placeholder_bindings(
            decls,
            {
                "registryConnector": "my-workspace/my-registry",
                "bogus": "value",
            },
        )
    issue = exc.value.issues[0]
    assert issue.code == "unknown_placeholder"
    assert issue.path == "bindings.bogus"


def test_validate_bindings_collects_all_issues_in_one_pass() -> None:
    decls = _decls_for_bindings()
    with pytest.raises(PlaceholderBindingError) as exc:
        validate_placeholder_bindings(
            decls,
            {"retries": "lots", "bogus": "value"},
        )
    codes = sorted(i.code for i in exc.value.issues)
    assert codes == ["binding_type_mismatch", "required_binding_missing", "unknown_placeholder"]


def test_validate_bindings_passes_expression_binding_through() -> None:
    decls = _decls_for_bindings()
    validate_placeholder_bindings(
        decls,
        {
            "registryConnector": "my-workspace/my-registry",
            "retries": "${{ inputs.count }}",
        },
    )


def test_validate_bindings_integer_excludes_bool() -> None:
    decls = parse_declarations([{"name": "n", "type": "integer", "required": True}])
    with pytest.raises(PlaceholderBindingError) as exc:
        validate_placeholder_bindings(decls, {"n": True})
    assert exc.value.issues[0].code == "binding_type_mismatch"


def test_validate_bindings_boolean_rejects_int() -> None:
    decls = parse_declarations([{"name": "b", "type": "boolean", "required": True}])
    with pytest.raises(PlaceholderBindingError) as exc:
        validate_placeholder_bindings(decls, {"b": 1})
    assert exc.value.issues[0].code == "binding_type_mismatch"


def test_validate_bindings_json_accepts_any_json() -> None:
    decls = parse_declarations([{"name": "x", "type": "json"}])
    validate_placeholder_bindings(decls, {"x": {"nested": [1, 2.5, True, None, "s"]}})


# ---------------------------------------------------------------------------
# effective_bindings
# ---------------------------------------------------------------------------


def test_effective_bindings_applies_defaults() -> None:
    decls = _decls_for_bindings()
    result = effective_bindings(
        decls,
        {"registryConnector": "my-workspace/my-registry"},
    )
    assert result == {
        "registryConnector": "my-workspace/my-registry",
        "topic": "default-topic",
    }
    # retries: optional, no default, no binding → omitted
    assert "retries" not in result


def test_effective_bindings_overrides_default() -> None:
    decls = _decls_for_bindings()
    result = effective_bindings(
        decls,
        {
            "registryConnector": "my-workspace/my-registry",
            "topic": "explicit-topic",
        },
    )
    assert result["topic"] == "explicit-topic"


def test_effective_bindings_drops_unknown() -> None:
    decls = _decls_for_bindings()
    result = effective_bindings(
        decls,
        {
            "registryConnector": "my-workspace/my-registry",
            "bogus": "ignored",
        },
    )
    assert "bogus" not in result
