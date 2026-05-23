"""Tests for :mod:`custos_catalog.extract` (CS-IMPL-014)."""

from __future__ import annotations

from typing import Any

import pytest

from custos_catalog.extract import (
    ExtractError,
    RoundtripViolation,
    Selector,
    extract,
    self_check_roundtrip,
)


def _workflow(spec: dict[str, Any], name: str = "wf-a") -> dict[str, Any]:
    return {
        "apiVersion": "custos.dev/v1",
        "kind": "Workflow",
        "metadata": {"name": name, "workspace": "ws-1"},
        "spec": spec,
    }


# ---------------------------------------------------------------------------
# Path parsing + navigation
# ---------------------------------------------------------------------------


def test_extract_simple_dotted_path_string() -> None:
    wf = _workflow({"steps": [{"id": "a", "activity": "custos/scan@1"}]})
    sel = Selector(
        path="spec.steps[0].activity",
        placeholder_name="scanActivity",
        placeholder_type="activityRef",
        activity_type="scan",
    )
    template, captured = extract(wf, [sel], template_name="t1")
    assert (
        template["spec"]["workflow"]["steps"][0]["activity"] == "${{ placeholders.scanActivity }}"
    )
    assert captured == {"scanActivity": "custos/scan@1"}


def test_extract_integer_scalar() -> None:
    wf = _workflow({"settings": {"retries": 3}})
    sel = Selector(
        path="spec.settings.retries",
        placeholder_name="retries",
        placeholder_type="integer",
    )
    template, captured = extract(wf, [sel], template_name="t1")
    assert template["spec"]["workflow"]["settings"]["retries"] == "${{ placeholders.retries }}"
    assert captured == {"retries": 3}


def test_extract_boolean_scalar() -> None:
    wf = _workflow({"settings": {"strict": True}})
    sel = Selector(path="spec.settings.strict", placeholder_name="s", placeholder_type="boolean")
    _, captured = extract(wf, [sel], template_name="t1")
    assert captured["s"] is True


def test_extract_null_scalar() -> None:
    wf = _workflow({"settings": {"override": None}})
    sel = Selector(path="spec.settings.override", placeholder_name="o", placeholder_type="json")
    _, captured = extract(wf, [sel], template_name="t1")
    assert captured["o"] is None


def test_extract_declares_placeholder_with_required_default_description() -> None:
    wf = _workflow({"x": "v"})
    sel = Selector(
        path="spec.x",
        placeholder_name="topic",
        placeholder_type="string",
        required=False,
        default="default-topic",
        description="topic name",
    )
    template, _ = extract(wf, [sel], template_name="t1")
    decl = template["spec"]["placeholders"][0]
    assert decl == {
        "name": "topic",
        "type": "string",
        "required": False,
        "default": "default-topic",
        "description": "topic name",
    }


def test_extract_declares_connector_ref_with_connector_type() -> None:
    wf = _workflow({"steps": [{"id": "a", "connector": "docker"}]})
    sel = Selector(
        path="spec.steps[0].connector",
        placeholder_name="c",
        placeholder_type="connectorRef",
        connector_type="docker",
    )
    template, _ = extract(wf, [sel], template_name="t1")
    decl = template["spec"]["placeholders"][0]
    assert decl["type"] == "connectorRef"
    assert decl["connectorType"] == "docker"


def test_extract_declares_activity_ref_with_activity_type() -> None:
    wf = _workflow({"steps": [{"id": "a", "activity": "custos/scan@1"}]})
    sel = Selector(
        path="spec.steps[0].activity",
        placeholder_name="a",
        placeholder_type="activityRef",
        activity_type="scan",
    )
    template, _ = extract(wf, [sel], template_name="t1")
    decl = template["spec"]["placeholders"][0]
    assert decl["activityType"] == "scan"


def test_extract_wildcard_matches_homogeneous_list() -> None:
    wf = _workflow(
        {
            "steps": [
                {"id": "a", "activity": "custos/scan@1"},
                {"id": "b", "activity": "custos/scan@1"},
                {"id": "c", "activity": "custos/scan@1"},
            ],
        },
    )
    sel = Selector(
        path="spec.steps[*].activity",
        placeholder_name="act",
        placeholder_type="activityRef",
        activity_type="scan",
    )
    template, captured = extract(wf, [sel], template_name="t1")
    for step in template["spec"]["workflow"]["steps"]:
        assert step["activity"] == "${{ placeholders.act }}"
    assert captured == {"act": "custos/scan@1"}


def test_extract_metadata_carries_workspace() -> None:
    wf = _workflow({"steps": [{"id": "a"}]})
    template, _ = extract(wf, [], template_name="my-tmpl")
    assert template["metadata"] == {"name": "my-tmpl", "workspace": "ws-1"}
    assert template["apiVersion"] == "custos.dev/v1"
    assert template["kind"] == "WorkflowTemplate"


def test_extract_metadata_without_workspace() -> None:
    wf = {
        "apiVersion": "custos.dev/v1",
        "kind": "Workflow",
        "metadata": {"name": "wf"},
        "spec": {"steps": [{"id": "a"}]},
    }
    template, _ = extract(wf, [], template_name="t")
    assert "workspace" not in template["metadata"]


def test_extract_does_not_mutate_input() -> None:
    wf = _workflow({"x": "original"})
    sel = Selector(path="spec.x", placeholder_name="x", placeholder_type="string")
    extract(wf, [sel], template_name="t1")
    assert wf["spec"]["x"] == "original"  # source must remain untouched


# ---------------------------------------------------------------------------
# Error mapping
# ---------------------------------------------------------------------------


def test_extract_invalid_path_unterminated_bracket() -> None:
    wf = _workflow({"steps": []})
    sel = Selector(path="spec.steps[0", placeholder_name="x", placeholder_type="string")
    with pytest.raises(ExtractError) as exc:
        extract(wf, [sel], template_name="t1")
    assert exc.value.issues[0].code == "invalid_path"


def test_extract_invalid_path_non_integer_index() -> None:
    wf = _workflow({"steps": []})
    sel = Selector(path="spec.steps[abc]", placeholder_name="x", placeholder_type="string")
    with pytest.raises(ExtractError) as exc:
        extract(wf, [sel], template_name="t1")
    assert exc.value.issues[0].code == "invalid_path"


def test_extract_invalid_path_empty() -> None:
    wf = _workflow({"x": "v"})
    sel = Selector(path="", placeholder_name="x", placeholder_type="string")
    with pytest.raises(ExtractError) as exc:
        extract(wf, [sel], template_name="t1")
    assert exc.value.issues[0].code == "invalid_path"


def test_extract_no_match() -> None:
    wf = _workflow({"steps": [{"id": "a"}]})
    sel = Selector(path="spec.missing", placeholder_name="x", placeholder_type="string")
    with pytest.raises(ExtractError) as exc:
        extract(wf, [sel], template_name="t1")
    assert exc.value.issues[0].code == "no_match"


def test_extract_non_scalar_target_dict() -> None:
    wf = _workflow({"settings": {"a": 1}})
    sel = Selector(path="spec.settings", placeholder_name="x", placeholder_type="string")
    with pytest.raises(ExtractError) as exc:
        extract(wf, [sel], template_name="t1")
    assert exc.value.issues[0].code == "non_scalar_target"


def test_extract_non_scalar_target_list() -> None:
    wf = _workflow({"steps": [{"id": "a"}]})
    sel = Selector(path="spec.steps", placeholder_name="x", placeholder_type="json")
    with pytest.raises(ExtractError) as exc:
        extract(wf, [sel], template_name="t1")
    assert exc.value.issues[0].code == "non_scalar_target"


def test_extract_wildcard_inhomogeneous_rejected() -> None:
    wf = _workflow(
        {
            "steps": [
                {"id": "a", "activity": "custos/scan@1"},
                {"id": "b", "activity": "custos/scan@2"},
            ],
        },
    )
    sel = Selector(
        path="spec.steps[*].activity",
        placeholder_name="act",
        placeholder_type="activityRef",
    )
    with pytest.raises(ExtractError) as exc:
        extract(wf, [sel], template_name="t1")
    assert exc.value.issues[0].code == "inhomogeneous_wildcard"


def test_extract_duplicate_placeholder_name_rejected() -> None:
    wf = _workflow({"a": "1", "b": "2"})
    selectors = [
        Selector(path="spec.a", placeholder_name="dup", placeholder_type="string"),
        Selector(path="spec.b", placeholder_name="dup", placeholder_type="string"),
    ]
    with pytest.raises(ExtractError) as exc:
        extract(wf, selectors, template_name="t1")
    assert any(i.code == "duplicate_placeholder_name" for i in exc.value.issues)


def test_extract_collects_multiple_issues() -> None:
    wf = _workflow({"steps": [{"id": "a"}]})
    selectors = [
        Selector(path="spec.missing", placeholder_name="x", placeholder_type="string"),
        Selector(path="spec.[", placeholder_name="y", placeholder_type="string"),
    ]
    with pytest.raises(ExtractError) as exc:
        extract(wf, selectors, template_name="t1")
    codes = {i.code for i in exc.value.issues}
    assert "no_match" in codes
    assert "invalid_path" in codes


# ---------------------------------------------------------------------------
# Round-trip property
# ---------------------------------------------------------------------------


def test_roundtrip_holds_for_scalar_selectors() -> None:
    wf = _workflow(
        {
            "steps": [
                {"id": "a", "activity": "custos/scan@1"},
            ],
        },
    )
    sel = Selector(
        path="spec.steps[0].activity",
        placeholder_name="act",
        placeholder_type="activityRef",
        activity_type="scan",
    )
    template, captured = extract(wf, [sel], template_name="t1")
    # Should not raise.
    self_check_roundtrip(template, wf, captured)


def test_roundtrip_holds_for_integer_placeholder() -> None:
    wf = _workflow({"settings": {"retries": 5}})
    sel = Selector(
        path="spec.settings.retries",
        placeholder_name="retries",
        placeholder_type="integer",
    )
    template, captured = extract(wf, [sel], template_name="t1")
    self_check_roundtrip(template, wf, captured)


def test_roundtrip_holds_for_wildcard() -> None:
    wf = _workflow(
        {
            "steps": [
                {"id": "a", "activity": "custos/scan@1"},
                {"id": "b", "activity": "custos/scan@1"},
            ],
        },
    )
    sel = Selector(
        path="spec.steps[*].activity",
        placeholder_name="act",
        placeholder_type="activityRef",
        activity_type="scan",
    )
    template, captured = extract(wf, [sel], template_name="t1")
    self_check_roundtrip(template, wf, captured)


def test_roundtrip_violation_when_template_tampered_with() -> None:
    wf = _workflow({"x": "v1"})
    sel = Selector(path="spec.x", placeholder_name="x", placeholder_type="string")
    template, captured = extract(wf, [sel], template_name="t1")
    # Corrupt the template body so re-materialization drifts.
    template["spec"]["workflow"]["x"] = "${{ placeholders.x }}"  # fine
    template["spec"]["workflow"]["extra"] = "drift"
    with pytest.raises(RoundtripViolation) as exc:
        self_check_roundtrip(template, wf, captured)
    # Diff should reference the drifting field.
    assert "drift" in exc.value.diff or "extra" in exc.value.diff
