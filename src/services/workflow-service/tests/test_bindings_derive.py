"""Tests for :mod:`custos_workflow.bindings.derive`."""

from __future__ import annotations

import logging
import textwrap

import pytest

from custos_workflow.bindings import (
    ActivityTypeNotFoundError,
    InMemoryActivityTypeRegistry,
    derive_bindings,
)
from custos_workflow.document import parse_document


def _registry() -> InMemoryActivityTypeRegistry:
    return InMemoryActivityTypeRegistry(
        {
            "security/scan@1": {
                "type": "object",
                "properties": {
                    "critical": {"type": "integer"},
                    "findings": {"type": "array"},
                },
            },
            "image-promote/copy@1": {
                "type": "object",
                "properties": {"digest": {"type": "string"}},
            },
        }
    )


_DOC_YAML = textwrap.dedent(
    """\
    apiVersion: custos.dev/v1
    kind: Workflow
    metadata:
      name: pipeline
      workspace: security
    spec:
      inputs:
        target:
          type: string
          required: true
        threshold:
          type: integer
          default: 10
          description: critical-count threshold
      steps:
        - id: scan
          activity: security/scan@1
          connector: primary
        - id: derive
          let:
            severity: ${{ steps.scan.outputs.critical }}
            verdict: ${{ steps.scan.outputs.critical > inputs.threshold }}
        - id: promote
          workflow: security/promote@1
        - id: notify
          activity: image-promote/copy@1
          connector: target-reg
    """
)


class TestInputsSchema:
    def test_inputs_translated_with_required(self) -> None:
        doc = parse_document(_DOC_YAML)
        bindings = derive_bindings(doc, _registry())
        scan_inputs = dict(bindings["scan"].inputs)
        assert scan_inputs["type"] == "object"
        assert scan_inputs["properties"]["target"]["type"] == "string"
        assert scan_inputs["properties"]["threshold"]["type"] == "integer"
        assert scan_inputs["properties"]["threshold"]["default"] == 10
        assert scan_inputs["properties"]["threshold"]["description"] == ("critical-count threshold")
        assert scan_inputs["required"] == ["target"]

    def test_inputs_omitted_when_spec_has_none(self) -> None:
        yaml_text = textwrap.dedent(
            """\
            apiVersion: custos.dev/v1
            kind: Workflow
            metadata:
              name: minimal
            spec:
              steps:
                - id: scan
                  activity: security/scan@1
                  connector: primary
            """
        )
        doc = parse_document(yaml_text)
        bindings = derive_bindings(doc, _registry())
        scan_inputs = dict(bindings["scan"].inputs)
        assert scan_inputs == {"type": "object", "properties": {}}


class TestPriorStepOrdering:
    def test_first_step_has_empty_prior(self) -> None:
        doc = parse_document(_DOC_YAML)
        bindings = derive_bindings(doc, _registry())
        assert bindings["scan"].prior_steps == ()

    def test_each_step_sees_only_prior_steps(self) -> None:
        doc = parse_document(_DOC_YAML)
        bindings = derive_bindings(doc, _registry())
        derive_prior = [sid for sid, _ in bindings["derive"].prior_steps]
        promote_prior = [sid for sid, _ in bindings["promote"].prior_steps]
        notify_prior = [sid for sid, _ in bindings["notify"].prior_steps]
        assert derive_prior == ["scan"]
        assert promote_prior == ["scan", "derive"]
        assert notify_prior == ["scan", "derive", "promote"]

    def test_step_outputs_lookup_helper(self) -> None:
        # Sanity: the bindings.step_outputs_schema helper resolves
        # by id, not by position.
        doc = parse_document(_DOC_YAML)
        bindings = derive_bindings(doc, _registry())
        scan_schema = bindings["notify"].step_outputs_schema("scan")
        assert scan_schema is not None
        assert scan_schema["properties"]["critical"]["type"] == "integer"

    def test_step_not_yet_visible(self) -> None:
        # ``promote`` cannot see ``notify`` (which runs after it).
        doc = parse_document(_DOC_YAML)
        bindings = derive_bindings(doc, _registry())
        assert bindings["promote"].step_outputs_schema("notify") is None


class TestActivityOutputs:
    def test_activity_schema_resolved_from_registry(self) -> None:
        doc = parse_document(_DOC_YAML)
        bindings = derive_bindings(doc, _registry())
        scan_schema = bindings["derive"].step_outputs_schema("scan")
        assert scan_schema is not None
        assert "critical" in scan_schema["properties"]

    def test_unknown_activity_raises(self) -> None:
        doc = parse_document(_DOC_YAML)
        empty = InMemoryActivityTypeRegistry({})
        with pytest.raises(ActivityTypeNotFoundError) as exc_info:
            derive_bindings(doc, empty)
        # Error message names the offending step + ref.
        assert "scan" in str(exc_info.value)
        assert "security/scan@1" in str(exc_info.value)


class TestLetOutputs:
    def test_let_outputs_expose_keys_with_open_props(self) -> None:
        doc = parse_document(_DOC_YAML)
        bindings = derive_bindings(doc, _registry())
        derive_schema = bindings["promote"].step_outputs_schema("derive")
        assert derive_schema is not None
        assert derive_schema["type"] == "object"
        props = derive_schema["properties"]
        assert set(props.keys()) == {"severity", "verdict"}
        # Permissive value types until WF-IMPL-022 tightens them.
        assert props["severity"] == {}
        assert props["verdict"] == {}


class TestSubWorkflowStub:
    def test_workflow_outputs_permissive_and_warns(self, caplog: pytest.LogCaptureFixture) -> None:
        doc = parse_document(_DOC_YAML)
        caplog.set_level(logging.WARNING, logger="custos_workflow.bindings.derive")
        bindings = derive_bindings(doc, _registry())
        promote_schema = bindings["notify"].step_outputs_schema("promote")
        assert promote_schema == {"type": "object"}
        warnings = [r for r in caplog.records if r.message == "binding.unresolved_sub_workflow"]
        assert len(warnings) == 1
        assert warnings[0].step_id == "promote"  # type: ignore[attr-defined]
        assert warnings[0].workflow_ref == "security/promote@1"  # type: ignore[attr-defined]

    def test_logger_override_routes_warning(self) -> None:
        doc = parse_document(_DOC_YAML)
        custom = logging.getLogger("test.custom.derive")
        records: list[logging.LogRecord] = []
        handler = logging.Handler()
        handler.emit = records.append  # type: ignore[assignment,method-assign]
        custom.addHandler(handler)
        custom.setLevel(logging.WARNING)
        try:
            derive_bindings(doc, _registry(), logger=custom)
        finally:
            custom.removeHandler(handler)
        assert any(r.message == "binding.unresolved_sub_workflow" for r in records)


class TestDefaultsAndRunWorkflow:
    def test_run_and_workflow_use_defaults(self) -> None:
        doc = parse_document(_DOC_YAML)
        bindings = derive_bindings(doc, _registry())
        # The SchemaBindings defaults are baked-in static types; we
        # assert they are populated (the exact CelType is the cel
        # library's concern).
        scan = bindings["scan"]
        assert "id" in scan.run
        assert "workspace" in scan.run
        assert "name" in scan.workflow
        assert "version" in scan.workflow
        assert scan.now is not None

    def test_step_let_scope_is_empty_at_step_level(self) -> None:
        # Per-call-site let layering is the call-site collector's job.
        doc = parse_document(_DOC_YAML)
        bindings = derive_bindings(doc, _registry())
        assert dict(bindings["derive"].let) == {}
