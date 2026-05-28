"""Unit tests for :mod:`custos_workflow.document.loader`."""

from __future__ import annotations

import textwrap

import pytest

from custos_workflow.document import (
    ActivityStep,
    DocumentParseError,
    LetStep,
    parse_document,
)

_MINIMAL_YAML = textwrap.dedent(
    """\
    apiVersion: custos.dev/v1
    kind: Workflow
    metadata:
      name: demo
      workspace: default
    spec:
      steps:
        - id: scan
          activity: security/scan@1
          connector: primary
    """
)


class TestParseDocument:
    def test_minimal_document(self) -> None:
        doc = parse_document(_MINIMAL_YAML)
        assert doc.metadata.name == "demo"
        assert doc.metadata.workspace == "default"
        assert isinstance(doc.spec.steps[0], ActivityStep)
        assert doc.spec.steps[0].activity == "security/scan@1"

    def test_block_scalar_preserves_cel_token(self) -> None:
        # CEL tokens contain ``${{ }}`` which YAML treats as plain
        # strings, but authors often use block scalars for readability.
        # The loader must preserve the wrapper byte-for-byte so the
        # CEL parser later sees the original source.
        yaml_text = textwrap.dedent(
            """\
            apiVersion: custos.dev/v1
            kind: Workflow
            metadata:
              name: demo
            spec:
              steps:
                - id: compute
                  let:
                    total: |-
                      ${{ steps.scan.findings.size() }}
            """
        )
        doc = parse_document(yaml_text)
        let_step = doc.spec.steps[0]
        assert isinstance(let_step, LetStep)
        assert let_step.let["total"] == "${{ steps.scan.findings.size() }}"

    def test_invalid_yaml_raises_document_parse_error(self) -> None:
        with pytest.raises(DocumentParseError) as exc_info:
            parse_document("apiVersion: custos.dev/v1\n  : bad indent\n")
        assert "invalid YAML" in str(exc_info.value)
        # The original ``yaml.YAMLError`` is preserved as ``__cause__``.
        assert exc_info.value.__cause__ is not None

    def test_non_mapping_root_rejected(self) -> None:
        with pytest.raises(DocumentParseError) as exc_info:
            parse_document("- just a list\n- of values\n")
        assert "must be a YAML mapping" in str(exc_info.value)

    def test_empty_input_rejected(self) -> None:
        with pytest.raises(DocumentParseError):
            parse_document("")

    def test_schema_violation_wrapped(self) -> None:
        yaml_text = textwrap.dedent(
            """\
            apiVersion: custos.dev/v1
            kind: Workflow
            metadata:
              name: demo
            spec:
              steps:
                - id: scan
                  activity: scan
            """
        )
        with pytest.raises(DocumentParseError) as exc_info:
            parse_document(yaml_text)
        assert "schema validation" in str(exc_info.value)
        # The Pydantic ``ValidationError`` is the cause.
        from pydantic import ValidationError

        assert isinstance(exc_info.value.__cause__, ValidationError)

    def test_full_document_round_trip_via_yaml(self) -> None:
        # A representative document covering every step kind plus
        # defaults, triggers, on_error, and inputs.
        yaml_text = textwrap.dedent(
            """\
            apiVersion: custos.dev/v1
            kind: Workflow
            metadata:
              name: scan-and-promote
              workspace: security
              labels:
                team: appsec
            spec:
              inputs:
                target:
                  type: string
                  required: true
                  description: image reference
                threshold:
                  type: integer
                  default: 10
              defaults:
                retry:
                  maxAttempts: 3
                  backoff:
                    strategy: exponential
                    initialDelay: PT1S
                    maxDelay: PT5M
                    multiplier: 2.0
                  jitter: full
                  respectRetryAfter: true
              triggers:
                - type: schedule
                  connector: scheduler
              steps:
                - id: scan
                  activity: security/scan@1
                  connector: primary
                  with:
                    target: ${{ inputs.target }}
                  on_error:
                    - match:
                        codePrefix: HTTP_5
                      do: retry
                      maxAttempts: 5
                    - match:
                        class: Permanent
                      do: fail
                - id: compute
                  let:
                    severity: ${{ steps.scan.findings.size() }}
                  if: ${{ steps.scan.findings.size() > inputs.threshold }}
                - id: promote
                  workflow: security/promote@1
                  with:
                    image: ${{ inputs.target }}
              on_error:
                - match:
                    code: GLOBAL_KILL
                  do: fail
            """
        )
        doc = parse_document(yaml_text)
        assert len(doc.spec.steps) == 3
        assert doc.spec.inputs is not None
        assert doc.spec.inputs["target"].required is True
        assert doc.spec.defaults is not None
        assert doc.spec.defaults.retry is not None
        assert doc.spec.defaults.retry.max_attempts == 3
        assert doc.spec.triggers is not None
        assert doc.spec.triggers[0].type == "schedule"
        assert doc.spec.on_error is not None
        assert doc.spec.on_error[0].match.code == "GLOBAL_KILL"
