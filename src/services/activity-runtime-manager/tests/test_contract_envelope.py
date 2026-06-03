"""Round-trip + validation tests for the Activity Contract envelopes (ARM-IMPL-003)."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from custos_arm.contract import (
    ActivitySpec,
    CtxEnvelope,
    InputsEnvelope,
    OutputsEnvelope,
    StepRef,
)

# Verbatim JSON examples from the design (§ Activity Contract v1).

_INPUTS_EXAMPLE = {
    "schemaVersion": "1",
    "contractVersion": "1",
    "activity": {"type": "scan-image", "version": "1.2.0"},
    "step": {"runId": "run-1", "stepId": "step-1", "attempt": 1},
    "inputs": {
        "image": {"ref": "ghcr.io/acme/app:v1", "digest": "sha256:abc"},
        "severity": "high",
    },
}

_OUTPUTS_SUCCESS_AUTHOR = {
    "schemaVersion": "1",
    "contractVersion": "1",
    "status": "success",
    "outputs": {
        "findings": 12,
        "reportRef": {"kind": "ArtifactRef", "name": "report"},
    },
}

_OUTPUTS_SUCCESS_FINALIZED = {
    "schemaVersion": "1",
    "contractVersion": "1",
    "status": "success",
    "outputs": {
        "findings": 12,
        "reportRef": {
            "kind": "ArtifactRef",
            "name": "report",
            "id": "art-9f3a",
            "mediaType": "application/vnd.cyclonedx+json",
            "digest": "sha256:def",
            "size": 84231,
        },
    },
    "produced": [
        {
            "kind": "ArtifactRef",
            "name": "report",
            "id": "art-9f3a",
            "mediaType": "application/vnd.cyclonedx+json",
            "digest": "sha256:def",
            "size": 84231,
        }
    ],
}

_OUTPUTS_FAILURE = {
    "schemaVersion": "1",
    "contractVersion": "1",
    "status": "failure",
    "error": {
        "code": "registry.unauthorized",
        "class": "permanent",
        "message": "no credentials for ghcr.io/acme/app",
        "details": {"registry": "ghcr.io"},
    },
    "outputs": {},
}


def test_inputs_envelope_round_trips() -> None:
    env = InputsEnvelope.model_validate(_INPUTS_EXAMPLE)
    assert env.model_dump(by_alias=True, exclude_none=True) == _INPUTS_EXAMPLE


def test_outputs_success_author_round_trips() -> None:
    env = OutputsEnvelope.model_validate(_OUTPUTS_SUCCESS_AUTHOR)
    assert env.model_dump(by_alias=True, exclude_none=True) == _OUTPUTS_SUCCESS_AUTHOR


def test_outputs_success_finalized_round_trips() -> None:
    env = OutputsEnvelope.model_validate(_OUTPUTS_SUCCESS_FINALIZED)
    assert env.model_dump(by_alias=True, exclude_none=True) == _OUTPUTS_SUCCESS_FINALIZED


def test_outputs_failure_round_trips() -> None:
    env = OutputsEnvelope.model_validate(_OUTPUTS_FAILURE)
    assert env.model_dump(by_alias=True, exclude_none=True) == _OUTPUTS_FAILURE


def test_envelopes_default_schema_and_contract_versions() -> None:
    env = InputsEnvelope(
        activity=ActivitySpec(type="t", version="1"),
        step=StepRef(runId="r", stepId="s", attempt=1),
    )
    assert env.schema_version == "1"
    assert env.contract_version == "1"


def test_failure_status_requires_error() -> None:
    with pytest.raises(ValidationError, match="requires an 'error' envelope"):
        OutputsEnvelope.model_validate(
            {"schemaVersion": "1", "contractVersion": "1", "status": "failure", "outputs": {}}
        )


def test_success_status_rejects_error() -> None:
    payload = dict(_OUTPUTS_FAILURE)
    payload["status"] = "success"
    with pytest.raises(ValidationError, match="must not carry an 'error' envelope"):
        OutputsEnvelope.model_validate(payload)


def test_step_attempt_must_be_positive() -> None:
    with pytest.raises(ValidationError):
        StepRef(runId="r", stepId="s", attempt=0)


def test_unknown_envelope_field_is_rejected() -> None:
    payload = dict(_INPUTS_EXAMPLE, surprise="nope")
    with pytest.raises(ValidationError):
        InputsEnvelope.model_validate(payload)


def test_ctx_envelope_round_trips() -> None:
    payload = {
        "schemaVersion": "1",
        "contractVersion": "1",
        "runId": "run-1",
        "stepId": "step-1",
        "attempt": 2,
        "workspaceId": "ws-1",
        "activity": {"type": "scan-image", "version": "1.2.0"},
        "connectors": {
            "ghcr": {
                "host": "ghcr.io",
                "endpoint": "https://ghcr.io",
                "type": "oci-registry",
                "labels": {"env": "prod"},
            }
        },
        "deadline": "2026-06-03T12:00:00Z",
    }
    ctx = CtxEnvelope.model_validate(payload)
    assert ctx.workspace_id == "ws-1"
    assert ctx.connectors["ghcr"].host == "ghcr.io"
    assert ctx.attempt == 2
