"""Unit tests for the RPC wire models / translation (ARM-IMPL-018)."""

from __future__ import annotations

from datetime import UTC, datetime

from custos_arm.rpc import (
    CancelActivityWire,
    ConnectorContextWire,
    ScheduleActivityWire,
)


def test_connector_context_wire_drops_secrets_and_keeps_handle() -> None:
    wire = ConnectorContextWire.model_validate(
        {
            "slotName": "registry",
            "handle": "lease-abc",
            "connectorKind": "oci-registry",
            "expiresAt": "2030-01-01T00:00:00Z",
        }
    )

    ctx = wire.to_connector_context()

    assert ctx.slot_name == "registry"
    assert ctx.connector_type == "oci-registry"
    assert ctx.connector_instance_id == "lease-abc"
    assert ctx.lease_id == "lease-abc"
    assert dict(ctx.secrets) == {}


def test_schedule_wire_translation_takes_workspace_from_argument() -> None:
    deadline = datetime(2030, 1, 1, tzinfo=UTC)
    wire = ScheduleActivityWire.model_validate(
        {
            "runId": "run-1",
            "stepId": "step-1",
            "attempt": 2,
            "activityRef": "acme/echo@1.0.0",
            "inputs": {"message": "hi"},
            "deadline": "2030-01-01T00:00:00Z",
        }
    )

    request = wire.to_schedule_request(workspace_id="ws-9")

    assert request.workspace_id == "ws-9"
    assert request.step.run_id == "run-1"
    assert request.step.step_id == "step-1"
    assert request.step.attempt == 2
    assert request.activity_ref == "acme/echo@1.0.0"
    assert request.inputs == {"message": "hi"}
    assert request.connector_contexts == ()
    assert request.step_deadline == deadline


def test_schedule_wire_idempotency_key_format() -> None:
    wire = ScheduleActivityWire.model_validate(
        {
            "runId": "run-1",
            "stepId": "step-1",
            "attempt": 3,
            "activityRef": "acme/echo@1.0.0",
        }
    )

    assert wire.idempotency_key() == "run-1|step-1|3"


def test_cancel_wire_parses_coordinates() -> None:
    wire = CancelActivityWire.model_validate({"runId": "run-1", "stepId": "step-1"})

    assert wire.run_id == "run-1"
    assert wire.step_id == "step-1"
