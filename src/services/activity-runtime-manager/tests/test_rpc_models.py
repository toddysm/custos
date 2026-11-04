"""Unit tests for the RPC wire models / translation (ARM-IMPL-018)."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from custos_arm.rpc import (
    CancelActivityWire,
    ConnectorContextWire,
    ScheduleActivityWire,
)
from custos_arm.rpc.models import BoundConnectorContextWire


def canonical_binding() -> dict[str, object]:
    """Return a structured binding with no made-up lease or expiry."""
    return {
        "schemaVersion": "2",
        "slotName": "registry",
        "connectorInstanceId": "instance-1",
        "connectorKind": "oci-registry",
        "capabilities": ["oci.pull"],
        "context": {
            "endpoint": "https://registry.test",
            "tokenTypeHint": None,
            "handle": {"opaque": {"key": "private-binding-value"}},
            "extras": {"region": "west"},
        },
    }


def test_canonical_binding_preserves_private_structure_without_synthetic_lease() -> None:
    """The actual runtime consumer keeps authoritative metadata and the complete opaque object."""
    wire = BoundConnectorContextWire.model_validate(canonical_binding())
    context = wire.to_connector_context()
    assert context.connector_instance_id == "instance-1"
    assert context.capabilities == ("oci.pull",)
    assert context.lease_id is None and not context.secrets
    assert context.binding == canonical_binding()["context"]
    assert "private-binding-value" not in repr(context)
    assert "private-binding-value" not in repr(wire)


def test_canonical_binding_projects_only_public_connector_metadata() -> None:
    """Actual RPC translation supplies the endpoint required by bundled activities."""
    request = ScheduleActivityWire.model_validate(
        {
            "runId": "run",
            "stepId": "step",
            "attempt": 1,
            "activityRef": "custos.builtin/copy-image@0.1.0",
            "connectorContexts": {"registry": canonical_binding()},
        }
    ).to_schedule_request(workspace_id="ws")
    public = request.connectors["registry"]
    assert public.host == "registry.test"
    assert public.endpoint == "https://registry.test"
    assert public.type == "oci-registry"
    assert "private-binding-value" not in public.model_dump_json()


@pytest.mark.parametrize(
    "endpoint",
    [
        "https://user:private@registry.test",
        "https://registry.test?token=private",
        "https://registry.test#private",
        "https://registry.test:bad",
        "https:///missing",
        "https://registry.\ntest",
    ],
)
def test_unsafe_public_endpoint_is_rejected(endpoint: str) -> None:
    """Canonical endpoints cannot publish embedded credentials or ambiguous authorities."""
    descriptor = canonical_binding()
    context = descriptor["context"]
    assert isinstance(context, dict)
    context["endpoint"] = endpoint
    with pytest.raises(ValidationError):
        BoundConnectorContextWire.model_validate(descriptor)


@pytest.mark.parametrize("version", ["2", "3"])
@pytest.mark.parametrize("version_key", ["schemaVersion", "schema_version"])
def test_versioned_binding_never_downgrades_to_legacy(version: str, version_key: str) -> None:
    """A legacy-looking handle cannot hide malformed or unknown version-two metadata."""
    descriptor = canonical_binding()
    descriptor.pop("schemaVersion")
    descriptor[version_key] = version
    descriptor["handle"] = "fake-lease"
    with pytest.raises(ValidationError):
        ScheduleActivityWire.model_validate(
            {
                "runId": "run",
                "stepId": "step",
                "attempt": 1,
                "activityRef": "acme/test@1",
                "connectorContexts": {"registry": descriptor},
            }
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
