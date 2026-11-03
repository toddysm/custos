"""Replay snapshots validate both runtime protocol and pinned execution identity."""

import json

import pytest

from custos_spl.ids import WorkspaceId
from custos_spl.interfaces.delivery import WorkflowStart, delivery_fingerprint


def snapshot(
    protocol: object, identity: object = None, *, scoped_hash: bool = True
) -> WorkflowStart:
    """Construct the exact runtime-input and request-hash shapes used by Workflow."""
    request: dict[str, object] = {"workflowVersionId": "version", "inputs": {}}
    payload = {
        "workspace_id": "ws",
        "workflow_version_id": "version",
        "inputs": {},
        "delivery_protocol": protocol,
    }
    if identity is not None:
        payload["execution_identity"] = identity
        if scoped_hash:
            request["executionIdentity"] = identity
    return WorkflowStart(
        WorkspaceId("ws"), "run", delivery_fingerprint(request), json.dumps(payload)
    )


def test_legacy_and_execution_snapshots_use_their_own_fingerprint_contract() -> None:
    """Protocol two stays byte-compatible; protocol three binds the complete stable reference."""
    snapshot(2)
    identity = {"workspaceId": "ws", "principalId": "account", "bindingId": "stable"}
    snapshot(3, identity)
    with pytest.raises(ValueError):
        snapshot(3, identity, scoped_hash=False)
    with pytest.raises(ValueError):
        snapshot(2, identity, scoped_hash=False)
    with pytest.raises(ValueError):
        WorkflowStart(
            WorkspaceId("ws"),
            "run",
            delivery_fingerprint({"workflowVersionId": "version", "inputs": {}}),
            json.dumps(
                {
                    "workspace_id": "ws",
                    "workflow_version_id": "version",
                    "inputs": {},
                    "delivery_protocol": 2,
                    "executionIdentity": identity,
                }
            ),
        )


@pytest.mark.parametrize("protocol", [None, True, 2.0, "3", 1, 4])
def test_snapshot_rejects_unknown_or_coerced_protocol(protocol: object) -> None:
    """Only exact integer runtime protocol versions are meaningful."""
    with pytest.raises(ValueError):
        snapshot(protocol)


@pytest.mark.parametrize(
    "identity",
    [
        None,
        {},
        {"workspaceId": "ws", "principalId": "account"},
        {"workspaceId": "other", "principalId": "account", "bindingId": "stable"},
        {"workspaceId": "ws", "principalId": " account ", "bindingId": "stable"},
        {"workspaceId": "ws", "principalId": "account", "bindingId": "stable", "token": "private"},
        {"workspaceId": "ws", "principalId": "account", "bindingId": True},
    ],
)
def test_execution_snapshot_requires_a_closed_workspace_matching_reference(
    identity: object,
) -> None:
    """Credentials, partial references and cross-workspace identity cannot enter runtime state."""
    with pytest.raises(ValueError):
        snapshot(3, identity)
