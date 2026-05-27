"""Tests for the sidecar control-channel revoke endpoint (CONN-IMPL-020)."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from custos_sidecar.lease_gateway import GatewayTransportError
from custos_sidecar.revocation import RevocationRegistry

from .conftest import FakeLeaseGateway


def test_revoke_single_lease_returns_revoked(
    control_client: TestClient,
    fake_gateway: FakeLeaseGateway,
    revocation_registry: RevocationRegistry,
) -> None:
    """Happy path: registry empty, CS acks ``revoked`` → 200 + record locally."""
    fake_gateway.next_revoke_statuses = ["revoked"]
    resp = control_client.post(
        "/sidecar-admin/v1/revoke",
        json={"leaseIds": ["lease_001"], "reason": "rotation"},
    )
    assert resp.status_code == 200
    assert resp.json() == {"results": [{"leaseId": "lease_001", "status": "revoked"}]}
    assert fake_gateway.revoke_calls == [(["lease_001"], "rotation")]
    assert revocation_registry.is_revoked("lease_001")
    assert revocation_registry.reason_for("lease_001") == "rotation"


def test_revoke_already_in_registry_short_circuits_cs(
    control_client: TestClient,
    fake_gateway: FakeLeaseGateway,
    revocation_registry: RevocationRegistry,
) -> None:
    """A lease already in the registry returns ``already-revoked`` without
    forwarding to Connector Service (no duplicate audit emission)."""
    # Pre-seed the registry.
    import asyncio

    asyncio.run(revocation_registry.mark_revoked("lease_001", "earlier-rotation"))
    resp = control_client.post(
        "/sidecar-admin/v1/revoke",
        json={"leaseIds": ["lease_001"], "reason": "second-attempt"},
    )
    assert resp.status_code == 200
    assert resp.json() == {"results": [{"leaseId": "lease_001", "status": "already-revoked"}]}
    # CS was NOT called for this id.
    assert fake_gateway.revoke_calls == []
    # First reason preserved.
    assert revocation_registry.reason_for("lease_001") == "earlier-rotation"


def test_revoke_returns_per_lease_statuses_in_order(
    control_client: TestClient,
    fake_gateway: FakeLeaseGateway,
    revocation_registry: RevocationRegistry,
) -> None:
    """Batch revoke: ack order matches input order; CS statuses pass through."""
    fake_gateway.next_revoke_statuses = ["revoked", "not-found", "already-expired"]
    resp = control_client.post(
        "/sidecar-admin/v1/revoke",
        json={
            "leaseIds": ["lease_A", "lease_B", "lease_C"],
            "reason": "test",
        },
    )
    assert resp.status_code == 200
    assert resp.json() == {
        "results": [
            {"leaseId": "lease_A", "status": "revoked"},
            {"leaseId": "lease_B", "status": "not-found"},
            {"leaseId": "lease_C", "status": "already-expired"},
        ],
    }
    # Only the ``revoked`` lease lands in the local registry.
    assert revocation_registry.is_revoked("lease_A")
    assert not revocation_registry.is_revoked("lease_B")
    assert not revocation_registry.is_revoked("lease_C")


def test_revoke_mixes_local_hit_and_cs_call(
    control_client: TestClient,
    fake_gateway: FakeLeaseGateway,
    revocation_registry: RevocationRegistry,
) -> None:
    """Partial batch: some ids in registry, others go to CS — order preserved."""
    import asyncio

    asyncio.run(revocation_registry.mark_revoked("lease_A", "earlier"))
    fake_gateway.next_revoke_statuses = ["revoked", "not-found"]
    resp = control_client.post(
        "/sidecar-admin/v1/revoke",
        json={
            "leaseIds": ["lease_A", "lease_B", "lease_C"],
            "reason": "rotate",
        },
    )
    assert resp.status_code == 200
    assert resp.json() == {
        "results": [
            {"leaseId": "lease_A", "status": "already-revoked"},
            {"leaseId": "lease_B", "status": "revoked"},
            {"leaseId": "lease_C", "status": "not-found"},
        ],
    }
    # Only the ids not already in the registry were forwarded to CS,
    # in the original input order.
    assert fake_gateway.revoke_calls == [(["lease_B", "lease_C"], "rotate")]


def test_revoke_is_idempotent_on_repeat_call(
    control_client: TestClient,
    fake_gateway: FakeLeaseGateway,
    revocation_registry: RevocationRegistry,
) -> None:
    """Calling revoke a second time on the same lease returns ``already-revoked``."""
    fake_gateway.next_revoke_statuses = ["revoked"]
    first = control_client.post(
        "/sidecar-admin/v1/revoke",
        json={"leaseIds": ["lease_001"], "reason": "first"},
    )
    second = control_client.post(
        "/sidecar-admin/v1/revoke",
        json={"leaseIds": ["lease_001"], "reason": "second"},
    )
    assert first.json() == {"results": [{"leaseId": "lease_001", "status": "revoked"}]}
    assert second.json() == {"results": [{"leaseId": "lease_001", "status": "already-revoked"}]}
    # Second call must NOT forward to CS.
    assert fake_gateway.revoke_calls == [(["lease_001"], "first")]


def test_revoke_empty_body_returns_422(control_client: TestClient) -> None:
    resp = control_client.post(
        "/sidecar-admin/v1/revoke",
        json={"reason": "x"},
    )
    assert resp.status_code == 422


def test_revoke_empty_lease_ids_returns_422(control_client: TestClient) -> None:
    resp = control_client.post(
        "/sidecar-admin/v1/revoke",
        json={"leaseIds": [], "reason": "x"},
    )
    assert resp.status_code == 422


def test_revoke_missing_reason_returns_422(control_client: TestClient) -> None:
    resp = control_client.post(
        "/sidecar-admin/v1/revoke",
        json={"leaseIds": ["lease_001"]},
    )
    assert resp.status_code == 422


def test_revoke_extra_fields_rejected(control_client: TestClient) -> None:
    """Pydantic ``extra='forbid'`` rejects unknown body keys."""
    resp = control_client.post(
        "/sidecar-admin/v1/revoke",
        json={"leaseIds": ["lease_001"], "reason": "x", "extra": "boom"},
    )
    assert resp.status_code == 422


def test_revoke_cs_unreachable_returns_503_problem(
    control_client: TestClient,
    fake_gateway: FakeLeaseGateway,
) -> None:
    """CS transport failure surfaces as 503 ``connector-unavailable``."""
    fake_gateway.next_revoke_error = GatewayTransportError("cs down")
    resp = control_client.post(
        "/sidecar-admin/v1/revoke",
        json={"leaseIds": ["lease_001"], "reason": "x"},
    )
    assert resp.status_code == 503
    body = resp.json()
    assert body["title"] == "connector-unavailable"
    assert body["status"] == 503
    assert "cs down" in body["detail"]
    assert resp.headers["content-type"].startswith("application/problem+json")


def test_revoke_healthz_open(control_client: TestClient) -> None:
    """Control-app ``/healthz`` is unauthenticated."""
    resp = control_client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


@pytest.mark.parametrize(
    "ack_status,expected_local_revoked",
    [
        ("revoked", True),
        ("already-revoked", True),
        ("already-expired", False),
        ("not-found", False),
    ],
)
def test_revoke_local_enforcement_only_for_terminal_states(
    control_client: TestClient,
    fake_gateway: FakeLeaseGateway,
    revocation_registry: RevocationRegistry,
    ack_status: str,
    expected_local_revoked: bool,
) -> None:
    """Only ``revoked`` / ``already-revoked`` populate the local registry."""
    fake_gateway.next_revoke_statuses = [ack_status]
    control_client.post(
        "/sidecar-admin/v1/revoke",
        json={"leaseIds": ["lease_001"], "reason": "x"},
    )
    assert revocation_registry.is_revoked("lease_001") is expected_local_revoked
