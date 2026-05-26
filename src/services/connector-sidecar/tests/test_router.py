"""Unit tests for the sidecar router (every failure-mode-table entry)."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from fastapi.testclient import TestClient

from custos_sidecar import create_app
from custos_sidecar.auth import BootstrapTokenVerifier, BoundTriple, mint_bootstrap_token
from custos_sidecar.context_registry import ContextRegistry
from custos_sidecar.credential_minter import (
    CredentialMinter,
    stub_minter_returning_unavailable,
    stub_minter_returning_upstream_failure,
)
from custos_sidecar.lease_gateway import GatewayLeaseError, GatewayTransportError
from custos_sidecar.router import BOOTSTRAP_HEADER
from tests.conftest import ATTEMPT, RUN_ID, STEP_ID, FakeLeaseGateway


def _hdrs(token: str) -> dict[str, str]:
    return {BOOTSTRAP_HEADER: token}


# --------------------------------------------------------------------------- #
# GET /v1/token \u2014 happy path
# --------------------------------------------------------------------------- #


def test_get_token_happy_path(client: TestClient, bootstrap_token: str) -> None:
    resp = client.get(
        "/v1/token",
        params={"slot": "primary", "purpose": "read"},
        headers=_hdrs(bootstrap_token),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["leaseId"].startswith("lease_")
    assert body["tokenType"] == "Bearer"
    assert body["token"].startswith("stub-token::ci_01HZX1111")
    assert body["scope"] == {
        "connectorSlot": "primary",
        "connectorInstanceId": "ci_01HZX1111111111111111111111",
        "capability": "read",
        "runId": RUN_ID,
        "stepId": STEP_ID,
        "attempt": ATTEMPT,
    }
    assert body["endpoint"] == "https://upstream.example/primary"
    assert body["extras"] == {"region": "us-west-2"}


# --------------------------------------------------------------------------- #
# Bootstrap-token failures \u2192 401
# --------------------------------------------------------------------------- #


def test_get_token_missing_bootstrap_header_returns_401(client: TestClient) -> None:
    resp = client.get("/v1/token", params={"slot": "primary", "purpose": "read"})
    assert resp.status_code == 401
    assert resp.headers["content-type"] == "application/problem+json"
    assert resp.json()["title"] == "bootstrap-invalid"


def test_get_token_wrong_signature_returns_401(client: TestClient) -> None:
    bad = mint_bootstrap_token(
        key=b"WRONG_KEY_WRONG_KEY_WRONG_KEY_WR",
        triple=BoundTriple(run_id=RUN_ID, step_id=STEP_ID, attempt=ATTEMPT),
        ttl_sec=60,
    )
    resp = client.get(
        "/v1/token", params={"slot": "primary", "purpose": "read"}, headers=_hdrs(bad)
    )
    assert resp.status_code == 401
    assert resp.json()["title"] == "bootstrap-invalid"


# --------------------------------------------------------------------------- #
# Capability / slot failures
# --------------------------------------------------------------------------- #


def test_get_token_unknown_slot_returns_404(client: TestClient, bootstrap_token: str) -> None:
    resp = client.get(
        "/v1/token",
        params={"slot": "missing", "purpose": "read"},
        headers=_hdrs(bootstrap_token),
    )
    assert resp.status_code == 404
    assert resp.json()["title"] == "slot-not-found"


def test_get_token_forbidden_capability_returns_403(
    client: TestClient, bootstrap_token: str
) -> None:
    resp = client.get(
        "/v1/token",
        params={"slot": "readonly", "purpose": "write"},
        headers=_hdrs(bootstrap_token),
    )
    assert resp.status_code == 403
    assert resp.json()["title"] == "capability-forbidden"


def test_get_token_missing_query_returns_422(client: TestClient, bootstrap_token: str) -> None:
    resp = client.get("/v1/token", headers=_hdrs(bootstrap_token))
    # FastAPI returns 422 for query-validation failures; that's fine because
    # bootstrap auth has already run successfully (header was valid).
    assert resp.status_code == 422


# --------------------------------------------------------------------------- #
# Lease-gateway-driven failures
# --------------------------------------------------------------------------- #


def test_get_token_capacity_exceeded_returns_429_with_retry_after(
    client: TestClient, bootstrap_token: str, fake_gateway: FakeLeaseGateway
) -> None:
    fake_gateway.next_issue_error = GatewayLeaseError(
        "CAPACITY_EXCEEDED", "cap=1", http_status=429, retry_after_sec=5
    )
    resp = client.get(
        "/v1/token",
        params={"slot": "primary", "purpose": "read"},
        headers=_hdrs(bootstrap_token),
    )
    assert resp.status_code == 429
    assert resp.headers["retry-after"] == "5"
    assert resp.json()["title"] == "capacity-exceeded"


def test_get_token_invalid_request_from_cs_returns_400(
    client: TestClient, bootstrap_token: str, fake_gateway: FakeLeaseGateway
) -> None:
    fake_gateway.next_issue_error = GatewayLeaseError(
        "INVALID_REQUEST", "past deadline", http_status=400
    )
    resp = client.get(
        "/v1/token",
        params={"slot": "primary", "purpose": "read"},
        headers=_hdrs(bootstrap_token),
    )
    assert resp.status_code == 400
    assert resp.json()["title"] == "invalid-request"


def test_get_token_cs_unreachable_returns_503(
    client: TestClient, bootstrap_token: str, fake_gateway: FakeLeaseGateway
) -> None:
    fake_gateway.next_issue_error = GatewayTransportError("connect refused")
    resp = client.get(
        "/v1/token",
        params={"slot": "primary", "purpose": "read"},
        headers=_hdrs(bootstrap_token),
    )
    assert resp.status_code == 503
    assert resp.json()["title"] == "connector-unavailable"


# --------------------------------------------------------------------------- #
# Minter-driven failures (upstream 502 / unavailable 503)
# --------------------------------------------------------------------------- #


@contextmanager
def _client_with_minter(
    verifier: BootstrapTokenVerifier,
    registry: ContextRegistry,
    fake_gateway: FakeLeaseGateway,
    minter: CredentialMinter,
) -> Iterator[TestClient]:
    app = create_app(
        bootstrap_verifier=verifier,
        context_registry=registry,
        lease_gateway=fake_gateway,  # type: ignore[arg-type]
        credential_minter=minter,
        bound_triple=(RUN_ID, STEP_ID, ATTEMPT),
    )
    with TestClient(app) as test_client:
        yield test_client


def test_get_token_upstream_mint_failure_returns_502(
    verifier: BootstrapTokenVerifier,
    registry: ContextRegistry,
    fake_gateway: FakeLeaseGateway,
    bootstrap_token: str,
) -> None:
    with _client_with_minter(
        verifier, registry, fake_gateway, stub_minter_returning_upstream_failure()
    ) as client:
        resp = client.get(
            "/v1/token",
            params={"slot": "primary", "purpose": "read"},
            headers=_hdrs(bootstrap_token),
        )
        assert resp.status_code == 502
        assert resp.json()["title"] == "upstream-failed"
        # Lease should have been issued then best-effort-released
        assert len(fake_gateway.issued) == 1
        assert fake_gateway.released == [fake_gateway.issued[0].lease_id]


def test_get_token_minter_instance_unavailable_returns_503(
    verifier: BootstrapTokenVerifier,
    registry: ContextRegistry,
    fake_gateway: FakeLeaseGateway,
    bootstrap_token: str,
) -> None:
    with _client_with_minter(
        verifier, registry, fake_gateway, stub_minter_returning_unavailable()
    ) as client:
        resp = client.get(
            "/v1/token",
            params={"slot": "primary", "purpose": "read"},
            headers=_hdrs(bootstrap_token),
        )
        assert resp.status_code == 503
        assert resp.json()["title"] == "connector-unavailable"
        assert fake_gateway.released == [fake_gateway.issued[0].lease_id]


# --------------------------------------------------------------------------- #
# POST /v1/token/refresh
# --------------------------------------------------------------------------- #


def test_refresh_happy_path(
    client: TestClient, bootstrap_token: str, fake_gateway: FakeLeaseGateway
) -> None:
    # First issue so refresh has a target.
    issued = client.get(
        "/v1/token",
        params={"slot": "primary", "purpose": "read"},
        headers=_hdrs(bootstrap_token),
    ).json()
    resp = client.post(
        "/v1/token/refresh",
        json={"leaseId": issued["leaseId"]},
        headers=_hdrs(bootstrap_token),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["leaseId"] == issued["leaseId"]
    # New token bytes from a fresh mint call.
    assert body["token"].startswith("stub-token::ci_01HZX1111")
    assert fake_gateway.refreshed == [issued["leaseId"]]


def test_refresh_unknown_lease_returns_404(
    client: TestClient, bootstrap_token: str, fake_gateway: FakeLeaseGateway
) -> None:
    fake_gateway.next_refresh_error = GatewayLeaseError("NOT_FOUND", "x", http_status=404)
    resp = client.post(
        "/v1/token/refresh",
        json={"leaseId": "lease_missing"},
        headers=_hdrs(bootstrap_token),
    )
    assert resp.status_code == 404
    assert resp.json()["title"] == "lease-not-found"


def test_refresh_released_lease_returns_410(
    client: TestClient, bootstrap_token: str, fake_gateway: FakeLeaseGateway
) -> None:
    fake_gateway.next_refresh_error = GatewayLeaseError("ALREADY_RELEASED", "gone", http_status=410)
    resp = client.post(
        "/v1/token/refresh",
        json={"leaseId": "lease_x"},
        headers=_hdrs(bootstrap_token),
    )
    assert resp.status_code == 410
    assert resp.json()["title"] == "lease-revoked"


def test_refresh_missing_body_returns_422(client: TestClient, bootstrap_token: str) -> None:
    resp = client.post("/v1/token/refresh", json={}, headers=_hdrs(bootstrap_token))
    assert resp.status_code == 422


def test_refresh_extra_field_rejected_422(client: TestClient, bootstrap_token: str) -> None:
    resp = client.post(
        "/v1/token/refresh",
        json={"leaseId": "lease_x", "extra": True},
        headers=_hdrs(bootstrap_token),
    )
    assert resp.status_code == 422


# --------------------------------------------------------------------------- #
# POST /v1/token/release
# --------------------------------------------------------------------------- #


def test_release_returns_204(
    client: TestClient, bootstrap_token: str, fake_gateway: FakeLeaseGateway
) -> None:
    resp = client.post(
        "/v1/token/release",
        json={"leaseId": "lease_xyz"},
        headers=_hdrs(bootstrap_token),
    )
    assert resp.status_code == 204
    assert fake_gateway.released == ["lease_xyz"]


def test_release_swallows_cs_unavailable_and_returns_204(
    client: TestClient, bootstrap_token: str, fake_gateway: FakeLeaseGateway
) -> None:
    fake_gateway.next_release_error = GatewayTransportError("connect refused")
    resp = client.post(
        "/v1/token/release",
        json={"leaseId": "lease_xyz"},
        headers=_hdrs(bootstrap_token),
    )
    assert resp.status_code == 204
    assert fake_gateway.released == []  # best-effort; no record


def test_release_without_bootstrap_returns_401(client: TestClient) -> None:
    resp = client.post("/v1/token/release", json={"leaseId": "lease_x"})
    assert resp.status_code == 401


# --------------------------------------------------------------------------- #
# /healthz
# --------------------------------------------------------------------------- #


def test_healthz_open(client: TestClient) -> None:
    resp = client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}
