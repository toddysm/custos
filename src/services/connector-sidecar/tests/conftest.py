"""Shared fixtures for the sidecar unit suite.

Builds collaborators with predictable behaviour so each test file can
focus on one slice of the wire surface. Tests that need behaviour
variants (e.g. an upstream-failure minter) construct their own
collaborator inline.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

from custos_sidecar import create_app
from custos_sidecar.auth import BootstrapTokenVerifier, BoundTriple, mint_bootstrap_token
from custos_sidecar.context_registry import ContextRegistry, SlotContext
from custos_sidecar.credential_minter import StubCredentialMinter
from custos_sidecar.lease_gateway import LeaseRecord

# --------------------------------------------------------------------------- #
# Bootstrap / triple
# --------------------------------------------------------------------------- #

HMAC_KEY = b"0123456789abcdef0123456789abcdef"
RUN_ID = "run_01HZX0000000000000000000"
STEP_ID = "build"
ATTEMPT = 1


@pytest.fixture
def bound_triple() -> BoundTriple:
    return BoundTriple(run_id=RUN_ID, step_id=STEP_ID, attempt=ATTEMPT)


@pytest.fixture
def verifier(bound_triple: BoundTriple) -> BootstrapTokenVerifier:
    return BootstrapTokenVerifier(key=HMAC_KEY, triple=bound_triple)


@pytest.fixture
def bootstrap_token(bound_triple: BoundTriple) -> str:
    return mint_bootstrap_token(key=HMAC_KEY, triple=bound_triple, ttl_sec=600)


# --------------------------------------------------------------------------- #
# Context registry
# --------------------------------------------------------------------------- #


@pytest.fixture
def registry() -> ContextRegistry:
    return ContextRegistry(
        [
            SlotContext(
                slot="primary",
                connector_instance_id="ci_01HZX1111111111111111111111",
                capabilities=("read", "write"),
                endpoint="https://upstream.example/primary",
                token_type="Bearer",
                extras={"region": "us-west-2"},
            ),
            SlotContext(
                slot="readonly",
                connector_instance_id="ci_01HZX2222222222222222222222",
                capabilities=("read",),
                endpoint="https://upstream.example/readonly",
                token_type="Bearer",
                extras={},
            ),
        ]
    )


# --------------------------------------------------------------------------- #
# Lease gateway fakes
# --------------------------------------------------------------------------- #


class FakeLeaseGateway:
    """In-memory fake matching the :class:`LeaseGateway` shape.

    Tracks issued lease ids, exposes hooks to flip the next call to
    raise either a :class:`GatewayLeaseError` or a
    :class:`GatewayTransportError`, and records every call for
    assertions.
    """

    def __init__(self) -> None:
        from custos_sidecar.lease_gateway import (  # local import keeps top tidy
            GatewayLeaseError,
            GatewayTransportError,
        )

        self.GatewayLeaseError = GatewayLeaseError
        self.GatewayTransportError = GatewayTransportError
        self.issued: list[LeaseRecord] = []
        self.refreshed: list[str] = []
        self.released: list[str] = []
        self.next_issue_error: GatewayLeaseError | GatewayTransportError | None = None
        self.next_refresh_error: GatewayLeaseError | GatewayTransportError | None = None
        self.next_release_error: GatewayTransportError | None = None
        self._counter = 0

    def _make_record(self, body: dict[str, object]) -> LeaseRecord:
        self._counter += 1
        lease_id = f"lease_{self._counter:03d}"
        now = datetime(2026, 5, 27, 0, 0, 0, tzinfo=UTC)
        expires = datetime(2026, 5, 27, 1, 0, 0, tzinfo=UTC)
        return LeaseRecord(
            workspace_id="ws_test",
            lease_id=lease_id,
            run_id=str(body["runId"]),
            step_id=str(body["stepId"]),
            attempt=int(str(body["attempt"])),
            slot=str(body["slot"]),
            capability=str(body["capability"]),
            connector_instance_id=str(body["connectorInstanceId"]),
            token_type=str(body["tokenType"]),
            issued_at=now,
            expires_at=expires,
            released_at=None,
            revoked_at=None,
            revoke_reason=None,
        )

    async def issue(self, body: dict[str, object]) -> LeaseRecord:
        if self.next_issue_error is not None:
            err = self.next_issue_error
            self.next_issue_error = None
            raise err
        record = self._make_record(body)
        self.issued.append(record)
        return record

    async def refresh(self, body: dict[str, object]) -> LeaseRecord:
        if self.next_refresh_error is not None:
            err = self.next_refresh_error
            self.next_refresh_error = None
            raise err
        lease_id = str(body["leaseId"])
        self.refreshed.append(lease_id)
        # Pretend we know which lease this corresponds to: reuse the
        # most recently issued record but bump expiry.
        if not self.issued:
            # Synthesize a refreshed record so refresh tests can run
            # without first calling issue.
            body = {
                "runId": RUN_ID,
                "stepId": STEP_ID,
                "attempt": ATTEMPT,
                "slot": "primary",
                "capability": "read",
                "connectorInstanceId": "ci_01HZX1111111111111111111111",
                "tokenType": "Bearer",
            }
            record = self._make_record(body)
        else:
            record = self.issued[-1]
        return LeaseRecord(
            workspace_id=record.workspace_id,
            lease_id=lease_id,
            run_id=record.run_id,
            step_id=record.step_id,
            attempt=record.attempt,
            slot=record.slot,
            capability=record.capability,
            connector_instance_id=record.connector_instance_id,
            token_type=record.token_type,
            issued_at=record.issued_at,
            expires_at=datetime(2026, 5, 27, 2, 0, 0, tzinfo=UTC),
            released_at=None,
            revoked_at=None,
            revoke_reason=None,
        )

    async def release(self, lease_id: str) -> None:
        if self.next_release_error is not None:
            err = self.next_release_error
            self.next_release_error = None
            raise err
        self.released.append(lease_id)


@pytest.fixture
def fake_gateway() -> FakeLeaseGateway:
    return FakeLeaseGateway()


@pytest.fixture
def stub_minter() -> StubCredentialMinter:
    return StubCredentialMinter()


# --------------------------------------------------------------------------- #
# Client factory
# --------------------------------------------------------------------------- #


@pytest.fixture
def client(
    verifier: BootstrapTokenVerifier,
    registry: ContextRegistry,
    fake_gateway: FakeLeaseGateway,
    stub_minter: StubCredentialMinter,
) -> Iterator[TestClient]:
    app = create_app(
        bootstrap_verifier=verifier,
        context_registry=registry,
        lease_gateway=fake_gateway,  # type: ignore[arg-type]
        credential_minter=stub_minter,
        bound_triple=(RUN_ID, STEP_ID, ATTEMPT),
    )
    with TestClient(app) as test_client:
        yield test_client
