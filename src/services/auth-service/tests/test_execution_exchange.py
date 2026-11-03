"""Real Auth credential verification, scoped authorization and signing for execution contexts."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from custos_spl.errors import BackendUnavailable
from custos_spl.ids import PrincipalId, RoleBindingId, WorkspaceId
from custos_spl.interfaces.auth_store import RoleBinding, WorkspaceScope
from custos_spl.interfaces.metadata_store import AuditEvent
from fastapi import FastAPI
from fastapi.testclient import TestClient

from custos_auth.callctx_signer import decode_claims_unverified
from custos_auth.roles import ROLE_WORKSPACE_EXECUTOR
from custos_auth.settings import SettingsError
from custos_auth.tokens import mint_token
from tests._fakes import FakeAuthAdapter, FakeMetadataAdapter
from tests.test_rpc import SA, WORKSPACE, _seed_sa, _seed_token, _seed_workspace

PATH = "/rpc/callctx.exchangeExecution"
HEADERS = {
    "dapr-api-token": "unit-app-token",
    "dapr-caller-app-id": "workflow-service",
    "dapr-caller-namespace": "custos",
}


@pytest.fixture
def configured(client: TestClient, app: FastAPI) -> TestClient:
    """Enable only the new guarded exchange while preserving existing test resource ownership."""
    app.state.settings = replace(
        app.state.settings,
        execution_context_enabled=True,
        app_api_token=HEADERS["dapr-api-token"],
        execution_caller_namespace="custos",
    )
    return client


def credential(store: FakeAuthAdapter, *, grant: bool = True) -> tuple[str, str]:
    """Provision a real hashed service credential and an explicit workspace execution grant."""
    _seed_workspace(store)
    _seed_sa(store)
    token, hashed = mint_token()
    token_id = _seed_token(store, hashed)
    if grant:
        identifier = RoleBindingId(uuid4().hex)
        store.role_bindings[identifier] = RoleBinding(
            identifier,
            PrincipalId(SA),
            ROLE_WORKSPACE_EXECUTOR,
            WorkspaceScope(WorkspaceId(WORKSPACE)),
            datetime.now(UTC),
            PrincipalId("operator"),
        )
    return token, token_id


def body(token: str) -> dict[str, str]:
    """Keep the request free of caller-selected permissions, audience and lifetime."""
    return {
        "token": token,
        "workspaceId": WORKSPACE,
        "principalId": SA,
        "operation": "connector-bind",
    }


def test_exchange_issues_one_verified_grant_and_a_fresh_context(
    configured: TestClient,
    fake_auth_store: FakeAuthAdapter,
) -> None:
    """The actual signer/verifier round trip binds scope, principal, operation and short expiry."""
    token, _ = credential(fake_auth_store)
    issued = []
    for _ in range(2):
        response = configured.post(PATH, json=body(token), headers=HEADERS)
        assert response.status_code == 200, response.text
        signed = response.json()
        assert 0 < signed["exp"] - signed["iat"] <= 300
        verified = configured.post(
            "/rpc/callctx.verify",
            json={"token": signed["token"]},
            headers={"x-custos-callctx": json.dumps({"principal_id": "unit-verifier"})},
        ).json()
        assert verified["valid"] is True
        assert verified["acting_principal_id"] == SA
        assert verified["workspace_id"] == WORKSPACE
        assert verified["caller_component"] == "workflow-service"
        assert verified["permissions"] == ["connector:bind"]
        assert decode_claims_unverified(signed["token"])["executionOperation"] == "connector-bind"
        issued.append(signed["jti"])
    assert issued[0] != issued[1]


@pytest.mark.parametrize(
    "change,status",
    [
        ({}, 401),
        ({"dapr-api-token": "incorrect"}, 401),
        ({"dapr-caller-app-id": "api-gateway"}, 403),
        ({"dapr-caller-namespace": "other"}, 403),
    ],
)
def test_exchange_requires_exact_mesh_caller(
    configured: TestClient,
    fake_auth_store: FakeAuthAdapter,
    change: dict[str, str],
    status: int,
) -> None:
    """A valid account credential cannot bypass the local-token and app/namespace checks."""
    token, _ = credential(fake_auth_store)
    headers = {**HEADERS, **change} if change else {}
    response = configured.post(PATH, json=body(token), headers=headers)
    assert response.status_code == status
    assert token not in response.text


@pytest.mark.parametrize(
    "field,value",
    [
        ("principalId", "another-account"),
        ("workspaceId", "another-workspace"),
    ],
)
def test_credential_cannot_assert_a_different_identity(
    configured: TestClient,
    fake_auth_store: FakeAuthAdapter,
    field: str,
    value: str,
) -> None:
    """Signed identity comes from a credential verified against the exact workspace."""
    token, _ = credential(fake_auth_store)
    response = configured.post(PATH, json={**body(token), field: value}, headers=HEADERS)
    assert response.status_code == 403


@pytest.mark.parametrize("field", ["permissions", "audience", "callerComponent", "ttlSeconds"])
def test_exchange_rejects_asserted_grants(
    configured: TestClient,
    fake_auth_store: FakeAuthAdapter,
    field: str,
) -> None:
    """No arbitrary signer input is accepted even from an authenticated execution caller."""
    token, _ = credential(fake_auth_store)
    response = configured.post(
        PATH,
        json={**body(token), field: "untrusted"},
        headers=HEADERS,
    )
    assert response.status_code == 422 and token not in response.text


@pytest.mark.parametrize("failure", ["no-grant", "revoked", "expired", "disabled"])
def test_invalid_or_unprivileged_account_is_not_signed(
    configured: TestClient,
    fake_auth_store: FakeAuthAdapter,
    failure: str,
) -> None:
    """Negative credential/grant state is enforced before the first cache entry can be accepted."""
    token, identifier = credential(fake_auth_store, grant=failure != "no-grant")
    row = fake_auth_store.service_tokens[identifier]
    if failure == "revoked":
        fake_auth_store.service_tokens[identifier] = replace(row, revoked_at=datetime.now(UTC))
    elif failure == "expired":
        fake_auth_store.service_tokens[identifier] = replace(
            row, expires_at=datetime.now(UTC) - timedelta(seconds=1)
        )
    elif failure == "disabled":
        fake_auth_store.principals[SA] = replace(
            fake_auth_store.principals[SA],
            disabled_at=datetime.now(UTC),
        )
    response = configured.post(PATH, json=body(token), headers=HEADERS)
    assert response.status_code == (403 if failure == "no-grant" else 401)
    assert token not in response.text


def test_execution_workload_cannot_use_legacy_arbitrary_signing(
    configured: TestClient,
) -> None:
    """Enabling execution exchange restricts legacy signing to the authenticated Gateway."""
    request = {
        "principal_id": "forged-admin",
        "workspace_id": WORKSPACE,
        "caller_component": "api-gateway",
        "permissions": ["admin:service-account"],
    }
    assert configured.post("/rpc/callctx.sign", json=request, headers=HEADERS).status_code == 403
    assert configured.post("/rpc/callctx.sign", json=request).status_code == 401


@pytest.mark.parametrize("field", ["executionOperation", "execution_operation"])
def test_legacy_signer_cannot_assert_execution_provenance(
    configured: TestClient, field: str
) -> None:
    """Even the authenticated legacy Gateway cannot request the credential-exchange marker."""
    response = configured.post(
        "/rpc/callctx.sign",
        headers={**HEADERS, "dapr-caller-app-id": "api-gateway"},
        json={
            "principal_id": SA,
            "workspace_id": WORKSPACE,
            "caller_component": "api-gateway",
            "permissions": ["connector:bind"],
            field: "connector-bind",
        },
    )
    assert response.status_code == 422


def test_storage_outage_is_explicit_and_private(
    configured: TestClient,
    fake_auth_store: FakeAuthAdapter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Backend failure cannot produce a token or expose raw driver diagnostics."""
    token, _ = credential(fake_auth_store)
    monkeypatch.setattr(
        fake_auth_store, "get_workspace", AsyncMock(side_effect=BackendUnavailable("private-value"))
    )
    response = configured.post(PATH, json=body(token), headers=HEADERS)
    assert response.status_code == 503 and "private-value" not in response.text


def test_exchange_is_default_off(client: TestClient) -> None:
    """Adding a route cannot activate credential exchange on an existing deployment."""
    response = client.post(PATH, json=body("synthetic-not-a-token"), headers=HEADERS)
    assert response.status_code == 503


def test_duplicate_mesh_headers_are_rejected(
    configured: TestClient,
    fake_auth_store: FakeAuthAdapter,
) -> None:
    """Even identical duplicate identities/tokens are ambiguous, not an accepted caller."""
    token, _ = credential(fake_auth_store)
    for name in HEADERS:
        response = configured.post(
            PATH, json=body(token), headers=[*HEADERS.items(), (name, HEADERS[name])]
        )
        assert response.status_code == (401 if name == "dapr-api-token" else 403)


def test_workflow_and_gateway_identities_cannot_collapse(
    configured: TestClient,
    app: FastAPI,
) -> None:
    """A configuration mistake cannot grant a background workload the legacy Gateway signer."""
    with pytest.raises(SettingsError):
        replace(app.state.settings, execution_gateway_app_id="workflow-service")


def test_legacy_gateway_cannot_assert_a_different_caller(configured: TestClient) -> None:
    """The authenticated Gateway path cannot misattribute newly signed contexts."""
    response = configured.post(
        "/rpc/callctx.sign",
        headers={**HEADERS, "dapr-caller-app-id": "api-gateway"},
        json={
            "principal_id": SA,
            "workspace_id": WORKSPACE,
            "caller_component": "workflow-service",
        },
    )
    assert response.status_code == 403


def test_issuance_audit_contains_no_credential_or_signed_context(
    configured: TestClient,
    fake_auth_store: FakeAuthAdapter,
    fake_metadata_store: FakeMetadataAdapter,
) -> None:
    """Audit the authorized operation and identity, never either bearer secret."""
    token, _ = credential(fake_auth_store)
    response = configured.post(PATH, json=body(token), headers=HEADERS)
    assert response.status_code == 200
    records = [
        event
        for _, event in fake_metadata_store.append_audit_calls
        if isinstance(event, AuditEvent) and event.event_type == "execution.context.issued"
    ]
    assert len(records) == 1
    assert records[0].actor == SA and records[0].payload["permission"] == "connector:bind"
    recorded = repr(fake_metadata_store.append_audit_calls)
    assert token not in recorded and response.json()["token"] not in recorded


def test_issuance_audit_failure_cannot_return_a_token(
    configured: TestClient,
    fake_auth_store: FakeAuthAdapter,
    fake_metadata_store: FakeMetadataAdapter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The exchange refuses acknowledgement when its required issuance evidence cannot persist."""
    token, _ = credential(fake_auth_store)
    append = fake_metadata_store.append_audit

    async def failing(workspace_id: object, event: object, tx: object = None) -> None:
        """Fail the issuance record independently of the pre-existing verification audit calls."""
        if isinstance(event, AuditEvent) and event.event_type == "execution.context.issued":
            raise BackendUnavailable("synthetic-private-diagnostic")
        await append(workspace_id, event, tx)

    monkeypatch.setattr(fake_metadata_store, "append_audit", failing)
    response = configured.post(PATH, json=body(token), headers=HEADERS)
    assert response.status_code == 503
    assert "token" not in response.json()
    assert "synthetic-private-diagnostic" not in response.text
