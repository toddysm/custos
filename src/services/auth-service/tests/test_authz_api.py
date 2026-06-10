"""HTTP-level tests for ``/v1/authz/verify-and-authorize`` (AS-IMPL-016)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

from custos_spl.ids import (
    PrincipalId,
    RoleBindingId,
    ServiceTokenId,
    TenantId,
    WorkspaceId,
)
from custos_spl.interfaces.auth_store import (
    RoleBinding,
    ServiceAccount,
    ServiceToken,
    Workspace,
    WorkspaceScope,
)
from fastapi.testclient import TestClient

from custos_auth.audit import EVENT_AUTHN_SUCCESS, EVENT_AUTHZ_DECISION
from custos_auth.roles import ROLE_WORKSPACE_VIEWER
from custos_auth.tokens import hash_token, mint_token
from tests._fakes import FakeAuthAdapter, FakeMetadataAdapter

WORKSPACE = "ws-1"
TENANT = "t-1"
SA = "sa-1"


def _seed_workspace(store: FakeAuthAdapter) -> None:
    store.workspaces[WORKSPACE] = Workspace(
        workspace_id=WorkspaceId(WORKSPACE),
        tenant_id=TenantId(TENANT),
        display_name="ws-1",
        disabled_at=None,
        created_at=datetime.now(UTC),
    )


def _seed_sa(store: FakeAuthAdapter) -> None:
    store.principals[SA] = ServiceAccount(
        kind="serviceAccount",
        principal_id=PrincipalId(SA),
        workspace_id=WorkspaceId(WORKSPACE),
        display_name="bot",
        disabled_at=None,
        disabled_reason=None,
        created_at=datetime.now(UTC),
    )


def _seed_token(store: FakeAuthAdapter, plaintext: str, hash: str) -> str:
    token_id = f"tok-{uuid4().hex[:8]}"
    store.service_tokens[token_id] = ServiceToken(
        token_id=ServiceTokenId(token_id),
        service_account_id=PrincipalId(SA),
        hash=hash,
        issued_at=datetime.now(UTC),
        expires_at=datetime.now(UTC) + timedelta(days=30),
        revoked_at=None,
        revoked_by=None,
        revoked_reason=None,
    )
    return token_id


def _grant_viewer(store: FakeAuthAdapter) -> None:
    binding_id = str(uuid4())
    store.role_bindings[binding_id] = RoleBinding(
        binding_id=RoleBindingId(binding_id),
        principal_id=PrincipalId(SA),
        role_id=ROLE_WORKSPACE_VIEWER,
        scope=WorkspaceScope(workspace_id=WorkspaceId(WORKSPACE)),
        bound_at=datetime.now(UTC),
        bound_by=PrincipalId("seed"),
    )


def test_verify_and_authorize_allow_path(
    client: TestClient,
    fake_auth_store: FakeAuthAdapter,
    fake_metadata_store: FakeMetadataAdapter,
) -> None:
    _seed_workspace(fake_auth_store)
    _seed_sa(fake_auth_store)
    _grant_viewer(fake_auth_store)
    plaintext, h = mint_token()
    _seed_token(fake_auth_store, plaintext, h)

    resp = client.post(
        "/v1/authz/verify-and-authorize",
        json={
            "token": plaintext,
            "permission": "catalog:workflows:read",
            "workspace_id": WORKSPACE,
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["principal_id"] == SA
    assert body["allowed"] is True
    assert body["reason"] == "allow-bound"
    # ``audit_event_id`` matches the row authorize emitted.
    authz_events = [
        e
        for _ws, e in fake_metadata_store.append_audit_calls
        if e.event_type == EVENT_AUTHZ_DECISION
    ]
    assert len(authz_events) == 1
    assert body["audit_event_id"] == str(authz_events[0].event_id)
    # And the verify path emitted an authn.success row.
    assert any(
        e.event_type == EVENT_AUTHN_SUCCESS for _ws, e in fake_metadata_store.append_audit_calls
    )


def test_verify_and_authorize_deny_path_returns_200_with_allowed_false(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    # No binding granted — verify succeeds (the bearer is valid) but
    # authorize returns deny. Wire shape: HTTP 200 carrying
    # ``allowed: false`` so the gateway can map this to its own 403
    # without re-interpreting the auth-service response code.
    _seed_workspace(fake_auth_store)
    _seed_sa(fake_auth_store)
    plaintext, h = mint_token()
    _seed_token(fake_auth_store, plaintext, h)

    resp = client.post(
        "/v1/authz/verify-and-authorize",
        json={
            "token": plaintext,
            "permission": "workflow:read",
            "workspace_id": WORKSPACE,
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["allowed"] is False
    assert body["reason"] == "deny-no-binding"


def test_verify_and_authorize_401_when_token_unknown(client: TestClient) -> None:
    resp = client.post(
        "/v1/authz/verify-and-authorize",
        json={
            "token": "custos_unknown",
            "permission": "workflow:read",
            "workspace_id": WORKSPACE,
        },
    )
    assert resp.status_code == 401


def test_verify_and_authorize_401_when_token_expired(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    # AC: "Expired tokens fail verification with token-expired (401)
    # even before the sweeper runs."
    _seed_workspace(fake_auth_store)
    _seed_sa(fake_auth_store)
    plaintext, h = mint_token()
    token_id = _seed_token(fake_auth_store, plaintext, h)
    # Backdate expires_at so the verifier rejects.
    row = fake_auth_store.service_tokens[token_id]
    fake_auth_store.service_tokens[token_id] = ServiceToken(
        token_id=row.token_id,
        service_account_id=row.service_account_id,
        hash=row.hash,
        issued_at=row.issued_at,
        expires_at=datetime.now(UTC) - timedelta(days=1),
        revoked_at=None,
        revoked_by=None,
        revoked_reason=None,
    )

    resp = client.post(
        "/v1/authz/verify-and-authorize",
        json={
            "token": plaintext,
            "permission": "workflow:read",
            "workspace_id": WORKSPACE,
        },
    )
    assert resp.status_code == 401


def test_verify_and_authorize_401_when_token_revoked(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    _seed_workspace(fake_auth_store)
    _seed_sa(fake_auth_store)
    plaintext, h = mint_token()
    token_id = _seed_token(fake_auth_store, plaintext, h)
    # Mark revoked.
    row = fake_auth_store.service_tokens[token_id]
    fake_auth_store.service_tokens[token_id] = ServiceToken(
        token_id=row.token_id,
        service_account_id=row.service_account_id,
        hash=row.hash,
        issued_at=row.issued_at,
        expires_at=row.expires_at,
        revoked_at=datetime.now(UTC),
        revoked_by=PrincipalId("admin"),
        revoked_reason="rotation",
    )
    resp = client.post(
        "/v1/authz/verify-and-authorize",
        json={
            "token": plaintext,
            "permission": "workflow:read",
            "workspace_id": WORKSPACE,
        },
    )
    assert resp.status_code == 401


def test_verify_and_authorize_rejects_empty_token(client: TestClient) -> None:
    resp = client.post(
        "/v1/authz/verify-and-authorize",
        json={"token": "", "permission": "workflow:read", "workspace_id": WORKSPACE},
    )
    assert resp.status_code == 422


def test_verify_and_authorize_rejects_extra_fields(client: TestClient) -> None:
    resp = client.post(
        "/v1/authz/verify-and-authorize",
        json={
            "token": "x",
            "permission": "workflow:read",
            "workspace_id": WORKSPACE,
            "extra": "x",
        },
    )
    assert resp.status_code == 422


def test_verify_and_authorize_bypasses_callctx_middleware(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    # The gateway hits this endpoint *before* it has a call-context,
    # so the middleware must let the request through even with no
    # ``x-custos-callctx`` header.
    _seed_workspace(fake_auth_store)
    _seed_sa(fake_auth_store)
    plaintext, h = mint_token()
    _seed_token(fake_auth_store, plaintext, h)
    resp = client.post(
        "/v1/authz/verify-and-authorize",
        json={
            "token": plaintext,
            "permission": "workflow:read",
            "workspace_id": WORKSPACE,
        },
    )
    # 200 with a deny decision (no binding) is the success
    # path here — the request reached the route, that is the test.
    assert resp.status_code == 200


def test_verify_and_authorize_hashes_token_exactly_once(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    # Sanity check: the SPL lookup must match on the deterministic
    # SHA-256 hash (no bcrypt). Regression: a previous draft used a
    # per-mint salt which broke the lookup.
    _seed_workspace(fake_auth_store)
    _seed_sa(fake_auth_store)
    plaintext, h = mint_token()
    _seed_token(fake_auth_store, plaintext, h)
    assert hash_token(plaintext) == h

    resp = client.post(
        "/v1/authz/verify-and-authorize",
        json={
            "token": plaintext,
            "permission": "workflow:read",
            "workspace_id": WORKSPACE,
        },
    )
    assert resp.status_code == 200
