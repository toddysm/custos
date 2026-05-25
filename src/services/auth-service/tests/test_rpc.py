"""HTTP-level tests for the internal RPC inbound surface (AS-IMPL-025)."""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import jwt
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

from custos_auth.callctx_signer import ALGORITHM, DEFAULT_AUDIENCE, ISSUER
from custos_auth.roles import ROLE_WORKSPACE_VIEWER
from custos_auth.tokens import mint_token
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


def _seed_token(store: FakeAuthAdapter, h: str) -> str:
    token_id = f"tok-{uuid4().hex[:8]}"
    store.service_tokens[token_id] = ServiceToken(
        token_id=ServiceTokenId(token_id),
        service_account_id=PrincipalId(SA),
        hash=h,
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


# ---------------------------------------------------------------------------
# rpc/authn.verifyToken
# ---------------------------------------------------------------------------


def test_rpc_authn_verify_token_returns_principal_on_valid_bearer(
    client: TestClient,
    fake_auth_store: FakeAuthAdapter,
) -> None:
    _seed_workspace(fake_auth_store)
    _seed_sa(fake_auth_store)
    plaintext, h = mint_token()
    _seed_token(fake_auth_store, h)

    resp = client.post("/rpc/authn.verifyToken", json={"token": plaintext})
    assert resp.status_code == 200
    body = resp.json()
    assert body["principal"]["kind"] == "serviceAccount"
    assert body["principal"]["principal_id"] == SA


def test_rpc_authn_verify_token_returns_null_principal_on_unknown_bearer(
    client: TestClient,
) -> None:
    # Unlike the REST verify endpoint, the RPC surface returns a 200
    # with ``principal=null`` so callers don't have to disambiguate
    # transport errors from auth failures by HTTP status.
    resp = client.post("/rpc/authn.verifyToken", json={"token": "not-a-real-token"})
    assert resp.status_code == 200
    assert resp.json() == {"principal": None}


def test_rpc_authn_verify_token_rejects_extra_fields(client: TestClient) -> None:
    resp = client.post(
        "/rpc/authn.verifyToken",
        json={"token": "x", "intent": "evil"},
    )
    assert resp.status_code == 422


def test_rpc_authn_verify_token_does_not_require_call_context_header() -> None:
    # /rpc/authn.verifyToken sits in the middleware bypass list — it
    # is how internal services bootstrap a call-context from a raw
    # bearer.
    from custos_auth.middleware.callctx import _BYPASS_PATHS

    assert "/rpc/authn.verifyToken" in _BYPASS_PATHS


# ---------------------------------------------------------------------------
# rpc/authz.authorize
# ---------------------------------------------------------------------------


def test_rpc_authz_authorize_allow_path(
    client: TestClient,
    fake_auth_store: FakeAuthAdapter,
) -> None:
    _seed_workspace(fake_auth_store)
    _seed_sa(fake_auth_store)
    _grant_viewer(fake_auth_store)

    resp = client.post(
        "/rpc/authz.authorize",
        json={
            "principal_id": SA,
            "permission": "workflow:read",
            "workspace_id": WORKSPACE,
            "caller_component": "workflow-service",
        },
        headers=_callctx_header(),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["allowed"] is True
    assert body["reason"] == "allow-bound"
    assert isinstance(body["audit_event_id"], str)


def test_rpc_authz_authorize_deny_path_returns_200_with_allowed_false(
    client: TestClient,
    fake_auth_store: FakeAuthAdapter,
) -> None:
    _seed_workspace(fake_auth_store)
    _seed_sa(fake_auth_store)
    # No bindings granted.

    resp = client.post(
        "/rpc/authz.authorize",
        json={
            "principal_id": SA,
            "permission": "workflow:execute",
            "workspace_id": WORKSPACE,
            "caller_component": "workflow-service",
        },
        headers=_callctx_header(),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["allowed"] is False


def test_rpc_authz_authorize_requires_call_context_header(client: TestClient) -> None:
    # ``authz.authorize`` is NOT bypassed — by the time a component
    # calls it, it already holds a verified call-context.
    resp = client.post(
        "/rpc/authz.authorize",
        json={
            "principal_id": SA,
            "permission": "workflow:read",
            "workspace_id": WORKSPACE,
            "caller_component": "workflow-service",
        },
    )
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "callctx_missing"


# ---------------------------------------------------------------------------
# rpc/authz.verifyAndAuthorize
# ---------------------------------------------------------------------------


def test_rpc_authz_verify_and_authorize_allow_path(
    client: TestClient,
    fake_auth_store: FakeAuthAdapter,
) -> None:
    _seed_workspace(fake_auth_store)
    _seed_sa(fake_auth_store)
    _grant_viewer(fake_auth_store)
    plaintext, h = mint_token()
    _seed_token(fake_auth_store, h)

    resp = client.post(
        "/rpc/authz.verifyAndAuthorize",
        json={
            "token": plaintext,
            "permission": "workflow:read",
            "workspace_id": WORKSPACE,
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["principal_id"] == SA
    assert body["allowed"] is True


def test_rpc_authz_verify_and_authorize_unauthenticated_returns_401(
    client: TestClient,
) -> None:
    resp = client.post(
        "/rpc/authz.verifyAndAuthorize",
        json={
            "token": "not-a-real-token",
            "permission": "workflow:read",
            "workspace_id": WORKSPACE,
        },
    )
    # Same shape as the REST /v1/authz/verify-and-authorize endpoint
    # so the design's information-hiding guarantee is preserved.
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "unauthenticated"


def test_rpc_authz_verify_and_authorize_in_bypass_list() -> None:
    from custos_auth.middleware.callctx import _BYPASS_PATHS

    assert "/rpc/authz.verifyAndAuthorize" in _BYPASS_PATHS


# ---------------------------------------------------------------------------
# rpc/callctx.sign
# ---------------------------------------------------------------------------


def test_rpc_callctx_sign_returns_token(client: TestClient) -> None:
    resp = client.post(
        "/rpc/callctx.sign",
        json={
            "principal_id": "user-1",
            "workspace_id": WORKSPACE,
            "caller_component": "api-gateway",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert isinstance(body["token"], str) and body["token"]
    assert isinstance(body["kid"], str) and len(body["kid"]) == 16
    assert isinstance(body["jti"], str)
    assert body["exp"] > body["iat"] > 0


def test_rpc_callctx_sign_honors_explicit_ttl(client: TestClient) -> None:
    resp = client.post(
        "/rpc/callctx.sign",
        json={
            "principal_id": "user-1",
            "workspace_id": WORKSPACE,
            "caller_component": "api-gateway",
            "ttl_seconds": 60,
        },
    )
    body = resp.json()
    assert body["exp"] - body["iat"] == 60


def test_rpc_callctx_sign_accepts_null_workspace_id(client: TestClient) -> None:
    # Platform-global RPCs may have no workspace scope.
    resp = client.post(
        "/rpc/callctx.sign",
        json={"principal_id": "user-1", "caller_component": "api-gateway"},
    )
    assert resp.status_code == 200


def test_rpc_callctx_sign_rejects_extra_fields(client: TestClient) -> None:
    resp = client.post(
        "/rpc/callctx.sign",
        json={
            "principal_id": "user-1",
            "caller_component": "api-gateway",
            "extra": True,
        },
    )
    assert resp.status_code == 422


def test_rpc_callctx_sign_in_bypass_list() -> None:
    # callctx.sign is the bootstrap mint endpoint — by definition the
    # caller has no call-context to send yet.
    from custos_auth.middleware.callctx import _BYPASS_PATHS

    assert "/rpc/callctx.sign" in _BYPASS_PATHS


# AS-IMPL-030 Option D enablers — permissions claim + per-mint audience.


def test_rpc_callctx_sign_embeds_permissions_claim(client: TestClient) -> None:
    resp = client.post(
        "/rpc/callctx.sign",
        json={
            "principal_id": "user-1",
            "workspace_id": WORKSPACE,
            "caller_component": "api-gateway",
            "permissions": ["catalog:workflows:read", "catalog:workflows:write"],
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    # The on-wire token must verify back with the embedded permissions
    # claim populated. We round-trip through callctx.verify (rather
    # than re-decoding here) so we exercise the verify path too.
    verify = client.post(
        "/rpc/callctx.verify",
        json={"token": body["token"]},
        headers=_callctx_header(),
    )
    assert verify.status_code == 200
    vbody = verify.json()
    assert vbody["valid"] is True
    assert vbody["permissions"] == [
        "catalog:workflows:read",
        "catalog:workflows:write",
    ]


def test_rpc_callctx_sign_default_omits_permissions_claim(
    client: TestClient,
) -> None:
    """A mint without ``permissions`` produces a token with no claim — and
    ``callctx.verify`` surfaces that as an empty list (the wire shape
    distinguishes "no embedded grant" from "decode failure" via
    ``valid``, not via a missing field)."""
    signed = client.post(
        "/rpc/callctx.sign",
        json={
            "principal_id": "user-1",
            "workspace_id": WORKSPACE,
            "caller_component": "api-gateway",
        },
    ).json()
    verify = client.post(
        "/rpc/callctx.verify",
        json={"token": signed["token"]},
        headers=_callctx_header(),
    )
    assert verify.json()["permissions"] == []


def test_rpc_callctx_sign_rejects_empty_permission_string(
    client: TestClient,
) -> None:
    resp = client.post(
        "/rpc/callctx.sign",
        json={
            "principal_id": "user-1",
            "workspace_id": WORKSPACE,
            "caller_component": "api-gateway",
            "permissions": ["catalog:read", ""],
        },
    )
    assert resp.status_code == 422


def test_rpc_callctx_sign_accepts_audience_override(client: TestClient) -> None:
    """Per-mint audience override targets a downstream component."""
    signed = client.post(
        "/rpc/callctx.sign",
        json={
            "principal_id": "user-1",
            "workspace_id": WORKSPACE,
            "caller_component": "api-gateway",
            "audience": "custos.catalog",
        },
    ).json()
    # The default verify path expects ``custos.internal`` and should
    # therefore refuse this token; passing the explicit override
    # makes it round-trip.
    rejected = client.post(
        "/rpc/callctx.verify",
        json={"token": signed["token"]},
        headers=_callctx_header(),
    )
    rbody = rejected.json()
    assert rbody["valid"] is False
    assert rbody["reason"] == "wrong_audience"

    accepted = client.post(
        "/rpc/callctx.verify",
        json={"token": signed["token"], "audience": "custos.catalog"},
        headers=_callctx_header(),
    )
    abody = accepted.json()
    assert abody["valid"] is True
    assert abody["acting_principal_id"] == "user-1"


def test_rpc_callctx_sign_rejects_empty_audience_override(
    client: TestClient,
) -> None:
    resp = client.post(
        "/rpc/callctx.sign",
        json={
            "principal_id": "user-1",
            "workspace_id": WORKSPACE,
            "caller_component": "api-gateway",
            "audience": "",
        },
    )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# rpc/callctx.verify
# ---------------------------------------------------------------------------


def test_rpc_callctx_verify_round_trips_a_freshly_signed_token(
    client: TestClient,
) -> None:
    # Mint then verify — the happy path. We use the live signer via
    # the sign RPC so we don't have to reach into the app state.
    signed = client.post(
        "/rpc/callctx.sign",
        json={
            "principal_id": "user-1",
            "workspace_id": WORKSPACE,
            "caller_component": "api-gateway",
        },
    ).json()
    resp = client.post(
        "/rpc/callctx.verify",
        json={"token": signed["token"]},
        headers=_callctx_header(),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["valid"] is True
    assert body["reason"] == ""
    assert body["acting_principal_id"] == "user-1"
    assert body["workspace_id"] == WORKSPACE
    assert body["caller_component"] == "api-gateway"
    assert body["kid"] == signed["kid"]
    assert body["jti"] == signed["jti"]


def test_rpc_callctx_verify_rejects_malformed_token(client: TestClient) -> None:
    resp = client.post(
        "/rpc/callctx.verify",
        json={"token": "not-a-jwt"},
        headers=_callctx_header(),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["valid"] is False
    assert body["reason"] == "malformed"


def test_rpc_callctx_verify_rejects_unknown_kid(client: TestClient) -> None:
    # Sign a token with a fresh Ed25519 keypair whose ``kid`` the
    # live KeyRing has never seen. ``jwt.encode`` propagates the
    # ``kid`` header verbatim, so the verifier should reject with
    # ``unknown_kid``.
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    private = Ed25519PrivateKey.generate()
    payload = {
        "iss": ISSUER,
        "aud": DEFAULT_AUDIENCE,
        "iat": int(time.time()),
        "exp": int(time.time()) + 300,
        "actingPrincipalId": "user-1",
        "workspaceId": WORKSPACE,
        "callerComponent": "api-gateway",
    }
    token = jwt.encode(
        payload,
        private,
        algorithm=ALGORITHM,
        headers={"kid": "deadbeefdeadbeef", "typ": "JWT"},
    )
    resp = client.post(
        "/rpc/callctx.verify",
        json={"token": token},
        headers=_callctx_header(),
    )
    body = resp.json()
    assert body["valid"] is False
    assert body["reason"] == "unknown_kid"


def test_rpc_callctx_verify_rejects_expired_token(client: TestClient) -> None:
    # Mint via the signer with a 1-second TTL, then sleep briefly to
    # let it expire (PyJWT enforces ``exp`` strictly).
    signed = client.post(
        "/rpc/callctx.sign",
        json={
            "principal_id": "user-1",
            "workspace_id": WORKSPACE,
            "caller_component": "api-gateway",
            "ttl_seconds": 1,
        },
    ).json()
    # Sleep just past the TTL.
    time.sleep(1.1)
    resp = client.post(
        "/rpc/callctx.verify",
        json={"token": signed["token"]},
        headers=_callctx_header(),
    )
    body = resp.json()
    assert body["valid"] is False
    assert body["reason"] == "expired"


def test_rpc_callctx_verify_rejects_wrong_audience(client: TestClient) -> None:
    # Mint a token with a non-default audience using the live active
    # signing key. ``jwt.decode`` enforces ``aud`` strictly, so the
    # verifier should return ``wrong_audience``.
    app = client.app
    active = app.state.call_context_key_ring.active  # type: ignore[attr-defined]
    payload = {
        "iss": ISSUER,
        "aud": "evil.audience",
        "iat": int(time.time()),
        "exp": int(time.time()) + 300,
        "actingPrincipalId": "user-1",
        "callerComponent": "api-gateway",
    }
    token = jwt.encode(
        payload,
        active.private_key,
        algorithm=ALGORITHM,
        headers={"kid": active.kid, "typ": "JWT"},
    )
    resp = client.post(
        "/rpc/callctx.verify",
        json={"token": token},
        headers=_callctx_header(),
    )
    body = resp.json()
    assert body["valid"] is False
    assert body["reason"] == "wrong_audience"


def test_rpc_callctx_verify_rejects_wrong_issuer(client: TestClient) -> None:
    # Sign with the live key but with a different ``iss`` claim.
    app = client.app
    active = app.state.call_context_key_ring.active  # type: ignore[attr-defined]
    payload = {
        "iss": "evil-issuer",
        "aud": DEFAULT_AUDIENCE,
        "iat": int(time.time()),
        "exp": int(time.time()) + 300,
        "actingPrincipalId": "user-1",
        "callerComponent": "api-gateway",
    }
    token = jwt.encode(
        payload,
        active.private_key,
        algorithm=ALGORITHM,
        headers={"kid": active.kid, "typ": "JWT"},
    )
    resp = client.post(
        "/rpc/callctx.verify",
        json={"token": token},
        headers=_callctx_header(),
    )
    body = resp.json()
    assert body["valid"] is False
    assert body["reason"] == "wrong_issuer"


def test_rpc_callctx_verify_rejects_bad_signature(client: TestClient) -> None:
    # Mint via the signer, then mutate the signature suffix to break
    # the EdDSA verification. PyJWT raises ``InvalidSignatureError``.
    signed = client.post(
        "/rpc/callctx.sign",
        json={"principal_id": "user-1", "caller_component": "api-gateway"},
    ).json()
    parts = signed["token"].split(".")
    # Flip the first signature byte (base64url) — any change breaks
    # the EdDSA check.
    bad_sig = parts[2]
    bad_sig = ("B" if bad_sig[0] == "A" else "A") + bad_sig[1:]
    tampered = ".".join([parts[0], parts[1], bad_sig])
    resp = client.post(
        "/rpc/callctx.verify",
        json={"token": tampered},
        headers=_callctx_header(),
    )
    body = resp.json()
    assert body["valid"] is False
    assert body["reason"] == "bad_signature"


def test_rpc_callctx_verify_requires_call_context_header(client: TestClient) -> None:
    resp = client.post("/rpc/callctx.verify", json={"token": "x"})
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "callctx_missing"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _callctx_header() -> dict[str, str]:
    """Dev-shim call-context header sufficient to pass the middleware."""
    import json as _json

    return {
        "x-custos-callctx": _json.dumps(
            {
                "principal_id": "rpc-caller",
                "workspace_id": WORKSPACE,
                "permissions": [],
            }
        ),
    }


# ---------------------------------------------------------------------------
# AS-IMPL-026: every callctx.verify failure path emits ``call-context.invalid``
# ---------------------------------------------------------------------------


def test_rpc_callctx_verify_malformed_emits_audit(
    client: TestClient,
    fake_metadata_store: FakeMetadataAdapter,
) -> None:
    client.post(
        "/rpc/callctx.verify",
        json={"token": "not-a-jwt"},
        headers=_callctx_header(),
    )
    rows = [
        (ws, ev)
        for ws, ev in fake_metadata_store.append_audit_calls
        if ev.event_type == "call-context.invalid"
    ]
    assert len(rows) == 1
    ws_id, event = rows[0]
    # Filed under the platform sentinel — call-contexts are platform-scoped.
    from custos_auth.audit import PLATFORM_WORKSPACE_ID

    assert ws_id == PLATFORM_WORKSPACE_ID
    assert event.subject == {"reason": "malformed"}
    # No raw token should appear anywhere in the audit row.
    assert "not-a-jwt" not in repr(event.payload)
    assert "not-a-jwt" not in repr(event.subject)


def test_rpc_callctx_verify_unknown_kid_emits_audit_with_kid(
    client: TestClient,
    fake_metadata_store: FakeMetadataAdapter,
) -> None:
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    private = Ed25519PrivateKey.generate()
    payload = {
        "iss": ISSUER,
        "aud": DEFAULT_AUDIENCE,
        "iat": int(time.time()),
        "exp": int(time.time()) + 300,
        "actingPrincipalId": "user-1",
        "workspaceId": WORKSPACE,
        "callerComponent": "api-gateway",
    }
    token = jwt.encode(
        payload,
        private,
        algorithm=ALGORITHM,
        headers={"kid": "deadbeefdeadbeef", "typ": "JWT"},
    )
    client.post(
        "/rpc/callctx.verify",
        json={"token": token},
        headers=_callctx_header(),
    )
    rows = [
        (ws, ev)
        for ws, ev in fake_metadata_store.append_audit_calls
        if ev.event_type == "call-context.invalid"
    ]
    assert len(rows) == 1
    _, event = rows[0]
    assert event.subject == {"reason": "unknown_kid"}
    # ``kid`` is public via JWKS and safe to attach for incident response.
    assert event.payload == {"reason": "unknown_kid", "kid": "deadbeefdeadbeef"}
    # The raw token must not be in the payload anywhere.
    assert token not in repr(event.payload)


def test_rpc_callctx_verify_bad_signature_emits_audit(
    client: TestClient,
    fake_metadata_store: FakeMetadataAdapter,
) -> None:
    signed = client.post(
        "/rpc/callctx.sign",
        json={
            "principal_id": "user-1",
            "workspace_id": WORKSPACE,
            "caller_component": "api-gateway",
        },
    ).json()
    parts = signed["token"].split(".")
    bad_sig = parts[2]
    bad_sig = ("B" if bad_sig[0] == "A" else "A") + bad_sig[1:]
    tampered = ".".join([parts[0], parts[1], bad_sig])
    # Clear audit rows from the sign step so we only see the verify-side row.
    fake_metadata_store.append_audit_calls.clear()
    client.post(
        "/rpc/callctx.verify",
        json={"token": tampered},
        headers=_callctx_header(),
    )
    rows = [
        (ws, ev)
        for ws, ev in fake_metadata_store.append_audit_calls
        if ev.event_type == "call-context.invalid"
    ]
    assert len(rows) == 1
    _, event = rows[0]
    assert event.subject == {"reason": "bad_signature"}
    # The tampered JWT must never enter the payload.
    assert tampered not in repr(event.payload)
    assert tampered not in repr(event.subject)


def test_rpc_callctx_verify_valid_token_does_not_emit_audit(
    client: TestClient,
    fake_metadata_store: FakeMetadataAdapter,
) -> None:
    signed = client.post(
        "/rpc/callctx.sign",
        json={
            "principal_id": "user-1",
            "workspace_id": WORKSPACE,
            "caller_component": "api-gateway",
        },
    ).json()
    fake_metadata_store.append_audit_calls.clear()
    resp = client.post(
        "/rpc/callctx.verify",
        json={"token": signed["token"]},
        headers=_callctx_header(),
    )
    assert resp.json()["valid"] is True
    rows = [
        ev
        for _, ev in fake_metadata_store.append_audit_calls
        if ev.event_type == "call-context.invalid"
    ]
    assert rows == []
