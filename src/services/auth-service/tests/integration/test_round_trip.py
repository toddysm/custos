"""Integration round-trip tests for auth-service (AS-IMPL-028).

Each test exercises a complete end-to-end contract against a live
Postgres so the FastAPI + SPL adapter + RBAC + signer wiring is
verified against real DDL, real connection pools, and real
cross-component event delivery (the in-process
``LocalBindingChangedBus`` / ``LocalTokenRevokedBus``).

The five scenarios match the AS-IMPL-028 issue (#263) scope:

1. ``mint → verify``                      — :func:`test_mint_then_verify_round_trip`
2. ``grant → authorize → revoke``         — :func:`test_grant_authorize_revoke_round_trip`
3. ``sign-callctx → verify-callctx``      — :func:`test_callctx_sign_then_verify_round_trip`
4. ``JWKS rotation overlap``              — :func:`test_jwks_rotation_overlap`
5. ``token-revoked cache eviction``       — :func:`test_token_revoke_evicts_authn_cache`
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from custos_auth.callctx_keyring import KeyRing
from custos_auth.callctx_signer import SigningKey, StaticSigningKeyResolver
from tests.integration.conftest import (
    callctx_header,
    platform_admin_header,
    workspace_admin_header,
)

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Setup helpers
# ---------------------------------------------------------------------------


def _seed_tenant_workspace_sa(
    client: TestClient,
    *,
    tenant_id: str = "t-int",
    workspace_id: str = "ws-int",
    sa_id: str = "sa-int",
) -> None:
    """Create a tenant, a workspace under it, and a service account in it.

    Returns nothing — the caller addresses the resources by the same
    IDs it passed in. Asserts each step so a regression in one of
    these prerequisites surfaces with a clear failure point rather
    than as a cryptic 4xx three steps later.
    """
    resp = client.post(
        "/v1/tenants",
        headers=platform_admin_header(),
        json={"tenant_id": tenant_id, "display_name": f"Tenant {tenant_id}"},
    )
    assert resp.status_code == 201, resp.text

    resp = client.post(
        f"/v1/tenants/{tenant_id}/workspaces",
        headers=platform_admin_header(),
        json={"workspace_id": workspace_id, "display_name": f"Workspace {workspace_id}"},
    )
    assert resp.status_code == 201, resp.text

    resp = client.post(
        "/v1/service-accounts",
        headers=workspace_admin_header(workspace_id),
        json={"principal_id": sa_id, "display_name": f"SA {sa_id}"},
    )
    assert resp.status_code == 201, resp.text


def _mint_token(
    client: TestClient,
    *,
    workspace_id: str,
    sa_id: str,
    ttl_seconds: int = 3600,
) -> dict[str, object]:
    """POST a fresh token for ``sa_id`` and return the parsed mint envelope."""
    resp = client.post(
        f"/v1/service-accounts/{sa_id}/tokens",
        headers=workspace_admin_header(workspace_id),
        json={"ttl_seconds": ttl_seconds},
    )
    assert resp.status_code == 201, resp.text
    body: dict[str, object] = resp.json()
    assert isinstance(body["token"], str) and body["token"]
    assert isinstance(body["token_id"], str) and body["token_id"]
    return body


# ---------------------------------------------------------------------------
# 1. mint → verify
# ---------------------------------------------------------------------------


def test_mint_then_verify_round_trip(client: TestClient) -> None:
    """Mint a service-token via REST and verify it via REST + RPC.

    Both surfaces must project the same SA principal envelope —
    differences are a regression in the verify hot path. The RPC
    surface returns ``200`` with the principal nested under
    ``principal``; the REST surface returns ``200`` with the
    principal as the response root. Both are exercised so the
    catalog-service / workflow-service / activity-runtime-manager
    callers (which use the RPC) and the API gateway (which uses
    the REST verify) stay in sync.
    """
    _seed_tenant_workspace_sa(client)

    mint = _mint_token(client, workspace_id="ws-int", sa_id="sa-int")
    plaintext = mint["token"]

    # REST verify --------------------------------------------------------
    rest = client.post("/v1/auth/verify", json={"token": plaintext})
    assert rest.status_code == 200, rest.text
    rest_body = rest.json()
    assert rest_body["kind"] == "serviceAccount"
    assert rest_body["principal_id"] == "sa-int"
    assert rest_body["workspace_id"] == "ws-int"

    # RPC verify ---------------------------------------------------------
    rpc = client.post(
        "/rpc/authn.verifyToken",
        json={"token": plaintext},
    )
    assert rpc.status_code == 200, rpc.text
    rpc_body = rpc.json()
    assert rpc_body["principal"] is not None
    assert rpc_body["principal"]["principal_id"] == "sa-int"
    assert rpc_body["principal"]["workspace_id"] == "ws-int"

    # An unknown token must fail closed across both surfaces.
    rest_bad = client.post("/v1/auth/verify", json={"token": "not-a-real-token"})
    assert rest_bad.status_code == 401
    assert rest_bad.json()["error"]["code"] == "unauthenticated"

    rpc_bad = client.post("/rpc/authn.verifyToken", json={"token": "not-a-real-token"})
    assert rpc_bad.status_code == 200
    assert rpc_bad.json()["principal"] is None


# ---------------------------------------------------------------------------
# 2. grant → authorize → revoke
# ---------------------------------------------------------------------------


def test_grant_authorize_revoke_round_trip(client: TestClient) -> None:
    """End-to-end authorization lifecycle:

    1. Seed tenant + workspace + SA.
    2. Bind ``role:workspace.viewer`` to the SA at workspace scope.
    3. ``rpc/authz.authorize`` for one of the role's permissions
       (``catalog:workflows:read``) returns ``allowed=True``.
    4. Revoke the binding.
    5. The same RPC now returns ``allowed=False`` — the in-process
       :class:`LocalBindingChangedBus` must have evicted the cache
       entry written in step 3, otherwise the post-revoke check
       would erroneously return ``True`` until the TTL elapsed.

    This is the design's "revoke-then-recheck within one round
    trip" acceptance criterion.
    """
    _seed_tenant_workspace_sa(client)

    # Step 2 — bind the role.
    grant = client.post(
        "/v1/workspaces/ws-int/role-bindings",
        headers=callctx_header(
            tenant_id="t-int",
            workspace_id="ws-int",
            permissions=["admin:role-binding"],
        ),
        json={"principal_id": "sa-int", "role_id": "role:workspace.viewer"},
    )
    assert grant.status_code == 201, grant.text
    binding_id = grant.json()["binding_id"]

    # Step 3 — authorize returns True.
    allowed_body = {
        "principal_id": "sa-int",
        "permission": "catalog:workflows:read",
        "workspace_id": "ws-int",
        "caller_component": "integration-test",
    }
    allowed_ctx = callctx_header(workspace_id="ws-int")
    resp_allow = client.post(
        "/rpc/authz.authorize",
        json=allowed_body,
        headers=allowed_ctx,
    )
    assert resp_allow.status_code == 200, resp_allow.text
    decision = resp_allow.json()
    assert decision["allowed"] is True
    assert decision["audit_event_id"]
    # A repeated call exercises the cache hit path; outcome must be
    # identical (decision cache is per-principal/workspace/permission).
    resp_allow_cached = client.post(
        "/rpc/authz.authorize",
        json=allowed_body,
        headers=allowed_ctx,
    )
    assert resp_allow_cached.status_code == 200
    assert resp_allow_cached.json()["allowed"] is True

    # Step 4 — revoke the binding.
    revoke = client.request(
        "DELETE",
        f"/v1/workspaces/ws-int/role-bindings/{binding_id}",
        headers=callctx_header(
            tenant_id="t-int",
            workspace_id="ws-int",
            permissions=["admin:role-binding"],
        ),
    )
    assert revoke.status_code == 204, revoke.text

    # Step 5 — same RPC now returns False (cache was invalidated by the
    # in-process binding-changed bus during the DELETE).
    resp_deny = client.post(
        "/rpc/authz.authorize",
        json=allowed_body,
        headers=allowed_ctx,
    )
    assert resp_deny.status_code == 200, resp_deny.text
    assert resp_deny.json()["allowed"] is False


# ---------------------------------------------------------------------------
# 3. sign-callctx → verify-callctx
# ---------------------------------------------------------------------------


def test_callctx_sign_then_verify_round_trip(client: TestClient) -> None:
    """Mint a signed call-context via ``rpc/callctx.sign`` and round-trip
    it through ``rpc/callctx.verify`` — every claim from the mint
    must reappear in the verification verdict.
    """
    sign_resp = client.post(
        "/rpc/callctx.sign",
        json={
            "principal_id": "user-42",
            "workspace_id": "ws-int",
            "caller_component": "api-gateway",
            "ttl_seconds": 120,
        },
    )
    assert sign_resp.status_code == 200, sign_resp.text
    minted = sign_resp.json()
    assert isinstance(minted["token"], str) and minted["token"]
    assert isinstance(minted["kid"], str) and len(minted["kid"]) == 16
    assert minted["exp"] - minted["iat"] == 120

    # Verify echoes every signed claim.
    verify_resp = client.post(
        "/rpc/callctx.verify",
        json={"token": minted["token"]},
        # callctx.verify is not in the bypass list, so the call needs
        # *a* call-context to clear the middleware. The verify route
        # itself does not gate on permissions.
        headers=callctx_header(workspace_id="ws-int"),
    )
    assert verify_resp.status_code == 200, verify_resp.text
    verdict = verify_resp.json()
    assert verdict["valid"] is True
    assert verdict["reason"] == ""
    assert verdict["acting_principal_id"] == "user-42"
    assert verdict["workspace_id"] == "ws-int"
    assert verdict["caller_component"] == "api-gateway"
    assert verdict["kid"] == minted["kid"]
    assert verdict["jti"] == minted["jti"]
    assert verdict["iat"] == minted["iat"]
    assert verdict["exp"] == minted["exp"]


def test_callctx_sign_with_permissions_and_audience_round_trip(
    client: TestClient,
) -> None:
    """AS-IMPL-030 fat call-context: a mint that embeds permissions
    and overrides the audience must round-trip every new field
    through ``rpc/callctx.verify``, including the per-component
    audience-override-only-on-explicit-request behaviour.
    """
    sign_resp = client.post(
        "/rpc/callctx.sign",
        json={
            "principal_id": "user-42",
            "workspace_id": "ws-int",
            "caller_component": "api-gateway",
            "permissions": [
                "catalog:workflows:read",
                "catalog:workflows:write",
            ],
            "audience": "custos.catalog",
        },
    )
    assert sign_resp.status_code == 200, sign_resp.text
    minted = sign_resp.json()

    # The default-audience verify rejects this token: the gateway-
    # minted JWT is targeted at custos.catalog, not custos.internal.
    rejected = client.post(
        "/rpc/callctx.verify",
        json={"token": minted["token"]},
        headers=callctx_header(workspace_id="ws-int"),
    ).json()
    assert rejected["valid"] is False
    assert rejected["reason"] == "wrong_audience"

    # Passing the per-component audience override makes it round-trip.
    verify_resp = client.post(
        "/rpc/callctx.verify",
        json={"token": minted["token"], "audience": "custos.catalog"},
        headers=callctx_header(workspace_id="ws-int"),
    )
    assert verify_resp.status_code == 200, verify_resp.text
    verdict = verify_resp.json()
    assert verdict["valid"] is True
    assert verdict["acting_principal_id"] == "user-42"
    assert verdict["permissions"] == [
        "catalog:workflows:read",
        "catalog:workflows:write",
    ]


# ---------------------------------------------------------------------------
# 4. JWKS rotation overlap
# ---------------------------------------------------------------------------


def test_jwks_rotation_overlap(client: TestClient) -> None:
    """Rotate the call-context signing key and assert that:

    * the JWKS endpoint now advertises both the new active key and
      the previous one (retired but still within the overlap window),
    * a call-context minted under the *old* key still verifies
      (``rpc/callctx.verify`` returns ``valid=True``) because the
      verifier resolves ``kid`` against the full ring,
    * a call-context minted *after* rotation uses the new ``kid``.

    The rotation is driven directly through the lifespan-owned
    :class:`KeyRing` rather than through the in-process rotation loop
    (which the integration env intentionally disables — see
    :func:`_integration_env`) so the assertion has no wall-clock
    flake.
    """
    ring: KeyRing = client.app.state.call_context_key_ring  # type: ignore[attr-defined]
    initial_kid = ring.active.kid

    # Mint a call-context under the initial key.
    before = client.post(
        "/rpc/callctx.sign",
        json={
            "principal_id": "user-1",
            "workspace_id": "ws-int",
            "caller_component": "api-gateway",
        },
    )
    assert before.status_code == 200
    token_before = before.json()["token"]
    kid_before = before.json()["kid"]
    assert kid_before == initial_kid

    # JWKS pre-rotation: a single key advertised.
    jwks_before = client.get("/.well-known/jwks.json")
    assert jwks_before.status_code == 200
    kids_before = {entry["kid"] for entry in jwks_before.json()["keys"]}
    assert kids_before == {initial_kid}

    # Rotate the ring directly. The signer reads via the
    # KeyRingObservingResolver, but the lifespan also holds the
    # underlying StaticSigningKeyResolver so we have to push the
    # new key there too — otherwise subsequent mints would still
    # use the old key (resolver is the source of truth).
    new_key = SigningKey.generate()
    resolver = client.app.state.call_context_signing_key_resolver  # type: ignore[attr-defined]
    # The dev-mode resolver is a StaticSigningKeyResolver — the only
    # one whose `set_key` is part of its public API.
    assert isinstance(resolver, StaticSigningKeyResolver)
    resolver.set_key(new_key)
    ring.rotate(new_key)

    # JWKS post-rotation: both kids advertised, active first.
    jwks_after = client.get("/.well-known/jwks.json")
    assert jwks_after.status_code == 200
    entries_after = jwks_after.json()["keys"]
    kids_after = [entry["kid"] for entry in entries_after]
    assert kids_after == [new_key.kid, initial_kid]

    # Old token still verifies via the retired entry.
    verify_old = client.post(
        "/rpc/callctx.verify",
        json={"token": token_before},
        headers=callctx_header(workspace_id="ws-int"),
    )
    assert verify_old.status_code == 200
    assert verify_old.json()["valid"] is True
    assert verify_old.json()["kid"] == initial_kid

    # New mint uses the new kid.
    after = client.post(
        "/rpc/callctx.sign",
        json={
            "principal_id": "user-1",
            "workspace_id": "ws-int",
            "caller_component": "api-gateway",
        },
    )
    assert after.status_code == 200
    assert after.json()["kid"] == new_key.kid


# ---------------------------------------------------------------------------
# 5. token-revoked cache eviction
# ---------------------------------------------------------------------------


def test_token_revoke_evicts_authn_cache(client: TestClient) -> None:
    """Verify a token, revoke it, re-verify, and assert the second
    verify returns 401 / ``principal=None`` immediately — i.e. the
    in-process :class:`LocalTokenRevokedBus` evicted the warmed
    authn-cache entry rather than serving the stale ``principal``
    until the TTL expired.
    """
    _seed_tenant_workspace_sa(client)

    mint = _mint_token(client, workspace_id="ws-int", sa_id="sa-int")
    plaintext = mint["token"]
    token_id = mint["token_id"]

    # Warm the authn cache.
    first = client.post("/v1/auth/verify", json={"token": plaintext})
    assert first.status_code == 200
    assert first.json()["principal_id"] == "sa-int"
    # Repeated verify must still return 200 (cache hit).
    second = client.post("/v1/auth/verify", json={"token": plaintext})
    assert second.status_code == 200

    # Revoke the token. The DELETE handler publishes a
    # TokenRevokedEvent onto the LocalTokenRevokedBus which the
    # lifespan subscribed the authn cache to.
    revoke = client.request(
        "DELETE",
        f"/v1/tokens/{token_id}",
        headers=workspace_admin_header("ws-int"),
        json={"reason": "integration-test revoke"},
    )
    assert revoke.status_code == 204, revoke.text

    # Re-verify: the cache must no longer hold the stale principal.
    after_rest = client.post("/v1/auth/verify", json={"token": plaintext})
    assert after_rest.status_code == 401
    assert after_rest.json()["error"]["code"] == "unauthenticated"

    after_rpc = client.post("/rpc/authn.verifyToken", json={"token": plaintext})
    assert after_rpc.status_code == 200
    assert after_rpc.json()["principal"] is None
