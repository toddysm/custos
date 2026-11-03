"""Verify production Connector contexts with real signatures and explicit failure boundaries."""

from __future__ import annotations

import base64
import time
from dataclasses import dataclass, replace
from typing import Annotated, Any

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from custos_callctx import CallContextVerifier, JwksCache
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from starlette.responses import JSONResponse

from custos_connector import create_app
from custos_connector.api._common import workspace_mismatch_response
from custos_connector.middleware import (
    CALLCTX_HEADER,
    CallContext,
    CallContextError,
    CallContextMiddleware,
    call_context_error_handler,
    get_call_context,
    require_permission,
)
from custos_connector.permissions import ADMIN_CONNECTOR
from tests.test_app import _BASE_SETTINGS, _providers


@dataclass
class Signing:
    """Keep only an ephemeral private key and its actual public-key verifier."""

    key: Ed25519PrivateKey
    verifier: CallContextVerifier

    def token(self, overrides: dict[str, object] | None = None, *, forged: bool = False) -> str:
        """Mint one realistic token, allowing individual invalid claim/signature cases."""
        now = int(time.time())
        claims: dict[str, Any] = {
            "iss": "custos-auth",
            "aud": "custos.internal",
            "iat": now,
            "exp": now + 300,
            "actingPrincipalId": "operator",
            "callerComponent": "api-gateway",
            "workspaceId": "workspace",
            "permissions": [ADMIN_CONNECTOR],
            "jti": "test",
        }
        claims.update(overrides or {})
        return jwt.encode(
            claims,
            Ed25519PrivateKey.generate() if forged else self.key,
            algorithm="EdDSA",
            headers={"kid": "test"},
        )


@pytest.fixture
def signing() -> Signing:
    """Use the production JWKS cache without network requests or persisted private material."""
    key = Ed25519PrivateKey.generate()
    public = base64.urlsafe_b64encode(key.public_key().public_bytes_raw()).decode().rstrip("=")

    async def fetch(url: str) -> tuple[dict[str, str], dict[str, Any]]:
        """Expose only the fixture's public key."""
        assert url == "http://auth/.well-known/jwks.json"
        return {"cache-control": "max-age=300"}, {
            "keys": [{"kty": "OKP", "crv": "Ed25519", "kid": "test", "x": public}],
        }

    return Signing(
        key,
        CallContextVerifier(
            jwks_cache=JwksCache(jwks_url="http://auth/.well-known/jwks.json", fetcher=fetch),
        ),
    )


def protected_app(verifier: CallContextVerifier) -> FastAPI:
    """Exercise existing permission and workspace guards after real signed verification."""
    app = FastAPI()
    app.state.entered = 0
    app.add_middleware(
        CallContextMiddleware,
        authz_endpoint="http://auth",
        environment="production",
        verifier=verifier,
    )
    app.add_exception_handler(CallContextError, call_context_error_handler)

    @app.get("/v1/workspaces/{workspace_id}/admin")
    async def admin(
        workspace_id: str,
        ctx: Annotated[CallContext, Depends(require_permission(ADMIN_CONNECTOR))],
    ) -> JSONResponse:
        """Record entry only after both authorization boundaries pass."""
        mismatch = workspace_mismatch_response(ctx, workspace_id)
        if mismatch is not None:
            return mismatch
        app.state.entered += 1
        return JSONResponse({"principal": ctx.principal_id, "workspace": ctx.workspace_id})

    return app


def test_real_signature_preserves_scope_and_permissions(signing: Signing) -> None:
    """A trusted signature cannot grant another workspace or an absent permission."""
    app = protected_app(signing.verifier)
    with TestClient(app) as client:
        headers = {CALLCTX_HEADER: signing.token()}
        allowed = client.get("/v1/workspaces/workspace/admin", headers=headers)
        assert allowed.status_code == 200 and allowed.json()["principal"] == "operator"
        assert client.get("/v1/workspaces/other/admin", headers=headers).status_code == 403
        assert (
            client.get(
                "/v1/workspaces/workspace/admin",
                headers={CALLCTX_HEADER: signing.token({"permissions": []})},
            ).status_code
            == 403
        )
    assert app.state.entered == 1


@pytest.mark.parametrize(
    "overrides",
    [
        {"iss": "other"},
        {"aud": "other"},
        {"exp": 1},
        {"iat": 4_000_000_000, "exp": 4_000_000_300},
        {"permissions": "admin"},
    ],
)
def test_invalid_claims_do_not_enter_or_leak_tokens(
    signing: Signing,
    overrides: dict[str, object],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Reject invalid signed claims without disclosing their token in errors or logs."""
    app = protected_app(signing.verifier)
    token = signing.token(overrides)
    with TestClient(app) as client:
        response = client.get("/v1/workspaces/workspace/admin", headers={CALLCTX_HEADER: token})
    assert response.status_code == 401 and response.json()["error"]["code"] == "callctx_invalid"
    assert not app.state.entered and token not in caplog.text and token not in response.text


@pytest.mark.parametrize("workspace", [None, "", " "])
def test_signed_context_requires_workspace(signing: Signing, workspace: str | None) -> None:
    """Platform-global or blank context is not authority over a tenant."""
    app = protected_app(signing.verifier)
    with TestClient(app) as client:
        response = client.get(
            "/v1/workspaces/workspace/admin",
            headers={CALLCTX_HEADER: signing.token({"workspaceId": workspace})},
        )
    assert response.status_code == 403 and not app.state.entered


def test_unsigned_forged_and_duplicate_contexts_fail_closed(signing: Signing) -> None:
    """Neither an unsigned shape, another key nor ambiguous headers can manufacture identity."""
    app = protected_app(signing.verifier)
    with TestClient(app) as client:
        for token in ('{"workspace_id":"workspace"}', signing.token(forged=True)):
            assert (
                client.get(
                    "/v1/workspaces/workspace/admin",
                    headers={CALLCTX_HEADER: token},
                ).status_code
                == 401
            )
        token = signing.token()
        assert (
            client.get(
                "/v1/workspaces/workspace/admin",
                headers=[(CALLCTX_HEADER, token), (CALLCTX_HEADER, token)],
            ).status_code
            == 401
        )
    assert not app.state.entered


def test_key_service_outage_never_falls_back_to_development(signing: Signing) -> None:
    """An unavailable public-key source is a controlled503, not invalid-user success or a500."""

    async def unavailable(url: str) -> tuple[dict[str, str], dict[str, Any]]:
        """Model a key-source transport outage without returning an empty trusted key set."""
        raise OSError("private backend details")

    verifier = CallContextVerifier(
        jwks_cache=JwksCache(
            jwks_url="http://auth/.well-known/jwks.json",
            fetcher=unavailable,
        )
    )
    app = protected_app(verifier)
    with TestClient(app) as client:
        response = client.get(
            "/v1/workspaces/workspace/admin",
            headers={CALLCTX_HEADER: signing.token()},
        )
    assert response.status_code == 503 and not app.state.entered
    assert "private backend details" not in response.text


def test_factory_injects_signed_verifier_without_network(signing: Signing) -> None:
    """The actual application factory passes the verifier through its production middleware."""
    app = create_app(
        settings=replace(_BASE_SETTINGS, authz_endpoint="http://auth", environment="production"),
        providers=_providers(),
        call_context_verifier=signing.verifier,
    )

    @app.get("/signed-probe")
    async def probe(ctx: Annotated[CallContext, Depends(get_call_context)]) -> dict[str, str]:
        """Return only the verified caller's identity through the assembled application."""
        return {"principal": ctx.principal_id}

    with TestClient(app) as client:
        response = client.get("/signed-probe", headers={CALLCTX_HEADER: signing.token()})
    assert response.status_code == 200 and response.json() == {"principal": "operator"}
