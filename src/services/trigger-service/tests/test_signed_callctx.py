"""Real signed-context and sidecar-boundary tests for production Trigger authorization."""

from __future__ import annotations

import base64
import time
from dataclasses import dataclass
from typing import Annotated, Any
from unittest.mock import AsyncMock

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from custos_callctx import CallContextVerifier, JwksCache
from custos_gateway.middleware.auth import AuthorizedCaller
from custos_gateway.middleware.auth import require_permission as gateway_permission
from custos_gateway.middleware.callctx_mint import MintedCallContext, mint_call_context
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from starlette.responses import JSONResponse

from custos_trigger.app import create_app
from custos_trigger.clients import FakeWorkflowServiceClient
from custos_trigger.dedup import Deduplicator
from custos_trigger.middleware import (
    CALLCTX_HEADER,
    TRIGGER_ADMIN_PERMISSION,
    CallContext,
    CallContextError,
    CallContextMiddleware,
    call_context_error_handler,
    require_permission,
)
from custos_trigger.pipeline.dispatch import Dispatcher
from custos_trigger.providers import InMemoryTriggerMetadataStore, Providers

TOKEN = "fixture-only-sidecar-token"
WORKSPACE = "ws-one"


@dataclass
class Signing:
    """Keep an ephemeral signing key and its real public-key verifier in the same fixture."""

    key: Ed25519PrivateKey
    verifier: CallContextVerifier

    def token(
        self,
        overrides: dict[str, object] | None = None,
        *,
        key: Ed25519PrivateKey | None = None,
    ) -> str:
        """Mint a realistic internal token, permitting individual negative claim variants."""
        now = int(time.time())
        claims: dict[str, Any] = {
            "iss": "custos-auth",
            "aud": "custos.internal",
            "iat": now,
            "exp": now + 300,
            "actingPrincipalId": "operator",
            "callerComponent": "api-gateway",
            "workspaceId": WORKSPACE,
            "permissions": [TRIGGER_ADMIN_PERMISSION],
            "jti": "test",
        }
        claims.update(overrides or {})
        return jwt.encode(claims, key or self.key, algorithm="EdDSA", headers={"kid": "test-key"})


@pytest.fixture
def signing() -> Signing:
    """Verify actual Ed25519 signatures without network requests or persisted private keys."""
    key = Ed25519PrivateKey.generate()
    public = base64.urlsafe_b64encode(key.public_key().public_bytes_raw()).decode().rstrip("=")

    async def fetch(url: str) -> tuple[dict[str, str], dict[str, Any]]:
        """Expose only the fixture's public key through the production JWKS cache."""
        assert url == "http://auth/.well-known/jwks.json"
        return {"cache-control": "max-age=300"}, {
            "keys": [{"kty": "OKP", "crv": "Ed25519", "kid": "test-key", "x": public}],
        }

    cache = JwksCache(jwks_url="http://auth/.well-known/jwks.json", fetcher=fetch)
    return Signing(key, CallContextVerifier(jwks_cache=cache))


def app_for(signing: Signing, *, token: str | None = TOKEN) -> FastAPI:
    """Mount representative protected routes under the actual production middleware."""
    app = FastAPI()
    app.state.entered = []
    app.add_middleware(
        CallContextMiddleware,
        authz_endpoint="http://auth",
        environment="production",
        verifier=signing.verifier,
        app_api_token=token,
        workflow_namespace="custos",
    )
    app.add_exception_handler(CallContextError, call_context_error_handler)

    @app.get("/v1/workspaces/{workspace_id}/admin")
    async def admin(
        workspace_id: str,
        ctx: Annotated[CallContext, Depends(require_permission(TRIGGER_ADMIN_PERMISSION))],
    ) -> dict[str, str]:
        """Record entry only after workspace-bound administration is authorized."""
        app.state.entered.append(workspace_id)
        return {"workspace": ctx.workspace_id, "principal": ctx.principal_id}

    @app.post("/v1/workspaces/{workspace_id}/subscription")
    async def subscription(
        workspace_id: str,
        _ctx: Annotated[CallContext, Depends(require_permission("trigger:subscriptions:write"))],
    ) -> dict[str, str]:
        """Keep subscription mutation independent of the administration permission."""
        app.state.entered.append(workspace_id)
        return {"status": "ok"}

    async def mesh() -> dict[str, str]:
        """Expose middleware disposition without provider or business-logic side effects."""
        app.state.entered.append("mesh")
        return {"status": "ok"}

    app.add_api_route("/RegisterResumeSubscription", mesh, methods=["POST"])
    app.add_api_route("/CancelResumeSubscription", mesh, methods=["POST"])
    app.add_api_route("/dapr/subscribe", mesh, methods=["GET"])
    app.add_api_route("/internal/events/workflow", mesh, methods=["POST"])
    app.add_api_route("/healthz", mesh, methods=["GET"])
    return app


def test_real_signature_grants_only_its_workspace_and_permission(signing: Signing) -> None:
    """Verify a real signature, then enforce tenancy and exact permission membership."""
    app = app_for(signing)
    with TestClient(app) as client:
        headers = {CALLCTX_HEADER: signing.token()}
        allowed = client.get(f"/v1/workspaces/{WORKSPACE}/admin", headers=headers)
        assert allowed.status_code == 200 and allowed.json()["principal"] == "operator"
        assert client.get("/v1/workspaces/other/admin", headers=headers).status_code == 403
        assert (
            client.post(
                f"/v1/workspaces/{WORKSPACE}/subscription",
                headers=headers,
            ).status_code
            == 403
        )
        assert (
            client.get(
                f"/v1/workspaces/{WORKSPACE}/admin",
                headers={
                    CALLCTX_HEADER: signing.token({"permissions": ["trigger:subscriptions:write"]})
                },
            ).status_code
            == 403
        )
    assert app.state.entered == [WORKSPACE]


@pytest.mark.parametrize(
    "overrides",
    [
        {"iss": "untrusted"},
        {"aud": "another-service"},
        {"exp": 1},
        {"iat": 4_000_000_000, "exp": 4_000_000_300},
        {"permissions": "trigger:admin"},
    ],
)
def test_invalid_signed_claims_fail_closed(
    signing: Signing,
    overrides: dict[str, object],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Invalid signed tokens never reach handlers or appear in logs."""
    app = app_for(signing)
    token = signing.token(overrides)
    with TestClient(app) as client:
        response = client.get(
            f"/v1/workspaces/{WORKSPACE}/admin",
            headers={CALLCTX_HEADER: token},
        )
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "callctx_invalid"
    assert not app.state.entered
    assert token not in caplog.text


@pytest.mark.parametrize("workspace", [None, "", " "])
def test_signed_global_or_blank_scope_cannot_administer_workspace(
    signing: Signing,
    workspace: str | None,
) -> None:
    """A global or empty claim is not an implicit grant over every workspace."""
    app = app_for(signing)
    with TestClient(app) as client:
        response = client.get(
            f"/v1/workspaces/{WORKSPACE}/admin",
            headers={CALLCTX_HEADER: signing.token({"workspaceId": workspace})},
        )
    assert response.status_code == 403 and not app.state.entered


def test_forged_signature_unsigned_header_and_duplicate_tokens_are_rejected(
    signing: Signing,
) -> None:
    """Neither an alternate signing key nor ambiguous headers can manufacture authority."""
    app = app_for(signing)
    with TestClient(app) as client:
        for token in (
            signing.token(key=Ed25519PrivateKey.generate()),
            '{"permissions":["trigger:admin"]}',
        ):
            response = client.get(
                f"/v1/workspaces/{WORKSPACE}/admin",
                headers={CALLCTX_HEADER: token},
            )
            assert response.status_code == 401
        duplicated = client.get(
            f"/v1/workspaces/{WORKSPACE}/admin",
            headers=[(CALLCTX_HEADER, signing.token()), (CALLCTX_HEADER, signing.token())],
        )
        assert duplicated.status_code == 401
    assert not app.state.entered


def test_jwks_outage_is_explicit_and_never_uses_unsigned_fallback(signing: Signing) -> None:
    """Unavailable verification is infrastructure failure, never development authentication."""

    async def unavailable(_url: str) -> tuple[dict[str, str], dict[str, Any]]:
        """Model an unavailable Auth Service key endpoint."""
        raise httpx.ConnectError("unavailable")

    signing.verifier = CallContextVerifier(
        jwks_cache=JwksCache(jwks_url="http://auth/.well-known/jwks.json", fetcher=unavailable),
    )
    app = app_for(signing)
    with TestClient(app) as client:
        result = client.get(
            f"/v1/workspaces/{WORKSPACE}/admin",
            headers={CALLCTX_HEADER: signing.token()},
        )
    assert result.status_code == 503
    assert result.json()["error"]["code"] == "callctx_unavailable"
    assert not app.state.entered


@pytest.mark.parametrize("path", ["/RegisterResumeSubscription", "/CancelResumeSubscription"])
@pytest.mark.parametrize(
    ("headers", "status"),
    [
        ({"dapr-caller-app-id": "workflow-service"}, 401),
        ({"dapr-api-token": "wrong", "dapr-caller-app-id": "workflow-service"}, 401),
        ({"dapr-api-token": TOKEN, "dapr-caller-app-id": "other-service"}, 403),
        ({"dapr-api-token": TOKEN, "dapr-caller-app-id": "workflow-service"}, 200),
    ],
)
def test_mesh_resume_requires_sidecar_and_workflow_identity(
    signing: Signing,
    path: str,
    headers: dict[str, str],
    status: int,
) -> None:
    """A forged caller header or a different authenticated service never reaches resume handlers."""
    app = app_for(signing)
    with TestClient(app) as client:
        result = client.post(
            path,
            headers={
                **headers,
                CALLCTX_HEADER: signing.token(),
                "dapr-caller-namespace": "custos",
            },
        )
    assert result.status_code == status
    assert app.state.entered == (["mesh"] if status == 200 else [])


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("GET", "/dapr/subscribe"),
        ("POST", "/internal/events/workflow"),
    ],
)
def test_sidecar_control_routes_require_app_token(
    signing: Signing,
    method: str,
    path: str,
) -> None:
    """Pub/sub and discovery bypass user claims but never bypass sidecar authentication."""
    app = app_for(signing)
    with TestClient(app) as client:
        assert client.request(method, path).status_code == 401
        assert client.request(method, path, headers={"dapr-api-token": TOKEN}).status_code == 200
        assert (
            client.request(
                method,
                path,
                headers={"dapr-api-token": TOKEN, "dapr-caller-app-id": "workflow-service"},
            ).status_code
            == 403
        )
    assert app.state.entered == ["mesh"]


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("GET", "/RegisterResumeSubscription"),
        ("POST", "/RegisterResumeSubscription/extra"),
        ("POST", "/dapr/subscribe"),
        ("POST", "/internal/events/workflow/extra"),
        ("POST", "/v1/webhooks/arbitrary"),
        ("GET", "/healthz/extra"),
    ],
)
def test_mesh_and_webhook_exemptions_are_not_prefixes(
    signing: Signing,
    method: str,
    path: str,
) -> None:
    """Only exact authenticated method/path pairs can avoid signed user-context verification."""
    app = app_for(signing)
    with TestClient(app) as client:
        result = client.request(
            method,
            path,
            headers={"dapr-api-token": TOKEN, "dapr-caller-app-id": "workflow-service"},
        )
    assert result.status_code == 401 and not app.state.entered


def test_missing_mesh_configuration_fails_closed_but_probe_remains_public(signing: Signing) -> None:
    """A configured signed verifier does not implicitly grant unauthenticated mesh access."""
    app = app_for(signing, token=None)
    with TestClient(app) as client:
        assert (
            client.post(
                "/RegisterResumeSubscription",
                headers={"dapr-caller-app-id": "workflow-service"},
            ).status_code
            == 503
        )
        assert client.get("/dapr/subscribe").status_code == 503
        assert client.get("/healthz").status_code == 200
    assert app.state.entered == ["mesh"]


def test_duplicate_mesh_credentials_never_select_a_trusted_value(signing: Signing) -> None:
    """Reject ambiguous token and caller headers even if one of their values is valid."""
    app = app_for(signing)
    with TestClient(app) as client:
        token = client.post(
            "/RegisterResumeSubscription",
            headers=[
                ("dapr-api-token", TOKEN),
                ("dapr-api-token", "wrong"),
                ("dapr-caller-app-id", "workflow-service"),
            ],
        )
        caller = client.post(
            "/RegisterResumeSubscription",
            headers=[
                ("dapr-api-token", TOKEN),
                ("dapr-caller-app-id", "workflow-service"),
                ("dapr-caller-app-id", "other-service"),
                ("dapr-caller-namespace", "custos"),
            ],
        )
    assert token.status_code == 401 and caller.status_code == 403
    assert not app.state.entered


@pytest.mark.parametrize("namespaces", [[], ["other"], ["custos", "other"]])
def test_workflow_namespace_is_required_and_unambiguous(
    signing: Signing,
    namespaces: list[str],
) -> None:
    """A same-name app in another namespace cannot inherit Workflow authority."""
    app = app_for(signing)
    with TestClient(app) as client:
        result = client.post(
            "/RegisterResumeSubscription",
            headers=[
                ("dapr-api-token", TOKEN),
                ("dapr-caller-app-id", "workflow-service"),
                *(("dapr-caller-namespace", namespace) for namespace in namespaces),
            ],
        )
    assert result.status_code == 403 and not app.state.entered


def test_factory_wires_real_verifiers_before_scoped_resume_reads(
    signing: Signing,
    metadata_store: InMemoryTriggerMetadataStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Production composition authenticates the sidecar before any tenant resume lookup."""
    monkeypatch.setenv("APP_API_TOKEN", TOKEN)
    monkeypatch.setenv("TRIGGER_WF_ENDPOINT", "http://workflow-service:8080")
    monkeypatch.setenv("TRIGGER_WORKFLOW_APP_ID", "workflow-service")
    monkeypatch.setenv("TRIGGER_WORKFLOW_NAMESPACE", "custos")
    reads = AsyncMock(wraps=metadata_store.get_resume_subscription)
    monkeypatch.setattr(metadata_store, "get_resume_subscription", reads)
    app = create_app(
        authz_endpoint="http://auth",
        environment="production",
        call_context_verifier=signing.verifier,
        providers=Providers(metadata_store=metadata_store),
        dispatcher=Dispatcher(FakeWorkflowServiceClient(), Deduplicator(metadata_store)),
        receiver_configuration_enabled=True,
    )
    body = {
        "workspaceId": WORKSPACE,
        "runId": "run",
        "stepId": "wait",
        "eventKey": "ready",
        "deliveryProtocol": 2,
    }
    with TestClient(app) as client:
        denied = client.post(
            "/RegisterResumeSubscription",
            json=body,
            headers={"dapr-caller-app-id": "workflow-service", CALLCTX_HEADER: signing.token()},
        )
        assert denied.status_code == 401
        reads.assert_not_awaited()
        registered = client.post(
            "/RegisterResumeSubscription",
            json=body,
            headers={
                "dapr-api-token": TOKEN,
                "dapr-caller-app-id": "workflow-service",
                "dapr-caller-namespace": "custos",
            },
        )
        assert registered.status_code == 200 and reads.await_count > 0
        headers = {CALLCTX_HEADER: signing.token({"permissions": ["trigger:subscriptions:read"]})}
        assert (
            client.get(
                f"/v1/workspaces/{WORKSPACE}/triggers/missing",
                headers=headers,
            ).status_code
            == 404
        )
        assert (
            client.get(
                "/v1/workspaces/other/triggers/missing",
                headers=headers,
            ).status_code
            == 403
        )


async def test_gateway_minting_authorized_permission_reaches_trigger(signing: Signing) -> None:
    """Run Gateway authorization/minting into actual Trigger signature and permission checks."""
    from custos_gateway.clients.auth import (
        CallctxSignRequest,
        CallctxSignResponse,
        FakeAuthServiceClient,
        VerifyAndAuthorizeResponse,
    )
    from custos_gateway.middleware.auth import (
        AUTH_CLIENT_STATE_ATTR,
    )
    from custos_gateway.middleware.correlation import CorrelationIdMiddleware

    class SigningAuth(FakeAuthServiceClient):
        """Sign exactly the real Gateway request, not a prepopulated permission fixture."""

        async def callctx_sign(self, request: CallctxSignRequest) -> CallctxSignResponse:
            """Record and sign the permission selected by Gateway authorization."""
            self.sign_calls.append(request)
            now = int(time.time())
            token = signing.token(
                {
                    "actingPrincipalId": request.principal_id,
                    "workspaceId": request.workspace_id,
                    "callerComponent": request.caller_component,
                    "permissions": request.permissions,
                }
            )
            return CallctxSignResponse(
                token=token, kid="test-key", jti="test", iat=now, exp=now + 300
            )

    auth = SigningAuth(
        decision=VerifyAndAuthorizeResponse(
            principal_id="operator",
            allowed=True,
            reason="allow",
            audit_event_id="audit",
        )
    )
    trigger = app_for(signing)
    gateway = FastAPI()
    gateway.add_middleware(CorrelationIdMiddleware)
    setattr(gateway.state, AUTH_CLIENT_STATE_ATTR, auth)

    @gateway.get("/v1/workspaces/{workspaceId}/admin")
    async def proxy(
        _caller: Annotated[AuthorizedCaller, Depends(gateway_permission(TRIGGER_ADMIN_PERMISSION))],
        minted: Annotated[MintedCallContext, Depends(mint_call_context)],
    ) -> JSONResponse:
        """Forward freshly minted metadata into the downstream production verifier."""
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=trigger),
            base_url="http://trigger",
        ) as downstream:
            response = await downstream.get(
                f"/v1/workspaces/{WORKSPACE}/admin",
                headers=dict(minted.metadata),
            )
        return JSONResponse(response.json(), status_code=response.status_code)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=gateway),
        base_url="http://gateway",
    ) as client:
        response = await client.get(
            f"/v1/workspaces/{WORKSPACE}/admin",
            headers={"Authorization": "Bearer fixture-only-token", CALLCTX_HEADER: "forged"},
        )
    assert response.status_code == 200, response.text
    assert trigger.state.entered == [WORKSPACE]
    assert len(auth.sign_calls) == 1
    assert auth.sign_calls[0].permissions == [TRIGGER_ADMIN_PERMISSION]


def test_dispatch_factory_prefers_explicit_app_id_over_http_address(
    metadata_store: InMemoryTriggerMetadataStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The shipped chart's legacy HTTP address cannot become a Dapr invocation target."""
    from collections.abc import Mapping

    import custos_trigger.clients as clients
    from custos_trigger.clients.workflow import DaprEndpoint

    original = clients.read_dapr_endpoint
    targets: list[str] = []

    def capture(env: Mapping[str, str], *, app_id: str) -> DaprEndpoint:
        """Observe the actual composition input while retaining normal endpoint validation."""
        targets.append(app_id)
        return original(env, app_id=app_id)

    monkeypatch.setattr(clients, "read_dapr_endpoint", capture)
    monkeypatch.setenv("TRIGGER_WF_ENDPOINT", "http://workflow-service:8080")
    monkeypatch.setenv("TRIGGER_WORKFLOW_APP_ID", "workflow-service")
    with TestClient(
        create_app(authz_endpoint="", providers=Providers(metadata_store=metadata_store))
    ):
        pass
    assert targets == ["workflow-service"]
