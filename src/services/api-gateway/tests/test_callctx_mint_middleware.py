"""Tests for the Call-Context Minter dependency (AGW-IMPL-007)."""

from __future__ import annotations

from custos_callctx import CALLCTX_HEADER
from fastapi import Depends, FastAPI, Request
from fastapi.testclient import TestClient

from custos_gateway.clients.auth import (
    AuthServiceClientTransportError,
    CallctxSignRequest,
    CallctxSignResponse,
    FakeAuthServiceClient,
    VerifyAndAuthorizeResponse,
)
from custos_gateway.errors import CORRELATION_ID_HEADER, register_exception_handlers
from custos_gateway.middleware.auth import (
    AUTH_CLIENT_STATE_ATTR,
    AuthorizedCaller,
    require_permission,
)
from custos_gateway.middleware.callctx_mint import (
    CALL_CONTEXT_STATE_ATTR,
    CALLER_COMPONENT,
    OUTBOUND_METADATA_STATE_ATTR,
    MintedCallContext,
    mint_call_context,
)
from custos_gateway.middleware.correlation import CorrelationIdMiddleware

#: Authorization header presented on authenticated requests.
_BEARER: dict[str, str] = {"Authorization": "Bearer tok"}


def _build_app(client: FakeAuthServiceClient) -> FastAPI:
    app = FastAPI()
    app.add_middleware(CorrelationIdMiddleware)
    register_exception_handlers(app)
    setattr(app.state, AUTH_CLIENT_STATE_ATTR, client)

    @app.post("/v1/workspaces/{workspaceId}/things")
    async def scoped(
        request: Request,
        _caller: AuthorizedCaller = Depends(require_permission("things:write")),
        minted: MintedCallContext = Depends(mint_call_context),
    ) -> dict[str, object]:
        return {
            "token": minted.token,
            "correlation_id": minted.correlation_id,
            "metadata": dict(minted.metadata),
            "state_is_minted": getattr(request.state, CALL_CONTEXT_STATE_ATTR) is minted,
            "outbound": getattr(request.state, OUTBOUND_METADATA_STATE_ATTR),
        }

    # A bypass-style route: no auth, no minter.
    @app.post("/v1/webhooks/{connectorInstanceId}")
    async def webhook() -> dict[str, bool]:
        return {"ok": True}

    return app


def _allow(principal: str = "sa_1", workspace: str = "ws_42") -> FakeAuthServiceClient:
    return FakeAuthServiceClient(
        decision=VerifyAndAuthorizeResponse(
            principal_id=principal,
            allowed=True,
            reason="allow",
            audit_event_id="evt_1",
        )
    )


def test_authenticated_request_mints_one_context() -> None:
    fake = _allow()
    client = TestClient(_build_app(fake))
    resp = client.post("/v1/workspaces/ws_42/things", json={"name": "thing"}, headers=_BEARER)
    assert resp.status_code == 200
    body = resp.json()
    # Exactly one sign call for the authorized principal + workspace.
    assert len(fake.sign_calls) == 1
    sign = fake.sign_calls[0]
    assert sign.principal_id == "sa_1"
    assert sign.workspace_id == "ws_42"
    assert sign.caller_component == CALLER_COMPONENT
    assert body["token"] == "token-fake"
    assert body["state_is_minted"] is True


def test_outbound_metadata_carries_callctx_and_correlation() -> None:
    fake = FakeAuthServiceClient(
        decision=VerifyAndAuthorizeResponse(
            principal_id="sa_1", allowed=True, reason="allow", audit_event_id="evt_1"
        ),
        signed=CallctxSignResponse(token="signed.jwt", kid="k1", jti="j1", iat=1, exp=2),
    )
    client = TestClient(_build_app(fake))
    resp = client.post(
        "/v1/workspaces/ws_42/things",
        json={"name": "thing"},
        headers={**_BEARER, CORRELATION_ID_HEADER: "corr-123"},
    )
    assert resp.status_code == 200
    body = resp.json()
    metadata = body["metadata"]
    assert metadata[CALLCTX_HEADER] == "signed.jwt"
    assert metadata[CORRELATION_ID_HEADER] == "corr-123"
    assert body["correlation_id"] == "corr-123"
    assert body["outbound"] == metadata


def test_minted_correlation_id_propagates_when_generated() -> None:
    fake = _allow()
    client = TestClient(_build_app(fake))
    resp = client.post("/v1/workspaces/ws_42/things", json={"name": "thing"}, headers=_BEARER)
    assert resp.status_code == 200
    # No inbound correlation id → the gateway generates one; the minted context
    # carries the same id the response header returns.
    assert resp.json()["correlation_id"] == resp.headers[CORRELATION_ID_HEADER]


def test_unscoped_platform_workspace_is_signed() -> None:
    fake = _allow(workspace="__platform__")
    app = FastAPI()
    app.add_middleware(CorrelationIdMiddleware)
    register_exception_handlers(app)
    setattr(app.state, AUTH_CLIENT_STATE_ATTR, fake)

    @app.post("/v1/platform/admin")
    async def admin(
        _caller: AuthorizedCaller = Depends(require_permission("platform:write")),
        minted: MintedCallContext = Depends(mint_call_context),
    ) -> dict[str, str]:
        return {"token": minted.token}

    resp = TestClient(app).post("/v1/platform/admin", json={}, headers=_BEARER)
    assert resp.status_code == 200
    assert fake.sign_calls[0].workspace_id == "__platform__"


def test_sign_failure_maps_to_downstream_unavailable() -> None:
    # verify_and_authorize succeeds, but callctx.sign fails — only the minter's
    # branch should map the failure onto the locked taxonomy.
    class _SignFails(FakeAuthServiceClient):
        async def callctx_sign(self, request: CallctxSignRequest) -> CallctxSignResponse:
            raise AuthServiceClientTransportError("auth down")

    fake = _SignFails(
        decision=VerifyAndAuthorizeResponse(
            principal_id="sa_1", allowed=True, reason="allow", audit_event_id="evt_1"
        )
    )
    client = TestClient(_build_app(fake))
    resp = client.post("/v1/workspaces/ws_42/things", json={"name": "thing"}, headers=_BEARER)
    assert resp.status_code == 503
    assert resp.json()["code"] == "downstream-unavailable"
    # The token verified — only the minting hop failed.
    assert len(fake.verify_calls) == 1


def test_bypass_route_mints_no_context() -> None:
    fake = _allow()
    client = TestClient(_build_app(fake))
    resp = client.post("/v1/webhooks/conn_1", json={"event": "x"})
    assert resp.status_code == 200
    assert fake.sign_calls == []
    assert fake.verify_calls == []


def test_minting_before_authorize_is_downstream_unavailable() -> None:
    # The minter declared without the authz dependency: no AuthorizedCaller on
    # request.state → the gateway refuses with a Problem+JSON 503 rather than
    # leaking a raw 500.
    fake = _allow()
    app = FastAPI()
    app.add_middleware(CorrelationIdMiddleware)
    register_exception_handlers(app)
    setattr(app.state, AUTH_CLIENT_STATE_ATTR, fake)

    @app.post("/v1/workspaces/{workspaceId}/oops")
    async def oops(
        minted: MintedCallContext = Depends(mint_call_context),
    ) -> dict[str, str]:
        return {"token": minted.token}

    resp = TestClient(app).post("/v1/workspaces/ws_42/oops", json={})
    assert resp.status_code == 503
    assert resp.json()["code"] == "downstream-unavailable"
    assert fake.sign_calls == []
