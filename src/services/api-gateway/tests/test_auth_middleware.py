"""Tests for the AuthN/AuthZ dependency + bypass classifier (AGW-IMPL-005)."""

from __future__ import annotations

import pytest
from fastapi import Depends, FastAPI, Request
from fastapi.testclient import TestClient

from custos_gateway.clients.auth import (
    AuthServiceClientStatusError,
    AuthServiceClientTransportError,
    FakeAuthServiceClient,
    VerifyAndAuthorizeResponse,
)
from custos_gateway.errors import (
    GatewayError,
    GatewayErrorCode,
    register_exception_handlers,
)
from custos_gateway.middleware.auth import (
    AUTH_CLIENT_STATE_ATTR,
    PLATFORM_WORKSPACE_ID,
    AuthorizedCaller,
    get_auth_client,
    is_auth_bypass_path,
    require_permission,
)


def _build_app(client: FakeAuthServiceClient | None) -> FastAPI:
    app = FastAPI()
    register_exception_handlers(app)
    if client is not None:
        setattr(app.state, AUTH_CLIENT_STATE_ATTR, client)

    @app.get("/v1/workspaces/{workspaceId}/things")
    async def scoped(
        request: Request,
        caller: AuthorizedCaller = Depends(require_permission("things:read")),
    ) -> dict[str, str]:
        # The dependency also binds the caller to request.state.auth.
        assert request.state.auth is caller
        return {
            "principal_id": caller.principal_id,
            "audit_event_id": caller.audit_event_id,
            "workspace_id": caller.workspace_id,
            "permission": caller.permission,
        }

    @app.get("/v1/platform/admin")
    async def unscoped(
        caller: AuthorizedCaller = Depends(require_permission("platform:read")),
    ) -> dict[str, str]:
        return {"workspace_id": caller.workspace_id}

    @app.post("/v1/webhooks/{connectorInstanceId}")
    async def webhook() -> dict[str, bool]:
        # No auth dependency — must never touch the Auth Service.
        return {"ok": True}

    return app


# --- Bypass classifier -------------------------------------------------------


@pytest.mark.parametrize(
    "path",
    [
        "/v1/webhooks/abc",
        "/v1/auth/login/device",
        "/v1/auth/login/oidc/callback",
        "/v1/auth/login/device/CODE/poll",
    ],
)
def test_is_auth_bypass_path_true(path: str) -> None:
    assert is_auth_bypass_path(path) is True


@pytest.mark.parametrize(
    "path",
    [
        "/v1/workspaces/ws1/things",
        "/v1/auth/logout",
        "/healthz",
        "/v1/webhook",  # singular, not the bypass prefix
        "/v1/auth/login2/device",  # sibling segment, not the login family
        "/v1/auth/loginX",
    ],
)
def test_is_auth_bypass_path_false(path: str) -> None:
    assert is_auth_bypass_path(path) is False


# --- get_auth_client ---------------------------------------------------------


def test_get_auth_client_raises_when_unset() -> None:
    app = FastAPI()

    class _FakeRequest:
        def __init__(self, application: FastAPI) -> None:
            self.app = application

    with pytest.raises(GatewayError) as exc_info:
        get_auth_client(_FakeRequest(app))  # type: ignore[arg-type]
    assert exc_info.value.code is GatewayErrorCode.DOWNSTREAM_UNAVAILABLE


def test_get_auth_client_returns_attached() -> None:
    fake = FakeAuthServiceClient()
    app = FastAPI()
    setattr(app.state, AUTH_CLIENT_STATE_ATTR, fake)

    class _FakeRequest:
        def __init__(self, application: FastAPI) -> None:
            self.app = application

    assert get_auth_client(_FakeRequest(app)) is fake  # type: ignore[arg-type]


# --- require_permission factory ---------------------------------------------


def test_require_permission_rejects_empty() -> None:
    with pytest.raises(ValueError, match="non-empty permission"):
        require_permission("")


def test_allows_and_attaches_principal() -> None:
    fake = FakeAuthServiceClient(
        decision=VerifyAndAuthorizeResponse(
            principal_id="sa_1", allowed=True, reason="allow", audit_event_id="evt_1"
        )
    )
    client = TestClient(_build_app(fake))
    resp = client.get("/v1/workspaces/ws_42/things", headers={"Authorization": "Bearer tok"})
    assert resp.status_code == 200
    assert resp.json() == {
        "principal_id": "sa_1",
        "audit_event_id": "evt_1",
        "workspace_id": "ws_42",
        "permission": "things:read",
    }
    # The decision used the path workspace + the route's required permission.
    assert len(fake.verify_calls) == 1
    sent = fake.verify_calls[0]
    assert sent.token == "tok"
    assert sent.permission == "things:read"
    assert sent.workspace_id == "ws_42"


def test_unscoped_route_uses_platform_sentinel() -> None:
    fake = FakeAuthServiceClient()
    client = TestClient(_build_app(fake))
    resp = client.get("/v1/platform/admin", headers={"Authorization": "Bearer tok"})
    assert resp.status_code == 200
    assert resp.json() == {"workspace_id": PLATFORM_WORKSPACE_ID}
    assert fake.verify_calls[0].workspace_id == PLATFORM_WORKSPACE_ID


def test_pre_bound_workspace_state_wins_over_path() -> None:
    # Forward-compat with the Workspace Resolver (AGW-IMPL-006): a value already
    # bound to request.state.workspace_id takes precedence over the path param.
    fake = FakeAuthServiceClient()
    app = _build_app(fake)

    @app.middleware("http")
    async def _bind_workspace(request: Request, call_next):  # type: ignore[no-untyped-def]
        request.state.workspace_id = "ws_from_state"
        return await call_next(request)

    client = TestClient(app)
    resp = client.get("/v1/workspaces/ws_from_path/things", headers={"Authorization": "Bearer tok"})
    assert resp.status_code == 200
    assert resp.json()["workspace_id"] == "ws_from_state"
    assert fake.verify_calls[0].workspace_id == "ws_from_state"


def test_missing_bearer_is_invalid_token() -> None:
    fake = FakeAuthServiceClient()
    client = TestClient(_build_app(fake))
    resp = client.get("/v1/workspaces/ws_1/things")
    assert resp.status_code == 401
    body = resp.json()
    assert body["code"] == "invalid-token"
    # Bypass-style guard: a rejected bearer never reaches Auth Service.
    assert fake.verify_calls == []


@pytest.mark.parametrize(
    "header",
    ["Token abc", "Bearer", "Bearer    ", "bearer ", "Bearer tok extra"],
)
def test_malformed_bearer_is_invalid_token(header: str) -> None:
    fake = FakeAuthServiceClient()
    client = TestClient(_build_app(fake))
    resp = client.get("/v1/workspaces/ws_1/things", headers={"Authorization": header})
    assert resp.status_code == 401
    assert resp.json()["code"] == "invalid-token"
    assert fake.verify_calls == []


def test_case_insensitive_bearer_scheme() -> None:
    fake = FakeAuthServiceClient()
    client = TestClient(_build_app(fake))
    resp = client.get("/v1/workspaces/ws_1/things", headers={"Authorization": "bEaReR tok"})
    assert resp.status_code == 200
    assert fake.verify_calls[0].token == "tok"


def test_denied_returns_403_with_audit_event_id() -> None:
    fake = FakeAuthServiceClient(
        decision=VerifyAndAuthorizeResponse(
            principal_id="sa_1",
            allowed=False,
            reason="not your workspace",
            audit_event_id="evt_deny",
        )
    )
    client = TestClient(_build_app(fake))
    resp = client.get("/v1/workspaces/ws_1/things", headers={"Authorization": "Bearer tok"})
    assert resp.status_code == 403
    body = resp.json()
    assert body["code"] == "permission-denied"
    assert body["auditEventId"] == "evt_deny"
    assert body["detail"] == "not your workspace"


def test_auth_status_401_maps_to_invalid_token() -> None:
    fake = FakeAuthServiceClient(
        error=AuthServiceClientStatusError("verify failed", status_code=401)
    )
    client = TestClient(_build_app(fake))
    resp = client.get("/v1/workspaces/ws_1/things", headers={"Authorization": "Bearer tok"})
    assert resp.status_code == 401
    assert resp.json()["code"] == "invalid-token"


def test_auth_status_5xx_maps_to_downstream_unavailable() -> None:
    fake = FakeAuthServiceClient(error=AuthServiceClientStatusError("boom", status_code=500))
    client = TestClient(_build_app(fake))
    resp = client.get("/v1/workspaces/ws_1/things", headers={"Authorization": "Bearer tok"})
    assert resp.status_code == 503
    assert resp.json()["code"] == "downstream-unavailable"


def test_transport_error_maps_to_downstream_unavailable() -> None:
    fake = FakeAuthServiceClient(error=AuthServiceClientTransportError("no route"))
    client = TestClient(_build_app(fake))
    resp = client.get("/v1/workspaces/ws_1/things", headers={"Authorization": "Bearer tok"})
    assert resp.status_code == 503
    assert resp.json()["code"] == "downstream-unavailable"


def test_bypass_route_never_calls_auth() -> None:
    fake = FakeAuthServiceClient()
    client = TestClient(_build_app(fake))
    resp = client.post("/v1/webhooks/conn_1")
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}
    assert fake.verify_calls == []
