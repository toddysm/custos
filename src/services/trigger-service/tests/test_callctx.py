"""Tests for :mod:`custos_trigger.middleware.callctx` (TS-IMPL-003)."""

from __future__ import annotations

import json
import logging

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from pydantic import ValidationError

from custos_trigger.middleware import (
    CALLCTX_HEADER,
    CallContext,
    CallContextError,
    CallContextMiddleware,
    DevShimDisabledInProductionError,
    call_context_error_handler,
    get_call_context,
    require_permission,
)

_VALID_CTX_HEADER = json.dumps(
    {
        "workspace_id": "ws_demo",
        "principal_id": "user_alice",
        "permissions": ["trigger:read", "trigger:write"],
    },
)


def _build_app(*, authz_endpoint: str = "", environment: str = "development") -> FastAPI:
    app = FastAPI()
    app.add_middleware(
        CallContextMiddleware,
        authz_endpoint=authz_endpoint,
        environment=environment,
    )
    # Mirror what `custos_trigger.create_app` does: pair the middleware with
    # its exception handler so the dependency-side 4xx responses share the
    # middleware's `{"error": {"code", "detail"}}` envelope.
    app.add_exception_handler(CallContextError, call_context_error_handler)

    @app.get("/healthz")
    async def _healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/whoami")
    async def _whoami(ctx: CallContext = Depends(get_call_context)) -> dict[str, object]:
        return {
            "workspace_id": ctx.workspace_id,
            "principal_id": ctx.principal_id,
            "permissions": sorted(ctx.permissions),
        }

    @app.get("/write")
    async def _write(
        ctx: CallContext = Depends(require_permission("trigger:write")),
    ) -> dict[str, str]:
        return {"workspace_id": ctx.workspace_id}

    # Force eager middleware instantiation so __init__-time guards (e.g. the
    # dev-shim production check) surface here rather than on the first request.
    app.build_middleware_stack()
    return app


def test_dev_shim_in_production_refuses_to_start() -> None:
    with pytest.raises(DevShimDisabledInProductionError):
        _build_app(authz_endpoint="", environment="production")


def test_dev_shim_in_uppercase_production_refuses_to_start() -> None:
    with pytest.raises(DevShimDisabledInProductionError):
        _build_app(authz_endpoint="", environment="PRODUCTION")


def test_dev_shim_with_authz_endpoint_set_in_production_is_allowed() -> None:
    _build_app(authz_endpoint="http://auth-service:8080", environment="production")


def test_missing_header_returns_401() -> None:
    client = TestClient(_build_app())
    resp = client.get("/whoami")
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "callctx_missing"


def test_malformed_json_header_returns_400() -> None:
    client = TestClient(_build_app())
    resp = client.get("/whoami", headers={CALLCTX_HEADER: "not-json"})
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "callctx_malformed"


def test_invalid_schema_header_returns_400() -> None:
    client = TestClient(_build_app())
    # Missing required fields: workspace_id, principal_id.
    payload = json.dumps({"permissions": ["trigger:read"]})
    resp = client.get("/whoami", headers={CALLCTX_HEADER: payload})
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "callctx_invalid"


def test_array_header_is_rejected_as_malformed() -> None:
    client = TestClient(_build_app())
    resp = client.get("/whoami", headers={CALLCTX_HEADER: json.dumps([1, 2, 3])})
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "callctx_malformed"


def test_dev_shim_valid_header_populates_call_context(
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = TestClient(_build_app())
    with caplog.at_level(logging.WARNING, logger="custos_trigger.middleware.callctx"):
        resp = client.get("/whoami", headers={CALLCTX_HEADER: _VALID_CTX_HEADER})
    assert resp.status_code == 200
    assert resp.json() == {
        "workspace_id": "ws_demo",
        "principal_id": "user_alice",
        "permissions": ["trigger:read", "trigger:write"],
    }
    assert any("dev shim active" in r.message for r in caplog.records)


def test_require_permission_grants_when_present() -> None:
    client = TestClient(_build_app())
    resp = client.get("/write", headers={CALLCTX_HEADER: _VALID_CTX_HEADER})
    assert resp.status_code == 200
    assert resp.json() == {"workspace_id": "ws_demo"}


def test_require_permission_denies_when_missing() -> None:
    client = TestClient(_build_app())
    ctx = json.dumps(
        {
            "workspace_id": "ws_demo",
            "principal_id": "user_alice",
            "permissions": ["trigger:read"],
        },
    )
    resp = client.get("/write", headers={CALLCTX_HEADER: ctx})
    assert resp.status_code == 403
    body = resp.json()
    assert body["error"]["code"] == "permission_denied"
    assert "trigger:write" in body["error"]["detail"]


def test_dependency_unmounted_middleware_returns_shared_envelope() -> None:
    """get_call_context renders the same envelope as the middleware.

    Builds an app with the dependency-using route but WITHOUT the middleware
    mounted (mirroring a misconfigured deployment), then confirms the 401
    response matches the `callctx_missing` envelope the middleware itself
    emits when the header is absent.
    """
    app = FastAPI()
    app.add_exception_handler(CallContextError, call_context_error_handler)

    @app.get("/whoami")
    async def _whoami(ctx: CallContext = Depends(get_call_context)) -> dict[str, str]:
        return {"workspace_id": ctx.workspace_id}

    client = TestClient(app)
    resp = client.get("/whoami")
    assert resp.status_code == 401
    assert resp.json() == {
        "error": {
            "code": "callctx_missing",
            "detail": f"{CALLCTX_HEADER} header is required",
        },
    }


def test_production_path_with_authz_endpoint_raises_not_implemented() -> None:
    app = _build_app(authz_endpoint="http://auth-service:8080", environment="staging")
    client = TestClient(app, raise_server_exceptions=False)
    resp = client.get("/whoami", headers={CALLCTX_HEADER: _VALID_CTX_HEADER})
    # Starlette converts uncaught middleware exceptions to a 500 by default.
    assert resp.status_code == 500


def test_healthz_bypasses_middleware() -> None:
    client = TestClient(_build_app())
    resp = client.get("/healthz")  # no callctx header
    assert resp.status_code == 200


def test_call_context_model_is_frozen() -> None:
    ctx = CallContext(workspace_id="ws", principal_id="u")
    with pytest.raises(ValidationError):
        ctx.workspace_id = "other"
