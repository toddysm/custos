"""Tests for the call-context middleware + dependencies (Phase C scaffolding)."""

from __future__ import annotations

import json
from typing import Any

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from starlette.requests import Request

from custos_auth.middleware.callctx import (
    CALLCTX_HEADER,
    CallContext,
    CallContextError,
    CallContextMiddleware,
    DevShimDisabledInProductionError,
    call_context_error_handler,
    get_call_context,
    require_permission,
)


def _app_with_middleware(
    *,
    verifier_url: str = "",
    environment: str = "development",
) -> FastAPI:
    app = FastAPI()
    app.add_middleware(
        CallContextMiddleware,
        verifier_url=verifier_url,
        environment=environment,
    )
    app.add_exception_handler(CallContextError, call_context_error_handler)

    @app.get("/whoami")
    async def whoami(request: Request) -> dict[str, object]:
        ctx = await get_call_context(request)
        return {
            "principal_id": ctx.principal_id,
            "tenant_id": ctx.tenant_id,
            "workspace_id": ctx.workspace_id,
            "permissions": sorted(ctx.permissions),
        }

    @app.get("/admin", dependencies=[Depends(require_permission("platform.admin"))])
    async def admin() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return app


def _header(ctx: dict[str, Any]) -> dict[str, str]:
    return {CALLCTX_HEADER: json.dumps(ctx)}


# ---------------------------------------------------------------------------
# Bypass paths
# ---------------------------------------------------------------------------


def test_healthz_bypass_does_not_require_callctx_header() -> None:
    app = _app_with_middleware()
    with TestClient(app) as client:
        resp = client.get("/healthz")
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Missing / malformed header
# ---------------------------------------------------------------------------


def test_missing_callctx_header_returns_401() -> None:
    app = _app_with_middleware()
    with TestClient(app) as client:
        resp = client.get("/whoami")
    assert resp.status_code == 401
    body = resp.json()
    assert body == {
        "error": {
            "code": "callctx_missing",
            "detail": f"{CALLCTX_HEADER} header is required",
        }
    }


def test_malformed_callctx_header_returns_400() -> None:
    app = _app_with_middleware()
    with TestClient(app) as client:
        resp = client.get("/whoami", headers={CALLCTX_HEADER: "not-json"})
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "callctx_malformed"


def test_non_object_callctx_header_returns_400() -> None:
    app = _app_with_middleware()
    with TestClient(app) as client:
        # A JSON array decodes but is not an object → ValueError path.
        resp = client.get("/whoami", headers={CALLCTX_HEADER: "[1, 2, 3]"})
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "callctx_malformed"


def test_invalid_callctx_fields_returns_400() -> None:
    app = _app_with_middleware()
    # Missing required ``principal_id`` field.
    with TestClient(app) as client:
        resp = client.get("/whoami", headers=_header({"tenant_id": "t1"}))
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "callctx_invalid"


# ---------------------------------------------------------------------------
# Happy path: dev-shim parses header onto request.state.call_context
# ---------------------------------------------------------------------------


def test_dev_shim_parses_full_context_payload() -> None:
    app = _app_with_middleware()
    payload = {
        "principal_id": "user-1",
        "tenant_id": "tenant-1",
        "workspace_id": "ws-1",
        "permissions": ["platform.admin", "tenant.admin"],
    }
    with TestClient(app) as client:
        resp = client.get("/whoami", headers=_header(payload))
    assert resp.status_code == 200
    body = resp.json()
    assert body["principal_id"] == "user-1"
    assert body["tenant_id"] == "tenant-1"
    assert body["workspace_id"] == "ws-1"
    assert body["permissions"] == ["platform.admin", "tenant.admin"]


def test_dev_shim_accepts_minimal_payload_without_workspace() -> None:
    """A platform-admin call (POST /v1/tenants) carries no workspace."""
    app = _app_with_middleware()
    payload = {"principal_id": "user-1", "permissions": ["platform.admin"]}
    with TestClient(app) as client:
        resp = client.get("/whoami", headers=_header(payload))
    assert resp.status_code == 200
    body = resp.json()
    assert body["tenant_id"] is None
    assert body["workspace_id"] is None


# ---------------------------------------------------------------------------
# require_permission dependency
# ---------------------------------------------------------------------------


def test_require_permission_blocks_caller_without_perm() -> None:
    app = _app_with_middleware()
    payload = {"principal_id": "user-1", "permissions": ["tenant.admin"]}
    with TestClient(app) as client:
        resp = client.get("/admin", headers=_header(payload))
    assert resp.status_code == 403
    body = resp.json()
    assert body == {
        "error": {
            "code": "permission_denied",
            "detail": "missing required permission: platform.admin",
        }
    }


def test_require_permission_admits_caller_with_perm() -> None:
    app = _app_with_middleware()
    payload = {"principal_id": "user-1", "permissions": ["platform.admin"]}
    with TestClient(app) as client:
        resp = client.get("/admin", headers=_header(payload))
    assert resp.status_code == 200


def test_require_permission_with_multiple_names_or_semantics() -> None:
    app = FastAPI()
    app.add_middleware(CallContextMiddleware, verifier_url="", environment="development")
    app.add_exception_handler(CallContextError, call_context_error_handler)

    @app.get(
        "/either",
        dependencies=[Depends(require_permission("platform.admin", "tenant.admin"))],
    )
    async def either() -> dict[str, str]:
        return {"ok": "1"}

    # Only tenant.admin → still admitted.
    payload = {"principal_id": "user-1", "permissions": ["tenant.admin"]}
    with TestClient(app) as client:
        resp = client.get("/either", headers=_header(payload))
    assert resp.status_code == 200

    # Neither → 403 with "one of …" detail.
    payload2 = {"principal_id": "user-1", "permissions": ["something.else"]}
    with TestClient(app) as client:
        resp = client.get("/either", headers=_header(payload2))
    assert resp.status_code == 403
    assert "one of" in resp.json()["error"]["detail"]


def test_require_permission_rejects_empty_name_tuple() -> None:
    with pytest.raises(ValueError, match="at least one permission"):
        require_permission()


# ---------------------------------------------------------------------------
# Production guard
# ---------------------------------------------------------------------------


def test_dev_shim_refuses_to_start_in_production() -> None:
    app = FastAPI()
    app.add_middleware(
        CallContextMiddleware,
        verifier_url="",
        environment="production",
    )
    # Force eager middleware-stack construction so the __init__-time
    # guard surfaces here rather than lazily on the first request.
    with pytest.raises(DevShimDisabledInProductionError):
        app.build_middleware_stack()


def test_dev_shim_refuses_to_start_in_production_case_insensitive() -> None:
    app = FastAPI()
    app.add_middleware(
        CallContextMiddleware,
        verifier_url="",
        environment="PRODUCTION",
    )
    with pytest.raises(DevShimDisabledInProductionError):
        app.build_middleware_stack()


def test_verifier_url_set_in_production_is_allowed() -> None:
    """Production + non-empty verifier URL is the legal configuration."""
    app = FastAPI()
    app.add_middleware(
        CallContextMiddleware,
        verifier_url="https://auth.example.com/.well-known/jwks.json",
        environment="production",
    )
    app.build_middleware_stack()  # No exception → pass.


def test_dev_shim_starts_in_non_production_when_verifier_url_empty() -> None:
    app = FastAPI()
    app.add_middleware(
        CallContextMiddleware,
        verifier_url="",
        environment="development",
    )

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    with TestClient(app) as client:
        resp = client.get("/healthz")
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Verifier URL set → NotImplementedError until Phase G
# ---------------------------------------------------------------------------


def test_verifier_url_set_raises_not_implemented_on_protected_request() -> None:
    app = _app_with_middleware(
        verifier_url="https://auth.example.com/.well-known/jwks.json",
        environment="production",
    )
    payload = {"principal_id": "user-1"}
    with TestClient(app, raise_server_exceptions=False) as client:
        resp = client.get("/whoami", headers=_header(payload))
    assert resp.status_code == 500


def test_verifier_url_set_still_bypasses_healthz() -> None:
    app = _app_with_middleware(
        verifier_url="https://auth.example.com/.well-known/jwks.json",
        environment="production",
    )
    with TestClient(app) as client:
        resp = client.get("/healthz")
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# CallContext model invariants
# ---------------------------------------------------------------------------


def test_callcontext_permissions_coerced_to_frozenset() -> None:
    ctx = CallContext.model_validate(
        {
            "principal_id": "user-1",
            "permissions": frozenset({"a", "b"}),
        }
    )
    assert ctx.has_permission("a")
    assert ctx.has_any_permission("a", "z")
    assert not ctx.has_permission("z")


def test_callcontext_extra_fields_forbidden() -> None:
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        CallContext.model_validate({"principal_id": "user-1", "unknown_field": "x"})


def test_get_call_context_raises_when_state_unpopulated() -> None:
    """Endpoints outside the middleware's stack must still get the
    same envelope-shaped 401."""
    app = FastAPI()
    app.add_exception_handler(CallContextError, call_context_error_handler)

    @app.get("/no-middleware")
    async def no_middleware(request: Request) -> dict[str, object]:
        ctx = await get_call_context(request)
        return {"principal_id": ctx.principal_id}

    with TestClient(app) as client:
        resp = client.get("/no-middleware")
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "callctx_missing"
