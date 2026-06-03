"""Tests for the call-context middleware + dev shim (ARM-IMPL-002)."""

from __future__ import annotations

import json
import logging

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from custos_arm import create_app
from custos_arm.config import Settings, load_settings
from custos_arm.middleware import (
    CALLCTX_HEADER,
    CallContext,
    CallContextError,
    CallContextMiddleware,
    DevShimDisabledInProductionError,
    call_context_error_handler,
    get_call_context,
)

_BASE_ENV: dict[str, str] = {
    "ARM_ARTIFACT_STORE": "artifacts",
    "ARM_METADATA_STORE": "metadata",
    "ARM_CATALOG_ENDPOINT": "http://catalog.svc:8080",
    "ARM_CONNECTOR_ENDPOINT": "http://connector.svc:8080",
    "ARM_SANDBOX_NAMESPACE": "custos-activities",
    "ARM_SIDECAR_IMAGE": "ghcr.io/custos/connector-sidecar:0.1.0",
}


def _settings(**overrides: str) -> Settings:
    env = dict(_BASE_ENV)
    env.update(overrides)
    return load_settings(env)


def _app_with_protected_route(settings: Settings) -> FastAPI:
    """Build an app whose ``/whoami`` route requires the call context."""
    app = FastAPI()
    app.add_middleware(
        CallContextMiddleware,
        authz_endpoint=settings.authz_endpoint,
        environment=settings.environment,
    )
    app.add_exception_handler(CallContextError, call_context_error_handler)

    @app.get("/whoami")
    async def whoami(ctx: CallContext = Depends(get_call_context)) -> dict[str, str]:
        return {"workspace": ctx.workspace_id, "principal": ctx.principal_id}

    return app


def _ctx_header(**overrides: object) -> dict[str, str]:
    payload: dict[str, object] = {
        "workspace_id": "ws-1",
        "principal_id": "user-1",
        "permissions": ["activities:run"],
    }
    payload.update(overrides)
    return {CALLCTX_HEADER: json.dumps(payload)}


def test_dev_shim_accepts_valid_header_and_warns(caplog: pytest.LogCaptureFixture) -> None:
    app = _app_with_protected_route(_settings())
    with TestClient(app) as client, caplog.at_level(logging.WARNING, logger="custos_arm"):
        response = client.get("/whoami", headers=_ctx_header())
    assert response.status_code == 200
    assert response.json() == {"workspace": "ws-1", "principal": "user-1"}
    assert any("dev shim active" in rec.message for rec in caplog.records)


def test_missing_header_is_rejected_401() -> None:
    app = _app_with_protected_route(_settings())
    with TestClient(app) as client:
        response = client.get("/whoami")
    assert response.status_code == 401
    assert response.json() == {
        "error": {"code": "callctx_missing", "detail": f"{CALLCTX_HEADER} header is required"}
    }


def test_malformed_json_header_is_rejected_400() -> None:
    app = _app_with_protected_route(_settings())
    with TestClient(app) as client:
        response = client.get("/whoami", headers={CALLCTX_HEADER: "{not json"})
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "callctx_malformed"


def test_non_object_json_header_is_rejected_400() -> None:
    app = _app_with_protected_route(_settings())
    with TestClient(app) as client:
        response = client.get("/whoami", headers={CALLCTX_HEADER: "[1, 2, 3]"})
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "callctx_malformed"


def test_invalid_context_shape_is_rejected_400() -> None:
    app = _app_with_protected_route(_settings())
    with TestClient(app) as client:
        response = client.get("/whoami", headers=_ctx_header(workspace_id=""))
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "callctx_invalid"


def test_production_dev_shim_refuses_to_start() -> None:
    settings = _settings(ENVIRONMENT="production")
    app = _app_with_protected_route(settings)
    # Starlette builds the middleware stack lazily on startup; entering the
    # TestClient lifespan forces construction, which must refuse to boot.
    with pytest.raises(DevShimDisabledInProductionError, match="forbidden in production"):  # noqa: SIM117
        with TestClient(app):
            pass


def test_production_with_authz_endpoint_starts() -> None:
    settings = _settings(ENVIRONMENT="production", ARM_AUTHZ_ENDPOINT="http://auth.svc:8080")
    # Construction must succeed; the request path is NotImplementedError
    # until the real verifier lands.
    app = _app_with_protected_route(settings)
    with TestClient(app):
        pass


def test_metrics_path_bypasses_the_middleware() -> None:
    # ``/metrics`` is reserved for the Prometheus scraper (ARM-IMPL-020) and
    # must not require a call-context header. No route is mounted yet, so a
    # bypassed request 404s rather than 401s.
    app = _app_with_protected_route(_settings())
    with TestClient(app) as client:
        response = client.get("/metrics")
    assert response.status_code == 404


def test_real_verifier_path_raises_not_implemented() -> None:
    settings = _settings(ARM_AUTHZ_ENDPOINT="http://auth.svc:8080")
    app = _app_with_protected_route(settings)
    client = TestClient(app, raise_server_exceptions=False)
    response = client.get("/whoami", headers=_ctx_header())
    assert response.status_code == 500


def test_probes_bypass_the_middleware() -> None:
    app = create_app()
    with TestClient(app) as client:
        assert client.get("/healthz").status_code == 200
        assert client.get("/readyz").status_code == 200


def test_create_app_mounts_middleware_and_requires_header() -> None:
    app = create_app()
    with TestClient(app) as client:
        # ``/openapi.json`` is bypassed; a non-bypassed unknown path still
        # passes through the middleware and demands the header.
        response = client.get("/v1/anything")
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "callctx_missing"


def test_get_call_context_without_middleware_raises() -> None:
    settings = _settings()
    app = FastAPI()
    app.add_exception_handler(CallContextError, call_context_error_handler)

    @app.get("/whoami")
    async def whoami(ctx: CallContext = Depends(get_call_context)) -> dict[str, str]:
        return {"workspace": ctx.workspace_id}

    # No middleware mounted -> dependency observes no context.
    assert settings.use_callctx_dev_shim is True
    client = TestClient(app)
    response = client.get("/whoami")
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "callctx_missing"


def test_call_context_has_permission() -> None:
    ctx = CallContext(workspace_id="ws", principal_id="p", permissions=frozenset({"a"}))
    assert ctx.has_permission("a") is True
    assert ctx.has_permission("b") is False
