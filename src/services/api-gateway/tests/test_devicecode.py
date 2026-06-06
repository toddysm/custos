"""Tests for the device-code session manager M1 503 stub (AGW-IMPL-015)."""

from __future__ import annotations

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from custos_gateway.errors import GatewayError, GatewayErrorCode, register_exception_handlers
from custos_gateway.middleware.auth import is_auth_bypass_path
from custos_gateway.routes.devicecode import (
    DEVICE_CODE_LANDING_PATH,
    DEVICE_CODE_POLL_PATH,
    DEVICE_CODE_START_PATH,
    DEVICE_CODE_STORE_STATE_ATTR,
    build_device_code_router,
    device_code_flow_unavailable,
    get_device_code_store,
)
from custos_gateway.settings import Settings, load_settings

from .conftest import minimal_gateway_env

# --- router shape ------------------------------------------------------------


def test_router_mounts_the_three_auth_bootstrap_routes() -> None:
    router = build_device_code_router()
    mounted = {
        (method, route.path)  # type: ignore[attr-defined]
        for route in router.routes
        for method in route.methods  # type: ignore[attr-defined]
        if method != "HEAD"  # Starlette auto-adds HEAD for every GET route.
    }
    assert mounted == {
        ("POST", DEVICE_CODE_START_PATH),
        ("POST", DEVICE_CODE_POLL_PATH),
        ("GET", DEVICE_CODE_LANDING_PATH),
    }


def test_routes_declare_no_dependencies_and_bypass_authn() -> None:
    router = build_device_code_router()
    for route in router.routes:
        # Anonymous auth-bootstrap: no require_permission (or any) dependency.
        assert route.dependencies == []  # type: ignore[attr-defined]
        assert is_auth_bypass_path(route.path)  # type: ignore[attr-defined]


# --- M1 503 behavior ---------------------------------------------------------


def _disabled_settings() -> Settings:
    """Settings with no OIDC issuer configured (device-code flow disabled)."""
    settings = load_settings(minimal_gateway_env())
    assert not settings.device_code_enabled
    return settings


def _app() -> FastAPI:
    app = FastAPI()
    register_exception_handlers(app)
    app.include_router(build_device_code_router())
    app.state.settings = _disabled_settings()
    return app


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("post", "/v1/auth/login/device"),
        ("post", "/v1/auth/login/device/dev-123/poll"),
        ("get", "/v1/auth/login/device/USER-CODE"),
    ],
)
def test_handlers_return_503_while_oidc_disabled(method: str, path: str) -> None:
    client = TestClient(_app())
    response = client.request(method, path)
    assert response.status_code == 503
    body = response.json()
    assert body["code"] == "downstream-unavailable"
    assert "device-code" in body["detail"]


# --- persistence seam --------------------------------------------------------


def _request_with_store(store: object | None) -> Request:
    """Build a minimal ``Request`` whose ``app.state`` carries ``store``."""
    state = type("_State", (), {})()
    if store is not None:
        setattr(state, DEVICE_CODE_STORE_STATE_ATTR, store)
    app = type("_App", (), {"state": state})()
    return Request({"type": "http", "app": app, "headers": []})


def test_get_device_code_store_returns_bound_store() -> None:
    sentinel = object()
    request = _request_with_store(sentinel)
    assert get_device_code_store(request) is sentinel


def test_get_device_code_store_raises_503_when_unbound() -> None:
    request = _request_with_store(None)
    with pytest.raises(GatewayError) as excinfo:
        get_device_code_store(request)
    assert excinfo.value.code is GatewayErrorCode.DOWNSTREAM_UNAVAILABLE
    assert excinfo.value.status == 503


def test_device_code_flow_unavailable_is_a_503() -> None:
    error = device_code_flow_unavailable()
    assert error.code is GatewayErrorCode.DOWNSTREAM_UNAVAILABLE
    assert error.status == 503


def test_store_state_attr_is_stable() -> None:
    assert DEVICE_CODE_STORE_STATE_ATTR == "device_code_store"
