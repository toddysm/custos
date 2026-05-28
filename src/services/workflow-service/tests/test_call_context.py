"""Tests for the call-context middleware shim (WF-IMPL-015).

The shim extracts ``X-Custos-Workspace`` and ``X-Custos-Principal``
headers onto ``request.state.call_context``. In dev mode missing
headers yield ``CallContext(workspace=None, principal=None)``; in
production mode (``require_call_context=True``) missing headers yield
a 401 with a stable error envelope.

We mount the middleware against a small ad-hoc FastAPI app rather than
using ``create_app`` so the tests target the middleware contract in
isolation. ``test_app.py`` already covers the full factory wiring.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.requests import Request
from starlette.responses import JSONResponse

from custos_workflow.call_context import (
    PRINCIPAL_HEADER,
    WORKSPACE_HEADER,
    CallContext,
    CallContextMiddleware,
)


def _mount_echo_app(*, require_call_context: bool) -> FastAPI:
    app = FastAPI()
    app.add_middleware(CallContextMiddleware, require_call_context=require_call_context)

    @app.get("/echo")
    async def echo(request: Request) -> JSONResponse:
        ctx = request.state.call_context
        assert isinstance(ctx, CallContext)
        return JSONResponse({"workspace": ctx.workspace, "principal": ctx.principal})

    @app.get("/healthz")
    async def healthz(request: Request) -> JSONResponse:
        # Bypassed by the middleware — request.state.call_context is NOT set.
        return JSONResponse({"has_ctx": hasattr(request.state, "call_context")})

    @app.get("/readyz")
    async def readyz(request: Request) -> JSONResponse:
        return JSONResponse({"has_ctx": hasattr(request.state, "call_context")})

    return app


# ---- Dev-mode behaviour -----------------------------------------------------


def test_dev_mode_missing_headers_yield_none_context() -> None:
    app = _mount_echo_app(require_call_context=False)
    with TestClient(app) as client:
        response = client.get("/echo")
    assert response.status_code == 200
    assert response.json() == {"workspace": None, "principal": None}


def test_dev_mode_present_headers_populate_context() -> None:
    app = _mount_echo_app(require_call_context=False)
    with TestClient(app) as client:
        response = client.get(
            "/echo",
            headers={
                WORKSPACE_HEADER: "ws-42",
                PRINCIPAL_HEADER: "alice",
            },
        )
    assert response.status_code == 200
    assert response.json() == {"workspace": "ws-42", "principal": "alice"}


def test_dev_mode_partial_headers_populate_what_is_present() -> None:
    app = _mount_echo_app(require_call_context=False)
    with TestClient(app) as client:
        response = client.get("/echo", headers={WORKSPACE_HEADER: "ws-42"})
    assert response.status_code == 200
    assert response.json() == {"workspace": "ws-42", "principal": None}


# ---- Production-mode behaviour ---------------------------------------------


def test_production_mode_missing_both_headers_returns_401() -> None:
    app = _mount_echo_app(require_call_context=True)
    with TestClient(app) as client:
        response = client.get("/echo")
    assert response.status_code == 401
    body = response.json()
    assert body["error"]["code"] == "callctx_missing"
    assert WORKSPACE_HEADER in body["error"]["detail"]
    assert PRINCIPAL_HEADER in body["error"]["detail"]


def test_production_mode_missing_workspace_returns_401_listing_only_it() -> None:
    app = _mount_echo_app(require_call_context=True)
    with TestClient(app) as client:
        response = client.get("/echo", headers={PRINCIPAL_HEADER: "alice"})
    assert response.status_code == 401
    body = response.json()
    assert body["error"]["code"] == "callctx_missing"
    assert WORKSPACE_HEADER in body["error"]["detail"]
    assert PRINCIPAL_HEADER not in body["error"]["detail"]


def test_production_mode_with_both_headers_passes_through() -> None:
    app = _mount_echo_app(require_call_context=True)
    with TestClient(app) as client:
        response = client.get(
            "/echo",
            headers={
                WORKSPACE_HEADER: "ws-42",
                PRINCIPAL_HEADER: "alice",
            },
        )
    assert response.status_code == 200
    assert response.json() == {"workspace": "ws-42", "principal": "alice"}


# ---- Probe bypass -----------------------------------------------------------


@pytest.mark.parametrize("probe", ["/healthz", "/readyz"])
def test_probes_bypass_middleware_in_dev_mode(probe: str) -> None:
    app = _mount_echo_app(require_call_context=False)
    with TestClient(app) as client:
        response = client.get(probe)
    assert response.status_code == 200
    assert response.json() == {"has_ctx": False}


@pytest.mark.parametrize("probe", ["/healthz", "/readyz"])
def test_probes_bypass_middleware_in_production_mode(probe: str) -> None:
    app = _mount_echo_app(require_call_context=True)
    with TestClient(app) as client:
        # No headers — production mode still lets probes through.
        response = client.get(probe)
    assert response.status_code == 200
    assert response.json() == {"has_ctx": False}


# ---- Dataclass invariants ---------------------------------------------------


def test_call_context_is_frozen_dataclass() -> None:
    ctx = CallContext(workspace="ws-42", principal="alice")
    with pytest.raises(AttributeError):
        ctx.workspace = "other"  # type: ignore[misc]


def test_call_context_accepts_both_none() -> None:
    ctx = CallContext(workspace=None, principal=None)
    assert ctx.workspace is None
    assert ctx.principal is None
