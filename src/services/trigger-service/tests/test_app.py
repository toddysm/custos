"""ASGI smoke tests for :func:`custos_trigger.create_app` (TS-IMPL-003)."""

from __future__ import annotations

import json

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from custos_trigger import create_app
from custos_trigger.middleware import DevShimDisabledInProductionError


def test_create_app_returns_a_fastapi_instance() -> None:
    app = create_app()
    assert isinstance(app, FastAPI)


def test_healthz_returns_200() -> None:
    app = create_app()
    with TestClient(app) as client:
        resp = client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_readyz_returns_200_after_startup() -> None:
    app = create_app()
    with TestClient(app) as client:
        resp = client.get("/readyz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ready"}


def test_readyz_returns_503_before_lifespan_marks_ready() -> None:
    # Without entering the lifespan context (no ``with TestClient``), the
    # readiness flag is unset, so the probe must report 503.
    app = create_app()
    # Drive the readyz handler directly against a minimal scope so we observe
    # the pre-startup branch (app.state.ready is unset).
    import asyncio

    from starlette.requests import Request

    from custos_trigger.health import readyz

    scope = {"type": "http", "app": app, "headers": []}
    request = Request(scope)
    resp = asyncio.run(readyz(request))
    assert resp.status_code == 503
    body = json.loads(bytes(resp.body))
    assert body["status"] == "not_ready"


def test_app_state_is_ready_within_lifespan() -> None:
    app = create_app()
    with TestClient(app):
        assert app.state.ready is True


def test_create_app_in_production_with_dev_shim_refuses_to_boot() -> None:
    # The dev shim is forbidden in production. The middleware is constructed
    # lazily on the first request, so the guard fires when the request runs.
    app = create_app(authz_endpoint="", environment="production")
    with pytest.raises(DevShimDisabledInProductionError), TestClient(app) as client:
        client.get("/healthz")
