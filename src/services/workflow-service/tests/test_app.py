"""Tests for ``custos_workflow.create_app`` (WF-IMPL-015).

These tests assert the factory shape — routes, middleware, lifespan
behaviour — without exercising the request path semantics (those live
in ``test_healthz.py`` and ``test_call_context.py``).
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from unittest import mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from custos_workflow import create_app
from custos_workflow.call_context import CallContextMiddleware


def test_create_app_returns_fastapi_with_expected_metadata() -> None:
    app = create_app(require_call_context=False)
    assert isinstance(app, FastAPI)
    assert app.title == "Custos Workflow Service"
    assert app.version == "0.1.0"


def test_create_app_registers_health_routes() -> None:
    app = create_app(require_call_context=False)
    paths = {route.path for route in app.routes if hasattr(route, "path")}
    assert "/healthz" in paths
    assert "/readyz" in paths


def test_create_app_installs_call_context_middleware() -> None:
    app = create_app(require_call_context=False)
    middleware_classes = [m.cls for m in app.user_middleware]
    assert CallContextMiddleware in middleware_classes  # type: ignore[comparison-overlap]


def test_lifespan_flips_ready_to_true() -> None:
    app = create_app(require_call_context=False)
    with TestClient(app) as client:
        # Inside the ``with`` block FastAPI has run the lifespan startup.
        response = client.get("/readyz")
        assert response.status_code == 200
        assert response.json() == {"status": "ready"}
        assert app.state.ready is True
        assert app.state.ready_detail is None


@pytest.fixture
def _clean_env() -> Iterator[None]:
    """Snapshot + restore ``WF_REQUIRE_CALL_CONTEXT`` around env-driven tests."""
    with mock.patch.dict(os.environ, {}, clear=False):
        os.environ.pop("WF_REQUIRE_CALL_CONTEXT", None)
        yield


def test_create_app_honours_env_flag_dev_default(_clean_env: None) -> None:
    """Unset env → dev shim; create_app with no explicit flag does not raise."""
    app = create_app()
    # Smoke through TestClient to confirm the middleware is installed in the
    # dev shape (no headers required).
    with TestClient(app) as client:
        assert client.get("/healthz").status_code == 200


def test_create_app_honours_env_flag_production(_clean_env: None) -> None:
    """``WF_REQUIRE_CALL_CONTEXT=1`` → production-mode middleware."""
    os.environ["WF_REQUIRE_CALL_CONTEXT"] = "1"
    app = create_app()
    with TestClient(app) as client:
        # /healthz still passes — probes are bypassed even in production mode.
        assert client.get("/healthz").status_code == 200
        # A non-bypass route without headers would be 401, but WF-IMPL-015
        # has no non-probe routes yet, so we assert the middleware
        # construction picked up the strict flag via attribute inspection.
    installed = next(
        m
        for m in app.user_middleware
        if m.cls is CallContextMiddleware  # type: ignore[comparison-overlap]
    )
    # Starlette stores the kwargs on the wrapper's ``kwargs`` mapping.
    assert installed.kwargs == {"require_call_context": True}


def test_create_app_env_flag_only_truthy_on_exact_1(_clean_env: None) -> None:
    """``true`` / ``yes`` must NOT silently flip into production mode."""
    for non_truthy in ("true", "yes", "TRUE", "1 ", "", "0"):
        os.environ["WF_REQUIRE_CALL_CONTEXT"] = non_truthy
        app = create_app()
        installed = next(
            m
            for m in app.user_middleware
            if m.cls is CallContextMiddleware  # type: ignore[comparison-overlap]
        )
        assert installed.kwargs == {"require_call_context": False}, (
            f"WF_REQUIRE_CALL_CONTEXT={non_truthy!r} must not enable production "
            "mode — only the literal string '1' may flip the gate."
        )


def test_explicit_kwarg_overrides_env(_clean_env: None) -> None:
    """The explicit ``require_call_context`` kwarg wins over the env var."""
    os.environ["WF_REQUIRE_CALL_CONTEXT"] = "1"
    app = create_app(require_call_context=False)
    installed = next(
        m
        for m in app.user_middleware
        if m.cls is CallContextMiddleware  # type: ignore[comparison-overlap]
    )
    assert installed.kwargs == {"require_call_context": False}
