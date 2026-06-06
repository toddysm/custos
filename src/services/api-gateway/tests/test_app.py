"""Tests for the app factory + health probes (AGW-IMPL-002)."""

from __future__ import annotations

from fastapi.testclient import TestClient

from custos_gateway.app import create_app
from custos_gateway.settings import Settings


def test_healthz_always_ok(settings: Settings) -> None:
    app = create_app(settings=settings)
    # No lifespan entered: app.state.ready is False, but liveness is independent.
    with TestClient(app, raise_server_exceptions=True) as client:
        # Inside the context the lifespan has run, so probe liveness directly.
        response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_readyz_503_before_ready_then_200_after(settings: Settings) -> None:
    app = create_app(settings=settings)

    # Before the lifespan runs the readiness gate is closed.
    assert app.state.ready is False
    pre_client = TestClient(app)  # does not enter lifespan
    pre = pre_client.get("/readyz")
    assert pre.status_code == 503
    assert pre.json()["status"] == "not_ready"

    # Entering the lifespan flips the gate to ready.
    with TestClient(app) as client:
        post = client.get("/readyz")
    assert post.status_code == 200
    assert post.json() == {"status": "ready"}


def test_settings_attached_to_app_state(settings: Settings) -> None:
    app = create_app(settings=settings)
    assert app.state.settings is settings
