"""Tests for the ``/healthz`` and ``/readyz`` endpoints (ARM-IMPL-001)."""

from __future__ import annotations

from fastapi.testclient import TestClient

from custos_arm import create_app


def test_healthz_is_always_200() -> None:
    app = create_app()
    with TestClient(app) as client:
        response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_readyz_is_200_after_lifespan_startup() -> None:
    app = create_app()
    # TestClient enters the lifespan on ``__enter__`` so by the time we
    # issue a request, ``app.state.ready`` has been flipped.
    with TestClient(app) as client:
        response = client.get("/readyz")
    assert response.status_code == 200
    assert response.json() == {"status": "ready"}


def test_readyz_is_503_before_lifespan_flips_ready() -> None:
    """When the lifespan has not run, ``/readyz`` reports 503 with detail."""
    app = create_app()
    app.state.ready = False
    app.state.ready_detail = "resolver warm-up pending"
    # Use TestClient WITHOUT the ``with`` block so the lifespan does not run.
    client = TestClient(app)
    response = client.get("/readyz")
    assert response.status_code == 503
    assert response.json() == {
        "status": "not_ready",
        "detail": "resolver warm-up pending",
    }


def test_readyz_503_uses_default_detail_when_unset() -> None:
    """Missing ``ready_detail`` falls back to a stable default string."""
    app = create_app()
    app.state.ready = False
    if hasattr(app.state, "ready_detail"):
        delattr(app.state, "ready_detail")
    client = TestClient(app)
    response = client.get("/readyz")
    assert response.status_code == 503
    payload = response.json()
    assert payload["status"] == "not_ready"
    assert payload["detail"] == "activity-runtime-manager has not finished startup"


def test_healthz_and_readyz_are_excluded_from_openapi_schema() -> None:
    """Probes are noise on the public OpenAPI surface."""
    app = create_app()
    with TestClient(app) as client:
        schema = client.get("/openapi.json").json()
    assert "/healthz" not in schema["paths"]
    assert "/readyz" not in schema["paths"]
