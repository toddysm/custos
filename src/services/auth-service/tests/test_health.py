"""Smoke tests for the AS-IMPL-001 scaffold."""

from __future__ import annotations

from fastapi.testclient import TestClient

from custos_auth import __version__, create_app


def test_create_app_returns_fastapi_instance() -> None:
    app = create_app()
    assert app.title == "Custos Auth Service"
    assert app.version == __version__


def test_healthz_returns_200() -> None:
    client = TestClient(create_app())
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_readyz_returns_200_in_scaffold() -> None:
    """AS-IMPL-001 scaffold: /readyz always returns 200.

    AS-IMPL-004 will gate this on the SPL schema-revision check; AS-IMPL-018
    will additionally report JWKS-rotation health. Update this test then.
    """
    client = TestClient(create_app())
    response = client.get("/readyz")
    assert response.status_code == 200
    assert response.json() == {"status": "ready"}


def test_openapi_schema_renders() -> None:
    client = TestClient(create_app())
    response = client.get("/openapi.json")
    assert response.status_code == 200
    schema = response.json()
    assert schema["info"]["title"] == "Custos Auth Service"
    assert schema["info"]["version"] == __version__
