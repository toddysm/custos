"""Tests for the ``/healthz`` and ``/readyz`` endpoints (WF-IMPL-015)."""

from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING

from fastapi.testclient import TestClient

from custos_workflow import create_app

if TYPE_CHECKING:
    from collections.abc import Iterator

    from fastapi import FastAPI


@contextmanager
def _client(**kwargs: bool) -> Iterator[tuple[TestClient, FastAPI]]:
    app = create_app(require_call_context=False, **kwargs)
    with TestClient(app) as tc:
        yield tc, app


def test_healthz_is_always_200() -> None:
    with _client() as (client, _app):
        response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_readyz_is_200_after_lifespan_startup() -> None:
    with _client() as (client, _app):
        # TestClient enters the lifespan on ``__enter__`` so by the time
        # we issue a request, ``app.state.ready`` has been flipped.
        response = client.get("/readyz")
    assert response.status_code == 200
    assert response.json() == {"status": "ready"}


def test_readyz_is_503_before_lifespan_flips_ready() -> None:
    """When the lifespan has not run, ``/readyz`` reports 503 with detail.

    We construct the app, override ``state.ready`` to ``False`` manually,
    and hit ``/readyz`` outside the lifespan to simulate the gate's
    closed-state behaviour. This is the operator-facing failure mode
    the next compiler bootstrap task will exercise for real.
    """
    app = create_app(require_call_context=False)
    app.state.ready = False
    app.state.ready_detail = "compiler bootstrap pending"
    # Use TestClient WITHOUT the ``with`` block so the lifespan does not run.
    client = TestClient(app)
    response = client.get("/readyz")
    assert response.status_code == 503
    assert response.json() == {
        "status": "not_ready",
        "detail": "compiler bootstrap pending",
    }


def test_readyz_503_uses_default_detail_when_unset() -> None:
    """Missing ``ready_detail`` falls back to a stable default string."""
    app = create_app(require_call_context=False)
    app.state.ready = False
    # Explicitly remove the attribute populated by the lifespan default.
    if hasattr(app.state, "ready_detail"):
        delattr(app.state, "ready_detail")
    client = TestClient(app)
    response = client.get("/readyz")
    assert response.status_code == 503
    payload = response.json()
    assert payload["status"] == "not_ready"
    assert payload["detail"] == "workflow-service has not finished startup"


def test_healthz_and_readyz_are_excluded_from_openapi_schema() -> None:
    """Probes are noise on the public OpenAPI surface."""
    with _client() as (client, _app):
        schema = client.get("/openapi.json").json()
    assert "/healthz" not in schema["paths"]
    assert "/readyz" not in schema["paths"]
