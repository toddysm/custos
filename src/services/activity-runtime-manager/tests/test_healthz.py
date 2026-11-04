"""Tests for the ``/healthz`` and ``/readyz`` endpoints (ARM-IMPL-001)."""

from __future__ import annotations

from dataclasses import replace

import pytest
from fastapi.testclient import TestClient

from custos_arm import create_app
from custos_arm.config import Settings
from tests.test_rpc_schedule_activity import (
    _body,
    _callctx_header,
    _RecordingScheduler,
    _success_envelope,
)


def test_healthz_is_always_200() -> None:
    app = create_app()
    with TestClient(app) as client:
        response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.parametrize("environment", ["development", "production"])
@pytest.mark.parametrize("scheduler", [None, object()])
def test_unwired_scheduler_never_reports_ready(
    settings: Settings, environment: str, scheduler: object, caplog: pytest.LogCaptureFixture
) -> None:
    """Starting the actual factory is not proof that it can schedule work."""
    app = create_app(
        replace(
            settings,
            environment=environment,
            authz_endpoint="http://auth" if environment == "production" else "",
        )
    )
    app.state.scheduler = scheduler
    with TestClient(app) as client:
        response = client.get("/readyz")
        assert client.get("/healthz").status_code == 200
    assert response.status_code == 503
    assert response.json() == {
        "status": "not_ready",
        "detail": "activity scheduler is not configured",
    }
    assert "activity scheduler is not configured" in caplog.text


def test_configured_scheduler_opens_readiness_only_during_lifespan() -> None:
    """A supplied scheduler follows the readiness lifecycle, including shutdown."""
    app = create_app()
    scheduler = _RecordingScheduler(result=_success_envelope())
    app.state.scheduler = scheduler
    assert app.state.ready is False
    with TestClient(app) as client:
        response = client.get("/readyz")
        assert response.status_code == 200
        assert response.json() == {"status": "ready"}
        scheduled = client.post(
            "/ScheduleActivity", json=_body(), headers={"x-custos-callctx": _callctx_header()}
        )
        assert scheduled.status_code == 200
        assert len(scheduler.scheduled) == 1
    assert app.state.ready is False
    assert app.state.ready_detail == "activity-runtime-manager is shutting down"


def test_ready_flag_cannot_hide_a_lost_scheduler() -> None:
    """Readiness also checks the actual dispatcher rather than trusting a stale boolean."""
    app = create_app()
    app.state.scheduler = _RecordingScheduler(result=_success_envelope())
    with TestClient(app) as client:
        assert client.get("/readyz").status_code == 200
        app.state.scheduler = None
        response = client.get("/readyz")
    assert response.status_code == 503
    assert response.json()["detail"] == "activity scheduler is not configured"


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
