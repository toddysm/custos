"""Tests for the inbound CancelActivity RPC handler (ARM-IMPL-018)."""

from __future__ import annotations

import json

from fastapi import FastAPI
from fastapi.testclient import TestClient

from custos_arm import create_app
from custos_arm.config import Settings
from custos_arm.result import ActivityResultEnvelope
from custos_arm.scheduler import ActivityScheduler, CancelOutcome, ScheduleRequest


class _CancelScheduler(ActivityScheduler):
    """A stand-in scheduler that records cancellations and returns a canned outcome."""

    def __init__(self, outcome: CancelOutcome) -> None:
        self._outcome = outcome
        self.cancelled: list[tuple[str, str, str]] = []

    async def schedule(self, request: ScheduleRequest) -> ActivityResultEnvelope:
        raise AssertionError("schedule should not be called by cancel tests")

    async def cancel(self, *, workspace_id: str, run_id: str, step_id: str) -> CancelOutcome:
        self.cancelled.append((workspace_id, run_id, step_id))
        return self._outcome


def _callctx_header(workspace_id: str = "ws-1", principal_id: str = "p-1") -> str:
    return json.dumps({"workspace_id": workspace_id, "principal_id": principal_id})


def _client(settings: Settings, scheduler: ActivityScheduler | None) -> TestClient:
    app: FastAPI = create_app(settings)
    app.state.scheduler = scheduler
    return TestClient(app)


def test_cancel_activity_accepted_returns_204(settings: Settings) -> None:
    scheduler = _CancelScheduler(CancelOutcome.ACCEPTED)
    client = _client(settings, scheduler)

    response = client.post(
        "/CancelActivity",
        json={"runId": "run-1", "stepId": "step-1"},
        headers={"x-custos-callctx": _callctx_header()},
    )

    assert response.status_code == 204
    assert scheduler.cancelled == [("ws-1", "run-1", "step-1")]


def test_cancel_activity_unknown_returns_404(settings: Settings) -> None:
    scheduler = _CancelScheduler(CancelOutcome.UNKNOWN)
    client = _client(settings, scheduler)

    response = client.post(
        "/CancelActivity",
        json={"runId": "run-1", "stepId": "step-1"},
        headers={"x-custos-callctx": _callctx_header()},
    )

    assert response.status_code == 404


def test_cancel_activity_terminated_returns_409(settings: Settings) -> None:
    scheduler = _CancelScheduler(CancelOutcome.TERMINATED)
    client = _client(settings, scheduler)

    response = client.post(
        "/CancelActivity",
        json={"runId": "run-1", "stepId": "step-1"},
        headers={"x-custos-callctx": _callctx_header()},
    )

    assert response.status_code == 409


def test_cancel_activity_missing_callctx_is_unauthorized(settings: Settings) -> None:
    scheduler = _CancelScheduler(CancelOutcome.ACCEPTED)
    client = _client(settings, scheduler)

    response = client.post(
        "/CancelActivity",
        json={"runId": "run-1", "stepId": "step-1"},
    )

    assert response.status_code == 401
    assert scheduler.cancelled == []


def test_cancel_activity_invalid_body_is_unprocessable(settings: Settings) -> None:
    scheduler = _CancelScheduler(CancelOutcome.ACCEPTED)
    client = _client(settings, scheduler)

    response = client.post(
        "/CancelActivity",
        json={"runId": "run-1"},
        headers={"x-custos-callctx": _callctx_header()},
    )

    assert response.status_code == 422
    assert scheduler.cancelled == []


def test_cancel_activity_returns_503_when_scheduler_unset(settings: Settings) -> None:
    client = _client(settings, None)

    response = client.post(
        "/CancelActivity",
        json={"runId": "run-1", "stepId": "step-1"},
        headers={"x-custos-callctx": _callctx_header()},
    )

    assert response.status_code == 503
