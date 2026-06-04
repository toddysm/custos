"""Tests for the inbound ScheduleActivity RPC handler (ARM-IMPL-018)."""

from __future__ import annotations

import json
from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient

from custos_arm import create_app
from custos_arm.config import Settings
from custos_arm.result import ActivityResultEnvelope
from custos_arm.scheduler import ActivityScheduler, CancelOutcome, ScheduleRequest


class _RecordingScheduler(ActivityScheduler):
    """A stand-in scheduler that records dispatches and returns canned results.

    Overrides ``__init__`` so it stays a genuine :class:`ActivityScheduler`
    subclass (satisfying the adapter's ``isinstance`` guard) without building
    the heavy real dependency graph.
    """

    def __init__(
        self,
        *,
        result: ActivityResultEnvelope | None = None,
        cancel_outcome: CancelOutcome = CancelOutcome.ACCEPTED,
    ) -> None:
        self._result = result
        self._cancel_outcome = cancel_outcome
        self.scheduled: list[ScheduleRequest] = []
        self.cancelled: list[tuple[str, str, str]] = []

    async def schedule(self, request: ScheduleRequest) -> ActivityResultEnvelope:
        self.scheduled.append(request)
        assert self._result is not None
        return self._result

    async def cancel(self, *, workspace_id: str, run_id: str, step_id: str) -> CancelOutcome:
        self.cancelled.append((workspace_id, run_id, step_id))
        return self._cancel_outcome


def _success_envelope() -> ActivityResultEnvelope:
    return ActivityResultEnvelope.model_validate(
        {"class": "success", "attempt": 1, "outputs": {"ok": True}}
    )


def _callctx_header(workspace_id: str = "ws-1", principal_id: str = "p-1") -> str:
    return json.dumps({"workspace_id": workspace_id, "principal_id": principal_id})


def _client(settings: Settings, scheduler: ActivityScheduler | None) -> TestClient:
    app: FastAPI = create_app(settings)
    app.state.scheduler = scheduler
    return TestClient(app)


def _body(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "runId": "run-1",
        "stepId": "step-1",
        "attempt": 1,
        "activityRef": "acme/echo@1.0.0",
        "inputs": {"message": "hi"},
    }
    base.update(overrides)
    return base


def test_schedule_activity_dispatches_and_returns_envelope(settings: Settings) -> None:
    scheduler = _RecordingScheduler(result=_success_envelope())
    client = _client(settings, scheduler)

    response = client.post(
        "/ScheduleActivity",
        json=_body(),
        headers={"x-custos-callctx": _callctx_header()},
    )

    assert response.status_code == 200
    assert response.json() == {
        "class": "success",
        "attempt": 1,
        "outputs": {"ok": True},
        "error": None,
    }
    assert len(scheduler.scheduled) == 1
    request = scheduler.scheduled[0]
    assert request.workspace_id == "ws-1"  # taken from the call context, not the body
    assert request.step.run_id == "run-1"
    assert request.step.step_id == "step-1"
    assert request.activity_ref == "acme/echo@1.0.0"
    assert request.inputs == {"message": "hi"}


def test_schedule_activity_translates_connector_contexts(settings: Settings) -> None:
    scheduler = _RecordingScheduler(result=_success_envelope())
    client = _client(settings, scheduler)

    body = _body(
        connectorContexts={
            "registry": {
                "slotName": "registry",
                "handle": "lease-abc",
                "connectorKind": "oci-registry",
            }
        }
    )
    response = client.post(
        "/ScheduleActivity",
        json=body,
        headers={"x-custos-callctx": _callctx_header()},
    )

    assert response.status_code == 200
    contexts = scheduler.scheduled[0].connector_contexts
    assert len(contexts) == 1
    ctx = contexts[0]
    assert ctx.slot_name == "registry"
    assert ctx.connector_type == "oci-registry"
    assert ctx.connector_instance_id == "lease-abc"
    assert ctx.lease_id == "lease-abc"
    assert dict(ctx.secrets) == {}


def test_schedule_activity_idempotency_key_match_is_accepted(settings: Settings) -> None:
    scheduler = _RecordingScheduler(result=_success_envelope())
    client = _client(settings, scheduler)

    response = client.post(
        "/ScheduleActivity",
        json=_body(),
        headers={
            "x-custos-callctx": _callctx_header(),
            "Idempotency-Key": "run-1|step-1|1",
        },
    )

    assert response.status_code == 200


def test_schedule_activity_idempotency_key_mismatch_is_rejected(settings: Settings) -> None:
    scheduler = _RecordingScheduler(result=_success_envelope())
    client = _client(settings, scheduler)

    response = client.post(
        "/ScheduleActivity",
        json=_body(),
        headers={
            "x-custos-callctx": _callctx_header(),
            "Idempotency-Key": "run-1|step-1|9",
        },
    )

    assert response.status_code == 400
    assert scheduler.scheduled == []


def test_schedule_activity_missing_callctx_is_unauthorized(settings: Settings) -> None:
    scheduler = _RecordingScheduler(result=_success_envelope())
    client = _client(settings, scheduler)

    response = client.post("/ScheduleActivity", json=_body())

    assert response.status_code == 401
    assert scheduler.scheduled == []


def test_schedule_activity_invalid_body_is_unprocessable(settings: Settings) -> None:
    scheduler = _RecordingScheduler(result=_success_envelope())
    client = _client(settings, scheduler)

    body = _body()
    del body["activityRef"]
    response = client.post(
        "/ScheduleActivity",
        json=body,
        headers={"x-custos-callctx": _callctx_header()},
    )

    assert response.status_code == 422
    assert scheduler.scheduled == []


def test_schedule_activity_returns_503_when_scheduler_unset(settings: Settings) -> None:
    client = _client(settings, None)

    response = client.post(
        "/ScheduleActivity",
        json=_body(),
        headers={"x-custos-callctx": _callctx_header()},
    )

    assert response.status_code == 503
