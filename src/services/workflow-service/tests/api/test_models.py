"""Round-trip + alias-generation coverage for ``custos_workflow.api.models``.

Locks the wire shape of every WF-IMPL-062 request / response model:

* ``StartRunRequest`` / ``StartRunResponse`` / ``RunRefResponse``
* ``RunResponse`` + ``StepResponse`` + ``StepAttemptSummary``
* ``CancelRunRequest``
* ``RaiseExternalEventRequest``
* ``RunListQuery`` + ``RunListResponse`` (+ ``PageRefResponse``)

Each test exercises three guarantees:

1. ``model_dump_json(by_alias=True)`` produces the wire camelCase shape.
2. ``model_validate(...)`` accepts the wire camelCase shape AND the
   Python snake_case shape (``populate_by_name=True``).
3. ``model_validate({"unknownField": ...})`` raises ``ValidationError``
   on every request model (``extra="forbid"``).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import pytest
from pydantic import ValidationError

from custos_workflow.api.models import (
    MAX_LIST_LIMIT,
    CancelRunRequest,
    PageRefResponse,
    RaiseExternalEventRequest,
    RunListQuery,
    RunListResponse,
    RunRefResponse,
    RunResponse,
    StartRunRequest,
    StartRunResponse,
    StepAttemptSummary,
    StepResponse,
)
from custos_workflow.runs.model import RunStatus

# ---------------------------------------------------------------------------
# StartRunRequest
# ---------------------------------------------------------------------------


class TestStartRunRequest:
    def test_wire_camel_case_round_trip(self) -> None:
        wire = {
            "workflowVersionId": "wfv-abc",
            "inputs": {"name": "alice", "count": 3},
            "idempotencyKey": "key-1",
        }
        req = StartRunRequest.model_validate(wire)
        assert req.workflow_version_id == "wfv-abc"
        assert req.inputs == {"name": "alice", "count": 3}
        assert req.idempotency_key == "key-1"
        assert json.loads(req.model_dump_json(by_alias=True)) == wire

    def test_snake_case_alias_accepted(self) -> None:
        # populate_by_name=True allows the Python attr name on the wire too.
        req = StartRunRequest.model_validate({"workflow_version_id": "wfv-1", "inputs": {}})
        assert req.workflow_version_id == "wfv-1"
        assert req.idempotency_key is None

    def test_inputs_defaults_to_empty_dict(self) -> None:
        req = StartRunRequest.model_validate({"workflowVersionId": "wfv-1"})
        assert req.inputs == {}

    def test_workflow_version_id_required(self) -> None:
        with pytest.raises(ValidationError):
            StartRunRequest.model_validate({"inputs": {}})

    def test_workflow_version_id_non_empty(self) -> None:
        with pytest.raises(ValidationError):
            StartRunRequest.model_validate({"workflowVersionId": ""})

    def test_unknown_field_rejected(self) -> None:
        with pytest.raises(ValidationError):
            StartRunRequest.model_validate({"workflowVersionId": "wfv-1", "runID": "evil"})

    def test_str_strip_whitespace_on_workflow_version_id(self) -> None:
        req = StartRunRequest.model_validate({"workflowVersionId": "  wfv-1  "})
        assert req.workflow_version_id == "wfv-1"


# ---------------------------------------------------------------------------
# RunRefResponse / StartRunResponse
# ---------------------------------------------------------------------------


class TestRunRefResponse:
    def _kwargs(self) -> dict[str, Any]:
        return {
            "run_id": "run-1",
            "status": RunStatus.QUEUED,
            "workspace_id": "ws-1",
            "workflow_version_id": "wfv-1",
            "started_at": datetime(2026, 5, 31, 12, 0, tzinfo=UTC),
        }

    def test_camel_case_wire_round_trip(self) -> None:
        ref = RunRefResponse(**self._kwargs())
        wire = json.loads(ref.model_dump_json(by_alias=True))
        assert wire == {
            "runId": "run-1",
            "status": "queued",
            "workspaceId": "ws-1",
            "workflowVersionId": "wfv-1",
            "startedAt": "2026-05-31T12:00:00Z",
        }
        # Round-trip parse.
        parsed = RunRefResponse.model_validate(wire)
        assert parsed == ref

    def test_started_at_optional(self) -> None:
        kwargs = self._kwargs()
        kwargs["started_at"] = None
        ref = RunRefResponse(**kwargs)
        assert ref.started_at is None
        wire = json.loads(ref.model_dump_json(by_alias=True))
        assert wire["startedAt"] is None

    def test_invalid_status_rejected(self) -> None:
        with pytest.raises(ValidationError):
            RunRefResponse.model_validate(
                {
                    "runId": "r",
                    "status": "not-a-real-status",
                    "workspaceId": "ws",
                    "workflowVersionId": "wfv",
                }
            )

    def test_start_run_response_is_run_ref_alias(self) -> None:
        # The implementation plan + design call out StartRunResponse
        # and RunRefResponse as the same wire shape; preserve that
        # equivalence at the class level so downstream code can
        # name-import either.
        assert StartRunResponse is RunRefResponse

    def test_unknown_field_rejected(self) -> None:
        with pytest.raises(ValidationError):
            RunRefResponse.model_validate(
                {
                    "runId": "r",
                    "status": "queued",
                    "workspaceId": "ws",
                    "workflowVersionId": "wfv",
                    "extra": "nope",
                }
            )

    @pytest.mark.parametrize("field", ["runId", "workspaceId", "workflowVersionId"])
    def test_identifier_fields_reject_empty_string(self, field: str) -> None:
        # Lock the min_length=1 constraint added in response to PR #462 review:
        # opaque IDs must be non-empty so clients cannot smuggle invalid
        # references through the public wire surface.
        wire = {
            "runId": "r",
            "status": "queued",
            "workspaceId": "ws",
            "workflowVersionId": "wfv",
        }
        wire[field] = ""
        with pytest.raises(ValidationError):
            RunRefResponse.model_validate(wire)


# ---------------------------------------------------------------------------
# StepAttemptSummary + StepResponse
# ---------------------------------------------------------------------------


class TestStepAttemptSummary:
    def test_round_trip(self) -> None:
        a = StepAttemptSummary(
            attempt=2,
            status="failed",
            started_at=datetime(2026, 5, 31, 12, 0, tzinfo=UTC),
            finished_at=datetime(2026, 5, 31, 12, 1, tzinfo=UTC),
            error="connector timeout",
        )
        wire = json.loads(a.model_dump_json(by_alias=True))
        assert wire == {
            "attempt": 2,
            "status": "failed",
            "startedAt": "2026-05-31T12:00:00Z",
            "finishedAt": "2026-05-31T12:01:00Z",
            "error": "connector timeout",
        }
        assert StepAttemptSummary.model_validate(wire) == a

    def test_attempt_must_be_positive(self) -> None:
        with pytest.raises(ValidationError):
            StepAttemptSummary.model_validate({"attempt": 0, "status": "started"})

    def test_unknown_field_rejected(self) -> None:
        with pytest.raises(ValidationError):
            StepAttemptSummary.model_validate({"attempt": 1, "status": "started", "extra": "no"})


class TestStepResponse:
    def test_round_trip_with_attempts(self) -> None:
        attempt = StepAttemptSummary(attempt=1, status="succeeded")
        step = StepResponse(
            step_id="extract",
            kind="activity",
            status="succeeded",
            attempts=[attempt],
            outputs={"rows": 10},
        )
        wire = json.loads(step.model_dump_json(by_alias=True))
        assert wire == {
            "stepId": "extract",
            "kind": "activity",
            "status": "succeeded",
            "attempts": [
                {
                    "attempt": 1,
                    "status": "succeeded",
                    "startedAt": None,
                    "finishedAt": None,
                    "error": None,
                }
            ],
            "startedAt": None,
            "finishedAt": None,
            "outputs": {"rows": 10},
        }
        assert StepResponse.model_validate(wire) == step

    def test_outputs_optional_for_pending(self) -> None:
        step = StepResponse(step_id="s1", kind="activity", status="pending")
        assert step.outputs is None
        assert step.attempts == []

    def test_step_id_non_empty(self) -> None:
        with pytest.raises(ValidationError):
            StepResponse.model_validate({"stepId": "", "kind": "activity", "status": "pending"})


# ---------------------------------------------------------------------------
# RunResponse
# ---------------------------------------------------------------------------


class TestRunResponse:
    def _run(self) -> RunResponse:
        return RunResponse(
            run_id="run-1",
            status=RunStatus.SUCCEEDED,
            workspace_id="ws-1",
            workflow_version_id="wfv-1",
            reason=None,
            started_at=datetime(2026, 5, 31, 12, 0, tzinfo=UTC),
            updated_at=datetime(2026, 5, 31, 12, 5, tzinfo=UTC),
            inputs={"name": "alice"},
            outputs={"rows": 10},
            steps=[
                StepResponse(
                    step_id="extract",
                    kind="activity",
                    status="succeeded",
                    attempts=[StepAttemptSummary(attempt=1, status="succeeded")],
                )
            ],
        )

    def test_round_trip(self) -> None:
        run = self._run()
        wire = json.loads(run.model_dump_json(by_alias=True))
        assert wire["runId"] == "run-1"
        assert wire["workflowVersionId"] == "wfv-1"
        assert wire["startedAt"] == "2026-05-31T12:00:00Z"
        assert wire["updatedAt"] == "2026-05-31T12:05:00Z"
        assert wire["steps"][0]["stepId"] == "extract"
        assert wire["steps"][0]["attempts"][0]["attempt"] == 1
        assert wire["outputs"] == {"rows": 10}
        assert wire["reason"] is None
        # Parse back.
        assert RunResponse.model_validate(wire) == run

    def test_outputs_optional(self) -> None:
        # Mirror a not-yet-finished run.
        run = RunResponse(
            run_id="r",
            status=RunStatus.RUNNING,
            workspace_id="ws",
            workflow_version_id="wfv",
            started_at=datetime(2026, 5, 31, 12, 0, tzinfo=UTC),
            updated_at=datetime(2026, 5, 31, 12, 0, tzinfo=UTC),
        )
        assert run.outputs is None
        assert run.steps == []
        assert run.inputs == {}

    def test_unknown_field_rejected(self) -> None:
        with pytest.raises(ValidationError):
            RunResponse.model_validate(
                {
                    "runId": "r",
                    "status": "running",
                    "workspaceId": "ws",
                    "workflowVersionId": "wfv",
                    "startedAt": "2026-05-31T12:00:00Z",
                    "updatedAt": "2026-05-31T12:00:00Z",
                    "extra": "no",
                }
            )

    @pytest.mark.parametrize("field", ["runId", "workspaceId", "workflowVersionId"])
    def test_identifier_fields_reject_empty_string(self, field: str) -> None:
        wire = {
            "runId": "r",
            "status": "running",
            "workspaceId": "ws",
            "workflowVersionId": "wfv",
            "startedAt": "2026-05-31T12:00:00Z",
            "updatedAt": "2026-05-31T12:00:00Z",
        }
        wire[field] = ""
        with pytest.raises(ValidationError):
            RunResponse.model_validate(wire)


# ---------------------------------------------------------------------------
# CancelRunRequest
# ---------------------------------------------------------------------------


class TestCancelRunRequest:
    def test_round_trip_with_reason(self) -> None:
        req = CancelRunRequest(reason="operator initiated")
        wire = json.loads(req.model_dump_json(by_alias=True))
        assert wire == {"reason": "operator initiated"}
        assert CancelRunRequest.model_validate(wire) == req

    def test_reason_optional(self) -> None:
        req = CancelRunRequest.model_validate({})
        assert req.reason is None

    def test_reason_length_capped(self) -> None:
        with pytest.raises(ValidationError):
            CancelRunRequest.model_validate({"reason": "x" * 1025})

    def test_unknown_field_rejected(self) -> None:
        with pytest.raises(ValidationError):
            CancelRunRequest.model_validate({"reason": "ok", "extra": "no"})


# ---------------------------------------------------------------------------
# RaiseExternalEventRequest
# ---------------------------------------------------------------------------


class TestRaiseExternalEventRequest:
    def test_camel_case_round_trip(self) -> None:
        wire = {
            "eventName": "approval.granted",
            "payload": {"by": "alice"},
            "idempotencyKey": "evt-1",
        }
        req = RaiseExternalEventRequest.model_validate(wire)
        assert req.event_name == "approval.granted"
        assert req.payload == {"by": "alice"}
        assert req.idempotency_key == "evt-1"
        assert json.loads(req.model_dump_json(by_alias=True)) == wire

    def test_event_name_required(self) -> None:
        with pytest.raises(ValidationError):
            RaiseExternalEventRequest.model_validate({"payload": {}})

    def test_event_name_non_empty(self) -> None:
        with pytest.raises(ValidationError):
            RaiseExternalEventRequest.model_validate({"eventName": ""})

    def test_payload_defaults_to_empty(self) -> None:
        req = RaiseExternalEventRequest.model_validate({"eventName": "x"})
        assert req.payload == {}
        assert req.idempotency_key is None

    def test_unknown_field_rejected(self) -> None:
        with pytest.raises(ValidationError):
            RaiseExternalEventRequest.model_validate({"eventName": "x", "extra": "no"})


# ---------------------------------------------------------------------------
# RunListQuery + RunListResponse + PageRefResponse
# ---------------------------------------------------------------------------


class TestRunListQuery:
    def test_all_fields_optional(self) -> None:
        q = RunListQuery.model_validate({})
        assert q.status is None
        assert q.workflow_version_id is None
        assert q.cursor is None
        assert q.limit is None

    def test_camel_case_round_trip(self) -> None:
        wire = {
            "status": "running",
            "workflowVersionId": "wfv-1",
            "cursor": "opaque-token",
            "limit": 50,
        }
        q = RunListQuery.model_validate(wire)
        assert q.status is RunStatus.RUNNING
        assert q.workflow_version_id == "wfv-1"
        assert q.cursor == "opaque-token"
        assert q.limit == 50
        assert json.loads(q.model_dump_json(by_alias=True)) == wire

    def test_limit_lower_bound(self) -> None:
        with pytest.raises(ValidationError):
            RunListQuery.model_validate({"limit": 0})

    def test_limit_upper_bound(self) -> None:
        with pytest.raises(ValidationError):
            RunListQuery.model_validate({"limit": MAX_LIST_LIMIT + 1})

    def test_invalid_status_rejected(self) -> None:
        with pytest.raises(ValidationError):
            RunListQuery.model_validate({"status": "not-real"})

    def test_unknown_field_rejected(self) -> None:
        with pytest.raises(ValidationError):
            RunListQuery.model_validate({"extra": "no"})

    @pytest.mark.parametrize("field", ["workflowVersionId", "cursor"])
    def test_optional_strings_reject_empty(self, field: str) -> None:
        # min_length=1 was added in response to PR #462 review so callers
        # cannot conflate "unset" with an empty string on either the
        # workflow filter or the opaque cursor.
        with pytest.raises(ValidationError):
            RunListQuery.model_validate({field: ""})


class TestRunListResponse:
    def test_round_trip(self) -> None:
        ref = RunRefResponse(
            run_id="r1",
            status=RunStatus.RUNNING,
            workspace_id="ws",
            workflow_version_id="wfv",
        )
        resp = RunListResponse(items=[ref], next_cursor="next")
        wire = json.loads(resp.model_dump_json(by_alias=True))
        assert wire == {
            "items": [
                {
                    "runId": "r1",
                    "status": "running",
                    "workspaceId": "ws",
                    "workflowVersionId": "wfv",
                    "startedAt": None,
                }
            ],
            "nextCursor": "next",
        }
        assert RunListResponse.model_validate(wire) == resp

    def test_defaults(self) -> None:
        resp = RunListResponse.model_validate({})
        assert resp.items == []
        assert resp.next_cursor is None

    def test_empty_items_with_cursor_is_legal(self) -> None:
        # design.md § Public Interface: "An empty items tuple with
        # a non-None next_cursor is legal — keep paging".
        resp = RunListResponse.model_validate({"items": [], "nextCursor": "more"})
        assert resp.items == []
        assert resp.next_cursor == "more"


class TestPageRefResponse:
    def test_generic_envelope_round_trip(self) -> None:
        page = PageRefResponse(items=[{"x": 1}], next_cursor="c")
        wire = json.loads(page.model_dump_json(by_alias=True))
        assert wire == {"items": [{"x": 1}], "nextCursor": "c"}
        assert PageRefResponse.model_validate(wire) == page

    def test_defaults(self) -> None:
        page = PageRefResponse.model_validate({})
        assert page.items == []
        assert page.next_cursor is None

    def test_next_cursor_rejects_empty_string(self) -> None:
        with pytest.raises(ValidationError):
            PageRefResponse.model_validate({"nextCursor": ""})
