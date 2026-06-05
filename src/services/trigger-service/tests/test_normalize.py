"""Event normalizer tests (TS-IMPL-010)."""

from __future__ import annotations

import json

import pytest

from custos_trigger.models import SourceType
from custos_trigger.normalize import (
    MANUAL_FIRE_KIND,
    EventNormalizationError,
    generate_event_id,
    normalize_manual_fire,
    normalize_workflow_event,
    workflow_kind_from_status,
)
from custos_trigger.taxonomy import InvalidKindError

_OCCURRED_AT = "2026-06-04T12:00:00Z"


# --- generate_event_id -------------------------------------------------------


def test_generate_event_id_is_deterministic() -> None:
    first = generate_event_id("manual", "sub-1", _OCCURRED_AT)
    second = generate_event_id("manual", "sub-1", _OCCURRED_AT)
    assert first == second


def test_generate_event_id_varies_by_parts() -> None:
    base = generate_event_id("manual", "sub-1", _OCCURRED_AT)
    assert generate_event_id("manual", "sub-2", _OCCURRED_AT) != base


def test_generate_event_id_no_delimiter_collision() -> None:
    # NUL-joining keeps these two part splits distinct.
    assert generate_event_id("a", "bc") != generate_event_id("ab", "c")


# --- workflow_kind_from_status ----------------------------------------------


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        ("queued", "workflow.started"),
        ("running", "workflow.started"),
        ("started", "workflow.started"),
        ("succeeded", "workflow.completed"),
        ("completed", "workflow.completed"),
        ("failed", "workflow.failed"),
        ("cancelled", "workflow.cancelled"),
    ],
)
def test_workflow_kind_from_status(status: str, expected: str) -> None:
    assert workflow_kind_from_status(status) == expected


def test_workflow_kind_from_status_run_domain() -> None:
    assert workflow_kind_from_status("succeeded", domain="run") == "run.completed"


def test_workflow_kind_from_status_unknown_raises_taxonomy_error() -> None:
    with pytest.raises(InvalidKindError):
        workflow_kind_from_status("paused")


# --- normalize_manual_fire ---------------------------------------------------


def test_normalize_manual_fire_minimal() -> None:
    event = normalize_manual_fire(occurred_at=_OCCURRED_AT)

    assert event.kind == MANUAL_FIRE_KIND
    assert event.source.type is SourceType.MANUAL
    assert event.source.occurred_at == _OCCURRED_AT
    assert event.data == {}
    assert event.event_id  # generated


def test_normalize_manual_fire_carries_inputs_and_subscription() -> None:
    event = normalize_manual_fire(
        occurred_at=_OCCURRED_AT,
        subscription_id="sub-1",
        inputs={"ref": "main", "force": True},
    )

    assert event.source.subscription_id == "sub-1"
    assert event.subject == "sub-1"
    assert event.data == {"ref": "main", "force": True}
    assert json.loads(event.raw.body) == {"ref": "main", "force": True}


def test_normalize_manual_fire_deterministic_event_id() -> None:
    first = normalize_manual_fire(
        occurred_at=_OCCURRED_AT, subscription_id="sub-1", inputs={"a": 1}
    )
    second = normalize_manual_fire(
        occurred_at=_OCCURRED_AT, subscription_id="sub-1", inputs={"a": 1}
    )
    assert first.event_id == second.event_id


def test_normalize_manual_fire_honors_explicit_event_id() -> None:
    event = normalize_manual_fire(occurred_at=_OCCURRED_AT, event_id="evt-supplied")
    assert event.event_id == "evt-supplied"


def test_normalize_manual_fire_empty_occurred_at_raises() -> None:
    with pytest.raises(EventNormalizationError):
        normalize_manual_fire(occurred_at="")


# --- normalize_workflow_event ------------------------------------------------


def _envelope(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "workflowVersionId": "wfv-1",
        "runId": "run-1",
        "workspace": "ws-1",
        "occurredAt": _OCCURRED_AT,
        "status": "succeeded",
    }
    base.update(overrides)
    return base


def test_normalize_workflow_event_maps_status_to_kind() -> None:
    event = normalize_workflow_event(_envelope())

    assert event.kind == "workflow.completed"
    assert event.source.type is SourceType.INTERNAL
    assert event.subject == "run-1"
    assert event.data["workflowVersionId"] == "wfv-1"
    assert event.data["status"] == "succeeded"
    assert event.source.occurred_at == _OCCURRED_AT


def test_normalize_workflow_event_prefers_explicit_kind() -> None:
    event = normalize_workflow_event(_envelope(kind="workflow.failed", status="succeeded"))
    # Explicit, taxonomy-valid kind wins over status-derived.
    assert event.kind == "workflow.failed"


def test_normalize_workflow_event_carries_outputs_and_step() -> None:
    event = normalize_workflow_event(_envelope(outputs={"digest": "sha256:abc"}, stepId="step-7"))
    assert event.data["outputs"] == {"digest": "sha256:abc"}
    assert event.data["stepId"] == "step-7"


def test_normalize_workflow_event_deterministic_event_id() -> None:
    first = normalize_workflow_event(_envelope())
    second = normalize_workflow_event(_envelope())
    assert first.event_id == second.event_id


def test_normalize_workflow_event_honors_explicit_event_id() -> None:
    event = normalize_workflow_event(_envelope(), event_id="evt-x")
    assert event.event_id == "evt-x"


def test_normalize_workflow_event_run_domain() -> None:
    event = normalize_workflow_event(_envelope(status="failed"), domain="run")
    assert event.kind == "run.failed"


def test_normalize_workflow_event_unknown_status_raises_taxonomy_error() -> None:
    with pytest.raises(InvalidKindError):
        normalize_workflow_event(_envelope(status="paused"))


def test_normalize_workflow_event_invalid_explicit_kind_raises() -> None:
    with pytest.raises(InvalidKindError):
        normalize_workflow_event(_envelope(kind="workflow.bogus", status=None))


@pytest.mark.parametrize("missing", ["occurredAt", "runId", "workflowVersionId"])
def test_normalize_workflow_event_missing_required_field_raises(missing: str) -> None:
    envelope = _envelope()
    del envelope[missing]
    with pytest.raises(EventNormalizationError):
        normalize_workflow_event(envelope)


def test_normalize_workflow_event_requires_kind_or_status() -> None:
    envelope = _envelope()
    del envelope["status"]
    with pytest.raises(EventNormalizationError):
        normalize_workflow_event(envelope)


def test_normalize_workflow_event_raw_body_roundtrips_envelope() -> None:
    envelope = _envelope()
    event = normalize_workflow_event(envelope)
    assert json.loads(event.raw.body) == envelope
