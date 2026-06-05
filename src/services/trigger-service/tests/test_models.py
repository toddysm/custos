"""Tests for the subscription wire/domain models + SPL mapping (TS-IMPL-007)."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from custos_spl.ids import SubscriptionId, WorkflowId, WorkspaceId
from custos_spl.interfaces.metadata_store import (
    DedupKey as SplDedupKey,
)
from custos_spl.interfaces.metadata_store import (
    ResumeSubscription as SplResumeSubscription,
)
from custos_spl.interfaces.metadata_store import (
    Schedule as SplSchedule,
)
from custos_spl.interfaces.metadata_store import (
    Subscription as SplSubscription,
)
from custos_spl.interfaces.metadata_store import (
    SubscriptionSelector as SplSubscriptionSelector,
)
from pydantic import ValidationError

from custos_trigger.models import (
    ResumeRegistration,
    SelectorMatchType,
    SourceType,
    Subscription,
    SubscriptionCreate,
    SubscriptionKind,
    SubscriptionPatch,
    SubscriptionState,
    build_selector_blob,
    resume_registration_from_spl,
    subscription_from_spl,
    to_spl_dedup_key,
    to_spl_resume_subscription,
    to_spl_schedule,
    to_spl_subscription,
    to_spl_subscription_selector,
)

_CREATED = datetime(2026, 5, 16, 12, 0, 0, tzinfo=UTC)
_UPDATED = datetime(2026, 5, 16, 12, 30, 0, tzinfo=UTC)
_EXPIRES = datetime(2026, 5, 16, 13, 0, 0, tzinfo=UTC)


def _subscription() -> Subscription:
    return Subscription(
        workspaceId="ws-1",
        subscriptionId="sub-1",
        kind=SubscriptionKind.START,
        sourceType=SourceType.VENDOR_PUSH,
        workflowId="wf-1",
        targetWorkflowVersionId="wf-1@7",
        selector='event.kind == "registry.push"',
        inputMapping={"image": "${{ event.subject }}"},
        state=SubscriptionState.ACTIVE,
        createdAt=_CREATED,
        updatedAt=_UPDATED,
    )


# --------------------------------------------------------------------------
# Enum value pins
# --------------------------------------------------------------------------


def test_subscription_kind_values() -> None:
    assert SubscriptionKind.START.value == "start"
    assert SubscriptionKind.RESUME.value == "resume"


def test_source_type_values() -> None:
    assert [member.value for member in SourceType] == [
        "manual",
        "scheduled",
        "webhook",
        "vendor-push",
        "pull",
        "internal",
    ]


def test_subscription_state_values() -> None:
    assert [member.value for member in SubscriptionState] == ["active", "paused", "expired"]


def test_selector_match_type_values() -> None:
    assert [member.value for member in SelectorMatchType] == [
        "cel",
        "eq",
        "prefix",
        "regex",
        "jsonpath",
    ]


# --------------------------------------------------------------------------
# Wire-model JSON round-trips
# --------------------------------------------------------------------------


def test_subscription_create_round_trips() -> None:
    create = SubscriptionCreate(
        sourceType=SourceType.MANUAL,
        workflowId="wf-1",
        selector='event.kind == "manual.fire"',
        inputMapping={"k": "v"},
    )
    restored = SubscriptionCreate.model_validate_json(create.model_dump_json(by_alias=True))
    assert restored == create


def test_subscription_create_camel_case_aliases() -> None:
    dumped = SubscriptionCreate(sourceType=SourceType.MANUAL, workflowId="wf-1").model_dump(
        by_alias=True
    )
    assert "sourceType" in dumped
    assert "workflowId" in dumped
    assert "inputMapping" in dumped


def test_subscription_patch_round_trips_partial() -> None:
    patch = SubscriptionPatch(state=SubscriptionState.PAUSED)
    restored = SubscriptionPatch.model_validate_json(patch.model_dump_json(by_alias=True))
    assert restored == patch
    assert restored.selector is None
    assert restored.input_mapping is None


def test_subscription_round_trips() -> None:
    sub = _subscription()
    restored = Subscription.model_validate_json(sub.model_dump_json(by_alias=True))
    assert restored == sub


def test_resume_registration_round_trips() -> None:
    reg = ResumeRegistration(
        runId="run-1",
        stepId="step-1",
        eventKey="approval",
        selector='event.kind == "manual.fire"',
    )
    restored = ResumeRegistration.model_validate_json(reg.model_dump_json(by_alias=True))
    assert restored == reg


# --------------------------------------------------------------------------
# Validation
# --------------------------------------------------------------------------


def test_invalid_source_type_rejected() -> None:
    with pytest.raises(ValidationError):
        SubscriptionCreate(sourceType="bogus", workflowId="wf-1")  # type: ignore[arg-type]


def test_empty_workflow_id_rejected() -> None:
    with pytest.raises(ValidationError):
        SubscriptionCreate(sourceType=SourceType.MANUAL, workflowId="")


def test_extra_field_forbidden() -> None:
    with pytest.raises(ValidationError):
        SubscriptionCreate(sourceType=SourceType.MANUAL, workflowId="wf-1", surprise=1)  # type: ignore[call-arg]


# --------------------------------------------------------------------------
# SPL mapping — subscription
# --------------------------------------------------------------------------


def test_build_selector_blob_shape() -> None:
    blob = build_selector_blob(_subscription())
    assert blob["matchType"] == SelectorMatchType.CEL.value
    assert blob["fieldPath"] == ""
    assert blob["value"] == 'event.kind == "registry.push"'
    assert blob["kind"] == "start"
    assert blob["sourceType"] == "vendor-push"
    assert blob["targetWorkflowVersionId"] == "wf-1@7"
    assert blob["inputMapping"] == {"image": "${{ event.subject }}"}


def test_to_spl_subscription_maps_minimal_row() -> None:
    spl = to_spl_subscription(_subscription())
    assert isinstance(spl, SplSubscription)
    assert spl.workspace_id == "ws-1"
    assert spl.subscription_id == "sub-1"
    assert spl.workflow_id == "wf-1"
    assert spl.state == "active"
    assert spl.created_at == _CREATED
    assert spl.updated_at == _UPDATED


def test_to_spl_subscription_selector_row() -> None:
    added = datetime(2026, 5, 16, 12, 5, 0, tzinfo=UTC)
    spl = to_spl_subscription_selector(_subscription(), added_at=added)
    assert isinstance(spl, SplSubscriptionSelector)
    assert spl.workspace_id == "ws-1"
    assert spl.subscription_id == "sub-1"
    assert spl.added_at == added
    assert spl.selector["matchType"] == "cel"


def test_subscription_spl_round_trips() -> None:
    sub = _subscription()
    row = to_spl_subscription(sub)
    selector = to_spl_subscription_selector(sub, added_at=_CREATED)
    assert subscription_from_spl(row, selector) == sub


def test_subscription_from_spl_without_selector_uses_defaults() -> None:
    row = SplSubscription(
        workspace_id=WorkspaceId("ws-9"),
        subscription_id=SubscriptionId("sub-9"),
        workflow_id=WorkflowId("wf-9"),
        state="paused",
        created_at=_CREATED,
        updated_at=_UPDATED,
    )
    sub = subscription_from_spl(row, None)
    assert sub.kind is SubscriptionKind.START
    assert sub.source_type is SourceType.MANUAL
    assert sub.selector is None
    assert sub.input_mapping == {}
    assert sub.state is SubscriptionState.PAUSED


# --------------------------------------------------------------------------
# SPL mapping — resume / dedup / schedule
# --------------------------------------------------------------------------


def test_to_spl_resume_subscription_payload() -> None:
    reg = ResumeRegistration(
        runId="run-1", stepId="step-1", eventKey="approval", selector="event.subject == 'x'"
    )
    spl = to_spl_resume_subscription(
        reg, workspace_id="ws-1", resume_id="res-1", expires_at=_EXPIRES
    )
    assert isinstance(spl, SplResumeSubscription)
    assert spl.run_id == "run-1"
    assert spl.step_id == "step-1"
    assert spl.expires_at == _EXPIRES
    assert spl.payload == {"eventKey": "approval", "selector": "event.subject == 'x'"}


def test_resume_registration_spl_round_trips() -> None:
    reg = ResumeRegistration(runId="run-2", stepId="step-2", eventKey="k", selector=None)
    spl = to_spl_resume_subscription(
        reg, workspace_id="ws-1", resume_id="res-2", expires_at=_EXPIRES
    )
    assert resume_registration_from_spl(spl) == reg


def test_to_spl_dedup_key() -> None:
    spl = to_spl_dedup_key(workspace_id="ws-1", key="hash-1", expires_at=_EXPIRES)
    assert isinstance(spl, SplDedupKey)
    assert spl.workspace_id == "ws-1"
    assert spl.key == "hash-1"
    assert spl.expires_at == _EXPIRES


def test_to_spl_schedule() -> None:
    fire = datetime(2026, 5, 16, 18, 0, 0, tzinfo=UTC)
    spl = to_spl_schedule(
        workspace_id="ws-1",
        schedule_id="sch-1",
        workflow_id="wf-1",
        cron="0 */6 * * *",
        next_fire_at=fire,
    )
    assert isinstance(spl, SplSchedule)
    assert spl.cron == "0 */6 * * *"
    assert spl.next_fire_at == fire
    assert spl.enabled is True
