"""Store-adapter CRUD round-trips against the in-memory backend (TS-IMPL-008)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from custos_spl.errors import ImmutableViolation

from custos_trigger.models import (
    ResumeRegistration,
    SourceType,
    Subscription,
    SubscriptionKind,
    SubscriptionState,
)
from custos_trigger.providers import InMemoryTriggerMetadataStore
from custos_trigger.stores import (
    ResumeSubscriptionStore,
    ScheduleStore,
    SubscriptionStore,
)

# Mirrors the frozen clock the ``metadata_store`` fixture installs (conftest).
FIXED_NOW = datetime(2026, 6, 4, 12, 0, 0, tzinfo=UTC)
_CREATED = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)


def _subscription(**overrides: object) -> Subscription:
    base: dict[str, object] = {
        "workspace_id": "ws-1",
        "subscription_id": "sub-1",
        "source_type": SourceType.WEBHOOK,
        "workflow_id": "wf-1",
        "target_workflow_version_id": "wfv-1",
        "selector": "event.kind == 'github.push'",
        "input_mapping": {"ref": "${{ event.data.ref }}"},
        "created_at": _CREATED,
        "updated_at": _CREATED,
    }
    base.update(overrides)
    return Subscription(**base)  # type: ignore[arg-type]


# --- SubscriptionStore -------------------------------------------------------


async def test_create_subscription_round_trips(
    metadata_store: InMemoryTriggerMetadataStore,
) -> None:
    store = SubscriptionStore(metadata_store, now=lambda: FIXED_NOW)
    subscription = _subscription()

    result = await store.create(subscription)

    assert result == subscription
    # The minimal base row + the first selector revision are persisted.
    assert metadata_store.subscription("ws-1", "sub-1") is not None
    selectors = metadata_store.subscription_selectors("ws-1", "sub-1")
    assert len(selectors) == 1
    assert selectors[0].added_at == FIXED_NOW
    assert selectors[0].selector["value"] == "event.kind == 'github.push'"


async def test_create_subscription_is_immutable(
    metadata_store: InMemoryTriggerMetadataStore,
) -> None:
    store = SubscriptionStore(metadata_store, now=lambda: FIXED_NOW)
    await store.create(_subscription())

    with pytest.raises(ImmutableViolation):
        await store.create(_subscription())


async def test_create_subscription_defaults_clock_to_utcnow() -> None:
    # Exercise the default ``now`` branch (no injected clock): the persisted
    # selector revision's ``added_at`` must be stamped from the wall clock.
    backend = InMemoryTriggerMetadataStore()
    store = SubscriptionStore(backend)
    before = datetime.now(UTC)

    result = await store.create(_subscription(selector=None, input_mapping={}))

    after = datetime.now(UTC)
    assert result.selector is None
    assert result.created_at == _CREATED
    selectors = backend.subscription_selectors("ws-1", "sub-1")
    assert len(selectors) == 1
    assert before <= selectors[0].added_at <= after


async def test_set_state_transitions(
    metadata_store: InMemoryTriggerMetadataStore,
) -> None:
    store = SubscriptionStore(metadata_store, now=lambda: FIXED_NOW)
    await store.create(_subscription())

    result = await store.set_state("ws-1", "sub-1", SubscriptionState.PAUSED)

    assert result.state is SubscriptionState.PAUSED
    assert result.workflow_id == "wf-1"
    assert result.updated_at == FIXED_NOW
    assert metadata_store.subscription("ws-1", "sub-1").state == "paused"  # type: ignore[union-attr]


async def test_set_state_unknown_subscription_raises(
    metadata_store: InMemoryTriggerMetadataStore,
) -> None:
    store = SubscriptionStore(metadata_store)

    with pytest.raises(ValueError, match="unknown subscription"):
        await store.set_state("ws-1", "missing", SubscriptionState.EXPIRED)


async def test_reauthor_selector_appends_revision(
    metadata_store: InMemoryTriggerMetadataStore,
) -> None:
    store = SubscriptionStore(metadata_store, now=lambda: FIXED_NOW)
    await store.create(_subscription())

    await store.reauthor_selector(_subscription(selector="event.kind == 'gitlab.push'"))

    selectors = metadata_store.subscription_selectors("ws-1", "sub-1")
    assert len(selectors) == 2
    assert selectors[1].selector["value"] == "event.kind == 'gitlab.push'"


# --- ResumeSubscriptionStore -------------------------------------------------


async def test_register_resume_round_trips(
    metadata_store: InMemoryTriggerMetadataStore,
) -> None:
    store = ResumeSubscriptionStore(metadata_store)
    registration = ResumeRegistration(
        run_id="run-1",
        step_id="step-1",
        event_key="approval-42",
        selector="event.data.approved == true",
    )
    expires_at = FIXED_NOW + timedelta(days=7)

    result = await store.register(
        registration,
        workspace_id="ws-1",
        resume_id="resume-1",
        expires_at=expires_at,
    )

    assert result == registration
    row = metadata_store.resume_subscription("ws-1", "resume-1")
    assert row is not None
    assert row.expires_at == expires_at
    assert row.payload["eventKey"] == "approval-42"


async def test_cancel_resume_removes_row(
    metadata_store: InMemoryTriggerMetadataStore,
) -> None:
    store = ResumeSubscriptionStore(metadata_store)
    await store.register(
        ResumeRegistration(run_id="run-1", step_id="step-1", event_key="k"),
        workspace_id="ws-1",
        resume_id="resume-1",
        expires_at=FIXED_NOW,
    )

    await store.cancel("ws-1", "resume-1")

    assert metadata_store.resume_subscription("ws-1", "resume-1") is None


async def test_cancel_resume_is_idempotent(
    metadata_store: InMemoryTriggerMetadataStore,
) -> None:
    store = ResumeSubscriptionStore(metadata_store)
    # Cancelling an absent token must not raise.
    await store.cancel("ws-1", "never-registered")


# --- ScheduleStore -----------------------------------------------------------


async def test_put_schedule_round_trips(
    metadata_store: InMemoryTriggerMetadataStore,
) -> None:
    store = ScheduleStore(metadata_store)
    next_fire = FIXED_NOW + timedelta(hours=1)

    result = await store.put(
        workspace_id="ws-1",
        schedule_id="sched-1",
        workflow_id="wf-1",
        cron="0 * * * *",
        next_fire_at=next_fire,
    )

    assert result.cron == "0 * * * *"
    assert result.enabled is True
    stored = metadata_store.schedule("ws-1", "sched-1")
    assert stored is not None
    assert stored.next_fire_at == next_fire


async def test_kind_defaults_to_start(
    metadata_store: InMemoryTriggerMetadataStore,
) -> None:
    store = SubscriptionStore(metadata_store, now=lambda: FIXED_NOW)
    result = await store.create(_subscription())
    assert result.kind is SubscriptionKind.START
