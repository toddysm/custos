"""Store-adapter CRUD round-trips against the in-memory backend (TS-IMPL-008)."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import cast
from unittest.mock import Mock

import pytest
from custos_spl.errors import ImmutableViolation
from custos_spl.ids import SubscriptionId, WorkspaceId
from custos_spl.interfaces.metadata_store import Subscription as SplSubscription
from fastapi.testclient import TestClient

from custos_trigger.app import create_app
from custos_trigger.models import (
    ResumeRegistration,
    SourceType,
    Subscription,
    SubscriptionKind,
    SubscriptionState,
    to_spl_subscription_selector,
)
from custos_trigger.providers import InMemoryTriggerMetadataStore, Providers
from custos_trigger.source_config import SourceConfig
from custos_trigger.stores import (
    ResumeSubscriptionStore,
    ScheduleStore,
    SubscriptionReadUnsupportedError,
    SubscriptionStore,
)
from custos_trigger.stores.base import TriggerMetadataStore

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


def _legacy_backend(
    backend: InMemoryTriggerMetadataStore, *, reads: bool = True
) -> TriggerMetadataStore:
    """Expose the existing write/read capabilities without the optional atomic mutation API."""
    legacy = Mock(spec=TriggerMetadataStore)
    methods = [
        "put_subscription",
        "update_subscription_state",
        "append_subscription_selector",
        "put_resume_subscription",
        "delete_resume_subscription",
        "put_dedup_key",
        "put_schedule",
        "update_schedule_next_fire",
    ]
    if reads:
        methods += [
            "get_subscription",
            "query_subscriptions",
            "get_resume_subscription",
            "query_resume_subscriptions",
            "get_schedule",
            "query_due_schedules",
        ]
    for name in methods:
        setattr(legacy, name, getattr(backend, name))
    return cast(TriggerMetadataStore, legacy)


@pytest.mark.parametrize("kind", [SourceType.MANUAL, SourceType.INTERNAL])
@pytest.mark.parametrize("reads", [False, True])
async def test_legacy_provider_preserves_manual_internal_creates(
    metadata_store: InMemoryTriggerMetadataStore, kind: SourceType, reads: bool
) -> None:
    """Existing write-only providers can still create sources without managed schedules."""
    store = SubscriptionStore(_legacy_backend(metadata_store, reads=reads))
    original = _subscription(source_type=kind)
    created = await store.create(original)
    assert created.model_dump() == original.model_dump()
    assert created.store_revision == 0
    assert metadata_store.subscription("ws-1", "sub-1") is not None


@pytest.mark.parametrize("kind", ["manual", "internal"])
def test_legacy_write_only_provider_accepts_existing_http_bodies(
    metadata_store: InMemoryTriggerMetadataStore, kind: str
) -> None:
    """The public M1 create API still returns 201 with no query or mutation capability."""
    app = create_app(
        authz_endpoint="",
        providers=Providers(metadata_store=_legacy_backend(metadata_store, reads=False)),
        receiver_configuration_enabled=False,
    )
    with TestClient(app) as client:
        response = client.post(
            "/v1/workspaces/ws-1/triggers",
            json={"sourceType": kind, "workflowId": "workflow"},
            headers={
                "x-custos-callctx": json.dumps(
                    {
                        "workspace_id": "ws-1",
                        "principal_id": "author",
                        "permissions": ["trigger:subscriptions:write"],
                    }
                )
            },
        )
    assert response.status_code == 201, response.text
    assert response.json()["sourceType"] == kind


async def test_legacy_unmanaged_updates_preserve_state(
    metadata_store: InMemoryTriggerMetadataStore,
) -> None:
    """Legacy metadata edits remain append-only while explicit state changes use the old API."""
    store = SubscriptionStore(_legacy_backend(metadata_store))
    original = await store.create(_subscription(source_type=SourceType.MANUAL))
    await store.set_state("ws-1", "sub-1", SubscriptionState.PAUSED)
    await store.reauthor_selector(original.model_copy(update={"selector": "true"}))
    saved = await store.get("ws-1", "sub-1")
    assert (
        saved is not None and saved.state is SubscriptionState.PAUSED and saved.selector == "true"
    )
    await store.set_state("ws-1", "sub-1", SubscriptionState.EXPIRED)
    with pytest.raises(ImmutableViolation, match="terminal"):
        await store.set_state("ws-1", "sub-1", SubscriptionState.ACTIVE)


@pytest.mark.parametrize("state", [SubscriptionState.PAUSED, SubscriptionState.EXPIRED])
async def test_legacy_state_only_write_cannot_overwrite_concurrent_metadata(
    metadata_store: InMemoryTriggerMetadataStore,
    monkeypatch: pytest.MonkeyPatch,
    state: SubscriptionState,
) -> None:
    """Interleave a selector edit after the state caller's read; state writes must not append."""
    legacy = _legacy_backend(metadata_store)
    store = SubscriptionStore(legacy)
    original = await store.create(_subscription(source_type=SourceType.MANUAL))
    changed = original.model_copy(update={"selector": "false", "input_mapping": {"new": "value"}})

    async def concurrent(
        workspace: WorkspaceId, identifier: SubscriptionId, next_state: str
    ) -> SplSubscription:
        """Publish the competing metadata immediately before the legacy state update."""
        await metadata_store.append_subscription_selector(
            workspace, identifier, to_spl_subscription_selector(changed, added_at=FIXED_NOW)
        )
        return await metadata_store.update_subscription_state(workspace, identifier, next_state)

    monkeypatch.setattr(legacy, "update_subscription_state", concurrent)
    result = await store.set_state("ws-1", "sub-1", state)
    assert result.state is state
    assert result.selector == "false" and result.input_mapping == {"new": "value"}
    assert len(metadata_store.subscription_selectors("ws-1", "sub-1")) == 2


async def test_legacy_provider_cannot_mutate_managed_state(
    metadata_store: InMemoryTriggerMetadataStore,
) -> None:
    """New receivers and sources retaining managed schedules cannot use fallback writes."""
    legacy = SubscriptionStore(_legacy_backend(metadata_store))
    configured = _subscription(
        source_type=SourceType.SCHEDULED, source_config=SourceConfig(interval_seconds=30)
    )
    with pytest.raises(SubscriptionReadUnsupportedError, match="atomic"):
        await legacy.create(configured)
    assert metadata_store.subscription("ws-1", "sub-1") is None
    modern = SubscriptionStore(metadata_store)
    created = await modern.create(configured)
    manual = await modern.update(
        created.model_copy(update={"source_type": SourceType.MANUAL, "source_config": None}),
        expected_revision=created.store_revision,
    )
    with pytest.raises(SubscriptionReadUnsupportedError, match="managed"):
        await legacy.reauthor_selector(manual.model_copy(update={"selector": "true"}))
    assert await modern.get("ws-1", "sub-1") == manual


# --- SubscriptionStore -------------------------------------------------------


async def test_create_subscription_round_trips(
    metadata_store: InMemoryTriggerMetadataStore,
) -> None:
    store = SubscriptionStore(metadata_store, now=lambda: FIXED_NOW)
    subscription = _subscription()

    result = await store.create(subscription)

    assert result.model_dump() == subscription.model_dump()
    assert result.store_revision == 1
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
    original = await store.create(_subscription())

    await store.reauthor_selector(
        original.model_copy(update={"selector": "event.kind == 'gitlab.push'"})
    )

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
