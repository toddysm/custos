"""Cadence and cooperative coalescing tests for the scheduled receiver."""

from datetime import UTC, datetime, timedelta

import pytest

from custos_trigger.receivers.scheduled import plan_schedule
from custos_trigger.source_config import SourceConfig, SourceConfigurationError


@pytest.mark.parametrize(
    ("config", "due", "now", "planned", "following", "skipped"),
    [
        (
            SourceConfig(interval_seconds=30),
            "2026-01-01T00:00:00+00:00",
            "2026-01-01T00:02:15+00:00",
            "2026-01-01T00:02:00+00:00",
            "2026-01-01T00:02:30+00:00",
            4,
        ),
        (
            SourceConfig(cron="30 2 * * *", timezone="America/New_York"),
            "2026-03-07T07:30:00+00:00",
            "2026-03-09T07:00:00+00:00",
            "2026-03-09T06:30:00+00:00",
            "2026-03-10T06:30:00+00:00",
            1,
        ),
        (
            SourceConfig(cron="30 1 * * *", timezone="America/New_York"),
            "2026-11-01T05:30:00+00:00",
            "2026-11-01T06:45:00+00:00",
            "2026-11-01T06:30:00+00:00",
            "2026-11-02T06:30:00+00:00",
            1,
        ),
        (
            SourceConfig(cron="30 1 * * *", timezone="America/New_York"),
            "2026-11-01T06:30:00+00:00",
            "2026-11-01T06:45:00+00:00",
            "2026-11-01T06:30:00+00:00",
            "2026-11-02T06:30:00+00:00",
            0,
        ),
    ],
)
async def test_coalescing_uses_actual_utc_occurrences(
    config: SourceConfig,
    due: str,
    now: str,
    planned: str,
    following: str,
    skipped: int,
) -> None:
    """Skip nonexistent local times, retain both repeated instants and anchor intervals."""
    cutoff = datetime.fromisoformat(now)

    async def heartbeat() -> datetime:
        """Return the deterministic provider cutoff."""
        return cutoff

    result = await plan_schedule(
        config,
        anchor=datetime.fromisoformat(due),
        due=datetime.fromisoformat(due),
        now=cutoff,
        heartbeat=heartbeat,
    )
    assert result.planned_at == datetime.fromisoformat(planned)
    assert result.next_at == datetime.fromisoformat(following)
    assert result.skipped_count == skipped


async def test_large_cron_coalescing_yields_and_refreshes_provider_authority() -> None:
    """Counting missed ticks stays cooperative while preserving the exact skipped total."""
    due = datetime(2026, 1, 1, tzinfo=UTC)
    cutoff = due + timedelta(minutes=300)
    renewals = 0

    async def heartbeat() -> datetime:
        """Record bounded counting chunks rather than sleeping through a wall-clock outage."""
        nonlocal renewals
        renewals += 1
        return cutoff

    result = await plan_schedule(
        SourceConfig(cron="* * * * *"),
        anchor=due,
        due=due,
        now=cutoff,
        heartbeat=heartbeat,
    )
    assert result.planned_at == cutoff and result.skipped_count == 300
    assert result.next_at == cutoff + timedelta(minutes=1)
    assert renewals == 2


async def test_invalid_interval_alignment_is_explicit() -> None:
    """Do not silently re-anchor corrupt or unmanaged persisted schedule state."""
    anchor = datetime(2026, 1, 1, tzinfo=UTC)

    async def heartbeat() -> datetime:
        """Provide an otherwise eligible cutoff."""
        return anchor + timedelta(minutes=1)

    with pytest.raises(SourceConfigurationError, match="aligned"):
        await plan_schedule(
            SourceConfig(interval_seconds=30),
            anchor=anchor,
            due=anchor + timedelta(seconds=1),
            now=await heartbeat(),
            heartbeat=heartbeat,
        )


async def test_invalid_cron_due_instant_is_explicit() -> None:
    """A corrupted persisted second is not accepted as a valid five-field cron occurrence."""
    anchor = datetime(2026, 1, 1, tzinfo=UTC)

    async def heartbeat() -> datetime:
        """Provide an otherwise eligible cutoff."""
        return anchor + timedelta(minutes=2)

    with pytest.raises(SourceConfigurationError, match="does not match"):
        await plan_schedule(
            SourceConfig(cron="* * * * *"),
            anchor=anchor,
            due=anchor + timedelta(seconds=15),
            now=await heartbeat(),
            heartbeat=heartbeat,
        )


@pytest.mark.parametrize(
    "reason", ["paused", "expired", "changed", "missing", "filtered", "resume"]
)
async def test_consumer_rechecks_eligibility_and_never_routes_unrelated_work(reason: str) -> None:
    """A state change after publication cannot be bypassed by the shared pipeline consumer."""
    from collections.abc import Mapping
    from typing import Any

    from custos_spl.ids import WorkspaceId
    from custos_spl.interfaces.scheduler import PendingScheduleFire

    from custos_trigger.clients import FakeWorkflowServiceClient
    from custos_trigger.dedup import Deduplicator
    from custos_trigger.models import SourceType, Subscription, SubscriptionState
    from custos_trigger.pipeline.dispatch import Dispatcher
    from custos_trigger.providers import InMemoryTriggerMetadataStore
    from custos_trigger.receivers.scheduled import process_scheduled_event, scheduled_event
    from custos_trigger.selector import SelectorEvaluator
    from custos_trigger.stores.subscriptions import SubscriptionStore

    now = datetime(2026, 1, 1, tzinfo=UTC)
    provider = InMemoryTriggerMetadataStore(now=lambda: now)
    store = SubscriptionStore(provider, now=lambda: now)
    subscription = await store.create(
        Subscription(
            workspace_id="ws",
            subscription_id="schedule",
            workflow_id="workflow",
            target_workflow_version_id="version",
            source_type=SourceType.SCHEDULED,
            source_config=SourceConfig(interval_seconds=30),
            created_at=now,
            updated_at=now,
            selector="false" if reason == "filtered" else None,
        )
    )
    event = scheduled_event(
        PendingScheduleFire(
            WorkspaceId("ws"),
            "fire",
            "schedule",
            "workflow",
            "deployment",
            1,
            subscription.store_revision,
            now,
            now,
        )
    )
    if reason in ("paused", "expired"):
        await store.set_state("ws", "schedule", SubscriptionState(reason))
    elif reason == "changed":
        await store.update(
            subscription.model_copy(update={"input_mapping": {"different": 1}}),
            expected_revision=subscription.store_revision,
        )
    elif reason == "missing":
        event.data["workspace"] = "other"
    elif reason == "resume":
        event.data.update(runId="unrelated-run", stepId="wait")

    audits: list[str] = []

    class Audit:
        """Record explicit suppression/filter evidence rather than treating it as dispatch."""

        async def emit(
            self,
            event_name: str,
            *,
            workspace_id: str,
            attributes: Mapping[str, Any],
        ) -> None:
            """Record the selected outcome without exporting fixture state."""
            audits.append(event_name)

    client = FakeWorkflowServiceClient()
    dispatcher = Dispatcher(client, Deduplicator(provider))
    if reason == "resume":
        with pytest.raises(SourceConfigurationError, match="resume routing"):
            await process_scheduled_event(
                event,
                subscription_store=store,
                evaluator=SelectorEvaluator(),
                dispatcher=dispatcher,
                audit=Audit(),
            )
    else:
        assert (
            await process_scheduled_event(
                event,
                subscription_store=store,
                evaluator=SelectorEvaluator(),
                dispatcher=dispatcher,
                audit=Audit(),
            )
            is None
        )
        assert audits == [
            "trigger.schedule.filtered" if reason == "filtered" else "trigger.schedule.suppressed"
        ]
    assert not client.start_run_calls and not client.raise_event_calls
