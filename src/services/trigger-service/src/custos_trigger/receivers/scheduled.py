"""Fenced scheduled occurrences, bounded scans and shared-pipeline delivery.

The receiver is a workspace-bound collaborator, not an automatically started
worker. Persisted occurrences survive producer death; retries retain the exact
event identity. Broker/lifecycle activation is composed separately.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Protocol, runtime_checkable

from custos_spl.errors import ImmutableViolation, LeaseExpired, QueryUnsupported
from custos_spl.ids import WorkspaceId
from custos_spl.interfaces.dead_letters import DeadLetterData
from custos_spl.interfaces.scheduler import (
    PendingScheduleFire,
    SchedulerLease,
    SchedulerStateProvider,
    schedule_fire_id,
    utc_instant,
)
from custos_spl.interfaces.trigger_queries import TriggerQueryProvider
from custos_spl.pagination import Cursor, query_page_limit

from custos_trigger.events import EventSource, NormalizedEvent
from custos_trigger.input_mapping import resolve_input_mapping
from custos_trigger.models import SourceType, Subscription, SubscriptionKind, SubscriptionState
from custos_trigger.pipeline.classify import classify
from custos_trigger.pipeline.dispatch import AuditSink, Dispatcher, DispatchOutcome
from custos_trigger.pipeline.match_resume import resume_key_from_event
from custos_trigger.pipeline.match_start import StartMatch, StartMatcher
from custos_trigger.pipeline.reliable import build_start_target
from custos_trigger.scheduler import SchedulerLeadership
from custos_trigger.selector import SelectorEvaluator
from custos_trigger.source_config import (
    SourceConfig,
    SourceConfigurationError,
    next_schedule_fire,
    validate_source_config,
)
from custos_trigger.stores.base import TriggerMetadataStore
from custos_trigger.stores.subscriptions import SubscriptionStore

logger = logging.getLogger(__name__)


class ScheduledOccurrenceError(SourceConfigurationError):
    """A permanent invalid occurrence, distinct from retryable subscription configuration."""


@runtime_checkable
class ScheduledStorage(
    TriggerMetadataStore, TriggerQueryProvider, SchedulerStateProvider, Protocol
):
    """The existing query/write/fencing capabilities required by a scheduled receiver."""


@dataclass(frozen=True)
class SchedulePlan:
    """One coalesced occurrence plus the first future fire and exact skipped count."""

    planned_at: datetime
    next_at: datetime
    skipped_count: int


async def plan_schedule(
    config: SourceConfig,
    *,
    anchor: datetime,
    due: datetime,
    now: datetime,
    heartbeat: Callable[[], Awaitable[datetime]],
) -> SchedulePlan:
    """Coalesce without replaying a backlog; long cron counts yield and renew ownership."""
    validate_source_config("scheduled", config)
    due, now, anchor = map(utc_instant, (due, now, anchor))
    if due > now:
        raise SourceConfigurationError("schedule is not due")
    if config.interval_seconds is not None:
        interval = timedelta(seconds=config.interval_seconds)
        if (due - anchor) % interval:
            raise SourceConfigurationError(
                "stored interval occurrence is not aligned to its anchor"
            )
        skipped = (now - due) // interval
        planned = due + skipped * interval
        return SchedulePlan(planned, next_schedule_fire(config, anchor=anchor, after=now), skipped)
    if next_schedule_fire(config, anchor=anchor, after=due - timedelta(microseconds=1)) != due:
        raise SourceConfigurationError("stored cron occurrence does not match its configuration")
    planned, skipped = due, 0
    following = next_schedule_fire(config, anchor=anchor, after=planned)
    while following <= now:
        planned, skipped = following, skipped + 1
        following = next_schedule_fire(config, anchor=anchor, after=planned)
        if skipped % 128 == 0:
            await asyncio.sleep(0)
            now = utc_instant(await heartbeat())
    return SchedulePlan(planned, following, skipped)


def scheduled_event(fire: PendingScheduleFire) -> NormalizedEvent:
    """Normalize only immutable occurrence data, preserving identity across recovery and retries."""
    instant = utc_instant(fire.planned_fire_at).isoformat(timespec="microseconds")
    return NormalizedEvent(
        event_id=fire.fire_id,
        kind="cron.tick",
        source=EventSource(
            type=SourceType.SCHEDULED,
            subscription_id=fire.schedule_id,
            occurred_at=instant,
        ),
        subject=fire.schedule_id,
        data={
            "workspace": str(fire.workspace_id),
            "scheduleId": fire.schedule_id,
            "subscriptionRevision": fire.subscription_revision,
            "plannedFireAt": instant,
            "skippedCount": fire.skipped_count,
        },
    )


def _suppression(subscription: Subscription | None, revision: int) -> str | None:
    """A changed or inactive association cannot rebind a previously planned occurrence."""
    if subscription is None:
        return "missing"
    if subscription.state is not SubscriptionState.ACTIVE:
        return "inactive"
    if (
        subscription.kind is not SubscriptionKind.START
        or subscription.source_type is not SourceType.SCHEDULED
        or subscription.source_config is None
        or subscription.store_revision != revision
    ):
        return "changed"
    return None


async def process_scheduled_event(
    event: NormalizedEvent,
    *,
    subscription_store: SubscriptionStore,
    provider: SchedulerStateProvider,
    evaluator: SelectorEvaluator,
    dispatcher: Dispatcher,
    audit: AuditSink,
) -> DispatchOutcome | None:
    """Route a scheduled occurrence only to its own eligible subscription, never every cron row."""
    workspace = event.data.get("workspace")
    revision = event.data.get("subscriptionRevision")
    identifier = event.source.subscription_id
    if (
        event.source.type is not SourceType.SCHEDULED
        or event.kind != "cron.tick"
        or not isinstance(workspace, str)
        or not workspace.strip()
        or not identifier
        or event.data.get("scheduleId") != identifier
        or type(revision) is not int
        or revision < 0
    ):
        logger.error("Invalid scheduled event association: %s", event.event_id)
        raise ScheduledOccurrenceError("scheduled event has no valid scoped association")
    if resume_key_from_event(event) is not None:
        logger.error("Scheduled event carries unsupported resume routing: %s", event.event_id)
        raise ScheduledOccurrenceError("scheduled source cannot inject resume routing identifiers")
    fire = await provider.get_schedule_fire(WorkspaceId(workspace), event.event_id)
    if (
        fire is None
        or fire.fire_id != schedule_fire_id(fire.schedule_id, fire.planned_fire_at)
        or event.model_dump(exclude={"raw"}) != scheduled_event(fire).model_dump(exclude={"raw"})
    ):
        logger.error("Unproven scheduled occurrence: %s", event.event_id)
        raise ScheduledOccurrenceError("scheduled event does not match canonical occurrence proof")
    event = scheduled_event(fire)
    reliable = dispatcher.reliable

    def prepare_inputs(subscription: Subscription) -> dict[str, Any]:
        """Resolve a new occurrence only after previously accepted intent has been recovered."""
        return resolve_input_mapping(
            subscription.input_mapping,
            event,
            evaluator=evaluator,
            subscription_id=subscription.subscription_id,
        )

    if reliable is not None:

        async def select() -> AsyncIterator[DeadLetterData]:
            """Persist the original scheduled target before attempting its downstream effect."""
            subscription = await _match_scheduled(
                event, workspace, fire, subscription_store, evaluator, audit
            )
            if subscription is not None:
                request = await reliable.prepare_request(
                    event,
                    workspace,
                    identifier,
                    build_start_target(subscription),
                    prepare_inputs=lambda: prepare_inputs(subscription),
                )
                if request is not None:
                    yield request

        outcomes = await reliable.dispatch_event(event, workspace, select)
        return outcomes[0][1] if outcomes else None
    subscription = await _match_scheduled(
        event, workspace, fire, subscription_store, evaluator, audit
    )
    if subscription is None:
        return None
    return await dispatcher.dispatch_start(
        event, StartMatch(subscription), prepare_inputs=lambda: prepare_inputs(subscription)
    )


async def _match_scheduled(
    event: NormalizedEvent,
    workspace: str,
    fire: PendingScheduleFire,
    subscription_store: SubscriptionStore,
    evaluator: SelectorEvaluator,
    audit: AuditSink,
) -> Subscription | None:
    """Validate current intent only when there is no persisted source selection to recover."""
    identifier = event.source.subscription_id
    assert identifier is not None
    if fire.discarded_at is not None:
        await audit.emit(
            "trigger.schedule.suppressed",
            workspace_id=workspace,
            attributes={
                "eventId": event.event_id,
                "subscriptionId": identifier,
                "reason": fire.discard_reason,
            },
        )
        return None
    subscription = await subscription_store.get(workspace, identifier)
    reason = _suppression(subscription, fire.subscription_revision)
    if reason is not None:
        await audit.emit(
            "trigger.schedule.suppressed",
            workspace_id=workspace,
            attributes={"eventId": event.event_id, "subscriptionId": identifier, "reason": reason},
        )
        return None
    assert subscription is not None
    classification = classify(event)
    matches = (
        StartMatcher(evaluator).match(event, [subscription]) if classification.to_start else []
    )
    if not matches:
        await audit.emit(
            "trigger.schedule.filtered",
            workspace_id=workspace,
            attributes={"eventId": event.event_id, "subscriptionId": identifier},
        )
        return None

    return subscription


@dataclass(frozen=True)
class ScheduledScan:
    """Observable bounded scan result; standby is distinct from a successful publication."""

    leader: bool
    planned: int = 0
    published: int = 0
    suppressed: int = 0


class ScheduledReceiver:
    """Plan and recover one workspace's bounded batch using shared deployment leadership."""

    def __init__(
        self,
        provider: object,
        *,
        workspace_id: str,
        leadership: SchedulerLeadership,
        publish: Callable[[NormalizedEvent], Awaitable[None]],
        audit: AuditSink,
        page_size: int = 50,
    ) -> None:
        """Bind explicit scope, publication acknowledgement and storage capabilities."""
        if not workspace_id.strip():
            raise ValueError("scheduled receiver requires a workspace")
        if not isinstance(provider, ScheduledStorage):
            raise QueryUnsupported(
                "scheduled receiver requires query, mutation and fencing capabilities"
            )
        self.store = provider
        self.workspace = WorkspaceId(workspace_id)
        self.leadership, self.publish, self.audit = leadership, publish, audit
        self.page_size = query_page_limit(page_size)
        self.subscriptions = SubscriptionStore(provider)
        self._lease: SchedulerLease | None = None
        self._lock = asyncio.Lock()
        self._due_cutoff: datetime | None = None
        self._due_cursor: Cursor | None = None

    async def _heartbeat(self) -> datetime:
        """Refresh provider authority before side effects and during cooperative cron counting."""
        self._lease = await self.leadership.poll(force_renew=True)
        if self._lease is None:
            raise LeaseExpired("scheduled receiver lost deployment leadership")
        return await self.store.scheduler_now()

    async def tick(self) -> ScheduledScan:
        """Recover one pending page, then plan one due page; failed publication stays pending."""
        async with self._lock:
            self._lease = await self.leadership.poll(force_renew=True)
            if self._lease is None:
                return ScheduledScan(leader=False)
            published, suppressed = await self._recover()
            now = await self._heartbeat()
            if self._due_cutoff is None:
                self._due_cutoff = now
            page = await self.store.query_due_schedules(
                self.workspace,
                due_at=self._due_cutoff,
                cursor=self._due_cursor,
                limit=self.page_size,
            )
            planned = 0
            for schedule in page.items:
                subscription = await self.subscriptions.get(
                    str(self.workspace),
                    schedule.schedule_id,
                )
                current_schedule = await self.store.get_schedule(
                    self.workspace, schedule.schedule_id
                )
                if current_schedule != schedule:
                    logger.info(
                        "Deferring changed schedule candidate before planning: %s",
                        schedule.schedule_id,
                    )
                    continue
                if subscription is None or subscription.source_config is None:
                    logger.error(
                        "Due schedule has no managed configuration: %s", schedule.schedule_id
                    )
                    raise SourceConfigurationError("due schedule has no managed configuration")
                if _suppression(subscription, subscription.store_revision) is not None:
                    continue
                now = await self._heartbeat()
                plan = await plan_schedule(
                    subscription.source_config,
                    anchor=subscription.created_at,
                    due=schedule.next_fire_at,
                    now=now,
                    heartbeat=self._heartbeat,
                )
                assert self._lease is not None
                try:
                    fire = await self.store.advance_schedule(
                        self.workspace,
                        schedule.schedule_id,
                        self._lease,
                        expected_next_fire_at=schedule.next_fire_at,
                        expected_revision=subscription.store_revision,
                        planned_fire_at=plan.planned_at,
                        next_fire_at=plan.next_at,
                        skipped_count=plan.skipped_count,
                    )
                except ImmutableViolation:
                    logger.info(
                        "Deferring changed or elapsed schedule plan: %s", schedule.schedule_id
                    )
                    continue
                planned += 1
                accepted = await self._publish(fire)
                published += int(accepted)
                suppressed += int(not accepted)
            self._due_cursor = page.next_cursor
            if self._due_cursor is None:
                self._due_cutoff = None
            return ScheduledScan(True, planned, published, suppressed)

    async def _recover(self) -> tuple[int, int]:
        """Retry durable pending occurrences with their original event identities and counts."""
        assert self._lease is not None
        page = await self.store.query_pending_schedule_fires(
            self.workspace,
            self._lease.deployment_id,
            limit=self.page_size,
        )
        published = suppressed = 0
        for fire in page.items:
            accepted = await self._publish(fire)
            published += int(accepted)
            suppressed += int(not accepted)
        return published, suppressed

    async def _publish(self, fire: PendingScheduleFire) -> bool:
        """Settle ineligible work separately; acknowledge eligible work only after publication."""
        await self._heartbeat()
        subscription = await self.subscriptions.get(str(self.workspace), fire.schedule_id)
        reason = _suppression(subscription, fire.subscription_revision)
        assert self._lease is not None
        attributes = {
            "eventId": fire.fire_id,
            "subscriptionId": fire.schedule_id,
            "skippedCount": fire.skipped_count,
        }
        if reason is not None:
            await self.audit.emit(
                "trigger.schedule.suppressed",
                workspace_id=str(self.workspace),
                attributes={**attributes, "reason": reason},
            )
            await self.store.discard_schedule_fire(
                self.workspace,
                fire.fire_id,
                self._lease,
                reason=reason,
            )
            return False
        if fire.skipped_count:
            await self.audit.emit(
                "trigger.schedule.coalesced",
                workspace_id=str(self.workspace),
                attributes=attributes,
            )
        await self.publish(scheduled_event(fire))
        await self.store.acknowledge_schedule_fire(self.workspace, fire.fire_id, self._lease)
        return True
