"""Real PostgreSQL pull policy, demand, cursor fencing and atomic audit contracts."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import timedelta

import asyncpg
import pytest
from custos_pg import PgMetadataAdapter
from custos_spl.errors import ImmutableViolation, LeaseExpired
from custos_spl.ids import (
    ConnectorInstanceId,
    RunId,
    StepId,
    SubscriptionId,
    WorkflowId,
    WorkspaceId,
)
from custos_spl.interfaces.metadata_store import (
    ResumeSubscription,
    Subscription,
    SubscriptionSelector,
)
from custos_spl.interfaces.pull_control import PullTickBlocked

from custos_connector.audit import (
    EVENT_PULL_LOOP_PAUSED,
    EVENT_PULL_LOOP_RESUMED,
    build_audit_event,
)

pytestmark = pytest.mark.integration
WS = WorkspaceId("pull-a")
INSTANCE = ConnectorInstanceId("same")


@pytest.fixture
async def pull_store(pg_dsn: str) -> AsyncIterator[PgMetadataAdapter]:
    """Use a real migrated, event-loop-local provider without private test query hydration."""
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=5)
    assert pool is not None
    try:
        store = PgMetadataAdapter(pool=pool)
        await store.check_pull_control_ready()
        yield store
    finally:
        await pool.close()


async def test_shared_interval_survives_fast_commit_and_scope_collision(
    pull_store: PgMetadataAdapter,
) -> None:
    """A fresh caller cannot acquire inside the actual DB reservation, even after lease release."""
    for workspace in (WS, WorkspaceId("other")):
        await pull_store.initialize_pull_control(workspace, INSTANCE, actor="creator")
        await pull_store.set_pull_native_demand(
            workspace, INSTANCE, interval_seconds=60, actor="connector-service"
        )
    _, lease = await pull_store.acquire_pull_tick(WS, INSTANCE, "same-process", ttl_seconds=60)
    control = await pull_store.get_pull_control(WS, INSTANCE)
    assert control is not None
    assert control.next_eligible_at == lease.expires_at
    await pull_store.commit_pull_tick(WS, lease, '"cursor-1"', await pull_store.pull_now())
    assert await pull_store.pull_now() < control.next_eligible_at
    with pytest.raises(PullTickBlocked, match="not_due"):
        await pull_store.acquire_pull_tick(WS, INSTANCE, "replica-b", ttl_seconds=60)
    await pull_store.acquire_pull_tick(WorkspaceId("other"), INSTANCE, "replica-b", ttl_seconds=60)
    updated = await pull_store.get_pull_control(WS, INSTANCE)
    assert updated is not None and updated.next_eligible_at == control.next_eligible_at
    with pytest.raises(LeaseExpired):
        await pull_store.commit_pull_tick(WS, lease, '"stale"', await pull_store.pull_now())


async def test_failed_attempt_keeps_cursor_and_shared_backoff(
    pull_store: PgMetadataAdapter,
) -> None:
    """Failure releases the cursor, not the instance-wide interval or failed-attempt state."""
    await pull_store.initialize_pull_control(WS, INSTANCE, actor="creator")
    await pull_store.set_pull_native_demand(
        WS, INSTANCE, interval_seconds=60, actor="connector-service"
    )
    _, lease = await pull_store.acquire_pull_tick(WS, INSTANCE, "replica-a", ttl_seconds=60)
    await pull_store.fail_pull_tick(WS, lease, backoff_cap_seconds=300)
    cursor = await pull_store.read_cursor(WS, INSTANCE)
    assert cursor is not None and cursor.value == "" and cursor.lease_holder is None
    control = await pull_store.get_pull_control(WS, INSTANCE)
    assert control is not None and control.consecutive_failures == 1
    assert control.next_eligible_at >= lease.expires_at
    with pytest.raises(PullTickBlocked, match="not_due"):
        await pull_store.acquire_pull_tick(WS, INSTANCE, "replica-b", ttl_seconds=60)


async def test_operator_policy_and_audit_roll_back_together(
    pull_store: PgMetadataAdapter,
) -> None:
    """An audit conflict rolls back even the newly inserted control, not just its update."""
    event = build_audit_event(
        workspace_id=WS,
        event_type=EVENT_PULL_LOOP_PAUSED,
        actor="operator",
        subject={"instance_id": "first"},
        payload={"reason": "maintenance"},
    )
    await pull_store.set_pull_paused(
        WS,
        ConnectorInstanceId("first"),
        paused=True,
        audit=event,
    )
    conflicting = replace(
        event, event_type=EVENT_PULL_LOOP_RESUMED, subject={"instance_id": INSTANCE}, payload={}
    )
    with pytest.raises(ImmutableViolation):
        await pull_store.set_pull_paused(WS, INSTANCE, paused=False, audit=conflicting)
    assert await pull_store.get_pull_control(WS, INSTANCE) is None
    first = await pull_store.get_pull_control(WS, ConnectorInstanceId("first"))
    assert first is not None and first.operator_paused
    await pull_store.initialize_pull_control(WS, ConnectorInstanceId("first"), actor="creator")
    assert (await pull_store.get_pull_control(WS, ConnectorInstanceId("first"))) == first


async def test_actual_start_resume_and_native_demand_reconcile(
    pull_store: PgMetadataAdapter,
) -> None:
    """Scoped rows determine demand; expiry, cancellation and pause remove contributions."""
    now = await pull_store.pull_now()
    sub = Subscription(WS, SubscriptionId("start"), WorkflowId("wf"), "active", now, now)
    await pull_store.put_subscription(WS, sub)
    await pull_store.append_subscription_selector(
        WS,
        sub.subscription_id,
        SubscriptionSelector(
            WS,
            sub.subscription_id,
            {
                "sourceType": "pull",
                "sourceConfig": {
                    "connectorInstanceId": INSTANCE,
                    "pollIntervalSeconds": 60,
                },
            },
            now,
        ),
    )
    await pull_store.put_resume_subscription(
        WS,
        ResumeSubscription(
            WS,
            "wait",
            RunId("run"),
            StepId("step"),
            now + timedelta(hours=1),
            {
                "source": {
                    "connectorInstanceId": INSTANCE,
                    "deliveryModes": ["pull"],
                    "pollIntervalSeconds": 20,
                }
            },
        ),
    )
    await pull_store.put_resume_subscription(
        WS,
        ResumeSubscription(
            WS,
            "expired",
            RunId("run"),
            StepId("expired"),
            now - timedelta(seconds=1),
            {
                "source": {
                    "connectorInstanceId": INSTANCE,
                    "deliveryModes": ["pull"],
                    "pollIntervalSeconds": 10,
                }
            },
        ),
    )
    await pull_store.reconcile_pull_demand(
        WS,
        default_interval_seconds=60,
        minimum_interval_seconds=10,
    )
    control = await pull_store.get_pull_control(WS, INSTANCE)
    assert control is not None and control.operator_paused
    assert control.effective_interval_seconds == 20
    await pull_store.set_pull_native_demand(WS, INSTANCE, interval_seconds=30, actor="connector")
    await pull_store.delete_resume_subscription(WS, "wait")
    await pull_store.reconcile_pull_demand(
        WS,
        default_interval_seconds=60,
        minimum_interval_seconds=10,
    )
    control = await pull_store.get_pull_control(WS, INSTANCE)
    assert control is not None and control.effective_interval_seconds == 30
    await pull_store.update_subscription_state(WS, sub.subscription_id, "paused")
    await pull_store.reconcile_pull_demand(
        WS,
        default_interval_seconds=60,
        minimum_interval_seconds=10,
    )
    control = await pull_store.set_pull_native_demand(
        WS, INSTANCE, interval_seconds=None, actor="connector"
    )
    assert control.operator_paused and control.effective_interval_seconds is None
    assert not (await pull_store.query_pull_controls(WS)).items


async def test_pull_control_pages_are_scoped(pull_store: PgMetadataAdapter) -> None:
    """Control paging preserves the workspace/instance composite identity."""
    for workspace in (WS, WorkspaceId("other")):
        for identifier in ("a", "b", "c"):
            await pull_store.set_pull_native_demand(
                workspace, ConnectorInstanceId(identifier), interval_seconds=10, actor="connector"
            )
    first = await pull_store.query_pull_controls(WS, limit=2)
    assert [row.instance_id for row in first.items] == ["a", "b"]
    second = await pull_store.query_pull_controls(WS, cursor=first.next_cursor, limit=2)
    assert [row.instance_id for row in second.items] == ["c"] and second.next_cursor is None
    with pytest.raises(ValueError):
        await pull_store.query_pull_controls(WorkspaceId("other"), cursor=first.next_cursor)
    rows = await pull_store.read_pull_controls(
        WS, [ConnectorInstanceId("c"), ConnectorInstanceId("a"), ConnectorInstanceId("missing")]
    )
    assert [row.instance_id for row in rows] == ["a", "c"]
    assert all(row.workspace_id == WS for row in rows)
