"""Durable pull-control invariants over deterministic shared memory state."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from custos_spl.errors import ImmutableViolation, LeaseExpired, QueryUnsupported
from custos_spl.ids import ConnectorInstanceId, WorkspaceId
from custos_spl.interfaces.metadata_store import AuditEvent
from custos_spl.interfaces.pull_control import (
    ConnectorPullControlProvider,
    PullTickBlocked,
    collect_pull_demand,
    pull_deadline,
    pull_interval,
)
from custos_spl.pull_memory import MemoryPullControl

WS = WorkspaceId("a")
INSTANCE = ConnectorInstanceId("same")
NOW = datetime(2026, 10, 30, tzinfo=UTC)


def audit(paused: bool, workspace: WorkspaceId = WS) -> AuditEvent:
    """Build the existing Connector audit shape with a fresh operator-action identity."""
    return AuditEvent(
        workspace,
        uuid4().hex,
        "connector.pull-loop.paused" if paused else "connector.pull-loop.resumed",
        "operator",
        {"instance_id": INSTANCE},
        {},
        NOW,
    )


@pytest.mark.parametrize("value", [True, False, 9, 10.0, "10", None, 10**30])
def test_invalid_intervals_are_explicit(value: object) -> None:
    """Reject coercion, sub-floor values and arithmetic overflow."""
    with pytest.raises(ValueError):
        pull_interval(value)


def test_demand_is_union_of_scoped_projections() -> None:
    """Resume-only demand participates and the effective interval respects the connector floor."""
    assert collect_pull_demand(
        [
            {"connectorInstanceId": "same"},
            {"connectorInstanceId": "other", "pollIntervalSeconds": 90},
        ],
        [
            {"connectorInstanceId": "same", "deliveryModes": ["pull"], "pollIntervalSeconds": 10},
            {"connectorInstanceId": "resume-only", "deliveryModes": ["push", "pull"]},
        ],
        default_interval_seconds=60,
        minimum_interval_seconds=20,
    ) == {"same": 20, "other": 90, "resume-only": 60}
    with pytest.raises(ValueError):
        collect_pull_demand(
            [],
            [{"connectorInstanceId": "same", "deliveryModes": ["push"]}],
            default_interval_seconds=60,
            minimum_interval_seconds=10,
        )


async def test_pause_before_registration_and_explicit_confirmation() -> None:
    """Reconciliation and new-instance initialization never clear an operator pause."""
    sources: list[object] = [{"connectorInstanceId": INSTANCE}]
    rows: list[tuple[str, AuditEvent]] = []
    store = MemoryPullControl(
        now=lambda: NOW,
        audit_rows=rows,
        demand_reader=lambda ws, now: (sources, []),
    )
    assert isinstance(store, ConnectorPullControlProvider)
    await store.set_pull_paused(WS, INSTANCE, paused=True, audit=audit(True))
    await store.initialize_pull_control(WS, INSTANCE, actor="creator")
    await store.reconcile_pull_demand(
        WS,
        default_interval_seconds=60,
        minimum_interval_seconds=10,
    )
    with pytest.raises(PullTickBlocked, match="paused"):
        await store.acquire_pull_tick(WS, INSTANCE, "worker", ttl_seconds=30)
    await store.set_pull_paused(WS, INSTANCE, paused=False, audit=audit(False))
    _, lease = await store.acquire_pull_tick(WS, INSTANCE, "worker", ttl_seconds=30)
    assert lease.interval_seconds == 60 and len(rows) == 2
    sources.clear()
    await store.reconcile_pull_demand(
        WS,
        default_interval_seconds=60,
        minimum_interval_seconds=10,
    )
    control = await store.get_pull_control(WS, INSTANCE)
    assert control is not None and control.effective_interval_seconds is None
    assert control.next_eligible_at == NOW + timedelta(seconds=60)


async def test_unknown_legacy_state_fails_closed() -> None:
    """Active subscriptions do not authorize an unknown legacy instance to resume polling."""
    store = MemoryPullControl(
        now=lambda: NOW,
        demand_reader=lambda ws, now: ([{"connectorInstanceId": INSTANCE}], []),
    )
    with pytest.raises(PullTickBlocked, match="unconfirmed"):
        await store.acquire_pull_tick(WS, INSTANCE, "worker", ttl_seconds=30)
    await store.reconcile_pull_demand(
        WS,
        default_interval_seconds=10,
        minimum_interval_seconds=10,
    )
    control = await store.get_pull_control(WS, INSTANCE)
    assert control is not None and control.operator_paused


async def test_commit_and_interval_changes_preserve_shared_reservation() -> None:
    """Staggered workers and shorter new intervals cannot poll inside an existing reservation."""
    now = [NOW]
    source: dict[str, object] = {"connectorInstanceId": INSTANCE, "pollIntervalSeconds": 60}
    store = MemoryPullControl(
        now=lambda: now[0],
        demand_reader=lambda ws, instant: ([source], []),
    )
    await store.initialize_pull_control(WS, INSTANCE, actor="creator")
    await store.reconcile_pull_demand(
        WS,
        default_interval_seconds=60,
        minimum_interval_seconds=10,
    )
    _, first = await store.acquire_pull_tick(WS, INSTANCE, "worker", ttl_seconds=30)
    row = await store.commit_pull_tick(WS, first, '"cursor-1"', NOW)
    assert row.lease_holder is None
    now[0] += timedelta(seconds=1)
    source["pollIntervalSeconds"] = 10
    await store.reconcile_pull_demand(
        WS,
        default_interval_seconds=60,
        minimum_interval_seconds=10,
    )
    await store.set_pull_paused(WS, INSTANCE, paused=True, audit=audit(True))
    await store.set_pull_paused(WS, INSTANCE, paused=False, audit=audit(False))
    with pytest.raises(PullTickBlocked, match="not_due"):
        await store.acquire_pull_tick(WS, INSTANCE, "another-worker", ttl_seconds=30)
    now[0] = NOW + timedelta(seconds=60)
    row, second = await store.acquire_pull_tick(WS, INSTANCE, "worker", ttl_seconds=30)
    assert row.value == '"cursor-1"' and second.token != first.token
    assert second.interval_seconds == 10
    with pytest.raises(LeaseExpired):
        await store.commit_pull_tick(WS, first, '"stale"', now[0])


async def test_backoff_is_shared_and_cursor_unchanged() -> None:
    """Repeated failures reserve capped backoff without committing any unpublished cursor."""
    now = [NOW]
    store = MemoryPullControl(
        now=lambda: now[0],
        demand_reader=lambda ws, instant: ([{"connectorInstanceId": INSTANCE}], []),
    )
    await store.initialize_pull_control(WS, INSTANCE, actor="creator")
    await store.reconcile_pull_demand(
        WS,
        default_interval_seconds=10,
        minimum_interval_seconds=10,
    )
    for delay in (10, 20, 40, 40):
        row, lease = await store.acquire_pull_tick(WS, INSTANCE, uuid4().hex, ttl_seconds=30)
        assert row.value == ""
        await store.fail_pull_tick(WS, lease, backoff_cap_seconds=40)
        control = await store.get_pull_control(WS, INSTANCE)
        assert control is not None
        assert control.next_eligible_at == now[0] + timedelta(seconds=delay)
        with pytest.raises(PullTickBlocked, match="not_due"):
            await store.acquire_pull_tick(WS, INSTANCE, "staggered", ttl_seconds=30)
        now[0] = control.next_eligible_at


async def test_audit_failure_cannot_change_operator_intent() -> None:
    """Duplicate audit identities fail atomically, matching PostgreSQL's append-only contract."""
    store = MemoryPullControl(now=lambda: NOW)
    event = audit(True)
    before = await store.set_pull_paused(WS, INSTANCE, paused=True, audit=event)
    with pytest.raises(ImmutableViolation):
        await store.set_pull_paused(WS, INSTANCE, paused=True, audit=event)
    assert await store.get_pull_control(WS, INSTANCE) == before
    with pytest.raises(QueryUnsupported):
        await store.reconcile_pull_demand(
            WS,
            default_interval_seconds=60,
            minimum_interval_seconds=10,
        )


async def test_native_listening_and_trigger_demand_do_not_erase_each_other() -> None:
    """One shared timer honors both registration owners and preserves explicit pause."""
    sources: list[object] = [{"connectorInstanceId": INSTANCE, "pollIntervalSeconds": 60}]
    store = MemoryPullControl(
        now=lambda: NOW,
        demand_reader=lambda ws, now: (sources, []),
    )
    await store.initialize_pull_control(WS, INSTANCE, actor="creator")
    await store.set_pull_native_demand(WS, INSTANCE, interval_seconds=20, actor="connector-service")
    await store.reconcile_pull_demand(
        WS,
        default_interval_seconds=60,
        minimum_interval_seconds=10,
    )
    control = await store.get_pull_control(WS, INSTANCE)
    assert control is not None and control.effective_interval_seconds == 20
    await store.set_pull_paused(WS, INSTANCE, paused=True, audit=audit(True))
    control = await store.set_pull_native_demand(
        WS, INSTANCE, interval_seconds=None, actor="connector-service"
    )
    assert control.operator_paused and control.effective_interval_seconds == 60
    sources.clear()
    await store.reconcile_pull_demand(
        WS,
        default_interval_seconds=60,
        minimum_interval_seconds=10,
    )
    assert not (await store.query_pull_controls(WS)).items
    control = await store.set_pull_native_demand(
        WS, INSTANCE, interval_seconds=20, actor="connector-service"
    )
    assert control.operator_paused


async def test_query_cursors_are_workspace_scoped() -> None:
    """Paged demand never leaks colliding instance identifiers across workspaces."""
    store = MemoryPullControl(now=lambda: NOW)
    for workspace in (WS, WorkspaceId("b")):
        for identifier in ("a", "b", "c"):
            await store.set_pull_native_demand(
                workspace, ConnectorInstanceId(identifier), interval_seconds=10, actor="creator"
            )
    first = await store.query_pull_controls(WS, limit=2)
    assert [row.instance_id for row in first.items] == ["a", "b"] and first.next_cursor
    second = await store.query_pull_controls(WS, cursor=first.next_cursor, limit=2)
    assert [row.instance_id for row in second.items] == ["c"] and second.next_cursor is None
    with pytest.raises(ValueError):
        await store.query_pull_controls(WorkspaceId("b"), cursor=first.next_cursor)
    assert await store.pull_now() == NOW
    rows = await store.read_pull_controls(
        WS, [ConnectorInstanceId("c"), ConnectorInstanceId("a"), ConnectorInstanceId("missing")]
    )
    assert [row.instance_id for row in rows] == ["a", "c"]
    with pytest.raises(ValueError):
        await store.read_pull_controls(WS, [])
    with pytest.raises(ValueError):
        await store.read_pull_controls(WS, [INSTANCE, INSTANCE])
    assert (
        await store.read_pull_controls(
            WS, [ConnectorInstanceId(f"missing-{index}") for index in range(200)]
        )
        == ()
    )
    with pytest.raises(ValueError):
        await store.read_pull_controls(
            WS, [ConnectorInstanceId(f"missing-{index}") for index in range(201)]
        )


def test_timestamp_overflow_is_configuration_error() -> None:
    """Reject timestamp overflow before acquiring cursor ownership."""
    with pytest.raises(ValueError, match="timestamp"):
        pull_deadline(datetime.max.replace(tzinfo=UTC), 10)
