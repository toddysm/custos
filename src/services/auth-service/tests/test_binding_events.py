"""Unit tests for the binding-changed event types and publishers."""

from __future__ import annotations

import pytest
from custos_spl.ids import (
    PrincipalId,
    RoleBindingId,
    RoleId,
    TenantId,
    WorkspaceId,
)
from custos_spl.interfaces.auth_store import (
    GlobalScope,
    TenantScope,
    WorkspaceScope,
)

from custos_auth.binding_events import (
    BindingChangedEvent,
    LocalBindingChangedBus,
    NoOpBindingChangedPublisher,
    NoOpBindingChangedSubscriber,
    RecordingBindingChangedPublisher,
    RecordingBindingChangedSubscriber,
)


def _event(scope: object) -> BindingChangedEvent:
    return BindingChangedEvent(
        principal_id=PrincipalId("user-1"),
        role_id=RoleId("role:workspace.viewer"),
        scope=scope,  # type: ignore[arg-type]
        action="granted",
        binding_id=RoleBindingId("b-1"),
    )


def test_scope_kind_workspace() -> None:
    event = _event(WorkspaceScope(workspace_id=WorkspaceId("w")))
    assert event.scope_kind == "workspace"


def test_scope_kind_tenant() -> None:
    event = _event(TenantScope(tenant_id=TenantId("t")))
    assert event.scope_kind == "tenant"


def test_scope_kind_platform() -> None:
    event = _event(GlobalScope())
    assert event.scope_kind == "platform"


async def test_noop_publisher_does_not_raise() -> None:
    publisher = NoOpBindingChangedPublisher()
    await publisher.publish(_event(GlobalScope()))


async def test_recording_publisher_captures_in_order() -> None:
    publisher = RecordingBindingChangedPublisher()
    granted = _event(GlobalScope())
    revoked = BindingChangedEvent(
        principal_id=PrincipalId("user-1"),
        role_id=RoleId("role:workspace.viewer"),
        scope=GlobalScope(),
        action="revoked",
        binding_id=RoleBindingId("b-1"),
    )
    await publisher.publish(granted)
    await publisher.publish(revoked)
    assert publisher.published == [granted, revoked]


def test_binding_changed_event_action_literal() -> None:
    # Construction with arbitrary strings is rejected by mypy but
    # accepted at runtime — this test is a smoke for the runtime path.
    event = _event(GlobalScope())
    assert event.action in ("granted", "revoked")


def test_recording_publisher_starts_empty() -> None:
    publisher = RecordingBindingChangedPublisher()
    assert publisher.published == []


@pytest.mark.parametrize(
    "scope, expected",
    [
        (WorkspaceScope(workspace_id=WorkspaceId("w")), "workspace"),
        (TenantScope(tenant_id=TenantId("t")), "tenant"),
        (GlobalScope(), "platform"),
    ],
)
def test_scope_kind_parametrised(scope: object, expected: str) -> None:
    assert _event(scope).scope_kind == expected


# ---------------------------------------------------------------------------
# Subscriber Protocol (AS-IMPL-012)
# ---------------------------------------------------------------------------


async def test_noop_subscriber_records_handler_without_delivering() -> None:
    # The no-op subscriber is the M1 single-replica default. It must
    # accept a handler registration (so the lifespan startup path
    # works uniformly) but never deliver an event — cross-pod
    # invalidation is moot with one pod.
    subscriber = NoOpBindingChangedSubscriber()

    async def _handler(event: BindingChangedEvent) -> None:  # pragma: no cover
        raise AssertionError("no-op subscriber must not deliver events")

    await subscriber.start(_handler)
    assert subscriber.started is True
    assert subscriber.handler is _handler
    await subscriber.stop()
    assert subscriber.stopped is True


async def test_recording_subscriber_delivers_on_demand() -> None:
    subscriber = RecordingBindingChangedSubscriber()
    received: list[BindingChangedEvent] = []

    async def _handler(event: BindingChangedEvent) -> None:
        received.append(event)

    await subscriber.start(_handler)
    event = _event(GlobalScope())
    await subscriber.deliver(event)
    assert received == [event]


async def test_recording_subscriber_deliver_before_start_raises() -> None:
    subscriber = RecordingBindingChangedSubscriber()
    with pytest.raises(RuntimeError):
        await subscriber.deliver(_event(GlobalScope()))


# ---------------------------------------------------------------------------
# LocalBindingChangedBus (AS-IMPL-012)
# ---------------------------------------------------------------------------


async def test_local_bus_synchronously_fans_out_to_subscribed_handlers() -> None:
    # The single-replica deployment uses LocalBindingChangedBus as
    # both publisher and subscriber container so the cache
    # invalidation runs on the same replica that performed the
    # binding mutation. The handlers fire in subscription order.
    bus = LocalBindingChangedBus()
    seen: list[tuple[str, BindingChangedEvent]] = []

    async def _h1(event: BindingChangedEvent) -> None:
        seen.append(("h1", event))

    async def _h2(event: BindingChangedEvent) -> None:
        seen.append(("h2", event))

    bus.subscribe(_h1)
    bus.subscribe(_h2)
    event = _event(GlobalScope())
    await bus.publish(event)
    assert seen == [("h1", event), ("h2", event)]


async def test_local_bus_continues_when_a_handler_raises() -> None:
    # A misbehaving handler must not break the publish path; the
    # binding mutation has already committed. Other handlers still
    # observe the event and the publish call returns normally.
    bus = LocalBindingChangedBus()
    delivered: list[BindingChangedEvent] = []

    async def _broken(event: BindingChangedEvent) -> None:
        raise RuntimeError("boom")

    async def _good(event: BindingChangedEvent) -> None:
        delivered.append(event)

    bus.subscribe(_broken)
    bus.subscribe(_good)
    event = _event(GlobalScope())
    await bus.publish(event)
    assert delivered == [event]


async def test_local_bus_publish_with_no_subscribers_is_silent() -> None:
    bus = LocalBindingChangedBus()
    await bus.publish(_event(GlobalScope()))
