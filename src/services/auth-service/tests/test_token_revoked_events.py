"""Tests for :mod:`custos_auth.token_revoked_events` (AS-IMPL-014)."""

from __future__ import annotations

import pytest

from custos_auth.token_revoked_events import (
    LocalTokenRevokedBus,
    NoOpTokenRevokedPublisher,
    NoOpTokenRevokedSubscriber,
    RecordingTokenRevokedPublisher,
    RecordingTokenRevokedSubscriber,
    TokenRevokedEvent,
)


def _event(token_id: str = "tok-1") -> TokenRevokedEvent:
    return TokenRevokedEvent(
        token_id=token_id,
        token_hash="hash-" + token_id,
        service_account_id="sa-1",
    )


@pytest.mark.asyncio
async def test_noop_publisher_swallows_event_without_raise() -> None:
    pub = NoOpTokenRevokedPublisher()
    await pub.publish(_event())  # must not raise; no state to inspect


@pytest.mark.asyncio
async def test_recording_publisher_captures_every_event() -> None:
    pub = RecordingTokenRevokedPublisher()
    await pub.publish(_event("tok-A"))
    await pub.publish(_event("tok-B"))
    assert [e.token_id for e in pub.published] == ["tok-A", "tok-B"]


@pytest.mark.asyncio
async def test_local_bus_delivers_synchronously_to_subscribed_handlers() -> None:
    bus = LocalTokenRevokedBus()
    seen: list[TokenRevokedEvent] = []

    async def handler(event: TokenRevokedEvent) -> None:
        seen.append(event)

    bus.subscribe(handler)
    e = _event("tok-1")
    await bus.publish(e)
    assert seen == [e]


@pytest.mark.asyncio
async def test_local_bus_publishes_to_handlers_in_subscription_order() -> None:
    bus = LocalTokenRevokedBus()
    order: list[str] = []

    async def h1(_event: TokenRevokedEvent) -> None:
        order.append("h1")

    async def h2(_event: TokenRevokedEvent) -> None:
        order.append("h2")

    bus.subscribe(h1)
    bus.subscribe(h2)
    await bus.publish(_event())
    assert order == ["h1", "h2"]


@pytest.mark.asyncio
async def test_local_bus_continues_when_a_handler_raises() -> None:
    bus = LocalTokenRevokedBus()
    seen: list[str] = []

    async def boom(_event: TokenRevokedEvent) -> None:
        raise RuntimeError("oops")

    async def witness(event: TokenRevokedEvent) -> None:
        seen.append(event.token_id)

    bus.subscribe(boom)
    bus.subscribe(witness)
    # Publish must not raise — the revoke has already committed and
    # the bus must not propagate a handler bug back into the route.
    await bus.publish(_event("tok-survives"))
    assert seen == ["tok-survives"]


@pytest.mark.asyncio
async def test_noop_subscriber_records_handler_and_lifecycle_flags() -> None:
    sub = NoOpTokenRevokedSubscriber()
    assert sub.handler is None
    assert sub.started is False

    async def handler(_event: TokenRevokedEvent) -> None: ...

    await sub.start(handler)
    assert sub.handler is handler
    assert sub.started is True
    await sub.stop()
    assert sub.stopped is True


@pytest.mark.asyncio
async def test_recording_subscriber_delivers_events_to_started_handler() -> None:
    sub = RecordingTokenRevokedSubscriber()
    seen: list[TokenRevokedEvent] = []

    async def handler(event: TokenRevokedEvent) -> None:
        seen.append(event)

    await sub.start(handler)
    e = _event("tok-via-subscriber")
    await sub.deliver(e)
    assert seen == [e]


@pytest.mark.asyncio
async def test_recording_subscriber_raises_before_start_is_called() -> None:
    sub = RecordingTokenRevokedSubscriber()
    with pytest.raises(RuntimeError, match="start"):
        await sub.deliver(_event())
