"""Cross-pod cache-invalidation bus for service-token revocations.

AS-IMPL-014 introduces a second bus alongside the binding-changed
bus shipped by AS-IMPL-010 / 012. The structure mirrors
:mod:`custos_auth.binding_events` so operators learn the pattern
once: a Protocol for publishers, a Protocol for subscribers, a
no-op default for single-replica deployments, a recording variant
for tests, and an in-process ``LocalTokenRevokedBus`` that satisfies
the M1 single-replica acceptance criterion ("revoke + immediate re-
verify returns 401 within one round trip") without standing up a
real transport.

Publish semantics
-----------------

The revoke HTTP handler publishes exactly **one**
:class:`TokenRevokedEvent` per successful revoke transaction. The
publish call happens **after** the SPL ``revoke_service_token``
mutation succeeds — never before — so a rolled-back revoke never
produces a phantom eviction. A failure to publish is logged at
WARNING but does not roll back the (already-committed) revoke; the
authn cache recovers on TTL.

Payload shape
-------------

The event carries:

* ``token_id`` — the operator-facing identifier. The
  :class:`~custos_auth.authn_cache.AuthnCache` indexes by token id
  in addition to hash so it can resolve the row without the hash
  ever crossing the bus.
* ``token_hash`` — the SHA-256 hex digest. Included so caches with
  out-of-band hash knowledge (e.g. the verify path that just
  finished a miss) can evict in O(1) without consulting the
  reverse index.
* ``service_account_id`` — surfaced purely for observability; the
  cache eviction path does not need it.

Including the hash on the bus is acceptable because the hash is
**not** a credential — it is the persisted storage representation
and possessing it does not let an attacker authenticate; only the
plaintext bearer does, and the plaintext is never logged or
persisted past the mint response. The workspace authn audit trail
still records the token lifecycle action (for example, identifiers
such as ``token_id`` and ``service_account_id``), but intentionally
omits both the plaintext bearer and the token hash.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Protocol

_LOGGER = logging.getLogger("custos_auth.token_revoked_events")


@dataclass(frozen=True, slots=True)
class TokenRevokedEvent:
    """Payload published when a service token is revoked.

    Subscribers (the per-pod :class:`~custos_auth.authn_cache.AuthnCache`
    in AS-IMPL-014) key on ``token_id`` or ``token_hash`` to
    invalidate the cached verify row.
    """

    token_id: str
    token_hash: str
    service_account_id: str


class TokenRevokedPublisher(Protocol):
    """Cache-invalidation publisher for service-token revocations."""

    async def publish(self, event: TokenRevokedEvent) -> None:
        """Publish exactly one cache-invalidation event.

        Implementations MUST be best-effort: any failure to reach the
        downstream transport is the caller's problem only insofar as
        it should be logged; the revoke has already committed.
        """
        ...


class NoOpTokenRevokedPublisher:
    """Default publisher used in dev / single-replica deployments.

    Logs each event at INFO so operators can grep the audit trail
    and otherwise does nothing.
    """

    async def publish(self, event: TokenRevokedEvent) -> None:
        _LOGGER.info(
            "token-revoked event token_id=%s sa=%s",
            event.token_id,
            event.service_account_id,
        )


class RecordingTokenRevokedPublisher:
    """Test-only publisher that captures every event in ``published``."""

    def __init__(self) -> None:
        self.published: list[TokenRevokedEvent] = []

    async def publish(self, event: TokenRevokedEvent) -> None:
        self.published.append(event)


#: Signature of a token-revoked event handler.
TokenRevokedHandler = Callable[[TokenRevokedEvent], Awaitable[None]]


class TokenRevokedSubscriber(Protocol):
    """Cross-replica subscriber to the token-revoked event stream.

    The M1 single-replica deployment ships with the no-op subscriber
    (no second pod, no second cache, nothing to invalidate). The
    multi-replica deployment swaps in a Dapr Pub/Sub or SPL-outbox-
    backed implementation that delivers every event published on any
    replica to the local handler on this one.

    Implementations MUST:

    * deliver each event at-most-once-per-replica (duplicate delivery
      is harmless because invalidation is idempotent),
    * survive transient transport errors without dropping the
      handler registration,
    * stop cleanly when :meth:`stop` is awaited.
    """

    async def start(self, handler: TokenRevokedHandler) -> None:
        """Begin delivering events to ``handler``.

        Called exactly once from the FastAPI lifespan startup.
        """
        ...

    async def stop(self) -> None:
        """Stop delivery and release any background tasks."""
        ...


class NoOpTokenRevokedSubscriber:
    """Default subscriber for single-replica deployments.

    Records the handler so it can be inspected from tests, but never
    delivers an event. Cross-pod invalidation is moot when there is
    only one pod — the :class:`LocalTokenRevokedBus` already delivers
    locally on the same replica that published.
    """

    def __init__(self) -> None:
        self.handler: TokenRevokedHandler | None = None
        self.started: bool = False
        self.stopped: bool = False

    async def start(self, handler: TokenRevokedHandler) -> None:
        self.handler = handler
        self.started = True

    async def stop(self) -> None:
        self.stopped = True


class RecordingTokenRevokedSubscriber:
    """Test-only subscriber that exposes a manual ``deliver`` hook.

    Tests construct one of these, wire it via
    :class:`~custos_auth.providers.Providers.token_revoked_subscriber`,
    then call :meth:`deliver` to simulate a cross-replica event
    arrival and assert the wired handler updates the cache.
    """

    def __init__(self) -> None:
        self.handler: TokenRevokedHandler | None = None
        self.started: bool = False
        self.stopped: bool = False

    async def start(self, handler: TokenRevokedHandler) -> None:
        self.handler = handler
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def deliver(self, event: TokenRevokedEvent) -> None:
        """Simulate a cross-replica delivery of ``event``."""
        if self.handler is None:
            raise RuntimeError(
                "RecordingTokenRevokedSubscriber.deliver: start() must be called before deliver()"
            )
        await self.handler(event)


@dataclass(slots=True)
class LocalTokenRevokedBus:
    """In-process publisher that synchronously fans out to local handlers.

    Plays two roles:

    * Implements :class:`TokenRevokedPublisher` so the revoke route
      handler can publish through the same interface it uses today.
    * Holds a list of subscribed handlers that fire synchronously on
      :meth:`publish`. This delivers token-revoked events to the
      per-pod authn cache on the same replica that performed the
      revoke without standing up a real transport.

    Handlers that raise are logged at WARNING and skipped so one
    misbehaving consumer cannot break the publish path (the revoke
    has already committed; the cache recovers on TTL).
    """

    handlers: list[TokenRevokedHandler] = field(default_factory=list)

    def subscribe(self, handler: TokenRevokedHandler) -> None:
        """Register ``handler`` to receive every published event."""
        self.handlers.append(handler)

    async def publish(self, event: TokenRevokedEvent) -> None:
        """Deliver ``event`` to every subscribed handler in order."""
        _LOGGER.info(
            "token-revoked event token_id=%s sa=%s",
            event.token_id,
            event.service_account_id,
        )
        for handler in self.handlers:
            try:
                await handler(event)
            except Exception:  # guard the publish path
                _LOGGER.warning(
                    "token-revoked handler raised; continuing",
                    exc_info=True,
                )


__all__ = [
    "LocalTokenRevokedBus",
    "NoOpTokenRevokedPublisher",
    "NoOpTokenRevokedSubscriber",
    "RecordingTokenRevokedPublisher",
    "RecordingTokenRevokedSubscriber",
    "TokenRevokedEvent",
    "TokenRevokedHandler",
    "TokenRevokedPublisher",
    "TokenRevokedSubscriber",
]
