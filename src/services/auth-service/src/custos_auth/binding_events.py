"""Cross-pod cache-invalidation bus for role-binding mutations.

Phase D / AS-IMPL-010 shipped the publisher contract and a no-op
default implementation. AS-IMPL-012 (Phase E) adds:

* a :class:`BindingChangedSubscriber` Protocol for the consumer side
  that drives the per-pod authorize cache invalidation,
* the in-process :class:`LocalBindingChangedBus` that is both a
  publisher and a subscriber container — used by the M1 single-
  replica deployment so the cache invalidates synchronously when the
  role-binding handler publishes, satisfying the
  "revoke-then-recheck within one round trip" acceptance criterion
  without a real pub/sub transport.

Publish semantics
-----------------

The role-binding HTTP handlers publish exactly **one**
:class:`BindingChangedEvent` per successful grant/revoke transaction.
The publish call happens **after** the SPL ``with_transaction`` commit
succeeds — never before — so a rolled-back binding never produces a
cache-invalidation event. A failure to publish is logged at WARNING
but does not roll back the (already-committed) binding mutation; the
caching layer recovers via its TTL.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Literal, Protocol

from custos_spl.interfaces.auth_store import RoleBindingScope

from custos_auth.roles import scope_kind

_LOGGER = logging.getLogger("custos_auth.binding_events")


#: Action discriminator on a :class:`BindingChangedEvent`.
BindingChangeAction = Literal["granted", "revoked"]


@dataclass(frozen=True, slots=True)
class BindingChangedEvent:
    """Payload published when a role binding is granted or revoked.

    Subscribers (the per-pod authorize cache in AS-IMPL-012) key on
    ``principal_id`` + the canonical :data:`scope_kind` to invalidate
    only the affected cache shard.
    """

    principal_id: str
    role_id: str
    scope: RoleBindingScope
    action: BindingChangeAction
    binding_id: str

    @property
    def scope_kind(self) -> str:
        """Canonical ``"workspace" | "tenant" | "platform"`` tag."""
        return scope_kind(self.scope)


class BindingChangedPublisher(Protocol):
    """Cache-invalidation publisher for role-binding mutations."""

    async def publish(self, event: BindingChangedEvent) -> None:
        """Publish exactly one cache-invalidation event.

        Implementations MUST be best-effort: any failure to reach the
        downstream transport (Redis pub/sub, the SPL outbox, …) is
        the caller's problem only insofar as it should be logged; the
        binding has already committed.
        """
        ...


class NoOpBindingChangedPublisher:
    """Default publisher used in dev / single-replica deployments.

    The Phase E subscriber will be wired against a real transport
    (Redis pub/sub or the SPL outbox); until then, a single-replica
    auth-service deployment is the supported configuration and per-pod
    cache invalidation is not needed. The implementation simply logs
    each event at INFO so operators can grep the audit trail.
    """

    async def publish(self, event: BindingChangedEvent) -> None:
        _LOGGER.info(
            "binding-changed event action=%s principal=%s role=%s scope=%s binding=%s",
            event.action,
            event.principal_id,
            event.role_id,
            event.scope_kind,
            event.binding_id,
        )


class RecordingBindingChangedPublisher:
    """Test-only publisher that captures every event in ``published``.

    Used by the role-binding API tests to assert exact-once semantics
    on the binding-changed bus without standing up a real transport.
    """

    def __init__(self) -> None:
        self.published: list[BindingChangedEvent] = []

    async def publish(self, event: BindingChangedEvent) -> None:
        self.published.append(event)


#: Signature of a binding-changed event handler.
#:
#: The subscriber side wires this to the per-pod cache's
#: :meth:`~custos_auth.authz_cache.AuthzDecisionCache.on_binding_changed`.
BindingChangedHandler = Callable[[BindingChangedEvent], Awaitable[None]]


class BindingChangedSubscriber(Protocol):
    """Cross-replica subscriber to the binding-changed event stream.

    The M1 single-replica deployment ships with the no-op subscriber
    (no second pod, no second cache, nothing to invalidate). The
    multi-replica deployment swaps in a Redis pub/sub or SPL-outbox-
    backed implementation that delivers every event published on any
    replica to the local handler on this one.

    Implementations MUST:

    * deliver each event at-most-once-per-replica (duplicate delivery
      is harmless because invalidation is idempotent, but unbounded
      replay defeats the cache),
    * survive transient transport errors without dropping the
      handler registration,
    * stop cleanly when :meth:`stop` is awaited so the lifespan
      shutdown path does not leak background tasks.
    """

    async def start(self, handler: BindingChangedHandler) -> None:
        """Begin delivering events to ``handler``.

        Called exactly once from the FastAPI lifespan startup.
        """
        ...

    async def stop(self) -> None:
        """Stop delivery and release any background tasks.

        Called exactly once from the FastAPI lifespan shutdown.
        """
        ...


class NoOpBindingChangedSubscriber:
    """Default subscriber for single-replica deployments.

    Records the handler so it can be inspected from tests, but never
    delivers an event. Cross-pod invalidation is moot when there is
    only one pod — the :class:`LocalBindingChangedBus` already
    delivers locally on the same replica that published.
    """

    def __init__(self) -> None:
        self.handler: BindingChangedHandler | None = None
        self.started: bool = False
        self.stopped: bool = False

    async def start(self, handler: BindingChangedHandler) -> None:
        self.handler = handler
        self.started = True

    async def stop(self) -> None:
        self.stopped = True


class RecordingBindingChangedSubscriber:
    """Test-only subscriber that exposes a manual ``deliver`` hook.

    Tests construct one of these, wire it via
    :class:`~custos_auth.providers.Providers.binding_changed_subscriber`,
    then call :meth:`deliver` to simulate a cross-replica event
    arrival and assert the wired handler updates the cache.
    """

    def __init__(self) -> None:
        self.handler: BindingChangedHandler | None = None
        self.started: bool = False
        self.stopped: bool = False

    async def start(self, handler: BindingChangedHandler) -> None:
        self.handler = handler
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def deliver(self, event: BindingChangedEvent) -> None:
        """Simulate a cross-replica delivery of ``event``.

        Raises :class:`RuntimeError` if :meth:`start` has not been
        called — the subscriber Protocol requires a registered
        handler before delivery.
        """
        if self.handler is None:
            raise RuntimeError(
                "RecordingBindingChangedSubscriber.deliver: start() must be called before deliver()"
            )
        await self.handler(event)


@dataclass(slots=True)
class LocalBindingChangedBus:
    """In-process publisher that synchronously fans out to local handlers.

    Plays two roles:

    * Implements :class:`BindingChangedPublisher` so the role-binding
      route handlers can publish through the same interface they use
      today.
    * Holds a list of subscribed handlers that fire synchronously on
      :meth:`publish`. This delivers binding-changed events to the
      per-pod cache on the same replica that performed the mutation
      without standing up a real transport — the M1 deployment is
      single-replica, and a single-replica deployment with this bus
      satisfies the AS-IMPL-012 "revoke-then-recheck within one
      round trip" acceptance criterion.

    Handlers that raise are logged at WARNING and skipped so one
    misbehaving consumer cannot break the publish path (the binding
    mutation has already committed; the cache recovers on TTL).
    """

    handlers: list[BindingChangedHandler] = field(default_factory=list)

    def subscribe(self, handler: BindingChangedHandler) -> None:
        """Register ``handler`` to receive every published event.

        Order matches subscription order. The bus is intended for
        single-replica use so subscribers are typically wired once at
        startup from the lifespan hook.
        """
        self.handlers.append(handler)

    async def publish(self, event: BindingChangedEvent) -> None:
        """Deliver ``event`` to every subscribed handler in order.

        Each handler runs inside a guard — a raise from one handler
        is logged and skipped so the next handler still observes the
        event and the publish call does not propagate the failure to
        the role-binding route handler.
        """
        _LOGGER.info(
            "binding-changed event action=%s principal=%s role=%s scope=%s binding=%s",
            event.action,
            event.principal_id,
            event.role_id,
            event.scope_kind,
            event.binding_id,
        )
        for handler in self.handlers:
            try:
                await handler(event)
            except Exception:  # guard the publish path
                _LOGGER.warning(
                    "binding-changed handler raised; continuing",
                    exc_info=True,
                )


__all__ = [
    "BindingChangeAction",
    "BindingChangedEvent",
    "BindingChangedHandler",
    "BindingChangedPublisher",
    "BindingChangedSubscriber",
    "LocalBindingChangedBus",
    "NoOpBindingChangedPublisher",
    "NoOpBindingChangedSubscriber",
    "RecordingBindingChangedPublisher",
    "RecordingBindingChangedSubscriber",
]
