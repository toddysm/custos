"""Cross-pod cache-invalidation bus for role-binding mutations.

Phase D / AS-IMPL-010 ships only the publisher contract and a no-op
default implementation. AS-IMPL-012 (Phase E) wires the consumer side
that drives the per-pod authorize cache invalidation.

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
from dataclasses import dataclass
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


__all__ = [
    "BindingChangeAction",
    "BindingChangedEvent",
    "BindingChangedPublisher",
    "NoOpBindingChangedPublisher",
    "RecordingBindingChangedPublisher",
]
