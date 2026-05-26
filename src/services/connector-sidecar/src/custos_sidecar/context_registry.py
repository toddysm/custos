"""Slot \u2192 ConnectorContext registry (CONN-IMPL-019).

ARM mints the ``ConnectorContexts`` per step (via Workflow Service's
``BindForStep`` call to Connector Service) and passes the resolved
named slots to the sidecar at pod start. The sidecar holds the map
in-memory for its lifetime; lookups are by slot name.

Each slot carries the connector instance metadata the sidecar needs to:

* Validate the activity's ``purpose`` query parameter against the
  slot's declared capabilities (else ``capability-forbidden``).
* Forward the connector-instance id to the Lease Gateway issue call.
* Surface the upstream endpoint + per-connector ``extras`` in the
  token envelope returned to the activity.

The registry is constructed once at startup from
:meth:`SlotContext.from_wire`-ready data. Tests build it inline; the
``__main__`` entry point loads it from a JSON env var (the
``CUSTOS_SIDECAR_CONTEXTS`` settings field).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from custos_sidecar.errors import SidecarError, SidecarErrorCode


@dataclass(frozen=True, slots=True)
class SlotContext:
    """A single slot the sidecar can serve.

    Attributes:
        slot: The slot name (matches the Workflow Service step's slot key).
        connector_instance_id: ULID/UUID identifying the connector
            instance backing this slot.
        capabilities: Tuple of capability tokens this slot is bound to.
            Activity ``purpose`` values must be members.
        endpoint: Upstream endpoint URL surfaced to the activity in the
            token envelope (e.g. an OCI registry URL).
        token_type: The credential type the upstream expects (e.g.
            ``Bearer`` or ``AWS-Sig-V4``). Forwarded verbatim to the
            Lease Manager and to the token envelope.
        extras: Opaque per-connector-type metadata. Passed through to
            the activity unchanged.
    """

    slot: str
    connector_instance_id: str
    capabilities: tuple[str, ...]
    endpoint: str
    token_type: str
    extras: Mapping[str, Any]

    @classmethod
    def from_wire(cls, wire: dict[str, Any]) -> SlotContext:
        """Decode the JSON envelope ARM seeds via the sidecar settings."""
        return cls(
            slot=str(wire["slot"]),
            connector_instance_id=str(wire["connectorInstanceId"]),
            capabilities=tuple(str(c) for c in wire["capabilities"]),
            endpoint=str(wire["endpoint"]),
            token_type=str(wire["tokenType"]),
            extras=dict(wire.get("extras", {})),
        )


class ContextRegistry:
    """Immutable slot-name \u2192 :class:`SlotContext` map.

    Construction validates that slot names are unique and non-empty;
    lookups raise :class:`SidecarError(SLOT_NOT_FOUND)` and
    :class:`SidecarError(CAPABILITY_FORBIDDEN)` so the router can fan
    them straight into RFC 7807 problem documents.
    """

    def __init__(self, contexts: Iterable[SlotContext]) -> None:
        contexts_list = list(contexts)
        seen: set[str] = set()
        for ctx in contexts_list:
            if not ctx.slot:
                raise ValueError("SlotContext.slot must be non-empty")
            if ctx.slot in seen:
                raise ValueError(f"duplicate slot name in ContextRegistry: {ctx.slot!r}")
            seen.add(ctx.slot)
        self._by_slot: dict[str, SlotContext] = {c.slot: c for c in contexts_list}

    @classmethod
    def from_wire(cls, wire_list: list[dict[str, Any]]) -> ContextRegistry:
        """Build a registry from the JSON envelope ARM seeds.

        Used by the production ``__main__`` to decode the
        ``CUSTOS_SIDECAR_CONTEXTS`` env var; tests can either call this
        or build :class:`SlotContext` instances directly.
        """
        return cls(SlotContext.from_wire(w) for w in wire_list)

    def slot_names(self) -> tuple[str, ...]:
        """Return the registered slot names (insertion order)."""
        return tuple(self._by_slot.keys())

    def resolve(self, slot: str, *, purpose: str) -> SlotContext:
        """Look up ``slot`` and verify ``purpose`` is a declared capability.

        Raises :class:`SidecarError(SLOT_NOT_FOUND)` when the slot is
        unknown and :class:`SidecarError(CAPABILITY_FORBIDDEN)` when
        the purpose is not declared. The two cases are distinguished
        so the activity's logs / metrics can tell "operator wired the
        wrong slot" from "activity asked for a capability the step
        manifest does not authorize".
        """
        ctx = self._by_slot.get(slot)
        if ctx is None:
            raise SidecarError(
                SidecarErrorCode.SLOT_NOT_FOUND,
                f"slot {slot!r} is not bound by this sidecar; known slots: {self.slot_names()}",
            )
        if purpose not in ctx.capabilities:
            raise SidecarError(
                SidecarErrorCode.CAPABILITY_FORBIDDEN,
                f"slot {slot!r} does not declare capability {purpose!r}; "
                f"declared: {ctx.capabilities}",
            )
        return ctx


__all__ = ["ContextRegistry", "SlotContext"]
