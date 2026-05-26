"""Request and response models for the ``BindForStep`` RPC.

The wire shape is intentionally close to the design's sequence-diagram
language: a step coordinate (``run_id`` / ``step_id`` / ``attempt`` /
``step_key``) plus a list of named slots, each carrying a connector
instance reference and the capabilities the step will invoke through
that slot. The response is the same slot map, keyed by name, with each
slot's resolved :class:`~custos_connector.runtime.ConnectorContext`
inline.

These are dataclasses (not pydantic models) for two reasons:

1. The service layer (:mod:`custos_connector.binding.service`) is the
   only consumer and benefits from frozen, slotted shapes.
2. The router layer (:mod:`custos_connector.binding.router`) does its
   own pydantic validation against the FastAPI request body and then
   adapts to these dataclasses; keeping the two layers decoupled means
   the service contract is testable without spinning up an HTTP client.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from custos_connector.runtime import ConnectorContext


@dataclass(frozen=True, slots=True)
class BindSlotRequest:
    """A single slot to bind in a ``BindForStep`` call.

    Attributes:
        name: Slot name from the activity manifest (e.g. ``"source"``
            or ``"destination"``). The response uses the same name as
            the key for the resolved context.
        instance_id: The ``ConnectorInstance`` to bind to. Resolved in
            the caller's workspace; cross-workspace references fail
            with :class:`~custos_connector.binding.errors.BindErrorCode.INSTANCE_NOT_FOUND`.
        required_capabilities: Capability tokens the step will invoke
            through this slot. The binder validates
            ``required_capabilities ⊆ instance.used_capabilities`` and
            uses the first element as the ``capability`` parameter for
            the plugin's ``bind`` hook. An empty sequence is a request
            error.
    """

    name: str
    instance_id: str
    required_capabilities: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class BindForStepRequest:
    """The ``BindForStep`` RPC input.

    Attributes:
        run_id: Workflow run UUID. Carried into the idempotency key,
            the audit subject, and the plugin bind hook context.
        step_id: Step UUID within ``run_id``.
        attempt: 1-based attempt counter. A retry for the same
            ``(run_id, step_id, attempt)`` returns the same context
            handles (in-memory idempotency, v1 limitation — see
            :class:`~custos_connector.binding.service.BindForStepService`).
        step_key: Stable activity-manifest key for the step (e.g.
            ``"copy.v1"``). Surfaced in the audit subject so operators
            can correlate binds across runs.
        slots: Non-empty sequence of slots to bind. Order is preserved
            in the response. Duplicate slot names are a request error.
        actor: Caller identity for audit emission. Defaults to a
            ``"workflow-service"`` sentinel when not supplied — the
            router layer fills this in from the call-context token.
    """

    run_id: str
    step_id: str
    attempt: int
    step_key: str
    slots: tuple[BindSlotRequest, ...]
    actor: str = "workflow-service"


@dataclass(frozen=True, slots=True)
class BindForStepResponse:
    """The ``BindForStep`` RPC output.

    Attributes:
        contexts: Resolved slot-name → :class:`ConnectorContext` map,
            preserving the input slot order. The mapping is frozen so
            downstream consumers cannot mutate the cached value.
    """

    contexts: Mapping[str, ConnectorContext]

    @classmethod
    def build(
        cls,
        contexts: Sequence[tuple[str, ConnectorContext]],
    ) -> BindForStepResponse:
        """Freeze the slot list into an ordered, immutable mapping."""
        return cls(contexts=MappingProxyType(dict(contexts)))


def freeze_request_payload(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    """Helper for tests: freeze an arbitrary dict into a read-only view."""
    return MappingProxyType(dict(payload))


__all__ = [
    "BindForStepRequest",
    "BindForStepResponse",
    "BindSlotRequest",
    "freeze_request_payload",
]
