"""Event receivers that feed normalized events into the match/dispatch pipeline.

A *receiver* is an ingress adapter: it accepts an event in some transport's
native shape, normalizes it to a :class:`custos_trigger.events.NormalizedEvent`,
and drives it through classify -> match -> dispatch. The internal receiver
(TS-IMPL-017) consumes the Workflow Service's ``custos.workflow.events`` Dapr
Pub/Sub topic so a completing workflow can both chain a downstream workflow
(start match) and resume a parent waiting on it (resume match).

Other receivers (scheduler, generic webhook, vendor push) are deferred to M2
(see the component design's *Out of scope* section).
"""

from __future__ import annotations

from custos_trigger.receivers.internal import (
    DAPR_SUBSCRIBE_PATH,
    INTERNAL_EVENTS_PATH,
    DeliveryStatus,
    InternalEventOutcome,
    build_internal_event_router,
    process_workflow_event,
)

__all__ = [
    "DAPR_SUBSCRIBE_PATH",
    "INTERNAL_EVENTS_PATH",
    "DeliveryStatus",
    "InternalEventOutcome",
    "build_internal_event_router",
    "process_workflow_event",
]
