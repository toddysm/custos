"""Internal workflow-event receiver (TS-IMPL-017, REQ-080 / REQ-081 / design
``§ Internal workflow-to-workflow trigger``).

The Workflow Service publishes lifecycle events to the ``custos.workflow.events``
Dapr Pub/Sub topic (``DaprPubSubLifecyclePublisher``). This receiver subscribes
to that topic via Dapr's programmatic subscription route (``GET /dapr/subscribe``)
and, for each delivered event, runs the full match/dispatch pipeline so a single
``workflow.completed`` can fan out two ways at once:

* **start** — a downstream start subscription whose CEL selector matches the
  event kicks off a chained workflow (``StartRun``); and
* **resume** — a parent workflow waiting on the child via ``waitFor:`` is
  resumed (``RaiseExternalEvent``).

Candidate lookup
----------------

The start and resume matchers are pure functions over a caller-supplied
candidate list (the locked SPL exposes no query surface), so this receiver owns
candidate enumeration:

* **start** subscriptions have no deterministic key — the receiver enumerates
  every subscription in the event's tenant ``workspace`` via the
  :class:`~custos_trigger.stores.base.SubscriptionListable` capability and lets
  :class:`~custos_trigger.pipeline.match_start.StartMatcher` filter to
  ``START`` / ``ACTIVE`` rows whose selector matches.
* **resume** registrations *do* have a deterministic key: ``resume_id`` is
  derived from the event's ``(runId, stepId, eventKey)`` triple
  (:func:`~custos_trigger.api.routes.rpc.compute_resume_id`). The receiver
  point-reads the single candidate under the
  :data:`~custos_trigger.api.routes.rpc.RESUME_WORKSPACE` partition rather than
  scanning, skipping a lapsed (TTL-expired) registration.

At-least-once + fan-out
-----------------------

Dapr delivers at-least-once. Duplicate deliveries normalize to the same
deterministic ``eventId`` (``normalize_workflow_event`` derives it from the
producer's ``(runId, kind, occurredAt)`` triple), so the dispatcher's dedup
window collapses the replay — the receiver itself is stateless. Inbound events
enter the pipeline at :data:`_INBOUND_DEPTH` (``0``); the dispatcher enforces
``TRIGGER_FANOUT_MAX_DEPTH`` per dispatch (a runaway chain dead-letters as
``trigger.loop.detected``). The Pub/Sub envelope carries no cross-event depth
field, so depth does not accumulate across the broker — that is a Workflow
Service responsibility and is out of scope here.

The receiver and its ``/dapr/subscribe`` route are *internal* control-plane
surfaces authenticated at the Dapr mesh layer; they carry no call-context
envelope and the call-context middleware bypasses both paths (see
:mod:`custos_trigger.middleware.callctx`).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Any, Final

from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict, Field
from starlette.responses import JSONResponse

from custos_trigger.api.routes.rpc import RESUME_WORKSPACE, compute_resume_id
from custos_trigger.dependencies import (
    get_dispatcher,
    get_resume_subscription_store,
    get_selector_evaluator,
    get_subscription_store,
)
from custos_trigger.events import NormalizedEvent
from custos_trigger.normalize import EventNormalizationError, normalize_workflow_event
from custos_trigger.pipeline import (
    ResumeCandidate,
    ResumeMatcher,
    StartMatcher,
    classify,
    resume_key_from_event,
)
from custos_trigger.pipeline.dispatch import Dispatcher, DispatchOutcome
from custos_trigger.selector import SelectorEvaluator
from custos_trigger.stores import ResumeSubscriptionStore, SubscriptionStore
from custos_trigger.taxonomy import InvalidKindError

logger = logging.getLogger(__name__)

__all__ = [
    "DAPR_SUBSCRIBE_PATH",
    "INTERNAL_EVENTS_PATH",
    "DeliveryStatus",
    "InternalEventOutcome",
    "build_internal_event_router",
    "process_workflow_event",
]

#: Dapr's fixed programmatic-subscription discovery route. Dapr calls this on
#: sidecar start to learn which ``(pubsub, topic)`` the app subscribes to.
DAPR_SUBSCRIBE_PATH: Final[str] = "/dapr/subscribe"

#: Delivery route Dapr POSTs each ``custos.workflow.events`` message to. The
#: call-context middleware bypass set must list this verbatim — Dapr Pub/Sub
#: deliveries carry no call-context header.
INTERNAL_EVENTS_PATH: Final[str] = "/internal/events/workflow"

#: Inbound Pub/Sub events enter the pipeline at depth 0; the dispatcher enforces
#: the per-dispatch fan-out cap from here. The envelope carries no cross-event
#: depth, so depth does not accumulate across the broker (see module docstring).
_INBOUND_DEPTH: Final[int] = 0

#: Tenant workspace key inside the normalized event's ``data`` map.
_DATA_WORKSPACE: Final[str] = "workspace"


class DeliveryStatus(StrEnum):
    """Dapr Pub/Sub ack statuses returned in the delivery response body.

    Dapr reads ``{"status": <value>}`` to decide redelivery: ``SUCCESS`` acks,
    ``RETRY`` redelivers with backoff, ``DROP`` discards without retry.
    """

    SUCCESS = "SUCCESS"
    RETRY = "RETRY"
    DROP = "DROP"


def _now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class InternalEventOutcome:
    """The dispatch outcomes a single internal event produced.

    Both arms can be non-empty for one event (a ``workflow.completed`` that
    chains a downstream workflow *and* resumes a waiting parent).
    """

    start: tuple[DispatchOutcome, ...] = ()
    resume: tuple[DispatchOutcome, ...] = ()


class _CloudEventEnvelope(BaseModel):
    """The CloudEvents wrapper Dapr delivers; the workflow envelope is ``data``.

    Dapr wraps published payloads in a CloudEvents 1.0 envelope by default and
    nests the original message under ``data``. Only ``data`` is consumed here;
    the remaining CloudEvents attributes (``id``, ``source``, ``type``, ...) are
    accepted and ignored.
    """

    model_config = ConfigDict(extra="allow")

    data: dict[str, Any] = Field(default_factory=dict)


async def process_workflow_event(
    event: NormalizedEvent,
    *,
    dispatcher: Dispatcher,
    evaluator: SelectorEvaluator,
    subscription_store: SubscriptionStore,
    resume_store: ResumeSubscriptionStore,
    now: Callable[[], datetime] | None = None,
) -> InternalEventOutcome:
    """Run *event* through the classify -> match -> dispatch pipeline.

    Returns the start and resume :class:`DispatchOutcome` lists. An event with
    no tenant ``workspace`` is unroutable (start subscriptions are
    workspace-partitioned and a resume dispatch needs the tenant context) and
    yields an empty outcome.
    """
    clock = now if now is not None else _now
    workspace = event.data.get(_DATA_WORKSPACE)
    if not isinstance(workspace, str) or not workspace:
        logger.warning("internal event %s carries no workspace; nothing to route", event.event_id)
        return InternalEventOutcome()

    classification = classify(event)
    start_outcomes: list[DispatchOutcome] = []
    resume_outcomes: list[DispatchOutcome] = []

    if classification.to_start:
        # Only start subscriptions whose declared source class matches the
        # event's origin are eligible. Workflow lifecycle events are
        # ``SourceType.INTERNAL``; a manual/webhook/cron start subscription must
        # be fired by its own receiver, never by a lifecycle event whose
        # selector happens to match (or is unconditional).
        candidates = [
            sub
            for sub in await subscription_store.list_in_workspace(workspace)
            if sub.source_type is event.source.type
        ]
        for match in StartMatcher(evaluator).match(event, candidates):
            start_outcomes.append(
                await dispatcher.dispatch_start(event, match, depth=_INBOUND_DEPTH)
            )

    if classification.to_resume:
        resume_outcomes.extend(
            await _dispatch_resumes(
                event,
                workspace=workspace,
                dispatcher=dispatcher,
                evaluator=evaluator,
                resume_store=resume_store,
                now=clock,
            )
        )

    return InternalEventOutcome(start=tuple(start_outcomes), resume=tuple(resume_outcomes))


async def _dispatch_resumes(
    event: NormalizedEvent,
    *,
    workspace: str,
    dispatcher: Dispatcher,
    evaluator: SelectorEvaluator,
    resume_store: ResumeSubscriptionStore,
    now: Callable[[], datetime],
) -> list[DispatchOutcome]:
    """Point-read the deterministic resume candidate and dispatch any match."""
    key = resume_key_from_event(event)
    if key is None:
        return []
    resume_id = compute_resume_id(key.run_id, key.step_id, key.event_key)
    stored = await resume_store.get(RESUME_WORKSPACE, resume_id)
    if stored is None:
        return []
    if stored.expires_at <= now():
        logger.debug("resume %s lapsed before internal event %s arrived", resume_id, event.event_id)
        return []
    candidate = ResumeCandidate(resume_id=resume_id, registration=stored.registration)
    return [
        await dispatcher.dispatch_resume(event, match, workspace_id=workspace, depth=_INBOUND_DEPTH)
        for match in ResumeMatcher(evaluator).match(event, [candidate])
    ]


def _ack(status: DeliveryStatus) -> JSONResponse:
    """Build the Dapr Pub/Sub ack response carrying *status*."""
    return JSONResponse(content={"status": status.value})


DispatcherDep = Annotated[Dispatcher, Depends(get_dispatcher)]
EvaluatorDep = Annotated[SelectorEvaluator, Depends(get_selector_evaluator)]
SubscriptionStoreDep = Annotated[SubscriptionStore, Depends(get_subscription_store)]
ResumeStoreDep = Annotated[ResumeSubscriptionStore, Depends(get_resume_subscription_store)]


def build_internal_event_router(*, pubsub_component: str, workflow_events_topic: str) -> APIRouter:
    """Build the internal-event router bound to a ``(pubsub, topic)`` pair.

    The router is a factory (rather than a module singleton) because the
    ``/dapr/subscribe`` payload closes over the configured Pub/Sub component and
    topic, which the host resolves from settings at app-wiring time
    (TS-IMPL-018).
    """
    router = APIRouter(tags=["internal-events"])

    @router.get(DAPR_SUBSCRIBE_PATH)
    async def dapr_subscribe() -> list[dict[str, Any]]:
        """Declare the workflow-events subscription to the Dapr sidecar."""
        return [
            {
                "pubsubname": pubsub_component,
                "topic": workflow_events_topic,
                "route": INTERNAL_EVENTS_PATH,
                "metadata": {},
            }
        ]

    @router.post(INTERNAL_EVENTS_PATH)
    async def receive_workflow_event(
        envelope: _CloudEventEnvelope,
        dispatcher: DispatcherDep,
        evaluator: EvaluatorDep,
        subscription_store: SubscriptionStoreDep,
        resume_store: ResumeStoreDep,
    ) -> JSONResponse:
        """Normalize and route one delivered ``custos.workflow.events`` message.

        A malformed / non-canonical envelope is dropped (retrying cannot fix it).
        Dispatch failures are terminal-handled inside the dispatcher (dead-letter
        + audit), so a routed event always acks ``SUCCESS`` — re-delivering it
        would only re-run already-succeeded matches, which the dedup window
        absorbs anyway.
        """
        try:
            event = normalize_workflow_event(envelope.data)
        except (EventNormalizationError, InvalidKindError) as exc:
            logger.warning("dropping unroutable workflow event: %s", exc)
            return _ack(DeliveryStatus.DROP)
        await process_workflow_event(
            event,
            dispatcher=dispatcher,
            evaluator=evaluator,
            subscription_store=subscription_store,
            resume_store=resume_store,
        )
        return _ack(DeliveryStatus.SUCCESS)

    return router
