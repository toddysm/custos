"""Adapt unchanged connector envelopes and route bounded, tenant-scoped durable deliveries."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Literal

from custos_cel import FixedClock
from custos_spl.errors import SPLError
from custos_spl.interfaces.dead_letters import (
    snapshot_dispatch_object,
    validate_dispatch_identifier,
    validate_dispatch_target,
)
from custos_spl.interfaces.scheduler import SchedulerStateProvider
from custos_spl.pagination import Cursor
from fastapi import APIRouter, Request
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from starlette.responses import JSONResponse

from custos_trigger._wire import WireModel
from custos_trigger.dependencies import (
    get_audit_sink,
    get_dispatcher,
    get_metadata_store,
    get_providers,
    get_resume_subscription_store,
    get_selector_evaluator,
    get_subscription_store,
)
from custos_trigger.errors import TriggerError, TriggerErrorKind
from custos_trigger.events import EventSource, NormalizedEvent
from custos_trigger.input_mapping import resolve_input_mapping
from custos_trigger.models import (
    SourceType,
    SubscriptionKind,
    SubscriptionState,
    resume_registration_from_spl,
)
from custos_trigger.pipeline.classify import classify
from custos_trigger.pipeline.dispatch import AuditSink, Dispatcher
from custos_trigger.pipeline.match_resume import ResumeMatch
from custos_trigger.pipeline.match_start import StartMatch, StartMatcher
from custos_trigger.pipeline.reliable import build_resume_target, build_start_target
from custos_trigger.receivers.internal import DeliveryStatus
from custos_trigger.receivers.scheduled import ScheduledOccurrenceError, process_scheduled_event
from custos_trigger.selector import SelectorEvaluator, SelectorTypeError
from custos_trigger.source_config import SourceConfigurationError
from custos_trigger.stores import ResumeSubscriptionStore, SubscriptionStore

logger = logging.getLogger(__name__)
CONNECTOR_EVENTS_PATH = "/internal/events/connectors"
NORMALIZED_EVENTS_PATH = "/internal/events/normalized"
MAX_INGEST_BYTES = 524_288
SOURCE_PAGE_SIZE = 100


class SourceEventRejected(ValueError):
    """A permanent source-envelope failure carrying only a sanitized reason code."""


class ScopedEvent(WireModel):
    """Private broker routing context; the unchanged normalized event remains nested."""

    schema_version: Literal["1"]
    workspace_id: str = Field(min_length=1)
    event: NormalizedEvent


class _ConnectorSource(BaseModel):
    """The existing Connector producer provenance, independent of payload contents."""

    model_config = ConfigDict(extra="forbid", strict=True)
    workspace_id: str = Field(alias="workspaceId")
    instance_id: str = Field(alias="instanceId")
    delivery_mode: Literal["push", "pull"] = Field(alias="deliveryMode")
    received_at: str = Field(alias="receivedAt")


class _ConnectorEnvelope(BaseModel):
    """Version-one Connector envelope; no producer field or identifier is rewritten."""

    model_config = ConfigDict(extra="forbid", strict=True)
    schema_version: Literal["1"] = Field(alias="schemaVersion")
    event_id: str = Field(alias="eventId")
    event_type: str = Field(alias="eventType")
    source: _ConnectorSource
    payload: dict[str, Any]


def _validate_scoped_event(scoped: ScopedEvent) -> None:
    """Reject unsafe/unretainable input before dispatch can produce any effects."""
    event = scoped.event
    try:
        validate_dispatch_identifier(scoped.workspace_id)
        validate_dispatch_identifier(event.event_id)
        validate_dispatch_identifier(event.kind)
        if datetime.fromisoformat(event.source.occurred_at).utcoffset() is None:
            raise ValueError("unqualified occurrence time")
        if event.raw.body or event.raw.headers:
            raise ValueError("private source transport must be absent")
        if event.source.type is SourceType.SCHEDULED:
            if event.data.get("workspace") != scoped.workspace_id:
                raise ValueError("schedule scope mismatch")
        elif event.source.type in (SourceType.VENDOR_PUSH, SourceType.WEBHOOK, SourceType.PULL):
            if event.source.subscription_id is not None:
                raise ValueError("external subscription routing is forbidden")
            validate_dispatch_identifier(event.source.connector_instance_id or "")
        else:
            raise ValueError("unsupported source")
        snapshot_dispatch_object(event.model_dump(mode="json", by_alias=True, exclude={"raw"}))
    except (ValueError, UnicodeError) as exc:
        raise SourceEventRejected("invalid-or-unretainable-source-event") from exc


def adapt_connector_event(payload: object, *, source_type: SourceType | None = None) -> ScopedEvent:
    """Map Connector eventType/payload/provenance explicitly, preserving IDs and business data."""
    try:
        envelope = _ConnectorEnvelope.model_validate(payload)
        expected = (
            SourceType.PULL if envelope.source.delivery_mode == "pull" else SourceType.VENDOR_PUSH
        )
        selected = expected if source_type is None else source_type
        if selected is not expected and not (
            selected is SourceType.WEBHOOK and envelope.source.delivery_mode == "push"
        ):
            raise ValueError("source and delivery mode disagree")
        scoped = ScopedEvent(
            schema_version="1",
            workspace_id=envelope.source.workspace_id,
            event=NormalizedEvent(
                event_id=envelope.event_id,
                kind=envelope.event_type,
                data=envelope.payload,
                source=EventSource(
                    type=selected,
                    connector_instance_id=envelope.source.instance_id,
                    occurred_at=envelope.source.received_at,
                ),
            ),
        )
        _validate_scoped_event(scoped)
        return scoped
    except (ValidationError, ValueError, UnicodeError) as exc:
        raise SourceEventRejected("invalid-connector-envelope") from exc


async def ingest_source_event(
    scoped: ScopedEvent,
    *,
    dispatcher: Dispatcher,
    evaluator: SelectorEvaluator,
    subscriptions: SubscriptionStore,
    resumes: ResumeSubscriptionStore,
    audit: AuditSink,
    scheduler: SchedulerStateProvider,
) -> None:
    """Acknowledge only after every match succeeds, deduplicates or is durably captured."""
    _validate_scoped_event(scoped)
    reliable = dispatcher.reliable
    if reliable is None:
        raise TriggerError(
            TriggerErrorKind.SOURCE_UNAVAILABLE, "source ingestion requires durable dispatch"
        )
    event, workspace = scoped.event, scoped.workspace_id
    if event.source.type is SourceType.SCHEDULED:
        await process_scheduled_event(
            event,
            subscription_store=subscriptions,
            provider=scheduler,
            evaluator=evaluator,
            dispatcher=dispatcher,
            audit=audit,
        )
        return
    connector = event.source.connector_instance_id
    assert connector is not None
    delivery_mode: Literal["push", "pull"] = (
        "pull" if event.source.type is SourceType.PULL else "push"
    )
    classification = classify(event)
    if classification.to_start:
        cursor: Cursor | None = None
        while True:
            page = await subscriptions.query(
                workspace, state=SubscriptionState.ACTIVE, cursor=cursor, limit=SOURCE_PAGE_SIZE
            )
            candidates = [
                sub
                for sub in page.items
                if sub.kind is SubscriptionKind.START
                and sub.source_type is event.source.type
                and sub.source_config is not None
                and sub.source_config.connector_instance_id == connector
            ]
            for match in StartMatcher(evaluator).match(event, candidates):
                sub = match.subscription
                current = await subscriptions.get(workspace, sub.subscription_id)
                if current is None or current.state is not SubscriptionState.ACTIVE:
                    logger.info("Skipping inactive source subscription: %s", sub.subscription_id)
                    continue
                if current.store_revision != sub.store_revision:
                    raise TriggerError(
                        TriggerErrorKind.DISPATCH_UNAVAILABLE,
                        "active source subscription changed before delivery; retry it",
                        details={"subscriptionId": sub.subscription_id},
                    )
                mapped = resolve_input_mapping(
                    sub.input_mapping,
                    event,
                    evaluator=evaluator,
                    subscription_id=sub.subscription_id,
                    source_type=sub.source_type,
                )
                resolved = sub.model_copy(update={"input_mapping": mapped})
                try:
                    validate_dispatch_target(
                        build_start_target(resolved), allow_unresolved_version=True
                    )
                except (ValueError, UnicodeError) as exc:
                    raise SourceConfigurationError(
                        "mapped source inputs cannot be retained"
                    ) from exc
                await dispatcher.dispatch_start(event, StartMatch(resolved))
            cursor = page.next_cursor
            if cursor is None:
                break
    if classification.to_resume:
        cursor = None
        now = await reliable.store.delivery_now()
        clock = FixedClock(datetime.fromisoformat(event.source.occurred_at))
        while True:
            resume_page = await resumes.query_for_source(
                workspace,
                connector_instance_id=connector,
                delivery_mode=delivery_mode,
                as_of=now,
                cursor=cursor,
                limit=SOURCE_PAGE_SIZE,
            )
            for row in resume_page.items:
                reg = resume_registration_from_spl(row)
                current_resume = await resumes.get(workspace, row.resume_id)
                if (
                    current_resume is None
                    or current_resume.registration != reg
                    or current_resume.expires_at <= await reliable.store.delivery_now()
                ):
                    continue
                if (
                    reg.source is None
                    or reg.workspace_id != workspace
                    or reg.source.connector_instance_id != connector
                    or delivery_mode not in reg.source.delivery_modes
                ):
                    continue
                try:
                    matched = evaluator.matches(
                        reg.selector or "false",
                        event,
                        subscription_id=row.resume_id,
                        clock=clock,
                    )
                except SelectorTypeError:
                    matched = False
                if matched:
                    resume_match = ResumeMatch(row.resume_id, reg)
                    try:
                        validate_dispatch_target(build_resume_target(event, resume_match))
                    except (ValueError, UnicodeError) as exc:
                        raise SourceConfigurationError(
                            "resolved source wait target cannot be retained"
                        ) from exc
                    await dispatcher.dispatch_resume(event, resume_match, workspace_id=workspace)
            cursor = resume_page.next_cursor
            if cursor is None:
                break


def _unique_fields(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Reject ambiguous routing fields rather than choosing a duplicate JSON value."""
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise SourceEventRejected("duplicate-delivery-field")
        result[key] = value
    return result


async def _read_delivery(request: Request) -> object:
    """Bound the authenticated broker body before JSON parsing."""
    data = bytearray()
    async for chunk in request.stream():
        if len(data) + len(chunk) > MAX_INGEST_BYTES:
            raise SourceEventRejected("delivery-too-large")
        data.extend(chunk)
    try:
        payload = json.loads(data.decode("utf-8"), object_pairs_hook=_unique_fields)
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise SourceEventRejected("invalid-delivery-json") from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("data"), dict):
        raise SourceEventRejected("invalid-cloudevent-envelope")
    return payload["data"]


def build_source_event_router(*, enabled: bool) -> APIRouter:
    """Build default-off callbacks that share ingestion and never bypass sidecar authentication."""
    router = APIRouter(tags=["source-events"])

    async def receive(request: Request, *, connector: bool) -> JSONResponse:
        """Return retry for configuration/storage failures, drop only permanent source poison."""
        if not enabled:
            logger.warning("Source receiver callback is disabled")
            return JSONResponse({"status": DeliveryStatus.RETRY.value}, status_code=503)
        try:
            payload = await _read_delivery(request)
            scoped = (
                adapt_connector_event(payload) if connector else ScopedEvent.model_validate(payload)
            )
            _validate_scoped_event(scoped)
        except (SourceEventRejected, ValidationError) as exc:
            logger.warning("Dropping invalid source delivery: %s", type(exc).__name__)
            return JSONResponse({"status": DeliveryStatus.DROP.value})
        try:
            provider = get_metadata_store(get_providers(request))
            if not isinstance(provider, SchedulerStateProvider):
                raise TriggerError(
                    TriggerErrorKind.SOURCE_UNAVAILABLE, "source storage unavailable"
                )
            await ingest_source_event(
                scoped,
                dispatcher=get_dispatcher(request),
                evaluator=get_selector_evaluator(request),
                subscriptions=get_subscription_store(request),
                resumes=get_resume_subscription_store(request),
                audit=get_audit_sink(request),
                scheduler=provider,
            )
        except (SourceEventRejected, ScheduledOccurrenceError) as exc:
            logger.warning("Dropping invalid source delivery: %s", type(exc).__name__)
            return JSONResponse({"status": DeliveryStatus.DROP.value})
        except (SPLError, TriggerError, SourceConfigurationError, ValidationError) as exc:
            logger.error("Source delivery requires retry: %s", type(exc).__name__)
            return JSONResponse({"status": DeliveryStatus.RETRY.value})
        return JSONResponse({"status": DeliveryStatus.SUCCESS.value})

    @router.post(CONNECTOR_EVENTS_PATH)
    async def receive_connector(request: Request) -> JSONResponse:
        """Adapt the unchanged Connector broker envelope."""
        return await receive(request, connector=True)

    @router.post(NORMALIZED_EVENTS_PATH)
    async def receive_normalized(request: Request) -> JSONResponse:
        """Consume private scoped normalized events, including proven scheduled occurrences."""
        return await receive(request, connector=False)

    return router
