"""Durable failed dispatch and fenced replay over immutable original targets.

An in-flight receipt is retryable contention, not duplicate success. A failure
is acknowledged only after durable capture. Replay bypasses old unsuccessful
fast-path dedup keys but never changes the downstream key or resolved target.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import Any, Literal, Protocol, runtime_checkable

from custos_spl.errors import ImmutableViolation, LeaseBusy, SPLError
from custos_spl.ids import WorkspaceId
from custos_spl.interfaces.bulk_replay import bulk_entry_identity
from custos_spl.interfaces.dead_letters import (
    DeadLetterData,
    DeadLetterStoreProvider,
    DispatchAttempt,
    json_value,
    validate_dispatch_identifier,
)
from custos_spl.interfaces.delivery import (
    DeliveryStateProvider,
    decode_delivery_request,
    delivery_fingerprint,
    delivery_identity,
    encode_delivery_request,
)
from custos_spl.interfaces.source_delivery import (
    MAX_SOURCE_SELECTION_BYTES,
    MAX_SOURCE_TARGETS,
    SourceDeliverySelection,
    SourceDeliveryStoreProvider,
    SourceSelectionLimitError,
)
from custos_spl.pagination import (
    Cursor,
    Page,
    decode_query_cursor,
    encode_query_cursor,
    query_page_limit,
)

from custos_trigger.clients.workflow import (
    RaiseExternalEventRequest,
    RunRef,
    StartRunRequest,
    WorkflowClientAcknowledgementPending,
    WorkflowClientError,
    WorkflowClientReplayUnsafeError,
    WorkflowClientStatusError,
    WorkflowClientTransportError,
    WorkflowServiceClient,
)
from custos_trigger.dedup import compute_dispatch_key
from custos_trigger.errors import TriggerError, TriggerErrorKind
from custos_trigger.events import NormalizedEvent
from custos_trigger.models import SourceType, Subscription
from custos_trigger.pipeline.dispatch import (
    AUDIT_DEDUPED,
    AUDIT_DISPATCH_FAILED,
    AUDIT_DISPATCHED,
    AUDIT_LOOP_DETECTED,
    AUDIT_MATCHED,
    AUDIT_RESUME_DELIVERED,
    AuditSink,
    DispatchOutcome,
    DispatchStatus,
)
from custos_trigger.pipeline.match_resume import ResumeMatch
from custos_trigger.pipeline.match_start import StartMatch
from custos_trigger.source_config import SourceConfigurationError

logger = logging.getLogger(__name__)
MAX_DISPATCH_SECONDS = 300
FINALIZATION_MARGIN_SECONDS = 30
SOURCE_RECONCILE_TARGET_LIMIT = 32
SOURCE_RECONCILE_QUERY_SECONDS = 15


def build_start_target(subscription: Subscription) -> dict[str, Any]:
    """Build the same complete target for source preflight and durable start capture."""
    return {
        "kind": "start",
        "workflowVersionId": subscription.target_workflow_version_id,
        "inputs": subscription.input_mapping,
        "deliveryProtocol": 2,
    }


def build_resume_target(event: NormalizedEvent, match: ResumeMatch) -> dict[str, Any]:
    """Build the stored signal destination and event payload for preflight and capture."""
    reg = match.registration
    return {
        "kind": "resume",
        "runId": reg.run_id,
        "stepId": reg.step_id,
        "eventName": reg.event_key,
        "payload": event.data,
        "deliveryProtocol": reg.delivery_protocol,
    }


def validate_dispatch_limits(
    max_retries: int,
    max_fanout_depth: int,
    retention_seconds: int,
    max_replay_attempts: int,
) -> None:
    """Validate opt-in configuration before creating clients or opening backend connections."""
    if type(max_retries) is not int or not 0 <= max_retries <= 63:
        raise ValueError("durable dispatch retries must be 0..63")
    if type(max_fanout_depth) is not int or max_fanout_depth < 0:
        raise ValueError("durable dispatch fanout depth must be non-negative")
    if type(retention_seconds) is not int or not 1 <= retention_seconds <= 31_536_000:
        raise ValueError("dead-letter retention must be 1..31536000 seconds")
    if type(max_replay_attempts) is not int or not 1 <= max_replay_attempts <= 100:
        raise ValueError("replay attempt limit must be 1..100")


@runtime_checkable
class ReliableStorage(
    DeliveryStateProvider, DeadLetterStoreProvider, SourceDeliveryStoreProvider, Protocol
):
    """Receipt, immutable source-selection and dead-letter capabilities for durable dispatch."""


@dataclass(frozen=True)
class SourceReconciliationPage:
    """Partial scan progress; advancing continuation never acknowledges pending targets."""

    selections: int
    settled_targets: int
    pending_targets: int
    next_cursor: Cursor | None
    deadline_exhausted: bool = False


def _source_progress_cursor(workspace: str, key: str, index: int | None = None) -> Cursor:
    """Encode an opaque workspace-bound position within, or after, an immutable selection."""
    return encode_query_cursor(("source-reconcile", workspace), json.dumps([key, index]))


def _source_progress(workspace: str, cursor: Cursor | None) -> tuple[str, int | None] | None:
    """Reject malformed or cross-workspace continuation before any dispatch."""
    raw = decode_query_cursor(cursor, ("source-reconcile", workspace))
    if raw is None:
        return None
    value = json.loads(raw)
    if not isinstance(value, list) or len(value) != 2:
        raise ValueError("invalid source reconciliation cursor")
    key, index = value
    validate_dispatch_identifier(key)
    if index is not None and (type(index) is not int or not 0 <= index <= MAX_SOURCE_TARGETS):
        raise ValueError("invalid source reconciliation target position")
    return key, index


def _source_fingerprint(event: NormalizedEvent, *, shared: bool = False) -> str:
    """Ignore publication time, retaining stable fields and original Connector snapshots."""
    connector = event.source.type in (SourceType.VENDOR_PUSH, SourceType.PULL, SourceType.WEBHOOK)
    source = event.source.model_dump(
        mode="json", by_alias=True, exclude={"occurred_at"} if connector else set()
    )
    if shared:
        if not connector:
            raise ValueError("shared resume selection requires a Connector source")
        source["type"] = "connector"
    return delivery_fingerprint(
        {
            **event.model_dump(mode="json", by_alias=True, exclude={"raw", "source"}),
            "source": source,
        }
    )


class ReliableDispatcher:
    """Capture failures and replay original requests under bounded provider-owned claims."""

    def __init__(
        self,
        client: WorkflowServiceClient,
        store: ReliableStorage,
        audit: AuditSink,
        *,
        max_retries: int = 5,
        max_fanout_depth: int = 16,
        retention_seconds: int = 2_592_000,
        max_replay_attempts: int = 3,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        """Validate retry/retention bounds before accepting receiver traffic."""
        validate_dispatch_limits(
            max_retries, max_fanout_depth, retention_seconds, max_replay_attempts
        )
        self.client, self.store, self.audit = client, store, audit
        self.max_retries, self.max_depth = max_retries, max_fanout_depth
        self.retention, self.max_replays, self.sleep = retention_seconds, max_replay_attempts, sleep

    def _snapshot(
        self,
        event: NormalizedEvent,
        workspace: str,
        subscription_id: str,
        target: Mapping[str, Any],
        now: datetime,
    ) -> DeadLetterData:
        """Validate one original target without performing any external side effect."""
        key = compute_dispatch_key(subscription_id, event)
        try:
            return DeadLetterData(
                workspace_id=WorkspaceId(workspace),
                entry_id="dl_" + delivery_identity(workspace, key),
                subscription_id=subscription_id,
                idempotency_key=key,
                event=event.model_dump(by_alias=True, exclude={"raw"}),
                target=target,
                history=(DispatchAttempt(now, "configuration"),),
                retention_seconds=self.retention,
                max_replay_attempts=self.max_replays,
            )
        except (ValueError, UnicodeError) as exc:
            raise SourceConfigurationError(
                "dispatch snapshot cannot be retained; correct its configuration"
            ) from exc

    async def prepare_request(
        self,
        event: NormalizedEvent,
        workspace: str,
        subscription_id: str,
        target: Mapping[str, Any],
        *,
        prepare_inputs: Callable[[], dict[str, Any]] | None = None,
    ) -> DeadLetterData | None:
        """Recover existing producer intent before evaluating a new mapping for selection."""
        now = await self.store.delivery_now()
        prior = await self.store.get_delivery(
            WorkspaceId(workspace), "trigger", compute_dispatch_key(subscription_id, event)
        )
        if prior is not None:
            if prior.completed:
                return None
            if prior.request_json is None:
                raise TriggerError(
                    TriggerErrorKind.REPLAY_UNSAFE, "original pending request is unavailable"
                )
            return decode_delivery_request(prior.request_json, now)
        if prepare_inputs is not None:
            target = {**target, "inputs": prepare_inputs()}
        return self._snapshot(event, workspace, subscription_id, target, now)

    async def dispatch_event(
        self,
        event: NormalizedEvent,
        workspace: str,
        select: Callable[[], AsyncIterator[DeadLetterData]],
        *,
        shared_resumes: Callable[[], AsyncIterator[DeadLetterData]] | None = None,
    ) -> tuple[tuple[Literal["start", "resume"], DispatchOutcome], ...]:
        """Persist the entire bounded selection before effects; retries never rematch intent."""
        key = delivery_identity(
            event.source.type.value, compute_dispatch_key("source-selection-v1", event)
        )
        groups = [(key, _source_fingerprint(event), select)]
        if shared_resumes is not None:
            groups.append(
                (
                    delivery_identity(
                        "shared-resumes", compute_dispatch_key("source-selection-v1", event)
                    ),
                    _source_fingerprint(event, shared=True),
                    shared_resumes,
                )
            )
        try:
            candidates: list[SourceDeliverySelection] = []
            for key, fingerprint, builder in groups:
                selection = await self.store.get_source_delivery(WorkspaceId(workspace), key)
                if selection is None:
                    requests: list[str] = []
                    size = 0
                    async for data in builder():
                        raw = encode_delivery_request(data)
                        size += len(raw.encode("utf-8"))
                        if len(requests) >= MAX_SOURCE_TARGETS or size > MAX_SOURCE_SELECTION_BYTES:
                            raise SourceSelectionLimitError("source selection exceeds its limits")
                        requests.append(raw)
                    selection = SourceDeliverySelection(
                        WorkspaceId(workspace),
                        key,
                        fingerprint,
                        await self.store.delivery_now() + timedelta(seconds=self.retention),
                        tuple(requests),
                    )
                if selection.request_hash != fingerprint:
                    raise ImmutableViolation("source event identity is bound to another request")
                candidates.append(selection)
            selections = await self.store.put_source_deliveries(tuple(candidates))
            outcomes: list[tuple[Literal["start", "resume"], DispatchOutcome]] = []
            for selection in selections:
                for raw in selection.requests or ():
                    outcomes.append(await self._dispatch_snapshot(workspace, raw))
            return tuple(outcomes)
        except SourceSelectionLimitError as exc:
            logger.warning("Source selection exceeds its bounded limits")
            raise SourceConfigurationError("source selection exceeds its bounded limits") from exc
        except SPLError as exc:
            logger.error("Source selection persistence failed: %s", type(exc).__name__)
            raise TriggerError(
                TriggerErrorKind.DISPATCH_UNAVAILABLE, "source selection is not acknowledged"
            ) from exc

    async def _dispatch_snapshot(
        self, workspace: str, raw: str
    ) -> tuple[Literal["start", "resume"], DispatchOutcome]:
        """Drive only the accepted immutable target through the existing receipt authority."""
        data = decode_delivery_request(raw, await self.store.delivery_now())
        original = NormalizedEvent.model_validate(json_value(data.event))
        outcome = await self._dispatch(
            original, workspace, data.subscription_id, data.target, 0, snapshot=data
        )
        return "start" if data.target["kind"] == "start" else "resume", outcome

    async def reconcile_pending(
        self, workspace: str, *, cursor: Cursor | None = None, limit: int = 50
    ) -> SourceReconciliationPage:
        """Probe at most 32 targets with resumable progress and bounded per-target time."""
        query_page_limit(limit)
        validate_dispatch_identifier(workspace)
        try:
            position = _source_progress(workspace, cursor)
        except ValueError:
            logger.warning("Invalid source reconciliation cursor")
            raise
        settled, pending, examined, probes = 0, 0, 0, 0
        progress: Cursor | None = cursor
        in_flight = False
        try:
            async with asyncio.timeout(MAX_DISPATCH_SECONDS):
                following: Cursor | None
                async with asyncio.timeout(SOURCE_RECONCILE_QUERY_SECONDS):
                    if position is not None and position[1] is not None:
                        current = await self.store.get_source_delivery(
                            WorkspaceId(workspace), position[0]
                        )
                        if current is None:
                            raise ImmutableViolation("source selection no longer exists")
                        page = Page([current], None)
                        following = _source_progress_cursor(workspace, position[0])
                    else:
                        page = await self.store.query_pending_source_deliveries(
                            WorkspaceId(workspace),
                            cursor=encode_query_cursor(
                                ("pending-source-delivery", workspace),
                                position[0],
                            )
                            if position
                            else None,
                            limit=limit,
                        )
                        after = decode_query_cursor(
                            page.next_cursor, ("pending-source-delivery", workspace)
                        )
                        following = _source_progress_cursor(workspace, after) if after else None
                for selection in page.items:
                    requests = selection.requests or ()
                    start = (
                        position[1] or 0
                        if position is not None and position[0] == selection.key
                        else 0
                    )
                    if selection.requests is not None and start > len(requests):
                        raise ValueError("source reconciliation cursor exceeds selection")
                    examined += 1
                    for index in range(start, len(requests)):
                        if probes == SOURCE_RECONCILE_TARGET_LIMIT:
                            return SourceReconciliationPage(
                                examined,
                                settled,
                                pending,
                                _source_progress_cursor(workspace, selection.key, index),
                            )
                        progress = _source_progress_cursor(workspace, selection.key, index + 1)
                        in_flight = True
                        try:
                            await self._dispatch_snapshot(workspace, requests[index])
                        except TriggerError as exc:
                            if exc.kind != TriggerErrorKind.DISPATCH_UNAVAILABLE:
                                raise
                            logger.warning("Source target remains pending during reconciliation")
                            pending += 1
                        except (SPLError, TimeoutError) as exc:
                            logger.warning("Source probe remains pending: %s", type(exc).__name__)
                            pending += 1
                        else:
                            settled += 1
                        in_flight = False
                        probes += 1
                    progress = _source_progress_cursor(workspace, selection.key)
                return SourceReconciliationPage(examined, settled, pending, following)
        except TimeoutError as exc:
            if probes or in_flight:
                logger.warning("Source reconciliation deadline reached; progress is resumable")
                return SourceReconciliationPage(
                    examined, settled, pending + int(in_flight), progress, True
                )
            logger.error("Source reconciliation query did not finish before its deadline")
            raise TriggerError(
                TriggerErrorKind.DISPATCH_UNAVAILABLE, "source reconciliation did not complete"
            ) from exc
        except SPLError as exc:
            logger.error("Source reconciliation unavailable: %s", type(exc).__name__)
            raise TriggerError(
                TriggerErrorKind.DISPATCH_UNAVAILABLE, "source reconciliation did not complete"
            ) from exc

    async def dispatch_start(
        self,
        event: NormalizedEvent,
        match: StartMatch,
        *,
        depth: int = 0,
        prepare_inputs: Callable[[], dict[str, Any]] | None = None,
    ) -> DispatchOutcome:
        """Dispatch a resolved start target without consulting mutable configuration on replay."""
        sub = match.subscription
        return await self._dispatch(
            event,
            sub.workspace_id,
            sub.subscription_id,
            build_start_target(sub),
            depth,
            prepare_inputs=prepare_inputs,
        )

    async def dispatch_resume(
        self,
        event: NormalizedEvent,
        match: ResumeMatch,
        *,
        workspace_id: str,
        depth: int = 0,
    ) -> DispatchOutcome:
        """Preserve the wait's persisted protocol, including explicit unsafe legacy replay."""
        return await self._dispatch(
            event,
            workspace_id,
            match.resume_id,
            build_resume_target(event, match),
            depth,
        )

    async def _send(self, data: DeadLetterData, retain_until: datetime) -> RunRef | None:
        """Issue exactly the stored request with its original key and protocol."""
        target = json_value(data.target)
        if not isinstance(target, dict):
            raise ValueError("resolved target must be an object")
        protocol = target.get("deliveryProtocol", 1)
        horizon = retain_until if protocol == 2 else None
        if target["kind"] == "start":
            if not target["workflowVersionId"]:
                raise WorkflowClientError("start target has no workflow version", retryable=False)
            return await self.client.start_run(
                StartRunRequest(
                    workspace_id=data.workspace_id,
                    workflow_version_id=target["workflowVersionId"],
                    inputs=target["inputs"],
                    idempotency_key=data.idempotency_key,
                    delivery_protocol=protocol,
                    retain_until=horizon,
                )
            )
        await self.client.raise_external_event(
            target["runId"],
            target["stepId"],
            RaiseExternalEventRequest(
                workspace_id=data.workspace_id,
                event_name=target["eventName"],
                payload=target["payload"],
                idempotency_key=data.idempotency_key,
                delivery_protocol=protocol,
                retain_until=horizon,
            ),
        )
        return None

    async def _attempts(
        self,
        data: DeadLetterData,
        until: datetime,
        history: list[DispatchAttempt],
        *,
        claim_deadline: datetime,
    ) -> RunRef | None:
        """Bound total elapsed time and attempts; record only safe failure classifications."""
        try:
            now = await self.store.delivery_now()
            budget = min(
                MAX_DISPATCH_SECONDS,
                (claim_deadline - now).total_seconds() - FINALIZATION_MARGIN_SECONDS,
            )
            if budget <= 0:
                raise TimeoutError("ownership must be finalized before dispatch")
            async with asyncio.timeout(budget):
                for attempt in range(self.max_retries + 1):
                    remaining = (
                        claim_deadline - await self.store.delivery_now()
                    ).total_seconds() - FINALIZATION_MARGIN_SECONDS
                    if remaining <= 0:
                        raise TimeoutError("ownership must be finalized before dispatch")
                    try:
                        async with asyncio.timeout(remaining):
                            return await self._send(data, until)
                    except WorkflowClientAcknowledgementPending:
                        raise
                    except WorkflowClientError as exc:
                        history.append(
                            DispatchAttempt(
                                await self.store.delivery_now(),
                                "unsafe"
                                if isinstance(exc, WorkflowClientReplayUnsafeError)
                                else "transient"
                                if exc.retryable
                                else "permanent",
                                exc.status_code
                                if isinstance(exc, WorkflowClientStatusError)
                                else None,
                            )
                        )
                        if not exc.retryable or attempt == self.max_retries:
                            raise
                        await self.sleep(min(0.5 * (2**attempt), 30, remaining))
        except TimeoutError as exc:
            history.append(DispatchAttempt(await self.store.delivery_now(), "transient"))
            raise WorkflowClientTransportError(
                "dispatch exceeded its bounded recovery window"
            ) from exc
        raise AssertionError("unreachable dispatch retry state")

    async def _dispatch(
        self,
        event: NormalizedEvent,
        workspace: str,
        subscription_id: str,
        target: Mapping[str, Any],
        depth: int,
        *,
        prepare_inputs: Callable[[], dict[str, Any]] | None = None,
        snapshot: DeadLetterData | None = None,
    ) -> DispatchOutcome:
        """Never acknowledge pending work, failed capture or lost ownership as success."""
        attributes = {
            "subscriptionId": subscription_id,
            "eventId": event.event_id,
            "kind": target["kind"],
        }
        if depth > self.max_depth:
            await self.audit.emit(
                AUDIT_LOOP_DETECTED, workspace_id=workspace, attributes=attributes
            )
            return DispatchOutcome(DispatchStatus.LOOP_REJECTED)
        key = compute_dispatch_key(subscription_id, event)
        try:
            now = await self.store.delivery_now()
            prior = await self.store.get_delivery(WorkspaceId(workspace), "trigger", key)
            if prior is not None and prior.completed:
                await self.audit.emit(AUDIT_DEDUPED, workspace_id=workspace, attributes=attributes)
                return DispatchOutcome(DispatchStatus.DUPLICATE)
            if prior is not None:
                if prior.request_json is None:
                    raise TriggerError(
                        TriggerErrorKind.REPLAY_UNSAFE, "original pending request is unavailable"
                    )
                data = decode_delivery_request(prior.request_json, now)
            else:
                if prepare_inputs is not None:
                    target = {**target, "inputs": prepare_inputs()}
                data = (
                    snapshot
                    if snapshot is not None
                    else self._snapshot(event, workspace, subscription_id, target, now)
                )
            fingerprint = delivery_fingerprint({"target": json_value(data.target)})
            claim = await self.store.reserve_delivery(
                data.workspace_id,
                "trigger",
                key,
                fingerprint,
                now + timedelta(seconds=self.retention + 2 * MAX_DISPATCH_SECONDS),
                request=data,
            )
            if claim.completed:
                await self.audit.emit(AUDIT_DEDUPED, workspace_id=workspace, attributes=attributes)
                return DispatchOutcome(DispatchStatus.DUPLICATE)
            if claim.request_json is None or claim.lease_until is None:
                raise RuntimeError("producer reservation has no original request or live lease")
            data = decode_delivery_request(claim.request_json, await self.store.delivery_now())
            target = data.target
            attributes["kind"] = target["kind"]
            await self.audit.emit(AUDIT_MATCHED, workspace_id=workspace, attributes=attributes)
            existing = await self.store.get_dead_letter(data.workspace_id, data.entry_id)
            if existing is not None:
                if existing.data.idempotency_key != key or existing.data.target != data.target:
                    raise ImmutableViolation("captured delivery target changed")
                await self.store.complete_delivery(
                    claim, json.dumps({"deadLetterId": data.entry_id})
                )
                return DispatchOutcome(DispatchStatus.DEAD_LETTERED)
            history: list[DispatchAttempt] = []
            failure: WorkflowClientError
            if target.get("deliveryProtocol", 1) != 2:
                history = [DispatchAttempt(now, "unsafe")]
                failure = WorkflowClientReplayUnsafeError()
            elif target["kind"] == "start" and target["workflowVersionId"] is None:
                history = list(data.history)
                failure = WorkflowClientError(
                    "start target has no workflow version", retryable=False
                )
            else:
                try:
                    ref = await self._attempts(
                        data, claim.retain_until, history, claim_deadline=claim.lease_until
                    )
                except WorkflowClientAcknowledgementPending as exc:
                    await self.store.release_delivery(claim)
                    logger.info("Resume submission is awaiting durable consumer acknowledgement")
                    raise TriggerError(
                        TriggerErrorKind.DISPATCH_UNAVAILABLE, "delivery acknowledgement is pending"
                    ) from exc
                except WorkflowClientError as exc:
                    failure = exc
                else:
                    await self.store.complete_delivery(
                        claim,
                        json.dumps(
                            {
                                "runId": ref.run_id if ref else None,
                                "dispatched": True,
                            }
                        ),
                    )
                    await self.audit.emit(
                        AUDIT_DISPATCHED if target["kind"] == "start" else AUDIT_RESUME_DELIVERED,
                        workspace_id=workspace,
                        attributes=attributes,
                    )
                    return DispatchOutcome(DispatchStatus.DISPATCHED, run_ref=ref)
            await self.store.capture_failed_delivery(claim, replace(data, history=tuple(history)))
            await self.audit.emit(
                AUDIT_DISPATCH_FAILED, workspace_id=workspace, attributes=attributes
            )
            return DispatchOutcome(DispatchStatus.DEAD_LETTERED, error=failure)
        except SPLError as exc:
            logger.error("Durable dispatch storage/ownership failed: %s", type(exc).__name__)
            raise TriggerError(
                TriggerErrorKind.DISPATCH_UNAVAILABLE,
                "delivery was not acknowledged; retry it",
                details={"subscriptionId": subscription_id, "eventId": event.event_id},
            ) from exc

    async def replay(
        self,
        workspace_id: str,
        entry_id: str,
        worker_id: str,
        *,
        bulk_id: str | None = None,
        bulk_token: str | None = None,
        expected_identity: str | None = None,
    ) -> DispatchOutcome:
        """Replay a claimed capture against its original downstream key and exact target."""
        ws = WorkspaceId(workspace_id)
        try:
            entry = await self.store.get_dead_letter(ws, entry_id)
            if entry is None:
                raise TriggerError(
                    TriggerErrorKind.SUBSCRIPTION_NOT_FOUND, "dead-letter entry not found"
                )
            identity = bulk_entry_identity(entry)
            if expected_identity is not None and identity != expected_identity:
                raise TriggerError(TriggerErrorKind.REPLAY_UNSAFE, "dead-letter generation changed")
            target = entry.data.target
            if target.get("deliveryProtocol", 1) != 2 or (
                target["kind"] == "start" and target.get("workflowVersionId") is None
            ):
                raise TriggerError(
                    TriggerErrorKind.REPLAY_UNSAFE, "entry has no safe versioned replay target"
                )
            try:
                owned = await self.store.claim_dead_letter(
                    ws,
                    entry_id,
                    worker_id,
                    ttl_seconds=300,
                    bulk_id=bulk_id,
                    bulk_token=bulk_token,
                    expected_identity=identity,
                )
            except LeaseBusy as exc:
                raise TriggerError(
                    TriggerErrorKind.REPLAY_IN_PROGRESS, "entry already has an active replay"
                ) from exc
            except ImmutableViolation as exc:
                try:
                    await self.store.recover_dead_letter(ws, entry_id, expected_identity=identity)
                except ImmutableViolation as missing:
                    if await self.store.get_dead_letter(ws, entry_id) is None:
                        raise TriggerError(
                            TriggerErrorKind.SUBSCRIPTION_NOT_FOUND, "dead-letter entry not found"
                        ) from missing
                    raise TriggerError(
                        TriggerErrorKind.REPLAY_UNSAFE, "dead-letter generation changed"
                    ) from missing
                raise TriggerError(
                    TriggerErrorKind.REPLAY_UNSAFE,
                    "entry is terminal or beyond its replay attempt/retention limit",
                ) from exc
            if owned.claim is None:
                raise RuntimeError("claimed entry has no fencing token")
            history: list[DispatchAttempt] = []
            try:
                ref = await self._attempts(
                    owned.data,
                    owned.expires_at + timedelta(seconds=MAX_DISPATCH_SECONDS),
                    history,
                    claim_deadline=owned.claim.expires_at,
                )
            except WorkflowClientAcknowledgementPending as exc:
                await self.store.defer_dead_letter(owned.claim)
                logger.info("Resume replay is awaiting durable consumer acknowledgement")
                raise TriggerError(
                    TriggerErrorKind.DISPATCH_UNAVAILABLE, "replay acknowledgement is pending"
                ) from exc
            except WorkflowClientError as exc:
                if (
                    owned.claim.awaiting_ack
                    and not isinstance(exc, WorkflowClientReplayUnsafeError)
                    and (exc.retryable or owned.replay_history[-1].pending_since is not None)
                ):
                    await self.store.defer_dead_letter(owned.claim)
                    logger.warning(
                        "Resume acknowledgement reconciliation is unavailable: %s",
                        type(exc).__name__,
                    )
                    raise TriggerError(
                        TriggerErrorKind.DISPATCH_UNAVAILABLE, "replay acknowledgement is pending"
                    ) from exc
                finished = await self.store.finish_dead_letter(owned.claim, failure=history[-1])
                if isinstance(exc, WorkflowClientReplayUnsafeError):
                    raise TriggerError(
                        TriggerErrorKind.REPLAY_UNSAFE, "workflow rejected this replay identity"
                    ) from exc
                assert finished.status in ("pending", "discarded")
                return DispatchOutcome(
                    DispatchStatus.DEAD_LETTERED, error=exc, replay_status=finished.status
                )
            await self.store.finish_dead_letter(owned.claim, result_id=ref.run_id if ref else None)
            return DispatchOutcome(DispatchStatus.DISPATCHED, run_ref=ref, replay_status="replayed")
        except SPLError as exc:
            logger.error("Durable replay storage/ownership failed: %s", type(exc).__name__)
            raise TriggerError(
                TriggerErrorKind.DISPATCH_UNAVAILABLE, "replay could not complete safely"
            ) from exc
