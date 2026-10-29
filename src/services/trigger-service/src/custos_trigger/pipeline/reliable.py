"""Durable failed dispatch and fenced replay over immutable original targets.

An in-flight receipt is retryable contention, not duplicate success. A failure
is acknowledged only after durable capture. Replay bypasses old unsuccessful
fast-path dedup keys but never changes the downstream key or resolved target.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import replace
from datetime import datetime, timedelta
from typing import Any, Protocol, runtime_checkable

from custos_spl.errors import ImmutableViolation, SPLError
from custos_spl.ids import WorkspaceId
from custos_spl.interfaces.dead_letters import (
    DeadLetterData,
    DeadLetterStoreProvider,
    DispatchAttempt,
    json_value,
)
from custos_spl.interfaces.delivery import (
    DeliveryStateProvider,
    decode_delivery_request,
    delivery_fingerprint,
    delivery_identity,
)

from custos_trigger.clients.workflow import (
    RaiseExternalEventRequest,
    RunRef,
    StartRunRequest,
    WorkflowClientError,
    WorkflowClientReplayUnsafeError,
    WorkflowClientStatusError,
    WorkflowClientTransportError,
    WorkflowServiceClient,
)
from custos_trigger.dedup import compute_dedup_key
from custos_trigger.errors import TriggerError, TriggerErrorKind
from custos_trigger.events import NormalizedEvent
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

logger = logging.getLogger(__name__)
MAX_DISPATCH_SECONDS = 300
FINALIZATION_MARGIN_SECONDS = 30


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
class ReliableStorage(DeliveryStateProvider, DeadLetterStoreProvider, Protocol):
    """The two optional capabilities required before durable dispatch can be enabled."""


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

    async def dispatch_start(
        self,
        event: NormalizedEvent,
        match: StartMatch,
        *,
        depth: int = 0,
    ) -> DispatchOutcome:
        """Dispatch a resolved start target without consulting mutable configuration on replay."""
        sub = match.subscription
        return await self._dispatch(
            event,
            sub.workspace_id,
            sub.subscription_id,
            {
                "kind": "start",
                "workflowVersionId": sub.target_workflow_version_id,
                "inputs": sub.input_mapping,
                "deliveryProtocol": 2,
            },
            depth,
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
        reg = match.registration
        return await self._dispatch(
            event,
            workspace_id,
            match.resume_id,
            {
                "kind": "resume",
                "runId": reg.run_id,
                "stepId": reg.step_id,
                "eventName": reg.event_key,
                "payload": event.data,
                "deliveryProtocol": reg.delivery_protocol,
            },
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
        key = compute_dedup_key(subscription_id, event.event_id)
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
                data = DeadLetterData(
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
            await self.audit.emit(
                "trigger.deadletter.stored",
                workspace_id=workspace,
                attributes={**attributes, "entryId": data.entry_id},
            )
            return DispatchOutcome(DispatchStatus.DEAD_LETTERED, error=failure)
        except SPLError as exc:
            logger.error("Durable dispatch storage/ownership failed: %s", type(exc).__name__)
            raise TriggerError(
                TriggerErrorKind.DISPATCH_UNAVAILABLE,
                "delivery was not acknowledged; retry it",
                details={"subscriptionId": subscription_id, "eventId": event.event_id},
            ) from exc

    async def replay(self, workspace_id: str, entry_id: str, worker_id: str) -> DispatchOutcome:
        """Replay a claimed capture against its original downstream key and exact target."""
        ws = WorkspaceId(workspace_id)
        try:
            entry = await self.store.get_dead_letter(ws, entry_id)
            if entry is None:
                raise TriggerError(
                    TriggerErrorKind.SUBSCRIPTION_NOT_FOUND, "dead-letter entry not found"
                )
            target = entry.data.target
            if target.get("deliveryProtocol", 1) != 2 or (
                target["kind"] == "start" and target.get("workflowVersionId") is None
            ):
                raise TriggerError(
                    TriggerErrorKind.REPLAY_UNSAFE, "entry has no safe versioned replay target"
                )
            owned = await self.store.claim_dead_letter(ws, entry_id, worker_id, ttl_seconds=300)
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
            except WorkflowClientError as exc:
                await self.store.finish_dead_letter(owned.claim, failure=history[-1])
                if isinstance(exc, WorkflowClientReplayUnsafeError):
                    raise TriggerError(
                        TriggerErrorKind.REPLAY_UNSAFE, "workflow rejected this replay identity"
                    ) from exc
                await self.audit.emit(
                    "trigger.deadletter.replay_failed",
                    workspace_id=workspace_id,
                    attributes={"entryId": entry_id},
                )
                return DispatchOutcome(DispatchStatus.DEAD_LETTERED, error=exc)
            await self.store.finish_dead_letter(owned.claim, result_id=ref.run_id if ref else None)
            await self.audit.emit(
                "trigger.deadletter.replayed",
                workspace_id=workspace_id,
                attributes={"entryId": entry_id},
            )
            return DispatchOutcome(DispatchStatus.DISPATCHED, run_ref=ref)
        except SPLError as exc:
            logger.error("Durable replay storage/ownership failed: %s", type(exc).__name__)
            raise TriggerError(
                TriggerErrorKind.DISPATCH_UNAVAILABLE, "replay could not complete safely"
            ) from exc
