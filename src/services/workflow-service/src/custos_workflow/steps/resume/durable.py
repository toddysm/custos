"""Durable version-2 wait driver and loop-safe Workflow activity bridge.

All persistence and Trigger calls execute as activities on the application's
own async loop. The orchestrator yields only durable tasks. A step-scoped event
must be acknowledged against its immutable receipt before becoming step output.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Callable, Generator, Mapping
from datetime import datetime, timedelta
from typing import Any

from custos_spl.errors import BackendUnavailable, ImmutableViolation
from custos_spl.ids import WorkspaceId
from custos_spl.interfaces.delivery import (
    DeliveryStateProvider,
    WorkflowMirror,
    delivery_fingerprint,
    resume_channel,
    resume_identity,
    resume_result,
)
from custos_spl.interfaces.trigger_source import ResumeSource
from dapr.ext.workflow import TaskFailedError, when_any

from custos_workflow.clients.trigger import (
    CancelResumeSubscriptionRequest,
    RegisterResumeSubscriptionRequest,
    RegisterResumeSubscriptionResponse,
    TriggerServiceClient,
)
from custos_workflow.graph.model import ExecutionGraph
from custos_workflow.runs.step_handler import (
    StepExecutionContext,
    StepFailed,
    StepResult,
    WorkflowContext,
)
from custos_workflow.steps.errors import ResumeDeliveryFailedError, ResumeWaitExpiredError
from custos_workflow.steps.resume.handler import (
    CancelResumeSubscriptionCall,
    DeleteMirrorCall,
    PersistMirrorCall,
    RegisterResumeSubscriptionCall,
    WaitForExternalEventCall,
    WaitForStepHandler,
)
from custos_workflow.steps.resume.mirror import ResumeSubscriptionMirror

RESUME_EFFECT_ACTIVITY = "custos.workflow.resume.v2"
logger = logging.getLogger(__name__)


class DurableResumeMirrorRepository:
    """A workspace-bound facade over indexed, durable mirror pages."""

    def __init__(self, provider: DeliveryStateProvider, workspace_id: str) -> None:
        """Bind scope so callers cannot enumerate or delete another workspace's mirrors."""
        self.provider, self.workspace_id = provider, WorkspaceId(workspace_id)

    async def put(self, mirror: ResumeSubscriptionMirror) -> ResumeSubscriptionMirror:
        """Update registration metadata without rebinding the immutable wait association."""
        if mirror.workspace_id != self.workspace_id or mirror.delivery_protocol != 2:
            raise ValueError("durable mirror requires the bound workspace and protocol 2")
        identity = delivery_fingerprint(
            {
                "workspace": mirror.workspace_id,
                "run": mirror.run_id,
                "step": mirror.step_id,
                "event": mirror.event_key,
                "selector": mirror.selector,
                "protocol": 2,
                "source": mirror.source.to_wire() if mirror.source else None,
            }
        )
        await self.provider.put_workflow_mirror(
            WorkflowMirror(
                self.workspace_id,
                mirror.mirror_id,
                mirror.run_id,
                mirror.step_id,
                identity,
                mirror.expires_at,
                mirror.to_json(),
            )
        )
        return mirror

    async def _list(
        self,
        *,
        run_id: str | None = None,
        step_id: str | None = None,
        expired_before: datetime | None = None,
    ) -> tuple[ResumeSubscriptionMirror, ...]:
        """Traverse scoped keyset pages to preserve the existing repository tuple contract."""
        rows: list[ResumeSubscriptionMirror] = []
        cursor = None
        while True:
            page = await self.provider.query_workflow_mirrors(
                self.workspace_id,
                run_id=run_id,
                step_id=step_id,
                expired_before=expired_before,
                cursor=cursor,
            )
            rows.extend(ResumeSubscriptionMirror.from_json(row.payload_json) for row in page.items)
            cursor = page.next_cursor
            if cursor is None:
                return tuple(rows)

    async def list_open(self, run_id: str) -> tuple[ResumeSubscriptionMirror, ...]:
        """Return persisted waits for one run within the bound workspace."""
        return await self._list(run_id=run_id)

    async def list_open_for_step(
        self, run_id: str, step_id: str
    ) -> tuple[ResumeSubscriptionMirror, ...]:
        """Return persisted waits for one logical step."""
        return await self._list(run_id=run_id, step_id=step_id)

    async def list_expired(self, before: datetime) -> tuple[ResumeSubscriptionMirror, ...]:
        """Return expired rows for scoped cleanup without consulting process-local state."""
        return await self._list(expired_before=before)

    async def delete(self, mirror_id: str) -> None:
        """Delete a scoped mirror idempotently after acknowledged consumption."""
        await self.provider.delete_workflow_mirror(self.workspace_id, mirror_id)


class ResumeActivityHandler:
    """Resolve the closed durable effect protocol using application-loop-owned clients."""

    def __init__(self, provider: DeliveryStateProvider, trigger: TriggerServiceClient) -> None:
        """Reuse the service's provider and authenticated Trigger client."""
        self.provider, self.trigger = provider, trigger

    async def execute(self, body: Mapping[str, Any]) -> object:
        """Perform one validated scoped effect, preserving errors for durable activity retries."""
        workspace = body["workspaceId"]
        if not isinstance(workspace, str) or not workspace:
            raise ValueError("resume activity requires workspace")
        repository = DurableResumeMirrorRepository(self.provider, workspace)
        operation = body["operation"]
        if operation == "persist":
            mirror = ResumeSubscriptionMirror.from_dict(body["mirror"])
            return (await repository.put(mirror)).to_dict()
        if operation == "register":
            data = body["request"]
            request = RegisterResumeSubscriptionRequest(
                run_id=data["runId"],
                step_id=data["stepId"],
                event_key=data["eventKey"],
                ttl=data["ttl"],
                selector=data.get("selector"),
                workspace_id=workspace,
                source=ResumeSource.from_wire(data["source"]) if data.get("source") else None,
                delivery_protocol=2,
            )
            result = self.trigger.register_resume_subscription(request)
            response = await result if inspect.isawaitable(result) else result
            return {"subscriptionId": response.ts_subscription_id}
        if operation == "ack":
            key = body["deliveryKey"]
            try:
                await self.provider.acknowledge_delivery(
                    WorkspaceId(workspace),
                    key,
                    body["requestHash"],
                    resume_result(key),
                )
            except ImmutableViolation:
                logger.warning("Rejected a resume acknowledgement with no matching receipt")
                return {"status": "rejected"}
            return {"status": "acknowledged"}
        if operation == "cancel":
            cancellation = CancelResumeSubscriptionRequest(
                body["runId"],
                body["stepId"],
                body["eventKey"],
                workspace_id=workspace,
            )
            cancelled = self.trigger.cancel_resume_subscription(cancellation)
            if inspect.isawaitable(cancelled):
                await cancelled
            return None
        if operation == "delete":
            await repository.delete(body["mirrorId"])
            return None
        raise ValueError("unknown durable resume operation")


def build_resume_activity(
    handler: ResumeActivityHandler,
    loop: asyncio.AbstractEventLoop,
) -> Callable[[Any, Mapping[str, Any]], object]:
    """Bridge worker threads onto the owning loop; never run pooled clients in a new loop."""

    def activity(_context: Any, body: Mapping[str, Any]) -> object:
        """Execute one effect on the service loop and propagate its actual outcome."""
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is loop:
            raise RuntimeError("durable resume activities must run on worker threads")
        return asyncio.run_coroutine_threadsafe(handler.execute(body), loop).result()

    activity.requires_async_bridge = True  # type: ignore[attr-defined]
    return activity


def _activity(ctx: WorkflowContext, body: Mapping[str, Any]) -> Generator[Any, Any, Any]:
    """Retry transient durable effects with durable timers, not thread sleeps or inline I/O."""
    operation = body["operation"]
    attempts = (
        None if operation in {"ack", "cancel", "delete"} else 1 if operation == "register" else 5
    )
    attempt = 0
    while True:
        try:
            result = yield ctx.call_activity(RESUME_EFFECT_ACTIVITY, input=dict(body))
            if attempt:
                ctx.set_custom_status(f"resume.{operation}.recovered")
            return result
        except (TaskFailedError, BackendUnavailable, OSError) as exc:
            attempt += 1
            if attempts is not None and attempt >= attempts:
                raise
            ctx.set_custom_status(f"resume.{operation}.pending")
            if not ctx.is_replaying:
                logger.warning("Durable resume %s pending after %s", operation, type(exc).__name__)
            yield ctx.create_timer(timedelta(seconds=min(2 ** min(attempt - 1, 6), 60)))


def _cleanup_wait(
    ctx: WorkflowContext,
    mirror: ResumeSubscriptionMirror | None,
) -> Generator[Any, Any, None]:
    """Finish terminal wait cleanup through recoverable activities before returning failure."""
    if mirror is None:
        raise RuntimeError("terminal durable wait has no mirror identity")
    yield from _activity(
        ctx,
        {
            "workspaceId": mirror.workspace_id,
            "operation": "cancel",
            "runId": mirror.run_id,
            "stepId": mirror.step_id,
            "eventKey": mirror.event_key,
        },
    )
    yield from _activity(
        ctx,
        {
            "workspaceId": mirror.workspace_id,
            "operation": "delete",
            "mirrorId": mirror.mirror_id,
        },
    )


def drive_durable_resume(
    handler: WaitForStepHandler,
    step: StepExecutionContext,
    graph: ExecutionGraph,
    step_id: str,
) -> Generator[Any, Any, StepResult]:
    """Drive every wait effect and bind only a validated, durably acknowledged step-scoped event."""
    ctx = step.workflow_context
    gen = handler.iter_resume(step, graph, step_id, delivery_protocol=2)
    sent: object = None
    pending: Exception | None = None
    workspace, run_id = step.workspace_id, str(step.run_id)
    registration_failures = 0
    active_mirror: ResumeSubscriptionMirror | None = None
    while True:
        try:
            if pending is None:
                token = gen.send(sent)
            else:
                error, pending = pending, None
                token = gen.throw(error)
        except StopIteration as stop:
            if isinstance(stop.value, StepFailed) and active_mirror is not None:
                yield from _cleanup_wait(ctx, active_mirror)
            return stop.value  # type: ignore[no-any-return]
        sent = None
        try:
            base: dict[str, Any] = {"workspaceId": workspace}
            if isinstance(token, PersistMirrorCall):
                active_mirror = token.mirror
                wire = yield from _activity(
                    ctx, {**base, "operation": "persist", "mirror": token.mirror.to_dict()}
                )
                sent = ResumeSubscriptionMirror.from_dict(wire)
                active_mirror = sent
            elif isinstance(token, RegisterResumeSubscriptionCall):
                request = token.request
                wire = yield from _activity(
                    ctx,
                    {
                        **base,
                        "operation": "register",
                        "request": {
                            "runId": request.run_id,
                            "stepId": request.step_id,
                            "eventKey": request.event_key,
                            "ttl": request.ttl,
                            "selector": request.selector,
                            "source": request.source.to_wire() if request.source else None,
                        },
                    },
                )
                sent = RegisterResumeSubscriptionResponse(wire["subscriptionId"])
            elif isinstance(token, WaitForExternalEventCall):
                event = ctx.wait_for_external_event(
                    resume_channel(workspace, run_id, step_id, token.event_key)
                )
                if token.expires_at is None:
                    raise ValueError("durable wait requires a persisted deadline")
                timer = ctx.create_timer(token.expires_at)
                combine = getattr(ctx, "when_any", None)
                winner = (
                    yield combine([event, timer])
                    if combine is not None
                    else when_any([event, timer])
                )
                if winner is not event:
                    gen.close()
                    yield from _cleanup_wait(ctx, active_mirror)
                    return StepFailed(
                        envelope={
                            **ResumeWaitExpiredError(
                                "wait expired before event consumption",
                                run_id=run_id,
                                step_id=step_id,
                            ).to_dict(),
                            "class": "permanent",
                        }
                    )
                wire = event.get_result()
                if (
                    not isinstance(wire, dict)
                    or wire.get("deliveryProtocol") != 2
                    or not isinstance(wire.get("payload"), dict)
                ):
                    raise ValueError("invalid version-2 resume envelope")
                key, fingerprint = resume_identity(
                    workspace,
                    run_id,
                    step_id,
                    token.event_key,
                    wire["idempotencyKey"],
                    wire["payload"],
                )
                if key != wire.get("deliveryKey") or fingerprint != wire.get("requestHash"):
                    raise ValueError("resume envelope does not match this wait")
                acknowledged = yield from _activity(
                    ctx,
                    {**base, "operation": "ack", "deliveryKey": key, "requestHash": fingerprint},
                )
                if acknowledged != {"status": "acknowledged"}:
                    raise ValueError("resume receipt rejected the acknowledgement")
                sent = wire["payload"]
            elif isinstance(token, CancelResumeSubscriptionCall):
                yield from _activity(
                    ctx,
                    {
                        **base,
                        "operation": "cancel",
                        "runId": token.request.run_id,
                        "stepId": token.request.step_id,
                        "eventKey": token.request.event_key,
                    },
                )
            elif isinstance(token, DeleteMirrorCall):
                yield from _activity(
                    ctx, {**base, "operation": "delete", "mirrorId": token.mirror_id}
                )
            else:
                raise TypeError("unknown durable resume effect")
        except (TaskFailedError, BackendUnavailable, OSError, ValueError, KeyError) as exc:
            if isinstance(token, (CancelResumeSubscriptionCall, DeleteMirrorCall)):
                logger.warning("Durable resume cleanup failed: %s", type(exc).__name__)
            if isinstance(token, RegisterResumeSubscriptionCall):
                yield ctx.create_timer(timedelta(seconds=2 ** min(registration_failures, 5)))
                registration_failures += 1
            if isinstance(token, WaitForExternalEventCall):
                gen.close()
                yield from _cleanup_wait(ctx, active_mirror)
                return StepFailed(
                    envelope={
                        **ResumeDeliveryFailedError(
                            "resume input could not be safely acknowledged",
                            run_id=run_id,
                            step_id=step_id,
                        ).to_dict(),
                        "class": "retryable",
                        "errorType": type(exc).__name__,
                    }
                )
            pending = exc
