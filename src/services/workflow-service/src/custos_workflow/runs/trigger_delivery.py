"""Version-2 Trigger delivery: durable receipts and restart-safe runtime effects.

Starts use Dapr's atomic ignore-existing policy for every runtime status.
Resumes use a distinct channel per logical step and a consumer acknowledgement,
so replay of an uncertain send cannot satisfy a later same-name wait.
Legacy public runs remain on the existing controller path.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from custos_spl.errors import ImmutableViolation
from custos_spl.ids import RunId as SplRunId
from custos_spl.ids import WorkflowId, WorkspaceId
from custos_spl.interfaces.delivery import (
    DeliveryReceipt,
    DeliveryStateProvider,
    WorkflowStart,
    delivery_fingerprint,
    resume_channel,
    resume_identity,
    resume_result,
)
from custos_spl.interfaces.metadata_store import Run
from grpc import RpcError

from custos_workflow.bindings.registry import ActivityTypeRegistry
from custos_workflow.compiler import RunMeta
from custos_workflow.compiler import compile as compile_workflow
from custos_workflow.graph.serialize import to_json
from custos_workflow.runs.controller import (
    LIFECYCLE_KIND_WORKFLOW_STARTED,
    LifecycleEvent,
    LifecycleEventPublisher,
    RunRef,
    WorkflowVersion,
    _WorkflowClient,
)
from custos_workflow.runs.errors import (
    RunNotFoundError,
    RunStateConflictError,
    WorkflowRuntimeUnavailableError,
)
from custos_workflow.runs.events import LifecycleEventPublishError
from custos_workflow.runs.ids import RunId, derive_run_id
from custos_workflow.runs.model import TERMINAL_STATUSES, RunStatus
from custos_workflow.runs.orchestrator import WORKFLOW_NAME, RunInput
from custos_workflow.runs.store import RunStore
from custos_workflow.runtime._common import (
    TERMINAL_STATUSES as RUNTIME_TERMINAL_STATUSES,
)
from custos_workflow.runtime._common import (
    GetRunStateRequest,
    RaiseRunEventRequest,
    ScheduleWorkflowRequest,
)


class TriggerDeliveryController:
    """Coordinate durable Trigger receipts with an existing run store and workflow runtime."""

    def __init__(
        self,
        provider: DeliveryStateProvider,
        store: RunStore,
        runtime: _WorkflowClient,
        registry: ActivityTypeRegistry,
        publisher: LifecycleEventPublisher,
    ) -> None:
        """Share production providers; never substitute an in-memory receipt store on failure."""
        self.provider, self.store, self.runtime = provider, store, runtime
        self.registry = registry
        self.publisher = publisher

    async def lookup_start(
        self,
        workspace_id: str,
        workflow_version_id: str,
        inputs: Mapping[str, Any],
        idempotency_key: str,
        retain_until: datetime,
    ) -> RunRef | None:
        """Recover previously validated work without Catalog, preserving legacy run identity."""
        run_id = derive_run_id(workspace_id, idempotency_key)
        row = await self.provider.get_delivery(WorkspaceId(workspace_id), "workflow-start", run_id)
        expected = delivery_fingerprint(
            {"workflowVersionId": workflow_version_id, "inputs": inputs}
        )
        if row is not None and row.request_hash != expected:
            raise ImmutableViolation("trigger start key is bound to another request")
        if row is not None and row.completed:
            extended = await self.provider.reserve_delivery(
                WorkspaceId(workspace_id), "workflow-start", run_id, expected, retain_until
            )
            return self._ref(extended)
        saved = await self.provider.get_workflow_start(WorkspaceId(workspace_id), run_id)
        if saved is not None:
            if saved.request_hash != expected:
                raise ImmutableViolation("trigger start key is bound to another request")
            claim = await self.provider.reserve_delivery(
                WorkspaceId(workspace_id),
                "workflow-start",
                run_id,
                expected,
                retain_until,
                lease_seconds=30,
            )
            return self._ref(claim) if claim.completed else await self._schedule(saved, claim)
        if await self.store.get_run(workspace_id, run_id) is not None:
            raise RunStateConflictError(
                "replay_unsafe: existing run has no versioned start snapshot", run_id=run_id
            )
        return None

    @staticmethod
    def _ref(row: DeliveryReceipt) -> RunRef:
        """Decode the stored result identity, not current mutable subscription configuration."""
        if row.result_json is None:
            raise ValueError("pending delivery has no run result")
        result = json.loads(row.result_json)
        return RunRef(
            workspace_id=row.workspace_id,
            run_id=RunId(result["runId"]),
            workflow_version_id=result["workflowVersionId"],
            status=RunStatus(result["status"]),
        )

    async def start(
        self,
        workspace_id: str,
        version: WorkflowVersion,
        inputs: Mapping[str, Any],
        idempotency_key: str,
        retain_until: datetime,
    ) -> RunRef:
        """Recover queued/uncertain scheduling without recreating a completed runtime instance."""
        await self.provider.check_delivery_ready()
        run_id = derive_run_id(workspace_id, idempotency_key)
        fingerprint = delivery_fingerprint({"workflowVersionId": version.id, "inputs": inputs})
        claim = await self.provider.reserve_delivery(
            WorkspaceId(workspace_id),
            "workflow-start",
            str(run_id),
            fingerprint,
            retain_until,
            lease_seconds=30,
        )
        if claim.completed:
            return self._ref(claim)
        now = await self.provider.delivery_now()
        graph = compile_workflow(
            version.document,
            RunMeta(
                workspace_id=workspace_id,
                workflow_version_id=version.id,
                workflow_name=version.name,
                workflow_version_label=version.version_label,
                started_at_default=now,
            ),
            self.registry,
        )
        request = RunInput(
            workspace_id,
            version.id,
            to_json(graph),
            dict(inputs),
            idempotency_key,
            delivery_protocol=2,
        )
        saved = await self.provider.put_workflow_start(
            Run(
                WorkspaceId(workspace_id),
                SplRunId(run_id),
                WorkflowId(version.workflow_id),
                version.id,
                "queued",
                None,
                now,
                now,
            ),
            WorkflowStart(
                WorkspaceId(workspace_id), run_id, fingerprint, json.dumps(request.to_dict())
            ),
        )
        return await self._schedule(saved, claim)

    async def _schedule(self, saved: WorkflowStart, claim: DeliveryReceipt) -> RunRef:
        """Recover from the atomic immutable input, using runtime-side duplicate protection."""
        request = RunInput.from_dict(json.loads(saved.input_json))
        run_id, workspace_id = RunId(saved.run_id), str(saved.workspace_id)
        try:
            scheduled = await self.runtime.schedule_new_workflow(
                ScheduleWorkflowRequest(
                    workflow=WORKFLOW_NAME,
                    instance_id=str(run_id),
                    input=request.to_dict(),
                    preserve_existing=True,
                )
            )
        except (OSError, TimeoutError, RpcError) as exc:
            await self.provider.release_delivery(claim)
            raise WorkflowRuntimeUnavailableError(
                "trigger start scheduling is uncertain; retry the same delivery",
                run_id=str(run_id),
                cause=type(exc).__name__,
            ) from exc
        if scheduled != str(run_id):
            raise WorkflowRuntimeUnavailableError(
                "runtime returned a different delivery identity", run_id=str(run_id)
            )
        current = await self.store.get_run(workspace_id, run_id)
        if current is None:
            raise RunNotFoundError("durable run metadata is missing", run_id=str(run_id))
        if current.status is RunStatus.QUEUED:
            current = await self.store.update_run_status(workspace_id, run_id, RunStatus.RUNNING)
        try:
            await self.publisher.publish(
                LifecycleEvent(
                    kind=LIFECYCLE_KIND_WORKFLOW_STARTED,
                    workspace_id=workspace_id,
                    run_id=run_id,
                    workflow_version_id=request.workflow_version_id,
                    occurred_at=current.started_at,
                )
            )
        except LifecycleEventPublishError as exc:
            await self.provider.release_delivery(claim)
            raise WorkflowRuntimeUnavailableError(
                "workflow start event was not acknowledged; retry the same delivery",
                run_id=str(run_id),
                cause=type(exc).__name__,
            ) from exc
        result = json.dumps(
            {
                "runId": str(run_id),
                "workflowVersionId": request.workflow_version_id,
                "status": current.status.value,
            }
        )
        return self._ref(await self.provider.complete_delivery(claim, result))

    async def resume(
        self,
        workspace_id: str,
        run_id: str,
        step_id: str,
        event_name: str,
        payload: Mapping[str, Any],
        idempotency_key: str,
        retain_until: datetime,
    ) -> None:
        """Deliver an immutable event to a versioned wait, never a legacy name-only channel."""
        await self.provider.check_delivery_ready()
        ws = WorkspaceId(workspace_id)
        key, fingerprint = resume_identity(
            workspace_id, run_id, step_id, event_name, idempotency_key, payload
        )
        prior = await self.provider.get_delivery(ws, "workflow-resume", key)
        if prior is not None and prior.completed:
            await self.provider.reserve_delivery(
                ws, "workflow-resume", key, fingerprint, retain_until
            )
            return
        mirrors = await self.provider.query_workflow_mirrors(
            ws, run_id=run_id, step_id=step_id, limit=2
        )
        if len(mirrors.items) != 1:
            if await self.store.get_run(workspace_id, RunId(run_id)) is None:
                raise RunNotFoundError(
                    "resume target does not exist in this workspace", run_id=run_id
                )
            raise RunStateConflictError(
                "replay_unsafe: wait has no unique durable mirror", run_id=run_id
            )
        from custos_workflow.steps.resume.mirror import ResumeSubscriptionMirror

        mirror = ResumeSubscriptionMirror.from_json(mirrors.items[0].payload_json)
        if (
            mirror.workspace_id != workspace_id
            or mirror.delivery_protocol != 2
            or mirror.event_key != event_name
            or mirror.expires_at <= await self.provider.delivery_now()
        ):
            raise RunStateConflictError(
                "replay_unsafe: wait is expired or does not match the delivery", run_id=run_id
            )
        runtime = await self.runtime.get_workflow_state(GetRunStateRequest(run_id))
        original = (
            json.loads(runtime.serialized_input) if runtime and runtime.serialized_input else None
        )
        if not isinstance(original, dict) or original.get("delivery_protocol", 1) != 2:
            raise RunStateConflictError(
                "replay_unsafe: run has no version-2 delivery contract",
                run_id=run_id,
                current_status="legacy",
                attempted_status="durable_resume",
            )
        if original.get("workspace_id") != workspace_id:
            raise RunNotFoundError("resume target does not exist in this workspace", run_id=run_id)
        if runtime is not None and runtime.status in RUNTIME_TERMINAL_STATUSES:
            acknowledged = await self.provider.get_delivery(ws, "workflow-resume", key)
            if acknowledged is not None and acknowledged.completed:
                await self.provider.reserve_delivery(
                    ws, "workflow-resume", key, fingerprint, retain_until
                )
                return
            raise RunStateConflictError(
                "resume runtime is terminal without acknowledgement", run_id=run_id
            )
        claim = await self.provider.reserve_delivery(
            ws, "workflow-resume", key, fingerprint, retain_until, lease_seconds=30
        )
        if claim.completed:
            return
        run = await self.store.get_run(workspace_id, RunId(run_id))
        if run is not None and run.status in TERMINAL_STATUSES:
            await self.provider.release_delivery(claim)
            raise RunStateConflictError(
                "resume target is terminal",
                run_id=run_id,
                current_status=run.status.value,
                attempted_status="durable_resume",
            )
        envelope = {
            "deliveryProtocol": 2,
            "deliveryKey": key,
            "requestHash": fingerprint,
            "idempotencyKey": idempotency_key,
            "payload": dict(payload),
        }
        try:
            await self.runtime.raise_workflow_event(
                RaiseRunEventRequest(
                    instance_id=run_id,
                    event_name=resume_channel(workspace_id, run_id, step_id, event_name),
                    data=envelope,
                )
            )
        except (OSError, TimeoutError, RpcError) as exc:
            completed = await self.provider.get_delivery(ws, "workflow-resume", key)
            if completed is not None and completed.completed:
                return
            await self.provider.release_delivery(claim)
            raise WorkflowRuntimeUnavailableError(
                "resume delivery is uncertain; retry the same delivery",
                run_id=run_id,
                cause=type(exc).__name__,
            ) from exc
        await self.provider.complete_delivery(claim, resume_result(key))
