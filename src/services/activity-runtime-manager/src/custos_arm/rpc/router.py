"""Inbound Dapr Service-Invocation handlers (ARM-IMPL-018).

ARM exposes one inbound RPC surface to the Workflow Service over Dapr
Service-Invocation (design § Internal RPC). The Dapr sidecar forwards
``…/v1.0/invoke/<arm-app-id>/method/<Method>`` to the app as ``POST /<Method>``,
so the two methods land here as ``POST /ScheduleActivity`` and
``POST /CancelActivity``.

This module is a thin adapter: it verifies the call context (via the
:class:`~custos_arm.middleware.CallContextMiddleware` that already populated
``request.state``), parses the wire envelope, and dispatches to the
:class:`~custos_arm.scheduler.ActivityScheduler`. The Scheduler owns idempotent
replay (keyed by ``(workspaceId, runId, stepId, attempt)``); the
``Idempotency-Key`` header is the canonical wire form of that key and is
validated for consistency with the body.

The Scheduler instance is resolved from ``request.app.state.scheduler`` so the
composition root (and tests) can inject it; the adapter returns ``503`` until
it is wired.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from custos_arm.middleware import CallContext, get_call_context
from custos_arm.scheduler import ActivityScheduler, CancelOutcome

from .models import CancelActivityWire, ScheduleActivityWire

__all__ = ["router"]

router = APIRouter()


def get_scheduler(request: Request) -> ActivityScheduler:
    """Resolve the :class:`ActivityScheduler` wired onto the application.

    The composition root attaches the Scheduler to ``app.state.scheduler``
    during startup; until then the RPC surface reports ``503`` so callers
    retry rather than observe a 500.
    """
    scheduler = getattr(request.app.state, "scheduler", None)
    if scheduler is None:
        raise HTTPException(
            status_code=503,
            detail="activity scheduler is not ready",
        )
    assert isinstance(scheduler, ActivityScheduler)
    return scheduler


CallContextDep = Annotated[CallContext, Depends(get_call_context)]
SchedulerDep = Annotated[ActivityScheduler, Depends(get_scheduler)]


@router.post("/ScheduleActivity", include_in_schema=False)
async def schedule_activity(
    body: ScheduleActivityWire,
    call_context: CallContextDep,
    scheduler: SchedulerDep,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> JSONResponse:
    """Dispatch a ``ScheduleActivity`` request and return the result envelope.

    The workspace is taken from the verified call context, never the body. The
    ``Idempotency-Key`` header (``runId|stepId|attempt``) must agree with the
    body when present; the Scheduler performs the actual dedup.
    """
    if idempotency_key is not None and idempotency_key != body.idempotency_key():
        raise HTTPException(
            status_code=400,
            detail="Idempotency-Key header does not match the request body",
        )
    schedule_request = body.to_schedule_request(workspace_id=call_context.workspace_id)
    result = await scheduler.schedule(schedule_request)
    return JSONResponse(content=result.model_dump(by_alias=True, mode="json"))


@router.post("/CancelActivity", include_in_schema=False)
async def cancel_activity(
    body: CancelActivityWire,
    call_context: CallContextDep,
    scheduler: SchedulerDep,
) -> Response:
    """Cancel the live attempt for a step, idempotently.

    Returns ``204`` when a live attempt is accepted for cancellation, ``404``
    when the step is unknown, and ``409`` when it has already terminated — the
    latter two are no-ops the caller collapses, per design § Internal RPC.
    """
    outcome = await scheduler.cancel(
        workspace_id=call_context.workspace_id,
        run_id=body.run_id,
        step_id=body.step_id,
    )
    if outcome is CancelOutcome.UNKNOWN:
        raise HTTPException(status_code=404, detail="step is unknown")
    if outcome is CancelOutcome.TERMINATED:
        raise HTTPException(status_code=409, detail="step has already terminated")
    return Response(status_code=204)
