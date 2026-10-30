"""Workspace-scoped, administrator-only failed-dispatch inspection and recovery routes."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Annotated, Any, Literal
from uuid import uuid4

from custos_spl.interfaces.dead_letters import (
    DeadLetterEntry,
    DeadLetterFilter,
    DeadLetterStatus,
    json_value,
    validate_discard_reason,
)
from fastapi import APIRouter, Depends, Header, Path, Query, Request, Response
from pydantic import Field, field_validator

from custos_trigger._wire import WireModel
from custos_trigger.dead_letters import DeadLetterAdmin, DeadLetterAdminError
from custos_trigger.dependencies import get_dispatcher
from custos_trigger.middleware import CallContext, ensure_workspace, require_permission
from custos_trigger.pipeline.dispatch import Dispatcher

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1/workspaces/{workspace_id}/triggers", tags=["dead-letters"])
WorkspacePath = Annotated[str, Path(min_length=1, max_length=512)]
EntryPath = Annotated[str, Path(min_length=1, max_length=512)]
AdminContext = Annotated[CallContext, Depends(require_permission("trigger:admin"))]


class FailureView(WireModel):
    """Closed failure classification without arbitrary exception text."""

    at: datetime
    code: str
    http_status: int | None


class ReplayView(WireModel):
    """Operator attempt history without exposing a live claim's fencing token."""

    attempt: int
    actor: str
    started_at: datetime
    finished_at: datetime | None
    outcome: str
    failure: FailureView | None
    pending_since: datetime | None


class EntryView(WireModel):
    """Version-one public snapshot of a retained failed dispatch and its replay history."""

    schema_version: Literal[1] = 1
    workspace_id: str
    entry_id: str
    subscription_id: str
    idempotency_key: str
    event: dict[str, Any]
    target: dict[str, Any]
    history: list[FailureView]
    replay_history: list[ReplayView]
    status: DeadLetterStatus
    created_at: datetime
    expires_at: datetime
    result_id: str | None
    claim_expires_at: datetime | None


class PageView(WireModel):
    """Stable keyset page with a filter-bound continuation cursor."""

    items: list[EntryView]
    next_cursor: str | None


class ReplayResult(WireModel):
    """Persisted lifecycle status following an idempotent replay operation."""

    status: Literal["replayed", "pending", "discarded"]


class BulkSelector(WireModel):
    """Strict bounded selection; unknown fields cannot silently broaden a replay."""

    subscription_id: str | None = Field(default=None, min_length=1, max_length=512)
    status: DeadLetterStatus | None = None
    since: datetime | None = None
    until: datetime | None = None
    limit: int = Field(default=50, ge=1, le=100, strict=True)
    cursor: str | None = Field(default=None, min_length=1, max_length=8192)


class BulkResult(WireModel):
    """Counts of completed/idempotent replays and per-entry rejected or failed attempts."""

    accepted: int
    rejected: int
    next_cursor: str | None


class DiscardRequest(WireModel):
    """Optional bounded operator justification persisted in the atomic discard audit."""

    reason: str | None = Field(default=None, min_length=1, max_length=1024, pattern=r"\S")

    @field_validator("reason")
    @classmethod
    def valid_reason(cls, value: str | None) -> str | None:
        """Validate storage-safe text before entering an audited provider operation."""
        return validate_discard_reason(value)


def _admin(dispatcher: Annotated[Dispatcher, Depends(get_dispatcher)]) -> DeadLetterAdmin:
    """Fail closed while durable dispatch is disabled, never falling back to volatile replay."""
    if dispatcher.reliable is None:
        logger.warning("Dead-letter administration requires durable dispatch")
        raise DeadLetterAdminError("unavailable", "Durable dispatch is not enabled.")
    return DeadLetterAdmin(dispatcher.reliable)


AdminDep = Annotated[DeadLetterAdmin, Depends(_admin)]


def _view(entry: DeadLetterEntry) -> EntryView:
    """Project immutable normalized input and safe history, excluding private claim authority."""
    return EntryView(
        workspace_id=str(entry.data.workspace_id),
        entry_id=entry.data.entry_id,
        subscription_id=entry.data.subscription_id,
        idempotency_key=entry.data.idempotency_key,
        event={key: json_value(value) for key, value in entry.data.event.items()},
        target={key: json_value(value) for key, value in entry.data.target.items()},
        history=[
            FailureView(at=attempt.at, code=attempt.code, http_status=attempt.http_status)
            for attempt in entry.data.history
        ],
        replay_history=[
            ReplayView(
                attempt=attempt.epoch,
                actor=attempt.worker_id,
                started_at=attempt.started_at,
                finished_at=attempt.finished_at,
                outcome=attempt.outcome,
                pending_since=attempt.pending_since,
                failure=(
                    FailureView(
                        at=attempt.failure.at,
                        code=attempt.failure.code,
                        http_status=attempt.failure.http_status,
                    )
                    if attempt.failure is not None
                    else None
                ),
            )
            for attempt in entry.replay_history
        ],
        status=entry.status,
        created_at=entry.created_at,
        expires_at=entry.expires_at,
        result_id=entry.result_id,
        claim_expires_at=entry.claim.expires_at if entry.claim else None,
    )


@router.get("/deadletter", response_model=PageView)
async def list_entries(
    workspace_id: WorkspacePath,
    ctx: AdminContext,
    admin: AdminDep,
    subscription_id: Annotated[
        str | None, Query(alias="subscriptionId", min_length=1, max_length=512)
    ] = None,
    status: DeadLetterStatus | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    cursor: Annotated[str | None, Query(min_length=1, max_length=8192)] = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
) -> PageView:
    """List only the caller's workspace using validated filters and stable continuation."""
    ensure_workspace(ctx, workspace_id)
    page = await admin.list(
        workspace_id,
        DeadLetterFilter(subscription_id, status, since, until),
        cursor=cursor,
        limit=limit,
    )
    return PageView(
        items=[_view(entry) for entry in page.items],
        next_cursor=page.next_cursor.token if page.next_cursor is not None else None,
    )


@router.get("/deadletter/{entry_id}", response_model=EntryView)
async def inspect_entry(
    workspace_id: WorkspacePath,
    entry_id: EntryPath,
    ctx: AdminContext,
    admin: AdminDep,
) -> EntryView:
    """Inspect an immutable captured target and bounded attempts without retargeting it."""
    ensure_workspace(ctx, workspace_id)
    return _view(await admin.inspect(workspace_id, entry_id))


@router.post("/deadletter/{entry_id}:replay", response_model=ReplayResult)
async def replay_entry(
    workspace_id: WorkspacePath,
    entry_id: EntryPath,
    ctx: AdminContext,
    admin: AdminDep,
) -> ReplayResult:
    """Replay once under durable ownership; active or unsafe attempts return a conflict."""
    ensure_workspace(ctx, workspace_id)
    return ReplayResult(status=await admin.replay(workspace_id, entry_id, ctx.principal_id))


@router.post("/deadletter/{entry_id}:discard", status_code=204)
async def discard_entry(
    workspace_id: WorkspacePath,
    entry_id: EntryPath,
    ctx: AdminContext,
    admin: AdminDep,
    body: DiscardRequest | None = None,
) -> Response:
    """Discard and audit atomically, accepting an already-discarded entry idempotently."""
    ensure_workspace(ctx, workspace_id)
    await admin.discard(workspace_id, entry_id, ctx.principal_id, body.reason if body else None)
    return Response(status_code=204)


@router.post("/deadletter:replay", response_model=BulkResult)
async def bulk_replay(
    workspace_id: WorkspacePath,
    ctx: AdminContext,
    admin: AdminDep,
    body: BulkSelector,
    response: Response,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key", max_length=512)] = None,
) -> BulkResult:
    """Replay at most 100 selected entries within one bounded request deadline."""
    ensure_workspace(ctx, workspace_id)
    key = idempotency_key if idempotency_key is not None else uuid4().hex
    accepted, rejected, cursor = await admin.bulk_replay(
        workspace_id,
        DeadLetterFilter(body.subscription_id, body.status, body.since, body.until),
        ctx.principal_id,
        limit=body.limit,
        cursor=body.cursor,
        request_key=key,
    )
    response.headers["Idempotency-Key"] = key
    return BulkResult(
        accepted=accepted,
        rejected=rejected,
        next_cursor=cursor.token if cursor is not None else None,
    )


async def dead_letter_error_handler(request: Request, exc: Exception) -> Response:
    """Use the existing problem-details envelope for every administration failure."""
    from custos_trigger.api.errors import problem_response

    assert isinstance(exc, DeadLetterAdminError)
    return problem_response(request, kind=exc.code, detail=exc.detail)
