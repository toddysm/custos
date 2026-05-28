"""Operator lease-admin REST surface (CONN-IMPL-026 + CONN-IMPL-028).

Maps the lease portion of
``design/components/connector-service/design.md`` § *Public Interface*:

* ``GET   /v1/workspaces/{ws}/connectors/{id}/leases``         — list active by instance
* ``GET   /v1/workspaces/{ws}/runs/{runId}/leases``            — list active by run
* ``POST  /v1/workspaces/{ws}/leases/{leaseId}:revoke``        — single
* ``POST  /v1/workspaces/{ws}/connectors/{id}/leases:revoke-all`` — fan-out
* ``POST  /v1/workspaces/{ws}/runs/{runId}/leases:revoke-all`` — fan-out

The internal RPC surface in :mod:`custos_connector.lease.router`
hosts the sidecar-facing ``/internal/v1/leases:issue|refresh|release|revoke``
endpoints — distinct callers, distinct permissions, distinct router.

Live-state listing reads
:meth:`~custos_spl.interfaces.lease_store.LeaseStoreProvider.list_active_leases`
on the SPL store, which is the authoritative source of truth for
lease state: every issue/refresh/release/revoke transition commits
to this store before it returns to the caller. Sidecars carry only
their local revocation cache; the CS store is the single ledger.

Revoke flow (CONN-IMPL-028)
---------------------------

Each operator-driven revoke endpoint:

1. Validates ``reason`` is a non-empty string; otherwise returns
   400 ``connector.reason_required`` (the design contract; pydantic's
   default 422 is not acceptable here because the operator surface
   wants a stable, code-named error).
2. Resolves the selector (lease id, instance id, or run id) into
   the concrete list of affected lease ids.
3. Emits ``lease.revoke-requested`` exactly once, carrying the
   operator's call-context principal id as both the audit ``actor``
   and the structured ``operator`` field.
4. For each affected lease, calls
   :meth:`LeaseManager.revoke_with_status` to record the terminal
   state in the SPL store and emit ``lease.revoked``. The operator's
   principal id flows through as the audit ``actor`` here too.
5. Best-effort signals each affected sidecar's
   ``POST /sidecar-admin/v1/revoke`` so its local revocation cache
   stops serving the lease before the DB revoke would naturally
   propagate. Transport errors and the documented ``503 sidecar
   shutting down`` are logged and swallowed — the CS store has
   already recorded the terminal state.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Annotated, Any

from custos_spl.ids import ConnectorInstanceId, RunId, WorkspaceId
from custos_spl.interfaces.lease_store import LeaseFilter
from custos_spl.pagination import Cursor
from fastapi import APIRouter, Depends, Path, Query, Request
from pydantic import BaseModel, ConfigDict, Field
from starlette.responses import JSONResponse, Response

from custos_connector.api._common import (
    error_response,
    resolve_lease_manager,
    resolve_lease_store,
    resolve_sidecar_admin_client,
    resolve_sidecar_registry,
    workspace_mismatch_response,
)
from custos_connector.lease.service import RevokeOutcomeStatus
from custos_connector.middleware import CallContext, require_permission
from custos_connector.permissions import ADMIN_CONNECTOR, CONNECTOR_READ
from custos_connector.sidecar_admin import SidecarAdminClient, SidecarRegistry

router = APIRouter(prefix="/v1", tags=["lease-admin"])


# ---------------------------------------------------------------------------
# Wire models
# ---------------------------------------------------------------------------


class _LeaseWire(BaseModel):
    """Wire shape of a :class:`Lease` row for list responses."""

    model_config = ConfigDict(populate_by_name=True)

    lease_id: str = Field(..., alias="leaseId")
    workspace_id: str = Field(..., alias="workspaceId")
    run_id: str = Field(..., alias="runId")
    step_id: str = Field(..., alias="stepId")
    attempt: int
    slot: str
    capability: str
    connector_instance_id: str = Field(..., alias="connectorInstanceId")
    token_type: str = Field(..., alias="tokenType")
    issued_at: datetime = Field(..., alias="issuedAt")
    expires_at: datetime = Field(..., alias="expiresAt")
    released_at: datetime | None = Field(default=None, alias="releasedAt")
    revoked_at: datetime | None = Field(default=None, alias="revokedAt")
    revoke_reason: str | None = Field(default=None, alias="revokeReason")


class _LeaseListResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    items: list[_LeaseWire]
    next_cursor: str | None = Field(default=None, alias="nextCursor")


class _RevokeRequest(BaseModel):
    """Body for the operator revoke endpoints.

    ``reason`` is parsed permissively here so the route handler can
    emit the design-mandated 400 ``connector.reason_required`` code
    when missing or empty. Letting pydantic enforce ``min_length=1``
    surfaces a 422 with a generic detail array, which the operator
    surface does not accept (the audit consumer keys off the
    canonical error code).
    """

    model_config = ConfigDict(extra="forbid")

    reason: str | None = None


class _RevokeResponse(BaseModel):
    """Single-lease revoke response."""

    model_config = ConfigDict(populate_by_name=True)

    lease_id: str = Field(..., alias="leaseId")
    status: str


class _RevokeAllResultEntry(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    lease_id: str = Field(..., alias="leaseId")
    status: str


class _RevokeAllResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    results: list[_RevokeAllResultEntry]
    revoked: int
    total: int


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


#: Code returned in the canonical error envelope when ``reason`` is
#: missing or empty. Pinned in the design \u00a7 Operator Admin Surface
#: so cross-service tooling can key off a stable string rather than
#: pydantic's variable detail-array shape.
_REASON_REQUIRED_CODE: str = "connector.reason_required"


#: OpenAPI request-body schema shared by all three revoke endpoints.
#: Hand-rolled so the generated spec still advertises the required
#: ``reason`` field even though we parse the body manually in the
#: handler (we need the canonical ``connector.reason_required`` 400
#: envelope rather than pydantic's default 422 detail-array, which is
#: what binding ``_RevokeRequest`` via ``Body(...)`` would give us).
_REVOKE_REQUEST_OPENAPI_EXTRA: dict[str, Any] = {
    "requestBody": {
        "required": True,
        "content": {
            "application/json": {
                "schema": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["reason"],
                    "properties": {
                        "reason": {
                            "type": "string",
                            "minLength": 1,
                            "maxLength": 500,
                            "description": (
                                "Operator-supplied justification for the "
                                "revoke. Flows to the lease.revoke-requested "
                                "and lease.revoked audit events."
                            ),
                        },
                    },
                },
            },
        },
    },
}


async def _read_revoke_body(request: Request) -> _RevokeRequest | JSONResponse:
    """Parse the operator revoke body or render a 400 envelope.

    We don't bind ``_RevokeRequest`` via ``Body(...)`` because pydantic
    rejects an empty body before our handler runs, which then funnels
    through FastAPI's ``RequestValidationError`` envelope rather than
    the canonical ``connector.reason_required`` code. Reading the
    body manually keeps the error shape uniform across the three
    revoke endpoints.
    """
    try:
        raw = await request.json()
    except ValueError:
        return error_response(
            status_code=400,
            code=_REASON_REQUIRED_CODE,
            detail="request body must be a JSON object containing a non-empty 'reason' field",
        )
    if not isinstance(raw, dict):
        return error_response(
            status_code=400,
            code=_REASON_REQUIRED_CODE,
            detail="request body must be a JSON object containing a non-empty 'reason' field",
        )
    try:
        return _RevokeRequest.model_validate(raw)
    except ValueError as exc:
        return error_response(
            status_code=400,
            code=_REASON_REQUIRED_CODE,
            detail=str(exc),
        )


def _validated_reason(body: _RevokeRequest) -> str | JSONResponse:
    """Return the non-empty reason or the 400 envelope.

    Whitespace-only reasons are treated as missing because the audit
    consumer indexes on free-text reasons and a blank string defeats
    the forensic purpose of the field.
    """
    reason = body.reason
    if reason is None or reason.strip() == "":
        return error_response(
            status_code=400,
            code=_REASON_REQUIRED_CODE,
            detail="'reason' is required and must be a non-empty string",
        )
    if len(reason) > 500:
        return error_response(
            status_code=400,
            code=_REASON_REQUIRED_CODE,
            detail="'reason' must be 500 characters or fewer",
        )
    return reason


def _lease_to_wire(lease: Any) -> _LeaseWire:
    return _LeaseWire(
        leaseId=lease.lease_id,
        workspaceId=str(lease.workspace_id),
        runId=str(lease.run_id),
        stepId=str(lease.step_id),
        attempt=lease.attempt,
        slot=lease.slot,
        capability=lease.capability,
        connectorInstanceId=str(lease.connector_instance_id),
        tokenType=lease.token_type,
        issuedAt=lease.issued_at,
        expiresAt=lease.expires_at,
        releasedAt=lease.released_at,
        revokedAt=lease.revoked_at,
        revokeReason=lease.revoke_reason,
    )


async def _list_active(
    request: Request,
    *,
    workspace_id: str,
    lease_filter: LeaseFilter,
    cursor: str | None,
    limit: int | None,
) -> JSONResponse:
    """Shared paginated lister used by both list endpoints."""
    lease_store = resolve_lease_store(request)
    page = await lease_store.list_active_leases(
        WorkspaceId(workspace_id),
        filter=lease_filter,
        cursor=Cursor(cursor) if cursor else None,
        limit=limit,
    )
    body = _LeaseListResponse(
        items=[_lease_to_wire(row) for row in page.items],
        nextCursor=str(page.next_cursor) if page.next_cursor is not None else None,
    )
    return JSONResponse(status_code=200, content=body.model_dump(by_alias=True, mode="json"))


def _revoke_status_to_http(status: RevokeOutcomeStatus) -> int:
    """Map a 4-way revoke outcome to a per-lease HTTP status.

    Used only by the single-lease ``:revoke`` route; the
    ``:revoke-all`` variants always return 200 with a per-lease ack
    list (matching the existing internal-RPC batch contract).
    """
    if status is RevokeOutcomeStatus.NOT_FOUND:
        return 404
    return 200


# ---------------------------------------------------------------------------
# Routes — list
# ---------------------------------------------------------------------------


@router.get(
    "/workspaces/{ws}/connectors/{instance_id}/leases",
    summary="List active leases for a connector instance.",
)
async def list_leases_for_instance(
    request: Request,
    ctx: Annotated[CallContext, Depends(require_permission(CONNECTOR_READ))],
    ws: Annotated[str, Path(min_length=1)],
    instance_id: Annotated[str, Path(min_length=1)],
    cursor: Annotated[str | None, Query()] = None,
    limit: Annotated[int | None, Query(ge=1, le=500)] = None,
) -> Response:
    mismatch = workspace_mismatch_response(ctx, ws)
    if mismatch is not None:
        return mismatch
    return await _list_active(
        request,
        workspace_id=ws,
        lease_filter=LeaseFilter(connector_instance_id=ConnectorInstanceId(instance_id)),
        cursor=cursor,
        limit=limit,
    )


@router.get(
    "/workspaces/{ws}/runs/{run_id}/leases",
    summary="List active leases for a run.",
)
async def list_leases_for_run(
    request: Request,
    ctx: Annotated[CallContext, Depends(require_permission(CONNECTOR_READ))],
    ws: Annotated[str, Path(min_length=1)],
    run_id: Annotated[str, Path(min_length=1)],
    cursor: Annotated[str | None, Query()] = None,
    limit: Annotated[int | None, Query(ge=1, le=500)] = None,
) -> Response:
    mismatch = workspace_mismatch_response(ctx, ws)
    if mismatch is not None:
        return mismatch
    return await _list_active(
        request,
        workspace_id=ws,
        lease_filter=LeaseFilter(run_id=RunId(run_id)),
        cursor=cursor,
        limit=limit,
    )


# ---------------------------------------------------------------------------
# Routes — revoke
# ---------------------------------------------------------------------------


async def _fan_out_to_sidecars(
    *,
    client: SidecarAdminClient | None,
    registry: SidecarRegistry,
    lease_ids: list[str],
    reason: str,
) -> None:
    """Best-effort signal each affected sidecar's control channel.

    Buckets ``lease_ids`` by their registered sidecar endpoint and
    issues one ``POST /sidecar-admin/v1/revoke`` per bucket. Leases
    with no registered sidecar are silently skipped (the SPL store
    revoke is the authoritative terminal state; the sidecar fan-out
    is purely an in-flight short-circuit optimization).

    Buckets are dispatched concurrently via :func:`asyncio.gather`
    with ``return_exceptions=True`` so a slow or unreachable sidecar
    in one bucket does not linearly inflate the operator request
    latency. The bounded per-call timeout in
    :class:`SidecarAdminClient` caps the worst-case wait at one
    sidecar round-trip regardless of how many endpoints we fan out
    to.

    Transport failures and non-2xx responses are logged inside
    :meth:`SidecarAdminClient.revoke` and not raised back to the
    operator route.
    """
    if client is None or not lease_ids:
        return
    buckets: dict[str, list[str]] = {}
    for lease_id in lease_ids:
        endpoint = registry.endpoint_for(lease_id)
        if endpoint is None:
            continue
        buckets.setdefault(endpoint, []).append(lease_id)
    if not buckets:
        return
    await asyncio.gather(
        *(
            client.revoke(endpoint=endpoint, lease_ids=ids, reason=reason)
            for endpoint, ids in buckets.items()
        ),
        return_exceptions=True,
    )


async def _enumerate_active_lease_ids(
    request: Request,
    *,
    workspace: WorkspaceId,
    lease_filter: LeaseFilter,
) -> list[str]:
    """Resolve the selector into a list of currently-active lease ids.

    Walks pagination to completion so the ``lease.revoke-requested``
    payload carries the full id set the operator's call will
    terminate. The same enumeration is then iterated row-by-row in
    :func:`_revoke_many` for the actual terminal-revoke writes; the
    duplicate pagination walk is acceptable for M1 scale (16-lease
    cap per step \u00d7 hundreds of steps per run) and keeps the audit
    payload deterministic.
    """
    lease_store = resolve_lease_store(request)
    ids: list[str] = []
    cursor: Cursor | None = None
    while True:
        page = await lease_store.list_active_leases(
            workspace,
            filter=lease_filter,
            cursor=cursor,
        )
        ids.extend(row.lease_id for row in page.items)
        if page.next_cursor is None:
            break
        cursor = page.next_cursor
    return ids


@router.post(
    "/workspaces/{ws}/leases/{lease_id}:revoke",
    summary="Revoke a single lease.",
    openapi_extra=_REVOKE_REQUEST_OPENAPI_EXTRA,
)
async def revoke_single_lease(
    request: Request,
    ctx: Annotated[CallContext, Depends(require_permission(ADMIN_CONNECTOR))],
    ws: Annotated[str, Path(min_length=1)],
    lease_id: Annotated[str, Path(min_length=1)],
) -> Response:
    """Revoke one lease.

    Returns 200 with ``{leaseId, status}`` when the lease was found
    (``status`` is one of ``revoked`` / ``already-revoked`` /
    ``already-expired``); returns 404 with the same envelope when
    the lease does not exist in this workspace; returns 400
    ``connector.reason_required`` when ``reason`` is missing/empty.

    Emits ``lease.revoke-requested`` once before the terminal-revoke
    write and ``lease.revoked`` from the SPL store on a successful
    state change. Both audit rows carry the call-context principal
    id as the ``actor`` so the forensic trail records the operator
    who initiated the revoke.
    """
    mismatch = workspace_mismatch_response(ctx, ws)
    if mismatch is not None:
        return mismatch
    body_or_error = await _read_revoke_body(request)
    if isinstance(body_or_error, JSONResponse):
        return body_or_error
    reason_or_error = _validated_reason(body_or_error)
    if isinstance(reason_or_error, JSONResponse):
        return reason_or_error
    reason = reason_or_error
    workspace = WorkspaceId(ws)
    manager = resolve_lease_manager(request)
    await manager.record_revoke_requested(
        workspace_id=workspace,
        selector_type="lease",
        selector_value=lease_id,
        lease_ids=[lease_id],
        reason=reason,
        operator=ctx.principal_id,
        actor=ctx.principal_id,
    )
    outcome = await manager.revoke_with_status(
        workspace_id=workspace,
        lease_id=lease_id,
        reason=reason,
        actor=ctx.principal_id,
    )
    if outcome.status is RevokeOutcomeStatus.NOT_FOUND:
        return error_response(
            status_code=404,
            code="connector.lease_not_found",
            detail=f"lease {lease_id!r} not found in workspace {ws!r}",
        )
    await _fan_out_to_sidecars(
        client=resolve_sidecar_admin_client(request),
        registry=resolve_sidecar_registry(request),
        lease_ids=[lease_id],
        reason=reason,
    )
    payload = _RevokeResponse(leaseId=lease_id, status=outcome.status.value)
    return JSONResponse(
        status_code=_revoke_status_to_http(outcome.status),
        content=payload.model_dump(by_alias=True),
    )


async def _revoke_many(
    request: Request,
    *,
    workspace_id: str,
    selector_type: str,
    selector_value: str,
    lease_filter: LeaseFilter,
    reason: str,
    operator: str,
) -> JSONResponse:
    """Walk every active lease matching ``lease_filter`` and revoke.

    Enumeration is taken to completion (no operator-visible cursor)
    so a single ``:revoke-all`` call mints one ack per active lease
    and so the ``lease.revoke-requested`` payload carries the full
    id list. After the SPL terminal-revoke writes, the active sidecars
    are best-effort signalled so their local revocation caches stop
    serving the leases immediately.
    """
    workspace = WorkspaceId(workspace_id)
    manager = resolve_lease_manager(request)
    lease_ids = await _enumerate_active_lease_ids(
        request,
        workspace=workspace,
        lease_filter=lease_filter,
    )
    await manager.record_revoke_requested(
        workspace_id=workspace,
        selector_type=selector_type,
        selector_value=selector_value,
        lease_ids=lease_ids,
        reason=reason,
        operator=operator,
        actor=operator,
    )
    results: list[_RevokeAllResultEntry] = []
    revoked_count = 0
    revoked_ids: list[str] = []
    for lid in lease_ids:
        outcome = await manager.revoke_with_status(
            workspace_id=workspace,
            lease_id=lid,
            reason=reason,
            actor=operator,
        )
        if outcome.status is RevokeOutcomeStatus.REVOKED:
            revoked_count += 1
            revoked_ids.append(lid)
        results.append(_RevokeAllResultEntry(leaseId=lid, status=outcome.status.value))
    await _fan_out_to_sidecars(
        client=resolve_sidecar_admin_client(request),
        registry=resolve_sidecar_registry(request),
        lease_ids=revoked_ids,
        reason=reason,
    )
    body = _RevokeAllResponse(results=results, revoked=revoked_count, total=len(results))
    return JSONResponse(status_code=200, content=body.model_dump(by_alias=True))


@router.post(
    "/workspaces/{ws}/connectors/{instance_id}/leases:revoke-all",
    summary="Revoke every active lease for a connector instance.",
    openapi_extra=_REVOKE_REQUEST_OPENAPI_EXTRA,
)
async def revoke_all_for_instance(
    request: Request,
    ctx: Annotated[CallContext, Depends(require_permission(ADMIN_CONNECTOR))],
    ws: Annotated[str, Path(min_length=1)],
    instance_id: Annotated[str, Path(min_length=1)],
) -> Response:
    mismatch = workspace_mismatch_response(ctx, ws)
    if mismatch is not None:
        return mismatch
    body_or_error = await _read_revoke_body(request)
    if isinstance(body_or_error, JSONResponse):
        return body_or_error
    reason_or_error = _validated_reason(body_or_error)
    if isinstance(reason_or_error, JSONResponse):
        return reason_or_error
    return await _revoke_many(
        request,
        workspace_id=ws,
        selector_type="instance",
        selector_value=instance_id,
        lease_filter=LeaseFilter(connector_instance_id=ConnectorInstanceId(instance_id)),
        reason=reason_or_error,
        operator=ctx.principal_id,
    )


@router.post(
    "/workspaces/{ws}/runs/{run_id}/leases:revoke-all",
    summary="Revoke every active lease for a run.",
    openapi_extra=_REVOKE_REQUEST_OPENAPI_EXTRA,
)
async def revoke_all_for_run(
    request: Request,
    ctx: Annotated[CallContext, Depends(require_permission(ADMIN_CONNECTOR))],
    ws: Annotated[str, Path(min_length=1)],
    run_id: Annotated[str, Path(min_length=1)],
) -> Response:
    mismatch = workspace_mismatch_response(ctx, ws)
    if mismatch is not None:
        return mismatch
    body_or_error = await _read_revoke_body(request)
    if isinstance(body_or_error, JSONResponse):
        return body_or_error
    reason_or_error = _validated_reason(body_or_error)
    if isinstance(reason_or_error, JSONResponse):
        return reason_or_error
    return await _revoke_many(
        request,
        workspace_id=ws,
        selector_type="run",
        selector_value=run_id,
        lease_filter=LeaseFilter(run_id=RunId(run_id)),
        reason=reason_or_error,
        operator=ctx.principal_id,
    )


__all__ = ["router"]
