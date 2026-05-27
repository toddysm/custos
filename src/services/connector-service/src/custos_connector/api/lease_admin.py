"""Operator lease-admin REST surface (CONN-IMPL-026).

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

Live-state listing currently reads
:meth:`~custos_spl.interfaces.lease_store.LeaseStoreProvider.list_active_leases`
on the local store only; cross-sidecar fan-out aggregation lands in
CONN-IMPL-028 (#311). The route shapes here are stable so that
follow-up swap can be a service-layer change.

Operator-identity-in-audit caveat
---------------------------------

The :class:`~custos_connector.lease.service.LeaseManager` was sized
for the internal CONN-IMPL-019/020 control-channel surface where the
actor is fixed at construction time. As a result, audit rows emitted
from these operator routes currently carry ``actor="connector-service"``
rather than the call-context principal id. The HTTP layer still
enforces the operator's permission token, but the forensic actor on
the audit row will be cleaned up in CONN-IMPL-028 (#311) when the
manager grows a per-call ``actor`` override.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any

from custos_spl.ids import ConnectorInstanceId, RunId, WorkspaceId
from custos_spl.interfaces.lease_store import LeaseFilter
from custos_spl.pagination import Cursor
from fastapi import APIRouter, Body, Depends, Path, Query, Request
from pydantic import BaseModel, ConfigDict, Field
from starlette.responses import JSONResponse, Response

from custos_connector.api._common import (
    error_response,
    resolve_lease_manager,
    resolve_lease_store,
    workspace_mismatch_response,
)
from custos_connector.lease.service import RevokeOutcomeStatus
from custos_connector.middleware import CallContext, require_permission
from custos_connector.permissions import ADMIN_CONNECTOR, CONNECTOR_READ

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
    """Body for ``POST /v1/workspaces/{ws}/leases/{leaseId}:revoke``."""

    model_config = ConfigDict(extra="forbid")

    reason: str = Field(..., min_length=1, max_length=500)


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


@router.post(
    "/workspaces/{ws}/leases/{lease_id}:revoke",
    summary="Revoke a single lease.",
)
async def revoke_single_lease(
    request: Request,
    ctx: Annotated[CallContext, Depends(require_permission(ADMIN_CONNECTOR))],
    ws: Annotated[str, Path(min_length=1)],
    lease_id: Annotated[str, Path(min_length=1)],
    body: _RevokeRequest = Body(...),
) -> Response:
    """Revoke one lease.

    Returns 200 with ``{leaseId, status}`` when the lease was found
    (``status`` is one of ``revoked`` / ``already-revoked`` /
    ``already-expired``); returns 404 with the same envelope when
    the lease does not exist in this workspace.
    """
    mismatch = workspace_mismatch_response(ctx, ws)
    if mismatch is not None:
        return mismatch
    manager = resolve_lease_manager(request)
    outcome = await manager.revoke_with_status(
        workspace_id=WorkspaceId(ws),
        lease_id=lease_id,
        reason=body.reason,
    )
    if outcome.status is RevokeOutcomeStatus.NOT_FOUND:
        return error_response(
            status_code=404,
            code="connector.lease_not_found",
            detail=f"lease {lease_id!r} not found in workspace {ws!r}",
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
    lease_filter: LeaseFilter,
    reason: str,
) -> JSONResponse:
    """Walk every active lease matching ``lease_filter`` and revoke.

    Pagination is taken to completion (no operator-visible cursor)
    so a single ``:revoke-all`` call mints one ack per active lease.
    Live-state fan-out across sidecars is CONN-IMPL-028 territory;
    this implementation reads the local store only.
    """
    lease_store = resolve_lease_store(request)
    manager = resolve_lease_manager(request)
    workspace = WorkspaceId(workspace_id)
    results: list[_RevokeAllResultEntry] = []
    revoked_count = 0
    cursor: Cursor | None = None
    while True:
        page = await lease_store.list_active_leases(
            workspace,
            filter=lease_filter,
            cursor=cursor,
        )
        for lease in page.items:
            outcome = await manager.revoke_with_status(
                workspace_id=workspace,
                lease_id=lease.lease_id,
                reason=reason,
            )
            if outcome.status is RevokeOutcomeStatus.REVOKED:
                revoked_count += 1
            results.append(
                _RevokeAllResultEntry(leaseId=lease.lease_id, status=outcome.status.value)
            )
        if page.next_cursor is None:
            break
        cursor = page.next_cursor
    body = _RevokeAllResponse(results=results, revoked=revoked_count, total=len(results))
    return JSONResponse(status_code=200, content=body.model_dump(by_alias=True))


@router.post(
    "/workspaces/{ws}/connectors/{instance_id}/leases:revoke-all",
    summary="Revoke every active lease for a connector instance.",
)
async def revoke_all_for_instance(
    request: Request,
    ctx: Annotated[CallContext, Depends(require_permission(ADMIN_CONNECTOR))],
    ws: Annotated[str, Path(min_length=1)],
    instance_id: Annotated[str, Path(min_length=1)],
    body: _RevokeRequest = Body(...),
) -> Response:
    mismatch = workspace_mismatch_response(ctx, ws)
    if mismatch is not None:
        return mismatch
    return await _revoke_many(
        request,
        workspace_id=ws,
        lease_filter=LeaseFilter(connector_instance_id=ConnectorInstanceId(instance_id)),
        reason=body.reason,
    )


@router.post(
    "/workspaces/{ws}/runs/{run_id}/leases:revoke-all",
    summary="Revoke every active lease for a run.",
)
async def revoke_all_for_run(
    request: Request,
    ctx: Annotated[CallContext, Depends(require_permission(ADMIN_CONNECTOR))],
    ws: Annotated[str, Path(min_length=1)],
    run_id: Annotated[str, Path(min_length=1)],
    body: _RevokeRequest = Body(...),
) -> Response:
    mismatch = workspace_mismatch_response(ctx, ws)
    if mismatch is not None:
        return mismatch
    return await _revoke_many(
        request,
        workspace_id=ws,
        lease_filter=LeaseFilter(run_id=RunId(run_id)),
        reason=body.reason,
    )


__all__ = ["router"]
