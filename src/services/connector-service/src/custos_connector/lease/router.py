"""Internal lease RPC router (CONN-IMPL-019, Phase H/1).

The secret-bridge sidecar delegates capacity-tracked lease bookkeeping
and audit emission to Connector Service so a single Postgres pool +
audit pipeline handle every running activity in the workspace. The
sidecar mints the upstream credential locally (off this surface) but
records the lease here.

Three endpoints, all under ``/internal/v1`` and gated by
:data:`~custos_connector.permissions.CONNECTOR_LEASE_MINT`:

* ``POST /internal/v1/leases:issue``    \u2192 :meth:`LeaseManager.issue`
* ``POST /internal/v1/leases:refresh``  \u2192 :meth:`LeaseManager.refresh`
* ``POST /internal/v1/leases:release``  \u2192 :meth:`LeaseManager.release`

The ``workspaceId`` is pulled from the call-context header (not the
request body) so the sidecar cannot forge a lease across workspaces.
Every other input (``runId``, ``stepId``, ``attempt``, slot, capability,
connector instance, TTL hints) comes from the body.

Domain failures raised by :class:`LeaseManager` are
:class:`LeaseError` instances and are mapped to HTTP via
:func:`~custos_connector.lease.errors.http_status_for`; the response
envelope ``{"error": {"code", "detail"}}`` matches the rest of the
service's error contract (and is what the sidecar's ``LeaseGateway``
unwraps back into a :class:`LeaseError`).

``CAPACITY_EXCEEDED`` (HTTP 429) additionally carries a fixed
``Retry-After: 5`` header so the sidecar can forward it to the
activity per the design's failure-mode table for ``GET /v1/token``.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Final

from custos_spl.ids import ConnectorInstanceId, RunId, StepId, WorkspaceId
from custos_spl.interfaces.lease_store import Lease
from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, ConfigDict, Field
from starlette.responses import JSONResponse, Response

from custos_connector.lease.errors import LeaseError, http_status_for
from custos_connector.lease.service import LeaseManager
from custos_connector.middleware import CallContext, require_permission
from custos_connector.permissions import CONNECTOR_LEASE_MINT

router = APIRouter(prefix="/internal/v1", tags=["lease"])


#: ``Retry-After`` seconds returned with a 429 ``CAPACITY_EXCEEDED``
#: response. Picked to be short enough that a chatty activity is not
#: stalled but long enough that another lease likely released in the
#: meantime. The sidecar forwards this header to the activity verbatim.
_CAPACITY_RETRY_AFTER_SEC: Final[int] = 5


# ---------------------------------------------------------------------------
# Wire models
# ---------------------------------------------------------------------------


class _IssueWire(BaseModel):
    """Wire shape of the ``issue`` request body.

    ``workspaceId`` is intentionally absent \u2014 it comes from the
    call-context header so the sidecar cannot forge a lease across
    workspaces.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    run_id: str = Field(..., min_length=1, alias="runId")
    step_id: str = Field(..., min_length=1, alias="stepId")
    attempt: int = Field(..., ge=1)
    slot: str = Field(..., min_length=1)
    capability: str = Field(..., min_length=1)
    connector_instance_id: str = Field(..., min_length=1, alias="connectorInstanceId")
    token_type: str = Field(..., min_length=1, alias="tokenType")
    requested_ttl_sec: int | None = Field(default=None, alias="requestedTtlSec")
    type_max_ttl_sec: int | None = Field(default=None, alias="typeMaxTtlSec")
    instance_ttl_sec: int | None = Field(default=None, alias="instanceTtlSec")
    step_deadline: datetime | None = Field(default=None, alias="stepDeadline")


class _RefreshWire(BaseModel):
    """Wire shape of the ``refresh`` request body."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    lease_id: str = Field(..., min_length=1, alias="leaseId")
    requested_ttl_sec: int | None = Field(default=None, alias="requestedTtlSec")
    type_max_ttl_sec: int | None = Field(default=None, alias="typeMaxTtlSec")
    instance_ttl_sec: int | None = Field(default=None, alias="instanceTtlSec")
    step_deadline: datetime | None = Field(default=None, alias="stepDeadline")


class _ReleaseWire(BaseModel):
    """Wire shape of the ``release`` request body."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    lease_id: str = Field(..., min_length=1, alias="leaseId")


class _RevokeWire(BaseModel):
    """Wire shape of the ``revoke`` request body (CONN-IMPL-020).

    Sidecar control channel sends ``{leaseIds: [...], reason: "..."}``.
    The endpoint returns a per-lease ack list so callers can
    distinguish ``revoked`` / ``already-revoked`` / ``already-expired``
    / ``not-found`` outcomes in a single round-trip.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    lease_ids: list[Annotated[str, Field(min_length=1)]] = Field(
        ..., min_length=1, alias="leaseIds"
    )
    reason: str = Field(..., min_length=1)


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------


def _lease_to_wire(lease: Lease) -> dict[str, Any]:
    """Render a :class:`Lease` as the JSON envelope returned to the sidecar.

    All datetimes are emitted as RFC 3339 strings (``isoformat()``); the
    sidecar parses them back with ``datetime.fromisoformat``. ``None``
    fields are preserved (the sidecar's ``LeaseGateway`` checks them).
    """
    return {
        "workspaceId": str(lease.workspace_id),
        "leaseId": lease.lease_id,
        "runId": str(lease.run_id),
        "stepId": str(lease.step_id),
        "attempt": lease.attempt,
        "slot": lease.slot,
        "capability": lease.capability,
        "connectorInstanceId": str(lease.connector_instance_id),
        "tokenType": lease.token_type,
        "issuedAt": lease.issued_at.isoformat(),
        "expiresAt": lease.expires_at.isoformat(),
        "releasedAt": lease.released_at.isoformat() if lease.released_at else None,
        "revokedAt": lease.revoked_at.isoformat() if lease.revoked_at else None,
        "revokeReason": lease.revoke_reason,
        "createdAt": lease.created_at.isoformat(),
        "updatedAt": lease.updated_at.isoformat(),
    }


def _lease_error_response(exc: LeaseError) -> JSONResponse:
    """Render a :class:`LeaseError` as the service ``{"error":...}`` envelope.

    Adds ``Retry-After`` for ``CAPACITY_EXCEEDED`` so the sidecar can
    propagate it to the activity (design § failure-mode table for
    ``GET /v1/token``).
    """
    status = http_status_for(exc.code)
    headers: dict[str, str] = {}
    if status == 429:
        headers["Retry-After"] = str(_CAPACITY_RETRY_AFTER_SEC)
    return JSONResponse(
        status_code=status,
        content={"error": {"code": str(exc.code.value), "detail": exc.detail}},
        headers=headers,
    )


# ---------------------------------------------------------------------------
# Dependency wiring
# ---------------------------------------------------------------------------


def _resolve_lease_manager(request: Request) -> LeaseManager:
    """Pull :class:`LeaseManager` off ``app.state.providers``.

    Mirrors the :func:`_resolve_service` pattern in
    :mod:`custos_connector.binding.router`; surfacing a startup wiring
    bug as a ``RuntimeError`` (not an opaque 500) is intentional.
    """
    providers = request.app.state.providers
    lease_manager = getattr(providers, "lease_manager", None)
    if lease_manager is None:
        raise RuntimeError(
            "lease_manager is not wired on app.state.providers; this is a startup wiring bug"
        )
    assert isinstance(lease_manager, LeaseManager)
    return lease_manager


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.post("/leases:issue")
async def issue_lease(
    body: _IssueWire,
    request: Request,
    ctx: Annotated[CallContext, Depends(require_permission(CONNECTOR_LEASE_MINT))],
) -> Response:
    """Mint a new lease for the calling sidecar.

    The ``workspaceId`` is taken from the call-context (not the body).
    On success returns the full lease envelope; on ``LeaseError`` the
    response carries the ``{"error": {"code", "detail"}}`` envelope and
    the HTTP status :func:`http_status_for` selects.
    """
    lease_manager = _resolve_lease_manager(request)
    try:
        lease = await lease_manager.issue(
            workspace_id=WorkspaceId(ctx.workspace_id),
            run_id=RunId(body.run_id),
            step_id=StepId(body.step_id),
            attempt=body.attempt,
            slot=body.slot,
            capability=body.capability,
            connector_instance_id=ConnectorInstanceId(body.connector_instance_id),
            token_type=body.token_type,
            requested_ttl_sec=body.requested_ttl_sec,
            type_max_ttl_sec=body.type_max_ttl_sec,
            instance_ttl_sec=body.instance_ttl_sec,
            step_deadline=body.step_deadline,
        )
    except LeaseError as exc:
        return _lease_error_response(exc)
    return JSONResponse(status_code=200, content={"lease": _lease_to_wire(lease)})


@router.post("/leases:refresh")
async def refresh_lease(
    body: _RefreshWire,
    request: Request,
    ctx: Annotated[CallContext, Depends(require_permission(CONNECTOR_LEASE_MINT))],
) -> Response:
    """Extend the expiry of an existing lease without changing its id."""
    lease_manager = _resolve_lease_manager(request)
    try:
        lease = await lease_manager.refresh(
            workspace_id=WorkspaceId(ctx.workspace_id),
            lease_id=body.lease_id,
            requested_ttl_sec=body.requested_ttl_sec,
            type_max_ttl_sec=body.type_max_ttl_sec,
            instance_ttl_sec=body.instance_ttl_sec,
            step_deadline=body.step_deadline,
        )
    except LeaseError as exc:
        return _lease_error_response(exc)
    return JSONResponse(status_code=200, content={"lease": _lease_to_wire(lease)})


@router.post("/leases:release")
async def release_lease(
    body: _ReleaseWire,
    request: Request,
    ctx: Annotated[CallContext, Depends(require_permission(CONNECTOR_LEASE_MINT))],
) -> Response:
    """Mark a lease as released. Always 204 (idempotent, best-effort).

    The design contract: release is best-effort and clients should not
    distinguish "lease found and released" from "lease unknown / already
    released". Either way the activity's hold on the upstream credential
    is over.
    """
    lease_manager = _resolve_lease_manager(request)
    await lease_manager.release(workspace_id=WorkspaceId(ctx.workspace_id), lease_id=body.lease_id)
    return Response(status_code=204)


@router.post("/leases:revoke")
async def revoke_leases(
    body: _RevokeWire,
    request: Request,
    ctx: Annotated[CallContext, Depends(require_permission(CONNECTOR_LEASE_MINT))],
) -> Response:
    """Revoke a batch of leases. Returns a per-lease ack list (CONN-IMPL-020).

    Body shape: ``{leaseIds: [...], reason: "..."}``.

    Response shape: ``{results: [{leaseId, status}, ...]}`` where
    ``status`` is one of ``revoked`` / ``already-revoked`` /
    ``already-expired`` / ``not-found`` (see
    :class:`~custos_connector.lease.service.RevokeOutcomeStatus`).

    The endpoint always returns 200; per-lease failures are not HTTP
    errors. This matches the sidecar control-channel contract — the
    caller is interested in the per-lease outcome, not a single
    pass/fail for the whole batch. Duplicate ids in the request body
    produce one ack each, in input order; the second ack for a
    duplicate sees ``already-revoked`` because the first call moved
    the lease into the revoked state.
    """
    lease_manager = _resolve_lease_manager(request)
    workspace = WorkspaceId(ctx.workspace_id)
    results: list[dict[str, str]] = []
    for lease_id in body.lease_ids:
        outcome = await lease_manager.revoke_with_status(
            workspace_id=workspace,
            lease_id=lease_id,
            reason=body.reason,
        )
        results.append({"leaseId": lease_id, "status": outcome.status.value})
    return JSONResponse(status_code=200, content={"results": results})


__all__ = ["router"]
