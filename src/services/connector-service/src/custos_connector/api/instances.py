"""REST routes for connector-instance CRUD + lifecycle (CONN-IMPL-026).

Maps the operator-facing instance surface from
``design/components/connector-service/design.md`` § *Public Interface*
to :class:`~custos_connector.instances.service.InstanceService`:

* ``POST   /v1/workspaces/{ws}/connectors``                        — create
* ``GET    /v1/workspaces/{ws}/connectors``                        — list (cursor + filters)
* ``GET    /v1/workspaces/{ws}/connectors/{id}``                   — read one
* ``PATCH  /v1/workspaces/{ws}/connectors/{id}``                   — partial update
* ``POST   /v1/workspaces/{ws}/connectors/{id}:enable``            — activation + probe
* ``POST   /v1/workspaces/{ws}/connectors/{id}:disable``           — soft-disable
* ``GET    /v1/workspaces/{ws}/connectors/{id}/health``            — cached probe
* ``POST   /v1/workspaces/{ws}/connectors/{id}:force-health-check``— bypass cache

Domain errors raised by :class:`InstanceService` are translated to
HTTP statuses by :func:`_error_response_for`. Wire models accept
``snake_case`` and ``camelCase`` aliases interchangeably (catalog
service does the same) so the operator surface tolerates both
conventions; responses always emit ``camelCase``.

Permission policy mirrors the cursor-admin router pattern:

* ``GET``  routes require :data:`CONNECTOR_READ`.
* Every mutating + ``:enable`` / ``:disable`` /
  ``:force-health-check`` route requires :data:`ADMIN_CONNECTOR`.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any

from custos_spl.pagination import Cursor
from fastapi import APIRouter, Body, Depends, Path, Query, Request
from pydantic import BaseModel, ConfigDict, Field
from starlette.responses import JSONResponse, Response

from custos_connector.api._common import (
    error_response,
    resolve_instance_service,
    workspace_mismatch_response,
)
from custos_connector.instances.service import (
    ActivationProbeFailed,
    ConnectorInstanceNotFound,
    ConnectorTypeNotRegistered,
    ImmutableFieldUpdate,
    InstanceHealthSnapshot,
    InstanceServiceError,
    InvalidInstancePayload,
    InvalidLeaseTtl,
)
from custos_connector.instances.validator import InstanceConfigValidationError
from custos_connector.middleware import CallContext, require_permission
from custos_connector.permissions import ADMIN_CONNECTOR, CONNECTOR_READ

router = APIRouter(prefix="/v1", tags=["connectors"])


# ---------------------------------------------------------------------------
# Wire models
# ---------------------------------------------------------------------------


class _ConnectorInstanceWire(BaseModel):
    """Wire shape of a :class:`ConnectorInstance` row.

    Server-supplied fields (``instanceId``, ``status``, ``healthStatus``,
    ``createdAt``, ``updatedAt``, ``workspaceId``) ride alongside the
    operator-supplied ones in the response. Requests must not set the
    server-supplied fields — the create / patch wire models below use
    a tighter shape.
    """

    model_config = ConfigDict(populate_by_name=True)

    workspace_id: str = Field(..., alias="workspaceId")
    instance_id: str = Field(..., alias="instanceId")
    type: str
    version: str
    name: str | None = None
    lease_ttl_seconds: int | None = Field(default=None, alias="leaseTtlSeconds")
    enabled: bool
    status: str
    health_status: str | None = Field(default=None, alias="healthStatus")
    target_config: dict[str, Any] = Field(default_factory=dict, alias="targetConfig")
    credentials_authentication: dict[str, Any] = Field(
        default_factory=dict, alias="credentialsAuthentication"
    )
    used_capabilities: list[str] | None = Field(default=None, alias="usedCapabilities")
    created_at: datetime = Field(..., alias="createdAt")
    updated_at: datetime = Field(..., alias="updatedAt")


class _CreateConnectorRequest(BaseModel):
    """Body for ``POST /v1/workspaces/{ws}/connectors``."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    type: str = Field(..., min_length=1)
    version: str = Field(..., min_length=1)
    name: str | None = Field(default=None)
    lease_ttl_seconds: int | None = Field(default=None, alias="leaseTtlSeconds")
    enabled: bool = Field(default=True)
    target_config: dict[str, Any] | None = Field(default=None, alias="targetConfig")
    credentials_authentication: dict[str, Any] | None = Field(
        default=None, alias="credentialsAuthentication"
    )
    used_capabilities: list[str] | None = Field(default=None, alias="usedCapabilities")


class _PatchConnectorRequest(BaseModel):
    """Body for ``PATCH /v1/workspaces/{ws}/connectors/{id}``.

    All fields are optional; only present keys are forwarded to
    :meth:`InstanceService.patch`. Setting ``leaseTtlSeconds`` to
    ``null`` clears the per-instance override and restores the
    deployment default. The handler never silently drops unknown
    fields — Pydantic's ``extra="forbid"`` rejects them at parse
    time so the API surface speaks with one voice (rather than
    delegating the "unknown field" 400 to ``ImmutableFieldUpdate``).
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    name: str | None = Field(default=None)
    lease_ttl_seconds: int | None = Field(default=None, alias="leaseTtlSeconds")
    enabled: bool | None = Field(default=None)


class _ConnectorInstanceListResponse(BaseModel):
    """Wire shape of ``GET /v1/workspaces/{ws}/connectors``."""

    model_config = ConfigDict(populate_by_name=True)

    items: list[_ConnectorInstanceWire]
    next_cursor: str | None = Field(default=None, alias="nextCursor")


class _HealthSnapshotWire(BaseModel):
    """Wire shape of an :class:`InstanceHealthSnapshot`."""

    model_config = ConfigDict(populate_by_name=True)

    workspace_id: str = Field(..., alias="workspaceId")
    instance_id: str = Field(..., alias="instanceId")
    healthy: bool
    detail: str | None
    checked_at: datetime = Field(..., alias="checkedAt")
    source: str


# ---------------------------------------------------------------------------
# Mapping helpers
# ---------------------------------------------------------------------------


def _instance_to_wire(row: Any) -> _ConnectorInstanceWire:
    return _ConnectorInstanceWire(
        workspaceId=str(row.workspace_id),
        instanceId=str(row.instance_id),
        type=row.type,
        version=row.version,
        name=row.name,
        leaseTtlSeconds=row.lease_ttl_seconds,
        enabled=row.enabled,
        status=row.status,
        healthStatus=row.health_status,
        targetConfig=dict(row.target_config),
        credentialsAuthentication=dict(row.credentials_authentication),
        usedCapabilities=(list(row.used_capabilities) if row.used_capabilities else None),
        createdAt=row.created_at,
        updatedAt=row.updated_at,
    )


def _health_to_wire(snapshot: InstanceHealthSnapshot) -> _HealthSnapshotWire:
    return _HealthSnapshotWire(
        workspaceId=snapshot.workspace_id,
        instanceId=snapshot.instance_id,
        healthy=snapshot.healthy,
        detail=snapshot.detail,
        checkedAt=snapshot.checked_at,
        source=snapshot.source,
    )


def _error_response_for(exc: InstanceServiceError) -> JSONResponse:
    """Translate a service-layer error into the canonical envelope."""
    if isinstance(exc, ConnectorInstanceNotFound):
        status = 404
    elif isinstance(exc, ConnectorTypeNotRegistered):
        status = 400
    elif isinstance(exc, ImmutableFieldUpdate):
        status = 409
    elif isinstance(exc, InvalidLeaseTtl | InvalidInstancePayload):
        status = 400
    elif isinstance(exc, ActivationProbeFailed):
        status = 503
    else:  # pragma: no cover - defensive default
        status = 500
    return error_response(status_code=status, code=exc.code, detail=str(exc))


def _validation_error_response(exc: InstanceConfigValidationError) -> JSONResponse:
    """Render the full per-issue list from the manifest validator.

    The service layer aggregates every issue from
    :func:`validate_instance_config` into a single
    :class:`InstanceConfigValidationError`; the API surface MUST
    surface them all rather than picking one — otherwise the
    operator has to play whack-a-mole. The wire envelope keeps the
    canonical ``error.code/detail`` shape and adds a sibling
    ``error.issues`` array carrying ``{code, path, detail}`` per
    issue.
    """
    issues_payload = [
        {"code": str(issue.code), "path": issue.path, "detail": issue.detail}
        for issue in exc.issues
    ]
    rendered = "; ".join(
        (f"[{i.code}] {i.path}: {i.detail}" if i.path else f"[{i.code}] {i.detail}")
        for i in exc.issues
    )
    return JSONResponse(
        status_code=400,
        content={
            "error": {
                "code": "connector.instance_config_invalid",
                "detail": (
                    f"instance config validation failed: {rendered}"
                    if rendered
                    else "instance config validation failed"
                ),
                "issues": issues_payload,
            }
        },
    )


def _patch_updates(body: _PatchConnectorRequest) -> dict[str, Any]:
    """Project a parsed PATCH body to the ``updates`` mapping ``InstanceService.patch`` accepts.

    Pydantic v2 distinguishes "field not present" from "field set to
    ``None``" via ``model_fields_set``; we honor that so a request
    that omits ``leaseTtlSeconds`` does NOT clear the per-instance
    override.
    """
    updates: dict[str, Any] = {}
    if "name" in body.model_fields_set:
        updates["name"] = body.name
    if "lease_ttl_seconds" in body.model_fields_set:
        updates["lease_ttl_seconds"] = body.lease_ttl_seconds
    if "enabled" in body.model_fields_set:
        updates["enabled"] = body.enabled
    return updates


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.post(
    "/workspaces/{ws}/connectors",
    status_code=201,
    summary="Create a connector instance.",
)
async def create_connector(
    request: Request,
    ctx: Annotated[CallContext, Depends(require_permission(ADMIN_CONNECTOR))],
    ws: Annotated[str, Path(min_length=1)],
    body: _CreateConnectorRequest = Body(...),
) -> Response:
    mismatch = workspace_mismatch_response(ctx, ws)
    if mismatch is not None:
        return mismatch
    service = resolve_instance_service(request)
    try:
        row = await service.create(
            ws,
            type=body.type,
            version=body.version,
            actor=ctx.principal_id,
            name=body.name,
            lease_ttl_seconds=body.lease_ttl_seconds,
            enabled=body.enabled,
            target_config=body.target_config,
            credentials_authentication=body.credentials_authentication,
            used_capabilities=(
                tuple(body.used_capabilities) if body.used_capabilities is not None else None
            ),
        )
    except InstanceConfigValidationError as exc:
        return _validation_error_response(exc)
    except InstanceServiceError as exc:
        return _error_response_for(exc)
    wire = _instance_to_wire(row)
    return JSONResponse(status_code=201, content=wire.model_dump(by_alias=True, mode="json"))


@router.get(
    "/workspaces/{ws}/connectors",
    summary="List connector instances in a workspace.",
)
async def list_connectors(
    request: Request,
    ctx: Annotated[CallContext, Depends(require_permission(CONNECTOR_READ))],
    ws: Annotated[str, Path(min_length=1)],
    type: Annotated[str | None, Query(min_length=1)] = None,
    enabled: Annotated[bool | None, Query()] = None,
    cursor: Annotated[str | None, Query()] = None,
    limit: Annotated[int | None, Query(ge=1, le=500)] = None,
) -> Response:
    mismatch = workspace_mismatch_response(ctx, ws)
    if mismatch is not None:
        return mismatch
    service = resolve_instance_service(request)
    page = await service.list(
        ws,
        type=type,
        enabled=enabled,
        cursor=Cursor(cursor) if cursor else None,
        limit=limit,
    )
    body = _ConnectorInstanceListResponse(
        items=[_instance_to_wire(r) for r in page.items],
        nextCursor=str(page.next_cursor) if page.next_cursor is not None else None,
    )
    return JSONResponse(status_code=200, content=body.model_dump(by_alias=True, mode="json"))


@router.get(
    "/workspaces/{ws}/connectors/{instance_id}",
    summary="Read a connector instance.",
)
async def get_connector(
    request: Request,
    ctx: Annotated[CallContext, Depends(require_permission(CONNECTOR_READ))],
    ws: Annotated[str, Path(min_length=1)],
    instance_id: Annotated[str, Path(min_length=1)],
) -> Response:
    mismatch = workspace_mismatch_response(ctx, ws)
    if mismatch is not None:
        return mismatch
    service = resolve_instance_service(request)
    try:
        row = await service.get(ws, instance_id)
    except InstanceServiceError as exc:
        return _error_response_for(exc)
    wire = _instance_to_wire(row)
    return JSONResponse(status_code=200, content=wire.model_dump(by_alias=True, mode="json"))


@router.patch(
    "/workspaces/{ws}/connectors/{instance_id}",
    summary="Partially update a connector instance.",
)
async def patch_connector(
    request: Request,
    ctx: Annotated[CallContext, Depends(require_permission(ADMIN_CONNECTOR))],
    ws: Annotated[str, Path(min_length=1)],
    instance_id: Annotated[str, Path(min_length=1)],
    body: _PatchConnectorRequest = Body(...),
) -> Response:
    mismatch = workspace_mismatch_response(ctx, ws)
    if mismatch is not None:
        return mismatch
    service = resolve_instance_service(request)
    updates = _patch_updates(body)
    try:
        row = await service.patch(ws, instance_id, actor=ctx.principal_id, updates=updates)
    except InstanceServiceError as exc:
        return _error_response_for(exc)
    wire = _instance_to_wire(row)
    return JSONResponse(status_code=200, content=wire.model_dump(by_alias=True, mode="json"))


@router.post(
    "/workspaces/{ws}/connectors/{instance_id}:enable",
    summary="Enable a connector instance (synchronous health probe).",
)
async def enable_connector(
    request: Request,
    ctx: Annotated[CallContext, Depends(require_permission(ADMIN_CONNECTOR))],
    ws: Annotated[str, Path(min_length=1)],
    instance_id: Annotated[str, Path(min_length=1)],
) -> Response:
    mismatch = workspace_mismatch_response(ctx, ws)
    if mismatch is not None:
        return mismatch
    service = resolve_instance_service(request)
    try:
        row = await service.enable(ws, instance_id, actor=ctx.principal_id)
    except InstanceServiceError as exc:
        return _error_response_for(exc)
    wire = _instance_to_wire(row)
    return JSONResponse(status_code=200, content=wire.model_dump(by_alias=True, mode="json"))


@router.post(
    "/workspaces/{ws}/connectors/{instance_id}:disable",
    summary="Disable a connector instance.",
)
async def disable_connector(
    request: Request,
    ctx: Annotated[CallContext, Depends(require_permission(ADMIN_CONNECTOR))],
    ws: Annotated[str, Path(min_length=1)],
    instance_id: Annotated[str, Path(min_length=1)],
) -> Response:
    mismatch = workspace_mismatch_response(ctx, ws)
    if mismatch is not None:
        return mismatch
    service = resolve_instance_service(request)
    try:
        row = await service.disable(ws, instance_id, actor=ctx.principal_id)
    except InstanceServiceError as exc:
        return _error_response_for(exc)
    wire = _instance_to_wire(row)
    return JSONResponse(status_code=200, content=wire.model_dump(by_alias=True, mode="json"))


@router.get(
    "/workspaces/{ws}/connectors/{instance_id}/health",
    summary="Return the cached health snapshot (probes only when stale).",
)
async def read_connector_health(
    request: Request,
    ctx: Annotated[CallContext, Depends(require_permission(CONNECTOR_READ))],
    ws: Annotated[str, Path(min_length=1)],
    instance_id: Annotated[str, Path(min_length=1)],
) -> Response:
    mismatch = workspace_mismatch_response(ctx, ws)
    if mismatch is not None:
        return mismatch
    service = resolve_instance_service(request)
    try:
        snapshot = await service.get_health(ws, instance_id)
    except InstanceServiceError as exc:
        return _error_response_for(exc)
    return JSONResponse(
        status_code=200, content=_health_to_wire(snapshot).model_dump(by_alias=True, mode="json")
    )


@router.post(
    "/workspaces/{ws}/connectors/{instance_id}:force-health-check",
    summary="Force a synchronous health probe (bypasses the cache).",
)
async def force_connector_health_check(
    request: Request,
    ctx: Annotated[CallContext, Depends(require_permission(ADMIN_CONNECTOR))],
    ws: Annotated[str, Path(min_length=1)],
    instance_id: Annotated[str, Path(min_length=1)],
) -> Response:
    mismatch = workspace_mismatch_response(ctx, ws)
    if mismatch is not None:
        return mismatch
    service = resolve_instance_service(request)
    try:
        snapshot = await service.force_health_check(ws, instance_id, actor=ctx.principal_id)
    except InstanceServiceError as exc:
        return _error_response_for(exc)
    return JSONResponse(
        status_code=200, content=_health_to_wire(snapshot).model_dump(by_alias=True, mode="json")
    )


__all__ = ["router"]
