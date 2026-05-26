"""Connector-type REST routes (CS-IMPL-017).

Per design § Public Interface, connector types are global (not
workspace-scoped):

* POST   ``/v1/catalog/connector-types`` — register.
* GET    ``/v1/catalog/connector-types?type=…`` — list versions for a type.
* GET    ``/v1/catalog/connector-types/{type}@{version}`` — get.
* POST   ``/v1/catalog/connector-types/{type}@{version}:deprecate``.

There is no workspace dimension, so :func:`require_permission_only` is
used in place of the workspace-matching variant.
"""

from __future__ import annotations

import re

from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query

from custos_catalog.api.dependencies import (
    get_connector_registry,
    require_permission_only,
)
from custos_catalog.api.models import (
    ConnectorTypeListResponse,
    ConnectorTypeRefBody,
    ConnectorTypeRegisterRequest,
    ConnectorTypeVersionBody,
    DeprecateRequest,
    StatusResponse,
)
from custos_catalog.managers.connector_registry import ConnectorTypeRegistry
from custos_catalog.middleware.callctx import CallContext

router = APIRouter()

_REF_RE = re.compile(r"^(?P<type>[^@/]+)@(?P<version>[^@/]+)$")


def _parse_ref(ref: str) -> tuple[str, str]:
    match = _REF_RE.match(ref)
    if match is None:
        raise HTTPException(
            status_code=400,
            detail={
                "error": {
                    "code": "catalog.connector_type_ref_invalid",
                    "detail": f"connector type ref {ref!r} is not of the form <type>@<version>",
                },
            },
        )
    return match.group("type"), match.group("version")


def _serialize(row: object) -> ConnectorTypeVersionBody:
    return ConnectorTypeVersionBody(
        type=getattr(row, "type"),  # noqa: B009
        version=getattr(row, "version"),  # noqa: B009
        digest=getattr(row, "digest"),  # noqa: B009
        normalizedManifest=dict(getattr(row, "normalized_manifest")),  # noqa: B009
        parentDeprecated=bool(getattr(row, "parent_deprecated", False)),
        publishedAt=getattr(row, "published_at"),  # noqa: B009
    )


def _serialize_ref(row: object) -> ConnectorTypeRefBody:
    """Project an SPL row down to its ref tuple for list responses.

    The list endpoint contract is ``[ConnectorTypeRef]`` (refs only) per
    design § Public Interface; callers fetch the full normalized
    manifest via the get-by-ref endpoint.
    """
    return ConnectorTypeRefBody(
        type=getattr(row, "type"),  # noqa: B009
        version=getattr(row, "version"),  # noqa: B009
        digest=getattr(row, "digest"),  # noqa: B009
    )


# ---------------------------------------------------------------------------
# Register
# ---------------------------------------------------------------------------


@router.post(
    "/v1/catalog/connector-types",
    response_model=ConnectorTypeRefBody,
    status_code=201,
    tags=["connector-types"],
    summary="Register a connector-type version.",
)
async def register_connector_type(
    body: ConnectorTypeRegisterRequest = Body(...),
    ctx: CallContext = Depends(require_permission_only("catalog:connector-types:write")),
    registry: ConnectorTypeRegistry = Depends(get_connector_registry),
) -> ConnectorTypeRefBody:
    ref = await registry.register(
        workspace_id=ctx.workspace_id,
        principal_id=ctx.principal_id,
        image_ref=body.image_ref,
        manifest=body.manifest,
    )
    return ConnectorTypeRefBody(
        type=ref.type,
        version=ref.version,
        digest=ref.digest,
    )


# ---------------------------------------------------------------------------
# List
# ---------------------------------------------------------------------------


@router.get(
    "/v1/catalog/connector-types",
    response_model=ConnectorTypeListResponse,
    tags=["connector-types"],
    summary="List versions for a connector type.",
)
async def list_connector_type_versions(
    type: str = Query(..., min_length=1, description="Connector type identifier."),
    cursor: str | None = Query(default=None),
    limit: int | None = Query(default=None, ge=1, le=1000),
    _ctx: CallContext = Depends(require_permission_only("catalog:connector-types:read")),
    registry: ConnectorTypeRegistry = Depends(get_connector_registry),
) -> ConnectorTypeListResponse:
    from custos_spl.pagination import Cursor

    page = await registry.list(
        type=type,
        cursor=Cursor(token=cursor) if cursor is not None else None,
        limit=limit,
    )
    return ConnectorTypeListResponse(
        items=[_serialize_ref(row) for row in page.items],
        nextCursor=page.next_cursor.token if page.next_cursor else None,
    )


# ---------------------------------------------------------------------------
# Get
# ---------------------------------------------------------------------------


@router.get(
    "/v1/catalog/connector-types/{ref}",
    response_model=ConnectorTypeVersionBody,
    tags=["connector-types"],
    summary="Fetch a connector-type version.",
)
async def get_connector_type_version(
    ref: str = Path(..., description="Connector type ref of the form <type>@<version>."),
    _ctx: CallContext = Depends(require_permission_only("catalog:connector-types:read")),
    registry: ConnectorTypeRegistry = Depends(get_connector_registry),
) -> ConnectorTypeVersionBody:
    type_, version = _parse_ref(ref)
    row = await registry.get(type=type_, version=version)
    return _serialize(row)


# ---------------------------------------------------------------------------
# Deprecate
# ---------------------------------------------------------------------------


@router.post(
    "/v1/catalog/connector-types/{ref}:deprecate",
    response_model=StatusResponse,
    tags=["connector-types"],
    summary="Deprecate a connector-type (parent-row flag).",
)
async def deprecate_connector_type(
    ref: str = Path(...),
    body: DeprecateRequest = Body(default_factory=DeprecateRequest),
    ctx: CallContext = Depends(require_permission_only("catalog:connector-types:write")),
    registry: ConnectorTypeRegistry = Depends(get_connector_registry),
) -> StatusResponse:
    type_, _version = _parse_ref(ref)
    # Version-level deprecation does not exist in v1 — flagging the
    # parent row is the only available semantic. The ``@<version>``
    # segment is parsed-and-discarded so the URL grammar matches the
    # design.
    await registry.deprecate(
        workspace_id=ctx.workspace_id,
        principal_id=ctx.principal_id,
        type=type_,
        reason=body.reason,
    )
    return StatusResponse()


__all__ = ["router"]
