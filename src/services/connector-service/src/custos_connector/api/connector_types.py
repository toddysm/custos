"""``GET /v1/workspaces/{ws}/connector-types`` — list versions for a type.

The Connector Service does not own the connector-type catalog (Catalog
Service does), but exposing a read-through here means operator tooling
that already targets a workspace's connector instances can look up the
matching connector-type versions without holding the catalog-service
client. The route is workspace-scoped so the call-context guard and the
workspace-mismatch check are uniform across the public surface — the
underlying catalog rows themselves are global.

The SPL surface
(:meth:`custos_spl.interfaces.catalog_store.CatalogStoreProvider.list_connector_type_versions`)
requires a ``type`` argument; this route exposes it as a mandatory
``type`` query parameter to keep the implementation contract honest. A
"list every type" endpoint is intentionally not part of this surface —
it would require a new SPL method and is not needed by any documented
operator workflow.
"""

from __future__ import annotations

from typing import Annotated, Any

from custos_spl.pagination import Cursor
from fastapi import APIRouter, Depends, Path, Query, Request
from pydantic import BaseModel, ConfigDict, Field
from starlette.responses import JSONResponse, Response

from custos_connector.api._common import (
    error_response,
    resolve_catalog_store,
    workspace_mismatch_response,
)
from custos_connector.middleware import CallContext, require_permission
from custos_connector.permissions import CONNECTOR_READ

router = APIRouter(prefix="/v1", tags=["connector-types"])


class _ConnectorTypeVersionWire(BaseModel):
    """Wire shape of one connector-type version row in the list response."""

    model_config = ConfigDict(populate_by_name=True)

    type: str
    version: str
    digest: str
    image_ref: str = Field(..., alias="imageRef")
    deprecated: bool = Field(default=False)


class _ConnectorTypeListResponse(BaseModel):
    """Wire shape of ``GET /v1/workspaces/{ws}/connector-types``."""

    model_config = ConfigDict(populate_by_name=True)

    items: list[_ConnectorTypeVersionWire]
    next_cursor: str | None = Field(default=None, alias="nextCursor")


def _row_to_wire(row: Any) -> _ConnectorTypeVersionWire:
    return _ConnectorTypeVersionWire(
        type=row.type,
        version=row.version,
        digest=row.digest,
        imageRef=row.image_ref,
        deprecated=bool(row.parent_deprecated),
    )


@router.get(
    "/workspaces/{ws}/connector-types",
    summary="List connector-type versions for a given type.",
)
async def list_connector_type_versions(
    request: Request,
    ctx: Annotated[CallContext, Depends(require_permission(CONNECTOR_READ))],
    ws: Annotated[str, Path(min_length=1)],
    type: Annotated[str, Query(min_length=1, description="Connector type to list.")],
    cursor: Annotated[str | None, Query(description="Opaque pagination cursor.")] = None,
    limit: Annotated[int | None, Query(ge=1, le=500)] = None,
) -> Response:
    """List every persisted version of a single connector type.

    Connector types themselves are global, but this route is mounted
    under ``/v1/workspaces/{ws}/...`` so the operator surface has a
    consistent shape and the call-context's ``workspace_id`` must
    still match the ``{ws}`` path segment — that keeps the route
    table workspace-pivoted even where the underlying data is not.

    Returns 200 with ``{items: [...], nextCursor: ...}``. The cursor
    is opaque; callers MUST pass it back verbatim to advance through
    pages.
    """
    mismatch = workspace_mismatch_response(ctx, ws)
    if mismatch is not None:
        return mismatch
    catalog_store = resolve_catalog_store(request)
    try:
        page = await catalog_store.list_connector_type_versions(
            type,
            cursor=Cursor(cursor) if cursor else None,
            limit=limit,
        )
    except ValueError as exc:
        return error_response(
            status_code=400,
            code="connector.connector_type_list_invalid",
            detail=str(exc),
        )
    body = _ConnectorTypeListResponse(
        items=[_row_to_wire(r) for r in page.items],
        nextCursor=str(page.next_cursor) if page.next_cursor is not None else None,
    )
    return JSONResponse(status_code=200, content=body.model_dump(by_alias=True))


__all__ = ["router"]
