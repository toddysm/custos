"""``GET /v1/workspaces/{ws}/audit/leases`` — operator audit history (CONN-IMPL-026).

The metadata store's audit table is the single source of truth for
lease lifecycle events (``lease.issued``, ``.refreshed``, ``.released``,
``.expired``, ``.revoke-requested``, ``.revoked``, ``.denied``). This
route exposes a paginated, filterable read-through scoped to those
event types.

``query_audit`` on the
:class:`~custos_spl.interfaces.metadata_store.MetadataStoreProvider`
accepts a single ``event_type`` exact-match filter. When the caller
omits ``eventType`` the route fetches without an SPL-level event
filter and post-filters to events whose ``event_type`` is one of the
known lease event types enumerated in :data:`_LEASE_EVENT_TYPES` --
that's how the route can return the union of lease events without
requiring an SPL surface change. Any future ``lease.*`` event type
must be added to the whitelist (and to the ``eventType`` query-param
docstring) before it becomes visible through this route -- the
closed-set design is intentional so the public surface stays a
documented contract rather than implicitly widening whenever a new
audit constant is added downstream. The post-filter is applied
after pagination, so callers MAY see "short pages" (fewer items
than ``limit``); the documented contract is "the SPL cursor is
opaque; pass ``nextCursor`` back to advance".
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Final

from custos_spl.ids import WorkspaceId
from custos_spl.interfaces.metadata_store import AuditFilter
from custos_spl.pagination import Cursor
from fastapi import APIRouter, Depends, Path, Query, Request
from pydantic import BaseModel, ConfigDict, Field
from starlette.responses import JSONResponse, Response

from custos_connector.api._common import (
    error_response,
    resolve_metadata_store,
    workspace_mismatch_response,
)
from custos_connector.middleware import CallContext, require_permission
from custos_connector.permissions import AUDIT_READ

router = APIRouter(prefix="/v1", tags=["audit"])


#: Event types this endpoint exposes. Mirrors the ``EVENT_LEASE_*``
#: constants in :mod:`custos_connector.audit`; we don't import them
#: here to keep the route's contract pinned to wire strings rather
#: than to the audit module's internal names.
_LEASE_EVENT_TYPES: Final[frozenset[str]] = frozenset(
    {
        "lease.issued",
        "lease.refreshed",
        "lease.released",
        "lease.expired",
        "lease.revoke-requested",
        "lease.revoked",
        "lease.denied",
    }
)

#: Hard cap on ``limit``. The SPL surface accepts ``None`` (adapter
#: default) but we want a documented ceiling on the public surface.
_DEFAULT_LIMIT: Final[int] = 100
_MAX_LIMIT: Final[int] = 500


class _AuditEventWire(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    workspace_id: str = Field(..., alias="workspaceId")
    event_id: str = Field(..., alias="eventId")
    event_type: str = Field(..., alias="eventType")
    actor: str
    subject: dict[str, Any]
    payload: dict[str, Any]
    occurred_at: datetime = Field(..., alias="occurredAt")


class _AuditListResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    items: list[_AuditEventWire]
    next_cursor: str | None = Field(default=None, alias="nextCursor")


def _row_to_wire(row: Any) -> _AuditEventWire:
    return _AuditEventWire(
        workspaceId=str(row.workspace_id),
        eventId=row.event_id,
        eventType=row.event_type,
        actor=row.actor,
        subject=dict(row.subject),
        payload=dict(row.payload),
        occurredAt=row.occurred_at,
    )


@router.get(
    "/workspaces/{ws}/audit/leases",
    summary="Query the audit log for lease-related events.",
)
async def list_lease_audit_events(
    request: Request,
    ctx: Annotated[CallContext, Depends(require_permission(AUDIT_READ))],
    ws: Annotated[str, Path(min_length=1)],
    event_type: Annotated[
        str | None,
        Query(
            alias="eventType",
            description=(
                "Optional single event-type filter. Must be one of: "
                "lease.issued, lease.refreshed, lease.released, "
                "lease.expired, lease.revoke-requested, lease.revoked, "
                "lease.denied. Omit to query all lease events."
            ),
        ),
    ] = None,
    actor: Annotated[str | None, Query(min_length=1)] = None,
    occurred_after: Annotated[datetime | None, Query(alias="occurredAfter")] = None,
    occurred_before: Annotated[datetime | None, Query(alias="occurredBefore")] = None,
    cursor: Annotated[str | None, Query()] = None,
    limit: Annotated[int | None, Query(ge=1, le=_MAX_LIMIT)] = None,
) -> Response:
    """List lease audit events for a workspace, newest-first.

    When ``eventType`` is omitted, the route fetches without an
    SPL-level event filter and post-filters to events whose
    ``event_type`` is one of the known lease event types in
    :data:`_LEASE_EVENT_TYPES`. Callers narrowing to a single lease
    event type SHOULD pass ``eventType`` so the SPL surface does the
    filter and pagination stays dense.
    """
    mismatch = workspace_mismatch_response(ctx, ws)
    if mismatch is not None:
        return mismatch
    if event_type is not None and event_type not in _LEASE_EVENT_TYPES:
        return error_response(
            status_code=400,
            code="connector.audit_event_type_invalid",
            detail=(
                f"eventType {event_type!r} is not a lease event type; "
                f"expected one of {sorted(_LEASE_EVENT_TYPES)}"
            ),
        )
    effective_limit = limit if limit is not None else _DEFAULT_LIMIT
    metadata_store = resolve_metadata_store(request)
    audit_filter = AuditFilter(
        event_type=event_type,
        actor=actor,
        occurred_after=occurred_after,
        occurred_before=occurred_before,
    )
    page = await metadata_store.query_audit(
        WorkspaceId(ws),
        filter=audit_filter,
        cursor=Cursor(cursor) if cursor else None,
        limit=effective_limit,
    )
    # Post-filter when the caller did not pin to a single event type.
    if event_type is None:
        items = [r for r in page.items if r.event_type in _LEASE_EVENT_TYPES]
    else:
        items = list(page.items)
    body = _AuditListResponse(
        items=[_row_to_wire(r) for r in items],
        nextCursor=str(page.next_cursor) if page.next_cursor is not None else None,
    )
    return JSONResponse(status_code=200, content=body.model_dump(by_alias=True, mode="json"))


__all__ = ["router"]
