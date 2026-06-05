"""Audit read-back routes (Concern B, audit side) — OBS-IMPL-014.

Two routes over the SPL :meth:`MetadataStoreProvider.query_audit` facade:

* ``GET /v1/workspaces/{ws}/audit`` — a paged search across a workspace's audit
  events, narrowed by ``actor`` / ``eventName`` / ``subjectId`` / ``from`` /
  ``to`` and continued by ``cursor``.
* ``GET /v1/workspaces/{ws}/audit/{eventId}`` — a single audit event lookup,
  ``404`` when the event is absent.

``query_audit`` accepts an :class:`AuditFilter` with ``event_type`` / ``actor``
/ ``occurred_after`` / ``occurred_before`` only — there is **no** backend
``subject`` predicate and **no** point lookup by ``event_id``. So:

* ``subjectId`` is applied as an in-route post-filter over the returned page
  (an event matches when the id appears among its subject values); the page's
  opaque cursor still drives pagination.
* the single-event lookup scans pages (cursor-driven) until the ``event_id`` is
  found or the cursor is exhausted, returning ``404`` if absent.

A ``noop``/unreachable metadata store surfaces as ``503``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated

from custos_spl import Cursor, WorkspaceId
from custos_spl.errors import BackendUnavailable, QueryUnsupported
from custos_spl.interfaces.metadata_store import AuditFilter, MetadataStoreProvider
from fastapi import APIRouter, Depends, HTTPException, Path, Query

from custos_obs.api.dependencies import get_metadata_store
from custos_obs.api.models import AuditEventModel, AuditEventPageModel
from custos_obs.api.routes._common import WorkspacePath, ensure_workspace, parse_iso_datetime
from custos_obs.errors import AuditQueryUnavailable
from custos_obs.middleware import CallContext, require_permission

if TYPE_CHECKING:
    from custos_spl import AuditEvent, Page

__all__ = ["router"]

router = APIRouter(prefix="/v1/workspaces/{workspace_id}", tags=["audit"])

#: Permission scope the call-context middleware enforces for audit read-back.
PERM_READ = "audit:read"

MetadataStoreDep = Annotated[MetadataStoreProvider, Depends(get_metadata_store)]
CtxDep = Annotated[CallContext, Depends(require_permission(PERM_READ))]

_EventIdPath = Annotated[str, Path(min_length=1, description="Audit event id.")]


def _audit_unavailable() -> AuditQueryUnavailable:
    """The metadata store is unreachable — surface as ``503`` problem+json.

    Mirrors the log / metrics 503 envelope (RFC 7807) so all read-back backend
    failures share one content-type and shape. The audit store has no external
    pointer URL, so no ``externalUrl`` extension is attached.
    """
    return AuditQueryUnavailable("the audit metadata store is not available")


def _subject_matches(event: AuditEvent, subject_id: str) -> bool:
    """True when ``subject_id`` appears among the event's subject values.

    The subject is a free-form mapping (commonly ``{"id": ..., "type": ...}``);
    matching against the stringified values avoids guessing the key while still
    honouring the ``subjectId`` filter.
    """
    return any(str(value) == subject_id for value in event.subject.values())


def _filter_page_by_subject(page: Page[AuditEvent], subject_id: str) -> Page[AuditEvent]:
    """Post-filter a page's items by ``subjectId``, preserving the cursor."""
    from custos_spl import Page as _Page

    matched = tuple(event for event in page.items if _subject_matches(event, subject_id))
    return _Page(items=matched, next_cursor=page.next_cursor)


@router.get("/audit")
async def search_audit_events(
    workspace_id: WorkspacePath,
    store: MetadataStoreDep,
    ctx: CtxDep,
    actor: Annotated[str | None, Query(description="Filter by acting principal.")] = None,
    event_name: Annotated[str | None, Query(alias="eventName")] = None,
    subject_id: Annotated[str | None, Query(alias="subjectId")] = None,
    from_: Annotated[str | None, Query(alias="from", description="ISO-8601 start.")] = None,
    to: Annotated[str | None, Query(alias="to", description="ISO-8601 end.")] = None,
    cursor: Annotated[str | None, Query(description="Opaque continuation cursor.")] = None,
) -> AuditEventPageModel:
    """Paged audit search across a workspace.

    ``actor`` / ``eventName`` / ``from`` / ``to`` map onto the backend
    :class:`AuditFilter`; ``subjectId`` is applied as an in-route post-filter.
    """
    ensure_workspace(ctx, workspace_id)
    audit_filter = AuditFilter(
        event_type=event_name,
        actor=actor,
        occurred_after=parse_iso_datetime(from_),
        occurred_before=parse_iso_datetime(to),
    )
    page_cursor = Cursor(token=cursor) if cursor is not None else None
    try:
        page = await store.query_audit(WorkspaceId(workspace_id), audit_filter, page_cursor)
    except (QueryUnsupported, BackendUnavailable) as exc:
        raise _audit_unavailable() from exc
    if subject_id is not None:
        page = _filter_page_by_subject(page, subject_id)
    return AuditEventPageModel.from_domain(page)


@router.get("/audit/{event_id}")
async def get_audit_event(
    workspace_id: WorkspacePath,
    event_id: _EventIdPath,
    store: MetadataStoreDep,
    ctx: CtxDep,
) -> AuditEventModel:
    """Look up a single audit event by id (``404`` when absent).

    ``query_audit`` has no point lookup, so this scans the workspace's audit
    pages (cursor-driven) until the id is found or the cursor is exhausted.
    """
    ensure_workspace(ctx, workspace_id)
    workspace = WorkspaceId(workspace_id)
    page_cursor: Cursor | None = None
    try:
        while True:
            page = await store.query_audit(workspace, None, page_cursor)
            for event in page.items:
                if event.event_id == event_id:
                    return AuditEventModel.from_domain(event)
            if page.next_cursor is None:
                break
            page_cursor = page.next_cursor
    except (QueryUnsupported, BackendUnavailable) as exc:
        raise _audit_unavailable() from exc
    raise HTTPException(status_code=404, detail=f"audit event not found: {event_id}")
