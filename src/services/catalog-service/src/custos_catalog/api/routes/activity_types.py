"""Activity-type REST routes (CS-IMPL-017).

Per design § Public Interface:

* POST   ``/v1/workspaces/{ws}/activity-types`` — register.
* GET    ``/v1/workspaces/{ws}/activity-types?namespace=…&type=…`` — list.
* GET    ``/v1/workspaces/{ws}/activity-types/{namespace}/{type}@{version}``.
* POST   ``/v1/workspaces/{ws}/activity-types/{namespace}/{type}@{version}:deprecate``.

The SPL list surface is keyed on the full ``(namespace, type)`` pair —
a workspace-wide fan-out (list every activity type under every
namespace visible to ``ws``) is the gateway's job and is intentionally
not implemented here. The ``namespace`` and ``type`` query params are
therefore required on the list endpoint.
"""

from __future__ import annotations

import re

from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query

from custos_catalog.api.dependencies import (
    get_activity_registry,
    require_workspace_access,
)
from custos_catalog.api.models import (
    ActivityTypeListResponse,
    ActivityTypeRefBody,
    ActivityTypeRegisterRequest,
    ActivityTypeVersionBody,
    DeprecateRequest,
    StatusResponse,
)
from custos_catalog.managers.activity_registry import ActivityTypeRegistry
from custos_catalog.middleware.callctx import CallContext

router = APIRouter()

# ``<namespace>/<type>@<version>`` — namespace tokens may include dots,
# slashes are reserved as the namespace/type boundary.
_NS_TYPE_REF_RE = re.compile(r"^(?P<namespace>[^/]+)/(?P<type>[^@/]+)@(?P<version>[^/@]+)$")


def _parse_ns_type_ref(ref: str) -> tuple[str, str, str]:
    """Parse ``"<namespace>/<type>@<version>"`` into a triple."""
    match = _NS_TYPE_REF_RE.match(ref)
    if match is None:
        raise HTTPException(
            status_code=400,
            detail={
                "error": {
                    "code": "catalog.activity_type_ref_invalid",
                    "detail": (
                        f"activity type ref {ref!r} is not of the form <namespace>/<type>@<version>"
                    ),
                },
            },
        )
    return match.group("namespace"), match.group("type"), match.group("version")


def _serialize(row: object) -> ActivityTypeVersionBody:
    return ActivityTypeVersionBody(
        namespace=getattr(row, "namespace"),  # noqa: B009
        type=getattr(row, "type"),  # noqa: B009
        version=getattr(row, "version"),  # noqa: B009
        digest=getattr(row, "digest"),  # noqa: B009
        normalizedManifest=dict(getattr(row, "normalized_manifest")),  # noqa: B009
        parentDeprecated=bool(getattr(row, "parent_deprecated", False)),
        publishedAt=getattr(row, "published_at"),  # noqa: B009
    )


def _serialize_ref(row: object) -> ActivityTypeRefBody:
    """Project an SPL row down to its ref tuple for list responses.

    The list endpoint contract is ``[ActivityTypeRef]`` (refs only) per
    design § Public Interface; callers fetch the full normalized
    manifest via the get-by-ref endpoint.
    """
    return ActivityTypeRefBody(
        namespace=getattr(row, "namespace"),  # noqa: B009
        type=getattr(row, "type"),  # noqa: B009
        version=getattr(row, "version"),  # noqa: B009
        digest=getattr(row, "digest"),  # noqa: B009
    )


# ---------------------------------------------------------------------------
# Register
# ---------------------------------------------------------------------------


@router.post(
    "/v1/workspaces/{ws}/activity-types",
    response_model=ActivityTypeRefBody,
    status_code=201,
    tags=["activity-types"],
    summary="Register an activity type version.",
)
async def register_activity_type(
    ws: str = Path(...),
    body: ActivityTypeRegisterRequest = Body(...),
    ctx: CallContext = Depends(require_workspace_access("catalog:activity-types:write")),
    registry: ActivityTypeRegistry = Depends(get_activity_registry),
) -> ActivityTypeRefBody:
    ref = await registry.register(
        workspace_id=ws,
        principal_id=ctx.principal_id,
        manifest=body.manifest,
        referrer_ref=body.referrer_ref,
    )
    return ActivityTypeRefBody(
        namespace=ref.namespace,
        type=ref.type,
        version=ref.version,
        digest=ref.digest,
    )


# ---------------------------------------------------------------------------
# List
# ---------------------------------------------------------------------------


@router.get(
    "/v1/workspaces/{ws}/activity-types",
    response_model=ActivityTypeListResponse,
    tags=["activity-types"],
    summary="List activity-type versions for a (namespace, type) pair.",
)
async def list_activity_type_versions(
    ws: str = Path(...),
    namespace: str = Query(
        ...,
        description="Activity type namespace (full token, no short forms).",
        min_length=1,
    ),
    type: str = Query(..., description="Activity type token.", min_length=1),
    cursor: str | None = Query(default=None),
    limit: int | None = Query(default=None, ge=1, le=1000),
    _ctx: CallContext = Depends(require_workspace_access("catalog:activity-types:read")),
    registry: ActivityTypeRegistry = Depends(get_activity_registry),
) -> ActivityTypeListResponse:
    from custos_spl.pagination import Cursor

    page = await registry.list(
        namespace=namespace,
        type=type,
        cursor=Cursor(token=cursor) if cursor is not None else None,
        limit=limit,
    )
    return ActivityTypeListResponse(
        items=[_serialize_ref(row) for row in page.items],
        nextCursor=page.next_cursor.token if page.next_cursor else None,
    )


# ---------------------------------------------------------------------------
# Get
# ---------------------------------------------------------------------------


@router.get(
    "/v1/workspaces/{ws}/activity-types/{ref:path}",
    response_model=ActivityTypeVersionBody,
    tags=["activity-types"],
    summary="Fetch a specific activity-type version.",
)
async def get_activity_type_version(
    ws: str = Path(...),
    ref: str = Path(
        ...,
        description="Activity type ref of the form <namespace>/<type>@<version>.",
    ),
    _ctx: CallContext = Depends(require_workspace_access("catalog:activity-types:read")),
    registry: ActivityTypeRegistry = Depends(get_activity_registry),
) -> ActivityTypeVersionBody:
    # Strip a trailing :deprecate suffix so this route handles only
    # the plain GET; the deprecate route handles the action.
    if ref.endswith(":deprecate"):
        raise HTTPException(
            status_code=405,
            detail={
                "error": {
                    "code": "catalog.method_not_allowed",
                    "detail": "use POST for :deprecate",
                },
            },
        )
    namespace, type_, version = _parse_ns_type_ref(ref)
    row = await registry.get(namespace=namespace, type=type_, version=version)
    return _serialize(row)


# ---------------------------------------------------------------------------
# Deprecate
# ---------------------------------------------------------------------------


@router.post(
    "/v1/workspaces/{ws}/activity-types/{ref:path}:deprecate",
    response_model=StatusResponse,
    tags=["activity-types"],
    summary="Deprecate an activity-type (parent-row flag).",
)
async def deprecate_activity_type(
    ws: str = Path(...),
    ref: str = Path(...),
    body: DeprecateRequest = Body(default_factory=DeprecateRequest),
    ctx: CallContext = Depends(require_workspace_access("catalog:activity-types:write")),
    registry: ActivityTypeRegistry = Depends(get_activity_registry),
) -> StatusResponse:
    namespace, type_, _version = _parse_ns_type_ref(ref)
    # Version-level deprecation does not exist in v1 — the parent row
    # is what gets flagged. The ``@<version>`` segment of the ref is
    # parsed-and-discarded so the URL grammar matches the design.
    await registry.deprecate(
        workspace_id=ws,
        principal_id=ctx.principal_id,
        namespace=namespace,
        type=type_,
        reason=body.reason,
    )
    return StatusResponse()


__all__ = ["router"]
