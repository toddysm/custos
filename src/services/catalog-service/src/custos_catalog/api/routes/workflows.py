"""Workflow REST routes (CS-IMPL-017).

Per design § Public Interface, the workflow surface is six routes:

* POST   ``/v1/workspaces/{ws}/workflows`` — publish.
* GET    ``/v1/workspaces/{ws}/workflows/{name}`` — list versions.
* GET    ``/v1/workspaces/{ws}/workflows/{name}@{version}`` — get-by-ref.
* GET    ``/v1/workflows/{workflowVersionId}`` — get-by-id (workspace-less).
* POST   ``/v1/workspaces/{ws}/workflows/{name}@{version}:deprecate``.
* POST   ``/v1/workspaces/{ws}/workflows/{name}@{version}:extractTemplate``.

The ``{name}@{version}`` shape is passed through :func:`_parse_ref`,
which accepts the at-sign separator and validates the integer version.
"""

from __future__ import annotations

import re

from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query

from custos_catalog.api.dependencies import (
    get_definition_manager,
    get_template_manager,
    require_permission_only,
    require_workspace_access,
)
from custos_catalog.api.models import (
    DefinitionPublishRequest,
    DeprecateRequest,
    ExtractTemplateRequest,
    StatusResponse,
    WorkflowListResponse,
    WorkflowTemplateVersionRefBody,
    WorkflowVersionBody,
    WorkflowVersionRefBody,
)
from custos_catalog.extract import Selector
from custos_catalog.managers.definition import DefinitionManager
from custos_catalog.managers.template import TemplateManager
from custos_catalog.middleware.callctx import CallContext, CallContextError

router = APIRouter()

_REF_RE = re.compile(r"^(?P<name>[^@/]+)@(?P<version>[0-9]+)$")


def _parse_ref(ref: str) -> tuple[str, int]:
    """Parse ``"<name>@<version>"`` into ``(name, int(version))``.

    Raises :class:`HTTPException` 400 on a malformed ref so the caller
    sees the same envelope as every other 4xx.
    """
    match = _REF_RE.match(ref)
    if match is None:
        raise HTTPException(
            status_code=400,
            detail={
                "error": {
                    "code": "catalog.workflow_ref_invalid",
                    "detail": f"workflow ref {ref!r} is not of the form <name>@<version>",
                },
            },
        )
    return match.group("name"), int(match.group("version"))


def _serialize_workflow_version(
    row: object, *, workspace_id: str, workflow_name: str
) -> WorkflowVersionBody:
    """Translate an SPL ``WorkflowVersion`` row to its wire body."""
    return WorkflowVersionBody(
        workspaceId=workspace_id,
        workflowName=workflow_name,
        version=int(getattr(row, "version")),  # noqa: B009
        document=dict(getattr(row, "normalized_doc")),  # noqa: B009
        derivedFromTemplateVersionId=getattr(row, "derived_from_template_version_id", None),
        parentDeprecated=bool(getattr(row, "parent_deprecated", False)),
        publishedAt=getattr(row, "published_at"),  # noqa: B009
    )


# ---------------------------------------------------------------------------
# Publish
# ---------------------------------------------------------------------------


@router.post(
    "/v1/workspaces/{ws}/workflows",
    response_model=WorkflowVersionRefBody,
    status_code=201,
    tags=["workflows"],
    summary="Publish a new workflow version.",
)
async def publish_workflow(
    ws: str = Path(..., description="Workspace id."),
    body: DefinitionPublishRequest = Body(...),
    ctx: CallContext = Depends(require_workspace_access("catalog:workflows:write")),
    manager: DefinitionManager = Depends(get_definition_manager),
) -> WorkflowVersionRefBody:
    """Publish a workflow document and return the resulting version ref."""
    source = (
        body.definition if isinstance(body.definition, str) else _dump_yaml_or_json(body.definition)
    )
    ref = await manager.publish_workflow(
        workspace_id=ws,
        principal_id=ctx.principal_id,
        source=source,
    )
    return WorkflowVersionRefBody(
        workspaceId=ref.workspace_id,
        workflowName=ref.workflow_name,
        version=ref.version,
    )


def _dump_yaml_or_json(payload: dict[str, object]) -> str:
    """Re-encode a pre-parsed publish body as JSON for the parse stage.

    The publish pipeline accepts JSON or YAML as text; serialising the
    dict back to JSON is unambiguous and keeps the manager's parse
    contract intact. Yaml-vs-json choice has no semantic impact since
    the document is canonicalised inside the pipeline.
    """
    import json

    return json.dumps(payload)


# ---------------------------------------------------------------------------
# List versions / get-by-ref (single matcher, dispatched by ``@``)
# ---------------------------------------------------------------------------
#
# The design's REST surface has two GETs under the same prefix:
#
#   GET /v1/workspaces/{ws}/workflows/{name}            -> list versions
#   GET /v1/workspaces/{ws}/workflows/{name}@{version}  -> get one
#
# FastAPI's path matcher treats both shapes as ``/{ws}/workflows/{x}`` —
# distinct route declarations are not enough because the first one wins.
# We register a single handler and branch on the presence of ``@``.


@router.get(
    "/v1/workspaces/{ws}/workflows/{name_or_ref}",
    tags=["workflows"],
    summary="List versions of a workflow, or fetch one by name@version.",
    response_model=None,
)
async def list_or_get_workflow(
    ws: str = Path(...),
    name_or_ref: str = Path(..., min_length=1),
    cursor: str | None = Query(
        default=None,
        description="Opaque pagination token returned as ``nextCursor`` by a prior call.",
    ),
    limit: int | None = Query(
        default=None,
        ge=1,
        le=1000,
        description=(
            "Maximum number of versions to return (1-1000). Defaults to the"
            " SPL provider's page size when omitted."
        ),
    ),
    _ctx: CallContext = Depends(require_workspace_access("catalog:workflows:read")),
    manager: DefinitionManager = Depends(get_definition_manager),
) -> WorkflowListResponse | WorkflowVersionBody:
    """Dispatch by ``@``: present → get-by-ref; absent → list versions.

    Returning ``WorkflowListResponse | WorkflowVersionBody`` keeps each
    branch's response shape exactly as the design specifies; the two
    shapes are mutually exclusive and the OpenAPI schema for this route
    is the union.
    """
    if "@" in name_or_ref:
        name, version = _parse_ref(name_or_ref)
        row = await manager.get_workflow_version_by_ref(
            workspace_id=ws,
            workflow_name=name,
            version=version,
        )
        return _serialize_workflow_version(row, workspace_id=ws, workflow_name=name)

    from custos_spl.pagination import Cursor

    page = await manager.list_workflow_versions(
        workspace_id=ws,
        workflow_name=name_or_ref,
        cursor=Cursor(token=cursor) if cursor is not None else None,
        limit=limit,
    )
    return WorkflowListResponse(
        items=[
            _serialize_workflow_version(row, workspace_id=ws, workflow_name=name_or_ref)
            for row in page.items
        ],
        nextCursor=page.next_cursor.token if page.next_cursor else None,
    )


# ---------------------------------------------------------------------------
# Get-by-id (workspaceless)
# ---------------------------------------------------------------------------


@router.get(
    "/v1/workflows/{workflow_version_id:path}",
    response_model=WorkflowVersionBody,
    tags=["workflows"],
    summary="Fetch a workflow version by its triple-encoded id.",
)
async def get_workflow_version_by_id(
    workflow_version_id: str = Path(
        ...,
        description=(
            "Triple-encoded workflow version id of the form "
            "``<workspaceId>/<workflowName>@<version>``."
        ),
    ),
    ctx: CallContext = Depends(require_permission_only("catalog:workflows:read")),
    manager: DefinitionManager = Depends(get_definition_manager),
) -> WorkflowVersionBody:
    """Fetch by the triple-encoded id, the design's canonical handle.

    SPL has no UUID column at v1, so the id is the triple
    ``<workspaceId>/<workflowName>@<version>``. The path is decoded
    with the same regex used for ``<name>@<version>`` after splitting
    on the first slash.

    Tenant principals carrying ``catalog:workflows:read`` are scoped
    to their own workspace: this handler rejects requests whose
    parsed ``<workspaceId>`` differs from ``ctx.workspace_id`` with
    the standard 403 ``catalog.workspace_mismatch`` envelope. Internal
    callers that legitimately need cross-workspace reads (the workflow
    runtime, activity dispatcher) must use the ``catalog:rpc:read``-gated
    ``GET /rpc/v1/workflow-versions/{id}`` route instead.
    """
    try:
        workspace_id, name_at_version = workflow_version_id.split("/", 1)
    except ValueError as exc:  # pragma: no cover - guarded by Path matcher
        raise HTTPException(
            status_code=400,
            detail={
                "error": {
                    "code": "catalog.workflow_version_id_invalid",
                    "detail": (
                        f"workflow version id {workflow_version_id!r} must be of the form "
                        "<workspaceId>/<workflowName>@<version>"
                    ),
                },
            },
        ) from exc
    name, version = _parse_ref(name_at_version)
    # Tenant boundary enforcement: ``require_permission_only`` only
    # asserts that the caller carries ``catalog:workflows:read``; it
    # does not constrain *which* workspace they may read. Without this
    # check, a principal with read access in workspace A could fetch
    # any other workspace's workflows simply by crafting the id.
    if ctx.workspace_id != workspace_id:
        raise CallContextError(
            403,
            "catalog.workspace_mismatch",
            (
                f"call context workspace {ctx.workspace_id!r} does not match "
                f"workflow version id workspace {workspace_id!r}"
            ),
        )
    row = await manager.get_workflow_version_by_ref(
        workspace_id=workspace_id,
        workflow_name=name,
        version=version,
    )
    return _serialize_workflow_version(row, workspace_id=workspace_id, workflow_name=name)


# ---------------------------------------------------------------------------
# Deprecate
# ---------------------------------------------------------------------------


@router.post(
    "/v1/workspaces/{ws}/workflows/{ref}:deprecate",
    response_model=StatusResponse,
    tags=["workflows"],
    summary="Deprecate a workflow (flips the parent-row flag).",
)
async def deprecate_workflow(
    ws: str = Path(...),
    ref: str = Path(...),
    body: DeprecateRequest = Body(default_factory=DeprecateRequest),
    ctx: CallContext = Depends(require_workspace_access("catalog:workflows:write")),
    manager: DefinitionManager = Depends(get_definition_manager),
) -> StatusResponse:
    name, _version = _parse_ref(ref)
    # The manager surface only takes a workflow_name — there is no
    # version-level deprecation at v1 (REQ-025 immutability). The
    # ``@<version>`` segment of the ref is parsed-and-discarded to
    # match the design URL grammar.
    await manager.deprecate_workflow(
        workspace_id=ws,
        workflow_name=name,
        principal_id=ctx.principal_id,
        reason=body.reason,
    )
    return StatusResponse()


# ---------------------------------------------------------------------------
# Extract template from workflow
# ---------------------------------------------------------------------------


@router.post(
    "/v1/workspaces/{ws}/workflows/{ref}:extractTemplate",
    response_model=WorkflowTemplateVersionRefBody,
    status_code=201,
    tags=["workflows", "templates"],
    summary="Extract a template from a published workflow version.",
)
async def extract_template_from_workflow(
    ws: str = Path(...),
    ref: str = Path(
        ...,
        description=(
            "Source workflow version, expressed as ``<name>@<version>`` within"
            " the path's ``{ws}``. The workspace prefix is implicit; cross-workspace"
            " callers must look up the workflow via the workspaceless"
            " ``GET /v1/workflows/{workflow_version_id}`` route instead."
        ),
    ),
    body: ExtractTemplateRequest = Body(...),
    ctx: CallContext = Depends(require_workspace_access("catalog:workflows:write")),
    template_manager: TemplateManager = Depends(get_template_manager),
) -> WorkflowTemplateVersionRefBody:
    name, version = _parse_ref(ref)
    selectors = [
        Selector(
            path=s.path,
            placeholder_name=s.placeholder_name,
            placeholder_type=s.placeholder_type,
            required=s.required,
            default=s.default,
            connector_type=s.connector_type,
            activity_type=s.activity_type,
            description=s.description,
        )
        for s in body.selectors
    ]
    template_ref = await template_manager.extract_from_workflow(
        workspace_id=ws,
        principal_id=ctx.principal_id,
        source_workflow_name=name,
        source_workflow_version=version,
        selectors=selectors,
        template_name=body.template_name,
    )
    return WorkflowTemplateVersionRefBody(
        workspaceId=template_ref.workspace_id,
        templateName=template_ref.template_name,
        version=template_ref.version,
    )


__all__ = ["router"]
