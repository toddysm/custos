"""Template REST routes (CS-IMPL-017).

Per design § Public Interface:

* POST   ``/v1/workspaces/{ws}/templates`` — publish a template.
* GET    ``/v1/workspaces/{ws}/templates/{name}@{version}`` — get-by-ref.
* POST   ``/v1/workspaces/{ws}/templates/{name}@{version}:materialize``.

There is no template-level deprecate route in the design's table; the
template manager exposes a deprecate method but the gateway does not
surface it in M1.
"""

from __future__ import annotations

from fastapi import APIRouter, Body, Depends, Path

from custos_catalog.api.dependencies import (
    get_template_manager,
    require_workspace_access,
)
from custos_catalog.api.models import (
    DefinitionPublishRequest,
    MaterializeRequest,
    WorkflowTemplateVersionBody,
    WorkflowTemplateVersionRefBody,
    WorkflowVersionRefBody,
)
from custos_catalog.api.routes.workflows import _dump_yaml_or_json, _parse_ref
from custos_catalog.managers.template import TemplateManager
from custos_catalog.middleware.callctx import CallContext

router = APIRouter()


def _serialize_template_version(
    row: object, *, workspace_id: str, template_name: str
) -> WorkflowTemplateVersionBody:
    return WorkflowTemplateVersionBody(
        workspaceId=workspace_id,
        templateName=template_name,
        version=int(getattr(row, "version")),  # noqa: B009
        document=dict(getattr(row, "normalized_doc")),  # noqa: B009
        derivedFromWorkflowVersionId=getattr(row, "derived_from_workflow_version_id", None),
        parentDeprecated=bool(getattr(row, "parent_deprecated", False)),
        publishedAt=getattr(row, "published_at"),  # noqa: B009
    )


# ---------------------------------------------------------------------------
# Publish
# ---------------------------------------------------------------------------


@router.post(
    "/v1/workspaces/{ws}/templates",
    response_model=WorkflowTemplateVersionRefBody,
    status_code=201,
    tags=["templates"],
    summary="Publish a new template version.",
)
async def publish_template(
    ws: str = Path(...),
    body: DefinitionPublishRequest = Body(...),
    ctx: CallContext = Depends(require_workspace_access("catalog:templates:write")),
    manager: TemplateManager = Depends(get_template_manager),
) -> WorkflowTemplateVersionRefBody:
    source = (
        body.definition if isinstance(body.definition, str) else _dump_yaml_or_json(body.definition)
    )
    ref = await manager.publish_template(
        workspace_id=ws,
        principal_id=ctx.principal_id,
        source=source,
    )
    return WorkflowTemplateVersionRefBody(
        workspaceId=ref.workspace_id,
        templateName=ref.template_name,
        version=ref.version,
    )


# ---------------------------------------------------------------------------
# Get by ref
# ---------------------------------------------------------------------------


@router.get(
    "/v1/workspaces/{ws}/templates/{ref}",
    response_model=WorkflowTemplateVersionBody,
    tags=["templates"],
    summary="Fetch a template version by name@version.",
)
async def get_template_version_by_ref(
    ws: str = Path(...),
    ref: str = Path(..., description="Template ref of the form <name>@<version>."),
    _ctx: CallContext = Depends(require_workspace_access("catalog:templates:read")),
    manager: TemplateManager = Depends(get_template_manager),
) -> WorkflowTemplateVersionBody:
    name, version = _parse_ref(ref)
    row = await manager.get_template_version_by_ref(
        workspace_id=ws,
        template_name=name,
        version=version,
    )
    return _serialize_template_version(row, workspace_id=ws, template_name=name)


# ---------------------------------------------------------------------------
# Materialize
# ---------------------------------------------------------------------------


@router.post(
    "/v1/workspaces/{ws}/templates/{ref}:materialize",
    response_model=WorkflowVersionRefBody,
    status_code=201,
    tags=["templates", "workflows"],
    summary="Materialize a template into a new workflow version.",
)
async def materialize_template(
    ws: str = Path(...),
    ref: str = Path(
        ...,
        description=(
            "Source template version, expressed as ``<name>@<version>`` within"
            " the path's ``{ws}``. The workspace prefix is implicit; templates"
            " cannot be materialized across workspaces."
        ),
    ),
    body: MaterializeRequest = Body(...),
    ctx: CallContext = Depends(require_workspace_access("catalog:templates:write")),
    manager: TemplateManager = Depends(get_template_manager),
) -> WorkflowVersionRefBody:
    name, version = _parse_ref(ref)
    workflow_ref = await manager.materialize(
        workspace_id=ws,
        template_name=name,
        template_version=version,
        target_workflow_name=body.target_name,
        bindings=body.bindings,
        principal_id=ctx.principal_id,
    )
    return WorkflowVersionRefBody(
        workspaceId=workflow_ref.workspace_id,
        workflowName=workflow_ref.workflow_name,
        version=workflow_ref.version,
    )


__all__ = ["router"]
