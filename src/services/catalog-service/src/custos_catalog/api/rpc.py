"""Internal RPC inbound surface (CS-IMPL-018).

The Catalog Service exposes a thin RPC sub-tree at ``/rpc/v1/`` that
the workflow service and other internal components call to resolve
catalog handles into full version rows. These routes are functionally
identical to the public REST gets, but live under a separate prefix
so the gateway can apply distinct rate-limits, auth policies, and
logging.

Routes:

* ``GET /rpc/v1/workflow-versions/{workflowVersionId}`` — full row.
* ``GET /rpc/v1/connector-types/{type}@{version}`` — resolve a ref.

Auth model:

  The internal gateway issues a context carrying ``catalog:rpc:read``
  (and the component's identity). That permission alone authorises
  **same-workspace** internal reads — i.e. the call-context's
  ``workspace_id`` must match the ``<workspaceId>`` embedded in the
  workflow-version id. Cross-workspace internal reads (the workflow
  runtime resolving a tenant workflow while running under a system
  workspace, the activity dispatcher fanning out across tenants)
  additionally require ``catalog:rpc:cross-workspace-read``. The
  permission catalog distinguishes these two grants so the gateway
  can issue least-privilege RPC contexts: a workflow runtime that
  only needs to resolve workflows inside its own workspace never
  receives the cross-workspace grant, and a principal that somehow
  acquires ``catalog:rpc:read`` cannot use the id-shaped path to
  escape its workspace.

  Connector-type refs are global (not workspace-scoped), so
  ``rpc_resolve_connector_type`` only requires ``catalog:rpc:read``.
"""

from __future__ import annotations

import re

from fastapi import APIRouter, Depends, HTTPException, Path

from custos_catalog.api.dependencies import (
    get_connector_registry,
    get_definition_manager,
    require_permission_only,
)
from custos_catalog.api.models import (
    ConnectorTypeVersionBody,
    WorkflowVersionBody,
)
from custos_catalog.managers.connector_registry import ConnectorTypeRegistry
from custos_catalog.managers.definition import DefinitionManager
from custos_catalog.middleware.callctx import CallContext, CallContextError

router = APIRouter()

_WORKFLOW_ID_RE = re.compile(r"^(?P<ws>[^/]+)/(?P<name>[^@/]+)@(?P<version>[0-9]+)$")
_CT_REF_RE = re.compile(r"^(?P<type>[^@/]+)@(?P<version>[^@/]+)$")

#: Permission required to read a workflow version belonging to a
#: workspace other than the call-context's own. Granted by the gateway
#: to internal services that legitimately fan out across tenants
#: (workflow runtime, activity dispatcher) and withheld from everyone
#: else — ``catalog:rpc:read`` alone is not enough.
CROSS_WORKSPACE_RPC_READ = "catalog:rpc:cross-workspace-read"


@router.get(
    "/rpc/v1/workflow-versions/{workflow_version_id:path}",
    response_model=WorkflowVersionBody,
    tags=["rpc"],
    summary="Fetch a workflow version by triple-encoded id (internal).",
)
async def rpc_get_workflow_version(
    workflow_version_id: str = Path(
        ...,
        description=(
            "Triple-encoded workflow version id of the form "
            "``<workspaceId>/<workflowName>@<version>``."
        ),
    ),
    ctx: CallContext = Depends(require_permission_only("catalog:rpc:read")),
    manager: DefinitionManager = Depends(get_definition_manager),
) -> WorkflowVersionBody:
    """Resolve a ``WorkflowVersionId`` to its full row.

    Implements design ``GetWorkflowVersion`` (component sequence
    diagrams "Run Workflow" and "Activity Inputs Resolution").

    Tenant boundary: ``catalog:rpc:read`` alone authorises
    **same-workspace** reads. If the workspace embedded in the id
    differs from ``ctx.workspace_id`` the caller must additionally
    carry :data:`CROSS_WORKSPACE_RPC_READ`; otherwise the handler
    rejects with the standard 403 ``catalog.workspace_mismatch``
    envelope. The cross-workspace grant is the gateway's explicit
    signal that a system principal (workflow runtime, activity
    dispatcher) is fanning out across tenants by design.
    """
    match = _WORKFLOW_ID_RE.match(workflow_version_id)
    if match is None:
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
        )
    workspace_id = match.group("ws")
    name = match.group("name")
    version = int(match.group("version"))
    # Tenant boundary enforcement: ``require_permission_only`` only
    # asserts ``catalog:rpc:read``; it does not constrain *which*
    # workspace the caller may read. Without this check, any internal
    # principal with rpc:read could craft an id pointing at another
    # workspace and cross the tenant boundary. Cross-workspace reads
    # require a separate explicit grant.
    if ctx.workspace_id != workspace_id and not ctx.has_permission(
        CROSS_WORKSPACE_RPC_READ,
    ):
        raise CallContextError(
            403,
            "catalog.workspace_mismatch",
            (
                f"call context workspace {ctx.workspace_id!r} does not match "
                f"workflow version id workspace {workspace_id!r}; "
                f"cross-workspace reads require {CROSS_WORKSPACE_RPC_READ!r}"
            ),
        )
    row = await manager.get_workflow_version_by_ref(
        workspace_id=workspace_id,
        workflow_name=name,
        version=version,
    )
    return WorkflowVersionBody(
        workspaceId=workspace_id,
        workflowName=name,
        version=int(row.version),
        document=dict(row.normalized_doc),
        derivedFromTemplateVersionId=row.derived_from_template_version_id,
        parentDeprecated=bool(row.parent_deprecated),
        publishedAt=row.published_at,
    )


@router.get(
    "/rpc/v1/connector-types/{ref}",
    response_model=ConnectorTypeVersionBody,
    tags=["rpc"],
    summary="Resolve a connector-type reference (internal).",
)
async def rpc_resolve_connector_type(
    ref: str = Path(..., description="Connector type ref of the form <type>@<version>."),
    _ctx: CallContext = Depends(require_permission_only("catalog:rpc:read")),
    registry: ConnectorTypeRegistry = Depends(get_connector_registry),
) -> ConnectorTypeVersionBody:
    """Resolve a connector-type ref to its full row.

    Implements design ``ResolveConnectorTypeRef`` (used by the runtime
    activity dispatcher when it needs the manifest body — driver
    image, capability map, secret schema — to launch an activity).
    """
    match = _CT_REF_RE.match(ref)
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
    type_ = match.group("type")
    version = match.group("version")
    row = await registry.get(type=type_, version=version)
    return ConnectorTypeVersionBody(
        type=row.type,
        version=row.version,
        digest=row.digest,
        normalizedManifest=dict(row.normalized_manifest),
        parentDeprecated=bool(row.parent_deprecated),
        publishedAt=row.published_at,
    )


__all__ = ["CROSS_WORKSPACE_RPC_READ", "router"]
