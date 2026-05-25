"""Workspace list + read endpoints (AS-IMPL-005, GH-#240).

* ``GET /v1/workspaces`` — list workspaces visible to the caller.
* ``GET /v1/workspaces/{workspace_id}`` — single read with
  cross-tenant 404 collapsing.

Visibility rules:

* ``platform.admin`` sees every workspace.
* ``tenant.admin`` sees every workspace inside ``ctx.tenant_id``.
* Any other authenticated principal sees workspaces they have a role
  binding to. Phase C MVP returns only the workspace pinned in their
  call context (``ctx.workspace_id``) because the broader RBAC lookup
  is wired in Phase E (AS-IMPL-010 — authorize() + role-binding
  enumeration). Until then, this is the safe, conservative behavior.

Workspace creation lives in :mod:`custos_auth.api.routes.tenants`
(under the natural ``/v1/tenants/{id}/workspaces`` path).
"""

from __future__ import annotations

from typing import Annotated

from custos_spl import AuthStoreProvider
from custos_spl.ids import TenantId, WorkspaceId
from custos_spl.interfaces.auth_store import WorkspaceFilter
from fastapi import APIRouter, Depends

from custos_auth import _telemetry as telemetry
from custos_auth.api.dependencies import (
    get_auth_store,
    get_call_context,
)
from custos_auth.api.errors import NotFound
from custos_auth.api.models import (
    WorkspaceListResponse,
    WorkspaceResponse,
    workspace_to_response,
)
from custos_auth.middleware.callctx import CallContext

router = APIRouter(prefix="/v1", tags=["workspaces"])


@router.get(
    "/workspaces",
    response_model=WorkspaceListResponse,
)
async def list_workspaces(
    ctx: Annotated[CallContext, Depends(get_call_context)],
    auth_store: Annotated[AuthStoreProvider, Depends(get_auth_store)],
) -> WorkspaceListResponse:
    """List workspaces the caller can see.

    See module docstring for the visibility matrix.
    """
    with telemetry.observe_operation(telemetry.OP_WORKSPACE_LIST):
        if ctx.has_permission("platform.admin"):
            workspaces = await auth_store.list_workspaces(WorkspaceFilter(include_disabled=True))
        elif ctx.has_permission("tenant.admin"):
            if ctx.tenant_id is None:
                return WorkspaceListResponse(workspaces=[])
            workspaces = await auth_store.list_workspaces(
                WorkspaceFilter(
                    tenant_id=TenantId(ctx.tenant_id),
                    include_disabled=True,
                )
            )
        else:
            # Phase C MVP: only the current workspace, if any.
            if ctx.workspace_id is None:
                return WorkspaceListResponse(workspaces=[])
            single = await auth_store.get_workspace(WorkspaceId(ctx.workspace_id))
            workspaces = (single,) if single is not None else ()
        return WorkspaceListResponse(workspaces=[workspace_to_response(w) for w in workspaces])


@router.get(
    "/workspaces/{workspace_id}",
    response_model=WorkspaceResponse,
)
async def get_workspace(
    workspace_id: str,
    ctx: Annotated[CallContext, Depends(get_call_context)],
    auth_store: Annotated[AuthStoreProvider, Depends(get_auth_store)],
) -> WorkspaceResponse:
    """Read a workspace by id.

    Returns 404 for both "doesn't exist" and "you can't see it"
    (existence-hiding); the only 2xx path is the caller being
    platform-admin, tenant-admin in the matching tenant, or holding a
    binding inside the workspace (Phase C MVP: pinned in
    ``ctx.workspace_id``).
    """
    with telemetry.observe_operation(
        telemetry.OP_WORKSPACE_GET,
        outcomes={NotFound: "not_found"},
    ):
        workspace = await auth_store.get_workspace(WorkspaceId(workspace_id))
        if workspace is None:
            raise NotFound(f"workspace '{workspace_id}' not found")

        if ctx.has_permission("platform.admin"):
            return workspace_to_response(workspace)
        if ctx.has_permission("tenant.admin") and ctx.tenant_id == str(workspace.tenant_id):
            return workspace_to_response(workspace)
        if ctx.workspace_id == workspace_id:
            return workspace_to_response(workspace)
        # Cross-tenant or cross-workspace read → collapse to 404.
        raise NotFound(f"workspace '{workspace_id}' not found")


__all__ = ["router"]
