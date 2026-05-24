"""Tenant + workspace creation endpoints (AS-IMPL-005, GH-#240).

Three routes:

* ``POST /v1/tenants`` — platform admin only (``platform.admin``).
* ``GET  /v1/tenants`` — platform admin or tenant admin
  (``platform.admin`` OR ``tenant.admin``). Tenant-admin callers see
  only their own tenant.
* ``POST /v1/tenants/{tenant_id}/workspaces`` — tenant admin or
  platform admin (``tenant.admin`` OR ``platform.admin``).

All mutating routes emit the canonical audit events
(``tenant.created`` / ``workspace.created``) through
:mod:`custos_auth.audit` after the SPL write commits. Audit emission is
best-effort post-commit; see that module's "Atomicity" note.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated

from custos_spl import AuthStoreProvider, MetadataStoreProvider
from custos_spl.errors import ImmutableViolation
from custos_spl.ids import TenantId, WorkspaceId
from custos_spl.interfaces.auth_store import (
    Tenant,
    TenantFilter,
    Workspace,
)
from fastapi import APIRouter, Depends, status

from custos_auth.api.dependencies import (
    get_auth_store,
    get_metadata_store,
    require_permission,
)
from custos_auth.api.errors import Conflict, NotFound, ValidationFailure
from custos_auth.api.models import (
    TenantCreateRequest,
    TenantListResponse,
    TenantResponse,
    WorkspaceCreateRequest,
    WorkspaceResponse,
    tenant_to_response,
    workspace_to_response,
)
from custos_auth.audit import audit_tenant_created, audit_workspace_created
from custos_auth.middleware.callctx import CallContext

router = APIRouter(prefix="/v1", tags=["tenants"])


@router.post(
    "/tenants",
    status_code=status.HTTP_201_CREATED,
    response_model=TenantResponse,
)
async def create_tenant(
    body: TenantCreateRequest,
    ctx: Annotated[CallContext, Depends(require_permission("platform.admin"))],
    auth_store: Annotated[AuthStoreProvider, Depends(get_auth_store)],
    metadata_store: Annotated[MetadataStoreProvider, Depends(get_metadata_store)],
) -> TenantResponse:
    """Create a tenant.

    Only ``platform.admin`` can call this — tenants are the platform's
    top-level isolation boundary.

    Emits ``tenant.created`` against the platform sentinel workspace
    after the SPL write commits.
    """
    tenant_id = TenantId(body.tenant_id)
    existing = await auth_store.get_tenant(tenant_id)
    if existing is not None:
        raise Conflict(f"tenant '{body.tenant_id}' already exists")
    tenant = Tenant(
        tenant_id=tenant_id,
        display_name=body.display_name,
        disabled_at=None,
        created_at=datetime.now(UTC),
    )
    await auth_store.put_tenant(tenant)
    await audit_tenant_created(
        metadata_store,
        actor=ctx.principal_id,
        tenant_id=str(tenant_id),
        name=body.display_name,
    )
    return tenant_to_response(tenant)


@router.get(
    "/tenants",
    response_model=TenantListResponse,
)
async def list_tenants(
    ctx: Annotated[
        CallContext,
        Depends(require_permission("platform.admin", "tenant.admin")),
    ],
    auth_store: Annotated[AuthStoreProvider, Depends(get_auth_store)],
) -> TenantListResponse:
    """List tenants.

    * ``platform.admin`` sees all tenants.
    * ``tenant.admin`` sees only the tenant attached to their call
      context (``ctx.tenant_id``). A tenant-admin caller without a
      tenant scope receives an empty list rather than a 4xx — the
      design preference is to never leak existence on cross-tenant
      reads.
    """
    if ctx.has_permission("platform.admin"):
        tenants = await auth_store.list_tenants(TenantFilter(include_disabled=True))
    else:
        # tenant.admin path
        if ctx.tenant_id is None:
            return TenantListResponse(tenants=[])
        tenant = await auth_store.get_tenant(TenantId(ctx.tenant_id))
        tenants = (tenant,) if tenant is not None else ()
    return TenantListResponse(tenants=[tenant_to_response(t) for t in tenants])


@router.post(
    "/tenants/{tenant_id}/workspaces",
    status_code=status.HTTP_201_CREATED,
    response_model=WorkspaceResponse,
)
async def create_workspace(
    tenant_id: str,
    body: WorkspaceCreateRequest,
    ctx: Annotated[
        CallContext,
        Depends(require_permission("platform.admin", "tenant.admin")),
    ],
    auth_store: Annotated[AuthStoreProvider, Depends(get_auth_store)],
    metadata_store: Annotated[MetadataStoreProvider, Depends(get_metadata_store)],
) -> WorkspaceResponse:
    """Create a workspace inside ``tenant_id``.

    * ``platform.admin`` may target any tenant.
    * ``tenant.admin`` is restricted to their own tenant; targeting a
      different tenant returns 404 (existence-hiding semantics).
    """
    tenant_typed = TenantId(tenant_id)
    tenant = await auth_store.get_tenant(tenant_typed)
    if tenant is None:
        raise NotFound(f"tenant '{tenant_id}' not found")
    if tenant.disabled_at is not None:
        raise ValidationFailure(
            f"tenant '{tenant_id}' is disabled and cannot accept new workspaces"
        )
    # Tenant-admin scope check (platform.admin bypasses).
    if not ctx.has_permission("platform.admin") and ctx.tenant_id != tenant_id:
        # Hide existence rather than leak a 403.
        raise NotFound(f"tenant '{tenant_id}' not found")

    workspace_id = WorkspaceId(body.workspace_id)
    existing = await auth_store.get_workspace(workspace_id)
    if existing is not None:
        raise Conflict(f"workspace '{body.workspace_id}' already exists")

    workspace = Workspace(
        workspace_id=workspace_id,
        tenant_id=tenant_typed,
        display_name=body.display_name,
        disabled_at=None,
        created_at=datetime.now(UTC),
    )
    try:
        await auth_store.put_workspace(workspace)
    except ImmutableViolation as exc:  # pragma: no cover - defensive
        raise Conflict(str(exc)) from exc

    await audit_workspace_created(
        metadata_store,
        actor=ctx.principal_id,
        tenant_id=tenant_id,
        workspace_id=body.workspace_id,
        name=body.display_name,
    )
    return workspace_to_response(workspace)


__all__ = ["router"]
