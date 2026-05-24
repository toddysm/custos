"""Principal introspection + admin endpoints (AS-IMPL-006, GH-#241).

* ``GET  /v1/principals/me`` — authenticated; returns the caller's own
  principal record. Useful for the gateway to render "who am I" and
  for clients to discover their permissions.
* ``POST /v1/principals/{principal_id}/disable`` — admin endpoint;
  soft-disables a principal and emits ``principal.disabled``.

Service-account *creation* lives in
:mod:`custos_auth.api.routes.service_accounts` to keep the resource
layout consistent (POST creates land under the resource type, not
under ``/principals``).
"""

from __future__ import annotations

from typing import Annotated

from custos_spl import AuthStoreProvider, MetadataStoreProvider
from custos_spl.ids import PrincipalId
from fastapi import APIRouter, Depends, status

from custos_auth.api.dependencies import (
    get_auth_store,
    get_call_context,
    get_metadata_store,
    require_permission,
)
from custos_auth.api.errors import NotFound
from custos_auth.api.models import (
    PrincipalDisableRequest,
    PrincipalResponse,
    principal_to_response,
)
from custos_auth.audit import (
    PLATFORM_WORKSPACE_ID,
    audit_principal_disabled,
)
from custos_auth.middleware.callctx import CallContext

router = APIRouter(prefix="/v1", tags=["principals"])


@router.get(
    "/principals/me",
    response_model=PrincipalResponse,
)
async def get_me(
    ctx: Annotated[CallContext, Depends(get_call_context)],
    auth_store: Annotated[AuthStoreProvider, Depends(get_auth_store)],
) -> PrincipalResponse:
    """Return the call-context's principal.

    A valid call-context already implies authentication, so any 4xx
    here means the call-context principal cannot be resolved in the
    auth store — most often "principal was disabled between bearer-
    token mint and this call". We surface it as 404 to keep the
    contract consistent with the other endpoints.
    """
    principal = await auth_store.get_principal(PrincipalId(ctx.principal_id))
    if principal is None:
        raise NotFound(f"principal '{ctx.principal_id}' not found")
    return principal_to_response(principal)


@router.post(
    "/principals/{principal_id}/disable",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def disable_principal(
    principal_id: str,
    body: PrincipalDisableRequest,
    ctx: Annotated[
        CallContext,
        Depends(require_permission("platform.admin", "tenant.admin")),
    ],
    auth_store: Annotated[AuthStoreProvider, Depends(get_auth_store)],
    metadata_store: Annotated[MetadataStoreProvider, Depends(get_metadata_store)],
) -> None:
    """Soft-disable a principal.

    Returns 204 on success. Existence-hiding semantics apply:
    tenant-admin callers attempting to disable a principal outside
    their tenant get 404 even though the row exists.

    Emits ``principal.disabled`` keyed to the principal's workspace
    (for service accounts) or to the platform sentinel workspace (for
    users — they live at tenant scope, which is not a valid SPL
    audit-row key).
    """
    target = await auth_store.get_principal(PrincipalId(principal_id))
    if target is None:
        raise NotFound(f"principal '{principal_id}' not found")

    # Scope check
    if not ctx.has_permission("platform.admin"):
        # tenant.admin path
        if target.kind == "user":
            if ctx.tenant_id != str(target.tenant_id):
                raise NotFound(f"principal '{principal_id}' not found")
        else:  # serviceAccount
            # Service accounts are workspace-scoped; tenant-admin needs
            # the SA's workspace tenant to match ctx.tenant_id. Phase C
            # MVP: require the call-context's workspace_id to match.
            if ctx.workspace_id != str(target.workspace_id):
                raise NotFound(f"principal '{principal_id}' not found")

    await auth_store.disable_principal(
        PrincipalId(principal_id),
        PrincipalId(ctx.principal_id),
        body.reason,
    )

    audit_workspace_id = (
        str(target.workspace_id) if target.kind == "serviceAccount" else PLATFORM_WORKSPACE_ID
    )
    await audit_principal_disabled(
        metadata_store,
        actor=ctx.principal_id,
        workspace_id=audit_workspace_id,
        principal_id=principal_id,
        reason=body.reason,
    )


__all__ = ["router"]
