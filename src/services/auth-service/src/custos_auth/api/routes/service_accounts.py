"""Service-account creation endpoint (AS-IMPL-006, GH-#241).

* ``POST /v1/service-accounts`` — admin endpoint, requires
  ``admin:service-account``. The service account is created in the
  caller's *current workspace* (pulled from the call context); the
  ``principal_id`` is operator-supplied so that downstream identity
  records can be wired with a stable handle.

Token minting (``ServiceToken``) is **not** part of this endpoint —
Phase F (AS-IMPL-013 / AS-IMPL-014) ships the token mint endpoint that
creates a credential against an existing service-account principal.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated

from custos_spl import AuthStoreProvider, MetadataStoreProvider
from custos_spl.ids import PrincipalId, WorkspaceId
from custos_spl.interfaces.auth_store import ServiceAccount
from fastapi import APIRouter, Depends, status

from custos_auth import _telemetry as telemetry
from custos_auth.api.dependencies import (
    get_auth_store,
    get_metadata_store,
    require_permission,
)
from custos_auth.api.errors import Conflict, ValidationFailure
from custos_auth.api.models import (
    ServiceAccountCreateRequest,
    ServiceAccountResponse,
    principal_to_response,
)
from custos_auth.audit import audit_principal_created
from custos_auth.middleware.callctx import CallContext

router = APIRouter(prefix="/v1", tags=["service-accounts"])


@router.post(
    "/service-accounts",
    status_code=status.HTTP_201_CREATED,
    response_model=ServiceAccountResponse,
)
async def create_service_account(
    body: ServiceAccountCreateRequest,
    ctx: Annotated[
        CallContext,
        Depends(require_permission("admin:service-account")),
    ],
    auth_store: Annotated[AuthStoreProvider, Depends(get_auth_store)],
    metadata_store: Annotated[MetadataStoreProvider, Depends(get_metadata_store)],
) -> ServiceAccountResponse:
    """Create a service account inside the caller's workspace.

    Emits ``principal.created`` keyed to the workspace.
    """
    with telemetry.observe_operation(
        telemetry.OP_SERVICE_ACCOUNT_CREATE,
        outcomes={
            ValidationFailure: "validation_failed",
            Conflict: "conflict",
        },
    ):
        if ctx.workspace_id is None:
            raise ValidationFailure(
                "service-account creation requires a workspace-scoped call context"
            )

        workspace_id = WorkspaceId(ctx.workspace_id)
        principal_id = PrincipalId(body.principal_id)
        existing = await auth_store.get_principal(principal_id)
        if existing is not None:
            raise Conflict(f"principal '{body.principal_id}' already exists")

        sa = ServiceAccount(
            kind="serviceAccount",
            principal_id=principal_id,
            workspace_id=workspace_id,
            display_name=body.display_name,
            disabled_at=None,
            disabled_reason=None,
            created_at=datetime.now(UTC),
        )
        await auth_store.put_principal(sa)
        await audit_principal_created(
            metadata_store,
            actor=ctx.principal_id,
            workspace_id=ctx.workspace_id,
            principal_id=body.principal_id,
            kind="serviceAccount",
            display_name=body.display_name,
        )
        response = principal_to_response(sa)
        # Narrow to the right union arm for the type annotation.
        assert isinstance(response, ServiceAccountResponse)
        return response


__all__ = ["router"]
