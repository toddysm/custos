"""Credential-backed, operation-scoped context exchange for background execution.

Unlike the legacy signing RPC, the request cannot assert an arbitrary grant,
audience, caller or lifetime. The account credential and current authorization
are checked before the existing Auth-owned signer is invoked.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Annotated
from uuid import uuid4

from custos_callctx.execution import EXECUTION_PERMISSIONS, ExecutionOperation
from custos_callctx.mesh import MeshAuthenticationError, require_service_caller
from custos_spl import AuthStoreProvider, MetadataStoreProvider
from custos_spl.errors import SPLError
from custos_spl.ids import WorkspaceId
from custos_spl.interfaces.auth_store import ServiceAccount
from custos_spl.interfaces.metadata_store import AuditEvent
from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, ConfigDict, Field, SecretStr

from custos_auth.api.dependencies import (
    get_auth_store,
    get_authn_cache,
    get_authz_cache,
    get_metadata_store,
    get_settings,
)
from custos_auth.api.errors import AuthApiError, Unauthenticated
from custos_auth.api.routes.rpc import CallctxSignRpcResponse, get_call_context_signer
from custos_auth.authn import verify_token
from custos_auth.authn_cache import AuthnCache
from custos_auth.authorize import authorize
from custos_auth.authz_cache import AuthzDecisionCache
from custos_auth.callctx_signer import (
    DEFAULT_AUDIENCE,
    DEFAULT_TTL_SECONDS,
    CallContextSigner,
    DaprSecretsResolutionError,
)
from custos_auth.middleware.callctx import CallContextError
from custos_auth.settings import Settings

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/rpc", tags=["execution"])


class ExecutionDenied(AuthApiError):
    """The account or operation is not authorized, without exposing credential state."""

    status_code = 403
    code = "execution_denied"


class ExecutionUnavailable(AuthApiError):
    """Execution context issuance is disabled or its dependencies are unavailable."""

    status_code = 503
    code = "execution_unavailable"


class ExchangeExecutionRequest(BaseModel):
    """Private credential proof plus the expected account/scope and one known operation."""

    model_config = ConfigDict(extra="forbid", strict=True)
    token: SecretStr = Field(min_length=1, max_length=4096, repr=False)
    workspace_id: str = Field(alias="workspaceId", min_length=1, max_length=120)
    principal_id: str = Field(alias="principalId", min_length=1, max_length=120)
    operation: ExecutionOperation


def execution_caller(request: Request, settings: Settings, operation: ExecutionOperation) -> str:
    """Authenticate the local sidecar and the operation's permitted app/namespace caller."""
    if not settings.execution_context_enabled:
        raise ExecutionUnavailable("execution context exchange is not enabled")
    caller = (
        settings.execution_trigger_app_id
        if operation in ("workflow-start", "workflow-resume")
        else settings.execution_workflow_app_id
    )
    if operation == "connector-credentials" or (
        operation == "catalog-activity-read"
        and request.headers.get("dapr-caller-app-id") == settings.execution_runtime_app_id
    ):
        caller = settings.execution_runtime_app_id
    try:
        require_service_caller(
            request.headers,
            token=settings.app_api_token,
            app_id=caller,
            namespace=settings.execution_caller_namespace,
        )
    except MeshAuthenticationError as exc:
        logger.warning("Execution caller rejected: %s", exc.code)
        raise CallContextError(exc.status_code, exc.code, exc.detail) from None
    return caller


@router.post("/callctx.exchangeExecution", response_model=CallctxSignRpcResponse)
async def exchange_execution(
    request: Request,
    body: ExchangeExecutionRequest,
    settings: Annotated[Settings, Depends(get_settings)],
    auth_store: Annotated[AuthStoreProvider, Depends(get_auth_store)],
    metadata_store: Annotated[MetadataStoreProvider, Depends(get_metadata_store)],
    authn_cache: Annotated[AuthnCache, Depends(get_authn_cache)],
    authz_cache: Annotated[AuthzDecisionCache, Depends(get_authz_cache)],
    signer: Annotated[CallContextSigner, Depends(get_call_context_signer)],
) -> CallctxSignRpcResponse:
    """Authorize the credential's exact workspace/account before minting one bounded grant."""
    caller = execution_caller(request, settings, body.operation)
    try:
        async with asyncio.timeout(15):
            principal = await verify_token(
                body.token.get_secret_value(),
                auth_store=auth_store,
                metadata_store=metadata_store,
                authn_cache=authn_cache,
            )
            if not isinstance(principal, ServiceAccount):
                raise Unauthenticated("execution credential could not be verified")
            if (
                str(principal.principal_id) != body.principal_id
                or str(principal.workspace_id) != body.workspace_id
            ):
                raise ExecutionDenied("execution identity is not authorized for this workspace")
            workspace = await auth_store.get_workspace(principal.workspace_id)
            if workspace is None or workspace.disabled_at is not None:
                raise ExecutionDenied("execution identity is not authorized for this workspace")
            permission = EXECUTION_PERMISSIONS[body.operation]
            decision = await authorize(
                auth_store,
                metadata_store,
                principal_id=str(principal.principal_id),
                permission=permission,
                workspace_id=body.workspace_id,
                caller_component=caller,
                caller_tenant_id=str(workspace.tenant_id),
                cache=authz_cache,
                declared_permissions=request.app.state.declared_permissions,
            )
            if not decision.allowed:
                raise ExecutionDenied("execution account lacks the required permission")
            signed = await signer.sign(
                principal_id=body.principal_id,
                workspace_id=body.workspace_id,
                caller_component=caller,
                permissions=[permission],
                audience=DEFAULT_AUDIENCE,
                ttl_seconds=min(settings.call_context_ttl_seconds, DEFAULT_TTL_SECONDS),
                execution_operation=body.operation,
            )
            await metadata_store.append_audit(
                WorkspaceId(body.workspace_id),
                AuditEvent(
                    WorkspaceId(body.workspace_id),
                    uuid4().hex,
                    "execution.context.issued",
                    body.principal_id,
                    {"principal_id": body.principal_id},
                    {
                        "operation": body.operation,
                        "permission": permission,
                        "caller_component": caller,
                        "jti": signed.jti,
                        "expires_at": signed.exp,
                    },
                    datetime.now(UTC),
                ),
            )
    except (SPLError, TimeoutError, OSError, DaprSecretsResolutionError) as exc:
        logger.warning("Execution context issuance unavailable: %s", type(exc).__name__)
        raise ExecutionUnavailable("execution context issuance is unavailable") from None
    return CallctxSignRpcResponse(
        token=signed.token,
        kid=signed.kid,
        jti=signed.jti,
        iat=signed.iat,
        exp=signed.exp,
    )
