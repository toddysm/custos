"""Authorization endpoint (AS-IMPL-016, GH-#251).

Hosts ``POST /v1/authz/verify-and-authorize`` — the API Gateway
hot-path RPC documented in
``design/components/auth-service/design.md`` § Endpoints. The
endpoint composes :func:`custos_auth.authn.verify_token` and
:func:`custos_auth.authorize.authorize` in a single round trip so
the gateway does not have to chain two calls per request.

The audit pipeline still records the underlying primitives:
:func:`verify_token` emits ``authn.success`` / ``authn.failure`` /
``token.used``; :func:`authorize` emits one ``authz.decision`` per
call. The combined endpoint does not emit any additional rows of
its own — the two primitive rows are sufficient.
"""

from __future__ import annotations

from typing import Annotated

from custos_spl import AuthStoreProvider, MetadataStoreProvider
from custos_spl.interfaces.auth_store import ServiceAccount
from fastapi import APIRouter, Depends, status

from custos_auth.api.dependencies import (
    get_auth_store,
    get_authn_cache,
    get_authz_cache,
    get_metadata_store,
)
from custos_auth.api.errors import Unauthenticated
from custos_auth.api.models import (
    VerifyAndAuthorizeRequest,
    VerifyAndAuthorizeResponse,
)
from custos_auth.authn import verify_token
from custos_auth.authn_cache import AuthnCache
from custos_auth.authorize import authorize
from custos_auth.authz_cache import AuthzDecisionCache

router = APIRouter(prefix="/v1", tags=["authz"])


@router.post(
    "/authz/verify-and-authorize",
    response_model=VerifyAndAuthorizeResponse,
    status_code=status.HTTP_200_OK,
)
async def verify_and_authorize(
    body: VerifyAndAuthorizeRequest,
    auth_store: Annotated[AuthStoreProvider, Depends(get_auth_store)],
    metadata_store: Annotated[MetadataStoreProvider, Depends(get_metadata_store)],
    authn_cache: Annotated[AuthnCache, Depends(get_authn_cache)],
    authz_cache: Annotated[AuthzDecisionCache, Depends(get_authz_cache)],
) -> VerifyAndAuthorizeResponse:
    """Authenticate ``token`` then authorize ``permission`` on ``workspace_id``.

    Wire shape and audit pipeline behaviour are documented at the
    module level.
    """
    principal = await verify_token(
        body.token,
        auth_store=auth_store,
        metadata_store=metadata_store,
        authn_cache=authn_cache,
    )
    if principal is None:
        # Verify failures are existence-hiding: the body never
        # disambiguates ``unknown-token`` / ``revoked`` / ``expired``
        # — the audit pipeline carries the disambiguating reason.
        raise Unauthenticated("Token verification failed.")
    # M1 only supports service-account tokens. The verifier already
    # narrows the row to a SA, but the explicit type-check keeps the
    # mypy --strict surface honest and guards against a future
    # principal kind landing in this method by accident.
    assert isinstance(principal, ServiceAccount)
    # Derive the caller's tenant from the SA's owning workspace so
    # ``authorize`` can apply its cross-tenant existence-hiding gate.
    # A missing home workspace collapses to ``deny-workspace-not-found``
    # via the tenant-mismatch branch — that is the correct
    # information-hiding outcome.
    home_ws = await auth_store.get_workspace(principal.workspace_id)
    caller_tenant_id = None if home_ws is None else str(home_ws.tenant_id)
    decision = await authorize(
        auth_store,
        metadata_store,
        principal_id=str(principal.principal_id),
        permission=body.permission,
        workspace_id=body.workspace_id,
        caller_component="api-gateway",
        caller_tenant_id=caller_tenant_id,
        cache=authz_cache,
    )
    return VerifyAndAuthorizeResponse(
        principal_id=str(principal.principal_id),
        allowed=decision.allowed,
        reason=decision.reason,
        audit_event_id=decision.audit_event_id,
    )


__all__ = ["router"]
