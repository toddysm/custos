"""Service-token endpoints (AS-IMPL-013, GH-#248).

* ``POST /v1/service-accounts/{principal_id}/tokens`` mints a bearer
  for the targeted service account. The plaintext is returned in the
  response body **exactly once** — the SPL row carries only a
  deterministic SHA-256 hash. The new token defaults to a 90-day TTL
  (configurable via ``CUSTOS_AUTH_SERVICE_TOKEN_TTL_DEFAULT``), with
  a per-mint override accepted on the request body.
* ``GET /v1/service-accounts/{principal_id}/tokens`` lists every
  token row for the SA — revoked rows included, plaintext excluded
  — so operators can render rotation history.

Both endpoints require ``admin:service-account`` and are scoped to
the caller's current workspace. Cross-workspace token enumeration is
explicitly disallowed: a token list query against a SA that lives in
another workspace collapses to ``404`` so the existence of the SA is
not leaked across the workspace boundary (mirrors the existence-
hiding gate documented in
:func:`custos_auth.authorize.authorize`).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Annotated
from uuid import uuid4

from custos_spl import AuthStoreProvider, MetadataStoreProvider
from custos_spl.ids import PrincipalId, ServiceTokenId
from custos_spl.interfaces.auth_store import ServiceAccount, ServiceToken
from fastapi import APIRouter, Depends, Path, status

from custos_auth.api.dependencies import (
    get_auth_store,
    get_metadata_store,
    get_settings,
    require_permission,
)
from custos_auth.api.errors import NotFound, ValidationFailure
from custos_auth.api.models import (
    ServiceTokenListResponse,
    ServiceTokenMintRequest,
    ServiceTokenMintResponse,
    service_token_to_response,
)
from custos_auth.audit import audit_token_issued
from custos_auth.middleware.callctx import CallContext
from custos_auth.settings import Settings
from custos_auth.tokens import mint_token

router = APIRouter(prefix="/v1", tags=["service-tokens"])


async def _load_service_account(
    auth_store: AuthStoreProvider,
    *,
    principal_id: str,
    caller_workspace_id: str | None,
) -> ServiceAccount:
    """Return the SA in the caller's workspace, or raise 404.

    Cross-workspace lookups collapse to ``NotFound`` so the SA's
    existence is not observable across workspace boundaries. A
    request whose target principal is a :class:`User` (not a
    :class:`ServiceAccount`) also raises ``NotFound`` for the same
    existence-hiding reason — users cannot hold service tokens, and
    surfacing a ``400`` would leak the principal-kind.
    """
    if caller_workspace_id is None:
        raise NotFound(f"service account '{principal_id}' not found")
    principal = await auth_store.get_principal(PrincipalId(principal_id))
    if not isinstance(principal, ServiceAccount):
        raise NotFound(f"service account '{principal_id}' not found")
    if str(principal.workspace_id) != caller_workspace_id:
        raise NotFound(f"service account '{principal_id}' not found")
    if principal.disabled_at is not None:
        # A disabled SA cannot mint or list — collapsing both to
        # ``ValidationFailure`` (400) is right because the resource
        # *does* exist in the workspace; the action is disallowed
        # rather than the resource hidden. List can read history
        # but mint must refuse; the caller handles the distinction.
        raise ValidationFailure(
            f"service account '{principal_id}' is disabled and cannot be acted on"
        )
    return principal


@router.post(
    "/service-accounts/{principal_id}/tokens",
    status_code=status.HTTP_201_CREATED,
    response_model=ServiceTokenMintResponse,
)
async def mint_service_token(
    body: ServiceTokenMintRequest,
    principal_id: Annotated[str, Path(min_length=1, max_length=120)],
    ctx: Annotated[
        CallContext,
        Depends(require_permission("admin:service-account")),
    ],
    auth_store: Annotated[AuthStoreProvider, Depends(get_auth_store)],
    metadata_store: Annotated[MetadataStoreProvider, Depends(get_metadata_store)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> ServiceTokenMintResponse:
    """Mint a bearer token for the targeted service account.

    Plaintext is returned in the response body and never persisted.
    Emits ``token.issued`` keyed to the SA's workspace.
    """
    sa = await _load_service_account(
        auth_store,
        principal_id=principal_id,
        caller_workspace_id=ctx.workspace_id,
    )
    ttl = body.ttl_seconds or settings.service_token_ttl_default_seconds
    plaintext, token_hash = mint_token()
    issued_at = datetime.now(UTC)
    expires_at = issued_at + timedelta(seconds=ttl)
    token_id = ServiceTokenId(str(uuid4()))
    token = ServiceToken(
        token_id=token_id,
        service_account_id=sa.principal_id,
        hash=token_hash,
        issued_at=issued_at,
        expires_at=expires_at,
        revoked_at=None,
        revoked_by=None,
        revoked_reason=None,
    )
    await auth_store.put_service_token(token)
    await audit_token_issued(
        metadata_store,
        actor=ctx.principal_id,
        workspace_id=str(sa.workspace_id),
        token_id=str(token_id),
        service_account_id=str(sa.principal_id),
        issued_at=issued_at,
        expires_at=expires_at,
    )
    return ServiceTokenMintResponse(
        token_id=str(token_id),
        service_account_id=str(sa.principal_id),
        token=plaintext,
        issued_at=issued_at,
        expires_at=expires_at,
    )


@router.get(
    "/service-accounts/{principal_id}/tokens",
    response_model=ServiceTokenListResponse,
)
async def list_service_tokens(
    principal_id: Annotated[str, Path(min_length=1, max_length=120)],
    ctx: Annotated[
        CallContext,
        Depends(require_permission("admin:service-account")),
    ],
    auth_store: Annotated[AuthStoreProvider, Depends(get_auth_store)],
) -> ServiceTokenListResponse:
    """List every token row owned by the SA (revoked rows included)."""
    # Existence-hiding gate; same lookup helper as mint. List is
    # permitted against a disabled SA so operators can audit the
    # history that led to disabling; the helper's
    # ``ValidationFailure`` is therefore caught and downgraded to
    # the regular lookup path for the read endpoint.
    try:
        sa = await _load_service_account(
            auth_store,
            principal_id=principal_id,
            caller_workspace_id=ctx.workspace_id,
        )
    except ValidationFailure:
        # The SA exists and lives in this workspace but is disabled;
        # we still allow read.
        sa_or_none = await auth_store.get_principal(PrincipalId(principal_id))
        assert isinstance(sa_or_none, ServiceAccount)  # invariant of the disabled branch
        sa = sa_or_none
    tokens = await auth_store.list_service_tokens_for_service_account(sa.principal_id)
    return ServiceTokenListResponse(
        tokens=[service_token_to_response(t) for t in tokens],
    )


__all__ = ["router"]
