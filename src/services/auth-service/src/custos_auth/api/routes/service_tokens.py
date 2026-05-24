"""Service-token endpoints (AS-IMPL-013, GH-#248; AS-IMPL-015, GH-#250).

* ``POST /v1/service-accounts/{principal_id}/tokens`` mints a bearer
  for the targeted service account. The plaintext is returned in the
  response body **exactly once** — the SPL row carries only a
  deterministic SHA-256 hash. The new token defaults to a 90-day TTL
  (configurable via ``CUSTOS_AUTH_SERVICE_TOKEN_TTL_DEFAULT``), with
  a per-mint override accepted on the request body.
* ``GET /v1/service-accounts/{principal_id}/tokens`` lists every
  token row for the SA — revoked rows included, plaintext excluded
  — so operators can render rotation history.
* ``DELETE /v1/tokens/{token_id}`` revokes a single token. Idempotent
  — a second revoke against an already-revoked row is a silent 204.
* ``DELETE /v1/service-accounts/{principal_id}/tokens`` bulk-revokes
  every live token owned by the SA. Emits one ``token.revoked`` row
  and one ``custos.auth.token-revoked`` event per token actually
  flipped from live → revoked; already-revoked rows are ignored.

All endpoints require ``admin:service-account`` and are scoped to
the caller's current workspace. Cross-workspace token access is
explicitly disallowed: any lookup against a SA (or a token whose
owning SA lives in another workspace) collapses to ``404`` so the
existence of the SA/token is not leaked across the workspace
boundary (mirrors the existence-hiding gate documented in
:func:`custos_auth.authorize.authorize`).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Annotated
from uuid import uuid4

from custos_spl import AuthStoreProvider, MetadataStoreProvider
from custos_spl.ids import PrincipalId, ServiceTokenId
from custos_spl.interfaces.auth_store import ServiceAccount, ServiceToken
from fastapi import APIRouter, Depends, Path, Response, status

from custos_auth.api.dependencies import (
    get_auth_store,
    get_metadata_store,
    get_settings,
    get_token_revoked_publisher,
    require_permission,
)
from custos_auth.api.errors import NotFound, ValidationFailure
from custos_auth.api.models import (
    ServiceTokenBulkRevokeResponse,
    ServiceTokenListResponse,
    ServiceTokenMintRequest,
    ServiceTokenMintResponse,
    ServiceTokenRevokeRequest,
    service_token_to_response,
)
from custos_auth.audit import audit_token_issued, audit_token_revoked
from custos_auth.middleware.callctx import CallContext
from custos_auth.settings import Settings
from custos_auth.token_revoked_events import (
    TokenRevokedEvent,
    TokenRevokedPublisher,
)
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


@router.delete(
    "/tokens/{token_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
)
async def revoke_service_token(
    body: ServiceTokenRevokeRequest,
    token_id: Annotated[str, Path(min_length=1, max_length=120)],
    ctx: Annotated[
        CallContext,
        Depends(require_permission("admin:service-account")),
    ],
    auth_store: Annotated[AuthStoreProvider, Depends(get_auth_store)],
    metadata_store: Annotated[MetadataStoreProvider, Depends(get_metadata_store)],
    publisher: Annotated[TokenRevokedPublisher, Depends(get_token_revoked_publisher)],
) -> Response:
    """Revoke a single service token.

    Cross-workspace lookups collapse to 404 (existence-hiding).
    Idempotent: a second revoke against the same id is a silent 204
    with no audit row and no eviction event — the SPL row is
    already in the terminal state and we don't want to spam either
    the audit pipeline or the cache-eviction bus.
    """
    if ctx.workspace_id is None:
        # No workspace context → caller cannot prove ownership of
        # any token. Collapse to 404 to keep the workspace-boundary
        # contract uniform across the surface.
        raise NotFound(f"service token '{token_id}' not found")
    token = await auth_store.get_service_token(ServiceTokenId(token_id))
    if token is None:
        raise NotFound(f"service token '{token_id}' not found")
    sa = await auth_store.get_principal(token.service_account_id)
    if not isinstance(sa, ServiceAccount) or str(sa.workspace_id) != ctx.workspace_id:
        # Either the owning SA was hard-deleted (defensive, design
        # forbids it) or the token belongs to a different
        # workspace. Same 404 either way so the response cannot be
        # used as a cross-workspace probe.
        raise NotFound(f"service token '{token_id}' not found")
    if token.revoked_at is not None:
        # Idempotency: silent 204, no audit row, no event publish.
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    await auth_store.revoke_service_token(
        ServiceTokenId(token_id),
        PrincipalId(ctx.principal_id),
        body.reason,
    )
    await audit_token_revoked(
        metadata_store,
        actor=ctx.principal_id,
        workspace_id=str(sa.workspace_id),
        token_id=token_id,
        service_account_id=str(sa.principal_id),
        reason=body.reason,
    )
    # Publish the eviction event AFTER the SPL commit and the audit
    # write so subscribers that re-read the SPL row always see the
    # ``revoked_at`` non-null. The local-bus subscriber in
    # :func:`custos_auth.create_app` invalidates the per-pod authn
    # cache; the cross-replica subscriber (no-op by default in M1)
    # would do the same on every other replica.
    await publisher.publish(
        TokenRevokedEvent(
            token_id=token_id,
            token_hash=token.hash,
            service_account_id=str(sa.principal_id),
        ),
    )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.delete(
    "/service-accounts/{principal_id}/tokens",
    response_model=ServiceTokenBulkRevokeResponse,
)
async def revoke_all_service_tokens(
    body: ServiceTokenRevokeRequest,
    principal_id: Annotated[str, Path(min_length=1, max_length=120)],
    ctx: Annotated[
        CallContext,
        Depends(require_permission("admin:service-account")),
    ],
    auth_store: Annotated[AuthStoreProvider, Depends(get_auth_store)],
    metadata_store: Annotated[MetadataStoreProvider, Depends(get_metadata_store)],
    publisher: Annotated[TokenRevokedPublisher, Depends(get_token_revoked_publisher)],
) -> ServiceTokenBulkRevokeResponse:
    """Revoke every live token owned by the SA.

    Already-revoked rows are skipped silently — only rows that
    transitioned live → revoked count toward the response's
    ``revoked_count`` and only those rows emit an audit/eviction
    pair. A disabled SA can still be the target of a bulk revoke
    (the design's "kill switch" flow chains disable → bulk revoke
    so leftover tokens are guaranteed dead even if a verifier
    races the disable step).
    """
    # Bypass the disabled-SA guard in :func:`_load_service_account`
    # because the bulk-revoke flow is exactly the path operators
    # take after disabling a SA to mop up its outstanding tokens.
    if ctx.workspace_id is None:
        raise NotFound(f"service account '{principal_id}' not found")
    sa_or_none = await auth_store.get_principal(PrincipalId(principal_id))
    if not isinstance(sa_or_none, ServiceAccount):
        raise NotFound(f"service account '{principal_id}' not found")
    if str(sa_or_none.workspace_id) != ctx.workspace_id:
        raise NotFound(f"service account '{principal_id}' not found")
    sa = sa_or_none

    tokens = await auth_store.list_service_tokens_for_service_account(sa.principal_id)
    revoked_count = 0
    for token in tokens:
        if token.revoked_at is not None:
            continue
        await auth_store.revoke_service_token(
            token.token_id,
            PrincipalId(ctx.principal_id),
            body.reason,
        )
        await audit_token_revoked(
            metadata_store,
            actor=ctx.principal_id,
            workspace_id=str(sa.workspace_id),
            token_id=str(token.token_id),
            service_account_id=str(sa.principal_id),
            reason=body.reason,
        )
        await publisher.publish(
            TokenRevokedEvent(
                token_id=str(token.token_id),
                token_hash=token.hash,
                service_account_id=str(sa.principal_id),
            ),
        )
        revoked_count += 1
    return ServiceTokenBulkRevokeResponse(revoked_count=revoked_count)


__all__ = ["router"]
