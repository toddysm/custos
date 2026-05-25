"""Internal RPC inbound surface (AS-IMPL-025, GH-#260).

The design's "Internal RPC (consumed by every other component)" table
lists five method signatures every Custos component invokes against
the auth-service through Dapr service-invocation:

* ``authn.verifyToken(rawToken) -> Principal | None``
* ``authz.authorize(principalId, permission, workspaceId) -> Decision``
* ``authz.verifyAndAuthorize(rawToken, permission, workspaceId)
  -> Decision``
* ``callctx.sign(principal, workspaceId, callerComponent)
  -> SignedContext``
* ``callctx.verify(signedContext) -> CallContext | InvalidContext``

Dapr's HTTP service-invocation forwards
``POST /v1.0/invoke/custos-auth/method/{name}`` to the application
as ``POST /{name}``. We project the dotted method name onto an
``/rpc/`` prefix so the internal surface stays visibly separate from
the public REST surface (``/v1/...``). Concretely:

* Dapr app-id: ``custos-auth``
* Method names: ``rpc/authn.verifyToken``, ``rpc/authz.authorize``,
  ``rpc/authz.verifyAndAuthorize``, ``rpc/callctx.sign``,
  ``rpc/callctx.verify``

Bootstrap-time RPCs (``authn.verifyToken``,
``authz.verifyAndAuthorize``, ``callctx.sign``) are bypassed by the
call-context middleware — these are the calls a component makes to
**obtain** a call-context, so requiring one would be a chicken-and-egg
deadlock. ``authz.authorize`` and ``callctx.verify`` still require a
valid call-context header because callers already hold one by the
time they invoke them.

For M1 ``callctx.verify`` runs the verification locally against the
in-process :class:`KeyRing` (auth-service is both signer and
verifier of its own tokens, so a JWKS round-trip would be pure
overhead). External components rely on the JWKS endpoint plus the
``custos_callctx`` helper library (AS-IMPL-019) instead.
"""

from __future__ import annotations

from typing import Annotated, Any

import jwt
from custos_spl import AuthStoreProvider, MetadataStoreProvider
from custos_spl.ids import WorkspaceId
from custos_spl.interfaces.auth_store import ServiceAccount
from fastapi import APIRouter, Depends, Request, status
from pydantic import BaseModel, ConfigDict, Field

from custos_auth import _telemetry as telemetry
from custos_auth.api.dependencies import (
    get_auth_store,
    get_authn_cache,
    get_authz_cache,
    get_metadata_store,
)
from custos_auth.api.errors import Unauthenticated
from custos_auth.api.models import (
    PrincipalResponse,
    VerifyAndAuthorizeResponse,
    principal_to_response,
)
from custos_auth.audit import audit_call_context_invalid
from custos_auth.authn import verify_token
from custos_auth.authn_cache import AuthnCache
from custos_auth.authorize import authorize
from custos_auth.authz_cache import AuthzDecisionCache
from custos_auth.callctx_keyring import KeyRing
from custos_auth.callctx_signer import (
    ALGORITHM,
    DEFAULT_AUDIENCE,
    ISSUER,
    CallContextSigner,
)

router = APIRouter(prefix="/rpc", tags=["rpc"])


# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------


def get_call_context_signer(request: Request) -> CallContextSigner:
    """FastAPI dependency that returns the lifespan-owned signer."""
    signer = getattr(request.app.state, "call_context_signer", None)
    if signer is None:
        raise RuntimeError(
            "request.app.state.call_context_signer is not configured; "
            "the lifespan must build a CallContextSigner before the RPC "
            "surface can answer (AS-IMPL-017 wiring)."
        )
    assert isinstance(signer, CallContextSigner)
    return signer


def get_call_context_key_ring(request: Request) -> KeyRing:
    """FastAPI dependency that returns the lifespan-owned :class:`KeyRing`.

    The ring carries both active and overlap-window-retired public
    keys; ``callctx.verify`` resolves tokens against the full set so
    a call-context minted just before a rotation still verifies until
    its ``exp``.
    """
    ring = getattr(request.app.state, "call_context_key_ring", None)
    if ring is None:
        raise RuntimeError(
            "request.app.state.call_context_key_ring is not configured; "
            "the lifespan must build a KeyRing before the RPC surface "
            "can answer (AS-IMPL-018 wiring)."
        )
    assert isinstance(ring, KeyRing)
    return ring


# ---------------------------------------------------------------------------
# authn.verifyToken
# ---------------------------------------------------------------------------


class VerifyTokenRpcRequest(BaseModel):
    """Inbound body for ``rpc/authn.verifyToken``.

    Mirrors the REST :class:`custos_auth.api.routes.auth.VerifyRequest`
    so a component switching from REST to Dapr only changes the URL,
    not the payload shape.
    """

    model_config = ConfigDict(extra="forbid")

    token: Annotated[str, Field(min_length=1, max_length=4096)]


class VerifyTokenRpcResponse(BaseModel):
    """Outbound body for ``rpc/authn.verifyToken``.

    The design return type is ``Principal | None``; we project that as
    ``{principal: PrincipalResponse | null}`` so callers don't have to
    branch on HTTP status. A 200 with ``principal=null`` is a verify
    failure (the audit pipeline carries the disambiguating reason); a
    non-200 indicates a transport / 5xx error.
    """

    model_config = ConfigDict(extra="forbid")

    principal: PrincipalResponse | None


@router.post(
    "/authn.verifyToken",
    response_model=VerifyTokenRpcResponse,
    status_code=status.HTTP_200_OK,
)
async def authn_verify_token(
    body: VerifyTokenRpcRequest,
    auth_store: Annotated[AuthStoreProvider, Depends(get_auth_store)],
    metadata_store: Annotated[MetadataStoreProvider, Depends(get_metadata_store)],
    authn_cache: Annotated[AuthnCache, Depends(get_authn_cache)],
) -> VerifyTokenRpcResponse:
    """Internal RPC: verify a bearer and return its Principal envelope.

    Unlike the REST ``/v1/auth/verify`` route (which raises 401 on
    failure), the RPC surface returns a 200 with a null ``principal``
    so the design's ``Principal | None`` return type maps onto a
    single response shape. Callers (workflow-service, ARM, …) treat
    ``principal is None`` as "deny".
    """
    with telemetry.observe_operation(telemetry.OP_RPC_AUTHN_VERIFY_TOKEN):
        principal = await verify_token(
            body.token,
            auth_store=auth_store,
            metadata_store=metadata_store,
            authn_cache=authn_cache,
        )
        if principal is None:
            return VerifyTokenRpcResponse(principal=None)
        return VerifyTokenRpcResponse(principal=principal_to_response(principal))


# ---------------------------------------------------------------------------
# authz.authorize
# ---------------------------------------------------------------------------


class AuthorizeRpcRequest(BaseModel):
    """Inbound body for ``rpc/authz.authorize``.

    Carries the principal id (already authenticated by the caller),
    the permission name, and the workspace id. ``caller_component``
    lets the audit row record which component initiated the check
    so a denial in the audit feed is traceable to its origin RPC.
    """

    model_config = ConfigDict(extra="forbid")

    principal_id: Annotated[str, Field(min_length=1, max_length=120)]
    permission: Annotated[str, Field(min_length=1, max_length=255)]
    workspace_id: Annotated[str, Field(min_length=1, max_length=120)]
    caller_component: Annotated[str, Field(min_length=1, max_length=64)]


class AuthorizeRpcResponse(BaseModel):
    """Outbound body for ``rpc/authz.authorize``.

    Flattens the :class:`custos_auth.authorize.Decision` triple — same
    shape as :class:`VerifyAndAuthorizeResponse` minus the principal
    id (the caller already supplied it).
    """

    model_config = ConfigDict(extra="forbid")

    allowed: bool
    reason: str
    audit_event_id: str


@router.post(
    "/authz.authorize",
    response_model=AuthorizeRpcResponse,
    status_code=status.HTTP_200_OK,
)
async def authz_authorize(
    body: AuthorizeRpcRequest,
    auth_store: Annotated[AuthStoreProvider, Depends(get_auth_store)],
    metadata_store: Annotated[MetadataStoreProvider, Depends(get_metadata_store)],
    authz_cache: Annotated[AuthzDecisionCache, Depends(get_authz_cache)],
) -> AuthorizeRpcResponse:
    """Internal RPC: decide whether ``principal_id`` may perform ``permission``.

    The caller has already authenticated the bearer (typically via a
    prior ``rpc/authn.verifyToken`` or via the call-context header)
    and is asking auth-service to render the binding-based decision.
    """
    with telemetry.observe_operation(telemetry.OP_RPC_AUTHZ_AUTHORIZE):
        home_ws = await auth_store.get_workspace(WorkspaceId(body.workspace_id))
        caller_tenant_id = None if home_ws is None else str(home_ws.tenant_id)
        decision = await authorize(
            auth_store,
            metadata_store,
            principal_id=body.principal_id,
            permission=body.permission,
            workspace_id=body.workspace_id,
            caller_component=body.caller_component,
            caller_tenant_id=caller_tenant_id,
            cache=authz_cache,
        )
        return AuthorizeRpcResponse(
            allowed=decision.allowed,
            reason=decision.reason,
            audit_event_id=decision.audit_event_id,
        )


# ---------------------------------------------------------------------------
# authz.verifyAndAuthorize
# ---------------------------------------------------------------------------


class VerifyAndAuthorizeRpcRequest(BaseModel):
    """Inbound body for ``rpc/authz.verifyAndAuthorize``.

    Mirrors the REST
    :class:`custos_auth.api.models.VerifyAndAuthorizeRequest`.
    """

    model_config = ConfigDict(extra="forbid")

    token: Annotated[str, Field(min_length=1, max_length=4096)]
    permission: Annotated[str, Field(min_length=1, max_length=255)]
    workspace_id: Annotated[str, Field(min_length=1, max_length=120)]


@router.post(
    "/authz.verifyAndAuthorize",
    response_model=VerifyAndAuthorizeResponse,
    status_code=status.HTTP_200_OK,
)
async def authz_verify_and_authorize(
    body: VerifyAndAuthorizeRpcRequest,
    auth_store: Annotated[AuthStoreProvider, Depends(get_auth_store)],
    metadata_store: Annotated[MetadataStoreProvider, Depends(get_metadata_store)],
    authn_cache: Annotated[AuthnCache, Depends(get_authn_cache)],
    authz_cache: Annotated[AuthzDecisionCache, Depends(get_authz_cache)],
) -> VerifyAndAuthorizeResponse:
    """Internal RPC equivalent of ``/v1/authz/verify-and-authorize``.

    Same composed verify-then-authorize shape as the REST route — the
    REST route stays as the API Gateway's hot-path entry; this RPC
    exists so other internal components can issue the same composed
    check via Dapr service-invocation without going through the
    gateway.
    """
    with telemetry.observe_operation(
        telemetry.OP_RPC_AUTHZ_VERIFY_AND_AUTHORIZE,
        outcomes={Unauthenticated: "unauthenticated"},
    ):
        principal = await verify_token(
            body.token,
            auth_store=auth_store,
            metadata_store=metadata_store,
            authn_cache=authn_cache,
        )
        if principal is None:
            raise Unauthenticated("Token verification failed.")
        # M1 only supports service-account tokens.
        assert isinstance(principal, ServiceAccount)
        home_ws = await auth_store.get_workspace(principal.workspace_id)
        caller_tenant_id = None if home_ws is None else str(home_ws.tenant_id)
        decision = await authorize(
            auth_store,
            metadata_store,
            principal_id=str(principal.principal_id),
            permission=body.permission,
            workspace_id=body.workspace_id,
            caller_component="rpc-client",
            caller_tenant_id=caller_tenant_id,
            cache=authz_cache,
        )
        return VerifyAndAuthorizeResponse(
            principal_id=str(principal.principal_id),
            allowed=decision.allowed,
            reason=decision.reason,
            audit_event_id=decision.audit_event_id,
        )


# ---------------------------------------------------------------------------
# callctx.sign
# ---------------------------------------------------------------------------


class CallctxSignRpcRequest(BaseModel):
    """Inbound body for ``rpc/callctx.sign``.

    Today only the API Gateway should be invoking this; M2 will pin
    the caller list via a permission registry entry
    (``callctx:sign``). For M1 we accept any successfully bootstrapped
    caller — the bypass list means the caller authenticated via the
    bearer in the request, not via the call-context header.

    ``permissions`` carries the principal's RBAC grant as an embedded
    claim so downstream components can enforce ``require_permission``
    without an additional Auth Service round-trip. Each entry is a
    component-defined permission string (e.g.
    ``"catalog:workflows:read"``). The list MUST contain only
    non-empty strings; duplicates are collapsed by the verifier.

    ``audience`` overrides the signer's default ``aud`` claim
    (``custos.internal``). The API Gateway sets it to a per-component
    audience (``"custos.catalog"``, ``"custos.workflow"``, …) so a
    token minted for one downstream cannot be replayed against
    another.
    """

    model_config = ConfigDict(extra="forbid")

    principal_id: Annotated[str, Field(min_length=1, max_length=120)]
    workspace_id: Annotated[str | None, Field(default=None, max_length=120)] = None
    caller_component: Annotated[str, Field(min_length=1, max_length=64)]
    ttl_seconds: Annotated[int | None, Field(default=None, ge=1, le=86_400)] = None
    permissions: Annotated[
        list[Annotated[str, Field(min_length=1, max_length=128)]],
        Field(default_factory=list, max_length=256),
    ]
    audience: Annotated[str | None, Field(default=None, min_length=1, max_length=120)] = None


class CallctxSignRpcResponse(BaseModel):
    """Outbound body for ``rpc/callctx.sign``.

    Flattens :class:`custos_auth.callctx_signer.SignedContext` onto
    the wire. Callers propagate ``token`` through Dapr metadata; the
    other fields are exposed for diagnostics / audit correlation.
    """

    model_config = ConfigDict(extra="forbid")

    token: str
    kid: str
    jti: str
    iat: int
    exp: int


@router.post(
    "/callctx.sign",
    response_model=CallctxSignRpcResponse,
    status_code=status.HTTP_200_OK,
)
async def callctx_sign(
    body: CallctxSignRpcRequest,
    signer: Annotated[CallContextSigner, Depends(get_call_context_signer)],
) -> CallctxSignRpcResponse:
    """Internal RPC: mint a signed call-context JWT.

    TODO(M2): pin this method to a ``callctx:sign`` permission and
    enforce it via the permission registry (currently the bypass
    list lets any bootstrapped caller invoke it, which mirrors the
    M1 trust assumption that only the API Gateway is wired into the
    cluster as a caller of this RPC).
    """
    with telemetry.observe_operation(telemetry.OP_RPC_CALLCTX_SIGN):
        signed = await signer.sign(
            principal_id=body.principal_id,
            workspace_id=body.workspace_id,
            caller_component=body.caller_component,
            ttl_seconds=body.ttl_seconds,
            permissions=body.permissions if body.permissions else None,
            audience=body.audience,
        )
        return CallctxSignRpcResponse(
            token=signed.token,
            kid=signed.kid,
            jti=signed.jti,
            iat=signed.iat,
            exp=signed.exp,
        )


# ---------------------------------------------------------------------------
# callctx.verify
# ---------------------------------------------------------------------------


class CallctxVerifyRpcRequest(BaseModel):
    """Inbound body for ``rpc/callctx.verify``.

    ``audience`` lets the caller diagnose tokens minted with the
    per-component audience override (e.g. ``"custos.catalog"``).
    Defaults to the signer's :data:`DEFAULT_AUDIENCE` so existing
    callers (and the byte-stable internal callctx flow) keep working
    unchanged.
    """

    model_config = ConfigDict(extra="forbid")

    token: Annotated[str, Field(min_length=1, max_length=8192)]
    audience: Annotated[str | None, Field(default=None, min_length=1, max_length=120)] = None


class CallctxVerifyRpcResponse(BaseModel):
    """Outbound body for ``rpc/callctx.verify``.

    Mirrors the design's ``CallContext | InvalidContext`` union on a
    single wire shape: when ``valid`` is ``True`` the call-context
    claims are populated and ``reason`` is empty; when ``valid`` is
    ``False`` the claim fields are ``None`` and ``reason`` carries
    one of the closed-set verification failure codes.

    ``permissions`` is the optional embedded RBAC grant (Option D
    fat call-context). On successful verification it is the (possibly
    empty) list of strings copied verbatim from the JWT payload; on
    failure it is ``None``. Missing-from-claim and explicit empty list
    both surface as ``[]``.
    """

    model_config = ConfigDict(extra="forbid")

    valid: bool
    reason: str
    acting_principal_id: str | None = None
    workspace_id: str | None = None
    caller_component: str | None = None
    iat: int | None = None
    exp: int | None = None
    kid: str | None = None
    jti: str | None = None
    permissions: list[str] | None = None


#: Closed-set reason codes for ``callctx.verify`` failures. External
#: callers MUST treat anything not in this set as ``invalid``.
REASON_MALFORMED = "malformed"
REASON_UNKNOWN_KID = "unknown_kid"
REASON_BAD_SIGNATURE = "bad_signature"
REASON_EXPIRED = "expired"
REASON_WRONG_AUDIENCE = "wrong_audience"
REASON_WRONG_ISSUER = "wrong_issuer"


@router.post(
    "/callctx.verify",
    response_model=CallctxVerifyRpcResponse,
    status_code=status.HTTP_200_OK,
)
async def callctx_verify(
    body: CallctxVerifyRpcRequest,
    ring: Annotated[KeyRing, Depends(get_call_context_key_ring)],
    metadata_store: Annotated[MetadataStoreProvider, Depends(get_metadata_store)],
) -> CallctxVerifyRpcResponse:
    """Internal RPC: verify a signed call-context JWT locally.

    Resolves the token's ``kid`` against the active and
    overlap-window-retired entries of the in-process
    :class:`KeyRing` and verifies the EdDSA signature with PyJWT
    against the matching public key. Tokens that fail signature,
    audience, issuer, or expiry checks return ``valid=False`` with
    a closed-set ``reason``.

    Every ``valid=False`` return additionally emits a
    ``call-context.invalid`` audit row (carrying only the closed-set
    reason code and, when parseable, the ``kid`` header — never the
    raw token) and bumps :data:`telemetry.ERRORS_TOTAL` under the
    ``callctx.<reason>`` ``kind`` label so SLO dashboards can track
    verification-failure rates independently from RPC HTTP status.

    External components do not need to call this RPC on the hot
    path — they fetch the JWKS at ``/.well-known/jwks.json`` once
    per rotation period and verify locally via the
    ``custos_callctx`` helper library (AS-IMPL-019). This RPC
    exists so audit / admin tooling can ask auth-service to render
    a verification verdict for an arbitrary token (e.g. inspecting
    a token surfaced by a downstream 401).
    """
    with telemetry.observe_operation(telemetry.OP_RPC_CALLCTX_VERIFY):

        async def _invalid(reason: str, *, kid: str | None = None) -> CallctxVerifyRpcResponse:
            """Record + audit a verification failure and shape the response.

            Centralises the three side effects every invalid branch
            performs so each return site stays a single line and we
            cannot accidentally drop the audit row on a new failure
            path. ``kid`` is forwarded only when the unverified
            header parsed cleanly; pre-header parse failures pass
            ``None`` because we never trust a token's claimed key id
            before the header has been syntactically validated.
            """
            await audit_call_context_invalid(metadata_store, reason=reason, kid=kid)
            telemetry.record_error_kind(f"callctx.{reason}")
            return CallctxVerifyRpcResponse(valid=False, reason=reason)

        try:
            header = jwt.get_unverified_header(body.token)
        except jwt.InvalidTokenError:
            return await _invalid(REASON_MALFORMED)
        kid = header.get("kid")
        if not isinstance(kid, str) or not kid:
            return await _invalid(REASON_MALFORMED)
        public_key = None
        for entry in ring.all_public_entries():
            if entry.kid == kid:
                public_key = entry.public_key
                break
        if public_key is None:
            return await _invalid(REASON_UNKNOWN_KID, kid=kid)
        try:
            claims: dict[str, Any] = jwt.decode(
                body.token,
                public_key,
                algorithms=[ALGORITHM],
                audience=body.audience if body.audience is not None else DEFAULT_AUDIENCE,
                issuer=ISSUER,
            )
        except jwt.ExpiredSignatureError:
            return await _invalid(REASON_EXPIRED, kid=kid)
        except jwt.InvalidAudienceError:
            return await _invalid(REASON_WRONG_AUDIENCE, kid=kid)
        except jwt.InvalidIssuerError:
            return await _invalid(REASON_WRONG_ISSUER, kid=kid)
        except jwt.InvalidSignatureError:
            return await _invalid(REASON_BAD_SIGNATURE, kid=kid)
        except jwt.InvalidTokenError:
            return await _invalid(REASON_MALFORMED, kid=kid)

        def _required_str_claim(name: str) -> str | None:
            value = claims.get(name)
            if not isinstance(value, str) or not value:
                return None
            return value

        def _required_int_claim(name: str) -> int | None:
            value = claims.get(name)
            if isinstance(value, bool) or value is None:
                return None
            try:
                return int(value)
            except (TypeError, ValueError):
                return None

        acting = _required_str_claim("actingPrincipalId")
        caller_component = _required_str_claim("callerComponent")
        jti = _required_str_claim("jti")
        iat = _required_int_claim("iat")
        exp = _required_int_claim("exp")
        workspace = claims.get("workspaceId")

        if (
            acting is None
            or caller_component is None
            or jti is None
            or iat is None
            or exp is None
            or ("workspaceId" in claims and not isinstance(workspace, str))
        ):
            return await _invalid(REASON_MALFORMED, kid=kid)

        permissions_claim = claims.get("permissions")
        permissions_out: list[str] = []
        if permissions_claim is not None:
            if not isinstance(permissions_claim, list):
                return await _invalid(REASON_MALFORMED, kid=kid)
            for entry in permissions_claim:
                if not isinstance(entry, str) or not entry:
                    return await _invalid(REASON_MALFORMED, kid=kid)
                permissions_out.append(entry)

        return CallctxVerifyRpcResponse(
            valid=True,
            reason="",
            acting_principal_id=acting,
            workspace_id=workspace if isinstance(workspace, str) else None,
            caller_component=caller_component,
            iat=iat,
            exp=exp,
            kid=kid,
            jti=jti,
            permissions=permissions_out,
        )


__all__ = ["router"]
