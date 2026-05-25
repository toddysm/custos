"""OIDC server-side login callback (AS-IMPL-020..023).

Hosts ``POST /v1/auth/login/oidc/callback`` per
``design/components/auth-service/design.md`` § Public Interface.

Flow (when ``CUSTOS_AUTH_OIDC_ENABLED=true`` and the operator has
configured at least one issuer):

1. Resolve the issuer entry from ``CUSTOS_AUTH_OIDC_ISSUERS`` by the
   request body's ``issuer`` id.
2. Exchange the authorization ``code`` for an ID token by POSTing to
   the issuer's ``token_endpoint`` with the configured ``client_id``
   and the env-var-projected client secret.
3. Verify the ID token via :class:`custos_auth.oidc.OidcVerifier`
   (claim enforcement + JWKS lookup).
4. Provision: link or create the internal :class:`User` via
   :class:`custos_auth.oidc.OidcProvisioner`. Zero-binding policy
   applies — newly provisioned users have no workspace bindings.
5. Emit ``authn.success`` (with ``authentication_type=oidc`` and
   preset-specific extras) and return the resolved
   :class:`PrincipalResponse`.

Every verification failure short-circuits to ``authn.failure`` with a
closed-set reason from
:data:`custos_auth.oidc.verifier.FAILURE_REASONS` and an HTTP 401.
OAuth code-exchange failures (provider reachable but rejected the
exchange) map to ``oidc_exchange_failed`` returned as 502 — config /
secret rotation problem, not a token-validation problem.

The endpoint stays bypassed by the call-context middleware (it sits
in front of any internal trust boundary — the inbound request is an
external OIDC redirect from a provider, not an internal RPC).
"""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from typing import Annotated, Any

import httpx
from custos_spl import MetadataStoreProvider
from fastapi import APIRouter, Depends, Request, status
from pydantic import BaseModel, ConfigDict, Field
from starlette.responses import JSONResponse

from custos_auth.api.dependencies import get_metadata_store, get_settings
from custos_auth.api.models import PrincipalResponse, principal_to_response
from custos_auth.audit import audit_authn_failure_oidc, audit_authn_success_oidc
from custos_auth.oidc import IssuersConfig, OidcVerifier, parse_issuers_config
from custos_auth.oidc.provisioning import OidcProvisioner, ProvisionResult
from custos_auth.oidc.verifier import (
    REASON_JWKS_FETCH_FAILED,
    REASON_MALFORMED,
    REASON_MISSING_CLAIM,
    OidcVerificationError,
)
from custos_auth.settings import Settings

_LOG = logging.getLogger(__name__)

router = APIRouter(prefix="/v1", tags=["oidc"])


class OidcCallbackRequest(BaseModel):
    """Authorization-code exchange payload posted by the gateway.

    The gateway re-POSTs the provider's ``code`` and ``state`` query
    params to auth-service so the code-to-token exchange happens
    server-side (the client secret never reaches the browser).
    ``issuer`` identifies which configured issuer (from
    ``CUSTOS_AUTH_OIDC_ISSUERS``) handled the login. ``redirect_uri``
    is the value the gateway sent on the original authorization
    request (the provider re-validates it on the exchange call).
    """

    model_config = ConfigDict(extra="forbid")

    issuer: str = Field(..., min_length=1, max_length=512)
    code: str = Field(..., min_length=1, max_length=4096)
    state: str = Field(..., min_length=1, max_length=4096)
    redirect_uri: str | None = Field(default=None, min_length=1, max_length=2048)


class OidcCallbackResponse(BaseModel):
    """Successful-callback envelope.

    Carries the resolved Principal (existing or freshly provisioned)
    plus a ``newly_provisioned`` flag the gateway can use to redirect
    first-login users through a setup flow.
    """

    model_config = ConfigDict(extra="forbid")

    principal: PrincipalResponse
    newly_provisioned: bool


class OidcCallbackErrorResponse(BaseModel):
    """OpenAPI shape for the 4xx / 5xx envelopes."""

    model_config = ConfigDict(extra="forbid")

    error: dict[str, str]


def _error_response(*, status_code: int, code: str, detail: str) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={"error": {"code": code, "detail": detail}},
    )


def _get_oidc_state(
    request: Request, settings: Settings
) -> tuple[
    IssuersConfig,
    OidcVerifier | None,
    OidcProvisioner | None,
    httpx.AsyncClient | None,
]:
    """Resolve the OIDC subsystem from ``request.app.state``.

    Tests that build the FastAPI app via :func:`create_app` get the
    wired state for free. Tests that construct a bare ``FastAPI``
    (no lifespan) fall back to parsing the settings raw JSON inline
    so the route still type-checks — but the verifier / provisioner
    / http client are ``None`` and the route returns
    ``oidc_not_implemented``.
    """
    state = request.app.state
    issuers = getattr(state, "oidc_issuers", None)
    if issuers is None:
        issuers = parse_issuers_config(settings.oidc_issuers_raw)
    return (
        issuers,
        getattr(state, "oidc_verifier", None),
        getattr(state, "oidc_provisioner", None),
        getattr(state, "oidc_http_client", None),
    )


async def _exchange_code_for_id_token(
    http_client: httpx.AsyncClient,
    *,
    token_endpoint: str,
    client_id: str,
    client_secret: str,
    code: str,
    redirect_uri: str | None,
) -> str:
    """Perform the OAuth 2.0 ``authorization_code`` token exchange.

    Provider returns ``{"id_token": ..., "access_token": ..., ...}``;
    we only consume ``id_token``. ``access_token`` is intentionally
    discarded — v1 does not call any provider APIs on the user's
    behalf.
    """
    payload = {
        "grant_type": "authorization_code",
        "code": code,
        "client_id": client_id,
        "client_secret": client_secret,
    }
    if redirect_uri:
        payload["redirect_uri"] = redirect_uri
    try:
        response = await http_client.post(
            token_endpoint,
            data=payload,
            headers={"Accept": "application/json"},
        )
    except Exception as exc:
        raise OidcVerificationError(
            REASON_JWKS_FETCH_FAILED,
            f"OIDC token exchange transport failed: {exc}",
        ) from exc
    if response.status_code != 200:
        raise OidcVerificationError(
            REASON_JWKS_FETCH_FAILED,
            (
                f"OIDC token exchange returned HTTP {response.status_code}; "
                "provider rejected the code or our client credentials"
            ),
        )
    try:
        body = response.json()
    except Exception as exc:
        raise OidcVerificationError(
            REASON_MALFORMED,
            f"OIDC token exchange response is not valid JSON: {exc}",
        ) from exc
    id_token = body.get("id_token") if isinstance(body, dict) else None
    if not isinstance(id_token, str) or not id_token:
        raise OidcVerificationError(
            REASON_MALFORMED,
            "OIDC token exchange response is missing the 'id_token' field",
        )
    return id_token


@router.post(
    "/auth/login/oidc/callback",
    responses={
        200: {"model": OidcCallbackResponse},
        401: {"model": OidcCallbackErrorResponse},
        502: {"model": OidcCallbackErrorResponse},
        503: {"model": OidcCallbackErrorResponse},
    },
)
async def oidc_callback(
    body: OidcCallbackRequest,
    request: Request,
    settings: Annotated[Settings, Depends(get_settings)],
    metadata_store: Annotated[MetadataStoreProvider, Depends(get_metadata_store)],
) -> JSONResponse:
    """Handle the OIDC server-side callback.

    Returns:
        * 200 on a successful verify + provision — body =
          :class:`OidcCallbackResponse`.
        * 401 ``oidc_verification_failed`` when the verifier
          rejects the ID token. The audit row carries the closed-set
          ``reason``; the response body only carries the operator-
          opaque code (we do not leak bad-signature vs expired-token
          to the caller).
        * 502 ``oidc_exchange_failed`` when we cannot reach the
          provider's token endpoint or it rejects our exchange.
        * 503 ``oidc_not_enabled`` when the flag is off,
          ``oidc_not_configured`` when the flag is on but no issuers
          match the request, or ``oidc_not_implemented`` when the
          app state is missing the verifier (bare-app test).
    """
    if not settings.oidc_enabled:
        return _error_response(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            code="oidc_not_enabled",
            detail=(
                "OIDC login is disabled on this deployment. Set "
                "CUSTOS_AUTH_OIDC_ENABLED=true to enable the verifier."
            ),
        )

    issuers, verifier, provisioner, http_client = _get_oidc_state(request, settings)
    if verifier is None or provisioner is None or http_client is None:
        return _error_response(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            code="oidc_not_implemented",
            detail=(
                "OIDC subsystem is not fully wired in the current app "
                "lifespan. This is usually a bare-app test fixture; "
                "production deployments wire the subsystem on startup."
            ),
        )

    if not issuers.issuers:
        return _error_response(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            code="oidc_not_configured",
            detail=(
                "OIDC is enabled but no issuers are configured. Populate CUSTOS_AUTH_OIDC_ISSUERS."
            ),
        )

    entry = issuers.by_id(body.issuer)
    if entry is None:
        # Configured-issuer-set is operator-visible; the response carries
        # a generic code so a probing attacker cannot enumerate known
        # issuer ids. Audit carries the requested id for forensics.
        await audit_authn_failure_oidc(
            metadata_store,
            reason=REASON_MISSING_CLAIM,
            issuer_id=body.issuer,
        )
        return _error_response(
            status_code=status.HTTP_401_UNAUTHORIZED,
            code="oidc_verification_failed",
            detail="OIDC issuer is not configured.",
        )

    # Step 1 — OAuth code exchange (skipped for workload-token-only
    # issuers like GitHub Actions where there is no code flow).
    if entry.token_endpoint is None:
        return _error_response(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            code="oidc_not_configured",
            detail=(
                f"Issuer {entry.id!r} has no token_endpoint configured "
                "and therefore does not support the authorization-code "
                "callback. Workload-token issuers verify tokens via "
                "/v1/auth/verify instead."
            ),
        )
    if entry.client_id is None or entry.client_secret_env is None:
        return _error_response(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            code="oidc_not_configured",
            detail=(
                f"Issuer {entry.id!r} is missing client_id or "
                "client_secret_env; cannot perform code exchange."
            ),
        )
    client_secret = os.environ.get(entry.client_secret_env, "").strip()
    if not client_secret:
        return _error_response(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            code="oidc_not_configured",
            detail=(
                f"Issuer {entry.id!r} client secret env var {entry.client_secret_env!r} is empty."
            ),
        )

    try:
        id_token = await _exchange_code_for_id_token(
            http_client,
            token_endpoint=entry.token_endpoint,
            client_id=entry.client_id,
            client_secret=client_secret,
            code=body.code,
            redirect_uri=body.redirect_uri,
        )
    except OidcVerificationError as exc:
        await audit_authn_failure_oidc(
            metadata_store,
            reason=exc.reason,
            issuer_id=entry.id,
            issuer=entry.issuer_url,
        )
        _LOG.warning(
            "OIDC code exchange failed issuer_id=%s reason=%s detail=%s",
            entry.id,
            exc.reason,
            exc.detail,
        )
        return _error_response(
            status_code=status.HTTP_502_BAD_GATEWAY,
            code="oidc_exchange_failed",
            detail="OIDC provider rejected the authorization-code exchange.",
        )

    # Step 2 — Verify the ID token.
    try:
        verified = await verifier.verify(entry.id, id_token)
    except OidcVerificationError as exc:
        await audit_authn_failure_oidc(
            metadata_store,
            reason=exc.reason,
            issuer_id=entry.id,
            issuer=entry.issuer_url,
        )
        _LOG.info(
            "OIDC verification failed issuer_id=%s reason=%s",
            entry.id,
            exc.reason,
        )
        return _error_response(
            status_code=status.HTTP_401_UNAUTHORIZED,
            code="oidc_verification_failed",
            detail="OIDC token verification failed.",
        )

    # Step 3 — Provision (link or zero-binding create).
    result: ProvisionResult = await provisioner.provision(verified)

    # Step 4 — Emit authn.success with preset-specific extras.
    extras = _preset_audit_extras(entry.preset, verified.claims)
    await audit_authn_success_oidc(
        metadata_store,
        user_id=str(result.user.principal_id),
        issuer=entry.issuer_url,
        subject=verified.subject,
        issuer_id=entry.id,
        extra_payload=extras,
    )

    principal_payload = principal_to_response(result.user)
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content=OidcCallbackResponse(
            principal=principal_payload,
            newly_provisioned=result.newly_provisioned,
        ).model_dump(mode="json"),
    )


def _preset_audit_extras(preset: str | None, claims: Mapping[str, Any]) -> dict[str, str]:
    """Pull preset-specific extras into the audit row.

    Returns an empty dict when no preset is configured (the generic
    verifier emits only the core ``authentication_type`` / ``issuer``
    / ``issuer_id`` fields).
    """
    if preset is None:
        return {}
    from custos_auth.oidc.presets import get_preset

    try:
        module = get_preset(preset)
    except ValueError:  # pragma: no cover — guarded by config parser
        return {}
    return module.extra_audit_payload(claims)


__all__ = ["router"]
