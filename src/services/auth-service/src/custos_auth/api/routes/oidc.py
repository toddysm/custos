"""OIDC server-side login callback (Phase I stub for Phase H).

Hosts ``POST /v1/auth/login/oidc/callback`` per
``design/components/auth-service/design.md`` § Public Interface.

Phase I (AS-IMPL-024) mounts the route so the REST surface is
complete and OpenAPI advertises the endpoint, but the actual OIDC
authorization-code exchange + identity-linking flow lands in
Phase H (AS-IMPL-020 — AS-IMPL-023). Until then the handler returns
``503 oidc_not_enabled`` so clients get a stable, machine-readable
signal that the feature exists but is gated behind the
``CUSTOS_AUTH_OIDC_ENABLED`` flag.

When ``CUSTOS_AUTH_OIDC_ENABLED=true`` is set *before* Phase H lands,
the handler returns ``503 oidc_not_implemented`` instead — that branch
is the safety net so an operator who flips the flag prematurely sees a
fail-loud diagnostic rather than a silent 404.

The endpoint is bypassed by the call-context middleware (it sits in
front of any internal trust boundary — the inbound request is an
external OIDC redirect from a provider, not an internal RPC carrying
a signed call-context header).
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, status
from pydantic import BaseModel, ConfigDict, Field
from starlette.responses import JSONResponse

from custos_auth.api.dependencies import get_settings
from custos_auth.settings import Settings

router = APIRouter(prefix="/v1", tags=["oidc"])


class OidcCallbackRequest(BaseModel):
    """Authorization-code exchange payload posted by the gateway.

    The OAuth2 / OIDC authorization-code flow redirects the user
    agent back to the gateway with ``code`` and ``state`` query
    params; the gateway re-POSTs them to auth-service so the
    code-to-token exchange happens server-side (the client secret
    never reaches the browser). ``issuer`` identifies which OIDC
    issuer (from ``CUSTOS_AUTH_OIDC_ISSUERS``) handled the login —
    multi-issuer deployments need this to route the exchange to
    the right verifier.

    Pydantic ``extra="forbid"`` so the wire schema is locked down
    and a misconfigured client cannot smuggle additional fields.
    """

    model_config = ConfigDict(extra="forbid")

    issuer: str = Field(..., min_length=1, max_length=512)
    code: str = Field(..., min_length=1, max_length=4096)
    state: str = Field(..., min_length=1, max_length=4096)


class OidcCallbackErrorResponse(BaseModel):
    """OpenAPI shape for the 503 envelope.

    Documents the response shape clients can rely on when the
    feature flag is off or Phase H has not landed. Mirrors the
    shared ``{"error": {"code", "detail"}}`` envelope used by
    every other auth-service route.
    """

    model_config = ConfigDict(extra="forbid")

    error: dict[str, str]


@router.post(
    "/auth/login/oidc/callback",
    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
    responses={
        503: {
            "description": (
                "OIDC verifier is disabled (Phase H pending) or the feature flag is off."
            ),
            "model": OidcCallbackErrorResponse,
        },
    },
)
async def oidc_callback(
    body: OidcCallbackRequest,
    settings: Annotated[Settings, Depends(get_settings)],
) -> JSONResponse:
    """Stub handler — returns 503 until Phase H ships the verifier.

    Two distinct 503 codes so an operator reading the audit feed
    can tell "flag intentionally off" apart from "flag flipped on
    but Phase H not yet deployed":

    * ``oidc_not_enabled`` — :attr:`Settings.oidc_enabled` is
      ``False`` (default). The expected M1 state.
    * ``oidc_not_implemented`` — flag is ``True`` but the
      Phase H route handlers have not landed. Programmer error
      that surfaces as a fail-loud diagnostic.

    The ``body`` parameter is accepted (and validated by Pydantic)
    so OpenAPI advertises the canonical request shape Phase H
    will consume; the values themselves are ignored in this stub.
    """
    # The body is intentionally unused in the stub but documented
    # via the Pydantic model so OpenAPI exposes the contract.
    _ = body
    if not settings.oidc_enabled:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "error": {
                    "code": "oidc_not_enabled",
                    "detail": (
                        "OIDC login is disabled on this deployment. Set "
                        "CUSTOS_AUTH_OIDC_ENABLED=true to enable the "
                        "verifier (Phase H must be deployed first)."
                    ),
                }
            },
        )
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={
            "error": {
                "code": "oidc_not_implemented",
                "detail": (
                    "OIDC verifier code paths have not landed yet "
                    "(Phase H, AS-IMPL-020 — AS-IMPL-023). Disable "
                    "CUSTOS_AUTH_OIDC_ENABLED until the upgrade "
                    "completes."
                ),
            }
        },
    )


__all__ = ["router"]
