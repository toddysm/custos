"""Token-verification endpoint (AS-IMPL-014, GH-#249).

``POST /v1/auth/verify`` accepts a bearer in the request body, runs
it through :func:`custos_auth.authn.verify_token`, and returns the
authenticated :class:`Principal` envelope on success or 401 on any
negative outcome. The endpoint deliberately does **not** require a
call-context header — it sits in front of the call-context shim and
is how internal services bootstrap a call-context from a raw bearer.

The endpoint is bypassed by the call-context middleware via the
``/v1/auth/verify`` entry in the
:mod:`custos_auth.middleware.callctx` ``_BYPASS_PATHS`` set; the
verifier is the source of call-context, not a consumer.
"""

from __future__ import annotations

from typing import Annotated

from custos_spl import AuthStoreProvider, MetadataStoreProvider
from fastapi import APIRouter, Depends, status
from pydantic import BaseModel, ConfigDict, Field

from custos_auth import _telemetry as telemetry
from custos_auth.api.dependencies import (
    get_auth_store,
    get_authn_cache,
    get_metadata_store,
)
from custos_auth.api.errors import Unauthenticated
from custos_auth.api.models import PrincipalResponse, principal_to_response
from custos_auth.authn import verify_token
from custos_auth.authn_cache import AuthnCache

router = APIRouter(prefix="/v1", tags=["auth"])


class VerifyRequest(BaseModel):
    """POST body for ``/v1/auth/verify``.

    A bare ``{"token": "..."}`` envelope keeps the wire format the
    same as the call-context bootstrap and lets the auth-service
    swap the verify path to a richer payload (e.g. carrying a
    requested workspace scope) without a breaking change.
    """

    model_config = ConfigDict(extra="forbid")

    token: str = Field(..., min_length=1, max_length=4096)


@router.post(
    "/auth/verify",
    response_model=PrincipalResponse,
    status_code=status.HTTP_200_OK,
)
async def verify(
    body: VerifyRequest,
    auth_store: Annotated[AuthStoreProvider, Depends(get_auth_store)],
    metadata_store: Annotated[MetadataStoreProvider, Depends(get_metadata_store)],
    authn_cache: Annotated[AuthnCache, Depends(get_authn_cache)],
) -> PrincipalResponse:
    """Verify a service-token bearer and return its Principal.

    Returns 200 with a :class:`PrincipalResponse` body when the
    bearer authenticates a live, non-disabled service account.
    Returns 401 with the standard call-context error envelope on
    any negative outcome — the body deliberately does not
    disambiguate ``unknown-token`` from ``revoked`` etc. so the
    endpoint cannot be used as an oracle. The audit pipeline
    carries the disambiguating ``reason``; see
    :mod:`custos_auth.authn` for the closed reason set.
    """
    with telemetry.observe_operation(
        telemetry.OP_AUTH_VERIFY,
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
        return principal_to_response(principal)


__all__ = ["router"]
