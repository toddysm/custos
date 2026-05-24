"""JWKS endpoint for the call-context signing key (AS-IMPL-018, GH-#253).

Publishes ``/.well-known/jwks.json`` carrying the currently active
call-context EdDSA public key plus any retired keys still within the
``2x CUSTOS_AUTH_CALL_CONTEXT_KEY_ROTATION`` overlap window. Every
other Custos component fetches this endpoint via the helper library
shipped in AS-IMPL-019 (:mod:`custos_callctx`) and verifies inbound
call-context JWTs locally against the cached JWKS — no Auth Service
round-trip per request.

Design reference:
``design/components/auth-service/design.md`` § Internal vs External
Auth — Trust Model: *"Receiving components verify the signature
locally (no Auth Service round-trip) using the published JWKS."*

The endpoint is bypassed by the call-context middleware (every
component must be able to fetch JWKS without already holding a
verified call context) — see :mod:`custos_auth.middleware.callctx`
``_BYPASS_PATHS``.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from custos_auth.callctx_keyring import JWKS_CACHE_FRACTION, KeyRing

router = APIRouter(tags=["jwks"])


def get_key_ring(request: Request) -> KeyRing:
    """FastAPI dependency that returns the lifespan-owned :class:`KeyRing`.

    The lifespan stashes the ring on ``app.state.call_context_key_ring``;
    failing to find it raises :class:`RuntimeError` so a misconfigured
    deployment fails loud rather than silently serving an empty JWKS.
    """
    ring = getattr(request.app.state, "call_context_key_ring", None)
    if ring is None:
        raise RuntimeError(
            "request.app.state.call_context_key_ring is not configured; "
            "the lifespan must build a KeyRing before the JWKS route can "
            "answer (AS-IMPL-018 wiring)."
        )
    assert isinstance(ring, KeyRing)
    return ring


@router.get(
    "/.well-known/jwks.json",
    response_class=JSONResponse,
    summary="Call-context signing-key JWKS",
)
async def jwks(
    ring: Annotated[KeyRing, Depends(get_key_ring)],
) -> JSONResponse:
    """Serve the active + overlap-window-retired call-context public keys.

    The response shape follows RFC 7517 (JWK Set) with RFC 8037 OKP
    keys for Ed25519:

    .. code-block:: json

        {
          "keys": [
            {
              "kty": "OKP", "crv": "Ed25519", "alg": "EdDSA",
              "use": "sig", "kid": "<16-hex>",
              "x": "<base64url raw public key>"
            },
            ...
          ]
        }

    The active key is listed first so verifiers that scan linearly
    for ``kid`` succeed faster on the common case.

    The ``Cache-Control`` header sets ``max-age`` to half the rotation
    period so verifiers refresh their cached JWKS before retired keys
    age out — that is the AS-IMPL-018 acceptance criterion "JWKS
    cacheable via standard HTTP caching headers".
    """
    keys = [entry.to_jwk() for entry in ring.all_public_entries()]
    max_age = max(1, int(ring.rotation_period_seconds * JWKS_CACHE_FRACTION))
    return JSONResponse(
        content={"keys": keys},
        headers={"Cache-Control": f"public, max-age={max_age}"},
    )


__all__ = ["get_key_ring", "router"]
