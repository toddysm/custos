"""Shared downstream-forwarding helpers for gateway route modules.

Both the declarative route registry (:mod:`custos_gateway.routes.registry`) and
the anonymous webhook pass-through (:mod:`custos_gateway.routes.webhook`) forward
a request to its owning component over Dapr and shape the reply back to the
caller. The two pieces of plumbing they share — locating the lifespan-owned
:class:`~custos_gateway.router.DownstreamRouter` on ``app.state`` and wrapping a
shaped :class:`~custos_gateway.router.DownstreamResponse` in a Starlette response
— live here so neither route module duplicates them.
"""

from __future__ import annotations

import base64
from typing import TYPE_CHECKING, Any, Final, cast

from fastapi import Response

from custos_gateway.errors import GatewayError, GatewayErrorCode
from custos_gateway.middleware.idempotency import METADATA_STORE_STATE_ATTR
from custos_gateway.middleware.ratelimit import RATE_LIMITER_STATE_ATTR

if TYPE_CHECKING:
    from collections.abc import Mapping

    from fastapi import Request

    from custos_gateway.middleware.idempotency import IdempotencyStore
    from custos_gateway.middleware.ratelimit import RateLimiter
    from custos_gateway.router import DownstreamResponse, DownstreamRouter

__all__ = [
    "DOWNSTREAM_ROUTER_STATE_ATTR",
    "get_downstream_router",
    "get_idempotency_store",
    "get_rate_limiter",
    "response_from_snapshot",
    "response_snapshot",
    "shaped_response",
]

#: ``app.state`` attribute holding the lifespan-owned
#: :class:`~custos_gateway.router.DownstreamRouter`. Bound by
#: :func:`custos_gateway.app.create_app` (AGW-IMPL-016).
DOWNSTREAM_ROUTER_STATE_ATTR: Final[str] = "downstream_router"


def get_downstream_router(request: Request) -> DownstreamRouter:
    """Return the lifespan-owned downstream router, or fail with 503.

    The router is bound to ``app.state`` by the application factory
    (AGW-IMPL-016); its absence means the gateway is not ready to forward.
    """
    router = getattr(request.app.state, DOWNSTREAM_ROUTER_STATE_ATTR, None)
    if router is None:
        raise GatewayError(
            GatewayErrorCode.DOWNSTREAM_UNAVAILABLE,
            detail="The gateway is not ready to forward requests.",
        )
    return cast("DownstreamRouter", router)


def get_rate_limiter(request: Request) -> RateLimiter | None:
    """Return the lifespan-owned rate limiter, or ``None`` when unbound.

    Unlike the downstream router, an absent limiter is *not* a failure: rate
    limiting is a best-effort shield, so the forwarder simply skips the check
    when no limiter is wired (the application factory always binds one in
    production).
    """
    return cast("RateLimiter | None", getattr(request.app.state, RATE_LIMITER_STATE_ATTR, None))


def get_idempotency_store(request: Request) -> IdempotencyStore | None:
    """Return the lifespan-owned SPL metadata store, or ``None`` when unbound.

    The write-path idempotency persistence is optional in M1 (no metadata-store
    DSN is configured for the gateway yet), so the forwarder skips deduplication
    when no store is bound rather than failing the request.
    """
    return cast(
        "IdempotencyStore | None", getattr(request.app.state, METADATA_STORE_STATE_ATTR, None)
    )


def shaped_response(reply: DownstreamResponse) -> Response:
    """Wrap a shaped downstream reply in a Starlette response.

    The downstream headers are already hop-by-hop-stripped and preserve repeated
    headers (e.g. ``Set-Cookie``); they are copied verbatim and ``content-length``
    is recomputed from the forwarded body.
    """
    response = Response(content=reply.body, status_code=reply.status_code)
    raw: list[tuple[bytes, bytes]] = [
        (name.encode("latin-1"), value.encode("latin-1")) for name, value in reply.headers
    ]
    raw.append((b"content-length", str(len(reply.body)).encode("latin-1")))
    response.raw_headers[:] = raw
    return response


def response_snapshot(reply: DownstreamResponse) -> dict[str, Any]:
    """Serialize a shaped downstream reply for idempotent-replay persistence.

    The body is base64-encoded so the snapshot is a JSON-safe mapping the SPL
    metadata store can persist verbatim and :func:`response_from_snapshot` can
    faithfully reconstruct on a later replay of the same idempotency key.
    """
    return {
        "status_code": reply.status_code,
        "headers": [[name, value] for name, value in reply.headers],
        "body_base64": base64.b64encode(reply.body).decode("ascii"),
    }


def response_from_snapshot(snapshot: Mapping[str, Any]) -> Response:
    """Rebuild a Starlette response from a persisted idempotency snapshot.

    The inverse of :func:`response_snapshot`: it restores the stored status,
    end-to-end headers (repeated headers preserved) and base64-decoded body, and
    recomputes ``content-length`` so the replay is byte-identical to the first
    response.
    """
    body = base64.b64decode(snapshot["body_base64"])
    response = Response(content=body, status_code=int(snapshot["status_code"]))
    raw: list[tuple[bytes, bytes]] = [
        (str(name).encode("latin-1"), str(value).encode("latin-1"))
        for name, value in snapshot["headers"]
    ]
    raw.append((b"content-length", str(len(body)).encode("latin-1")))
    response.raw_headers[:] = raw
    return response
