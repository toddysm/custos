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

from typing import TYPE_CHECKING, Final, cast

from fastapi import Response

from custos_gateway.errors import GatewayError, GatewayErrorCode

if TYPE_CHECKING:
    from fastapi import Request

    from custos_gateway.router import DownstreamResponse, DownstreamRouter

__all__ = [
    "DOWNSTREAM_ROUTER_STATE_ATTR",
    "get_downstream_router",
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
