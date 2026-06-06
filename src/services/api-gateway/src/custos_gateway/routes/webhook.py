"""Anonymous webhook pass-through for the Custos API Gateway (AGW-IMPL-014).

``POST /v1/webhooks/{connectorInstanceId}`` is the gateway's single *anonymous*
ingress (see ``design/components/api-gateway/design.md`` § "Webhook ingress").
Unlike every other route, it carries no bearer, resolves no workspace and mints
no call context: the request is anonymous at the gateway boundary and signature
verification belongs to Trigger Service and the per-connector plugin, not the
gateway. The gateway's job is narrow and mechanical:

1. Enforce a 1 MiB body-size cap (the body is never inspected or altered).
2. Forward the request to Trigger Service over Dapr — body verbatim, headers
   minus ``Authorization`` (and the hop-by-hop / framing headers the Dapr client
   re-derives), plus the caller's source IP appended to ``X-Forwarded-For``.
3. Generate / propagate a correlation id so the anonymous hop is still traceable.

An unknown ``{connectorInstanceId}`` is not the gateway's concern: it forwards
the request and Trigger Service surfaces ``404 webhook-route-not-found``, which
passes back through the response shaper unchanged (a downstream ``4xx`` is a raw
pass-through; only a downstream ``5xx`` or transport error is masked as
``503 downstream-unavailable``).

Scope boundary (M1): this module declares the route and its forwarding. The
correlation-id ingress middleware and the ``app.state`` downstream-router binding
are assembled by :func:`custos_gateway.app.create_app` (AGW-IMPL-016); when this
route runs standalone (no ingress middleware) it mints its own correlation id.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

from fastapi import APIRouter, Request, Response

from custos_gateway.errors import CORRELATION_ID_HEADER, correlation_id_of
from custos_gateway.middleware.auth import WEBHOOK_BYPASS_PREFIX
from custos_gateway.middleware.correlation import new_correlation_id
from custos_gateway.middleware.validate import enforce_body_size
from custos_gateway.router import HOP_BY_HOP_HEADERS, DownstreamCall
from custos_gateway.routes._forwarding import get_downstream_router, shaped_response
from custos_gateway.routes.registry import TRIGGER_APP_ID
from custos_gateway.settings import DEFAULT_BODY_MAX_BYTES_DEFAULT

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

__all__ = [
    "FORWARDED_FOR_HEADER",
    "STRIPPED_INBOUND_HEADERS",
    "WEBHOOK_BODY_MAX_BYTES",
    "WEBHOOK_PATH",
    "build_webhook_forwarder",
    "build_webhook_router",
    "forward_headers",
]

#: The single anonymous webhook ingress path. ``{connectorInstanceId}`` is
#: opaque to the gateway and forwarded verbatim to Trigger Service.
WEBHOOK_PATH: Final[str] = "/v1/webhooks/{connectorInstanceId}"

#: Gateway-enforced body-size cap for webhook ingress (1 MiB). The design allows
#: per-connector-type advice to tune this downstream, but the cap itself is
#: always enforced at the gateway.
WEBHOOK_BODY_MAX_BYTES: Final[int] = DEFAULT_BODY_MAX_BYTES_DEFAULT

#: Header that carries the caller's source IP chain to the downstream.
FORWARDED_FOR_HEADER: Final[str] = "x-forwarded-for"

#: Inbound headers the gateway never forwards on the anonymous webhook hop.
#: ``authorization`` is dropped because the request is anonymous at the boundary
#: — any caller-supplied bearer is meaningless downstream and must not leak — and
#: ``host`` plus the hop-by-hop / framing headers are re-derived by the Dapr
#: client for the sidecar hop.
STRIPPED_INBOUND_HEADERS: Final[frozenset[str]] = HOP_BY_HOP_HEADERS | {"authorization", "host"}


def forward_headers(
    request: Request, *, correlation_id: str, source_ip: str | None
) -> dict[str, str]:
    """Build the headers forwarded to Trigger Service for a webhook request.

    Copies every inbound header except :data:`STRIPPED_INBOUND_HEADERS`, appends
    ``source_ip`` to the ``X-Forwarded-For`` chain, and stamps the resolved
    correlation id. ``Authorization`` is always dropped (anonymous hop).
    """
    headers: dict[str, str] = {
        name: value
        for name, value in request.headers.items()
        if name.lower() not in STRIPPED_INBOUND_HEADERS
    }
    if source_ip:
        existing = request.headers.get(FORWARDED_FOR_HEADER)
        headers[FORWARDED_FOR_HEADER] = f"{existing}, {source_ip}" if existing else source_ip
    headers[CORRELATION_ID_HEADER] = correlation_id
    return headers


def build_webhook_forwarder() -> Callable[[Request], Awaitable[Response]]:
    """Build the anonymous webhook forwarding endpoint.

    Returns the coroutine FastAPI mounts on :data:`WEBHOOK_PATH`. The endpoint
    enforces the body cap, builds the anonymous forward headers, and proxies the
    request to Trigger Service via the lifespan-owned downstream router.
    """

    async def _forward(request: Request) -> Response:
        body = await request.body()
        enforce_body_size(len(body), WEBHOOK_BODY_MAX_BYTES)

        downstream = get_downstream_router(request)
        correlation_id = correlation_id_of(request) or new_correlation_id()
        source_ip = request.client.host if request.client else None
        headers = forward_headers(request, correlation_id=correlation_id, source_ip=source_ip)

        method_path = request.url.path.lstrip("/")
        if request.url.query:
            method_path = f"{method_path}?{request.url.query}"

        reply = await downstream.invoke(
            DownstreamCall(
                app_id=TRIGGER_APP_ID,
                http_method=request.method,
                method_path=method_path,
                headers=headers,
                body=body or None,
            )
        )
        return shaped_response(reply)

    return _forward


def build_webhook_router() -> APIRouter:
    """Materialize the anonymous webhook route onto a FastAPI router.

    The route declares no ``require_permission`` dependency, so it is anonymous
    at the gateway boundary; its path lives under
    :data:`~custos_gateway.middleware.auth.WEBHOOK_BYPASS_PREFIX`, so the auth
    bypass classifier already excludes it from authentication.
    """
    assert WEBHOOK_PATH.startswith(WEBHOOK_BYPASS_PREFIX)
    router = APIRouter()
    router.add_api_route(
        WEBHOOK_PATH,
        build_webhook_forwarder(),
        methods=["POST"],
        name="webhook-passthrough",
    )
    return router
