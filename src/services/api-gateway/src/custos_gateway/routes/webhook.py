"""Bounded scoped webhook ingress; external credentials are verification data only.

The Gateway forwards exact raw bytes to Trigger over Dapr without authenticating
an end user or minting a call context. Only the canonical workspace/connector
route tunnels Authorization into a private verification header. Connector owns
signature verification; ambiguous legacy ingress fails closed without forwarding.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

from custos_callctx import CALLCTX_HEADER
from fastapi import APIRouter, Request, Response
from starlette.routing import compile_path

from custos_gateway.errors import (
    CORRELATION_ID_HEADER,
    GatewayError,
    GatewayErrorCode,
    correlation_id_of,
)
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

WEBHOOK_PATH: Final[str] = (
    "/v1/webhooks/workspaces/{workspaceId}/connectors/{connectorInstanceId}/events"
)
LEGACY_WEBHOOK_PATH: Final[str] = "/v1/webhooks/{connectorInstanceId}"
WEBHOOK_AUTHORIZATION_HEADER: Final[str] = "x-custos-webhook-authorization"
#: Trigger bounds the complete handoff to 60 seconds; retain 10 seconds for transport.
WEBHOOK_READ_TIMEOUT_SECONDS: Final[float] = 70.0
_SCOPED_PATH = compile_path(WEBHOOK_PATH)[0]

#: Gateway-enforced body-size cap for webhook ingress (1 MiB). The design allows
#: per-connector-type advice to tune this downstream, but the cap itself is
#: always enforced at the gateway.
WEBHOOK_BODY_MAX_BYTES: Final[int] = DEFAULT_BODY_MAX_BYTES_DEFAULT

#: Header that carries the caller's source IP chain to the downstream.
FORWARDED_FOR_HEADER: Final[str] = "x-forwarded-for"

#: Inbound headers the gateway never forwards on the anonymous webhook hop.
#: Never forward user-supplied platform identity or the private transport header.
STRIPPED_INBOUND_HEADERS: Final[frozenset[str]] = HOP_BY_HOP_HEADERS | {
    "authorization",
    "host",
    CALLCTX_HEADER.lower(),
    WEBHOOK_AUTHORIZATION_HEADER,
}


def forward_headers(
    request: Request, *, correlation_id: str, source_ip: str | None
) -> dict[str, str]:
    """Build the headers forwarded to Trigger Service for a webhook request.

    Copies every inbound header except :data:`STRIPPED_INBOUND_HEADERS`, appends
    ``source_ip`` to the ``X-Forwarded-For`` chain, and stamps the resolved
    correlation id. Only the exact scoped POST tunnels the external credential;
    it never becomes a Custos identity or an ambient Authorization header.
    """
    headers: dict[str, str] = {
        name: value
        for name, value in request.headers.items()
        if name.lower() not in STRIPPED_INBOUND_HEADERS and not name.lower().startswith("dapr-")
    }
    if request.scope.get("method") == "POST" and _SCOPED_PATH.fullmatch(
        request.scope.get("path", "")
    ):
        credentials = request.headers.getlist("authorization")
        if len(credentials) > 1:
            raise GatewayError(
                GatewayErrorCode.INVALID_TOKEN,
                detail="Ambiguous webhook verification credential.",
            )
        if credentials:
            headers[WEBHOOK_AUTHORIZATION_HEADER] = credentials[0]
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
        """Stop reading oversized streams before buffering or forwarding their remaining bytes."""
        body = bytearray()
        async for chunk in request.stream():
            enforce_body_size(len(body) + len(chunk), WEBHOOK_BODY_MAX_BYTES)
            body.extend(chunk)

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
                body=bytes(body) if body else None,
                read_timeout=WEBHOOK_READ_TIMEOUT_SECONDS,
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
    if not WEBHOOK_PATH.startswith(WEBHOOK_BYPASS_PREFIX):  # pragma: no cover - invariant
        msg = (
            f"webhook path {WEBHOOK_PATH!r} must live under the auth-bypass prefix "
            f"{WEBHOOK_BYPASS_PREFIX!r} or it would require authentication"
        )
        raise RuntimeError(msg)
    router = APIRouter()
    router.add_api_route(
        WEBHOOK_PATH,
        build_webhook_forwarder(),
        methods=["POST"],
        name="webhook-passthrough",
        openapi_extra={
            "parameters": [
                {"name": name, "in": "path", "required": True, "schema": {"type": "string"}}
                for name in ("workspaceId", "connectorInstanceId")
            ]
        },
    )

    @router.post(LEGACY_WEBHOOK_PATH, include_in_schema=False)
    async def reject_legacy() -> Response:
        """Reject ambiguous workspace-free routing without inspecting the body or forwarding."""
        raise GatewayError(
            GatewayErrorCode.WEBHOOK_ROUTE_NOT_FOUND,
            detail="Use the workspace-scoped connector webhook URL.",
        )

    return router
