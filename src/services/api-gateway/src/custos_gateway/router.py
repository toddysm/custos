"""Downstream Router + Response Shaper for the Custos API Gateway (AGW-IMPL-012).

After the request has been validated, authorized, deduplicated, rate-limited and
the call context minted, the gateway forwards it to the *owning* downstream
component over Dapr service invocation and shapes the reply (see
``design/components/api-gateway/design.md`` § "Request Router" / "Response
Shaper"). The gateway contains no domain logic: a 2xx (and any non-server-error)
downstream response is passed through *raw* — body, status, and the relevant
response headers — while a server-side failure (downstream ``5xx`` or a
transport error reaching the sidecar) is masked as ``503 downstream-unavailable``
so a transient backend hiccup never leaks an opaque 5xx envelope to the caller.

The transport mirrors the Auth Service client precedent (AGW-IMPL-004): a
lifespan-owned :class:`httpx.AsyncClient` posting to the local Dapr sidecar at
``http://{host}:{port}/v1.0/invoke/{appId}/method/{methodPath}``. Unlike the
Auth client the router reaches *many* downstream app ids, so the target app id
travels on each :class:`DownstreamCall` rather than being bound to the router.
The signed ``x-custos-callctx`` and the ``x-correlation-id`` (staged on
``request.state`` by the call-context minter) are carried as ordinary request
headers on the forwarded call.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final

import httpx

from custos_gateway.clients.auth import (
    DEFAULT_DAPR_HTTP_HOST,
    DEFAULT_DAPR_HTTP_PORT,
    DEFAULT_RPC_TIMEOUT_SECONDS,
    DaprEndpoint,
    build_invoke_url,
)
from custos_gateway.errors import GatewayError, GatewayErrorCode

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = [
    "HOP_BY_HOP_HEADERS",
    "DownstreamCall",
    "DownstreamResponse",
    "DownstreamRouter",
    "is_transient_status",
    "shape_response_headers",
]

logger = logging.getLogger("custos_gateway.router")

#: Headers that are connection-scoped (RFC 9110 § 7.6.1, plus ``content-length``
#: which the ASGI server recomputes from the raw body). They describe the hop to
#: the Dapr sidecar, not the end-to-end response, so they are dropped when the
#: downstream reply is shaped back to the original caller. ``content-encoding``
#: is intentionally *kept*: the body is forwarded verbatim, so its encoding must
#: travel with it.
HOP_BY_HOP_HEADERS: Final[frozenset[str]] = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
        "content-length",
    }
)

#: A downstream response at or above this status is treated as a transient
#: server-side failure and masked as ``503 downstream-unavailable``.
_TRANSIENT_STATUS_FLOOR: Final[int] = 500


def is_transient_status(status_code: int) -> bool:
    """Return ``True`` when ``status_code`` is a server-side failure to mask.

    Downstream ``5xx`` responses are transient backend failures the gateway
    hides behind its own ``503 downstream-unavailable``. Every other status
    (including the downstream's own ``4xx`` domain errors) is authoritative and
    passes through raw.
    """
    return status_code >= _TRANSIENT_STATUS_FLOOR


def shape_response_headers(headers: httpx.Headers) -> list[tuple[str, str]]:
    """Drop hop-by-hop headers, returning the end-to-end response headers.

    The body is forwarded verbatim, so end-to-end headers — notably
    ``content-type`` and ``content-encoding`` — are preserved while
    connection-scoped headers (see :data:`HOP_BY_HOP_HEADERS`) are removed.
    ``multi_items()`` is used so repeated headers (e.g. several ``set-cookie``)
    are preserved as distinct entries rather than coalesced into one
    comma-joined value.
    """
    return [
        (name, value)
        for name, value in headers.multi_items()
        if name.lower() not in HOP_BY_HOP_HEADERS
    ]


@dataclass(frozen=True, slots=True)
class DownstreamCall:
    """A single request to forward to an owning downstream component.

    ``app_id`` is the target's Dapr app id (e.g. ``custos-workflow``);
    ``method_path`` is the downstream method segment of the Dapr invoke URL
    (e.g. ``v1/workspaces/{ws}/runs``). ``headers`` already includes the
    forwarded ``x-custos-callctx`` and ``x-correlation-id`` plus any
    content-type; ``body`` is the raw request body (``None`` for bodyless
    methods).
    """

    app_id: str
    http_method: str
    method_path: str
    headers: Mapping[str, str] = field(default_factory=dict)
    body: bytes | None = None


@dataclass(frozen=True, slots=True)
class DownstreamResponse:
    """A shaped downstream reply ready to return to the original caller.

    ``headers`` are the end-to-end response headers (hop-by-hop headers already
    stripped) and ``body`` is the raw downstream body, returned unmodified.
    """

    status_code: int
    headers: list[tuple[str, str]]
    body: bytes


@dataclass(slots=True)
class DownstreamRouter:
    """Forwards a request to its owning component over the local Dapr sidecar.

    The ``http_client`` is owned by the app lifespan (not by the router) so it
    is shared across requests and closed once at shutdown. ``host``/``http_port``
    address the local Dapr sidecar; the per-call :attr:`DownstreamCall.app_id`
    selects the target component.
    """

    http_client: httpx.AsyncClient
    host: str = DEFAULT_DAPR_HTTP_HOST
    http_port: int = DEFAULT_DAPR_HTTP_PORT
    timeout: float = DEFAULT_RPC_TIMEOUT_SECONDS

    async def invoke(self, call: DownstreamCall) -> DownstreamResponse:
        """Forward ``call`` to its owning component and shape the reply.

        Returns the raw downstream response (body + status + end-to-end headers)
        for any non-server-error status.

        Raises:
            GatewayError: ``downstream-unavailable`` (503) when the sidecar
                cannot be reached (transport error) or the downstream returns a
                ``5xx``. The ``correlationId`` is attached by the exception
                handler from ``request.state``.
        """
        endpoint = DaprEndpoint(host=self.host, http_port=self.http_port, app_id=call.app_id)
        url = build_invoke_url(endpoint, call.method_path)
        try:
            response = await self.http_client.request(
                call.http_method,
                url,
                content=call.body,
                headers=dict(call.headers),
                timeout=self.timeout,
            )
        except httpx.HTTPError as exc:
            logger.warning("downstream %r unreachable via Dapr sidecar: %r", call.app_id, exc)
            raise GatewayError(
                GatewayErrorCode.DOWNSTREAM_UNAVAILABLE,
                detail=(
                    f"Downstream component {call.app_id!r} is temporarily "
                    "unavailable. Please retry."
                ),
            ) from exc

        if is_transient_status(response.status_code):
            logger.warning("downstream %r returned HTTP %d", call.app_id, response.status_code)
            raise GatewayError(
                GatewayErrorCode.DOWNSTREAM_UNAVAILABLE,
                detail=(
                    f"Downstream component {call.app_id!r} is temporarily "
                    "unavailable. Please retry."
                ),
            )

        return DownstreamResponse(
            status_code=response.status_code,
            headers=shape_response_headers(response.headers),
            body=response.content,
        )
