"""Correlation-id ingress + ``x-correlation-id`` response propagation.

Every request entering the gateway is stamped with a correlation id so a single
logical operation can be traced across the gateway, the downstream service it is
proxied to, and the audit trail (design.md § Observability). The middleware:

* honours an inbound ``x-correlation-id`` header *unchanged* when the client (or
  an upstream proxy/mesh) already supplied one, so a trace started elsewhere is
  preserved end to end;
* otherwise mints a fresh time-ordered UUIDv7 (RFC 9562) at ingress — version 7
  keeps ids roughly sortable by creation time, which makes log scans cheap;
* binds the id to ``request.state.correlation_id`` so exception handlers and
  downstream clients can read it; and
* echoes it back in the ``x-correlation-id`` response header on *every* response,
  success or error.

The standard library has no UUIDv7 generator on the supported interpreters
(3.11 / 3.12), so :func:`new_correlation_id` implements the layout directly.
"""

from __future__ import annotations

import os
import time
import uuid
from typing import Final

from starlette.requests import Request
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from custos_gateway.errors import CORRELATION_ID_HEADER

__all__ = ["CorrelationIdMiddleware", "new_correlation_id"]

_RFC9562_VERSION_7: Final[int] = 0x7000
_RFC9562_VARIANT_RFC: Final[int] = 0x8000


def new_correlation_id() -> str:
    """Return a fresh UUIDv7 (RFC 9562 § 5.7) as a canonical string.

    The layout is a 48-bit Unix-millisecond timestamp, a 4-bit version (``7``),
    12 bits of randomness, the 2-bit RFC variant, and 62 further random bits.
    The leading timestamp makes successive ids monotonically sortable, which
    keeps correlation-id range scans in logs and audit stores efficient.
    """
    unix_ms = time.time_ns() // 1_000_000
    rand_a = int.from_bytes(os.urandom(2), "big") & 0x0FFF
    rand_b = int.from_bytes(os.urandom(8), "big") & 0x3FFFFFFFFFFFFFFF
    value = (unix_ms & 0xFFFFFFFFFFFF) << 80
    value |= _RFC9562_VERSION_7 << 64
    value |= rand_a << 64
    value |= _RFC9562_VARIANT_RFC << 48
    value |= rand_b
    return str(uuid.UUID(int=value))


def _inbound_correlation_id(request: Request) -> str | None:
    """Return a usable inbound correlation id, or ``None`` to mint a fresh one.

    A present-but-whitespace-only header is treated as absent; a non-blank value
    is returned *verbatim* (never trimmed) so the "honour an inbound id
    unchanged" contract holds even for ids that carry surrounding whitespace.
    Starlette has already rejected control characters at the HTTP parser, so a
    non-empty value is safe to echo back as-is.
    """
    raw = request.headers.get(CORRELATION_ID_HEADER)
    if raw is None or not raw.strip():
        return None
    return raw


class CorrelationIdMiddleware:
    """ASGI middleware binding a correlation id to each request/response."""

    def __init__(self, app: ASGIApp) -> None:
        self._app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return

        request = Request(scope, receive=receive)
        correlation_id = _inbound_correlation_id(request) or new_correlation_id()
        scope.setdefault("state", {})
        scope["state"]["correlation_id"] = correlation_id

        async def send_with_header(message: Message) -> None:
            if message["type"] == "http.response.start":
                name = CORRELATION_ID_HEADER.encode("latin-1")
                headers = [
                    (key, value)
                    for (key, value) in message.get("headers", [])
                    if key.lower() != name
                ]
                headers.append((name, correlation_id.encode("latin-1")))
                message["headers"] = headers
            await send(message)

        await self._app(scope, receive, send_with_header)
