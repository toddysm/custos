"""HTTP transport seam shared by all built-in identity resolvers.

The four built-in resolvers each speak a different upstream API but
have the same set of moving parts: an outbound JSON request with
optional bearer auth, a status code to interpret, a small typed
response. Defining a tiny :class:`AsyncHttpClient` Protocol here keeps
the resolvers free of an ``httpx.AsyncClient`` dependency at the type
level: unit tests pass a stub implementation that records requests
and returns canned responses; production wires in the
:class:`HttpxAsyncHttpClient` adapter against a configured
``httpx.AsyncClient``.

We deliberately use a tiny in-house request/response shape rather than
the ``httpx`` types so the seam is stable even if we eventually swap
the underlying client. The two methods ``request`` and ``aclose`` are
the only contract resolvers depend on.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Protocol


@dataclass(frozen=True, slots=True)
class HttpRequest:
    """Outbound HTTP request issued by an identity resolver.

    The shape is intentionally minimal: every built-in resolver here
    either issues a ``GET`` (KMS / JWKS) or a form-or-JSON ``POST``
    (OIDC token exchange, AWS STS). Resolvers that need a JSON body
    set ``json_body``; resolvers that need an ``application/x-www-form-
    urlencoded`` body set ``form_body``. Setting both is a programming
    error and is rejected by :meth:`HttpxAsyncHttpClient.request`.
    """

    method: str
    url: str
    headers: Mapping[str, str] = field(default_factory=dict)
    json_body: Mapping[str, Any] | None = None
    form_body: Mapping[str, str] | None = None
    timeout_seconds: float = 5.0


@dataclass(frozen=True, slots=True)
class HttpResponse:
    """Response surface the resolvers consume.

    ``body`` is the raw response bytes (resolvers parse JSON on demand
    via :meth:`json`). ``headers`` is a case-insensitive view; in the
    Protocol shape we just promise lowercase keys, which matches what
    ``httpx`` already gives us via ``Response.headers``.
    """

    status_code: int
    body: bytes
    headers: Mapping[str, str] = field(default_factory=dict)

    def json(self) -> Any:
        """Decode ``body`` as JSON.

        Raises:
            ValueError: When ``body`` is empty or not valid JSON.
        """
        if not self.body:
            raise ValueError("response body is empty")
        return json.loads(self.body.decode("utf-8"))


class AsyncHttpClient(Protocol):
    """Minimal async HTTP client the resolvers depend on.

    The Protocol is satisfied structurally — tests typically subclass
    :class:`object` and implement ``request`` directly. ``aclose`` is
    optional; resolvers do not call it, but the
    :class:`IdentityResolverRegistry` owns the lifecycle of the
    production :class:`HttpxAsyncHttpClient` and calls it on shutdown.
    """

    async def request(self, request: HttpRequest) -> HttpResponse: ...

    async def aclose(self) -> None: ...


class HttpxAsyncHttpClient:
    """Default :class:`AsyncHttpClient` implementation backed by httpx.

    The constructor accepts a pre-configured ``httpx.AsyncClient`` so
    the operator can centralise proxy, TLS-trust, and base-URL
    settings. The adapter does *not* own that client when
    ``owns_client=False``: that lets us share a single client across
    multiple resolvers (the registry pattern) without cross-talk on
    shutdown.
    """

    def __init__(self, client: Any, *, owns_client: bool = True) -> None:
        # ``Any`` here keeps the import of httpx confined to the
        # implementation file; the resolvers only ever touch the
        # Protocol-typed seam.
        self._client = client
        self._owns_client = owns_client

    async def request(self, request: HttpRequest) -> HttpResponse:
        if request.json_body is not None and request.form_body is not None:
            raise ValueError(
                "HttpRequest cannot carry both json_body and form_body; "
                "pick the encoding that matches the upstream API"
            )
        # Import locally so unit tests that monkeypatch this class never
        # have to import httpx.
        import httpx

        kwargs: dict[str, Any] = {
            "method": request.method,
            "url": request.url,
            "headers": dict(request.headers),
            "timeout": request.timeout_seconds,
        }
        if request.json_body is not None:
            kwargs["json"] = dict(request.json_body)
        elif request.form_body is not None:
            kwargs["data"] = dict(request.form_body)

        try:
            response = await self._client.request(**kwargs)
        except httpx.HTTPError as exc:
            # Surface a structured payload the resolver can wrap.
            raise UpstreamTransportError(str(exc)) from exc

        return HttpResponse(
            status_code=response.status_code,
            body=bytes(response.content),
            headers=MappingProxyType({k.lower(): v for k, v in response.headers.items()}),
        )

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()


class UpstreamTransportError(Exception):
    """Raised by :class:`HttpxAsyncHttpClient` on transport-layer failures.

    Resolvers catch this and translate it into an
    :class:`IdentityResolverError` with code
    :attr:`IdentityResolverErrorCode.UPSTREAM_UNAVAILABLE`. Kept as a
    separate exception type so the seam stays test-friendly (stubs
    raise this directly instead of an ``httpx`` exception).
    """


__all__ = [
    "AsyncHttpClient",
    "HttpRequest",
    "HttpResponse",
    "HttpxAsyncHttpClient",
    "UpstreamTransportError",
]
