"""Scope the local Dapr API credential to loopback HTTP calls, never upstream providers."""

import os
import ssl
from collections.abc import Mapping

import httpx


def configure_dapr_http(
    client: httpx.AsyncClient,
    env: Mapping[str, str] | None = None,
) -> httpx.AsyncClient:
    """Attach the configured API token on local sidecar requests, including redirect rechecks.

    The caller retains client ownership. Without a configured token this leaves
    legacy clients unchanged. An outbound redirect/external provider never gets
    the local API credential, even if a previous request carried the header.
    """
    source = os.environ if env is None else env
    token = source.get("DAPR_API_TOKEN")
    if token is None:
        return client
    if not token.strip():
        raise ValueError("DAPR_API_TOKEN must not be empty when configured")
    loopback = {"127.0.0.1", "localhost", "::1"}
    if source.get("DAPR_HTTP_HOST", "127.0.0.1") not in loopback:
        raise ValueError("authenticated Dapr HTTP must use a loopback sidecar")
    port = int(source.get("DAPR_HTTP_PORT", "3500"))
    if not 1 <= port <= 65535:
        raise ValueError("DAPR_HTTP_PORT must be in 1..65535")
    if client.trust_env:
        raise ValueError("authenticated Dapr clients must disable environment proxy routing")

    async def scoped_token(request: httpx.Request) -> None:
        """Check each outgoing hop, including redirects, before attaching private authority."""
        request.headers.pop("dapr-api-token", None)
        if (
            request.url.scheme == "http"
            and request.url.host in loopback
            and request.url.port == port
            and request.url.path.startswith("/v1.0/")
            and not request.url.username
        ):
            request.headers["dapr-api-token"] = token

    client.event_hooks["request"].append(scoped_token)
    return client


def dapr_http_client(
    *,
    timeout: float | httpx.Timeout = 5.0,
    env: Mapping[str, str] | None = None,
    base_url: str = "",
    transport: httpx.AsyncBaseTransport | None = None,
) -> httpx.AsyncClient:
    """Create an owned client without implicit proxies, including token-less secret reads.

    Dapr exchanges carry private response/body data even without an API token.
    Upstream providers requiring proxies must use a separate explicit transport.
    Explicit base URLs and test transports retain the same token and CA policy.
    """
    source = os.environ if env is None else env
    trust: ssl.SSLContext | bool = True
    if source.get("SSL_CERT_FILE") or source.get("SSL_CERT_DIR"):
        trust = ssl.create_default_context(
            cafile=source.get("SSL_CERT_FILE"),
            capath=source.get("SSL_CERT_DIR"),
        )
    client = httpx.AsyncClient(
        timeout=timeout,
        trust_env=False,
        verify=trust,
        base_url=base_url,
        transport=transport,
    )
    return configure_dapr_http(client, source)
