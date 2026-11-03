"""Bound private JSON responses without leaking upstream diagnostics or following redirects."""

import json
from collections.abc import Mapping

import httpx


class PrivateTransportError(RuntimeError):
    """Safe private-hop failure; no response body or transport exception is retained."""

    def __init__(self, status: int = 503) -> None:
        """Preserve only a safe rejection status for caller error mapping."""
        self.status = status
        super().__init__("private credential transport failed")


async def private_post(
    client: httpx.AsyncClient,
    url: str,
    payload: Mapping[str, object],
    headers: Mapping[str, str],
) -> dict[str, object]:
    """Send one private request with a 1 MiB response limit and a bounded I/O budget."""
    try:
        async with client.stream(
            "POST",
            url,
            json=dict(payload),
            headers=dict(headers),
            timeout=15,
            follow_redirects=False,
        ) as response:
            if response.status_code != 200:
                status = (
                    response.status_code if response.status_code in {401, 403, 410, 429} else 503
                )
                raise PrivateTransportError(status)
            raw = bytearray()
            async for chunk in response.aiter_bytes(chunk_size=16 * 1024):
                if len(raw) + len(chunk) > 1024 * 1024:
                    raise PrivateTransportError()
                raw.extend(chunk)
            result = json.loads(raw)
            if not isinstance(result, dict):
                raise PrivateTransportError()
            return result
    except (httpx.HTTPError, ValueError):
        raise PrivateTransportError() from None
