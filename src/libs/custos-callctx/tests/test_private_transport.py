"""Private credential responses are bounded, redirect-free and diagnostic-safe."""

import asyncio
import time
from collections.abc import AsyncIterator

import httpx
import pytest

from custos_callctx import private_transport
from custos_callctx.private_transport import PrivateTransportError, private_post


async def test_continuous_response_progress_still_has_a_total_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Frequent small chunks cannot extend the complete private response budget."""
    monkeypatch.setattr(private_transport, "PRIVATE_REQUEST_TIMEOUT_SECONDS", 0.05)

    class Trickle(httpx.AsyncByteStream):
        """Progress faster than a read timeout, but longer than the overall deadline."""

        closed = False

        async def __aiter__(self) -> AsyncIterator[bytes]:
            """Produce valid JSON slowly enough to exceed the elapsed-time budget."""
            yield b'{"value":"'
            for _ in range(100):
                await asyncio.sleep(0.01)
                yield b"x"
            yield b'"}'

        async def aclose(self) -> None:
            """Record that timeout closes the private response stream."""
            self.closed = True

    body = Trickle()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, stream=body)),
    ) as http:
        started = time.monotonic()
        with pytest.raises(PrivateTransportError) as error:
            await private_post(http, "https://broker", {}, {})
        assert time.monotonic() - started < 0.5
        assert error.value.__cause__ is None and body.closed


@pytest.mark.parametrize(
    "failure", [None, "transport", "redirect", "denied", "json", "shape", "oversize"]
)
async def test_private_post_retains_only_safe_failures(failure: str | None) -> None:
    """Untrusted private response bodies and request exceptions never become public diagnostics."""
    secret = "private-credential-fixture"
    calls = 0

    def respond(request: httpx.Request) -> httpx.Response:
        """Return one controlled boundary result without replacing the private transport itself."""
        nonlocal calls
        calls += 1
        assert request.headers["authorization"] == "Bearer " + secret
        if failure == "transport":
            raise httpx.ConnectError(secret, request=request)
        if failure == "redirect":
            return httpx.Response(307, headers={"location": "https://other.invalid"})
        if failure == "denied":
            return httpx.Response(403, text=secret)
        if failure == "json":
            return httpx.Response(200, text=secret)
        if failure == "shape":
            return httpx.Response(200, json=[secret])
        if failure == "oversize":
            return httpx.Response(200, content=b"x" * (1024 * 1024 + 1))
        return httpx.Response(200, json={"material": {"secret": secret}, "expiresAt": None})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        if failure is None:
            result = await private_post(
                http, "https://broker", {}, {"authorization": "Bearer " + secret}
            )
            assert result == {"material": {"secret": secret}, "expiresAt": None}
        else:
            with pytest.raises(PrivateTransportError) as failed:
                await private_post(
                    http, "https://broker", {}, {"authorization": "Bearer " + secret}
                )
            assert failed.value.__cause__ is None
            assert secret not in str(failed.value)
            assert failed.value.status == (403 if failure == "denied" else 503)
    assert calls == 1
