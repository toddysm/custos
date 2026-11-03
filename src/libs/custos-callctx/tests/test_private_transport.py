"""Private credential responses are bounded, redirect-free and diagnostic-safe."""

import httpx
import pytest

from custos_callctx.private_transport import PrivateTransportError, private_post


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
