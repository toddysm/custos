"""Private credential exchange preserves identity and strips dependency diagnostics."""

import json
import time

import httpx
import pytest

from custos_callctx.execution import ExecutionBinding, ExecutionBindings, ExecutionIdentity
from custos_callctx.execution_client import ExecutionAuthorizationError, ExecutionContextClient

IDENTITY = ExecutionIdentity("ws", "account", "stable")
BINDINGS = ExecutionBindings(
    (ExecutionBinding(IDENTITY, "credential-reference", "token"),), {"ws": "stable"}
)


async def test_every_request_resolves_rotated_credentials_and_a_fresh_context() -> None:
    """Neither credential material nor issued contexts are cached in the stable registry."""
    seen: list[dict[str, str]] = []
    generation = 0

    def respond(request: httpx.Request) -> httpx.Response:
        """Return observable rotating synthetic values on the same deployment-owned origin."""
        nonlocal generation
        assert request.url.host == "127.0.0.1"
        assert request.headers["dapr-api-token"] == "unit-sidecar-api"
        if request.method == "GET":
            generation += 1
            return httpx.Response(200, json={"token": f"synthetic-credential-{generation}"})
        seen.append(json.loads(request.content))
        now = int(time.time())
        return httpx.Response(
            200,
            json={
                "token": f"e30.context-{generation}.signature",
                "iat": now,
                "exp": now + 300,
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        client = ExecutionContextClient(http, BINDINGS, api_token="unit-sidecar-api")
        for index in (1, 2):
            headers = await client.headers(IDENTITY, "connector-bind")
            assert headers["x-custos-callctx"] == f"e30.context-{index}.signature"
            assert seen[-1] == {
                "token": f"synthetic-credential-{index}",
                "workspaceId": "ws",
                "principalId": "account",
                "operation": "connector-bind",
            }
    assert set(IDENTITY.to_dict()) == {"workspaceId", "principalId", "bindingId"}


@pytest.mark.parametrize(
    "failure,unavailable",
    [
        ("denied", False),
        ("outage", True),
        ("redirect", False),
        ("missing-credential", False),
        ("expired", False),
        ("oversized", False),
        ("transport", True),
    ],
)
async def test_failed_exchange_is_explicit_private_and_does_not_follow_redirects(
    failure: str,
    unavailable: bool,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Only the fixed sidecar is contacted; errors never expose credential-bearing causes."""
    contacted: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        """Inject each concrete failure after recording its destination."""
        contacted.append(request.url.host)
        if failure == "transport":
            raise httpx.ConnectError("synthetic-private-diagnostic", request=request)
        if failure == "missing-credential":
            return httpx.Response(200, json={"wrong": "synthetic-private-value"})
        if failure == "oversized":
            return httpx.Response(200, content=b"x" * (32 * 1024 + 1))
        if request.method == "GET":
            return httpx.Response(200, json={"token": "synthetic-private-credential"})
        if failure == "redirect":
            return httpx.Response(307, headers={"location": "https://unrelated.example/collect"})
        if failure in ("denied", "outage"):
            return httpx.Response(403 if failure == "denied" else 503, text="private-value")
        now = int(time.time())
        return httpx.Response(
            200, json={"token": "private-value", "iat": now - 400, "exp": now - 1}
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(respond), follow_redirects=True
    ) as http:
        client = ExecutionContextClient(http, BINDINGS)
        with pytest.raises(ExecutionAuthorizationError) as failed:
            await client.headers(IDENTITY, "connector-bind")
        assert failed.value.unavailable is unavailable
        assert "private" not in str(failed.value)
        assert failed.value.__cause__ is None
    assert set(contacted) == {"127.0.0.1"} and "private" not in caplog.text


async def test_changed_identity_is_rejected_before_secret_access() -> None:
    """A retained reference cannot acquire the current workspace default's different credential."""

    def unexpected(request: httpx.Request) -> httpx.Response:
        """Any network activity would mean the mismatched identity escaped the local gate."""
        pytest.fail("unexpected credential lookup")

    async with httpx.AsyncClient(transport=httpx.MockTransport(unexpected)) as http:
        with pytest.raises(ExecutionAuthorizationError):
            await ExecutionContextClient(http, BINDINGS).headers(
                ExecutionIdentity("ws", "changed", "stable"),
                "connector-bind",
            )
