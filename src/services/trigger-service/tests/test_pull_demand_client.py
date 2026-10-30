"""Trigger-owned reconciliation commands retain mesh identity and explicit failure semantics."""

import json

import httpx
import pytest

from custos_trigger.clients.pull_demand import DaprPullDemandClient
from custos_trigger.clients.workflow import DaprEndpoint
from custos_trigger.errors import TriggerError, TriggerErrorKind


@pytest.mark.parametrize("registered", [0, 3])
async def test_reconcile_sends_only_workspace_intent_hint(registered: int) -> None:
    """Zero registrations is an acknowledged result, not a fallback for transport failure."""

    def handle(request: httpx.Request) -> httpx.Response:
        """Check exact versioned addressing and independent sidecar authentication."""
        assert request.url.path == "/v1.0/invoke/connector-service/method/ReconcilePullDemand"
        assert json.loads(request.content) == {"schemaVersion": "1", "workspaceId": "workspace"}
        assert request.headers["dapr-api-token"] == "unit-sidecar-api"
        assert "x-custos-callctx" not in request.headers and "authorization" not in request.headers
        assert request.extensions["timeout"]["read"] == 70
        return httpx.Response(200, json={"schemaVersion": "1", "registered": registered})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        client = DaprPullDemandClient(
            http,
            DaprEndpoint("127.0.0.1", 3500, "connector-service"),
            api_token="unit-sidecar-api",
        )
        assert await client.reconcile("workspace") == registered
        assert not http.is_closed


@pytest.mark.parametrize("failure", ["network", "status", "shape", "size"])
async def test_reconcile_failures_are_explicit(failure: str) -> None:
    """Malformed/private transport failures cannot become successful empty reconciliation."""

    def handle(_request: httpx.Request) -> httpx.Response:
        """Return one controlled failure without exposing real credentials or payloads."""
        if failure == "network":
            raise httpx.ConnectError("unavailable")
        if failure == "status":
            return httpx.Response(503)
        if failure == "size":
            return httpx.Response(200, content=b"x" * 16385)
        return httpx.Response(200, json={"schemaVersion": "2", "registered": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        client = DaprPullDemandClient(http, DaprEndpoint("127.0.0.1", 3500, "connector-service"))
        with pytest.raises(TriggerError) as error:
            await client.reconcile("workspace")
        assert error.value.kind is TriggerErrorKind.SOURCE_UNAVAILABLE
