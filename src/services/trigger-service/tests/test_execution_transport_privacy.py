"""Execution delivery failures must not disclose transient credentials or signed authority."""

import time
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from custos_callctx.execution import ExecutionBinding, ExecutionBindings, ExecutionIdentity
from custos_callctx.execution_client import ExecutionContextClient

from custos_trigger.clients.workflow import (
    DaprEndpoint,
    DaprWorkflowServiceClient,
    RaiseExternalEventRequest,
    StartRunRequest,
    WorkflowClientError,
)


@pytest.mark.parametrize("operation", ["start", "resume"])
@pytest.mark.parametrize("failure", ["transport", "status", "json", "schema"])
async def test_execution_delivery_errors_are_private(
    operation: str, failure: str, caplog: pytest.LogCaptureFixture
) -> None:
    """Run real credential exchange and an upstream that echoes its private headers."""
    identity = ExecutionIdentity("ws", "account", "stable")
    registry = ExecutionBindings(
        (ExecutionBinding(identity, "credential-ref", "token"),), {"ws": "stable"}
    )
    token = "e30.privateExecutionContext.signature"
    credential = "synthetic-private-execution-credential"

    def respond(request: httpx.Request) -> httpx.Response:
        """Supply authority, then inject one concrete outbound failure."""
        if "/v1.0/secrets/" in request.url.path:
            return httpx.Response(200, json={"token": credential})
        if request.url.path.endswith("/rpc/callctx.exchangeExecution"):
            now = int(time.time())
            return httpx.Response(200, json={"token": token, "iat": now, "exp": now + 300})
        assert request.headers["x-custos-callctx"] == token
        if failure == "transport":
            raise httpx.ConnectError(token + credential, request=request)
        if failure == "status":
            return httpx.Response(503, text=token + credential)
        if failure == "json":
            return httpx.Response(200, text=token + credential)
        return httpx.Response(
            200,
            json={
                "runId": "run",
                "workspaceId": "ws",
                "workflowVersionId": "version",
                "status": "queued",
                "startedAt": token + credential,
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        client = DaprWorkflowServiceClient(
            http_client=http,
            endpoint=DaprEndpoint(host="127.0.0.1", http_port=3500, app_id="workflow"),
            execution_context=ExecutionContextClient(http, registry),
        )
        with pytest.raises(WorkflowClientError) as failed:
            if operation == "start":
                await client.start_run(
                    StartRunRequest(
                        workspace_id="ws",
                        workflow_version_id="version",
                        delivery_protocol=2,
                        idempotency_key="key",
                        retain_until=datetime.now(UTC) + timedelta(days=1),
                        execution_identity=identity.to_dict(),
                    )
                )
            else:
                await client.raise_external_event(
                    "run",
                    "wait",
                    RaiseExternalEventRequest(
                        workspace_id="ws",
                        event_name="ready",
                        delivery_protocol=2,
                        idempotency_key="key",
                        retain_until=datetime.now(UTC) + timedelta(days=1),
                        execution_identity=identity.to_dict(),
                    ),
                )
        assert failed.value.__cause__ is None
        for private in (token, credential):
            assert private not in str(failed.value)
            assert private not in caplog.text
