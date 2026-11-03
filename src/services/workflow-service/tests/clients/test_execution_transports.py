"""Transient execution authority never becomes durable outbound failure diagnostics."""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime

import httpx
import pytest
from custos_callctx.execution import ExecutionBinding, ExecutionBindings, ExecutionIdentity
from custos_callctx.execution_client import ExecutionContextClient

from custos_workflow.clients._dapr_invoke import DaprInvokeEndpoint
from custos_workflow.clients._errors import OutboundRpcError
from custos_workflow.clients.activity_runtime import (
    DaprActivityRuntimeClient,
    ScheduleActivityRequest,
)
from custos_workflow.clients.catalog import DaprCatalogClient
from custos_workflow.clients.connector import BindForStepRequest, DaprConnectorClient, SlotSpec
from custos_workflow.runtime.dapr_activities import _serialize_activity_result_envelope


@pytest.mark.parametrize("kind", ["binding", "activity", "catalog", "activity-metadata"])
@pytest.mark.parametrize("failure", ["transport", "status", "json", "schema"])
async def test_execution_failures_do_not_serialize_transient_authority(
    kind: str, failure: str, caplog: pytest.LogCaptureFixture
) -> None:
    """Exercise real secret/exchange clients and reject credential-bearing upstream diagnostics."""
    identity = ExecutionIdentity("ws", "account", "stable")
    bindings = ExecutionBindings(
        (ExecutionBinding(identity, "credential-ref", "token"),), {"ws": "stable"}
    )
    token = "e30.privateExecutionContext.signature"
    credential = "synthetic-private-execution-credential"
    expected_operation = {
        "binding": "connector-bind",
        "activity": "activity-execute",
        "catalog": "catalog-read",
        "activity-metadata": "catalog-read",
    }[kind]
    captured: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        """Return transient authority, then simulate an upstream that echoes it in an error."""
        captured.append(request)
        if "/v1.0/secrets/" in request.url.path:
            return httpx.Response(200, json={"token": credential})
        if request.url.path.endswith("/rpc/callctx.exchangeExecution"):
            assert json.loads(request.content) == {
                "token": credential,
                "workspaceId": "ws",
                "principalId": "account",
                "operation": (
                    "catalog-activity-read"
                    if kind == "activity-metadata" and len(captured) > 3
                    else expected_operation
                ),
            }
            now = int(time.time())
            return httpx.Response(200, json={"token": token, "iat": now, "exp": now + 300})
        assert request.headers["x-custos-callctx"] == token
        assert token.encode() not in request.content
        assert credential.encode() not in request.content
        if kind == "activity-metadata" and "/workflow-versions/" in request.url.path:
            return httpx.Response(
                200,
                json={
                    "workspaceId": "ws",
                    "workflowName": "flow",
                    "version": 1,
                    "document": {
                        "apiVersion": "custos.dev/v1",
                        "kind": "Workflow",
                        "metadata": {"name": "flow", "workspace": "ws"},
                        "spec": {"steps": [{"id": "step", "activity": "test/echo@1.0.0"}]},
                    },
                },
            )
        if failure == "transport":
            raise httpx.ConnectError(token + credential, request=request)
        if failure == "json":
            return httpx.Response(200, text=token + credential)
        if failure == "schema":
            if kind == "activity":
                return httpx.Response(
                    200, json={"class": token + credential, "attempt": 1, "outputs": {}}
                )
            if kind == "catalog":
                return httpx.Response(
                    200,
                    json={
                        "workspaceId": "ws",
                        "workflowName": "flow",
                        "version": 1,
                        "document": {"steps": token + credential},
                    },
                )
            return httpx.Response(200, json={"schemaVersion": "2", "contexts": token + credential})
        return httpx.Response(503, text=token + credential)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        execution = ExecutionContextClient(http, bindings)
        endpoint = DaprInvokeEndpoint(host="127.0.0.1", http_port=3500, app_id=kind)
        if kind == "activity":
            activity = DaprActivityRuntimeClient(
                http_client=http, endpoint=endpoint, execution_context=execution
            )
            envelope = await activity.schedule_activity(
                ScheduleActivityRequest(
                    run_id="run",
                    step_id="step",
                    attempt=1,
                    activity_ref="test/echo@1",
                    inputs={},
                    connector_contexts={},
                    deadline=datetime(2030, 1, 1, tzinfo=UTC),
                    execution_identity=identity,
                )
            )
            assert envelope.class_ == (
                "retryable" if failure in {"transport", "status"} else "permanent"
            )
            serialized = json.dumps(_serialize_activity_result_envelope(envelope))
        else:
            with pytest.raises(OutboundRpcError) as failed:
                if kind == "binding":
                    connector = DaprConnectorClient(
                        http_client=http, endpoint=endpoint, execution_context=execution
                    )
                    await connector.bind_for_step(
                        BindForStepRequest(
                            step_key="logical",
                            run_id="run",
                            step_id="step",
                            attempt=1,
                            slots=(SlotSpec("slot", "instance", ("objects.read",)),),
                            execution_identity=identity,
                        )
                    )
                else:
                    catalog = DaprCatalogClient(
                        http_client=http, endpoint=endpoint, execution_context=execution
                    )
                    await catalog.get_execution_workflow_version("ws", "ws/flow@1", identity)
            assert failed.value.__cause__ is None
            serialized = str(failed.value)
    assert len(captured) == (6 if kind == "activity-metadata" else 3)
    for private in (token, credential):
        assert private not in serialized
        assert private not in caplog.text
