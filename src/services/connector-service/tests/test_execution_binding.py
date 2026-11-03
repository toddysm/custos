"""Canonical binding authorization, authoritative metadata and actor-isolated caching."""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import replace
from datetime import UTC, datetime

import httpx
import pytest
from custos_callctx.execution import ExecutionBinding, ExecutionBindings, ExecutionIdentity
from custos_callctx.execution_client import ExecutionContextClient
from custos_cel.clock import FixedClock
from custos_workflow.bindings.registry import ResolvedActivityTypeRegistry
from custos_workflow.clients._dapr_invoke import DaprInvokeEndpoint
from custos_workflow.clients._errors import OutboundRpcDecodeError
from custos_workflow.clients.activity_runtime import (
    ActivityResultEnvelope,
    FakeActivityRuntimeClient,
)
from custos_workflow.clients.connector import (
    BindForStepRequest,
    BoundConnectorContext,
    DaprConnectorClient,
    FakeConnectorClient,
    SlotSpec,
)
from custos_workflow.compiler import RunMeta
from custos_workflow.compiler import compile as compile_workflow
from custos_workflow.document import WorkflowDocument
from custos_workflow.graph import ExecutionGraph
from custos_workflow.graph.serialize import from_json, to_json
from custos_workflow.runs import RunId, StepExecutionContext, StepSucceeded
from custos_workflow.runtime import FakeWorkflowContext
from custos_workflow.runtime.dapr_activities import BindForStepCallToken, ScheduleActivityCallToken
from custos_workflow.steps.activity_step import ActivityStepHandler
from fastapi.testclient import TestClient

from custos_connector import create_app
from custos_connector.middleware import CALLCTX_HEADER
from tests._fakes import FakeCatalogAdapter, FakeConnectorInstanceAdapter, StubPluginBinder
from tests.test_binding_router import (
    _BASE_SETTINGS,
    _build_providers,
    _make_instance,
    _payload,
    _seed,
)
from tests.test_signed_callctx import Signing
from tests.test_signed_callctx import signing as signing


@pytest.mark.parametrize("operation", [None, "connector-bind"])
def test_execution_binding_requires_issuance_provenance(
    signing: Signing, operation: str | None
) -> None:
    """A real legacy signature with the same grant cannot replace credential-backed issuance."""
    catalog = FakeCatalogAdapter(applied_revisions={1, 2})
    instances = FakeConnectorInstanceAdapter(applied_revisions={1})
    instance = _make_instance()
    asyncio.run(_seed(catalog, instances, instance))
    binder = StubPluginBinder()
    app = create_app(
        settings=replace(
            _BASE_SETTINGS,
            authz_endpoint="http://auth",
            app_api_token="local-test-token",
            workflow_namespace="custos",
        ),
        providers=_build_providers(catalog=catalog, instances=instances, plugin_binder=binder),
        call_context_verifier=signing.verifier,
    )
    claims: dict[str, object] = {
        "workspaceId": "ws-1",
        "permissions": ["connector:bind"],
        "callerComponent": "workflow-service",
    }
    if operation is not None:
        claims["executionOperation"] = operation
    headers = {
        CALLCTX_HEADER: signing.token(claims),
        "dapr-api-token": "local-test-token",
        "dapr-caller-app-id": "workflow-service",
        "dapr-caller-namespace": "custos",
    }
    with TestClient(app) as client:
        body = {**_payload(instance), "contextVersion": 2}
        response = client.post("/internal/v1/bind-for-step", json=body, headers=headers)
        if operation is None:
            assert response.status_code == 403
            assert not binder.calls
            return
        assert response.status_code == 200, response.text
        wire = response.json()
        assert wire["schemaVersion"] == "2"
        context = wire["contexts"]["source"]
        assert context["connectorInstanceId"] == str(instance.instance_id)
        assert context["connectorKind"] == instance.type
        assert context["capabilities"] == ["events.delivery"]
        assert isinstance(context["context"]["handle"], dict)
        assert not {"leaseId", "expiresAt"} & set(context)
        assert client.post("/internal/v1/bind-for-step", json=body, headers=headers).json() == wire
        assert len(binder.calls) == 1
        another_actor = signing.token({**claims, "actingPrincipalId": "another-executor"})
        assert (
            client.post(
                "/internal/v1/bind-for-step",
                json=body,
                headers={**headers, CALLCTX_HEADER: another_actor},
            ).status_code
            == 200
        )
        assert len(binder.calls) == 2
        assert (
            client.post(
                "/internal/v1/bind-for-step",
                json=body,
                headers={**headers, "dapr-caller-namespace": "other"},
            ).status_code
            == 403
        )
        assert len(binder.calls) == 2
        schemas = app.openapi()["components"]["schemas"]
        assert schemas["_ExecutionBindResponseWire"]["properties"]["schemaVersion"]["const"] == "2"


def _compiled_producer(
    identity: ExecutionIdentity, instance_id: str, kind: str, *, connectorless: bool
) -> tuple[ActivityStepHandler, StepExecutionContext, ExecutionGraph]:
    """Compile and replay authoritative manifest facts before invoking the owning step handler."""
    ref = "test/echo@1.0.0"
    metadata = {"namespace": "test", "type": "echo", "version": "1.0.0"}
    registry = ResolvedActivityTypeRegistry(
        {
            ref: {
                **metadata,
                "normalizedManifest": {
                    "metadata": metadata,
                    "spec": {
                        "outputs": {"schema": {"type": "object"}},
                        "connectors": []
                        if connectorless
                        else [
                            {
                                "name": "source",
                                "type": kind,
                                "required": True,
                                "capabilities": ["events.delivery"],
                            }
                        ],
                    },
                },
            }
        }
    )
    document = WorkflowDocument.model_validate(
        {
            "apiVersion": "custos.dev/v1",
            "kind": "Workflow",
            "metadata": {"name": "flow", "workspace": identity.workspace_id},
            "spec": {
                "steps": [
                    {
                        "id": "step",
                        "activity": ref,
                        **({} if connectorless else {"connector": instance_id}),
                    }
                ]
            },
        }
    )
    now = datetime.now(UTC)
    graph = from_json(
        to_json(
            compile_workflow(
                document,
                RunMeta(identity.workspace_id, "version", "flow", "1", now),
                registry,
            )
        )
    )
    ctx = StepExecutionContext(
        run_id=RunId("actual-run"),
        workspace_id=identity.workspace_id,
        workflow_version_id="version",
        inputs={},
        outputs={},
        workflow_context=FakeWorkflowContext(instance_id="actual-run", now=now),
        clock=FixedClock(now),
        execution_identity=identity,
    )
    return ActivityStepHandler(FakeActivityRuntimeClient(), FakeConnectorClient()), ctx, graph


@pytest.mark.parametrize(
    "mutation",
    [None, "instance", "capabilities", "kind", "slot", "schema", "producer", "connectorless"],
)
async def test_real_workflow_client_uses_actual_connector_contract(
    signing: Signing, mutation: str | None
) -> None:
    """Route the real caller through the real receiver, simulating only sidecar and plugin I/O."""
    catalog = FakeCatalogAdapter(applied_revisions={1, 2})
    instances = FakeConnectorInstanceAdapter(applied_revisions={1})
    instance = _make_instance()
    await _seed(catalog, instances, instance)
    binder = StubPluginBinder()
    app = create_app(
        settings=replace(
            _BASE_SETTINGS,
            authz_endpoint="http://auth",
            app_api_token="unit-app-token",
            workflow_namespace="custos",
        ),
        providers=_build_providers(catalog=catalog, instances=instances, plugin_binder=binder),
        call_context_verifier=signing.verifier,
    )
    identity = ExecutionIdentity("ws-1", "executor", "stable")
    bindings = ExecutionBindings(
        (ExecutionBinding(identity, "credential-ref", "token"),), {"ws-1": "stable"}
    )
    issued: list[str] = []
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://connector"
        ) as receiver,
    ):

        async def sidecar(request: httpx.Request) -> httpx.Response:
            """Simulate secret/exchange APIs and authenticated sidecar forwarding, not binding."""
            if "/v1.0/secrets/" in request.url.path:
                return httpx.Response(200, json={"token": "unit-account-credential"})
            if request.url.path.endswith("/rpc/callctx.exchangeExecution"):
                assert json.loads(request.content) == {
                    "token": "unit-account-credential",
                    "workspaceId": "ws-1",
                    "principalId": "executor",
                    "operation": "connector-bind",
                }
                token = signing.token(
                    {
                        "workspaceId": "ws-1",
                        "actingPrincipalId": "executor",
                        "callerComponent": "workflow-service",
                        "permissions": ["connector:bind"],
                        "executionOperation": "connector-bind",
                        "jti": str(len(issued)),
                    }
                )
                issued.append(token)
                now = int(time.time())
                return httpx.Response(200, json={"token": token, "iat": now, "exp": now + 300})
            assert request.url.path == (
                "/v1.0/invoke/connector-service/method/internal/v1/bind-for-step"
            )
            wire = json.loads(request.content)
            assert (wire["runId"], wire["stepId"], wire["attempt"]) == (
                "actual-run",
                "step",
                1 if mutation == "producer" else 3,
            )
            assert wire["contextVersion"] == 2
            response = await receiver.post(
                "/internal/v1/bind-for-step",
                content=request.content,
                headers={
                    **request.headers,
                    "dapr-api-token": "unit-app-token",
                    "dapr-caller-app-id": "workflow-service",
                    "dapr-caller-namespace": "custos",
                },
            )
            assert response.status_code == 200, response.text
            body = response.json()
            context = body["contexts"]["source"]
            if mutation == "instance":
                context["connectorInstanceId"] = "different-instance"
            elif mutation == "capabilities":
                context["capabilities"] = ["objects.read"]
            elif mutation == "kind":
                context["connectorKind"] = "different-kind"
            elif mutation == "slot":
                context["slotName"] = "different-slot"
            elif mutation == "schema":
                body["schemaVersion"] = "3"
            return httpx.Response(200, json=body)

        async with httpx.AsyncClient(transport=httpx.MockTransport(sidecar)) as http:
            client = DaprConnectorClient(
                http_client=http,
                endpoint=DaprInvokeEndpoint("127.0.0.1", 3500, "connector-service"),
                execution_context=ExecutionContextClient(http, bindings),
            )
            if mutation in {"producer", "connectorless"}:
                handler, ctx, graph = _compiled_producer(
                    identity,
                    str(instance.instance_id),
                    instance.type,
                    connectorless=mutation == "connectorless",
                )
                calls = handler.iter_calls(ctx, graph, "step")
                token = next(calls)
                if mutation == "producer":
                    assert isinstance(token, BindForStepCallToken)
                    token = calls.send(await client.bind_for_step(token.request))
                    assert len(binder.calls) == 1
                else:
                    assert not binder.calls and not issued
                assert isinstance(token, ScheduleActivityCallToken)
                assert bool(token.request.connector_contexts) == (mutation == "producer")
                with pytest.raises(StopIteration) as finished:
                    calls.send(ActivityResultEnvelope("success", {}, None, 1))
                assert isinstance(finished.value.value, StepSucceeded)
                return
            request = BindForStepRequest(
                step_key="logical",
                run_id="actual-run",
                step_id="step",
                attempt=3,
                slots=(
                    SlotSpec(
                        "source", str(instance.instance_id), ("events.delivery",), instance.type
                    ),
                ),
                execution_identity=identity,
            )
            if mutation is not None:
                with pytest.raises(OutboundRpcDecodeError):
                    await client.bind_for_step(request)
                return
            for _ in range(2):
                result = await client.bind_for_step(request)
                bound = result.contexts["source"]
                assert isinstance(bound, BoundConnectorContext)
                assert bound.connector_instance_id == str(instance.instance_id)
                assert isinstance(bound.to_wire()["context"]["handle"], dict)
                serialized = json.dumps(bound.to_wire())
                assert all(token not in serialized for token in issued)
                assert "unit-account-credential" not in serialized
            assert len(issued) == 2 and issued[0] != issued[1]
            assert len(binder.calls) == 1
