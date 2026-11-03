"""Pinned Catalog activity facts reach compilation and durable connector-binding requests."""

import json
import time
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
from custos_callctx.execution import ExecutionBinding, ExecutionBindings, ExecutionIdentity
from custos_callctx.execution_client import ExecutionContextClient
from custos_cel.clock import FixedClock

from custos_workflow.bindings.registry import ResolvedActivityTypeRegistry
from custos_workflow.clients._dapr_invoke import DaprInvokeEndpoint
from custos_workflow.clients.activity_runtime import FakeActivityRuntimeClient
from custos_workflow.clients.catalog import DaprCatalogClient
from custos_workflow.clients.connector import FakeConnectorClient
from custos_workflow.compiler import RunMeta
from custos_workflow.compiler import compile as compile_workflow
from custos_workflow.graph.serialize import from_json, to_json
from custos_workflow.runs import RunId, StepExecutionContext
from custos_workflow.runtime import FakeWorkflowContext
from custos_workflow.runtime.dapr_activities import (
    BindForStepCallToken,
    ScheduleActivityCallToken,
    _deserialize_slot_spec,
    _serialize_slot_spec,
)
from custos_workflow.steps.activity_step import ActivityStepHandler


def activity_body(*, connector: bool = True) -> dict[str, Any]:
    """Return the actual Catalog activity response shape, not pre-built SlotSpecs."""
    metadata = {"namespace": "test", "type": "echo", "version": "1.0.0"}
    return {
        **metadata,
        "normalizedManifest": {
            "metadata": metadata,
            "spec": {
                "outputs": {"schema": {"type": "object"}},
                "connectors": [
                    {
                        "name": "source",
                        "type": "registry",
                        "required": True,
                        "capabilities": ["oci.pull"],
                    }
                ]
                if connector
                else [],
            },
        },
    }


@pytest.mark.parametrize("connector", [False, True])
async def test_real_catalog_reader_compiler_replay_and_step_producer(connector: bool) -> None:
    """Obtain fresh authority per read and retain exact slot facts through graph serialization."""
    identity = ExecutionIdentity("ws", "account", "stable")
    bindings = ExecutionBindings(
        (ExecutionBinding(identity, "credential", "token"),), {"ws": "stable"}
    )
    operations: list[str] = []
    now = datetime.now(UTC)

    def respond(request: httpx.Request) -> httpx.Response:
        """Emulate Catalog/secret transport while running the actual caller and compiler."""
        if "/v1.0/secrets/" in request.url.path:
            return httpx.Response(200, json={"token": "private-credential"})
        if request.url.path.endswith("/rpc/callctx.exchangeExecution"):
            operation = json.loads(request.content)["operation"]
            operations.append(operation)
            timestamp = int(time.time())
            return httpx.Response(
                200,
                json={
                    "token": f"e30.{operation}.signature",
                    "iat": timestamp,
                    "exp": timestamp + 300,
                },
            )
        if "/activity-types/" in request.url.path:
            assert request.headers["x-custos-callctx"] == "e30.catalog-activity-read.signature"
            assert request.url.path.endswith("/v1/workspaces/ws/activity-types/test/echo@1.0.0")
            return httpx.Response(200, json=activity_body(connector=connector))
        assert request.headers["x-custos-callctx"] == "e30.catalog-read.signature"
        step: dict[str, Any] = {"id": "step", "activity": "test/echo@1.0.0"}
        if connector:
            step["connector"] = "instance-id"
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
                    "spec": {"steps": [step]},
                },
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        client = DaprCatalogClient(
            http,
            DaprInvokeEndpoint("127.0.0.1", 3500, "catalog"),
            execution_context=ExecutionContextClient(http, bindings),
        )
        version = await client.get_execution_workflow_version("ws", "ws/flow@1", identity)
    assert operations == ["catalog-read", "catalog-activity-read"]
    assert version.activity_registry is not None
    graph = compile_workflow(
        version.document,
        RunMeta("ws", "ws/flow@1", "flow", "1", now),
        version.activity_registry,
    )
    graph = from_json(to_json(graph))
    ctx = StepExecutionContext(
        run_id=RunId("run"),
        workspace_id="ws",
        workflow_version_id="ws/flow@1",
        inputs={},
        outputs={},
        workflow_context=FakeWorkflowContext(instance_id="run", now=now),
        clock=FixedClock(now),
        execution_identity=identity,
    )
    calls = ActivityStepHandler(FakeActivityRuntimeClient(), FakeConnectorClient()).iter_calls(
        ctx, graph, "step"
    )
    first = next(calls)
    if connector:
        assert isinstance(first, BindForStepCallToken)
        slot = first.request.slots[0]
        assert (slot.name, slot.connector_ref, slot.capabilities, slot.connector_kind) == (
            "source",
            "instance-id",
            ("oci.pull",),
            "registry",
        )
        assert _deserialize_slot_spec(_serialize_slot_spec(slot)) == slot
    else:
        assert isinstance(first, ScheduleActivityCallToken)
        assert first.request.connector_contexts == {}
    calls.close()


@pytest.mark.parametrize("mutation", ["reference", "metadata", "outputs", "duplicate", "boolean"])
def test_activity_metadata_rejects_untrusted_shapes(mutation: str) -> None:
    """Do not fabricate schemas or slot authority when Catalog returns malformed metadata."""
    body = activity_body()
    if mutation == "reference":
        body["version"] = "2.0.0"
    elif mutation == "metadata":
        body["normalizedManifest"]["metadata"]["type"] = "other"
    elif mutation == "outputs":
        body["normalizedManifest"]["spec"]["outputs"] = None
    elif mutation == "duplicate":
        body["normalizedManifest"]["spec"]["connectors"] *= 2
    else:
        body["normalizedManifest"]["spec"]["connectors"][0]["required"] = "true"
    with pytest.raises(ValueError):
        ResolvedActivityTypeRegistry({"test/echo@1.0.0": body})
