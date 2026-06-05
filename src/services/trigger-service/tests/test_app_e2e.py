"""End-to-end ASGI wiring tests for :func:`custos_trigger.create_app` (TS-IMPL-018).

These drive the *fully wired* application — every router mounted, the Dapr
programmatic subscription route live, and collaborators owned by the lifespan —
through a real :class:`fastapi.testclient.TestClient`. The app is booted
sidecar-free by injecting the in-process metadata store and a dispatcher
wrapping the recording :class:`FakeWorkflowServiceClient`, so the manual-fire
and resume-register flows exercise the real pipeline without a Workflow Service
or a Dapr sidecar.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import cast

import pytest
from fastapi.testclient import TestClient

from custos_trigger.app import create_app
from custos_trigger.clients import FakeWorkflowServiceClient
from custos_trigger.dedup import Deduplicator
from custos_trigger.middleware import CALLCTX_HEADER
from custos_trigger.pipeline.dispatch import Dispatcher
from custos_trigger.providers import InMemoryTriggerMetadataStore, Providers
from custos_trigger.settings import (
    DEFAULT_PUBSUB_COMPONENT,
    DEFAULT_WORKFLOW_EVENTS_TOPIC,
)

_WORKSPACE = "ws_demo"
_TRIGGERS = f"/v1/workspaces/{_WORKSPACE}/triggers"
_REGISTER = "/RegisterResumeSubscription"

_ALL_PERMS = [
    "trigger:subscriptions:read",
    "trigger:subscriptions:write",
    "trigger:subscriptions:delete",
    "trigger:subscriptions:fire",
]


def _ctx_header() -> dict[str, str]:
    payload = {
        "workspace_id": _WORKSPACE,
        "principal_id": "user_alice",
        "permissions": _ALL_PERMS,
    }
    return {CALLCTX_HEADER: json.dumps(payload)}


@pytest.fixture
def fake_workflow() -> FakeWorkflowServiceClient:
    return FakeWorkflowServiceClient()


@pytest.fixture
def client(
    providers: Providers,
    metadata_store: InMemoryTriggerMetadataStore,
    fake_workflow: FakeWorkflowServiceClient,
) -> Iterator[TestClient]:
    dispatcher = Dispatcher(fake_workflow, Deduplicator(metadata_store))
    app = create_app(authz_endpoint="", providers=providers, dispatcher=dispatcher)
    with TestClient(app) as test_client:
        yield test_client


def test_dapr_subscribe_lists_workflow_events_topic(client: TestClient) -> None:
    response = client.get("/dapr/subscribe")
    assert response.status_code == 200
    body = cast(list[dict[str, object]], response.json())
    assert body == [
        {
            "pubsubname": DEFAULT_PUBSUB_COMPONENT,
            "topic": DEFAULT_WORKFLOW_EVENTS_TOPIC,
            "route": "/internal/events/workflow",
            "metadata": {},
        }
    ]


def test_manual_fire_starts_a_run(
    client: TestClient,
    fake_workflow: FakeWorkflowServiceClient,
) -> None:
    create = client.post(
        _TRIGGERS,
        json={
            "sourceType": "manual",
            "workflowId": "wf_orders",
            "targetWorkflowVersionId": "wfv_1",
            "inputMapping": {"region": "emea"},
        },
        headers=_ctx_header(),
    )
    assert create.status_code == 201, create.text
    subscription_id = create.json()["subscriptionId"]

    fired = client.post(
        f"{_TRIGGERS}/{subscription_id}:fire",
        json={},
        headers=_ctx_header(),
    )
    assert fired.status_code == 200, fired.text
    assert fired.json() == {"runId": "run-fake"}
    assert len(fake_workflow.start_run_calls) == 1
    assert fake_workflow.start_run_calls[0].workspace_id == _WORKSPACE


def test_register_resume_subscription_round_trips(
    client: TestClient,
    metadata_store: InMemoryTriggerMetadataStore,
) -> None:
    from custos_trigger.api.routes.rpc import RESUME_WORKSPACE, compute_resume_id

    response = client.post(
        _REGISTER,
        json={
            "runId": "run-parent",
            "stepId": "step-wait",
            "eventKey": "workflow.completed",
            "selector": None,
            "ttl": "PT1H",
        },
    )
    assert response.status_code == 200, response.text
    resume_id = compute_resume_id("run-parent", "step-wait", "workflow.completed")
    assert response.json() == {"subscriptionId": resume_id}
    # The registration is persisted under the resume partition.
    row = metadata_store.resume_subscription(RESUME_WORKSPACE, resume_id)
    assert row is not None
    assert str(row.run_id) == "run-parent"
