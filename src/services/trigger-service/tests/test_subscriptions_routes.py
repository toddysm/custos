"""Tests for the manual REST CRUD + ``:fire`` surface (TS-IMPL-015).

The suite drives :mod:`custos_trigger.api.routes.subscriptions` through a real
:class:`fastapi.testclient.TestClient` over the in-process metadata store, with
a dispatcher wrapping the recording :class:`FakeWorkflowServiceClient` so the
``:fire`` path exercises the full normalize -> match -> dispatch pipeline
without a Workflow Service.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import cast

import pytest
from fastapi.testclient import TestClient

from custos_trigger.api.errors import PROBLEM_MEDIA_TYPE
from custos_trigger.app import create_app
from custos_trigger.clients import FakeWorkflowServiceClient
from custos_trigger.dedup import Deduplicator
from custos_trigger.middleware import CALLCTX_HEADER
from custos_trigger.pipeline.dispatch import Dispatcher
from custos_trigger.providers import InMemoryTriggerMetadataStore, Providers
from custos_trigger.stores import SubscriptionReadUnsupportedError, SubscriptionStore
from custos_trigger.stores.base import TriggerMetadataStore

_FROZEN = datetime(2026, 6, 4, 12, 0, 0, tzinfo=UTC)

_ALL_PERMS = [
    "trigger:subscriptions:read",
    "trigger:subscriptions:write",
    "trigger:subscriptions:delete",
    "trigger:subscriptions:fire",
]


def _ctx_header(permissions: list[str]) -> dict[str, str]:
    payload = {
        "workspace_id": "ws_demo",
        "principal_id": "user_alice",
        "permissions": permissions,
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


_TRIGGERS = "/v1/workspaces/ws_demo/triggers"


def _create(client: TestClient, **overrides: object) -> dict[str, object]:
    body: dict[str, object] = {
        "sourceType": "manual",
        "workflowId": "wf_orders",
        "targetWorkflowVersionId": "wfv_1",
        "inputMapping": {"region": "emea"},
    }
    body.update(overrides)
    response = client.post(_TRIGGERS, json=body, headers=_ctx_header(_ALL_PERMS))
    assert response.status_code == 201, response.text
    return cast(dict[str, object], response.json())


def test_full_crud_lifecycle(client: TestClient) -> None:
    created = _create(client, selector="event.data.region == 'emea'")
    subscription_id = created["subscriptionId"]
    assert created["state"] == "active"
    assert created["kind"] == "start"
    assert created["workflowId"] == "wf_orders"

    # Read it back.
    got = client.get(f"{_TRIGGERS}/{subscription_id}", headers=_ctx_header(_ALL_PERMS))
    assert got.status_code == 200
    assert got.json()["subscriptionId"] == subscription_id

    # Patch the selector + target.
    patched = client.patch(
        f"{_TRIGGERS}/{subscription_id}",
        json={"selector": "event.data.region == 'apac'", "targetWorkflowVersionId": "wfv_2"},
        headers=_ctx_header(_ALL_PERMS),
    )
    assert patched.status_code == 200, patched.text
    assert patched.json()["selector"] == "event.data.region == 'apac'"
    assert patched.json()["targetWorkflowVersionId"] == "wfv_2"

    # Soft-delete -> 204, then the row reads back as expired.
    deleted = client.delete(f"{_TRIGGERS}/{subscription_id}", headers=_ctx_header(_ALL_PERMS))
    assert deleted.status_code == 204
    after = client.get(f"{_TRIGGERS}/{subscription_id}", headers=_ctx_header(_ALL_PERMS))
    assert after.status_code == 200
    assert after.json()["state"] == "expired"


def test_patch_state_only(client: TestClient) -> None:
    created = _create(client)
    subscription_id = created["subscriptionId"]
    patched = client.patch(
        f"{_TRIGGERS}/{subscription_id}",
        json={"state": "paused"},
        headers=_ctx_header(_ALL_PERMS),
    )
    assert patched.status_code == 200
    assert patched.json()["state"] == "paused"


def test_patch_can_clear_selector_with_null(client: TestClient) -> None:
    created = _create(client, selector="event.data.region == 'emea'")
    subscription_id = created["subscriptionId"]
    patched = client.patch(
        f"{_TRIGGERS}/{subscription_id}",
        json={"selector": None},
        headers=_ctx_header(_ALL_PERMS),
    )
    assert patched.status_code == 200, patched.text
    assert patched.json().get("selector") is None


def test_patch_can_clear_target_version_with_null(client: TestClient) -> None:
    created = _create(client)
    subscription_id = created["subscriptionId"]
    patched = client.patch(
        f"{_TRIGGERS}/{subscription_id}",
        json={"targetWorkflowVersionId": None},
        headers=_ctx_header(_ALL_PERMS),
    )
    assert patched.status_code == 200, patched.text
    assert patched.json().get("targetWorkflowVersionId") is None


def test_fire_returns_run_id(client: TestClient, fake_workflow: FakeWorkflowServiceClient) -> None:
    created = _create(client, selector="event.data.region == 'emea'")
    subscription_id = created["subscriptionId"]
    response = client.post(
        f"{_TRIGGERS}/{subscription_id}:fire",
        json={"inputs": {"region": "emea"}},
        headers=_ctx_header(_ALL_PERMS),
    )
    assert response.status_code == 200, response.text
    assert response.json() == {"runId": "run-fake"}
    assert len(fake_workflow.start_run_calls) == 1
    assert fake_workflow.start_run_calls[0].inputs == {"region": "emea"}


def test_fire_unconditional_subscription(client: TestClient) -> None:
    created = _create(client, selector=None)
    subscription_id = created["subscriptionId"]
    response = client.post(
        f"{_TRIGGERS}/{subscription_id}:fire",
        json={},
        headers=_ctx_header(_ALL_PERMS),
    )
    assert response.status_code == 200
    assert response.json()["runId"] == "run-fake"


def test_fire_paused_subscription_not_fireable(client: TestClient) -> None:
    created = _create(client)
    subscription_id = created["subscriptionId"]
    client.patch(
        f"{_TRIGGERS}/{subscription_id}",
        json={"state": "paused"},
        headers=_ctx_header(_ALL_PERMS),
    )
    response = client.post(
        f"{_TRIGGERS}/{subscription_id}:fire",
        json={},
        headers=_ctx_header(_ALL_PERMS),
    )
    assert response.status_code == 409
    assert response.headers["content-type"].startswith(PROBLEM_MEDIA_TYPE)
    assert response.json()["code"] == "trigger.api.subscription_not_fireable"


def test_fire_missing_target_version_dead_letters(client: TestClient) -> None:
    created = _create(client, targetWorkflowVersionId=None)
    subscription_id = created["subscriptionId"]
    response = client.post(
        f"{_TRIGGERS}/{subscription_id}:fire",
        json={},
        headers=_ctx_header(_ALL_PERMS),
    )
    assert response.status_code == 502
    body = response.json()
    assert body["code"] == "trigger.dispatch_failed"
    assert body["subscriptionId"] == subscription_id


def test_fire_duplicate_is_rejected(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    # Freeze the fire clock so a redelivered fire with identical inputs derives
    # the same deterministic event id and collapses in the dedup window.
    from custos_trigger.api.routes import subscriptions as routes

    monkeypatch.setattr(routes, "_now", lambda: _FROZEN)
    created = _create(client, selector=None)
    subscription_id = created["subscriptionId"]

    first = client.post(
        f"{_TRIGGERS}/{subscription_id}:fire",
        json={"inputs": {"k": "v"}},
        headers=_ctx_header(_ALL_PERMS),
    )
    assert first.status_code == 200
    second = client.post(
        f"{_TRIGGERS}/{subscription_id}:fire",
        json={"inputs": {"k": "v"}},
        headers=_ctx_header(_ALL_PERMS),
    )
    assert second.status_code == 409
    assert second.json()["code"] == "trigger.dedup_duplicate"


def test_invalid_selector_rejected_on_create(client: TestClient) -> None:
    response = client.post(
        _TRIGGERS,
        json={
            "sourceType": "manual",
            "workflowId": "wf_orders",
            "targetWorkflowVersionId": "wfv_1",
            "selector": "this is not valid !!! cel",
        },
        headers=_ctx_header(_ALL_PERMS),
    )
    assert response.status_code == 422
    assert response.headers["content-type"].startswith(PROBLEM_MEDIA_TYPE)
    assert response.json()["code"] == "trigger.selector_invalid"


def test_invalid_selector_rejected_on_patch(client: TestClient) -> None:
    created = _create(client)
    subscription_id = created["subscriptionId"]
    response = client.patch(
        f"{_TRIGGERS}/{subscription_id}",
        json={"selector": "!!! nope"},
        headers=_ctx_header(_ALL_PERMS),
    )
    assert response.status_code == 422
    assert response.json()["code"] == "trigger.selector_invalid"


def test_get_unknown_subscription_404(client: TestClient) -> None:
    response = client.get(f"{_TRIGGERS}/does-not-exist", headers=_ctx_header(_ALL_PERMS))
    assert response.status_code == 404
    assert response.json()["code"] == "trigger.subscription_not_found"


def test_patch_unknown_subscription_404(client: TestClient) -> None:
    response = client.patch(
        f"{_TRIGGERS}/nope",
        json={"state": "paused"},
        headers=_ctx_header(_ALL_PERMS),
    )
    assert response.status_code == 404


def test_delete_unknown_subscription_404(client: TestClient) -> None:
    response = client.delete(f"{_TRIGGERS}/nope", headers=_ctx_header(_ALL_PERMS))
    assert response.status_code == 404


def test_fire_unknown_subscription_404(client: TestClient) -> None:
    response = client.post(f"{_TRIGGERS}/nope:fire", json={}, headers=_ctx_header(_ALL_PERMS))
    assert response.status_code == 404


def test_missing_permission_forbidden(client: TestClient) -> None:
    response = client.post(
        _TRIGGERS,
        json={"sourceType": "manual", "workflowId": "wf_orders"},
        headers=_ctx_header(["trigger:subscriptions:read"]),
    )
    assert response.status_code == 403


def test_missing_call_context_unauthorized(client: TestClient) -> None:
    response = client.get(f"{_TRIGGERS}/anything")
    assert response.status_code == 401


def test_workspace_mismatch_forbidden(client: TestClient) -> None:
    # The call context names ws_demo but the path targets ws_other.
    response = client.post(
        "/v1/workspaces/ws_other/triggers",
        json={
            "sourceType": "manual",
            "workflowId": "wf_orders",
            "targetWorkflowVersionId": "wfv_1",
        },
        headers=_ctx_header(_ALL_PERMS),
    )
    assert response.status_code == 403
    assert response.json()["error"]["code"] == "workspace_mismatch"


def test_malformed_body_is_bad_request(client: TestClient) -> None:
    response = client.post(_TRIGGERS, json={"workflowId": ""}, headers=_ctx_header(_ALL_PERMS))
    assert response.status_code in (400, 422)
    assert response.json()["code"] in ("trigger.api.bad_request", "trigger.selector_invalid")


def test_subscription_store_get_requires_readable_backend() -> None:
    write_only = cast(TriggerMetadataStore, object())
    store = SubscriptionStore(write_only)
    with pytest.raises(SubscriptionReadUnsupportedError):
        import asyncio

        asyncio.run(store.get("ws_demo", "sub_1"))


def test_read_unsupported_renders_problem_501() -> None:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient as _TestClient

    from custos_trigger.api import register_exception_handlers

    app = FastAPI()
    register_exception_handlers(app)

    @app.get("/boom")
    async def _boom() -> dict[str, str]:
        raise SubscriptionReadUnsupportedError("backend has no read surface")

    with _TestClient(app) as test_client:
        response = test_client.get("/boom")
    assert response.status_code == 501
    assert response.headers["content-type"].startswith(PROBLEM_MEDIA_TYPE)
    assert response.json()["code"] == "trigger.api.subscription_read_unsupported"
