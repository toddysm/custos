"""Webhook contracts with actual HTTP clients, routing and durable-source gates."""

from __future__ import annotations

import base64
import json
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient

from custos_trigger.app import create_app
from custos_trigger.clients import FakeWorkflowServiceClient
from custos_trigger.clients.workflow import DaprEndpoint
from custos_trigger.dependencies import WorkflowCallerVerifier
from custos_trigger.providers import InMemoryTriggerMetadataStore, Providers
from custos_trigger.receivers.webhook import (
    MAX_HEADER_BYTES,
    MAX_RAW_BYTES,
    MAX_RESPONSE_BYTES,
    DaprWebhookTransport,
    VerifiedBatch,
)
from custos_trigger.settings import SettingsError, read_webhook_receiver_enabled
from tests._source_helpers import NOW, connector_event, dispatcher

PATH = "/v1/webhooks/workspaces/source-a/connectors/same/events"
MESH = {
    "dapr-api-token": "unit-webhook-sidecar",
    "dapr-caller-app-id": "api-gateway",
    "dapr-caller-namespace": "custos",
}


def verified_batch() -> dict[str, Any]:
    """Provide one unchanged Connector envelope in an exact indexed verifier partition."""
    return {
        "schemaVersion": "1",
        "receivedCount": 1,
        "events": [{"index": 0, "event": connector_event()}],
        "rejections": [],
    }


@dataclass
class WebhookHarness:
    """Controllable verifier/broker transport and a sanitized recording audit sink."""

    reply: dict[str, Any] = field(default_factory=verified_batch)
    status: int = 200
    fail_publish_at: int = 0
    network_failure: bool = False
    publish_network_failure: bool = False
    raw_reply: bytes | None = None
    verified_requests: list[dict[str, Any]] = field(default_factory=list)
    published: list[dict[str, Any]] = field(default_factory=list)
    audit: list[dict[str, Any]] = field(default_factory=list)

    async def handle(self, request: httpx.Request) -> httpx.Response:
        """Record exact private wire data, returning configured failures without real sockets."""
        assert "authorization" not in request.headers
        if request.url.path.endswith("/VerifyWebhook"):
            self.verified_requests.append(json.loads(request.content))
            if self.network_failure:
                raise httpx.ConnectError("unavailable")
            return (
                httpx.Response(self.status, content=self.raw_reply)
                if self.raw_reply is not None
                else httpx.Response(self.status, json=self.reply)
            )
        assert request.url.path == "/v1.0/publish/custos-pubsub/custos.triggers.normalized"
        if self.publish_network_failure:
            raise httpx.WriteError("unavailable")
        self.published.append(json.loads(request.content))
        return httpx.Response(503 if len(self.published) == self.fail_publish_at else 204)

    async def emit(
        self, event_name: str, *, workspace_id: str, attributes: Mapping[str, Any]
    ) -> None:
        """Capture sanitized rejection indices and codes without retaining payloads."""
        self.audit.append({"name": event_name, "workspace": workspace_id, **attributes})


@pytest.fixture
def ingress(
    monkeypatch: pytest.MonkeyPatch,
    workflow_auth: tuple[WorkflowCallerVerifier, dict[str, str]],
) -> Iterator[tuple[TestClient, WebhookHarness]]:
    """Run the actual default-off route with explicit independent mesh and storage gates."""
    monkeypatch.setenv("TRIGGER_GATEWAY_NAMESPACE", "custos")
    harness = WebhookHarness()
    http = httpx.AsyncClient(transport=httpx.MockTransport(harness.handle))
    store = InMemoryTriggerMetadataStore(now=lambda: NOW)
    app = create_app(
        authz_endpoint="",
        providers=Providers(metadata_store=store),
        dispatcher=dispatcher(store, FakeWorkflowServiceClient()),
        receiver_configuration_enabled=True,
        source_receivers_enabled=True,
        webhook_receiver_enabled=True,
        app_api_token=MESH["dapr-api-token"],
        workflow_caller_verifier=workflow_auth[0],
        audit_sink=harness,
        webhook_transport=DaprWebhookTransport(
            http,
            DaprEndpoint("127.0.0.1", 3500, "connector-service"),
            pubsub="custos-pubsub",
            topic="custos.triggers.normalized",
        ),
    )
    with TestClient(app) as client:
        yield client, harness
        assert client.portal is not None
        client.portal.call(http.aclose)


def test_raw_bytes_and_private_bearer_are_verification_data_only(
    ingress: tuple[TestClient, WebhookHarness],
) -> None:
    """Neither raw transport nor a caller-supplied workspace becomes normalized routing data."""
    client, harness = ingress
    raw = b'{ "opaque": "vendor bytes" }\r\n'
    response = client.post(
        PATH,
        content=raw,
        headers={
            **MESH,
            "x-custos-webhook-authorization": "unit-external-bearer",
            "x-custos-callctx": "forged",
            "authorization": "never-platform-identity",
            "x-custos-signature": "unit-signature",
            "x-workspace-id": "other",
        },
    )
    assert response.status_code == 200
    assert response.json() == {"accepted": 1, "rejected": 0}
    request = harness.verified_requests[0]
    assert base64.b64decode(request["bodyBase64"]) == raw
    assert request["workspaceId"] == "source-a" and request["connectorInstanceId"] == "same"
    assert request["headers"]["authorization"] == "unit-external-bearer"
    assert not any(key.startswith("dapr-") for key in request["headers"])
    assert "x-custos-callctx" not in request["headers"]
    event = harness.published[0]
    assert event["workspaceId"] == "source-a"
    assert event["event"]["source"]["type"] == "webhook"
    assert event["event"]["data"]["workspace"] == "forged-workspace"
    assert "raw" not in event["event"]
    assert "unit-external-bearer" not in json.dumps(event)
    assert "unit-signature" not in json.dumps(event)


@pytest.mark.parametrize(
    "header,value,status",
    [
        ("dapr-api-token", "wrong", 401),
        ("dapr-caller-app-id", "other", 403),
        ("dapr-caller-namespace", "other", 403),
    ],
)
def test_gateway_identity_required_even_in_development(
    ingress: tuple[TestClient, WebhookHarness],
    header: str,
    value: str,
    status: int,
) -> None:
    """Forged service identity cannot reach verification, even with a platform call context."""
    client, harness = ingress
    response = client.post(PATH, content=b"raw", headers={**MESH, header: value})
    assert response.status_code == status
    assert not harness.verified_requests and not harness.published


@pytest.mark.parametrize(
    "status,code,expected",
    [
        (400, "webhook.malformed", 400),
        (401, "webhook.signature_invalid", 401),
        (404, "connector.instance_not_listening", 404),
        (413, "webhook.too_large", 413),
        (503, "webhook.verifier_unavailable", 503),
        (403, "mesh_caller_denied", 503),
        (404, "ERR_DIRECT_INVOKE", 503),
    ],
)
def test_verification_failures_never_publish_or_echo_remote_details(
    ingress: tuple[TestClient, WebhookHarness],
    status: int,
    code: str,
    expected: int,
) -> None:
    """Differentiate verified domain errors from unavailable private transport."""
    client, harness = ingress
    harness.status, harness.reply = status, {"error": {"code": code, "detail": "private-detail"}}
    response = client.post(PATH, content=b"raw", headers=MESH)
    assert response.status_code == expected
    assert not harness.published and "private-detail" not in response.text


def test_mixed_verifier_and_adapter_rejections_preserve_original_indices(
    ingress: tuple[TestClient, WebhookHarness],
) -> None:
    """Accepted plus rejected equals received count; audit contains only indexed safe reasons."""
    client, harness = ingress
    invalid = connector_event()
    invalid["source"]["receivedAt"] = "invalid"
    harness.reply = {
        "schemaVersion": "1",
        "receivedCount": 3,
        "events": [{"index": 0, "event": connector_event()}, {"index": 2, "event": invalid}],
        "rejections": [{"index": 1, "code": "missing-event-id"}],
    }
    response = client.post(PATH, content=b"raw", headers=MESH)
    assert response.json() == {"accepted": 1, "rejected": 2}
    assert [(row["index"], row["reason"]) for row in harness.audit] == [
        (1, "missing-event-id"),
        (2, "malformed"),
    ]
    assert len(harness.published) == 1


def test_all_invalid_is_not_an_empty_success(ingress: tuple[TestClient, WebhookHarness]) -> None:
    """A fully rejected verified batch has the documented distinct failure shape."""
    client, harness = ingress
    harness.reply = {
        "schemaVersion": "1",
        "receivedCount": 1,
        "events": [],
        "rejections": [{"index": 0, "code": "malformed"}],
    }
    response = client.post(PATH, content=b"raw", headers=MESH)
    assert response.status_code == 400
    assert response.json()["code"] == "trigger.webhook.no_valid_events"
    assert not harness.published


def test_partial_publication_failure_has_no_success_counts(
    ingress: tuple[TestClient, WebhookHarness],
) -> None:
    """A sender may retry the whole batch; already published events retain their logical IDs."""
    client, harness = ingress
    harness.reply["receivedCount"] = 2
    harness.reply["events"].append({"index": 1, "event": connector_event(event_id="second")})
    harness.fail_publish_at = 2
    response = client.post(PATH, content=b"raw", headers=MESH)
    assert response.status_code == 503 and "accepted" not in response.json()
    assert len(harness.published) == 2
    harness.fail_publish_at = 0
    assert client.post(PATH, content=b"raw", headers=MESH).json() == {"accepted": 2, "rejected": 0}
    assert harness.published[0]["event"]["eventId"] == harness.published[2]["event"]["eventId"]


@pytest.mark.parametrize("failure", ["network", "json", "size", "partition", "scope"])
def test_invalid_verifier_contract_is_retryable_before_publication(
    ingress: tuple[TestClient, WebhookHarness],
    failure: str,
) -> None:
    """Never acknowledge an incomplete partition, wrong scope or malformed private response."""
    client, harness = ingress
    if failure == "network":
        harness.network_failure = True
    elif failure == "json":
        harness.raw_reply = b"{"
    elif failure == "size":
        harness.raw_reply = b"x" * (MAX_RESPONSE_BYTES + 1)
    elif failure == "partition":
        harness.reply["receivedCount"] = 2
    else:
        harness.reply["events"][0]["event"]["source"]["workspaceId"] = "other"
    assert client.post(PATH, content=b"raw", headers=MESH).status_code == 503
    assert not harness.published


@pytest.mark.parametrize("resource", ["body", "headers"])
def test_resources_bounded_before_verifier(
    ingress: tuple[TestClient, WebhookHarness],
    resource: str,
) -> None:
    """Reject oversized raw bytes and verification headers without contacting Connector."""
    client, harness = ingress
    response = client.post(
        PATH,
        content=b"x" * (MAX_RAW_BYTES + 1) if resource == "body" else b"raw",
        headers={**MESH, **({"x-vendor": "x" * MAX_HEADER_BYTES} if resource == "headers" else {})},
    )
    assert response.status_code == 413 and not harness.verified_requests


def test_gate_is_explicit_and_requires_source_consumption() -> None:
    """Bad flag values and incomplete activation fail loudly rather than enabling ingress."""
    assert not read_webhook_receiver_enabled({})
    assert read_webhook_receiver_enabled({"TRIGGER_WEBHOOK_RECEIVER_ENABLED": "true"})
    with pytest.raises(SettingsError):
        read_webhook_receiver_enabled({"TRIGGER_WEBHOOK_RECEIVER_ENABLED": "invalid"})
    with pytest.raises(ValueError, match="authenticated Gateway"):
        create_app(webhook_receiver_enabled=True, source_receivers_enabled=False)


@pytest.mark.parametrize("indices", [[0, 0], [1, 0], [0, 2]])
def test_verifier_partition_is_exact_and_ordered(indices: list[int]) -> None:
    """Duplicate, reordered or out-of-range indices cannot become accepted counts."""
    body = verified_batch()
    body["receivedCount"] = 2
    body["events"] = [{"index": index, "event": connector_event()} for index in indices]
    with pytest.raises(ValueError, match="partition"):
        VerifiedBatch.model_validate(body)


def test_exact_raw_limit_is_accepted(ingress: tuple[TestClient, WebhookHarness]) -> None:
    """The documented one-MiB boundary is inclusive and preserved through base64 encoding."""
    client, harness = ingress
    raw = b"{}" + b" " * (MAX_RAW_BYTES - 2)
    assert client.post(PATH, content=raw, headers=MESH).status_code == 200
    assert base64.b64decode(harness.verified_requests[0]["bodyBase64"]) == raw


def test_ambiguous_private_credential_is_rejected(
    ingress: tuple[TestClient, WebhookHarness],
) -> None:
    """A trusted mesh hop still cannot supply ambiguous verification data."""
    client, harness = ingress
    response = client.post(
        PATH,
        content=b"raw",
        headers=[
            *MESH.items(),
            ("x-custos-webhook-authorization", "first"),
            ("x-custos-webhook-authorization", "second"),
        ],
    )
    assert response.status_code == 400 and not harness.verified_requests


def test_publish_transport_failure_is_retryable(
    ingress: tuple[TestClient, WebhookHarness],
) -> None:
    """Network publication failure cannot produce accepted counts."""
    client, harness = ingress
    harness.publish_network_failure = True
    response = client.post(PATH, content=b"raw", headers=MESH)
    assert response.status_code == 503 and "accepted" not in response.json()


def test_overall_deadline_is_retryable(
    ingress: tuple[TestClient, WebhookHarness],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A request deadline bounds the complete batch, not merely each individual HTTP call."""
    monkeypatch.setattr("custos_trigger.receivers.webhook.MAX_REQUEST_SECONDS", 0)
    client, _ = ingress
    response = client.post(PATH, content=b"raw", headers=MESH)
    assert response.status_code == 503 and "accepted" not in response.json()


@pytest.mark.parametrize("invalid_destination", [False, True])
def test_default_transport_is_owned_and_closed_on_startup_failure(
    monkeypatch: pytest.MonkeyPatch,
    workflow_auth: tuple[WorkflowCallerVerifier, dict[str, str]],
    invalid_destination: bool,
) -> None:
    """The lifespan closes its new client on both shutdown and transport configuration failure."""
    monkeypatch.setenv("TRIGGER_GATEWAY_NAMESPACE", "custos")
    if invalid_destination:
        monkeypatch.setenv("TRIGGER_PUBSUB_COMPONENT", "invalid/name")
    owned = httpx.AsyncClient()
    monkeypatch.setattr(httpx, "AsyncClient", lambda: owned)
    store = InMemoryTriggerMetadataStore(now=lambda: NOW)
    app = create_app(
        authz_endpoint="",
        providers=Providers(metadata_store=store),
        dispatcher=dispatcher(store, FakeWorkflowServiceClient()),
        receiver_configuration_enabled=True,
        source_receivers_enabled=True,
        webhook_receiver_enabled=True,
        app_api_token=MESH["dapr-api-token"],
        workflow_caller_verifier=workflow_auth[0],
    )
    if invalid_destination:
        with pytest.raises(ValueError, match="Dapr resource names"), TestClient(app):
            pytest.fail("invalid webhook destination started")
    else:
        with TestClient(app):
            assert app.state.webhook_transport.client is owned
            assert not owned.is_closed
    assert owned.is_closed
