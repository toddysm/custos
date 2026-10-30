"""Scoped adapter and broker callback contracts, including default-off authenticated activation."""

from __future__ import annotations

import json
from collections.abc import Iterator
from copy import deepcopy
from unittest.mock import AsyncMock

import pytest
from custos_spl.errors import BackendUnavailable
from custos_spl.interfaces.dead_letters import MAX_PAYLOAD_BYTES, snapshot_dispatch_object
from fastapi.testclient import TestClient

from custos_trigger.app import create_app
from custos_trigger.clients import FakeWorkflowServiceClient
from custos_trigger.clients.workflow import WorkflowClientTransportError
from custos_trigger.dependencies import WorkflowCallerVerifier
from custos_trigger.models import SourceType, SubscriptionState
from custos_trigger.providers import InMemoryTriggerMetadataStore, Providers
from custos_trigger.receivers.sources import (
    CONNECTOR_EVENTS_PATH,
    NORMALIZED_EVENTS_PATH,
    SourceEventRejected,
    adapt_connector_event,
)
from custos_trigger.settings import (
    ENV_SOURCE_RECEIVERS_ENABLED,
    SettingsError,
    read_source_receivers_enabled,
)
from custos_trigger.stores import SubscriptionStore
from tests._source_helpers import NOW, SourceStorage, connector_event, dispatcher, resume, start

HEADERS = {"dapr-api-token": "unit-source-sidecar"}
SourceClient = tuple[TestClient, InMemoryTriggerMetadataStore, FakeWorkflowServiceClient]


@pytest.fixture
def source_client(
    workflow_auth: tuple[WorkflowCallerVerifier, dict[str, str]],
) -> Iterator[SourceClient]:
    """Run actual source routes with explicit rollout, authentication and durability gates."""
    store = InMemoryTriggerMetadataStore(now=lambda: NOW)
    assert isinstance(store, SourceStorage)
    client = FakeWorkflowServiceClient()
    app = create_app(
        authz_endpoint="",
        providers=Providers(metadata_store=store),
        dispatcher=dispatcher(store, client),
        receiver_configuration_enabled=True,
        source_receivers_enabled=True,
        app_api_token=HEADERS["dapr-api-token"],
        workflow_caller_verifier=workflow_auth[0],
    )
    with TestClient(app) as opened:
        assert opened.portal is not None
        opened.portal.call(start, store, "start")
        yield opened, store, client


def test_adapter_preserves_producer_contract_and_business_payload() -> None:
    """Scope comes from producer provenance, not identically named payload fields."""
    incoming = connector_event()
    original = deepcopy(incoming)
    scoped = adapt_connector_event(incoming)
    assert incoming == original
    assert scoped.workspace_id == "source-a"
    assert scoped.event.event_id == incoming["eventId"]
    assert scoped.event.kind == incoming["eventType"]
    assert scoped.event.data == incoming["payload"]
    assert scoped.event.source.connector_instance_id == "same"
    assert scoped.event.source.type is SourceType.VENDOR_PUSH
    assert not scoped.event.raw.headers and not scoped.event.raw.body
    assert (
        adapt_connector_event(incoming, source_type=SourceType.WEBHOOK).event.source.type
        is SourceType.WEBHOOK
    )
    assert adapt_connector_event(connector_event(mode="pull")).event.source.type is SourceType.PULL


@pytest.mark.parametrize(
    "change", ["version", "time", "scope", "identifier", "mode", "kind", "private"]
)
def test_adapter_rejects_poison_without_unsafe_diagnostics(change: str) -> None:
    """Malformed provenance, unknown contracts and unretainable payloads fail before effects."""
    value = connector_event()
    if change == "version":
        value["schemaVersion"] = "2"
    elif change == "time":
        value["source"]["receivedAt"] = "2026-01-01"
    elif change == "scope":
        value["source"]["workspaceId"] = "bad\x00workspace"
    elif change == "identifier":
        value["eventId"] = " "
    elif change == "mode":
        value["source"]["deliveryMode"] = "unknown"
    elif change == "kind":
        value["eventType"] = "workflow.not-a-real-kind"
    else:
        value["payload"]["authorization"] = "private-value"
    with pytest.raises(SourceEventRejected) as caught:
        adapt_connector_event(value)
    assert str(caught.value) == "invalid-connector-envelope"


def test_callbacks_ack_only_after_durable_processing(source_client: SourceClient) -> None:
    """Actual callback delivery produces mapped inputs and persistent duplicate suppression."""
    client, _, workflow = source_client
    body = {"data": connector_event()}
    for _ in range(2):
        response = client.post(CONNECTOR_EVENTS_PATH, json=body, headers=HEADERS)
        assert response.status_code == 200 and response.json() == {"status": "SUCCESS"}
    assert len(workflow.start_run_calls) == 1
    call = workflow.start_run_calls[0]
    assert call.workspace_id == "source-a"
    assert call.inputs == {"amount": 7, "label": "ref-same"}
    assert call.delivery_protocol == 2


def test_storage_failure_is_retry_not_success(
    source_client: SourceClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed candidate lookup leaves the broker message unacknowledged."""
    client, store, workflow = source_client
    monkeypatch.setattr(
        store, "query_subscriptions", AsyncMock(side_effect=BackendUnavailable("offline"))
    )
    response = client.post(CONNECTOR_EVENTS_PATH, json={"data": connector_event()}, headers=HEADERS)
    assert response.json() == {"status": "RETRY"}
    assert not workflow.start_run_calls


def test_failed_capture_is_a_broker_retry(
    source_client: SourceClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A downstream failure is acknowledged only after its dead-letter write commits."""
    client, store, workflow = source_client
    workflow.error = WorkflowClientTransportError("downstream unavailable")
    capture = AsyncMock(side_effect=BackendUnavailable("capture unavailable"))
    monkeypatch.setattr(store, "capture_failed_delivery", capture)
    response = client.post(CONNECTOR_EVENTS_PATH, json={"data": connector_event()}, headers=HEADERS)
    assert response.json() == {"status": "RETRY"}
    capture.assert_awaited_once()


@pytest.mark.parametrize("boundary", ["size", "depth"])
def test_complete_wrapped_target_is_checked_before_dispatch(
    source_client: SourceClient, boundary: str
) -> None:
    """Inputs alone may fit while the complete target cannot be captured; this must retry."""
    client, store, workflow = source_client
    if boundary == "size":
        empty_size = len(json.dumps({"value": ""}, ensure_ascii=False).encode())
        mapping: dict[str, object] = {"value": "x" * (MAX_PAYLOAD_BYTES - empty_size)}
    else:
        mapping = {"value": 0}
        for _ in range(31):
            mapping = {"nested": mapping}
    snapshot_dispatch_object(mapping)
    assert client.portal is not None

    async def replace_mapping() -> None:
        """Seed a persisted mapping that needs full-target preflight on consumption."""
        subscriptions = SubscriptionStore(store)
        sub = await subscriptions.get("source-a", "start")
        assert sub is not None
        await subscriptions.update(
            sub.model_copy(update={"input_mapping": mapping}), expected_revision=sub.store_revision
        )

    client.portal.call(replace_mapping)
    response = client.post(CONNECTOR_EVENTS_PATH, json={"data": connector_event()}, headers=HEADERS)
    assert response.status_code == 200 and response.json() == {"status": "RETRY"}
    assert not workflow.start_run_calls


def test_poison_drop_and_untrusted_callback_rejection(source_client: SourceClient) -> None:
    """Invalid source data is dropped, but direct app-port spoofing never reaches processing."""
    client, _, workflow = source_client
    assert client.post(CONNECTOR_EVENTS_PATH, json={"data": connector_event()}).status_code == 401
    assert (
        client.post(
            CONNECTOR_EVENTS_PATH,
            json={"data": connector_event()},
            headers={**HEADERS, "dapr-caller-app-id": "connector-service"},
        ).status_code
        == 403
    )
    assert client.post(CONNECTOR_EVENTS_PATH, json={"data": {}}, headers=HEADERS).json() == {
        "status": "DROP"
    }
    assert not workflow.start_run_calls


def test_complete_resume_target_is_checked_before_dispatch(source_client: SourceClient) -> None:
    """Unretainable stored signal identifiers retry without a resume effect or raw 500."""
    client, store, workflow = source_client
    assert client.portal is not None
    assert isinstance(store, SourceStorage)

    async def seed_wait() -> None:
        """Create a source wait whose signal exceeds the capture identifier bound."""
        await SubscriptionStore(store).set_state("source-a", "start", SubscriptionState.PAUSED)
        await resume(store, "stored-run", event_key="x" * 513)

    client.portal.call(seed_wait)
    response = client.post(CONNECTOR_EVENTS_PATH, json={"data": connector_event()}, headers=HEADERS)
    assert response.status_code == 200 and response.json() == {"status": "RETRY"}
    assert not workflow.start_run_calls and not workflow.raise_event_calls


def test_private_normalized_route_uses_same_durable_path(source_client: SourceClient) -> None:
    """Normalized delivery retains independent routing context without rewriting business data."""
    client, _, workflow = source_client
    scoped = adapt_connector_event(connector_event())
    response = client.post(
        NORMALIZED_EVENTS_PATH,
        json={"data": scoped.model_dump(mode="json", by_alias=True)},
        headers=HEADERS,
    )
    assert response.json() == {"status": "SUCCESS"}
    assert len(workflow.start_run_calls) == 1


@pytest.mark.parametrize("version", [None, "2", 1])
def test_normalized_wrapper_requires_explicit_supported_version(
    source_client: SourceClient, version: object
) -> None:
    """Unversioned, unknown and wrongly typed transport wrappers never dispatch."""
    client, _, workflow = source_client
    wire = adapt_connector_event(connector_event()).model_dump(mode="json", by_alias=True)
    if version is None:
        wire.pop("schemaVersion")
    else:
        wire["schemaVersion"] = version
    response = client.post(NORMALIZED_EVENTS_PATH, json={"data": wire}, headers=HEADERS)
    assert response.json() == {"status": "DROP"}
    assert not workflow.start_run_calls and not workflow.raise_event_calls


def test_discovery_is_repeatable_and_disabled_by_default(source_client: SourceClient) -> None:
    """Dapr can rebuild registrations after restart; existing default discovery stays unchanged."""
    client, _, _ = source_client
    first = client.get("/dapr/subscribe", headers=HEADERS).json()
    assert client.get("/dapr/subscribe", headers=HEADERS).json() == first
    assert {item["route"] for item in first} == {
        "/internal/events/workflow",
        CONNECTOR_EVENTS_PATH,
        NORMALIZED_EVENTS_PATH,
    }
    with TestClient(
        create_app(
            authz_endpoint="",
            providers=Providers(metadata_store=InMemoryTriggerMetadataStore()),
            source_receivers_enabled=False,
        )
    ) as disabled:
        assert len(disabled.get("/dapr/subscribe").json()) == 1
        assert (
            disabled.post(CONNECTOR_EVENTS_PATH, json={"data": connector_event()}).status_code
            == 503
        )


def test_discovery_rebuilds_after_application_restart(
    workflow_auth: tuple[WorkflowCallerVerifier, dict[str, str]],
) -> None:
    """Registration is derived from configuration, not an ephemeral activation cache."""
    registrations = []
    for _ in range(2):
        store = InMemoryTriggerMetadataStore(now=lambda: NOW)
        assert isinstance(store, SourceStorage)
        app = create_app(
            authz_endpoint="",
            providers=Providers(metadata_store=store),
            dispatcher=dispatcher(store, FakeWorkflowServiceClient()),
            receiver_configuration_enabled=True,
            source_receivers_enabled=True,
            app_api_token=HEADERS["dapr-api-token"],
            workflow_caller_verifier=workflow_auth[0],
        )
        with TestClient(app) as client:
            registrations.append(client.get("/dapr/subscribe", headers=HEADERS).json())
    assert len(registrations[0]) == 3 and registrations[0] == registrations[1]


@pytest.mark.parametrize("raw", ["maybe", "-1", "yes"])
def test_source_activation_rejects_invalid_flags(raw: str) -> None:
    """No malformed configuration can silently enable a broker consumer."""
    with pytest.raises(SettingsError):
        read_source_receivers_enabled({ENV_SOURCE_RECEIVERS_ENABLED: raw})


def test_source_activation_requires_security_and_durable_dependencies() -> None:
    """New callbacks cannot activate from only the old authoring/development gate."""
    with (
        pytest.raises(ValueError, match="authenticated upgraded durable"),
        TestClient(
            create_app(
                authz_endpoint="",
                source_receivers_enabled=True,
                receiver_configuration_enabled=False,
                providers=Providers(metadata_store=InMemoryTriggerMetadataStore()),
            )
        ),
    ):
        pass


def test_production_source_activation_rejects_volatile_storage(
    workflow_auth: tuple[WorkflowCallerVerifier, dict[str, str]],
) -> None:
    """Protocol parity does not make development memory restart-durable in production."""
    store = InMemoryTriggerMetadataStore(now=lambda: NOW)
    assert isinstance(store, SourceStorage)
    with (
        pytest.raises(ValueError, match="restart-durable"),
        TestClient(
            create_app(
                authz_endpoint="https://auth.example",
                environment="production",
                providers=Providers(metadata_store=store),
                dispatcher=dispatcher(store, FakeWorkflowServiceClient()),
                receiver_configuration_enabled=True,
                source_receivers_enabled=True,
                app_api_token=HEADERS["dapr-api-token"],
                workflow_caller_verifier=workflow_auth[0],
            )
        ),
    ):
        pass


@pytest.mark.parametrize("length", [512, 513])
def test_event_kind_capture_boundary_is_checked_before_effects(
    source_client: SourceClient, length: int
) -> None:
    """Taxonomy-valid vendor names still obey the durable capture identifier limit."""
    client, _, workflow = source_client
    body = connector_event()
    body["eventType"] = "vendor." + "a" * (length - len("vendor."))
    response = client.post(CONNECTOR_EVENTS_PATH, json={"data": body}, headers=HEADERS)
    assert response.json() == {"status": "SUCCESS" if length == 512 else "DROP"}
    assert len(workflow.start_run_calls) == (1 if length == 512 else 0)


def test_source_topics_cannot_alias(monkeypatch: pytest.MonkeyPatch) -> None:
    """Different producer contracts cannot be silently registered on the same topic."""
    monkeypatch.setenv("TRIGGER_NORMALIZED_TOPIC", "custos.connector.events")
    with pytest.raises(ValueError, match="must be distinct"):
        create_app(authz_endpoint="", source_receivers_enabled=True)
