"""HTTP receiver configuration and tenant-scoped external resume contracts."""

import json
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock

import pytest
from custos_spl.errors import BackendUnavailable
from custos_spl.ids import ConnectorInstanceId, WorkspaceId
from fastapi.testclient import TestClient

from custos_trigger.api.routes import rpc
from custos_trigger.app import create_app
from custos_trigger.clients import FakeWorkflowServiceClient
from custos_trigger.dedup import Deduplicator
from custos_trigger.dependencies import WorkflowCallerVerifier
from custos_trigger.middleware import CALLCTX_HEADER
from custos_trigger.models import RegisterResumeRequest, ResumeRegistration
from custos_trigger.pipeline.dispatch import Dispatcher
from custos_trigger.providers import InMemoryTriggerMetadataStore, Providers
from custos_trigger.settings import (
    ENV_RECEIVER_CONFIGURATION_ENABLED,
    SettingsError,
    read_receiver_configuration_enabled,
)
from custos_trigger.stores import ResumeSubscriptionStore

_PATH = "/v1/workspaces/ws_demo/triggers"
_TRIPLE = {"runId": "run", "stepId": "wait", "eventKey": "event"}
_NOW = datetime(2026, 10, 28, tzinfo=UTC)


@dataclass
class References:
    """Credential-free reference reader with observable workspace lookups and outages."""

    rows: set[tuple[str, str]] = field(default_factory=lambda: {("ws_demo", "registry")})
    unavailable: bool = False

    async def has_connector_instance(
        self, workspace_id: WorkspaceId, instance_id: ConnectorInstanceId
    ) -> bool:
        """Return exact scoped existence or the provider's explicit failure."""
        if self.unavailable:
            raise BackendUnavailable("reference backend unavailable")
        return (workspace_id, instance_id) in self.rows


@dataclass
class Audit:
    """Record source-rebinding evidence without exporting it from the test process."""

    events: list[tuple[str, str, Mapping[str, Any]]] = field(default_factory=list)

    async def emit(
        self, event_name: str, *, workspace_id: str, attributes: Mapping[str, Any]
    ) -> None:
        """Capture the same event shape the application audit sink receives."""
        self.events.append((event_name, workspace_id, dict(attributes)))


@pytest.fixture
def references() -> References:
    """Provide one existing connector in the caller's workspace."""
    return References()


@pytest.fixture
def audit() -> Audit:
    """Provide independent audit history for each request scenario."""
    return Audit()


@pytest.fixture
def workflow() -> FakeWorkflowServiceClient:
    """Record accepted dispatch inputs independently for each route test."""
    return FakeWorkflowServiceClient()


@pytest.fixture
def client(
    metadata_store: InMemoryTriggerMetadataStore,
    references: References,
    audit: Audit,
    workflow: FakeWorkflowServiceClient,
    workflow_auth: tuple[WorkflowCallerVerifier, dict[str, str]],
) -> Iterator[TestClient]:
    """Run real route validation and persistence over injected local collaborators."""
    app = create_app(
        authz_endpoint="",
        providers=Providers(metadata_store=metadata_store, connector_references=references),
        dispatcher=Dispatcher(workflow, Deduplicator(metadata_store)),
        audit_sink=audit,
        receiver_configuration_enabled=True,
        workflow_caller_verifier=workflow_auth[0],
    )
    with TestClient(app, headers=workflow_auth[1]) as opened:
        yield opened


def _headers(workspace: str = "ws_demo") -> dict[str, str]:
    """Authorize only the tenant selected for this request."""
    return {
        CALLCTX_HEADER: json.dumps(
            {
                "workspace_id": workspace,
                "principal_id": "operator",
                "permissions": [
                    "trigger:subscriptions:read",
                    "trigger:subscriptions:write",
                    "trigger:subscriptions:delete",
                ],
            }
        )
    }


async def test_versioned_internal_wait_is_scoped_and_persisted(
    client: TestClient,
    metadata_store: InMemoryTriggerMetadataStore,
) -> None:
    """A trusted versioned internal wait retains its workspace without an external source."""
    response = client.post(
        "/RegisterResumeSubscription",
        json={**_TRIPLE, "workspaceId": "ws_demo", "deliveryProtocol": 2, "ttl": "PT1H"},
    )
    assert response.status_code == 200
    identifier = response.json()["subscriptionId"]
    row = await metadata_store.get_resume_subscription(WorkspaceId("ws_demo"), identifier)
    assert row is not None and row.payload["deliveryProtocol"] == 2
    assert await metadata_store.get_resume_subscription(WorkspaceId("_resume"), identifier) is None


def test_versioned_internal_wait_still_requires_workflow_identity(
    metadata_store: InMemoryTriggerMetadataStore,
) -> None:
    """Protocol selection never substitutes for authenticating the Workflow service principal."""
    app = create_app(
        authz_endpoint="",
        providers=Providers(metadata_store=metadata_store),
        receiver_configuration_enabled=True,
    )
    with TestClient(app) as opened:
        response = opened.post(
            "/RegisterResumeSubscription",
            json={**_TRIPLE, "workspaceId": "ws_demo", "deliveryProtocol": 2},
            headers={"dapr-caller-app-id": "workflow-service"},
        )
    assert response.status_code == 503


def _external(workspace: str = "ws_demo", connector: str = "registry") -> dict[str, object]:
    """Build an external wait with a selective expression and explicit delivery demand."""
    return {
        **_TRIPLE,
        "workspaceId": workspace,
        "selector": "event.data.key == 'one'",
        "source": {
            "connectorInstanceId": connector,
            "deliveryModes": ["push", "pull"],
            "pollIntervalSeconds": 10,
        },
        "ttl": "PT1H",
    }


def test_scheduled_selector_numeric_bindings_on_create_and_patch(client: TestClient) -> None:
    """Authoring accepts canonical numeric operations and still rejects mismatched types."""
    result = client.post(
        _PATH,
        json={
            "sourceType": "scheduled",
            "workflowId": "wf",
            "sourceConfig": {"intervalSeconds": 30},
            "selector": "event.data.skippedCount + 1 > 0",
        },
        headers=_headers(),
    )
    assert result.status_code == 201, result.text
    path = f"{_PATH}/{result.json()['subscriptionId']}"
    selector = "event.data.subscriptionRevision + 1 > 0"
    patched = client.patch(path, json={"selector": selector}, headers=_headers())
    assert patched.status_code == 200, patched.text
    assert patched.json()["selector"] == selector
    rejected = client.patch(
        path, json={"selector": "event.data.skippedCount + 'one' == 'one'"}, headers=_headers()
    )
    assert rejected.status_code == 422
    assert client.get(path, headers=_headers()).json()["selector"] == selector


@pytest.mark.parametrize("replacement", [None, "event.kind == 'manual.fired'"])
async def test_selector_validation_uses_merged_source_and_preserves_failed_changes(
    client: TestClient,
    metadata_store: InMemoryTriggerMetadataStore,
    replacement: str | None,
) -> None:
    """Source transitions validate retained or replaced selectors before any persisted mutation."""
    created = client.post(
        _PATH, json={"sourceType": "manual", "workflowId": "wf"}, headers=_headers()
    )
    assert created.status_code == 201
    identifier = created.json()["subscriptionId"]
    path = f"{_PATH}/{identifier}"
    scheduled = client.patch(
        path,
        json={
            "sourceType": "scheduled",
            "sourceConfig": {"intervalSeconds": 30},
            "selector": "event.data.skippedCount + 1 > 0",
        },
        headers=_headers(),
    )
    assert scheduled.status_code == 200, scheduled.text
    before = await metadata_store.get_schedule(WorkspaceId("ws_demo"), identifier)
    assert before is not None and before.enabled
    rejected = client.patch(
        path,
        json={"sourceType": "manual", "sourceConfig": None},
        headers=_headers(),
    )
    assert rejected.status_code == 422, rejected.text
    assert client.get(path, headers=_headers()).json() == scheduled.json()
    assert await metadata_store.get_schedule(WorkspaceId("ws_demo"), identifier) == before
    changed = client.patch(
        path,
        json={"sourceType": "manual", "sourceConfig": None, "selector": replacement},
        headers=_headers(),
    )
    assert changed.status_code == 200, changed.text
    assert changed.json()["sourceType"] == "manual"
    assert changed.json()["selector"] == replacement
    after = await metadata_store.get_schedule(WorkspaceId("ws_demo"), identifier)
    assert after is not None and not after.enabled


@pytest.mark.parametrize("count", [None, 0])
def test_manual_fire_retains_scheduled_selector_schema(
    client: TestClient, workflow: FakeWorkflowServiceClient, count: int | None
) -> None:
    """Manual transport keeps numeric authoring types without fabricating a scheduled event."""
    created = client.post(
        _PATH,
        json={
            "sourceType": "scheduled",
            "workflowId": "wf",
            "targetWorkflowVersionId": "version",
            "sourceConfig": {"intervalSeconds": 30},
            "inputMapping": {
                "count": "${{ event.data.skippedCount + 1 }}",
                "source": "${{ event.source.type }}",
            },
            "selector": (
                "event.source.type == 'manual' && event.kind == 'manual.fire' "
                "&& event.data.skippedCount + 1 > 0"
            ),
        },
        headers=_headers(),
    )
    assert created.status_code == 201, created.text
    path = f"{_PATH}/{created.json()['subscriptionId']}:fire"
    payload = {"inputs": {} if count is None else {"skippedCount": count}}
    assert client.post(path, json=payload, headers=_headers()).status_code == 403
    context = json.loads(_headers()[CALLCTX_HEADER])
    context["permissions"].append("trigger:subscriptions:fire")
    fired = client.post(path, json=payload, headers={CALLCTX_HEADER: json.dumps(context)})
    if count is None:
        assert fired.status_code == 409, fired.text
        assert fired.json()["code"] == "trigger.api.subscription_not_fireable"
        assert not workflow.start_run_calls
    else:
        assert fired.status_code == 200, fired.text
        assert fired.json()["runId"] == "run-fake"
        assert workflow.start_run_calls[0].inputs == {
            "count": 1,
            "source": "manual",
        }


@pytest.mark.parametrize("kind", ["webhook", "vendor-push", "pull"])
def test_connector_mapping_validation_and_manual_fire(
    client: TestClient, workflow: FakeWorkflowServiceClient, kind: str
) -> None:
    """Event-driven mappings validate at authoring and resolve consistently for explicit fires."""
    config: dict[str, object] = {"connectorInstanceId": "registry"}
    if kind == "pull":
        config["pollIntervalSeconds"] = 10
    body = {
        "sourceType": kind,
        "workflowId": "wf",
        "targetWorkflowVersionId": "version",
        "sourceConfig": config,
        "selector": "event.data.route == 'match'",
        "inputMapping": {"amount": "${{ event.data.amount }}"},
    }
    bad = client.post(
        _PATH, json={**body, "inputMapping": {"bad": "${{ unbound.value }}"}}, headers=_headers()
    )
    assert bad.status_code == 422
    created = client.post(_PATH, json=body, headers=_headers())
    assert created.status_code == 201
    context = json.loads(_headers()[CALLCTX_HEADER])
    context["permissions"].append("trigger:subscriptions:fire")
    fired = client.post(
        f"{_PATH}/{created.json()['subscriptionId']}:fire",
        json={"inputs": {"route": "match", "amount": 7}},
        headers={CALLCTX_HEADER: json.dumps(context)},
    )
    assert fired.status_code == 200
    assert workflow.start_run_calls[0].inputs == {"amount": 7}


@pytest.mark.parametrize("kind", ["webhook", "vendor-push", "pull"])
def test_connector_configuration_round_trip(client: TestClient, kind: str) -> None:
    """All connector-backed receiver tags persist the same validated reference contract."""
    config: dict[str, object] = {"connectorInstanceId": "registry"}
    if kind == "pull":
        config["pollIntervalSeconds"] = 10
    result = client.post(
        _PATH,
        json={"sourceType": kind, "workflowId": "wf", "sourceConfig": config},
        headers=_headers(),
    )
    assert result.status_code == 201, result.text
    body = result.json()
    assert body["sourceType"] == kind
    assert all(body["sourceConfig"][key] == value for key, value in config.items())
    assert "storeRevision" not in body and "_revision" not in body
    assert client.get(f"{_PATH}/{body['subscriptionId']}", headers=_headers()).json() == body


@pytest.mark.parametrize(
    "expression",
    ["${{ event.data.skippedCount + 'one' }}", "${{ missing.value }}", "${{ event.data"],
)
async def test_invalid_scheduled_mapping_never_persists(
    client: TestClient,
    metadata_store: InMemoryTriggerMetadataStore,
    expression: str,
) -> None:
    """Create and nested mapping patches fail before changing subscription or schedule state."""
    valid = {
        "sourceType": "scheduled",
        "workflowId": "wf",
        "sourceConfig": {"intervalSeconds": 30},
        "inputMapping": {"count": "${{ event.data.skippedCount + 1 }}"},
    }
    invalid = {"nested": [{"value": expression}]}
    rejected = client.post(_PATH, json={**valid, "inputMapping": invalid}, headers=_headers())
    assert rejected.status_code == 422, rejected.text
    assert not (await metadata_store.query_subscriptions(WorkspaceId("ws_demo"))).items
    created = client.post(_PATH, json=valid, headers=_headers())
    assert created.status_code == 201, created.text
    identifier = created.json()["subscriptionId"]
    before = await metadata_store.get_schedule(WorkspaceId("ws_demo"), identifier)
    rejected = client.patch(
        f"{_PATH}/{identifier}", json={"inputMapping": invalid}, headers=_headers()
    )
    assert rejected.status_code == 422, rejected.text
    assert client.get(f"{_PATH}/{identifier}", headers=_headers()).json() == created.json()
    assert await metadata_store.get_schedule(WorkspaceId("ws_demo"), identifier) == before


def test_source_transition_validates_retained_or_replacement_mapping(client: TestClient) -> None:
    """Source changes use the resulting mapping schema and compile without event-time evaluation."""
    created = client.post(
        _PATH,
        json={
            "sourceType": "manual",
            "workflowId": "wf",
            "inputMapping": {"count": "${{ event.data.skippedCount + 'one' }}"},
        },
        headers=_headers(),
    )
    assert created.status_code == 201
    path = f"{_PATH}/{created.json()['subscriptionId']}"
    scheduled = {"sourceType": "scheduled", "sourceConfig": {"intervalSeconds": 30}}
    assert client.patch(path, json=scheduled, headers=_headers()).status_code == 422
    assert client.get(path, headers=_headers()).json() == created.json()
    changed = client.patch(
        path,
        json={
            **scheduled,
            "inputMapping": {"count": "${{ event.data.skippedCount + 1 }}"},
        },
        headers=_headers(),
    )
    assert changed.status_code == 200, changed.text
    manual = {"sourceType": "manual", "sourceConfig": None}
    assert client.patch(path, json=manual, headers=_headers()).status_code == 422
    assert client.get(path, headers=_headers()).json() == changed.json()
    assert (
        client.patch(path, json={**manual, "inputMapping": {}}, headers=_headers()).status_code
        == 200
    )


@pytest.mark.parametrize(
    ("workspace", "connector"), [("ws_demo", "missing"), ("other", "registry")]
)
def test_missing_and_foreign_references_are_indistinguishable(
    client: TestClient, workspace: str, connector: str
) -> None:
    """A connector in another workspace is not an existence oracle."""
    response = client.post(
        f"/v1/workspaces/{workspace}/triggers",
        json={
            "sourceType": "pull",
            "workflowId": "wf",
            "sourceConfig": {"connectorInstanceId": connector},
        },
        headers=_headers(workspace),
    )
    assert response.status_code == 404
    assert "trigger.source_not_found" in response.text


def test_pause_and_delete_work_during_reference_outage(
    client: TestClient, references: References
) -> None:
    """Disabling a receiver never requires the upstream reference backend to recover."""
    created = client.post(
        _PATH,
        json={
            "sourceType": "pull",
            "workflowId": "wf",
            "sourceConfig": {"connectorInstanceId": "registry"},
        },
        headers=_headers(),
    )
    assert created.status_code == 201, created.text
    path = f"{_PATH}/{created.json()['subscriptionId']}"
    references.unavailable = True
    assert client.patch(path, json={"sourceType": "webhook"}, headers=_headers()).status_code == 503
    assert client.get(path, headers=_headers()).json()["sourceType"] == "pull"
    assert client.patch(path, json={"state": "paused"}, headers=_headers()).status_code == 200
    assert client.patch(path, json={"state": "active"}, headers=_headers()).status_code == 503
    assert client.get(path, headers=_headers()).json()["state"] == "paused"
    assert client.delete(path, headers=_headers()).status_code == 204


def test_unconfigured_reference_validation_fails_explicitly(
    metadata_store: InMemoryTriggerMetadataStore,
    workflow_auth: tuple[WorkflowCallerVerifier, dict[str, str]],
) -> None:
    """Memory mode must not silently accept arbitrary connector references."""
    app = create_app(
        authz_endpoint="",
        providers=Providers(metadata_store=metadata_store),
        receiver_configuration_enabled=True,
        workflow_caller_verifier=workflow_auth[0],
    )
    with TestClient(app, headers=workflow_auth[1]) as client:
        response = client.post("/RegisterResumeSubscription", json=_external())
    assert response.status_code == 503
    assert "trigger.source_unavailable" in response.text


@pytest.mark.parametrize(
    "patch",
    [
        {"sourceConfig": {"cron": "bad"}},
        {"sourceConfig": {"intervalSeconds": True}},
        {"sourceConfig": {"intervalSeconds": 0}},
        {"sourceConfig": {"intervalSeconds": 30, "timezone": "UTC"}},
        {"sourceConfig": {"intervalSeconds": 30, "credentials": {}}},
    ],
)
def test_invalid_patch_never_changes_existing_configuration(
    client: TestClient, patch: dict[str, object]
) -> None:
    """A rejected configuration/state edit leaves the previous complete record intact."""
    created = client.post(
        _PATH,
        json={
            "sourceType": "scheduled",
            "workflowId": "wf",
            "sourceConfig": {"intervalSeconds": 30},
        },
        headers=_headers(),
    )
    assert created.status_code == 201, created.text
    path = f"{_PATH}/{created.json()['subscriptionId']}"
    rejected = client.patch(path, json={**patch, "state": "paused"}, headers=_headers())
    assert rejected.status_code == 422, rejected.text
    assert client.get(path, headers=_headers()).json() == created.json()


async def test_mode_change_disables_managed_schedule(
    client: TestClient, metadata_store: InMemoryTriggerMetadataStore
) -> None:
    """Changing delivery mode commits the new source and old schedule eligibility together."""
    created = client.post(
        _PATH,
        json={
            "sourceType": "scheduled",
            "workflowId": "wf",
            "sourceConfig": {"intervalSeconds": 30},
        },
        headers=_headers(),
    )
    assert created.status_code == 201, created.text
    identifier = created.json()["subscriptionId"]
    changed = client.patch(
        f"{_PATH}/{identifier}",
        json={"sourceType": "pull", "sourceConfig": {"connectorInstanceId": "registry"}},
        headers=_headers(),
    )
    assert changed.status_code == 200, changed.text
    assert changed.json()["sourceType"] == "pull"
    schedule = await metadata_store.get_schedule(WorkspaceId("ws_demo"), identifier)
    assert schedule is not None and not schedule.enabled


async def test_rollout_gate_blocks_new_configuration_but_allows_disabling(
    metadata_store: InMemoryTriggerMetadataStore,
) -> None:
    """No managed receiver can be created before the explicit all-writers-upgraded gate."""
    providers = Providers(metadata_store=metadata_store)
    body = {"sourceType": "scheduled", "workflowId": "wf", "sourceConfig": {"intervalSeconds": 30}}
    with TestClient(create_app(authz_endpoint="", providers=providers)) as blocked:
        rejected = blocked.post(_PATH, json=body, headers=_headers())
        assert rejected.status_code == 503
        assert rejected.json()["code"] == "trigger.source_unavailable"
    assert (await metadata_store.query_subscriptions(WorkspaceId("ws_demo"))).items == ()
    with TestClient(
        create_app(authz_endpoint="", providers=providers, receiver_configuration_enabled=True)
    ) as enabled:
        created = enabled.post(_PATH, json=body, headers=_headers())
        assert created.status_code == 201, created.text
        identifier = created.json()["subscriptionId"]
    with TestClient(create_app(authz_endpoint="", providers=providers)) as disabled:
        path = f"{_PATH}/{identifier}"
        assert disabled.patch(path, json={"state": "paused"}, headers=_headers()).status_code == 200
        assert disabled.patch(path, json={"state": "active"}, headers=_headers()).status_code == 503
        assert disabled.delete(path, headers=_headers()).status_code == 204
    schedule = await metadata_store.get_schedule(WorkspaceId("ws_demo"), identifier)
    assert schedule is not None and not schedule.enabled


@pytest.mark.parametrize("configured", [False, True])
async def test_scoped_rpc_denies_direct_or_forged_callers_before_tenant_reads(
    metadata_store: InMemoryTriggerMetadataStore,
    references: References,
    workflow_auth: tuple[WorkflowCallerVerifier, dict[str, str]],
    monkeypatch: pytest.MonkeyPatch,
    configured: bool,
) -> None:
    """An unavailable authenticator or invalid credential cannot read or delete another wait."""
    identifier = rpc.compute_resume_id("run", "wait", "event")
    request = RegisterResumeRequest.model_validate(_external())
    await ResumeSubscriptionStore(metadata_store).register(
        ResumeRegistration.model_validate(request.model_dump(exclude={"ttl"})),
        workspace_id="ws_demo",
        resume_id=identifier,
        expires_at=_NOW + timedelta(days=1),
    )
    read = metadata_store.get_resume_subscription
    original = await read(WorkspaceId("ws_demo"), identifier)
    tracked = AsyncMock(wraps=read)
    monkeypatch.setattr(metadata_store, "get_resume_subscription", tracked)
    app = create_app(
        authz_endpoint="",
        providers=Providers(metadata_store=metadata_store, connector_references=references),
        receiver_configuration_enabled=True,
        workflow_caller_verifier=workflow_auth[0] if configured else None,
    )
    forged = {
        **_headers(),
        "dapr-caller-app-id": "workflow-service",
        "authorization": "Bearer forged",
    }
    with TestClient(app) as caller:
        register = caller.post("/RegisterResumeSubscription", json=_external(), headers=forged)
        cancel = caller.post(
            "/CancelResumeSubscription", json={**_TRIPLE, "workspaceId": "ws_demo"}, headers=forged
        )
    assert register.status_code == cancel.status_code == (403 if configured else 503)
    tracked.assert_not_awaited()
    assert await read(WorkspaceId("ws_demo"), identifier) == original


def test_authenticated_source_registration_still_requires_rollout_gate(
    metadata_store: InMemoryTriggerMetadataStore,
    references: References,
    workflow_auth: tuple[WorkflowCallerVerifier, dict[str, str]],
) -> None:
    """Authentication does not bypass the mixed-writer compatibility gate."""
    app = create_app(
        authz_endpoint="",
        providers=Providers(metadata_store=metadata_store, connector_references=references),
        workflow_caller_verifier=workflow_auth[0],
    )
    with TestClient(app, headers=workflow_auth[1]) as caller:
        response = caller.post("/RegisterResumeSubscription", json=_external())
    assert response.status_code == 503
    assert "upgrade all writers" in response.json()["detail"]


@pytest.mark.parametrize(
    ("path", "body"),
    [
        (_PATH, {"sourceType": "scheduled", "sourceConfig": {"intervalSeconds": 30}}),
        (
            "/CancelResumeSubscription",
            {"workspaceId": "ws_demo", "stepId": "wait", "eventKey": "event"},
        ),
    ],
)
def test_valid_source_fields_do_not_reclassify_unrelated_validation(
    client: TestClient, path: str, body: dict[str, object]
) -> None:
    """Missing workflow/run identity remains the established malformed-request error."""
    response = client.post(path, json=body, headers=_headers())
    assert response.status_code == 400
    assert response.json()["code"] == "trigger.api.bad_request"


async def test_deleted_receiver_cannot_be_reactivated(
    client: TestClient, metadata_store: InMemoryTriggerMetadataStore
) -> None:
    """Expired is terminal and neither active nor paused can revive a deleted schedule."""
    created = client.post(
        _PATH,
        json={
            "sourceType": "scheduled",
            "workflowId": "wf",
            "sourceConfig": {"intervalSeconds": 30},
        },
        headers=_headers(),
    )
    assert created.status_code == 201
    identifier = created.json()["subscriptionId"]
    path = f"{_PATH}/{identifier}"
    assert client.delete(path, headers=_headers()).status_code == 204
    for state in ("active", "paused"):
        response = client.patch(path, json={"state": state}, headers=_headers())
        assert response.status_code == 409, response.text
        assert "terminal" in response.json()["detail"]
    assert client.get(path, headers=_headers()).json()["state"] == "expired"
    schedule = await metadata_store.get_schedule(WorkspaceId("ws_demo"), identifier)
    assert schedule is not None and not schedule.enabled


@pytest.mark.parametrize(
    ("raw", "enabled"), [("", False), ("false", False), ("0", False), ("true", True), ("1", True)]
)
def test_rollout_flag_is_explicit(raw: str, enabled: bool) -> None:
    """Only explicit recognized true values enable the compatibility gate."""
    assert read_receiver_configuration_enabled({ENV_RECEIVER_CONFIGURATION_ENABLED: raw}) is enabled


def test_malformed_rollout_flag_fails_startup(monkeypatch: pytest.MonkeyPatch) -> None:
    """A typo cannot silently enable or disable a safety-critical rollout setting."""
    monkeypatch.setenv(ENV_RECEIVER_CONFIGURATION_ENABLED, "maybe")
    with (
        pytest.raises(SettingsError, match=ENV_RECEIVER_CONFIGURATION_ENABLED),
        TestClient(create_app(authz_endpoint="")),
    ):
        pass


@pytest.mark.parametrize(
    "patch",
    [
        {"workspaceId": None},
        {"workspaceId": "_resume"},
        {"selector": " "},
        {"source": {"connectorInstanceId": "registry", "deliveryModes": []}},
    ],
)
def test_external_wait_rejects_unscoped_or_unselective_input(
    client: TestClient, patch: dict[str, object]
) -> None:
    """External waits cannot match all traffic or use the legacy reserved workspace."""
    response = client.post("/RegisterResumeSubscription", json={**_external(), **patch})
    assert response.status_code == 422, response.text


@pytest.mark.parametrize("expired", [False, True])
async def test_source_rebinding_is_rejected_and_audited(
    client: TestClient,
    metadata_store: InMemoryTriggerMetadataStore,
    audit: Audit,
    monkeypatch: pytest.MonkeyPatch,
    expired: bool,
) -> None:
    """Live and expired keys retain their original source, selector and expiry."""
    monkeypatch.setattr(rpc, "_now", lambda: _NOW)
    registered = client.post("/RegisterResumeSubscription", json=_external())
    assert registered.status_code == 200, registered.text
    identifier = registered.json()["subscriptionId"]
    original = await metadata_store.get_resume_subscription(WorkspaceId("ws_demo"), identifier)
    if expired:
        monkeypatch.setattr(rpc, "_now", lambda: _NOW + timedelta(days=1))
    rejected = client.post("/RegisterResumeSubscription", json=_external(connector="different"))
    assert rejected.status_code == 409, rejected.text
    assert (
        await metadata_store.get_resume_subscription(WorkspaceId("ws_demo"), identifier) == original
    )
    assert audit.events[-1][0:2] == ("resume.subscription.divergent", "ws_demo")
    assert audit.events[-1][2]["reason"] == "source_rebinding"


async def test_scoped_registration_query_and_cancellation_are_isolated(
    client: TestClient,
    metadata_store: InMemoryTriggerMetadataStore,
    references: References,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Identical triples coexist in two tenants and the legacy partition without cross-cancel."""
    monkeypatch.setattr(rpc, "_now", lambda: _NOW)
    references.rows.add(("other", "registry"))
    first = client.post("/RegisterResumeSubscription", json=_external())
    second = client.post("/RegisterResumeSubscription", json=_external("other"))
    legacy = client.post("/RegisterResumeSubscription", json=_TRIPLE)
    assert first.status_code == second.status_code == legacy.status_code == 200
    identifier = first.json()["subscriptionId"]
    assert second.json()["subscriptionId"] == legacy.json()["subscriptionId"] == identifier
    page = await metadata_store.query_resume_subscriptions(
        WorkspaceId("ws_demo"), connector_instance_id="registry", delivery_mode="pull", as_of=_NOW
    )
    assert len(page.items) == 1
    assert (
        client.post(
            "/CancelResumeSubscription", json={**_TRIPLE, "workspaceId": "ws_demo"}
        ).status_code
        == 204
    )
    assert await metadata_store.get_resume_subscription(WorkspaceId("ws_demo"), identifier) is None
    assert (
        await metadata_store.get_resume_subscription(WorkspaceId("other"), identifier) is not None
    )
    assert (
        await metadata_store.get_resume_subscription(WorkspaceId("_resume"), identifier) is not None
    )
