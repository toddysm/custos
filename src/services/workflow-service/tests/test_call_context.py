"""Regress public Gateway trust, private Trigger provenance and the dev-only shim."""

from __future__ import annotations

import base64
import json
import time
from collections.abc import Iterator
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock
from uuid import UUID

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from custos_callctx import CallContextVerifier, InvalidCallContextError, InvalidReason, JwksCache
from custos_callctx.execution import ExecutionBinding, ExecutionBindings, ExecutionIdentity
from custos_spl.delivery_memory import MemoryDeliveryState
from custos_spl.ids import WorkspaceId
from custos_spl.interfaces.delivery import DeliveryStateProvider
from fastapi import FastAPI
from fastapi.testclient import TestClient
from httpx import Response
from starlette.requests import Request
from starlette.responses import JSONResponse

from custos_workflow.bindings.registry import InMemoryActivityTypeRegistry
from custos_workflow.call_context import (
    PRINCIPAL_HEADER,
    WORKSPACE_HEADER,
    CallContext,
    CallContextMiddleware,
)
from custos_workflow.clients.trigger import FakeTriggerServiceClient
from custos_workflow.document import WorkflowDocument
from custos_workflow.providers import RunComponents
from custos_workflow.runs.controller import WorkflowVersion
from custos_workflow.runs.errors import RunStateConflictError
from custos_workflow.runs.ids import derive_run_id
from custos_workflow.runs.model import RunStatus
from custos_workflow.runs.public_start import PublicRunStarter
from custos_workflow.runs.trigger_delivery import TriggerDeliveryController
from custos_workflow.runtime._common import ScheduleWorkflowRequest
from custos_workflow.validator import StartRunValidator
from custos_workflow.validator.errors import WorkspaceUnauthorizedError


def test_signed_gateway_context_reaches_public_handler_without_execution_bindings(
    monkeypatch: pytest.MonkeyPatch,
    fake_run_components: RunComponents,
) -> None:
    """Public verification must not depend on activating background execution bindings."""
    import custos_workflow.app as app_module

    key = Ed25519PrivateKey.generate()
    public = base64.urlsafe_b64encode(key.public_key().public_bytes_raw()).decode().rstrip("=")

    async def fetch(url: str) -> tuple[dict[str, str], dict[str, Any]]:
        """Serve the real verifier's synthetic signing key without network I/O."""
        assert url == "http://auth/.well-known/jwks.json"
        return {}, {
            "keys": [{"kty": "OKP", "crv": "Ed25519", "kid": "unit", "x": public}],
        }

    verifier = CallContextVerifier(
        jwks_cache=JwksCache(jwks_url="http://auth/.well-known/jwks.json", fetcher=fetch)
    )
    monkeypatch.setattr(app_module, "CallContextVerifier", lambda **kwargs: verifier)
    monkeypatch.setenv("WF_AUTHZ_ENDPOINT", "http://auth")
    monkeypatch.setenv("APP_API_TOKEN", "unit-sidecar-token")
    monkeypatch.setenv("WF_GATEWAY_NAMESPACE", "custos")
    monkeypatch.delenv("CUSTOS_EXECUTION_IDENTITY_ENABLED", raising=False)
    now = int(time.time())
    token = jwt.encode(
        {
            "iss": "custos-auth",
            "aud": "custos.internal",
            "iat": now,
            "exp": now + 300,
            "jti": "unit",
            "workspaceId": "ws",
            "actingPrincipalId": "operator",
            "callerComponent": "api-gateway",
            "permissions": ["run:read"],
        },
        key,
        algorithm="EdDSA",
        headers={"kid": "unit"},
    )
    app = app_module.create_app(require_call_context=True, run_components=fake_run_components)
    assert app.state.execution_bindings is None
    with TestClient(app) as client:
        response = client.get(
            "/v1/workspaces/ws/runs",
            headers={
                "x-custos-callctx": token,
                "x-correlation-id": "unit-correlation",
                "dapr-api-token": "unit-sidecar-token",
                "dapr-caller-app-id": "api-gateway",
                "dapr-caller-namespace": "custos",
            },
        )
    assert response.status_code == 200
    assert response.json() == {"items": [], "nextCursor": None}


def _mount_echo_app(*, require_call_context: bool) -> FastAPI:
    app = FastAPI()
    app.add_middleware(CallContextMiddleware, require_call_context=require_call_context)

    @app.get("/echo")
    async def echo(request: Request) -> JSONResponse:
        ctx = request.state.call_context
        assert isinstance(ctx, CallContext)
        return JSONResponse({"workspace": ctx.workspace, "principal": ctx.principal})

    @app.get("/healthz")
    async def healthz(request: Request) -> JSONResponse:
        # Bypassed by the middleware — request.state.call_context is NOT set.
        return JSONResponse({"has_ctx": hasattr(request.state, "call_context")})

    @app.get("/readyz")
    async def readyz(request: Request) -> JSONResponse:
        return JSONResponse({"has_ctx": hasattr(request.state, "call_context")})

    return app


# ---- Dev-mode behaviour -----------------------------------------------------


def test_dev_mode_missing_headers_yield_none_context() -> None:
    app = _mount_echo_app(require_call_context=False)
    with TestClient(app) as client:
        response = client.get("/echo")
    assert response.status_code == 200
    assert response.json() == {"workspace": None, "principal": None}


def test_dev_mode_present_headers_populate_context() -> None:
    app = _mount_echo_app(require_call_context=False)
    with TestClient(app) as client:
        response = client.get(
            "/echo",
            headers={
                WORKSPACE_HEADER: "ws-42",
                PRINCIPAL_HEADER: "alice",
            },
        )
    assert response.status_code == 200
    assert response.json() == {"workspace": "ws-42", "principal": "alice"}


def test_dev_mode_partial_headers_populate_what_is_present() -> None:
    app = _mount_echo_app(require_call_context=False)
    with TestClient(app) as client:
        response = client.get("/echo", headers={WORKSPACE_HEADER: "ws-42"})
    assert response.status_code == 200
    assert response.json() == {"workspace": "ws-42", "principal": None}


# ---- Production-mode behaviour ---------------------------------------------


@pytest.mark.parametrize(
    "headers",
    [{}, {PRINCIPAL_HEADER: "alice"}, {WORKSPACE_HEADER: "ws-42", PRINCIPAL_HEADER: "alice"}],
)
def test_required_context_without_verifier_never_falls_back_to_plain_headers(
    headers: dict[str, str],
) -> None:
    """Direct middleware users also fail closed when production verification is missing."""
    app = _mount_echo_app(require_call_context=True)
    with TestClient(app) as client:
        response = client.get("/echo", headers=headers)
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "callctx_unavailable"


# ---- Probe bypass -----------------------------------------------------------


@pytest.mark.parametrize("probe", ["/healthz", "/readyz"])
def test_probes_bypass_middleware_in_dev_mode(probe: str) -> None:
    app = _mount_echo_app(require_call_context=False)
    with TestClient(app) as client:
        response = client.get(probe)
    assert response.status_code == 200
    assert response.json() == {"has_ctx": False}


@pytest.mark.parametrize("probe", ["/healthz", "/readyz"])
def test_probes_bypass_middleware_in_production_mode(probe: str) -> None:
    app = _mount_echo_app(require_call_context=True)
    with TestClient(app) as client:
        # No headers — production mode still lets probes through.
        response = client.get(probe)
    assert response.status_code == 200
    assert response.json() == {"has_ctx": False}


# ---- Dataclass invariants ---------------------------------------------------


def test_call_context_is_frozen_dataclass() -> None:
    ctx = CallContext(workspace="ws-42", principal="alice")
    with pytest.raises(AttributeError):
        ctx.workspace = "other"  # type: ignore[misc]


def test_call_context_accepts_both_none() -> None:
    ctx = CallContext(workspace=None, principal=None)
    assert ctx.workspace is None
    assert ctx.principal is None


def _gateway_claims() -> dict[str, Any]:
    """Construct synthetic ordinary user claims, not private execution authority."""
    now = int(time.time())
    return {
        "iss": "custos-auth",
        "aud": "custos.internal",
        "iat": now,
        "exp": now + 300,
        "jti": "unit",
        "workspaceId": "ws",
        "actingPrincipalId": "operator",
        "callerComponent": "api-gateway",
        "permissions": ["run:read"],
    }


def _gateway_headers(key: Ed25519PrivateKey, claims: dict[str, Any]) -> list[tuple[str, str]]:
    """Model signed Gateway metadata plus identity supplied by its authenticated Dapr hop."""
    return [
        ("x-custos-callctx", jwt.encode(claims, key, algorithm="EdDSA", headers={"kid": "unit"})),
        ("x-correlation-id", "unit-correlation"),
        ("dapr-api-token", "unit-sidecar-token"),
        ("dapr-caller-app-id", "api-gateway"),
        ("dapr-caller-namespace", "custos"),
    ]


class _CallerCatalog:
    """Reject legacy reads and record the actual pinned identity selected by the public route."""

    def __init__(self) -> None:
        """Provide a pure Workflow; no downstream activity authority is fabricated."""
        self.calls: list[ExecutionIdentity] = []
        self.denied = False

    async def get_workflow_version(self, workspace: str, version: str) -> WorkflowVersion:
        """Make any validator/controller unsigned lookup fail this regression immediately."""
        raise AssertionError("public execution used the legacy Catalog path")

    async def get_execution_workflow_version(
        self, workspace: str, version: str, identity: ExecutionIdentity
    ) -> WorkflowVersion:
        """Record scoped reads and simulate a downstream authorization denial when requested."""
        self.calls.append(identity)
        if self.denied:
            raise WorkspaceUnauthorizedError("execution account cannot read Catalog")
        document = WorkflowDocument.model_validate(
            {
                "apiVersion": "custos.dev/v1",
                "kind": "Workflow",
                "metadata": {"name": "public", "workspace": workspace},
                "spec": {"steps": [{"id": "proof", "let": {"ok": "${{ true }}"}}]},
            }
        )
        return WorkflowVersion(version, workspace + "/public", "public", "1", document)


def _caller_bindings(*principals: str) -> ExecutionBindings:
    """Keep the background default distinct, including when a public caller lacks a binding."""
    entries = [
        ExecutionBinding(ExecutionIdentity("ws", principal, f"binding-{index}"), "secret", "token")
        for index, principal in enumerate(("background", *principals))
    ]
    return ExecutionBindings(tuple(entries), {"ws": "binding-0"})


@pytest.fixture
def caller_start(
    fake_run_components: RunComponents,
) -> Iterator[tuple[TestClient, Ed25519PrivateKey, RunComponents, _CallerCatalog]]:
    """Exercise signed entry, actual public routing and immutable start through the real factory."""
    from custos_workflow.app import create_app

    components = fake_run_components
    catalog = _CallerCatalog()
    components.start_run_validator = StartRunValidator(
        catalog=catalog, ledger=components.idempotency_ledger
    )
    provider = components.metadata_store
    assert isinstance(provider, DeliveryStateProvider)
    components.trigger_delivery_controller = TriggerDeliveryController(
        provider,
        components.run_store,
        components.workflow_client,
        InMemoryActivityTypeRegistry({}),
        components.lifecycle_publisher,
    )
    components.durable_resume_client = FakeTriggerServiceClient()
    key = Ed25519PrivateKey.generate()
    public = base64.urlsafe_b64encode(key.public_key().public_bytes_raw()).decode().rstrip("=")

    async def fetch(url: str) -> tuple[dict[str, str], dict[str, Any]]:
        """Publish the actual generated key to the shared signature verifier."""
        return {}, {"keys": [{"kty": "OKP", "crv": "Ed25519", "kid": "unit", "x": public}]}

    verifier = CallContextVerifier(
        jwks_cache=JwksCache(jwks_url="http://auth/.well-known/jwks.json", fetcher=fetch)
    )
    app = create_app(
        public_execution_enabled=True,
        run_components=components,
        execution_bindings=_caller_bindings("operator"),
        call_context_verifier=verifier,
        app_api_token="unit-sidecar-token",
        gateway_namespace="custos",
        trigger_namespace="custos",
    )
    with TestClient(app) as client:
        yield client, key, components, catalog


def _start_public(
    client: TestClient,
    key: Ed25519PrivateKey,
    *,
    principal: str = "operator",
    idempotency_key: str | None = "public-key",
    extra: dict[str, Any] | None = None,
) -> Response:
    """Issue a real signed public POST, retaining body/key validation at the HTTP boundary."""
    claims = _gateway_claims()
    claims.update(actingPrincipalId=principal, permissions=["workflow:execute"])
    body: dict[str, Any] = {"workflowVersionId": "ws/public@1"}
    if idempotency_key is not None:
        body["idempotencyKey"] = idempotency_key
    body.update(extra or {})
    response = client.post(
        "/v1/workspaces/ws/runs", headers=_gateway_headers(key, claims), json=body
    )
    assert isinstance(response, Response)
    return response


def test_signed_public_start_pins_caller_and_never_uses_background_default(
    caller_start: tuple[TestClient, Ed25519PrivateKey, RunComponents, _CallerCatalog],
) -> None:
    """Public entry reaches protocol3 with the caller pin, not either unsigned Catalog lookup."""
    client, key, components, catalog = caller_start
    first = _start_public(client, key)
    assert first.status_code == 202, first.text
    assert first.json()["runId"] == derive_run_id("ws", "public-key")
    assert catalog.calls == [ExecutionIdentity("ws", "operator", "binding-1")]
    controller = components.trigger_delivery_controller
    assert controller is not None and client.portal is not None
    saved = client.portal.call(
        controller.provider.get_workflow_start, WorkspaceId("ws"), first.json()["runId"]
    )
    assert saved is not None
    snapshot = json.loads(saved.input_json)
    assert snapshot["delivery_protocol"] == 3
    assert snapshot["execution_identity"] == catalog.calls[0].to_dict()
    second = _start_public(client, key)
    assert second.status_code == 202 and second.json()["runId"] == first.json()["runId"]
    assert len(catalog.calls) == 1
    conflict = _start_public(client, key, principal="background")
    assert conflict.status_code == 409
    changed = _start_public(client, key, extra={"inputs": {"edited": True}})
    assert changed.status_code == 409
    assert len(catalog.calls) == 1


@pytest.mark.parametrize("principals", [(), ("operator", "operator")])
def test_public_start_rejects_missing_or_ambiguous_caller_binding(
    caller_start: tuple[TestClient, Ed25519PrivateKey, RunComponents, _CallerCatalog],
    principals: tuple[str, ...],
) -> None:
    """Even a configured background default cannot authorize an unprovisioned public caller."""
    client, key, components, catalog = caller_start
    assert isinstance(client.app, FastAPI)
    controller = components.trigger_delivery_controller
    assert controller is not None
    client.app.state.public_run_starter = PublicRunStarter(
        _caller_bindings(*principals), controller, components.start_run_validator, timedelta(days=1)
    )
    response = _start_public(client, key)
    assert response.status_code == 503
    assert catalog.calls == []


def test_public_start_keyless_requests_keep_distinct_uuid4_ids(
    caller_start: tuple[TestClient, Ed25519PrivateKey, RunComponents, _CallerCatalog],
) -> None:
    """Sharing durable start must not turn keyless public calls into UUIDv5 deduplicated runs."""
    client, key, _, catalog = caller_start
    first = _start_public(client, key, idempotency_key=None)
    second = _start_public(client, key, idempotency_key=None)
    assert first.status_code == second.status_code == 202
    assert first.json()["runId"] != second.json()["runId"]
    assert UUID(first.json()["runId"]).version == UUID(second.json()["runId"]).version == 4
    assert len(catalog.calls) == 2


@pytest.mark.parametrize("error_type", [ValueError, RunStateConflictError])
def test_public_start_conflict_logs_only_its_source(
    caller_start: tuple[TestClient, Ed25519PrivateKey, RunComponents, _CallerCatalog],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    error_type: type[ValueError] | type[RunStateConflictError],
) -> None:
    """A rejected start keeps its HTTP semantics without logging the rejected value or message."""
    client, key, components, _ = caller_start

    async def reject(request: ScheduleWorkflowRequest) -> str:
        """Model a domain conflict or invalid backend value containing private diagnostic data."""
        raise error_type("private-start-value")

    monkeypatch.setattr(components.workflow_client, "schedule_new_workflow", reject)
    response = _start_public(client, key)
    assert response.status_code == 409
    assert (
        f"PUBLIC_START_REJECTED run={derive_run_id('ws', 'public-key')} "
        f"cause={error_type.__name__} origin=reject:"
    ) in caplog.text
    assert "private-start-value" not in caplog.text


def test_public_start_denied_catalog_grant_never_schedules(
    caller_start: tuple[TestClient, Ed25519PrivateKey, RunComponents, _CallerCatalog],
) -> None:
    """Gateway start authority cannot substitute for a denied scoped downstream read."""
    client, key, components, catalog = caller_start
    catalog.denied = True
    response = _start_public(client, key)
    assert response.status_code == 403
    assert client.portal is not None
    assert (
        client.portal.call(components.run_store.get_run, "ws", derive_run_id("ws", "public-key"))
        is None
    )


@pytest.mark.parametrize("field", ["executionIdentity", "permissions", "token"])
def test_public_start_rejects_forged_execution_fields(
    caller_start: tuple[TestClient, Ed25519PrivateKey, RunComponents, _CallerCatalog],
    field: str,
) -> None:
    """The unchanged public schema cannot select another account or assert downstream grants."""
    client, key, _, catalog = caller_start
    response = _start_public(client, key, extra={field: "forged"})
    assert response.status_code == 400
    assert catalog.calls == []


def test_public_replay_retains_original_binding_when_selection_becomes_ambiguous(
    caller_start: tuple[TestClient, Ed25519PrivateKey, RunComponents, _CallerCatalog],
) -> None:
    """Accepted work resolves its original pin instead of selecting from edited configuration."""
    client, key, components, catalog = caller_start
    first = _start_public(client, key)
    assert first.status_code == 202
    assert isinstance(client.app, FastAPI)
    controller = components.trigger_delivery_controller
    assert controller is not None
    client.app.state.public_run_starter = PublicRunStarter(
        _caller_bindings("operator", "operator"),
        controller,
        components.start_run_validator,
        timedelta(days=1),
    )
    replay = _start_public(client, key)
    assert replay.status_code == 202 and replay.json()["runId"] == first.json()["runId"]
    assert len(catalog.calls) == 1
    assert _start_public(client, key, idempotency_key="new-key").status_code == 503


def test_public_start_recovers_lost_runtime_ack_without_another_catalog_read(
    caller_start: tuple[TestClient, Ed25519PrivateKey, RunComponents, _CallerCatalog],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A signed public retry recovers accepted immutable work after a lost scheduling response."""
    client, key, components, catalog = caller_start
    original = components.workflow_client.schedule_new_workflow
    calls = 0

    async def lose_first_ack(request: ScheduleWorkflowRequest) -> str:
        """Lose the response after the real repository runtime has accepted the instance."""
        nonlocal calls
        result = await original(request)
        calls += 1
        if calls == 1:
            raise OSError("synthetic response loss")
        return result

    monkeypatch.setattr(components.workflow_client, "schedule_new_workflow", lose_first_ack)
    assert _start_public(client, key).status_code == 503
    recovered = _start_public(client, key)
    assert recovered.status_code == 202
    assert recovered.json()["runId"] == derive_run_id("ws", "public-key")
    assert calls == 2 and len(catalog.calls) == 1


def test_public_completed_replay_survives_missing_receipt_without_scheduling(
    caller_start: tuple[TestClient, Ed25519PrivateKey, RunComponents, _CallerCatalog],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Receipt garbage collection cannot cause completed public work to execute again."""
    client, key, components, catalog = caller_start
    assert _start_public(client, key).status_code == 202
    run_id = derive_run_id("ws", "public-key")
    assert client.portal is not None
    client.portal.call(components.run_store.update_run_status, "ws", run_id, RunStatus.SUCCEEDED)
    provider = components.metadata_store
    assert isinstance(provider, MemoryDeliveryState)
    provider._delivery_rows.pop(("ws", "workflow-start", run_id))
    schedule = AsyncMock(side_effect=AssertionError("completed work was scheduled again"))
    monkeypatch.setattr(components.workflow_client, "schedule_new_workflow", schedule)
    response = _start_public(client, key)
    assert response.status_code == 202 and response.json()["runId"] == run_id
    assert response.json()["status"] == "succeeded"
    schedule.assert_not_awaited()
    assert len(catalog.calls) == 1


def _handler_entries(client: TestClient) -> int:
    """Read the typed test application's downstream-entry counter."""
    assert isinstance(client.app, FastAPI)
    entries = client.app.state.entered
    assert isinstance(entries, int)
    return entries


@pytest.fixture
def public_boundary() -> Iterator[tuple[TestClient, Ed25519PrivateKey, CallContextVerifier]]:
    """Use real signature verification and count every downstream handler entry."""
    key = Ed25519PrivateKey.generate()
    public = base64.urlsafe_b64encode(key.public_key().public_bytes_raw()).decode().rstrip("=")

    async def fetch(url: str) -> tuple[dict[str, str], dict[str, Any]]:
        """Publish only the generated public key for the real shared verifier."""
        return {}, {
            "keys": [{"kty": "OKP", "crv": "Ed25519", "kid": "unit", "x": public}],
        }

    verifier = CallContextVerifier(
        jwks_cache=JwksCache(jwks_url="http://auth/.well-known/jwks.json", fetcher=fetch)
    )
    app = FastAPI()
    app.state.entered = 0
    app.add_middleware(
        CallContextMiddleware,
        # A configured verifier must enforce trust even without the legacy flag.
        require_call_context=False,
        call_context_verifier=verifier,
        app_api_token="unit-sidecar-token",
        gateway_namespace="custos",
    )

    @app.api_route("/{path:path}", methods=["GET", "POST", "DELETE"])
    async def echo(request: Request) -> JSONResponse:
        """Expose only the identity actually accepted by the middleware."""
        app.state.entered += 1
        context = request.state.call_context
        return JSONResponse(
            {
                "workspace": context.workspace,
                "principal": context.principal,
                "executionVerified": context.execution_verified,
            }
        )

    with TestClient(app) as client:
        yield client, key, verifier


@pytest.mark.parametrize(
    "method,path,permission",
    [
        ("POST", "/v1/workspaces/ws/runs", "workflow:execute"),
        ("GET", "/v1/workspaces/ws/runs", "run:read"),
        ("GET", "/v1/workspaces/ws/runs/opaque", "run:read"),
        ("POST", "/v1/workspaces/ws/runs/opaque:cancel", "run:cancel"),
        ("GET", "/v1/workspaces/ws/runs/opaque/steps/one", "run:read"),
        ("GET", "/v1/workspaces/ws/runs/opaque/steps/one/logs", "run:read"),
    ],
)
def test_public_routes_require_their_exact_grant_and_ignore_plain_identity(
    public_boundary: tuple[TestClient, Ed25519PrivateKey, CallContextVerifier],
    method: str,
    path: str,
    permission: str,
) -> None:
    """Each public operation rejects a different grant before accepting verified identity."""
    client, key, _ = public_boundary
    claims = _gateway_claims()
    claims["permissions"] = ["trigger:admin"]
    denied = client.request(method, path, headers=_gateway_headers(key, claims))
    assert denied.status_code == 403
    assert _handler_entries(client) == 0
    claims["permissions"] = [permission]
    headers = [
        *_gateway_headers(key, claims),
        (WORKSPACE_HEADER, "spoofed-workspace"),
        (PRINCIPAL_HEADER, "spoofed-principal"),
    ]
    response = client.request(method, path, headers=headers)
    assert response.status_code == 200
    assert response.json() == {
        "workspace": "ws",
        "principal": "operator",
        "executionVerified": False,
    }
    assert _handler_entries(client) == 1


@pytest.mark.parametrize(
    "claim,value,status",
    [
        ("workspaceId", "other", 403),
        ("workspaceId", None, 403),
        ("callerComponent", "trigger-service", 403),
        ("permissions", [], 403),
        ("exp", 1, 401),
        ("aud", "other-service", 401),
        ("iss", "untrusted", 401),
        ("executionOperation", "workflow-cancel", 403),
    ],
)
def test_public_claim_rejections_precede_any_handler(
    public_boundary: tuple[TestClient, Ed25519PrivateKey, CallContextVerifier],
    claim: str,
    value: Any,
    status: int,
) -> None:
    """Invalid authority and valid-but-wrong tenant/grants never reach protected work."""
    client, key, _ = public_boundary
    claims = _gateway_claims()
    claims[claim] = value
    if claim == "executionOperation":
        claims["permissions"] = ["run:cancel"]
    response = client.get("/v1/workspaces/ws/runs", headers=_gateway_headers(key, claims))
    assert response.status_code == status
    assert _handler_entries(client) == 0


@pytest.mark.parametrize(
    "name,value,duplicate,status",
    [
        ("dapr-api-token", None, False, 401),
        ("dapr-api-token", "wrong", False, 401),
        ("dapr-api-token", "unit-sidecar-token", True, 401),
        ("dapr-caller-app-id", None, False, 403),
        ("dapr-caller-app-id", "trigger-service", False, 403),
        ("dapr-caller-app-id", "api-gateway", True, 403),
        ("dapr-caller-namespace", None, False, 403),
        ("dapr-caller-namespace", "other", False, 403),
        ("dapr-caller-namespace", "custos", True, 403),
        ("x-custos-callctx", None, False, 401),
        ("x-custos-callctx", "malformed", False, 401),
        ("x-custos-callctx", "duplicate", True, 401),
    ],
)
def test_public_header_rejections_cannot_fall_back_to_legacy_identity(
    public_boundary: tuple[TestClient, Ed25519PrivateKey, CallContextVerifier],
    name: str,
    value: str | None,
    duplicate: bool,
    status: int,
) -> None:
    """Authentication uses duplicate-preserving headers, not caller-provided identity hints."""
    client, key, _ = public_boundary
    headers = _gateway_headers(key, _gateway_claims())
    if not duplicate:
        headers = [(key, entry) for key, entry in headers if key != name]
    if value is not None:
        headers.append((name, value))
    headers += [(WORKSPACE_HEADER, "ws"), (PRINCIPAL_HEADER, "operator")]
    response = client.get("/v1/workspaces/ws/runs", headers=headers)
    assert response.status_code == status
    assert _handler_entries(client) == 0


def test_public_wrong_signature_and_jwks_outage_remain_distinct(
    public_boundary: tuple[TestClient, Ed25519PrivateKey, CallContextVerifier],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Invalid credentials return 401; unavailable verification keys return explicit 503."""
    client, key, verifier = public_boundary
    response = client.get(
        "/v1/workspaces/ws/runs",
        headers=_gateway_headers(Ed25519PrivateKey.generate(), _gateway_claims()),
    )
    assert response.status_code == 401
    monkeypatch.setattr(
        verifier,
        "verify",
        AsyncMock(side_effect=InvalidCallContextError(InvalidReason.JWKS_UNAVAILABLE, "private")),
    )
    response = client.get(
        "/v1/workspaces/ws/runs", headers=_gateway_headers(key, _gateway_claims())
    )
    assert response.status_code == 503
    assert "private" not in response.text
    assert _handler_entries(client) == 0


@pytest.mark.parametrize(
    "method,path",
    [
        ("POST", "/internal/runs:start"),
        ("POST", "/internal/runs/opaque/steps/one:raiseEvent"),
        ("POST", "/v1/workspaces/ws/runs/opaque/steps/one"),
        ("DELETE", "/v1/workspaces/ws/runs"),
        ("GET", "/v1/workspaces/ws/runs/opaque/extra"),
    ],
)
def test_public_context_cannot_authorize_private_or_unrecognized_operations(
    public_boundary: tuple[TestClient, Ed25519PrivateKey, CallContextVerifier],
    method: str,
    path: str,
) -> None:
    """A signed public grant is not a private execution grant or a route-prefix bypass."""
    client, key, _ = public_boundary
    claims = _gateway_claims()
    claims["permissions"] = ["workflow:execute", "run:read", "run:cancel"]
    response = client.request(method, path, headers=_gateway_headers(key, claims))
    assert response.status_code == 403
    assert _handler_entries(client) == 0


@pytest.mark.parametrize(
    "operation,permission,path,expected_status",
    [
        (None, "workflow:execute", "/internal/runs:start", 403),
        ("workflow-start", "workflow:execute", "/internal/runs:start", 200),
        ("workflow-resume", "workflow:execute", "/internal/runs:start", 403),
        ("workflow-cancel", "run:cancel", "/internal/runs/root:cancel", 200),
        ("workflow-start", "workflow:execute", "/internal/runs/root:cancel", 403),
        (None, "run:cancel", "/internal/runs/root:cancel", 403),
        ("workflow-cancel", "workflow:execute", "/internal/runs/root:cancel", 401),
        ("workflow-cancel", "run:cancel", "/internal/runs:start", 403),
        ("workflow-cancel", "run:cancel", "/internal/runs:raiseEvent", 403),
    ],
)
def test_private_delivery_requires_signed_operation_provenance(
    operation: str | None, permission: str, path: str, expected_status: int
) -> None:
    """Only a valid operation-specific grant can cross its matching private boundary."""
    key = Ed25519PrivateKey.generate()
    public = base64.urlsafe_b64encode(key.public_key().public_bytes_raw()).decode().rstrip("=")

    async def fetch(url: str) -> tuple[dict[str, str], dict[str, Any]]:
        """Serve the actual signing key's public portion without a network dependency."""
        assert url == "http://auth/.well-known/jwks.json"
        return {"cache-control": "max-age=300"}, {
            "keys": [{"kty": "OKP", "crv": "Ed25519", "kid": "unit", "x": public}],
        }

    app = FastAPI()
    app.state.entered = 0
    app.add_middleware(
        CallContextMiddleware,
        require_call_context=True,
        execution_verifier=CallContextVerifier(
            jwks_cache=JwksCache(
                jwks_url="http://auth/.well-known/jwks.json",
                fetcher=fetch,
            )
        ),
        app_api_token="unit-sidecar-token",
        trigger_namespace="custos",
    )

    @app.post("/internal/{route:path}")
    async def start(request: Request) -> JSONResponse:
        """Record entry only after the actual private-delivery authentication boundary."""
        app.state.entered += 1
        context = request.state.call_context
        return JSONResponse(
            {
                "principal": context.principal,
                "workspace": context.workspace,
                "verified": context.execution_verified,
            }
        )

    now = int(time.time())
    claims: dict[str, Any] = {
        "iss": "custos-auth",
        "aud": "custos.internal",
        "iat": now,
        "exp": now + 300,
        "jti": "unit",
        "workspaceId": "ws",
        "actingPrincipalId": "executor",
        "callerComponent": "trigger-service",
        "permissions": [permission],
    }
    if operation is not None:
        claims["executionOperation"] = operation
    token = jwt.encode(claims, key, algorithm="EdDSA", headers={"kid": "unit"})
    with TestClient(app) as client:
        response = client.post(
            path,
            headers={
                "x-custos-callctx": token,
                "dapr-api-token": "unit-sidecar-token",
                "dapr-caller-app-id": "trigger-service",
                "dapr-caller-namespace": "custos",
            },
        )
    assert response.status_code == expected_status
    assert app.state.entered == int(expected_status == 200)
    if expected_status == 200:
        assert response.json() == {"principal": "executor", "workspace": "ws", "verified": True}
