"""Tests for the inbound ScheduleActivity RPC handler (ARM-IMPL-018)."""

from __future__ import annotations

import base64
import json
import time
from typing import Any

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from custos_callctx import CallContextVerifier, JwksCache
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.requests import Request

from custos_arm import create_app
from custos_arm.config import Settings
from custos_arm.middleware import CallContext, CallContextMiddleware, get_call_context
from custos_arm.result import ActivityResultEnvelope
from custos_arm.rpc.router import router
from custos_arm.scheduler import ActivityScheduler, CancelOutcome, ScheduleRequest
from tests.test_rpc_models import canonical_binding


class _RecordingScheduler(ActivityScheduler):
    """A stand-in scheduler that records dispatches and returns canned results.

    Overrides ``__init__`` so it stays a genuine :class:`ActivityScheduler`
    subclass (satisfying the adapter's ``isinstance`` guard) without building
    the heavy real dependency graph.
    """

    def __init__(
        self,
        *,
        result: ActivityResultEnvelope | None = None,
        cancel_outcome: CancelOutcome = CancelOutcome.ACCEPTED,
    ) -> None:
        self._result = result
        self._cancel_outcome = cancel_outcome
        self.scheduled: list[ScheduleRequest] = []
        self.cancelled: list[tuple[str, str, str]] = []

    async def schedule(self, request: ScheduleRequest) -> ActivityResultEnvelope:
        self.scheduled.append(request)
        assert self._result is not None
        return self._result

    async def cancel(self, *, workspace_id: str, run_id: str, step_id: str) -> CancelOutcome:
        self.cancelled.append((workspace_id, run_id, step_id))
        return self._cancel_outcome


def _success_envelope() -> ActivityResultEnvelope:
    return ActivityResultEnvelope.model_validate(
        {"class": "success", "attempt": 1, "outputs": {"ok": True}}
    )


def _callctx_header(workspace_id: str = "ws-1", principal_id: str = "p-1") -> str:
    return json.dumps({"workspace_id": workspace_id, "principal_id": principal_id})


def _client(settings: Settings, scheduler: ActivityScheduler | None) -> TestClient:
    app: FastAPI = create_app(settings)
    app.state.scheduler = scheduler
    return TestClient(app)


def _body(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "runId": "run-1",
        "stepId": "step-1",
        "attempt": 1,
        "activityRef": "acme/echo@1.0.0",
        "inputs": {"message": "hi"},
    }
    base.update(overrides)
    return base


@pytest.mark.parametrize("ready,dependencies_ready", [(True, False), (False, True), (False, False)])
@pytest.mark.parametrize("authorized", [True, False])
def test_cancellation_ignores_admission_but_keeps_authorization(
    ready: bool,
    dependencies_ready: bool,
    authorized: bool,
) -> None:
    """Unrelated dependency loss closes new admission, not authorized safety operations."""
    scheduler = _RecordingScheduler(result=_success_envelope())
    app = FastAPI()
    app.include_router(router)
    app.state.scheduler = scheduler
    app.state.runtime_managed = True
    app.state.ready, app.state.dependencies_ready = ready, dependencies_ready

    def verified_context(request: Request) -> CallContext:
        """Isolate real route admission/authorization from signature verification tested below."""
        request.state.signed_call_context = True
        return CallContext(
            workspace_id="ws-1",
            principal_id="p-1",
            permissions=frozenset(
                {"workflow:execute"} if authorized else {"catalog:workflows:read"}
            ),
        )

    app.dependency_overrides[get_call_context] = verified_context
    with TestClient(app) as client:
        scheduled = client.post("/ScheduleActivity", json=_body())
        cancelled = client.post("/CancelActivity", json={"runId": "run-1", "stepId": "step-1"})
    assert scheduled.status_code == (503 if authorized else 403)
    assert cancelled.status_code == (204 if authorized else 403)
    assert not scheduler.scheduled
    assert scheduler.cancelled == ([("ws-1", "run-1", "step-1")] if authorized else [])


@pytest.mark.parametrize("operation", ["catalog-read", "activity-execute"])
@pytest.mark.parametrize("method", ["ScheduleActivity", "CancelActivity"])
def test_execution_provenance_cannot_opt_out_with_a_legacy_body(
    operation: str, method: str
) -> None:
    """Verified execution contexts always require their pinned identity before dispatch."""
    scheduler = _RecordingScheduler(result=_success_envelope())
    app = FastAPI()
    app.include_router(router)
    app.state.scheduler = scheduler

    def verified_context(request: Request) -> CallContext:
        """Supply the verified middleware result to isolate the receiving-route guard."""
        request.state.signed_call_context = True
        return CallContext(
            workspace_id="ws-1",
            principal_id="p-1",
            permissions=frozenset(
                {"catalog:workflows:read" if operation == "catalog-read" else "workflow:execute"}
            ),
            execution_operation=operation,
        )

    app.dependency_overrides[get_call_context] = verified_context
    with TestClient(app) as client:
        response = client.post(
            "/" + method,
            json=_body(connectorContexts={})
            if method == "ScheduleActivity"
            else {"runId": "run-1", "stepId": "step-1"},
        )
    assert response.status_code == 403
    assert not scheduler.scheduled
    assert not scheduler.cancelled


@pytest.mark.parametrize(
    "operation,changed_field,status,legacy,method,permission",
    [
        ("activity-execute", None, 200, False, "ScheduleActivity", "workflow:execute"),
        (None, None, 403, False, "ScheduleActivity", "workflow:execute"),
        ("catalog-read", None, 403, False, "ScheduleActivity", "catalog:workflows:read"),
        ("activity-execute", "workspaceId", 403, False, "ScheduleActivity", "workflow:execute"),
        ("activity-execute", "principalId", 403, False, "ScheduleActivity", "workflow:execute"),
        (None, None, 403, True, "ScheduleActivity", "catalog:workflows:read"),
        (None, None, 403, True, "CancelActivity", "catalog:workflows:read"),
        (None, None, 200, True, "ScheduleActivity", "workflow:execute"),
        (None, None, 204, True, "CancelActivity", "workflow:execute"),
    ],
)
def test_real_signed_execution_binding_matches_the_scheduling_identity(
    operation: str | None,
    changed_field: str | None,
    status: int,
    legacy: bool,
    method: str,
    permission: str,
) -> None:
    """Verify actual signed permissions and scoped identity for both runtime RPC routes."""
    key = Ed25519PrivateKey.generate()
    public = base64.urlsafe_b64encode(key.public_key().public_bytes_raw()).decode().rstrip("=")

    async def fetch(url: str) -> tuple[dict[str, str], dict[str, Any]]:
        """Supply the public test key without replacing verification or authorization."""
        assert url == "http://auth/.well-known/jwks.json"
        return {"cache-control": "max-age=300"}, {
            "keys": [{"kty": "OKP", "crv": "Ed25519", "kid": "unit", "x": public}]
        }

    verifier = CallContextVerifier(
        jwks_cache=JwksCache(jwks_url="http://auth/.well-known/jwks.json", fetcher=fetch)
    )
    scheduler = _RecordingScheduler(result=_success_envelope())
    app = FastAPI()
    app.include_router(router)
    app.state.scheduler = scheduler
    app.add_middleware(
        CallContextMiddleware,
        authz_endpoint="http://auth",
        environment="production",
        verifier=verifier,
    )
    now = int(time.time())
    claims: dict[str, Any] = {
        "iss": "custos-auth",
        "aud": "custos.internal",
        "iat": now,
        "exp": now + 300,
        "jti": "unit",
        "workspaceId": "ws-1",
        "actingPrincipalId": "p-1",
        "callerComponent": "workflow-service",
        "permissions": [permission],
    }
    if operation is not None:
        claims["executionOperation"] = operation
    identity = {"workspaceId": "ws-1", "principalId": "p-1", "bindingId": "stable"}
    if changed_field:
        identity[changed_field] = "different"
    token = jwt.encode(claims, key, algorithm="EdDSA", headers={"kid": "unit"})
    payload = _body(executionIdentity=identity, connectorContexts={"registry": canonical_binding()})
    if legacy:
        payload = (
            _body() if method == "ScheduleActivity" else {"runId": "run-1", "stepId": "step-1"}
        )
    with TestClient(app) as client:
        response = client.post(
            "/" + method,
            json=payload,
            headers={"x-custos-callctx": token},
        )
    assert response.status_code == status, response.text
    assert len(scheduler.scheduled) == (1 if status == 200 else 0)
    assert len(scheduler.cancelled) == (1 if status == 204 else 0)


def test_schedule_activity_dispatches_and_returns_envelope(settings: Settings) -> None:
    scheduler = _RecordingScheduler(result=_success_envelope())
    client = _client(settings, scheduler)

    response = client.post(
        "/ScheduleActivity",
        json=_body(),
        headers={"x-custos-callctx": _callctx_header()},
    )

    assert response.status_code == 200
    assert response.json() == {
        "class": "success",
        "attempt": 1,
        "outputs": {"ok": True},
        "error": None,
    }
    assert len(scheduler.scheduled) == 1
    request = scheduler.scheduled[0]
    assert request.workspace_id == "ws-1"  # taken from the call context, not the body
    assert request.step.run_id == "run-1"
    assert request.step.step_id == "step-1"
    assert request.activity_ref == "acme/echo@1.0.0"
    assert request.inputs == {"message": "hi"}


def test_schedule_activity_translates_connector_contexts(settings: Settings) -> None:
    scheduler = _RecordingScheduler(result=_success_envelope())
    client = _client(settings, scheduler)

    body = _body(
        connectorContexts={
            "registry": {
                "slotName": "registry",
                "handle": "lease-abc",
                "connectorKind": "oci-registry",
            }
        }
    )
    response = client.post(
        "/ScheduleActivity",
        json=body,
        headers={"x-custos-callctx": _callctx_header()},
    )

    assert response.status_code == 200
    contexts = scheduler.scheduled[0].connector_contexts
    assert len(contexts) == 1
    ctx = contexts[0]
    assert ctx.slot_name == "registry"
    assert ctx.connector_type == "oci-registry"
    assert ctx.connector_instance_id == "lease-abc"
    assert ctx.lease_id == "lease-abc"
    assert dict(ctx.secrets) == {}


def test_invalid_private_binding_is_not_echoed(
    settings: Settings, caplog: pytest.LogCaptureFixture
) -> None:
    """Malformed opaque payloads fail before scheduling without returning their private data."""
    scheduler = _RecordingScheduler(result=_success_envelope())
    with _client(settings, scheduler) as client:
        response = client.post(
            "/ScheduleActivity",
            json=_body(
                connectorContexts={
                    "registry": {
                        **canonical_binding(),
                        "unexpected": "private-binding-value",
                    }
                }
            ),
            headers={"x-custos-callctx": _callctx_header()},
        )
    assert response.status_code == 422
    assert "private-binding-value" not in response.text
    assert "private-binding-value" not in caplog.text
    assert not scheduler.scheduled


def test_schedule_activity_idempotency_key_match_is_accepted(settings: Settings) -> None:
    scheduler = _RecordingScheduler(result=_success_envelope())
    client = _client(settings, scheduler)

    response = client.post(
        "/ScheduleActivity",
        json=_body(),
        headers={
            "x-custos-callctx": _callctx_header(),
            "Idempotency-Key": "run-1|step-1|1",
        },
    )

    assert response.status_code == 200


def test_schedule_activity_idempotency_key_mismatch_is_rejected(settings: Settings) -> None:
    scheduler = _RecordingScheduler(result=_success_envelope())
    client = _client(settings, scheduler)

    response = client.post(
        "/ScheduleActivity",
        json=_body(),
        headers={
            "x-custos-callctx": _callctx_header(),
            "Idempotency-Key": "run-1|step-1|9",
        },
    )

    assert response.status_code == 400
    assert scheduler.scheduled == []


def test_schedule_activity_missing_callctx_is_unauthorized(settings: Settings) -> None:
    scheduler = _RecordingScheduler(result=_success_envelope())
    client = _client(settings, scheduler)

    response = client.post("/ScheduleActivity", json=_body())

    assert response.status_code == 401
    assert scheduler.scheduled == []


def test_schedule_activity_invalid_body_is_unprocessable(settings: Settings) -> None:
    scheduler = _RecordingScheduler(result=_success_envelope())
    client = _client(settings, scheduler)

    body = _body()
    del body["activityRef"]
    response = client.post(
        "/ScheduleActivity",
        json=body,
        headers={"x-custos-callctx": _callctx_header()},
    )

    assert response.status_code == 422
    assert scheduler.scheduled == []


def test_schedule_activity_returns_503_when_scheduler_unset(settings: Settings) -> None:
    client = _client(settings, None)

    response = client.post(
        "/ScheduleActivity",
        json=_body(),
        headers={"x-custos-callctx": _callctx_header()},
    )

    assert response.status_code == 503
