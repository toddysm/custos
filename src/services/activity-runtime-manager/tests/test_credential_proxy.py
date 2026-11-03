"""Private sandbox proof is exchanged for fresh authority using only retained execution identity."""

import hashlib
import json
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from custos_callctx.execution import ExecutionBinding, ExecutionBindings, ExecutionIdentity
from custos_callctx.execution_client import ExecutionContextClient
from custos_spl.interfaces.activity_execution import ActivityExecutionRecord
from fastapi import HTTPException

from custos_arm.credential_proxy import CredentialProxy, ProxyRequest
from tests.test_durable_execution import SharedAttempts


@pytest.mark.parametrize(
    "failure", [None, "proof", "completed", "deadline", "workspace", "attempt"]
)
async def test_proxy_proof_and_fresh_control_context(failure: str | None) -> None:
    """Run the real secret/Auth client; sandbox coordinates cannot manufacture authority."""
    proof = "private-attempt-proof-" + "x" * 32
    identity = ExecutionIdentity("ws", "account", "stable")
    registry = ExecutionBindings(
        (ExecutionBinding(identity, "credential-source", "token"),),
        {"ws": "stable"},
    )
    store = SharedAttempts()
    now = datetime.now(UTC)
    snapshot = {
        "state": "running",
        "execution_identity": identity.to_dict(),
        "deadline": (now + timedelta(seconds=-1 if failure == "deadline" else 60)).isoformat(),
        "credential_grant_hash": hashlib.sha256(proof.encode()).hexdigest(),
    }
    row = ActivityExecutionRecord(
        "ws",
        "run",
        "step",
        1,
        "b" * 64,
        "a" * 32,
        json.dumps(snapshot),
        failure == "completed",
    )
    store.rows[("ws", "run", "step", 1)] = row
    calls: list[httpx.Request] = []

    def transport(request: httpx.Request) -> httpx.Response:
        """Simulate transport while the real proxy selects identity and acquires fresh grants."""
        calls.append(request)
        if "/v1.0/secrets/" in request.url.path:
            return httpx.Response(200, json={"token": "private-service-credential"})
        if request.url.path.endswith("/rpc/callctx.exchangeExecution"):
            assert json.loads(request.content) == {
                "workspaceId": "ws",
                "principalId": "account",
                "operation": "connector-credentials",
                "token": "private-service-credential",
            }
            timestamp = int(time.time())
            return httpx.Response(
                200,
                json={
                    "token": "e30.scoped.signature",
                    "iat": timestamp,
                    "exp": timestamp + 60,
                },
            )
        assert request.url.path == "/internal/v1/activity-credentials"
        assert request.headers["x-custos-callctx"] == "e30.scoped.signature"
        assert proof.encode() not in request.content
        assert "workspaceId" not in json.loads(request.content)
        return httpx.Response(
            200, json={"schemaVersion": "2", "credential": {"material": {"secret": "opaque"}}}
        )

    body = ProxyRequest.model_validate(
        {
            "workspaceId": "other" if failure == "workspace" else "ws",
            "runId": "run",
            "stepId": "step",
            "attempt": 2 if failure == "attempt" else 1,
            "operation": "issue",
            "slot": "source",
            "capability": "oci.pull",
        }
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
        proxy = CredentialProxy(
            store, ExecutionContextClient(http, registry), http, "http://connector"
        )
        if failure is not None:
            with pytest.raises(HTTPException) as denied:
                await proxy.forward(body, "Bearer " + ("y" * 43 if failure == "proof" else proof))
            assert denied.value.status_code == 403
            assert not calls
        else:
            for _ in range(2):
                result = await proxy.forward(body, "Bearer " + proof)
                assert result["schemaVersion"] == "2"
            assert len(calls) == 6
            store.rows[("ws", "run", "step", 1)] = replace(row, completed=True)
            with pytest.raises(HTTPException):
                await proxy.forward(body, "Bearer " + proof)
            assert len(calls) == 6
