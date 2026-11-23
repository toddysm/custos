"""Prove a public signed-context Workflow lifecycle using actual deployed services.

Database access stays in the seed Job and actual Workflow service. The client inherits the
chart's public Helm-test network identity and receives a workspace operator
credential, never database, Kubernetes or mesh credentials. Synthetic definition
data does not replace Gateway/Auth/Dapr/Workflow transport or execution. A
separate runner probe uses the actual Trigger container/sidecar for signed
cancellation and the Workflow container's production client to confirm runtime
termination; neither private authority is given to the public client. An opt-in
controlled invalid-event proof retains failed-step diagnostics separately from
ordinary lifecycle success and the SDK's public status projection.
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import hashlib
import json
import os
import re
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

WORKSPACE = "workspace-default"
WORKFLOW = "public-context-proof"
WAITING_WORKFLOW = "private-cancellation-proof"
OPERATOR = "public-context-operator"
RESOURCE = "smoke-public-workflow"
EXECUTION_RESOURCE = RESOURCE + "-execution"
REPORT_PREFIX = "PUBLIC_WORKFLOW_EVIDENCE "
DIAGNOSTIC_PREFIX = "PUBLIC_WORKFLOW_TERMINAL "
CANDIDATE_PREFIX = "PUBLIC_WORKFLOW_CANDIDATE "
REQUEST_PREFIX = "PUBLIC_WORKFLOW_REQUEST "
FAILURE_MARKERS = ("synthetic-credential-Error", "synthetic-payload-Exception")
DIAGNOSTIC_ERROR_TYPES = frozenset(
    {
        "WorkflowRuntimeUnavailableError",
        "RunStateConflictError",
        "ValueError",
        "BackendUnavailable",
        "LeaseBusy",
        "LeaseExpired",
        "MigrationRequired",
        "ImmutableViolation",
        "OutboundRpcStatusError",
        "OutboundRpcTransportError",
        "OutboundRpcDecodeError",
        "OSError",
        "TimeoutError",
        "LifecycleEventPublishError",
        "_InactiveRpcError",
        "TaskFailedError",
        "RuntimeError",
        "TypeError",
        "AttributeError",
        "KeyError",
        "unrecognized",
    }
)


def workflow_document() -> dict[str, Any]:
    """Return a pure Workflow that must complete in the actual worker."""
    return {
        "apiVersion": "custos.dev/v1",
        "kind": "Workflow",
        "metadata": {"name": WORKFLOW, "workspace": WORKSPACE},
        "spec": {"steps": [{"id": "proof", "let": {"observed": "${{ true }}"}}]},
    }


def waiting_workflow_document() -> dict[str, Any]:
    """Keep the real worker waiting for a never-sent event, bounded by a five-minute TTL."""
    document = workflow_document()
    document["metadata"]["name"] = WAITING_WORKFLOW
    document["spec"]["steps"] = [
        {
            "id": "await-cancellation",
            "waitFor": {"eventKey": "${{ 'never-sent' }}", "ttl": "PT5M"},
        }
    ]
    return document


async def seed() -> None:
    """Seed grants and definitions using their separate native-JSON and serialized-JSON contracts."""
    from datetime import UTC, datetime, timedelta

    from custos_auth.roles import ROLE_WORKSPACE_OPERATOR
    from custos_auth.tokens import hash_token, looks_like_custos_token
    from custos_bootstrap.__main__ import resolve_dsn
    from custos_pg import PgAuthAdapter, PgDefinitionAdapter
    from custos_pg.pool import LazyPool, register_jsonb_codec
    from custos_spl.ids import PrincipalId, RoleBindingId, ServiceTokenId, WorkspaceId
    from custos_spl.interfaces.auth_store import (
        RoleBinding,
        ServiceAccount,
        ServiceToken,
        WorkspaceScope,
    )

    token = os.environ["CUSTOS_TEST_TOKEN"]
    execution_token = os.environ["CUSTOS_TEST_EXECUTION_TOKEN"]
    dsn = resolve_dsn(os.environ)
    if (
        not dsn
        or not looks_like_custos_token(token)
        or not looks_like_custos_token(execution_token)
        or token == execution_token
    ):
        raise ValueError(
            "seed requires a database reference and canonical fixture credential"
        )
    pool = LazyPool(dsn, init=register_jsonb_codec)
    auth = PgAuthAdapter(lazy=pool)
    now = datetime.now(UTC)
    try:
        await auth.put_principal(
            ServiceAccount(
                kind="serviceAccount",
                principal_id=PrincipalId(OPERATOR),
                workspace_id=WorkspaceId(WORKSPACE),
                display_name="Disposable public API proof",
                disabled_at=None,
                disabled_reason=None,
                created_at=now,
            )
        )
        await auth.put_role_binding(
            RoleBinding(
                binding_id=RoleBindingId(OPERATOR),
                principal_id=PrincipalId(OPERATOR),
                role_id=ROLE_WORKSPACE_OPERATOR,
                scope=WorkspaceScope(WorkspaceId(WORKSPACE)),
                bound_at=now,
                bound_by=PrincipalId(OPERATOR),
            )
        )
        for purpose, credential in (("public", token), ("execution", execution_token)):
            await auth.put_service_token(
                ServiceToken(
                    token_id=ServiceTokenId(OPERATOR + "-" + purpose),
                    service_account_id=PrincipalId(OPERATOR),
                    hash=hash_token(credential),
                    issued_at=now,
                    expires_at=now + timedelta(hours=2),
                    revoked_at=None,
                    revoked_by=None,
                    revoked_reason=None,
                )
            )
    finally:
        await pool.aclose()
    definition_pool = LazyPool(dsn)
    try:
        definitions = PgDefinitionAdapter(lazy=definition_pool)
        for name, document in (
            (WORKFLOW, workflow_document()),
            (WAITING_WORKFLOW, waiting_workflow_document()),
        ):
            await definitions.put_workflow_version(WORKSPACE, name, "1", document)
    finally:
        await definition_pool.aclose()
    print("Synthetic definition and workspace-only public operator seeded.", flush=True)


def request(
    method: str, path: str, body: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Require the public response contract without printing credentials or response payloads."""
    sha = os.environ["GITHUB_SHA"]
    correlation = str(
        uuid5(NAMESPACE_URL, f"custos-public-proof:{sha}:{method}:{path}")
    )
    record = {
        "sha": sha,
        "correlationId": correlation,
        "method": method,
        "httpStatus": None,
    }
    print(REQUEST_PREFIX + json.dumps(record), flush=True)
    headers = {
        "authorization": "Bearer " + os.environ["CUSTOS_TEST_TOKEN"],
        "accept": "application/json",
        "x-correlation-id": correlation,
    }
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        headers["content-type"] = "application/json"
        headers["idempotency-key"] = "public-context-proof-" + os.environ["GITHUB_SHA"]
    outbound = urllib.request.Request(
        "http://custos-api-gateway:8080" + path,
        data=data,
        headers=headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(outbound, timeout=30) as response:
            status, raw = response.status, response.read()
    except urllib.error.HTTPError as error:
        with error:
            status, raw = error.code, error.read()
    record["httpStatus"] = status
    print(REQUEST_PREFIX + json.dumps(record), flush=True)
    if status != (202 if method == "POST" else 200):
        code = "unclassified"
        try:
            problem = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            code = "non-json-error"
        else:
            if isinstance(problem, dict):
                candidate = problem.get("code")
                if isinstance(problem.get("error"), dict):
                    candidate = problem["error"].get("code", candidate)
                if isinstance(candidate, str) and re.fullmatch(
                    r"[\w.:-]{1,100}", candidate
                ):
                    code = candidate
        raise RuntimeError(f"public {method}: HTTP {status} ({code})")
    payload: object = json.loads(raw)
    if not isinstance(payload, dict):
        raise TypeError("public response must be a JSON object")
    return payload


def exercise(*, controlled_failure: bool = False) -> dict[str, Any]:
    """Require public completion/readback and a separate candidate for the selected wait proof."""
    prefix = f"/v1/workspaces/{WORKSPACE}/runs"
    started = request(
        "POST",
        prefix,
        {"workflowVersionId": f"{WORKSPACE}/{WORKFLOW}@1", "inputs": {}},
    )
    run_id = started.get("runId")
    if not isinstance(run_id, str) or UUID(run_id).version != 5:
        raise ValueError("public start did not return an opaque run identifier")
    deadline = time.monotonic() + 120
    while True:
        observed = request("GET", prefix + "/" + run_id)
        status = observed.get("status")
        if status == "succeeded":
            break
        if status in {"failed", "cancelled"}:
            raise RuntimeError("public Workflow terminated without success")
        if time.monotonic() >= deadline:
            raise RuntimeError("public Workflow did not complete before its deadline")
        time.sleep(2)
    if (
        observed.get("runId") != run_id
        or observed.get("workspaceId") != WORKSPACE
        or observed.get("workflowVersionId") != f"{WORKSPACE}/{WORKFLOW}@1"
    ):
        raise ValueError("public readback does not match the started Workflow")
    listed = request("GET", prefix)
    if not any(
        isinstance(item, dict) and item.get("runId") == run_id
        for item in listed.get("items", [])
    ):
        raise RuntimeError("public list did not contain the completed run")
    return {
        "sha": os.environ["GITHUB_SHA"],
        "publicWorkflowCompleted": True,
        "publicReadbackVerified": True,
        "runId": run_id,
        "cancellationRunId": start_waiting_run(controlled_failure=controlled_failure),
        "fullReceiverCertification": False,
    }


def start_waiting_run(*, controlled_failure: bool = False) -> str:
    """Require a running candidate; the failure fixture never reuses the cancelled run's key."""
    prefix = f"/v1/workspaces/{WORKSPACE}/runs"
    started = request(
        "POST",
        prefix,
        {
            "workflowVersionId": f"{WORKSPACE}/{WAITING_WORKFLOW}@1",
            "idempotencyKey": WAITING_WORKFLOW
            + "-"
            + os.environ["GITHUB_SHA"]
            + ("-failure" if controlled_failure else ""),
        },
    )
    run_id = started.get("runId")
    if not isinstance(run_id, str) or UUID(run_id).version != 5:
        raise ValueError("cancellation start did not return an opaque run identifier")
    print(
        CANDIDATE_PREFIX
        + json.dumps({"sha": os.environ["GITHUB_SHA"], "runId": run_id}),
        flush=True,
    )
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        observed = request("GET", prefix + "/" + run_id)
        if observed.get("status") == "running":
            return run_id
        if observed.get("status") in {"succeeded", "failed", "cancelled"}:
            print(
                DIAGNOSTIC_PREFIX
                + json.dumps(
                    {
                        "sha": os.environ["GITHUB_SHA"],
                        "runId": run_id,
                        "status": observed["status"],
                    }
                ),
                flush=True,
            )
            raise RuntimeError(
                "cancellable Workflow terminated before the private proof"
            )
        time.sleep(2)
    raise RuntimeError("cancellable Workflow never became running")


def cancellation_readback(run_id: str) -> dict[str, Any]:
    """Require terminal public cancellation, not a 202 or in-progress cancelling record."""
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        observed = request("GET", f"/v1/workspaces/{WORKSPACE}/runs/{run_id}")
        if observed.get("status") == "cancelled":
            if (
                observed.get("runId") != run_id
                or observed.get("workspaceId") != WORKSPACE
                or observed.get("workflowVersionId")
                != f"{WORKSPACE}/{WAITING_WORKFLOW}@1"
            ):
                raise ValueError("cancelled public readback has the wrong identity")
            return {
                "sha": os.environ["GITHUB_SHA"],
                "runId": run_id,
                "publicCancellationReadback": True,
            }
        if observed.get("status") in {"succeeded", "failed"}:
            raise RuntimeError("cancellable Workflow ended without cancellation")
        time.sleep(2)
    raise RuntimeError("public cancellation readback never became terminal")


def failure_readback(run_id: str) -> dict[str, Any]:
    """Retain exact-fixture terminal public projection, not a claim of failed-step success."""
    deadline = time.monotonic() + 120
    while True:
        observed = request("GET", f"/v1/workspaces/{WORKSPACE}/runs/{run_id}")
        if (
            observed.get("runId") != run_id
            or observed.get("workspaceId") != WORKSPACE
            or observed.get("workflowVersionId") != f"{WORKSPACE}/{WAITING_WORKFLOW}@1"
        ):
            raise ValueError("failure readback does not match the controlled wait")
        status = observed.get("status")
        if status in {"succeeded", "failed"}:
            return {
                "sha": os.environ["GITHUB_SHA"],
                "runId": run_id,
                "publicFailureReadbackObserved": True,
                "publicStatus": status,
            }
        if status == "cancelled" or time.monotonic() >= deadline:
            raise RuntimeError(
                "controlled wait has no expected terminal public readback"
            )
        time.sleep(2)


async def private_cancellation(run_id: str, sha: str) -> dict[str, Any]:
    """Exchange the configured account through the real Trigger sidecar, then invoke its RPC."""
    import httpx
    from custos_callctx.execution import load_execution_bindings
    from custos_callctx.execution_client import ExecutionContextClient

    bindings = load_execution_bindings(os.environ)
    if bindings is None:
        raise ValueError("private proof requires configured execution bindings")
    identities = [
        identity
        for identity in bindings.identities
        if identity.workspace_id == WORKSPACE
        and identity.principal_id == OPERATOR
        and identity.binding_id == "public-context-v1"
    ]
    if len(identities) != 1:
        raise ValueError(
            "private proof requires exactly its explicitly provisioned account"
        )
    async with httpx.AsyncClient(follow_redirects=False, timeout=70) as client:
        execution = ExecutionContextClient.from_env(client, bindings, os.environ)
        headers = await execution.headers(identities[0], "workflow-cancel")
        response = await client.post(
            f"http://{os.environ['DAPR_HTTP_HOST']}:{os.environ['DAPR_HTTP_PORT']}"
            f"/v1.0/invoke/{os.environ['TRIGGER_WORKFLOW_APP_ID']}"
            f"/method/internal/runs/{run_id}:cancel",
            headers=headers,
            json={
                "workspaceId": WORKSPACE,
                "reason": "disposable signed cancellation proof",
            },
        )
        if response.status_code != 202:
            raise RuntimeError(f"private cancellation: HTTP {response.status_code}")
        payload = response.json()
        if (
            not isinstance(payload, dict)
            or payload.get("runId") != run_id
            or payload.get("workspaceId") != WORKSPACE
            or payload.get("status") != "cancelled"
        ):
            raise ValueError(
                "private cancellation did not confirm the requested terminal run"
            )
    return {"sha": sha, "runId": run_id, "privateCancellationVerified": True}


async def runtime_termination(run_id: str, sha: str) -> dict[str, Any]:
    """Require independent runtime termination and retain only the accepted snapshot's digest."""
    from custos_workflow.runtime._common import GetRunStateRequest, RunStatus
    from custos_workflow.runtime.dapr import WorkflowClient

    client = WorkflowClient()
    try:
        async with asyncio.timeout(60):
            state = await client.get_workflow_state(
                GetRunStateRequest(instance_id=run_id, fetch_payloads=False)
            )
            if state is None or state.status != RunStatus.TERMINATED:
                raise RuntimeError(
                    "actual Workflow runtime did not retain terminated state"
                )
    finally:
        await client.aclose()
    snapshot = await accepted_wait_snapshot(run_id, require_live_mirror=False)
    return {
        "sha": sha,
        "runId": run_id,
        "runtimeTerminationVerified": True,
        "acceptedInputHash": snapshot,
    }


async def accepted_wait_snapshot(
    run_id: str, *, require_live_mirror: bool, require_no_mirror: bool = False
) -> str:
    """Read the actual service's scoped durable state, returning a digest rather than payloads.

    Before cancellation require a registered, unexpired exact-fixture mirror.
    After cancellation inspect only the immutable accepted snapshot. The controlled
    failure proof additionally requires mirror removal. These requirements are
    mutually exclusive. No database authority reaches the public Job.
    """
    from custos_callctx.execution import ExecutionIdentity
    from custos_spl.ids import WorkspaceId
    from custos_spl.interfaces.delivery import DeliveryStateProvider
    from custos_workflow.providers import MetadataStorePool, _resolve_metadata_store_dsn
    from custos_workflow.runs import RunInput
    from custos_workflow.steps.resume.durable import DurableResumeMirrorRepository
    from custos_workflow.steps.resume.handler import PENDING_TS_SUBSCRIPTION_ID

    if require_live_mirror and require_no_mirror:
        raise ValueError("wait cannot require both a live and removed mirror")
    dsn = _resolve_metadata_store_dsn(os.environ)
    if dsn is None:
        raise RuntimeError(
            "actual wait proof requires the configured durable metadata store"
        )
    pool = MetadataStorePool(dsn)
    identity = ExecutionIdentity(WORKSPACE, OPERATOR, "public-context-v1")
    try:
        async with asyncio.timeout(90):
            provider = await pool.open()
            if not isinstance(provider, DeliveryStateProvider):
                raise TypeError(
                    "actual wait proof requires the durable delivery capability"
                )
            saved = await provider.get_workflow_start(WorkspaceId(WORKSPACE), run_id)
            if saved is None:
                raise RuntimeError("accepted wait snapshot was not found")
            accepted = RunInput.from_dict(json.loads(saved.input_json))
            if (
                accepted.delivery_protocol != 3
                or accepted.workspace_id != WORKSPACE
                or accepted.workflow_version_id != f"{WORKSPACE}/{WAITING_WORKFLOW}@1"
                or accepted.execution_identity != identity
                or dict(accepted.inputs) != {}
            ):
                raise ValueError(
                    "accepted wait snapshot does not retain the public caller fixture"
                )
            if require_live_mirror:
                repository = DurableResumeMirrorRepository(provider, WORKSPACE)
                while True:
                    mirrors = await repository.list_open_for_step(
                        run_id, "await-cancellation"
                    )
                    if len(mirrors) > 1:
                        raise ValueError("actual wait has multiple mirror associations")
                    if (
                        mirrors
                        and mirrors[0].ts_subscription_id != PENDING_TS_SUBSCRIPTION_ID
                    ):
                        mirror = mirrors[0]
                        if (
                            mirror.event_key != "never-sent"
                            or mirror.execution_identity != identity
                            or mirror.expires_at is None
                            or (
                                mirror.expires_at - mirror.registered_at
                            ).total_seconds()
                            != 300
                            or mirror.expires_at <= await provider.delivery_now()
                        ):
                            raise ValueError(
                                "actual live mirror does not retain the bounded caller wait"
                            )
                        break
                    await asyncio.sleep(1)
            if require_no_mirror:
                repository = DurableResumeMirrorRepository(provider, WORKSPACE)
                if await repository.list_open_for_step(run_id, "await-cancellation"):
                    raise RuntimeError(
                        "controlled failure left its durable wait mirror"
                    )
            return hashlib.sha256(saved.input_json.encode()).hexdigest()
    finally:
        await pool.aclose()


async def runtime_waiting(run_id: str, sha: str) -> dict[str, Any]:
    """Require real registered no-event suspension and nonterminal runtime before cancellation."""
    from custos_workflow.runtime._common import GetRunStateRequest, RunStatus
    from custos_workflow.runtime.dapr import WorkflowClient

    snapshot = await accepted_wait_snapshot(run_id, require_live_mirror=True)
    client = WorkflowClient()
    try:
        async with asyncio.timeout(30):
            state = await client.get_workflow_state(
                GetRunStateRequest(instance_id=run_id, fetch_payloads=False)
            )
            if state is None or state.status != RunStatus.RUNNING:
                raise RuntimeError("registered wait is not live in the actual runtime")
    finally:
        await client.aclose()
    return {
        "sha": sha,
        "runId": run_id,
        "liveWaitVerified": True,
        "acceptedInputHash": snapshot,
    }


async def controlled_wait_failure(run_id: str, sha: str) -> dict[str, Any]:
    """Inject one invalid event through the real SDK; require failed output and native cleanup.

    The SDK completes an orchestrator that returns a failed step. Retain that
    distinction without promoting its COMPLETED status to domain success.
    Synthetic payload markers must never reach diagnostic evidence.
    """
    from custos_spl.interfaces.delivery import resume_channel
    from custos_workflow.runs.orchestrator import RunOutput
    from custos_workflow.runtime._common import (
        GetRunStateRequest,
        RaiseRunEventRequest,
        RunStatus,
    )
    from custos_workflow.runtime.dapr import WorkflowClient

    snapshot = await accepted_wait_snapshot(run_id, require_live_mirror=True)
    client = WorkflowClient()
    try:
        async with asyncio.timeout(60):
            await client.raise_workflow_event(
                RaiseRunEventRequest(
                    instance_id=run_id,
                    event_name=resume_channel(
                        WORKSPACE, run_id, "await-cancellation", "never-sent"
                    ),
                    data={
                        "deliveryProtocol": 1,
                        "payload": {
                            "credential": FAILURE_MARKERS[0],
                            "payload": FAILURE_MARKERS[1],
                        },
                    },
                )
            )
            while True:
                state = await client.get_workflow_state(
                    GetRunStateRequest(instance_id=run_id, fetch_payloads=True)
                )
                if state is not None and state.status == RunStatus.COMPLETED:
                    if state.serialized_output is None:
                        raise ValueError("controlled failure has no runtime outcome")
                    raw: object = json.loads(state.serialized_output)
                    if not isinstance(raw, dict):
                        raise TypeError("controlled failure outcome must be an object")
                    outcome = RunOutput.model_validate(raw)
                    if (
                        outcome.status != "failed"
                        or outcome.failed_step != "await-cancellation"
                        or outcome.failure_envelope is None
                        or outcome.failure_envelope.get("kind")
                        != "step.resume_delivery_failed"
                    ):
                        raise ValueError(
                            "controlled event did not fail its intended wait step"
                        )
                    break
                if state is not None and state.status in {
                    RunStatus.FAILED,
                    RunStatus.TERMINATED,
                }:
                    raise RuntimeError(
                        "controlled wait ended with an unrelated runtime failure"
                    )
                await asyncio.sleep(1)
    finally:
        await client.aclose()
    after = await accepted_wait_snapshot(
        run_id, require_live_mirror=False, require_no_mirror=True
    )
    if after != snapshot:
        raise ValueError("controlled failure changed the accepted wait snapshot")
    return {
        "sha": sha,
        "runId": run_id,
        "controlledWaitFailureVerified": True,
        "runtimeStatus": state.status.value,
        "stepOutcome": "failed",
        "failedStep": outcome.failed_step,
        "failureKind": outcome.failure_envelope["kind"],
        "mirrorCleanupVerified": True,
        "acceptedInputHash": after,
    }


def failure_locations(text: str) -> dict[str, Any]:
    """Keep bounded Python source locations only, never types inferred from arbitrary text."""
    frames = re.findall(
        r'File "[^"\n]*/([A-Za-z_][A-Za-z0-9_]*\.py)", line ([0-9]+), '
        r"in ([A-Za-z_<>][A-Za-z0-9_<>]*)",
        text.replace("\\n", "\n").replace('\\"', '"'),
    )
    return {
        "frames": [
            {"file": file, "line": int(line), "function": function}
            for file, line, function in frames[-30:]
        ],
    }


async def runtime_diagnostics(run_id: str, sha: str) -> dict[str, Any]:
    """Read payload-free failure state through the deployed SDK, retaining sanitized stack frames."""
    from dapr.ext.workflow import DaprWorkflowClient

    client = DaprWorkflowClient()
    try:
        async with asyncio.timeout(60):
            state = await asyncio.to_thread(
                client.get_workflow_state, run_id, fetch_payloads=False
            )
            if state is None:
                raise RuntimeError("diagnostic runtime instance was not found")
            details = state.failure_details
            error_type = None if details is None else details.error_type
            if error_type is not None and error_type not in DIAGNOSTIC_ERROR_TYPES:
                error_type = "unrecognized"
            return {
                "sha": sha,
                "runId": run_id,
                "diagnosticOnly": True,
                "runtimeStatus": state.runtime_status.name,
                "failureType": error_type,
                "historyCapability": (
                    "available-not-queried"
                    if callable(getattr(client, "get_workflow_history", None))
                    else "unsupported-client-api"
                ),
                "failureLocations": failure_locations(
                    "" if details is None else (details.stack_trace or "")
                ),
            }
    finally:
        await asyncio.to_thread(client.close)


def client_job(
    public_pod: dict[str, Any],
    image: str,
    *,
    run_id: str | None = None,
    controlled_failure: bool = False,
) -> dict[str, Any]:
    """Reuse public authority, with distinct Jobs and keys for ordinary and controlled proofs."""
    pod = copy.deepcopy(
        {
            "metadata": {"labels": public_pod["metadata"]["labels"]},
            "spec": public_pod["spec"],
        }
    )
    spec = pod["spec"]
    spec.pop("serviceAccountName", None)
    spec["automountServiceAccountToken"] = False
    spec["restartPolicy"] = "Never"
    spec["securityContext"] = {
        "runAsNonRoot": True,
        "runAsUser": 1000,
        "seccompProfile": {"type": "RuntimeDefault"},
    }
    spec["volumes"] = [{"name": "script", "configMap": {"name": RESOURCE}}]
    spec["containers"] = [
        {
            "name": "client",
            "image": image,
            "command": (
                [
                    "python",
                    "/smoke/public_workflow.py",
                    "failure-client" if controlled_failure else "client",
                ]
                if run_id is None
                else [
                    "python",
                    "/smoke/public_workflow.py",
                    "failure-readback" if controlled_failure else "readback",
                    "--run-id",
                    run_id,
                ]
            ),
            "env": [
                {"name": "GITHUB_SHA", "value": os.environ["GITHUB_SHA"]},
                {"name": "PYTHONDONTWRITEBYTECODE", "value": "1"},
                {
                    "name": "CUSTOS_TEST_TOKEN",
                    "valueFrom": {
                        "secretKeyRef": {"name": RESOURCE, "key": "token"},
                    },
                },
            ],
            "volumeMounts": [
                {"name": "script", "mountPath": "/smoke", "readOnly": True}
            ],
            "securityContext": {
                "allowPrivilegeEscalation": False,
                "readOnlyRootFilesystem": True,
                "capabilities": {"drop": ["ALL"]},
            },
            "resources": {
                "requests": {"cpu": "50m", "memory": "64Mi"},
                "limits": {"cpu": "500m", "memory": "256Mi"},
            },
        }
    ]
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": RESOURCE
            + ("-failure" if controlled_failure else "")
            + ("" if run_id is None else "-readback")
        },
        "spec": {"backoffLimit": 0, "activeDeadlineSeconds": 300, "template": pod},
    }


def read_report(logs: str, sha: str) -> dict[str, Any]:
    """Require one exact-tested-commit evidence record without promoting other log output."""
    reports = [
        json.loads(line.removeprefix(REPORT_PREFIX))
        for line in logs.splitlines()
        if line.startswith(REPORT_PREFIX)
    ]
    if len(reports) != 1 or not isinstance(reports[0], dict):
        raise ValueError("exactly one public Workflow evidence record is required")
    report: dict[str, Any] = reports[0]
    if report.get("sha") != sha:
        raise ValueError("public Workflow evidence is stale or incomplete")
    return report


def report_from_logs(logs: str, sha: str) -> dict[str, Any]:
    """Reject missing effects or a missing cancellable run even after the public Job succeeds."""
    report = read_report(logs, sha)
    if (
        report.get("sha") != sha
        or report.get("publicWorkflowCompleted") is not True
        or report.get("publicReadbackVerified") is not True
        or report.get("fullReceiverCertification") is not False
        or not isinstance(report.get("runId"), str)
        or not report["runId"]
        or not isinstance(report.get("cancellationRunId"), str)
        or re.fullmatch(r"[\w-]+", report["cancellationRunId"]) is None
    ):
        raise ValueError("public Workflow evidence is stale or incomplete")
    return report


def service_probe(service: str, phase: str, run_id: str, sha: str) -> dict[str, Any]:
    """Exec only into a ready actual service container at the tested image, never an impersonator."""
    import runtime as smoke

    namespace = os.environ["NS"]
    deployment = json.loads(
        smoke.command(
            [
                "kubectl",
                "-n",
                namespace,
                "get",
                "deployment/custos-" + service,
                "-o",
                "json",
            ]
        )
    )
    labels = deployment["spec"]["selector"]["matchLabels"]
    pods = json.loads(
        smoke.command(
            [
                "kubectl",
                "-n",
                namespace,
                "get",
                "pods",
                "-l",
                ",".join(f"{key}={value}" for key, value in sorted(labels.items())),
                "-o",
                "json",
            ]
        )
    )["items"]
    expected_image = os.environ["IMAGE_PREFIX"] + "/" + service + ":" + sha
    candidates = [
        pod
        for pod in pods
        if not pod["metadata"].get("deletionTimestamp")
        and any(
            container["name"] == service and container["image"] == expected_image
            for container in pod["spec"]["containers"]
        )
        and any(
            status["name"] == service and status.get("ready") is True
            for status in pod.get("status", {}).get("containerStatuses", [])
        )
    ]
    if not candidates:
        raise RuntimeError("no ready actual proof container at the tested image")
    pod = min(candidates, key=lambda row: row["metadata"]["name"])
    logs = smoke.command(
        [
            "kubectl",
            "-n",
            namespace,
            "exec",
            pod["metadata"]["name"],
            "-c",
            service,
            "--",
            "python",
            "-c",
            Path(__file__).read_text(),
            phase,
            "--run-id",
            run_id,
            "--sha",
            sha,
        ],
        timeout=150,
    )
    report = read_report(logs, sha)
    if report.get("runId") != run_id:
        raise ValueError("private proof returned a different run")
    return report


def configure_public_execution(config: dict[str, Any]) -> None:
    """Add the explicit caller pin without changing the background default or storing secrets."""
    runtime = config["global"]["receiverRuntime"]
    if runtime.get("executionBindingsBundle"):
        raise ValueError(
            "the disposable public fixture requires its owned inline overlay"
        )
    runtime["publicExecutionEnabled"] = True
    runtime["executionBindings"]["bindings"].append(
        {
            "workspaceId": WORKSPACE,
            "principalId": OPERATOR,
            "bindingId": "public-context-v1",
            "secretRef": EXECUTION_RESOURCE,
            "secretKey": "token",
        }
    )


def rendered(values: Path) -> list[dict[str, Any]]:
    """Render the owned smoke overlay for seed and client identities without applying it."""
    import runtime as smoke
    import yaml

    return [
        row
        for row in yaml.safe_load_all(
            smoke.command(
                [
                    "helm",
                    "template",
                    "custos",
                    os.environ["CHART"],
                    "-n",
                    os.environ["NS"],
                    "-f",
                    os.environ["VALUES"],
                    "-f",
                    str(values),
                ]
            )
        )
        if row
    ]


def wait_job(job: dict[str, Any]) -> None:
    """Require bounded success of one owned Job without printing credential-bearing manifests."""
    import runtime as smoke

    namespace = os.environ["NS"]
    name = "job/" + job["metadata"]["name"]
    smoke.command(["kubectl", "-n", namespace, "create", "-f", "-"], job)
    deadline = time.monotonic() + 330
    while time.monotonic() < deadline:
        status = json.loads(
            smoke.command(["kubectl", "-n", namespace, "get", name, "-o", "json"])
        ).get("status", {})
        if status.get("succeeded"):
            return
        if status.get("failed"):
            raise RuntimeError(
                "public Workflow proof Job failed; inspect owned Job diagnostics"
            )
        time.sleep(2)
    raise RuntimeError("public Workflow proof Job exceeded its deadline")


def prepare(values: Path) -> None:
    """Provision separate caller/execution credentials and seed state before service activation."""
    import runtime as smoke
    import yaml
    from custos_auth.tokens import mint_token

    namespace, sha = os.environ["NS"], os.environ["GITHUB_SHA"]
    config = yaml.safe_load(values.read_text())
    configure_public_execution(config)
    values.write_text(yaml.safe_dump(config))
    documents = rendered(values)
    token, _ = mint_token()
    execution_token, _ = mint_token()
    for kind, name, data_key, data in (
        ("Secret", RESOURCE, "stringData", {"token": token}),
        ("Secret", EXECUTION_RESOURCE, "stringData", {"token": execution_token}),
        (
            "ConfigMap",
            RESOURCE,
            "data",
            {"public_workflow.py": Path(__file__).read_text()},
        ),
    ):
        smoke.command(
            ["kubectl", "-n", namespace, "create", "-f", "-"],
            {
                "apiVersion": "v1",
                "kind": kind,
                "metadata": {"name": name},
                data_key: data,
            },
        )
    seed_job = smoke.seed_job(documents, sha)
    seed_job["metadata"]["name"] = RESOURCE + "-seed"
    pod = seed_job["spec"]["template"]["spec"]
    pod["serviceAccountName"] = "smoke-custos-bootstrap"
    pod["volumes"] = [{"name": "script", "configMap": {"name": RESOURCE}}]
    container = pod["containers"][0]
    container["command"] = ["python", "/smoke/public_workflow.py", "seed"]
    container["env"] = [
        row
        for row in container.get("env", [])
        if row["name"] != "SMOKE_EXECUTION_TOKEN"
    ] + [
        {
            "name": "CUSTOS_TEST_TOKEN",
            "valueFrom": {"secretKeyRef": {"name": RESOURCE, "key": "token"}},
        },
        {
            "name": "CUSTOS_TEST_EXECUTION_TOKEN",
            "valueFrom": {"secretKeyRef": {"name": EXECUTION_RESOURCE, "key": "token"}},
        },
    ]
    wait_job(seed_job)


def public_start_diagnostics() -> list[str]:
    """Collect only payload-free rejection locations, never arbitrary application log lines."""
    import runtime as smoke

    logs = smoke.command(
        [
            "kubectl",
            "-n",
            os.environ["NS"],
            "logs",
            "deployment/custos-workflow-service",
            "-c",
            "workflow-service",
            "--tail=100",
        ]
    )
    error_types = "|".join(re.escape(name) for name in sorted(DIAGNOSTIC_ERROR_TYPES))
    pattern = re.compile(
        r"\bPUBLIC_START_REJECTED run=[0-9a-f-]{36} "
        rf"cause=(?:{error_types}) origin=[A-Za-z_<>][A-Za-z0-9_<>]*:[0-9]+"
        r"(?: status=(?:[1-5][0-9]{2}|None))?",
    )
    return [match.group(0) for match in pattern.finditer(logs)]


def collect_failure_diagnostics(
    output: Path,
    sha: str,
    *,
    run_id: str | None = None,
    phase: str = "public-execution",
    job_name: str = RESOURCE,
) -> None:
    """Retain safe all-phase failure evidence, including known nonterminal candidates.

    Collection errors are explicit. The selected owned Job provides candidate
    correlation; candidate identity alone is not terminal or successful proof.
    """
    import runtime as smoke

    namespace = os.environ["NS"]
    report: dict[str, Any] = {
        "sha": sha,
        "diagnosticOnly": True,
        "lifecycleProofFailed": True,
        "failedPhase": phase,
        "publicStartRejections": [],
        "serviceLogs": {},
        "causalFailures": [],
        "terminalRuns": [],
        "candidateRuns": [],
        "publicRequests": [],
        "dependencyFailures": [],
        "collectionErrors": [],
    }
    candidates: dict[str, dict[str, Any]] = {}

    def retain_candidate(record: object, *, terminal: bool) -> None:
        """Validate exact-source candidate correlation without retaining response or error values."""
        if not isinstance(record, dict) or record.get("sha") != sha:
            raise ValueError("candidate diagnostic identity is invalid or stale")
        candidate = record.get("runId")
        if (
            not isinstance(candidate, str)
            or re.fullmatch(
                r"[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}",
                candidate,
            )
            is None
        ):
            raise ValueError("candidate diagnostic identity is invalid or stale")
        status = record.get("status") if terminal else None
        if terminal and status not in {"succeeded", "failed", "cancelled"}:
            raise ValueError("terminal diagnostic status is invalid")
        if candidates and candidate not in candidates:
            raise ValueError("only one cancellation candidate diagnostic is permitted")
        retained = candidates.setdefault(
            candidate, {"runId": candidate, "status": None}
        )
        if terminal:
            retained["status"] = status

    if run_id is not None:
        retain_candidate({"sha": sha, "runId": run_id}, terminal=False)
    try:
        report["publicStartRejections"] = public_start_diagnostics()
    except (RuntimeError, TypeError, ValueError, KeyError, OSError) as exc:
        report["collectionErrors"].append(
            {"source": "public-start", "errorType": type(exc).__name__}
        )
    try:
        logs = smoke.command(["kubectl", "-n", namespace, "logs", "job/" + job_name])
        requests: dict[str, dict[str, Any]] = {}
        for line in logs.splitlines():
            if line.startswith(REQUEST_PREFIX):
                record = json.loads(line.removeprefix(REQUEST_PREFIX))
                if (
                    not isinstance(record, dict)
                    or record.get("sha") != sha
                    or not isinstance(record.get("correlationId"), str)
                    or re.fullmatch(
                        r"[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}",
                        record["correlationId"],
                    )
                    is None
                    or record.get("method") not in {"POST", "GET"}
                    or (
                        record.get("httpStatus") is not None
                        and (
                            type(record["httpStatus"]) is not int
                            or not 100 <= record["httpStatus"] <= 599
                        )
                    )
                ):
                    raise ValueError(
                        "public request diagnostic identity or status is invalid"
                    )
                requests[record["correlationId"]] = {
                    key: record[key]
                    for key in ("correlationId", "method", "httpStatus")
                }
                if len(requests) > 30:
                    raise ValueError(
                        "public request diagnostic count exceeds its bound"
                    )
                report["publicRequests"] = list(requests.values())
            for prefix in (CANDIDATE_PREFIX, DIAGNOSTIC_PREFIX):
                if line.startswith(prefix):
                    retain_candidate(
                        json.loads(line.removeprefix(prefix)),
                        terminal=prefix == DIAGNOSTIC_PREFIX,
                    )
    except (RuntimeError, TypeError, ValueError, KeyError, OSError) as exc:
        report["collectionErrors"].append(
            {"source": "client-job", "errorType": type(exc).__name__}
        )
    for service, container, source in (
        ("api-gateway", "api-gateway", "api-gateway"),
        ("api-gateway", "daprd", "api-gateway-daprd"),
        ("auth-service", "auth-service", "auth-service"),
        ("auth-service", "daprd", "auth-service-daprd"),
        ("workflow-service", "workflow-service", "workflow-service"),
        ("workflow-service", "daprd", "daprd"),
    ):
        try:
            logs = smoke.command(
                [
                    "kubectl",
                    "-n",
                    namespace,
                    "logs",
                    "deployment/custos-" + service,
                    "-c",
                    container,
                    "--tail=300",
                ]
            )
            report["serviceLogs"][source] = failure_locations(logs)
            if container == "workflow-service":
                report["causalFailures"].extend(
                    resume_failure_records(logs, set(candidates))
                )
            if container in {"api-gateway", "workflow-service"}:
                report["dependencyFailures"].extend(
                    dependency_failure_records(
                        logs,
                        {row["correlationId"] for row in report["publicRequests"]},
                        source,
                    )
                )
        except (RuntimeError, TypeError, ValueError, KeyError, OSError) as exc:
            report["collectionErrors"].append(
                {"source": source, "errorType": type(exc).__name__}
            )
    for record in candidates.values():
        try:
            record["runtime"] = service_probe(
                "workflow-service", "diagnose", record["runId"], sha
            )
        except (RuntimeError, TypeError, ValueError, KeyError, OSError) as exc:
            report["collectionErrors"].append(
                {"source": "runtime-state", "errorType": type(exc).__name__}
            )
    report["candidateRuns"] = list(candidates.values())
    report["terminalRuns"] = [
        record for record in candidates.values() if record["status"] is not None
    ]
    output.with_name("public-workflow-diagnostics.json").write_text(
        json.dumps(report, indent=2)
    )
    print(json.dumps(report), flush=True)


def diagnostic_source_frames(value: object) -> list[dict[str, Any]]:
    """Validate at most five structured source locations, discarding all extra frame fields."""
    if not isinstance(value, list) or len(value) > 5:
        raise ValueError("diagnostic source frames exceed their bound")
    frames = []
    for frame in value:
        if (
            not isinstance(frame, dict)
            or not isinstance(frame.get("file"), str)
            or re.fullmatch(r"[A-Za-z0-9_-]{1,100}\.py", frame["file"]) is None
            or not isinstance(frame.get("function"), str)
            or re.fullmatch(r"[A-Za-z0-9_<>-]{1,100}", frame["function"]) is None
            or type(frame.get("line")) is not int
            or frame["line"] < 1
        ):
            raise ValueError("diagnostic source frame is invalid")
        frames.append({key: frame[key] for key in ("file", "function", "line")})
    return frames


def dependency_failure_records(
    logs: str, correlations: set[str], source: str
) -> list[dict[str, Any]]:
    """Keep only matched request-boundary records with fixed taxonomy and bounded source frames."""
    prefix = (
        "GATEWAY_DEPENDENCY_FAILURE "
        if source == "api-gateway"
        else "WORKFLOW_PUBLIC_START_FAILURE "
    )
    records = []
    for line in logs.splitlines():
        if prefix not in line:
            continue
        row, _ = json.JSONDecoder().raw_decode(line.split(prefix, 1)[1])
        if not isinstance(row, dict):
            raise TypeError("dependency diagnostic must be an object")
        if row.get("correlationId") not in correlations:
            continue
        if (
            row.get("phase")
            not in {
                "auth-client",
                "auth-verify",
                "callctx-sign",
                "request-key",
                "dapr-route",
                "activation",
                "lookup",
                "identity",
                "retention",
                "recovery",
                "validate",
                "durable-start",
            }
            or row.get("kind")
            not in {
                None,
                "transport",
                "status",
                "decode",
                "unavailable",
                "unconfigured",
            }
            or row.get("errorType") not in DIAGNOSTIC_ERROR_TYPES | {None}
            or (
                row.get("runId") is not None
                and (
                    not isinstance(row["runId"], str)
                    or re.fullmatch(
                        r"[0-9a-f]{8}-[0-9a-f]{4}-[45][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}",
                        row["runId"],
                    )
                    is None
                )
            )
            or (
                row.get("httpStatus") is not None
                and (
                    type(row["httpStatus"]) is not int
                    or not 100 <= row["httpStatus"] <= 599
                )
            )
            or row.get("grpcStatus")
            not in {
                None,
                "OK",
                "CANCELLED",
                "UNKNOWN",
                "INVALID_ARGUMENT",
                "DEADLINE_EXCEEDED",
                "NOT_FOUND",
                "ALREADY_EXISTS",
                "PERMISSION_DENIED",
                "RESOURCE_EXHAUSTED",
                "FAILED_PRECONDITION",
                "ABORTED",
                "OUT_OF_RANGE",
                "UNIMPLEMENTED",
                "INTERNAL",
                "UNAVAILABLE",
                "DATA_LOSS",
                "UNAUTHENTICATED",
            }
        ):
            raise ValueError("dependency diagnostic has invalid taxonomy or status")
        safe_frames = diagnostic_source_frames(row.get("frames", []))
        records.append(
            {
                "source": source,
                "attemptedRunId": row.get("runId")
                if source == "workflow-service"
                else None,
                **{
                    key: row.get(key)
                    for key in (
                        "correlationId",
                        "phase",
                        "kind",
                        "errorType",
                        "httpStatus",
                        "grpcStatus",
                    )
                },
                "frames": safe_frames,
            }
        )
        if len(records) > 30:
            raise ValueError("dependency diagnostic count exceeds its bound")
    return records


def resume_failure_records(logs: str, candidates: set[str]) -> list[dict[str, Any]]:
    """Extract only candidate-correlated, bounded source failures using the closed diagnostic schema."""
    records: list[dict[str, Any]] = []
    prefix = "WORKFLOW_RESUME_FAILURE "
    for line in logs.splitlines():
        if prefix not in line:
            continue
        record, _ = json.JSONDecoder().raw_decode(line.split(prefix, 1)[1])
        if not isinstance(record, dict):
            raise TypeError("resume failure diagnostic must be an object")
        if record.get("runId") not in candidates:
            continue
        if (
            record.get("operation")
            not in {"persist", "register", "ack", "wait", "cancel", "delete", "unknown"}
            or record.get("phase")
            not in {"activity", "retry", "driver", "unhandled", "generator"}
            or not isinstance(record.get("stepId"), str)
            or re.fullmatch(r"[A-Za-z0-9_-]{1,100}", record["stepId"]) is None
            or record.get("errorType") not in DIAGNOSTIC_ERROR_TYPES
            or (
                record.get("attempt") is not None
                and (
                    type(record["attempt"]) is not int
                    or not 1 <= record["attempt"] <= 10000
                )
            )
            or record.get("kind")
            not in {
                None,
                "workflow.client.transport",
                "workflow.client.status",
                "workflow.client.decode",
                "workflow.client.cancelled",
            }
            or (
                record.get("taskId") is not None
                and (
                    type(record["taskId"]) is not int
                    or not 0 <= record["taskId"] <= 100000
                )
            )
            or (
                record.get("httpStatus") is not None
                and (
                    type(record["httpStatus"]) is not int
                    or not 100 <= record["httpStatus"] <= 599
                )
            )
            or record.get("grpcStatus")
            not in {
                None,
                "OK",
                "CANCELLED",
                "UNKNOWN",
                "INVALID_ARGUMENT",
                "DEADLINE_EXCEEDED",
                "NOT_FOUND",
                "ALREADY_EXISTS",
                "PERMISSION_DENIED",
                "RESOURCE_EXHAUSTED",
                "FAILED_PRECONDITION",
                "ABORTED",
                "OUT_OF_RANGE",
                "UNIMPLEMENTED",
                "INTERNAL",
                "UNAVAILABLE",
                "DATA_LOSS",
                "UNAUTHENTICATED",
            }
        ):
            raise ValueError("resume failure diagnostic has invalid fields")
        frames = diagnostic_source_frames(record.get("frames"))
        records.append(
            {
                key: record.get(key)
                for key in (
                    "runId",
                    "stepId",
                    "operation",
                    "phase",
                    "attempt",
                    "taskId",
                    "errorType",
                    "kind",
                    "httpStatus",
                    "grpcStatus",
                )
            }
            | {"frames": frames}
        )
        if len(records) > 100:
            raise ValueError("too many resume failure diagnostics")
    return records


def verify_controlled_failure_diagnostics(
    report: dict[str, Any], run_id: str, sha: str
) -> None:
    """Reject absent, uncorrelated, incomplete or marker-bearing controlled failure evidence."""
    if (
        report.get("sha") != sha
        or report.get("diagnosticOnly") is not True
        or report.get("lifecycleProofFailed") is not True
        or report.get("failedPhase") != "controlled-wait-failure"
        or report.get("collectionErrors") != []
    ):
        raise ValueError("controlled failure diagnostics are incomplete or stale")
    records = report.get("causalFailures")
    if not isinstance(records, list) or not any(
        isinstance(record, dict)
        and record.get("runId") == run_id
        and record.get("stepId") == "await-cancellation"
        and record.get("operation") == "wait"
        and record.get("phase") == "driver"
        and record.get("errorType") == "ValueError"
        and diagnostic_source_frames(record.get("frames"))
        for record in records
    ):
        raise ValueError("controlled wait has no correlated causal source evidence")
    if any(marker in json.dumps(report) for marker in FAILURE_MARKERS):
        raise ValueError(
            "controlled failure diagnostics retained synthetic private data"
        )


def run(output: Path, values: Path, *, controlled_failure: bool = False) -> None:
    """Require ordinary effects or explicit controlled-failure evidence; preserve proof errors."""
    import runtime as smoke

    namespace, sha = os.environ["NS"], os.environ["GITHUB_SHA"]
    run_id = None
    phase = "public-execution"
    job_name = RESOURCE + ("-failure" if controlled_failure else "")
    try:
        documents = rendered(values)
        public = next(
            (
                row
                for row in documents
                if row["kind"] == "Pod"
                and row["metadata"]["name"] == "custos-smoke-test"
            ),
            None,
        )
        if public is None:
            raise RuntimeError("rendered chart has no public smoke identity")
        client = client_job(
            public,
            os.environ["IMAGE_PREFIX"] + "/api-gateway:" + sha,
            controlled_failure=controlled_failure,
        )
        wait_job(client)
        logs = smoke.command(["kubectl", "-n", namespace, "logs", "job/" + job_name])
        report = report_from_logs(logs, sha)
        run_id = report["cancellationRunId"]
        phase = "runtime-wait"
        waiting = service_probe("workflow-service", "runtime-wait", run_id, sha)
        snapshot = waiting.get("acceptedInputHash")
        if (
            waiting.get("liveWaitVerified") is not True
            or not isinstance(snapshot, str)
            or re.fullmatch(r"[a-f0-9]{64}", snapshot) is None
        ):
            raise ValueError("actual proof did not confirm the live accepted wait")
        report["liveWaitVerified"] = True
        if controlled_failure:
            phase = "controlled-wait-failure"
            proof = service_probe("workflow-service", "fail-wait", run_id, sha)
            if (
                proof.get("controlledWaitFailureVerified") is not True
                or proof.get("mirrorCleanupVerified") is not True
                or proof.get("acceptedInputHash") != snapshot
            ):
                raise ValueError(
                    "controlled wait failure did not retain its required effects"
                )
            collect_failure_diagnostics(
                output, sha, run_id=run_id, phase=phase, job_name=job_name
            )
            diagnostics = json.loads(
                output.with_name("public-workflow-diagnostics.json").read_text()
            )
            verify_controlled_failure_diagnostics(diagnostics, run_id, sha)
            phase = "public-failure-readback"
            readback = client_job(
                public,
                os.environ["IMAGE_PREFIX"] + "/api-gateway:" + sha,
                run_id=run_id,
                controlled_failure=True,
            )
            wait_job(readback)
            observed = read_report(
                smoke.command(
                    [
                        "kubectl",
                        "-n",
                        namespace,
                        "logs",
                        "job/" + job_name + "-readback",
                    ]
                ),
                sha,
            )
            if (
                observed.get("runId") != run_id
                or observed.get("publicFailureReadbackObserved") is not True
                or observed.get("publicStatus") not in {"succeeded", "failed"}
            ):
                raise ValueError(
                    "controlled failure has no exact public terminal readback"
                )
            evidence = {
                key: proof[key]
                for key in (
                    "sha",
                    "runId",
                    "runtimeStatus",
                    "stepOutcome",
                    "failedStep",
                    "failureKind",
                    "mirrorCleanupVerified",
                    "acceptedInputHash",
                    "controlledWaitFailureVerified",
                )
            }
            evidence.update(
                {
                    "controlledFailureVerification": True,
                    "acceptedInputUnchanged": True,
                    "causalDiagnosticsVerified": True,
                    "privacyMarkersExcluded": True,
                    "publicStatus": observed["publicStatus"],
                    "pureWorkflowRunId": report["runId"],
                    "pureWorkflowCompleted": report["publicWorkflowCompleted"],
                    "original503CauseEstablished": False,
                    "fullReceiverCertification": False,
                }
            )
            serialized = json.dumps(evidence, indent=2)
            if any(marker in serialized for marker in FAILURE_MARKERS):
                raise ValueError(
                    "controlled verification retained synthetic private data"
                )
            output.write_text(serialized)
            print(
                "Controlled failed-step diagnostics verified; original 503 cause remains unknown."
            )
            return
        for service, probe_phase, field in (
            ("trigger-service", "cancel", "privateCancellationVerified"),
            ("workflow-service", "runtime-state", "runtimeTerminationVerified"),
        ):
            phase = (
                "private-cancellation" if probe_phase == "cancel" else "runtime-state"
            )
            proof = service_probe(service, probe_phase, run_id, sha)
            if proof.get(field) is not True:
                raise ValueError("private proof did not confirm its required effect")
            if (
                probe_phase == "runtime-state"
                and proof.get("acceptedInputHash") != snapshot
            ):
                raise ValueError(
                    "private cancellation changed the accepted wait snapshot"
                )
            report[field] = True
        report["acceptedInputUnchanged"] = True
        report["acceptedInputHash"] = snapshot
        phase = "public-readback"
        readback = client_job(
            public, os.environ["IMAGE_PREFIX"] + "/api-gateway:" + sha, run_id=run_id
        )
        wait_job(readback)
        proof = read_report(
            smoke.command(
                ["kubectl", "-n", namespace, "logs", "job/" + RESOURCE + "-readback"]
            ),
            sha,
        )
        if (
            proof.get("runId") != run_id
            or proof.get("publicCancellationReadback") is not True
        ):
            raise ValueError("public proof did not confirm terminal cancellation")
        report["publicCancellationReadback"] = True
        output.write_text(json.dumps(report, indent=2))
    except (RuntimeError, TypeError, ValueError, KeyError, OSError):
        try:
            collect_failure_diagnostics(
                output,
                sha,
                run_id=run_id,
                phase=phase,
                job_name=job_name,
            )
        except (RuntimeError, TypeError, ValueError, KeyError, OSError) as exc:
            print(
                json.dumps(
                    {
                        "sha": sha,
                        "lifecycleProofFailed": True,
                        "collectionErrors": [
                            {"source": "collector", "errorType": type(exc).__name__}
                        ],
                    }
                ),
                flush=True,
            )
        raise
    print(
        "Public completion/readback and actual private cancellation verified; receiver certification remains separate."
    )


def main() -> None:
    """Select runner orchestration or the separate seed/public-client container program."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "phase",
        choices=(
            "seed",
            "client",
            "failure-client",
            "prepare",
            "run",
            "cancel",
            "runtime-state",
            "runtime-wait",
            "diagnose",
            "readback",
            "failure-readback",
            "fail-wait",
        ),
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--values", type=Path)
    parser.add_argument("--run-id")
    parser.add_argument("--sha")
    parser.add_argument("--controlled-failure", action="store_true")
    args = parser.parse_args()
    if args.controlled_failure and args.phase != "run":
        parser.error("--controlled-failure is only valid for run")
    if args.phase == "seed":
        asyncio.run(seed())
    elif args.phase in {"client", "failure-client"}:
        print(
            REPORT_PREFIX
            + json.dumps(exercise(controlled_failure=args.phase == "failure-client")),
            flush=True,
        )
    elif args.phase in {
        "cancel",
        "runtime-state",
        "runtime-wait",
        "diagnose",
        "readback",
        "failure-readback",
        "fail-wait",
    }:
        if args.run_id is None or re.fullmatch(r"[\w-]+", args.run_id) is None:
            parser.error("--run-id must be an opaque root run identifier")
        if args.phase in {"readback", "failure-readback"}:
            report = (
                failure_readback(args.run_id)
                if args.phase == "failure-readback"
                else cancellation_readback(args.run_id)
            )
        else:
            if args.sha is None or re.fullmatch(r"[a-f0-9]{40}", args.sha) is None:
                parser.error("--sha must be the exact tested commit")
            if args.phase == "cancel":
                report = asyncio.run(private_cancellation(args.run_id, args.sha))
            elif args.phase == "diagnose":
                report = asyncio.run(runtime_diagnostics(args.run_id, args.sha))
            elif args.phase == "runtime-wait":
                report = asyncio.run(runtime_waiting(args.run_id, args.sha))
            elif args.phase == "fail-wait":
                report = asyncio.run(controlled_wait_failure(args.run_id, args.sha))
            else:
                report = asyncio.run(runtime_termination(args.run_id, args.sha))
        print(REPORT_PREFIX + json.dumps(report), flush=True)
    elif args.phase == "prepare":
        if args.values is None:
            parser.error("--values is required for prepare")
        prepare(args.values)
    elif args.output is None or args.values is None:
        parser.error("--output and --values are required for run")
    else:
        run(args.output, args.values, controlled_failure=args.controlled_failure)


if __name__ == "__main__":
    main()
