"""Prove a public signed-context Workflow lifecycle using actual deployed services.

Only the separate seed Job receives database access. The client inherits the
chart's public Helm-test network identity and receives a workspace operator
credential, never database, Kubernetes or mesh credentials. Synthetic definition
data does not replace Gateway/Auth/Dapr/Workflow transport or execution. A
separate runner probe uses the actual Trigger container/sidecar for signed
cancellation and the Workflow container's production client to confirm runtime
termination; neither private authority is given to the public client.
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import json
import os
import re
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any
from uuid import UUID

WORKSPACE = "workspace-default"
WORKFLOW = "public-context-proof"
WAITING_WORKFLOW = "private-cancellation-proof"
OPERATOR = "public-context-operator"
RESOURCE = "smoke-public-workflow"
EXECUTION_RESOURCE = RESOURCE + "-execution"
REPORT_PREFIX = "PUBLIC_WORKFLOW_EVIDENCE "


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
    """Use production PostgreSQL adapters for a definition and expiring workspace-only grant."""
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
        definitions = PgDefinitionAdapter(lazy=pool)
        for name, document in (
            (WORKFLOW, workflow_document()),
            (WAITING_WORKFLOW, waiting_workflow_document()),
        ):
            await definitions.put_workflow_version(WORKSPACE, name, "1", document)
    finally:
        await pool.aclose()
    print("Synthetic definition and workspace-only public operator seeded.", flush=True)


def request(
    method: str, path: str, body: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Require the public response contract without printing credentials or response payloads."""
    headers = {
        "authorization": "Bearer " + os.environ["CUSTOS_TEST_TOKEN"],
        "accept": "application/json",
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


def exercise() -> dict[str, Any]:
    """Require actual runtime completion and public list/get readback, not just acceptance."""
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
        "cancellationRunId": start_waiting_run(),
        "fullReceiverCertification": False,
    }


def start_waiting_run() -> str:
    """Require a running, cancellable real Workflow through the public-only client."""
    prefix = f"/v1/workspaces/{WORKSPACE}/runs"
    started = request(
        "POST",
        prefix,
        {
            "workflowVersionId": f"{WORKSPACE}/{WAITING_WORKFLOW}@1",
            "idempotencyKey": WAITING_WORKFLOW + "-" + os.environ["GITHUB_SHA"],
        },
    )
    run_id = started.get("runId")
    if not isinstance(run_id, str) or UUID(run_id).version != 5:
        raise ValueError("cancellation start did not return an opaque run identifier")
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        observed = request("GET", prefix + "/" + run_id)
        if observed.get("status") == "running":
            return run_id
        if observed.get("status") in {"succeeded", "failed", "cancelled"}:
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
    """Read the actual Workflow sidecar through its production client, independently of metadata."""
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
    return {"sha": sha, "runId": run_id, "runtimeTerminationVerified": True}


def client_job(
    public_pod: dict[str, Any], image: str, *, run_id: str | None = None
) -> dict[str, Any]:
    """Reuse only the chart's public identity for initial execution or final cancellation readback."""
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
                ["python", "/smoke/public_workflow.py", "client"]
                if run_id is None
                else [
                    "python",
                    "/smoke/public_workflow.py",
                    "readback",
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
        "metadata": {"name": RESOURCE if run_id is None else RESOURCE + "-readback"},
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
    return re.findall(
        r"\bPUBLIC_START_REJECTED run=[0-9a-f-]{36} "
        r"cause=[A-Za-z_][A-Za-z0-9_]* origin=[A-Za-z_<>][A-Za-z0-9_<>]*:[0-9]+",
        logs,
    )


def run(output: Path, values: Path) -> None:
    """Require separate public completion, actual private termination and final public readback."""
    import runtime as smoke

    namespace, sha = os.environ["NS"], os.environ["GITHUB_SHA"]
    documents = rendered(values)
    public = next(
        row
        for row in documents
        if row["kind"] == "Pod" and row["metadata"]["name"] == "custos-smoke-test"
    )
    client = client_job(public, os.environ["IMAGE_PREFIX"] + "/api-gateway:" + sha)
    try:
        wait_job(client)
    except RuntimeError:
        print(json.dumps({"publicStartRejections": public_start_diagnostics()}))
        raise
    logs = smoke.command(["kubectl", "-n", namespace, "logs", "job/" + RESOURCE])
    report = report_from_logs(logs, sha)
    run_id = report["cancellationRunId"]
    for service, phase, field in (
        ("trigger-service", "cancel", "privateCancellationVerified"),
        ("workflow-service", "runtime-state", "runtimeTerminationVerified"),
    ):
        proof = service_probe(service, phase, run_id, sha)
        if proof.get(field) is not True:
            raise ValueError("private proof did not confirm its required effect")
        report[field] = True
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
            "prepare",
            "run",
            "cancel",
            "runtime-state",
            "readback",
        ),
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--values", type=Path)
    parser.add_argument("--run-id")
    parser.add_argument("--sha")
    args = parser.parse_args()
    if args.phase == "seed":
        asyncio.run(seed())
    elif args.phase == "client":
        print(REPORT_PREFIX + json.dumps(exercise()), flush=True)
    elif args.phase in {"cancel", "runtime-state", "readback"}:
        if args.run_id is None or re.fullmatch(r"[\w-]+", args.run_id) is None:
            parser.error("--run-id must be an opaque root run identifier")
        if args.phase == "readback":
            report = cancellation_readback(args.run_id)
        else:
            if args.sha is None or re.fullmatch(r"[a-f0-9]{40}", args.sha) is None:
                parser.error("--sha must be the exact tested commit")
            report = asyncio.run(
                private_cancellation(args.run_id, args.sha)
                if args.phase == "cancel"
                else runtime_termination(args.run_id, args.sha)
            )
        print(REPORT_PREFIX + json.dumps(report), flush=True)
    elif args.phase == "prepare":
        if args.values is None:
            parser.error("--values is required for prepare")
        prepare(args.values)
    elif args.output is None or args.values is None:
        parser.error("--output and --values are required for run")
    else:
        run(args.output, args.values)


if __name__ == "__main__":
    main()
