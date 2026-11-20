"""Prove a public signed-context Workflow lifecycle using actual deployed services.

Only the separate seed Job receives database access. The client inherits the
chart's public Helm-test network identity and receives a workspace operator
credential, never database, Kubernetes or mesh credentials. Synthetic definition
data does not replace Gateway/Auth/Dapr/Workflow transport or execution.
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

WORKSPACE = "workspace-default"
WORKFLOW = "public-context-proof"
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
        await PgDefinitionAdapter(lazy=pool).put_workflow_version(
            WORKSPACE, WORKFLOW, "1", workflow_document()
        )
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
    if not isinstance(run_id, str) or not re.fullmatch(r"[\w-]+", run_id):
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
        "fullReceiverCertification": False,
    }


def client_job(public_pod: dict[str, Any], image: str) -> dict[str, Any]:
    """Reuse the chart's public-only test identity without its container or ambient authority."""
    pod = copy.deepcopy(public_pod)
    pod["metadata"] = {"labels": pod["metadata"]["labels"]}
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
            "command": ["python", "/smoke/public_workflow.py", "client"],
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
        "metadata": {"name": RESOURCE},
        "spec": {"backoffLimit": 0, "activeDeadlineSeconds": 300, "template": pod},
    }


def report_from_logs(logs: str, sha: str) -> dict[str, Any]:
    """Reject missing, duplicate, stale or incomplete evidence even after Job success."""
    reports = [
        json.loads(line.removeprefix(REPORT_PREFIX))
        for line in logs.splitlines()
        if line.startswith(REPORT_PREFIX)
    ]
    if len(reports) != 1 or not isinstance(reports[0], dict):
        raise ValueError("exactly one public Workflow evidence record is required")
    report: dict[str, Any] = reports[0]
    if (
        report.get("sha") != sha
        or report.get("publicWorkflowCompleted") is not True
        or report.get("publicReadbackVerified") is not True
        or report.get("fullReceiverCertification") is not False
        or not isinstance(report.get("runId"), str)
        or not report["runId"]
    ):
        raise ValueError("public Workflow evidence is stale or incomplete")
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


def run(output: Path, values: Path) -> None:
    """Require completion/readback from the public-only client after services are installed."""
    import runtime as smoke

    namespace, sha = os.environ["NS"], os.environ["GITHUB_SHA"]
    documents = rendered(values)
    public = next(
        row
        for row in documents
        if row["kind"] == "Pod" and row["metadata"]["name"] == "custos-smoke-test"
    )
    client = client_job(public, os.environ["IMAGE_PREFIX"] + "/api-gateway:" + sha)
    wait_job(client)
    logs = smoke.command(["kubectl", "-n", namespace, "logs", "job/" + RESOURCE])
    output.write_text(json.dumps(report_from_logs(logs, sha), indent=2))
    print(
        "Real public Workflow start/completion/readback verified; receiver certification remains separate."
    )


def main() -> None:
    """Select runner orchestration or the separate seed/public-client container program."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("seed", "client", "prepare", "run"))
    parser.add_argument("--output", type=Path)
    parser.add_argument("--values", type=Path)
    args = parser.parse_args()
    if args.phase == "seed":
        asyncio.run(seed())
    elif args.phase == "client":
        print(REPORT_PREFIX + json.dumps(exercise()), flush=True)
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
