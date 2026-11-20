"""Exercise real receiver effects through the disposable deployment's public Gateway.

The seed job uses production providers, never test SQL or application fakes.
The client has only a workspace operator credential and Gateway network access.
Its report distinguishes executed scenarios from the full, still-required
receiver certification matrix. No credentials or response bodies are printed.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

WORKSPACE = "workspace-default"
OPERATOR = "receiver-certification-operator"
OPERATOR_SECRET = "receiver-certification-operator"
WORKFLOW = "receiver-certification"
WAIT_WORKFLOW = "receiver-certification-wait"
REPORT_PREFIX = "RECEIVER_EVIDENCE "


class CertificationFailure(RuntimeError):
    """Carry only a fixed scenario label and HTTP status, never a response or credential."""


def workflow_document(*, waiting: bool = False) -> dict[str, Any]:
    """Build a pure workflow, optionally parked on a real source-associated durable wait."""
    steps: list[dict[str, Any]] = [{"id": "effect", "let": {"observed": "${{ true }}"}}]
    if waiting:
        steps.insert(
            0,
            {
                "id": "await-source",
                "waitFor": {
                    "eventKey": "${{ 'registry.push' }}",
                    "selector": "${{ \"event.data.route == 'match'\" }}",
                    "ttl": "PT1H",
                    "source": {
                        "connectorInstanceId": "receiver-fixture",
                        "deliveryModes": ["push"],
                    },
                },
            },
        )
        steps[1]["needs"] = ["await-source"]
    return {
        "apiVersion": "custos.dev/v1",
        "kind": "Workflow",
        "metadata": {
            "name": WAIT_WORKFLOW if waiting else WORKFLOW,
            "workspace": WORKSPACE,
        },
        "spec": {"steps": steps},
    }


async def seed() -> None:
    """Seed immutable synthetic definitions and an expiring workspace-only operator grant."""
    from datetime import UTC, datetime, timedelta

    from custos_auth.roles import ROLE_WORKSPACE_OPERATOR
    from custos_auth.tokens import hash_token
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

    dsn = resolve_dsn(os.environ)
    if not dsn:
        raise CertificationFailure("seed: database reference unavailable")
    pool = LazyPool(dsn, init=register_jsonb_codec)
    auth, definitions = PgAuthAdapter(lazy=pool), PgDefinitionAdapter(lazy=pool)
    now = datetime.now(UTC)
    try:
        await auth.put_principal(
            ServiceAccount(
                kind="serviceAccount",
                principal_id=PrincipalId(OPERATOR),
                workspace_id=WorkspaceId(WORKSPACE),
                display_name="Disposable receiver certification operator",
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
        await auth.put_service_token(
            ServiceToken(
                token_id=ServiceTokenId(OPERATOR),
                service_account_id=PrincipalId(OPERATOR),
                hash=hash_token(os.environ["CUSTOS_TEST_TOKEN"]),
                issued_at=now,
                expires_at=now + timedelta(hours=2),
                revoked_at=None,
                revoked_by=None,
                revoked_reason=None,
            )
        )
        for waiting in (False, True):
            name = WAIT_WORKFLOW if waiting else WORKFLOW
            await definitions.put_workflow_version(
                WORKSPACE, name, "1", workflow_document(waiting=waiting)
            )
    finally:
        await pool.aclose()
    print("Synthetic definitions and workspace-only operator grant seeded.", flush=True)


class Gateway:
    """Exercise deployed public contracts without database access or fabricated call contexts."""

    def __init__(self) -> None:
        """Read only explicitly projected test inputs; never acquire platform-wide authority."""
        self.base = os.environ["CUSTOS_GATEWAY_URL"].rstrip("/")
        if self.base != "http://custos-api-gateway:8080":
            raise CertificationFailure(
                "client must target the disposable public Gateway"
            )
        self.token = os.environ["CUSTOS_TEST_TOKEN"]
        self.sequence = 0

    def request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        *,
        expected: tuple[int, ...] = (200,),
        authenticated: bool = True,
    ) -> dict[str, Any]:
        """Require the exact expected status and JSON shape without publishing response content."""
        self.sequence += 1
        headers = {"accept": "application/json"}
        if authenticated:
            headers["authorization"] = "Bearer " + self.token
        data = None
        if body is not None:
            data = json.dumps(body).encode()
            headers["content-type"] = "application/json"
        if method not in ("GET", "HEAD"):
            headers["idempotency-key"] = f"receiver-certification-{self.sequence}"
        request = urllib.request.Request(
            self.base + path, data=data, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(request, timeout=90) as response:
                status, raw = response.status, response.read()
        except urllib.error.HTTPError as error:
            status, raw = error.code, error.read()
        if status not in expected:
            raise CertificationFailure(f"{method} {path}: HTTP {status}")
        if not raw:
            return {}
        decoded: object = json.loads(raw)
        if not isinstance(decoded, dict):
            raise CertificationFailure(f"{method} {path}: invalid response shape")
        return decoded

    def runs(self) -> list[dict[str, Any]]:
        """Read the real public run list; never infer consumer effects from producer claims."""
        result = self.request("GET", f"/v1/workspaces/{WORKSPACE}/runs")
        items = result.get("items")
        if not isinstance(items, list) or not all(
            isinstance(row, dict) for row in items
        ):
            raise CertificationFailure("runs: invalid list response shape")
        return items

    def wait_terminal(self, run_id: str, *, timeout: float = 180) -> None:
        """Require a successful real workflow; failures and expiry are not skipped scenarios."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            row = self.request("GET", f"/v1/workspaces/{WORKSPACE}/runs/{run_id}")
            state = row.get("status")
            if state == "succeeded":
                return
            if state in ("failed", "cancelled"):
                raise CertificationFailure(f"workflow: terminal {state}")
            time.sleep(1)
        raise CertificationFailure("workflow: completion deadline exceeded")

    def subscription(self, source: str) -> str:
        """Configure a real receiver while preserving normal Gateway authorization and validation."""
        body: dict[str, Any] = {
            "sourceType": source,
            "workflowId": f"{WORKSPACE}/{WORKFLOW}",
            "targetWorkflowVersionId": f"{WORKSPACE}/{WORKFLOW}@1",
        }
        if source == "scheduled":
            body["sourceConfig"] = {"intervalSeconds": 10}
        result = self.request(
            "POST", f"/v1/workspaces/{WORKSPACE}/triggers", body, expected=(201,)
        )
        identifier = result.get("subscriptionId")
        if not isinstance(identifier, str) or not identifier:
            raise CertificationFailure("subscription: missing identifier")
        return identifier


def exercise() -> None:
    """Prove manual regression, tenant denial and scheduled start effects in the real deployment."""
    gateway = Gateway()
    started = time.monotonic()
    completed: list[str] = []
    gateway.request(
        "GET",
        f"/v1/workspaces/{WORKSPACE}/runs",
        expected=(401,),
        authenticated=False,
    )
    gateway.request("GET", "/v1/workspaces/receiver-other-tenant/runs", expected=(403,))
    completed.append("public-gateway-authentication-and-workspace-denial")
    manual = gateway.subscription("manual")
    fired = gateway.request(
        "POST",
        f"/v1/workspaces/{WORKSPACE}/triggers/{manual}:fire",
        {},
    )
    run_id = fired.get("runId")
    if not isinstance(run_id, str) or not run_id:
        raise CertificationFailure("manual: missing run identifier")
    gateway.wait_terminal(run_id)
    completed.append("manual-trigger-real-workflow-completion")
    before = {row["runId"] for row in gateway.runs()}
    scheduled = gateway.subscription("scheduled")
    try:
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            new = [row for row in gateway.runs() if row["runId"] not in before]
            if new:
                for row in new:
                    gateway.wait_terminal(row["runId"])
                completed.append("scheduled-receiver-real-workflow-completion")
                break
            time.sleep(1)
        else:
            raise CertificationFailure("scheduled: no workflow effect before deadline")
    finally:
        gateway.request(
            "PATCH",
            f"/v1/workspaces/{WORKSPACE}/triggers/{scheduled}",
            {"state": "paused"},
        )
    print(
        REPORT_PREFIX
        + json.dumps(
            {
                "sha": os.environ["GITHUB_SHA"],
                "completedScenarios": completed,
                "elapsedSeconds": round(time.monotonic() - started, 3),
                "fullReceiverCertification": False,
            }
        ),
        flush=True,
    )


def prepare() -> None:
    """Create test-only Secret/script and seed through the actual bootstrap image before admission."""
    import runtime as smoke
    import yaml
    from custos_auth.tokens import mint_token

    namespace, tag = os.environ["NS"], os.environ["GITHUB_SHA"]
    config = Path(os.environ["RUNNER_TEMP"]) / "runtime-values.yaml"
    documents = [
        row
        for row in yaml.safe_load_all(
            smoke.command(
                [
                    "helm",
                    "template",
                    "custos",
                    os.environ["CHART"],
                    "-n",
                    namespace,
                    "-f",
                    os.environ["VALUES"],
                    "-f",
                    str(config),
                ]
            )
        )
        if row
    ]
    token, _ = mint_token()
    smoke.command(
        ["kubectl", "-n", namespace, "create", "-f", "-"],
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": OPERATOR_SECRET},
            "stringData": {"token": token},
        },
    )
    smoke.command(
        ["kubectl", "-n", namespace, "create", "-f", "-"],
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "receiver-certification-script"},
            "data": {"receivers.py": Path(__file__).read_text()},
        },
    )
    job = smoke.seed_job(documents, tag)
    job["metadata"]["name"] = "receiver-certification-seed"
    pod = job["spec"]["template"]["spec"]
    pod["serviceAccountName"] = "smoke-custos-bootstrap"
    pod["volumes"][0]["configMap"]["name"] = "receiver-certification-script"
    container = pod["containers"][0]
    container["command"] = ["python", "/smoke/receivers.py", "seed"]
    container["env"] = [
        row for row in container["env"] if row["name"] != "SMOKE_EXECUTION_TOKEN"
    ]
    container["env"].append(
        {
            "name": "CUSTOS_TEST_TOKEN",
            "valueFrom": {"secretKeyRef": {"name": OPERATOR_SECRET, "key": "token"}},
        }
    )
    smoke.command(["kubectl", "-n", namespace, "create", "-f", "-"], job)
    smoke.command(
        [
            "kubectl",
            "-n",
            namespace,
            "wait",
            "--for=condition=complete",
            "job/receiver-certification-seed",
            "--timeout=3m",
        ]
    )


def client_job(image: str) -> dict[str, Any]:
    """Run a credential-scoped, tokenless Kubernetes client with only existing public-client access."""
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "receiver-certification-client"},
        "spec": {
            "backoffLimit": 0,
            "activeDeadlineSeconds": 600,
            "template": {
                "metadata": {
                    "labels": {
                        "app.kubernetes.io/instance": "custos",
                        "app.kubernetes.io/component": "smoke-test",
                    }
                },
                "spec": {
                    "automountServiceAccountToken": False,
                    "restartPolicy": "Never",
                    "securityContext": {
                        "runAsNonRoot": True,
                        "runAsUser": 1000,
                        "seccompProfile": {"type": "RuntimeDefault"},
                    },
                    "containers": [
                        {
                            "name": "client",
                            "image": image,
                            "imagePullPolicy": "IfNotPresent",
                            "command": ["python", "/smoke/receivers.py", "exercise"],
                            "securityContext": {
                                "allowPrivilegeEscalation": False,
                                "readOnlyRootFilesystem": True,
                                "capabilities": {"drop": ["ALL"]},
                            },
                            "env": [
                                {
                                    "name": "CUSTOS_TEST_TOKEN",
                                    "valueFrom": {
                                        "secretKeyRef": {
                                            "name": OPERATOR_SECRET,
                                            "key": "token",
                                        }
                                    },
                                },
                                {
                                    "name": "CUSTOS_GATEWAY_URL",
                                    "value": "http://custos-api-gateway:8080",
                                },
                                {
                                    "name": "GITHUB_SHA",
                                    "value": os.environ["GITHUB_SHA"],
                                },
                                {"name": "PYTHONDONTWRITEBYTECODE", "value": "1"},
                            ],
                            "volumeMounts": [
                                {
                                    "name": "script",
                                    "mountPath": "/smoke",
                                    "readOnly": True,
                                }
                            ],
                            "resources": {
                                "requests": {"cpu": "50m", "memory": "64Mi"},
                                "limits": {"cpu": "500m", "memory": "256Mi"},
                            },
                        }
                    ],
                    "volumes": [
                        {
                            "name": "script",
                            "configMap": {"name": "receiver-certification-script"},
                        }
                    ],
                },
            },
        },
    }


def run(output: Path) -> None:
    """Require the real in-cluster job and extract only its explicitly sanitized evidence object."""
    import runtime as smoke

    namespace = os.environ["NS"]
    image = os.environ["IMAGE_PREFIX"] + "/api-gateway:" + os.environ["GITHUB_SHA"]
    smoke.command(["kubectl", "-n", namespace, "create", "-f", "-"], client_job(image))
    deadline = time.monotonic() + 600
    succeeded = False
    while time.monotonic() < deadline:
        job = json.loads(
            smoke.command(
                [
                    "kubectl",
                    "-n",
                    namespace,
                    "get",
                    "job/receiver-certification-client",
                    "-o",
                    "json",
                ]
            )
        )
        status = job.get("status", {})
        if status.get("succeeded"):
            succeeded = True
            break
        if status.get("failed"):
            break
        time.sleep(2)
    logs = smoke.command(
        ["kubectl", "-n", namespace, "logs", "job/receiver-certification-client"]
    )
    reports = [
        json.loads(line.removeprefix(REPORT_PREFIX))
        for line in logs.splitlines()
        if line.startswith(REPORT_PREFIX)
    ]
    if not succeeded or len(reports) != 1:
        failures = [
            line
            for line in logs.splitlines()
            if line.startswith("CERTIFICATION_FAILED ")
        ]
        for failure in failures:
            print(failure, file=sys.stderr)
        raise CertificationFailure(
            "receiver client job failed; no certification claimed"
        )
    report = reports[0]
    if report["sha"] != os.environ["GITHUB_SHA"]:
        raise CertificationFailure("receiver evidence does not match deployed commit")
    output.write_text(json.dumps(report, indent=2))
    print(
        "Real deployed receiver effect evidence captured; full matrix remains required."
    )


def main() -> None:
    """Separate runner orchestration, real-provider seeding and public-only application probing."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("prepare", "seed", "exercise", "run"))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.phase == "prepare":
        prepare()
    elif args.phase == "seed":
        asyncio.run(seed())
    elif args.phase == "exercise":
        exercise()
    elif args.output is None:
        parser.error("--output is required for run")
    else:
        run(args.output)


if __name__ == "__main__":
    try:
        main()
    except CertificationFailure as error:
        print("CERTIFICATION_FAILED " + str(error), file=sys.stderr, flush=True)
        sys.exit(1)
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as error:
        print(
            f"CERTIFICATION_FAILED {type(error).__name__}; inspect owned workload status",
            file=sys.stderr,
            flush=True,
        )
        sys.exit(1)
