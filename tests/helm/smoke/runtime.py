"""Provision the disposable smoke deployment's real managed runtime prerequisites.

Values contain references only. Secret material travels to Kubernetes on stdin,
never through Helm values or command arguments. The seed command runs inside the
production bootstrap image, using its actual domain seeder and PostgreSQL adapter.
This is a startup/readiness check, not receiver business or network-isolation certification.
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import ipaddress
import json
import os
import re
import secrets
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

import yaml

SERVICES = (
    "api-gateway",
    "auth-service",
    "workflow-service",
    "trigger-service",
    "connector-service",
    "activity-runtime-manager",
    "catalog-service",
    "observability-audit-service",
)
WORKSPACE = "workspace-default"
PRINCIPAL = "smoke-executor"
EXECUTION_SECRET = "smoke-execution"


def command(args: list[str], payload: object = None, *, timeout: int = 360) -> str:
    """Capture private command output; surface failures without echoing Secret-bearing streams."""
    result = subprocess.run(
        args,
        input=None if payload is None else json.dumps(payload),
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    if result.returncode:
        raise RuntimeError(f"smoke prerequisite command failed: {args[0]} {args[1]}")
    return result.stdout


def settings(
    tag: str,
    namespace: str,
    api_addresses: list[str],
    sidecar: str,
    helper: str,
) -> dict[str, Any]:
    """Build an opt-in reference-only overlay; ordinary eval/production defaults stay off."""
    if re.fullmatch(r"[a-f0-9]{40}", tag) is None:
        raise ValueError("smoke requires the exact tested Git commit as image tag")
    for image in (sidecar, helper):
        if re.fullmatch(r"localhost:5001/[\w-]+@sha256:[a-f0-9]{64}", image) is None:
            raise ValueError(
                "smoke helper images must be actual local-registry digest references"
            )
    cidrs = [str(ipaddress.ip_network(address + "/32")) for address in api_addresses]
    if not cidrs:
        raise ValueError("smoke requires explicit Kubernetes API addresses")
    return {
        "namespace": {"name": namespace, "create": False},
        "global": {
            "imageTag": tag,
            "networkPolicies": {"enabled": True},
            "receiverRuntime": {
                "enabled": True,
                "admissionEnabled": False,
                "deploymentId": "deployment-smoke",
                "apiTokenSecret": "smoke-dapr-api",
                "appTokenSecrets": {name: "smoke-" + name for name in SERVICES},
                "authSigningKeyRef": "smoke-signing-key",
                "executionBindings": {
                    "bindings": [
                        {
                            "workspaceId": WORKSPACE,
                            "principalId": PRINCIPAL,
                            "bindingId": "smoke-v1",
                            "secretRef": EXECUTION_SECRET,
                            "secretKey": "token",
                        }
                    ],
                    "defaults": {WORKSPACE: "smoke-v1"},
                },
                "sandboxNamespace": namespace + "-activities",
                "tlsSecret": "smoke-runtime-tls",
                "artifactClaim": "smoke-artifacts",
                "telemetryEndpoint": "http://custos-otel-collector:4318",
                "sidecarImage": sidecar,
                "ioBridgeImage": (
                    "busybox:1.37.0@sha256:"
                    "9532d8c39891ca2ecde4d30d7710e01fb739c87a8b9299685c63704296b16028"
                ),
                "kubernetesApiCIDRs": cidrs,
            },
        },
        "postgres": {"embedded": False},
        "certManager": {"install": False},
        "connector-service": {
            "hookRuntime": {
                "enabled": True,
                "namespace": namespace + "-hooks",
                "helperImage": helper,
                "registryUrl": "http://custos-smoke-registry:5000",
            },
        },
        "migrate": {"image": {"tag": tag}},
        "bootstrap": {"image": {"tag": tag}},
    }


def seed_job(documents: list[dict[str, Any]], image_tag: str) -> dict[str, Any]:
    """Run real platform bootstrap plus a workspace-only execution grant before service admission."""
    source = next(
        row
        for row in documents
        if row["kind"] == "Job" and row["metadata"]["name"] == "custos-bootstrap"
    )
    job = copy.deepcopy(source)
    job["metadata"]["name"] = "smoke-execution-bootstrap"
    job["metadata"].pop("annotations", None)
    pod = job["spec"]["template"]["spec"]
    # Use the chart's actual bootstrap service account, created for the preliminary job.
    pod["volumes"] = [{"name": "script", "configMap": {"name": "smoke-runtime-script"}}]
    container = pod["containers"][0]
    if not container["image"].endswith(":" + image_tag):
        raise ValueError("bootstrap image must match the tested commit")
    container["command"] = ["python", "/smoke/runtime.py", "seed"]
    container["env"].append(
        {
            "name": "SMOKE_EXECUTION_TOKEN",
            "valueFrom": {"secretKeyRef": {"name": EXECUTION_SECRET, "key": "token"}},
        }
    )
    container["volumeMounts"] = [
        {"name": "script", "mountPath": "/smoke", "readOnly": True}
    ]
    job["spec"]["backoffLimit"] = 0
    job["spec"]["activeDeadlineSeconds"] = 180
    return job


async def seed_execution() -> None:
    """Seed actual canonical platform state and a scoped, expiring executor; never use raw SQL."""
    from datetime import UTC, datetime, timedelta
    from typing import cast

    from custos_auth.roles import ROLE_WORKSPACE_EXECUTOR
    from custos_auth.tokens import hash_token, looks_like_custos_token
    from custos_bootstrap.__main__ import (
        resolve_dsn,
        resolve_permission_paths,
        seed_platform,
    )
    from custos_pg import PgAuthAdapter
    from custos_pg.pool import LazyPool
    from custos_spl import AuthStoreProvider
    from custos_spl.ids import PrincipalId, RoleBindingId, ServiceTokenId, WorkspaceId
    from custos_spl.interfaces.auth_store import (
        RoleBinding,
        ServiceAccount,
        ServiceToken,
        WorkspaceScope,
    )

    token = os.environ["SMOKE_EXECUTION_TOKEN"]
    if not looks_like_custos_token(token):
        raise ValueError("smoke requires a canonical execution credential")
    dsn = resolve_dsn(os.environ)
    if not dsn:
        raise ValueError("smoke bootstrap requires its database Secret")
    os.environ["CUSTOS_PG_DSN"] = dsn
    pool = LazyPool(dsn)
    store = PgAuthAdapter(lazy=pool)
    now = datetime.now(UTC)
    try:
        await seed_platform(
            cast(AuthStoreProvider, store),
            admin_oidc_issuer=None,
            admin_oidc_subject=None,
            now=now,
            permission_paths=resolve_permission_paths(os.environ),
        )
        await store.put_principal(
            ServiceAccount(
                kind="serviceAccount",
                principal_id=PrincipalId(PRINCIPAL),
                workspace_id=WorkspaceId(WORKSPACE),
                display_name="Disposable smoke executor",
                disabled_at=None,
                disabled_reason=None,
                created_at=now,
            )
        )
        await store.put_role_binding(
            RoleBinding(
                binding_id=RoleBindingId("smoke-executor"),
                principal_id=PrincipalId(PRINCIPAL),
                role_id=ROLE_WORKSPACE_EXECUTOR,
                scope=WorkspaceScope(WorkspaceId(WORKSPACE)),
                bound_at=now,
                bound_by=PrincipalId("smoke-bootstrap"),
            )
        )
        await store.put_service_token(
            ServiceToken(
                token_id=ServiceTokenId("smoke-execution"),
                service_account_id=PrincipalId(PRINCIPAL),
                hash=hash_token(token),
                issued_at=now,
                expires_at=now + timedelta(hours=2),
                revoked_at=None,
                revoked_by=None,
                revoked_reason=None,
            )
        )
    finally:
        await pool.aclose()
    print("Canonical platform and workspace-scoped execution identity seeded.")


def prepare(output: Path) -> None:
    """Create real prerequisites and preseed production state before Helm waits for applications."""
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    from custos_auth.tokens import mint_token

    namespace, tag = os.environ["NS"], os.environ["GITHUB_SHA"]

    def kube(*args: str, payload: object = None) -> str:
        """Address only the workflow's disposable namespace with private stdin/stdout."""
        return command(["kubectl", "-n", namespace, *args], payload)

    service = json.loads(
        command(["kubectl", "get", "service", "kubernetes", "-o", "json"])
    )
    endpoints = json.loads(
        command(["kubectl", "get", "endpoints", "kubernetes", "-o", "json"])
    )
    addresses = [
        service["spec"]["clusterIP"],
        *[
            address["ip"]
            for subset in endpoints["subsets"]
            for address in subset["addresses"]
        ],
    ]
    config = settings(
        tag,
        namespace,
        addresses,
        os.environ["SMOKE_SIDECAR_IMAGE"],
        os.environ["SMOKE_HOOK_HELPER_IMAGE"],
    )
    output.write_text(yaml.safe_dump(config))
    runtime = config["global"]["receiverRuntime"]
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        cert, key = root / "tls.crt", root / "tls.key"
        hostname = f"custos-activity-runtime-manager.{namespace}.svc"
        command(
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "2",
                "-subj",
                "/CN=" + hostname,
                "-addext",
                f"subjectAltName=DNS:{hostname},DNS:localhost,IP:127.0.0.1",
                "-out",
                str(cert),
                "-keyout",
                str(key),
            ]
        )
        signing_key = (
            Ed25519PrivateKey.generate()
            .private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
            .decode()
        )
        execution_token, _ = mint_token()
        material = {
            runtime["apiTokenSecret"]: {"token": secrets.token_hex(32)},
            **{
                name: {"token": secrets.token_hex(32)}
                for name in runtime["appTokenSecrets"].values()
            },
            runtime["authSigningKeyRef"]: {"key": signing_key},
            runtime["tlsSecret"]: {
                "tls.crt": cert.read_text(),
                "tls.key": key.read_text(),
                "ca.crt": cert.read_text(),
            },
            EXECUTION_SECRET: {"token": execution_token},
        }
        for name, data in material.items():
            kube(
                "create",
                "-f",
                "-",
                payload={
                    "apiVersion": "v1",
                    "kind": "Secret",
                    "metadata": {"name": name},
                    "stringData": data,
                },
            )
    kube(
        "create",
        "-f",
        "-",
        payload={
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {"name": runtime["artifactClaim"]},
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "storageClassName": "standard",
                "resources": {"requests": {"storage": "1Gi"}},
            },
        },
    )
    documents = [
        row
        for row in yaml.safe_load_all(
            command(
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
                    str(output),
                ]
            )
        )
        if row
    ]
    for name in ("custos-migrate", "custos-bootstrap"):
        account = next(
            row
            for row in documents
            if row["kind"] == "ServiceAccount" and row["metadata"]["name"] == name
        )
        resource = copy.deepcopy(account)
        resource["metadata"]["name"] = "smoke-" + name
        resource["metadata"].pop("annotations", None)
        kube("create", "-f", "-", payload=resource)
    migration = copy.deepcopy(
        next(
            row
            for row in documents
            if row["kind"] == "Job" and row["metadata"]["name"] == "custos-migrate"
        )
    )
    migration["metadata"]["name"] = "smoke-schema-migrate"
    migration["metadata"].pop("annotations", None)
    migration["spec"]["template"]["spec"]["serviceAccountName"] = "smoke-custos-migrate"
    migration["spec"]["activeDeadlineSeconds"] = 180
    kube("create", "-f", "-", payload=migration)
    kube("wait", "--for=condition=complete", "job/smoke-schema-migrate", "--timeout=3m")
    kube(
        "create",
        "-f",
        "-",
        payload={
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "smoke-runtime-script"},
            "data": {"runtime.py": Path(__file__).read_text()},
        },
    )
    bootstrap = seed_job(documents, tag)
    bootstrap["spec"]["template"]["spec"]["serviceAccountName"] = (
        "smoke-custos-bootstrap"
    )
    kube("create", "-f", "-", payload=bootstrap)
    kube(
        "wait",
        "--for=condition=complete",
        "job/smoke-execution-bootstrap",
        "--timeout=3m",
    )
    print(
        "Real schema, platform bootstrap, execution identity, TLS and CSI claim provisioned."
    )


def verify(output: Path) -> None:
    """Require the actual scheduler's recurring readiness probes over verified local TLS."""
    namespace = os.environ["NS"]
    script = """import json, os, ssl, urllib.request
assert os.environ["ARM_RUNTIME_ENABLED"] == "true"
assert os.environ["ARM_ARTIFACT_STORE"] == "csi"
context = ssl.create_default_context(cafile="/custos/tls/ca.crt")
with urllib.request.urlopen("https://localhost:8080/readyz", context=context, timeout=15) as result:
    assert result.status == 200
    assert json.load(result)["status"] == "ready"
print("Actual scheduler and recurring production dependency probes ready.")
"""
    command(
        [
            "kubectl",
            "-n",
            namespace,
            "exec",
            "deployment/custos-activity-runtime-manager",
            "-c",
            "activity-runtime-manager",
            "--",
            "python",
            "-c",
            script,
        ]
    )
    output.write_text(
        json.dumps(
            {
                "sha": os.environ["GITHUB_SHA"],
                "actualSchedulerReady": True,
                "artifactProvider": "csi",
                "executionWorkspace": WORKSPACE,
                "receiverAdmissionEnabled": False,
                "fullReceiverCertification": False,
            },
            indent=2,
        )
    )
    print(
        "Verified actual scheduler readiness; no receiver-effects certification claimed."
    )


def main() -> None:
    """Dispatch only explicit smoke setup/verification or in-Pod production seeding."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("prepare", "seed", "verify"))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.phase == "seed":
        asyncio.run(seed_execution())
    elif args.output is None:
        parser.error("--output is required for prepare/verify")
    elif args.phase == "prepare":
        prepare(args.output)
    else:
        verify(args.output)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as error:
        # External failures may contain credentials; publish only the failure category.
        print(
            f"Smoke runtime failed ({type(error).__name__}); inspect owned workload status.",
            file=sys.stderr,
        )
        sys.exit(1)
