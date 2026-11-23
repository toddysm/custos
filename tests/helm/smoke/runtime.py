"""Provision the disposable smoke deployment's real managed runtime prerequisites.

Values contain references only. Secret material travels to Kubernetes on stdin,
never through Helm values or command arguments. The seed command runs inside the
production bootstrap image, using its actual domain seeder and PostgreSQL adapter.
This is a startup/readiness check, not receiver business or network-isolation certification.
Failed verification retains only closed probe categories/status and source-only
current/previous ARM log summaries, independently of Kubernetes Pod readiness.
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import inspect
import ipaddress
import json
import os
import re
import secrets
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Literal

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
READINESS_PREFIX = "RUNTIME_READINESS_PROBE "
ARM_LOG_BYTE_LIMIT = 32768


def command(
    args: list[str],
    payload: object = None,
    *,
    timeout: int = 360,
    errors: Literal["strict", "replace"] = "strict",
) -> str:
    """Capture private output; surface failures without echoing Secret-bearing streams.

    Only log collection opts into replacement decoding for byte-cut UTF-8;
    all other command consumers retain strict decoding.
    """
    result = subprocess.run(
        args,
        input=None if payload is None else json.dumps(payload),
        capture_output=True,
        text=True,
        errors=errors,
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
    from custos_pg.pool import LazyPool, register_jsonb_codec
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
    pool = LazyPool(dsn, init=register_jsonb_codec)
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
                bound_by=PrincipalId(PRINCIPAL),
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


def readiness_probe() -> dict[str, object]:
    """Run once inside ARM, returning bounded metadata instead of private exception text.

    This function is serialized into the container; imports must be self-contained.
    TLS trust, the 15-second request timeout and the readiness contract are unchanged.
    Response decoding is bounded to 4096 bytes; no body or environment value is retained.
    """
    import json
    import os
    import re
    import ssl
    import traceback
    import urllib.error
    import urllib.request
    from pathlib import Path

    phase = "configuration"
    status = None
    try:
        assert os.environ.get("ARM_RUNTIME_ENABLED") == "true"
        assert os.environ.get("ARM_ARTIFACT_STORE") == "csi"
        phase = "tls"
        context = ssl.create_default_context(cafile="/custos/tls/ca.crt")
        phase = "request"
        with urllib.request.urlopen(
            "https://localhost:8080/readyz", context=context, timeout=15
        ) as result:
            status = result.status
            phase = "response"
            assert status == 200
            body = result.read(4097)
            if len(body) > 4096:
                raise ValueError("readiness response exceeds its bound")
            document = json.loads(body)
            assert isinstance(document, dict) and document.get("status") == "ready"
    except (AssertionError, OSError, ValueError, urllib.error.URLError) as error:
        root = (
            error.reason
            if isinstance(error, urllib.error.URLError)
            and isinstance(error.reason, BaseException)
            else error
        )
        if isinstance(error, urllib.error.HTTPError):
            status = error.code
            error.close()
            category = "http_status"
        elif isinstance(root, ssl.SSLError) or phase == "tls":
            category = "tls"
        elif isinstance(root, TimeoutError):
            category = "timeout"
        elif phase == "configuration":
            category = "configuration"
        elif phase == "response":
            category = "invalid_response"
        else:
            category = "transport"
        name = type(root).__name__
        allowed_types = {
            "AssertionError",
            "OSError",
            "FileNotFoundError",
            "PermissionError",
            "HTTPError",
            "URLError",
            "SSLError",
            "SSLCertVerificationError",
            "TimeoutError",
            "ConnectionRefusedError",
            "ConnectionResetError",
            "ValueError",
            "JSONDecodeError",
            "UnicodeDecodeError",
        }
        frames = []
        for frame in traceback.extract_tb(root.__traceback__ or error.__traceback__)[
            -5:
        ]:
            file = (
                "runtime.py"
                if frame.filename == "<string>"
                else Path(frame.filename).name
            )
            if re.fullmatch(r"[A-Za-z0-9_-]{1,100}\.py", file) and re.fullmatch(
                r"[A-Za-z0-9_<>-]{1,100}", frame.name
            ):
                frames.append(
                    {"file": file, "function": frame.name, "line": frame.lineno}
                )
        return {
            "ready": False,
            "phase": phase,
            "category": category,
            "errorType": name if name in allowed_types else "unrecognized",
            "httpStatus": status,
            "frames": frames,
        }
    return {
        "ready": True,
        "phase": "response",
        "category": "ready",
        "errorType": None,
        "httpStatus": status,
        "frames": [],
    }


def readiness_probe_record(text: str) -> dict[str, Any]:
    """Validate one bounded child record and discard all unrecognized fields."""
    from public_workflow import diagnostic_source_frames

    lines = [line for line in text.splitlines() if line.startswith(READINESS_PREFIX)]
    if len(lines) != 1 or len(lines[0]) > 8192:
        raise ValueError("readiness probe must emit one bounded record")
    row = json.loads(lines[0].removeprefix(READINESS_PREFIX))
    if (
        not isinstance(row, dict)
        or not {
            "ready",
            "phase",
            "category",
            "errorType",
            "httpStatus",
            "frames",
        }.issubset(row)
        or type(row.get("ready")) is not bool
        or not isinstance(row.get("phase"), str)
        or row.get("phase") not in {"configuration", "tls", "request", "response"}
        or not isinstance(row.get("category"), str)
        or row.get("category")
        not in {
            "ready",
            "configuration",
            "tls",
            "http_status",
            "timeout",
            "invalid_response",
            "transport",
        }
        or (row.get("errorType") is not None and not isinstance(row["errorType"], str))
        or row.get("errorType")
        not in {
            None,
            "AssertionError",
            "OSError",
            "FileNotFoundError",
            "PermissionError",
            "HTTPError",
            "URLError",
            "SSLError",
            "SSLCertVerificationError",
            "TimeoutError",
            "ConnectionRefusedError",
            "ConnectionResetError",
            "ValueError",
            "JSONDecodeError",
            "UnicodeDecodeError",
            "unrecognized",
        }
        or (
            row.get("httpStatus") is not None
            and (
                type(row["httpStatus"]) is not int
                or not 100 <= row["httpStatus"] <= 599
            )
        )
        or (
            row["ready"]
            and (
                row["category"] != "ready"
                or row["phase"] != "response"
                or row.get("httpStatus") != 200
                or row.get("errorType") is not None
            )
        )
        or (
            not row["ready"]
            and (row["category"] == "ready" or row.get("errorType") is None)
        )
    ):
        raise ValueError("readiness probe record has invalid taxonomy or status")
    return {
        key: row[key]
        for key in ("ready", "phase", "category", "errorType", "httpStatus")
    } | {"frames": diagnostic_source_frames(row.get("frames", []))}


def component_failure_records(logs: str) -> list[dict[str, Any]]:
    """Keep the last 50 closed ARM records in the supplied sample, never arbitrary log fields.

    Probe IDs group failures within one startup or readiness cycle; separate
    log streams remain separate observations, not proof of the active HTTP cause.
    The collector separately records truncation; malformed retained records raise.
    """
    from public_workflow import diagnostic_source_frames

    records: list[dict[str, Any]] = []
    prefix = "ARM_READINESS_FAILURE "
    for line in reversed(logs.splitlines()):
        if prefix not in line:
            continue
        if len(records) >= 50:
            break
        text = line.split(prefix, 1)[1]
        if len(text) > 4096:
            raise ValueError("ARM component evidence exceeds its bound")
        row = json.loads(text)
        if (
            not isinstance(row, dict)
            or not isinstance(row.get("probeId"), str)
            or re.fullmatch(r"[a-f0-9]{32}", row["probeId"]) is None
            or not isinstance(row.get("phase"), str)
            or row["phase"] not in {"startup", "readiness"}
            or not isinstance(row.get("component"), str)
            or row["component"]
            not in {
                "metadata-schema",
                "execution-schema",
                "lease-schema",
                "metadata",
                "artifacts",
                "execution-authorization",
                "catalog",
                "connector",
                "kubernetes-jobs",
                "kubernetes-pods",
                "dependencies",
                "scheduler",
                "lifecycle",
            }
            or not isinstance(row.get("reason"), str)
            or row["reason"]
            not in {
                "error",
                "http_status",
                "group_failed",
                "scheduler_missing",
                "gate_closed",
                "unavailable",
                "denied",
            }
            or (
                row.get("errorType") is not None
                and not isinstance(row["errorType"], str)
            )
            or row.get("errorType")
            not in {
                None,
                "RuntimeError",
                "ExceptionGroup",
                "TimeoutError",
                "OSError",
                "PermissionError",
                "FileNotFoundError",
                "BackendUnavailable",
                "MigrationRequired",
                "ExecutionAuthorizationError",
                "HTTPStatusError",
                "ConnectError",
                "ConnectTimeout",
                "ReadError",
                "ReadTimeout",
                "WriteError",
                "WriteTimeout",
                "PoolTimeout",
                "RemoteProtocolError",
                "ApiException",
                "MaxRetryError",
                "ProtocolError",
                "SSLError",
                "unrecognized",
            }
            or (
                row.get("httpStatus") is not None
                and (
                    type(row["httpStatus"]) is not int
                    or not 100 <= row["httpStatus"] <= 599
                )
            )
        ):
            raise ValueError("ARM component evidence has invalid identity or taxonomy")
        records.append(
            {
                key: row.get(key)
                for key in (
                    "probeId",
                    "phase",
                    "component",
                    "reason",
                    "errorType",
                    "httpStatus",
                )
            }
            | {"frames": diagnostic_source_frames(row.get("frames", []))}
        )
    return list(reversed(records))


def collect_readiness_failure(output: Path, report: dict[str, Any]) -> None:
    """Retain the primary failure plus bounded ARM log locations, even for Ready Pods.

    Current and previous streams include validated component failures and source
    locations as independent observations. Missing previous logs
    and other collection failures remain explicit; neither can replace the probe
    failure. No raw log text, exception message or process stream is printed.
    """
    from public_workflow import failure_locations

    for source, extra in (("arm-current", []), ("arm-previous", ["--previous"])):
        try:
            logs = command(
                [
                    "kubectl",
                    "-n",
                    os.environ["NS"],
                    "logs",
                    "deployment/custos-activity-runtime-manager",
                    "-c",
                    "activity-runtime-manager",
                    "--tail=200",
                    f"--limit-bytes={ARM_LOG_BYTE_LIMIT}",
                    *extra,
                ],
                timeout=20,
                errors="replace",
            )
            byte_limit_reached = len(logs.encode("utf-8")) >= ARM_LOG_BYTE_LIMIT
            if byte_limit_reached and not logs.endswith("\n"):
                # kubectl limits the beginning of the tail, possibly cutting its final record.
                logs = logs.rpartition("\n")[0]
            report["serviceLogs"][source] = failure_locations(logs) | {
                "logByteLimitReached": byte_limit_reached
            }
            report["serviceLogs"][source]["componentFailures"] = (
                component_failure_records(logs)
            )
            report["serviceLogs"][source]["componentFailuresTruncated"] = (
                byte_limit_reached
                or sum("ARM_READINESS_FAILURE " in line for line in logs.splitlines())
                > 50
            )
        except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired) as error:
            report["collectionErrors"].append(
                {
                    "source": source,
                    "errorType": (
                        "TimeoutExpired"
                        if isinstance(error, subprocess.TimeoutExpired)
                        else "OSError"
                        if isinstance(error, OSError)
                        else "RuntimeError"
                        if isinstance(error, RuntimeError)
                        else "ValueError"
                    ),
                }
            )
    try:
        output.with_name("runtime-readiness-diagnostics.json").write_text(
            json.dumps(report, indent=2)
        )
    except OSError:
        report["collectionErrors"].append(
            {"source": "artifact", "errorType": "OSError"}
        )
    print(json.dumps(report), flush=True)


def verify(output: Path) -> None:
    """Require one real trusted readiness check; retain safe failure evidence before raising."""
    sha = os.environ["GITHUB_SHA"]
    if re.fullmatch(r"[a-f0-9]{40}", sha) is None:
        raise ValueError("readiness verification requires the exact tested commit")
    report: dict[str, Any] = {
        "sha": sha,
        "diagnosticOnly": True,
        "readinessProofFailed": True,
        "failedPhase": "scheduler-readiness",
        "probe": None,
        "failure": None,
        "commandExitCode": None,
        "serviceLogs": {},
        "collectionErrors": [],
    }
    source, first_line = inspect.getsourcelines(readiness_probe)
    # Keep the serialized function's source locations aligned with this file.
    script = (
        "\n" * (first_line - 1)
        + "".join(source)
        + "\nimport json\n"
        + f"\nrecord = readiness_probe()\nprint({READINESS_PREFIX!r} + json.dumps(record), flush=True)"
        + "\nraise SystemExit(0 if record['ready'] else 1)\n"
    )
    try:
        result = subprocess.run(
            [
                "kubectl",
                "-n",
                os.environ["NS"],
                "exec",
                "deployment/custos-activity-runtime-manager",
                "-c",
                "activity-runtime-manager",
                "--",
                "python",
                "-c",
                script,
            ],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        report["commandExitCode"] = result.returncode
        if result.returncode and READINESS_PREFIX not in result.stdout:
            report["failure"] = {"category": "exec_exit"}
            raise RuntimeError("runtime readiness command failed before reporting")
        report["probe"] = readiness_probe_record(result.stdout)
        if result.returncode or not report["probe"]["ready"]:
            report["failure"] = {
                "category": "probe_rejected"
                if not report["probe"]["ready"]
                else "exec_exit"
            }
            raise RuntimeError("runtime readiness verification failed")
    except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired) as error:
        if report["failure"] is None:
            report["failure"] = {
                "category": (
                    "exec_timeout"
                    if isinstance(error, subprocess.TimeoutExpired)
                    else "exec_unavailable"
                    if isinstance(error, OSError)
                    else "invalid_probe_record"
                )
            }
        collect_readiness_failure(output, report)
        raise
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
