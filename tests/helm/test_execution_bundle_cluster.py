"""Hosted-only Helm 3 storage, real Dapr authorization and retained-snapshot lifecycle.

The full managed chart is installed with a configuration-probe image, not the
receiver business logic. Hook images are explicitly no-op fixtures. Actual
service startup contracts have separate service tests; deployed effects and
database migration certification remain #1018.
"""

from __future__ import annotations

import base64
import copy
import hashlib
import json
import os
import subprocess
import time
import tracemalloc
from pathlib import Path
from typing import Any

import pytest
import yaml
from test_execution_bundle import configuration, fixture, operator
from test_receiver_runtime import CHART, SERVICES

pytestmark = pytest.mark.skipif(
    os.environ.get("CUSTOS_BINDING_KIND") != "true",
    reason="requires explicitly selected disposable hosted kind; never run on an existing cluster",
)

DIAGNOSTIC_MARKERS = (
    "pubsub.redis",
    "secretstores.kubernetes",
    "state.postgresql",
    "state store is not configured",
    "actor state store",
    "failed to initialize",
    "failed to init component",
    "failed to get secret",
    "forbidden",
    "connection refused",
    "authentication failed",
    "password authentication failed",
    "noauth",
    "wrongpass",
    "no such host",
    "i/o timeout",
    "context deadline exceeded",
    "not registered",
    "failed to parse",
    "invalid character",
    "access control",
    "trust domain",
    "no module named",
    "permission denied",
    "failed to load",
    "certificate",
)


def diagnostic_markers(log: str) -> list[str]:
    """Return only fixed diagnostic categories; no raw log text or credential can escape."""
    return [marker for marker in DIAGNOSTIC_MARKERS if marker in log.lower()]


def diagnose_workloads(namespace: str) -> None:
    """Report unready regular/native-sidecar exits and fixed log categories, never raw logs."""
    inventory = json.loads(kube(namespace, "get", "pods", "-o", "json"))
    for pod in inventory["items"]:
        status = pod.get("status", {})
        for container in [
            *status.get("containerStatuses", []),
            *status.get("initContainerStatuses", []),
        ]:
            if container.get("ready"):
                continue
            previous = container.get("lastState", {}).get("terminated", {})
            args = [
                "kubectl",
                "-n",
                namespace,
                "logs",
                pod["metadata"]["name"],
                "-c",
                container["name"],
                "--tail=50",
            ]
            if previous:
                args.append("--previous")
            result = subprocess.run(
                args, capture_output=True, text=True, timeout=30, check=False
            )
            print(
                json.dumps(
                    {
                        "namespace": namespace,
                        "pod": pod["metadata"]["name"],
                        "container": container["name"],
                        "previousExit": previous.get("exitCode"),
                        "logsAvailable": result.returncode == 0,
                        "logCommandExit": result.returncode,
                        "diagnosticMarkers": diagnostic_markers(result.stdout),
                    }
                )
            )


def command(args: list[str], payload: Any = None, *, succeeds: bool = True) -> str:
    """Execute a bounded command without displaying values, manifests or Secret contents."""
    result = subprocess.run(
        args,
        input=None if payload is None else json.dumps(payload),
        capture_output=True,
        text=True,
        timeout=1500,
        check=False,
    )
    assert (result.returncode == 0) == succeeds, (
        f"{args[0]} {args[1]} unexpected exit {result.returncode}: {result.stderr[-1000:]}"
    )
    return result.stdout


def kube(namespace: str, *args: str, payload: Any = None, succeeds: bool = True) -> str:
    """Target only the explicitly owned namespace inside the verified disposable cluster."""
    return command(["kubectl", "-n", namespace, *args], payload, succeeds=succeeds)


def evidence(**values: Any) -> None:
    """Persist aggregate tested-SHA/storage/lifecycle evidence without registries or credentials."""
    path = Path(os.environ["BINDING_REPORT"])
    data = (
        json.loads(path.read_text())
        if path.exists()
        else {"sha": os.environ["GITHUB_SHA"]}
    )
    if "instrumented_consumer_parse_samples" in values:
        key = "instrumented_consumer_parse_samples"
        values[key] = [*data.get(key, []), *values[key]]
    data.update(values)
    path.write_text(json.dumps(data, indent=2))


def settings(namespace: str, *, chart_owned: bool = False) -> dict[str, Any]:
    """Keep actual managed policies/manifests, substituting only application/hook business logic."""
    config = configuration()
    config["namespace"] = {"name": namespace, "create": chart_owned}
    config["postgres"]["embedded"] = False
    config["secrets"]["eso"]["enabled"] = False
    config["gateway"]["tls"]["certManager"]["enabled"] = False
    config["envoyGateway"]["install"] = False
    config["certManager"]["install"] = False
    config["bootstrap"]["enabled"] = False
    config["global"]["imageTag"] = "tested"
    config["global"]["imagePullPolicy"] = "IfNotPresent"
    runtime = config["global"]["receiverRuntime"]
    runtime["sandboxNamespace"] = namespace + "-activities"
    runtime["artifactClaim"] = "binding-artifacts"
    config["connector-service"]["hookRuntime"]["namespace"] = namespace + "-hooks"
    config["connectorCredentials"]["namespace"] = namespace + "-credentials"
    config["dapr"]["injectorReadyGate"]["enabled"] = False
    config["dapr"]["components"]["pubsub"]["redis"]["host"] = "binding-redis:6379"
    config["dapr"]["components"]["pubsub"]["redis"]["passwordSecret"] = (
        "custos-redis-auth"
    )
    config["migrate"]["image"] = {"repository": "custos-binding-probe", "tag": "tested"}
    for service in SERVICES:
        config[service]["image"] = {
            "repository": "custos-binding-probe",
            "tag": "tested",
        }
        config[service]["resources"] = {
            "requests": {"cpu": "10m", "memory": "32Mi"},
            "limits": {"cpu": "500m", "memory": "256Mi"},
        }
        config[service]["env"] = [
            *config[service].get("env", []),
            {"name": "BINDING_UNRELATED", "value": "preserved"},
        ]
    return config


def backend(
    namespace: str,
    resource_name: str,
    port: int,
    container: dict[str, Any],
    *,
    labels: dict[str, str],
) -> None:
    """Create a ready backend selected by the unchanged chart's explicit network allow rules."""
    selector = {"app": resource_name, **labels}
    kube(
        namespace,
        "create",
        "-f",
        "-",
        payload={
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": resource_name},
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": selector},
                "template": {
                    "metadata": {"labels": selector},
                    "spec": {
                        "containers": [
                            {**container, "ports": [{"containerPort": port}]}
                        ]
                    },
                },
            },
        },
    )
    kube(namespace, "expose", "deployment", resource_name, f"--port={port}")
    kube(namespace, "rollout", "status", f"deployment/{resource_name}", "--timeout=3m")


def foundation(namespace: str, config: dict[str, Any], tmp_path: Path) -> str:
    """Create disposable TLS, Secrets, Redis and storage without changing chart security contracts."""
    metadata: dict[str, Any] = {"name": namespace}
    if config["namespace"]["create"]:
        metadata.update(
            {
                "labels": {"app.kubernetes.io/managed-by": "Helm"},
                "annotations": {
                    "meta.helm.sh/release-name": "custos",
                    "meta.helm.sh/release-namespace": namespace,
                },
            }
        )
    command(
        ["kubectl", "create", "-f", "-"],
        {"apiVersion": "v1", "kind": "Namespace", "metadata": metadata},
    )
    runtime = config["global"]["receiverRuntime"]
    certificate, key = tmp_path / "tls.crt", tmp_path / "tls.key"
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
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost,IP:127.0.0.1",
            "-out",
            str(certificate),
            "-keyout",
            str(key),
        ]
    )
    database_password = os.urandom(24).hex()
    refs = {
        runtime["apiTokenSecret"]: {"token": os.urandom(24).hex()},
        **{
            value: {"token": os.urandom(24).hex()}
            for value in runtime["appTokenSecrets"].values()
        },
        "custos-app": {
            "uri": f"postgresql://custos:{database_password}@binding-postgres:5432/custos?sslmode=disable",
            "password": database_password,
        },
        "custos-redis-auth": {"redis-password": os.urandom(24).hex()},
        runtime["tlsSecret"]: {
            "tls.crt": certificate.read_text(),
            "tls.key": key.read_text(),
            "ca.crt": certificate.read_text(),
        },
        "binding-denied": {"token": os.urandom(24).hex()},
        runtime["authSigningKeyRef"]: {"key": "fixture-only"},
    }
    for ref, data in refs.items():
        kube(
            namespace,
            "create",
            "-f",
            "-",
            payload={
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {"name": ref},
                "stringData": data,
            },
        )
    kube(
        namespace,
        "create",
        "-f",
        "-",
        payload={
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {"name": "binding-artifacts"},
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "resources": {"requests": {"storage": "1Gi"}},
            },
        },
    )
    backend(
        namespace,
        "binding-postgres",
        5432,
        {
            "name": "postgres",
            "image": "postgres:16-alpine",
            "env": [
                {"name": "POSTGRES_USER", "value": "custos"},
                {"name": "POSTGRES_DB", "value": "custos"},
                {
                    "name": "POSTGRES_PASSWORD",
                    "valueFrom": {
                        "secretKeyRef": {"name": "custos-app", "key": "password"}
                    },
                },
            ],
            "readinessProbe": {
                "exec": {"command": ["pg_isready", "-U", "custos", "-d", "custos"]},
                "periodSeconds": 2,
            },
            "resources": {
                "requests": {"cpu": "100m", "memory": "128Mi"},
                "limits": {"cpu": "500m", "memory": "512Mi"},
            },
        },
        labels=config["global"]["networkPolicies"]["postgresPodSelector"],
    )
    backend(
        namespace,
        "binding-redis",
        6379,
        {
            "name": "redis",
            "image": "redis:7-alpine",
            "args": ["--requirepass", refs["custos-redis-auth"]["redis-password"]],
        },
        labels={"app.kubernetes.io/name": "redis"},
    )
    uid = json.loads(command(["kubectl", "get", "namespace", namespace, "-o", "json"]))[
        "metadata"
    ]["uid"]
    assert isinstance(uid, str)
    return uid


def save_values(path: Path, config: dict[str, Any]) -> Path:
    """Write complete synthetic deployment values; generated bulk bundles never enter this file."""
    path.write_text(yaml.safe_dump(config))
    return path


def helm(
    namespace: str,
    action: str,
    chart: Path | None,
    path: Path | None,
    *,
    succeeds: bool = True,
    timeout: str = "5m",
) -> None:
    """Use actual default-Secret Helm storage; never use atomic/force/ownership bypass."""
    args = ["helm", action, "custos"]
    if chart is not None:
        args.append(str(chart))
    args.extend(["-n", namespace, "--wait", "--timeout", timeout])
    if path is not None:
        args.extend(["--reset-values"] if action == "upgrade" else [])
        args.extend(["-f", str(path)])
    try:
        command(args, succeeds=succeeds)
    except AssertionError:
        diagnose_workloads(namespace)
        raise


def execution_secrets(namespace: str, raw: bytes) -> None:
    """Provision synthetic keys outside Helm, testing real Secret admission at maximum lengths."""
    rows = json.loads(raw)["bindings"]
    kube(
        namespace,
        "apply",
        "-f",
        "-",
        payload={
            "apiVersion": "v1",
            "kind": "List",
            "items": [
                {
                    "apiVersion": "v1",
                    "kind": "Secret",
                    "metadata": {"name": row["secretRef"]},
                    "stringData": {row["secretKey"]: os.urandom(12).hex()},
                }
                for row in rows
            ],
        },
    )


def file_config(config: dict[str, Any], bundle: dict[str, Any]) -> dict[str, Any]:
    """Replace only the managed compact descriptor/mode, leaving original configuration intact."""
    config = copy.deepcopy(config)
    config["namespace"]["create"] = False
    runtime = config["global"]["receiverRuntime"]
    runtime["retainLegacyExecutionResources"] = False
    runtime["executionBindings"] = {"bindings": [], "defaults": None}
    runtime["executionBindingsBundle"] = bundle["descriptor"]
    return config


def object_uids(resources: list[dict[str, Any]]) -> dict[str, str]:
    """Capture original API identities so lifecycle assertions cannot accept silent recreation."""
    result = {}
    for resource in resources:
        live = operator.get_resource(resource)
        assert live is not None
        result[f"{resource['kind']}/{resource['metadata']['name']}"] = live["metadata"][
            "uid"
        ]
    return result


def pods(namespace: str) -> dict[str, dict[str, Any]]:
    """Select the ready consumer Pod for each real chart Deployment."""
    result = {}
    for service in operator.CONSUMERS:
        items = json.loads(
            kube(
                namespace,
                "get",
                "pods",
                "-l",
                f"app.kubernetes.io/name={service}",
                "-o",
                "json",
            )
        )["items"]
        result[service] = next(
            row
            for row in items
            if any(
                condition["type"] == "Ready" and condition["status"] == "True"
                for condition in row.get("status", {}).get("conditions", [])
            )
            and not row["metadata"].get("deletionTimestamp")
        )
    return result


def probe(
    namespace: str,
    pod: dict[str, Any],
    service: str,
    route: str,
    identity: dict[str, str] | None = None,
) -> Any:
    """Read synthetic aggregate diagnostics inside the original application container."""
    tls = service == "activity-runtime-manager"
    scheme = "https" if tls else "http"
    context = (
        ",context=ssl.create_default_context(cafile=os.environ['ARM_TLS_CERT_FILE'])"
        if tls
        else ""
    )
    code = (
        "import json,os,ssl,urllib.request;"
        f"data={None if identity is None else json.dumps(identity).encode()!r};"
        f"req=urllib.request.Request('{scheme}://127.0.0.1:8080{route}',data=data,headers={{'Content-Type':'application/json'}});"
        f"print(urllib.request.urlopen(req,timeout=30{context}).read().decode())"
    )
    return json.loads(
        kube(
            namespace,
            "exec",
            pod["metadata"]["name"],
            "-c",
            service,
            "--",
            "python",
            "-c",
            code,
        )
    )


def assert_consumers(namespace: str, raw: bytes) -> dict[str, dict[str, Any]]:
    """Verify exact snapshot, defaults, pinned identity and actual Dapr Secret boundaries."""
    current = pods(namespace)
    data = json.loads(raw)
    first = data["bindings"][0]
    identity = {key: first[key] for key in ("workspaceId", "principalId", "bindingId")}
    registry = operator.ExecutionBindings.from_file_bytes(raw)
    samples = []
    for service, pod in current.items():
        result = probe(namespace, pod, service, "/snapshot")
        assert result["count"] == len(data["bindings"])
        assert result["sha256"] == hashlib.sha256(raw).hexdigest()
        assert result["selected"] == registry.select(first["workspaceId"]).to_dict()
        samples.append(
            {
                "consumer": service,
                "bindings": result["count"],
                "parseSeconds": result["parseSeconds"],
                "parsePeakBytes": result["parsePeakBytes"],
            }
        )
        assert probe(namespace, pod, service, "/resolve", identity) == {
            "resolved": True
        }
        authority = probe(namespace, pod, service, "/authority")
        assert authority[:4] == [200, 200, 200, 200]
        assert authority[4] == 403
    evidence(instrumented_consumer_parse_samples=samples)
    return current


def storage_budget(namespace: str, raw: bytes) -> list[int]:
    """Measure decoded Secret data, retaining Helm's inner base64 release value in the count."""
    secrets = json.loads(
        kube(namespace, "get", "secret", "-l", "owner=helm,name=custos", "-o", "json")
    )["items"]
    sizes = [
        sum(len(base64.b64decode(value)) for value in row["data"].values())
        for row in secrets
    ]
    assert sizes and max(sizes) <= 1_048_576
    first = json.loads(raw)["bindings"][0]["workspaceId"]
    assert first not in command(["helm", "get", "manifest", "custos", "-n", namespace])
    assert first not in command(["helm", "get", "values", "custos", "-n", namespace])
    return sizes


def restart_container(namespace: str, pod: dict[str, Any], container: str) -> None:
    """Stop one exact CRI container in the disposable node; retain and verify its original Pod UID."""
    status = json.loads(
        kube(namespace, "get", "pod", pod["metadata"]["name"], "-o", "json")
    )
    statuses = [
        *status["status"].get("containerStatuses", []),
        *status["status"].get("initContainerStatuses", []),
    ]
    row = next(item for item in statuses if item["name"] == container)
    command(
        [
            "docker",
            "exec",
            status["spec"]["nodeName"],
            "crictl",
            "stop",
            row["containerID"].split("://")[1],
        ]
    )
    for _ in range(90):
        updated = json.loads(
            kube(namespace, "get", "pod", pod["metadata"]["name"], "-o", "json")
        )
        assert updated["metadata"]["uid"] == pod["metadata"]["uid"]
        statuses = [
            *updated["status"].get("containerStatuses", []),
            *updated["status"].get("initContainerStatuses", []),
        ]
        item = next(value for value in statuses if value["name"] == container)
        if item["restartCount"] > row["restartCount"] and item.get("ready"):
            return
        time.sleep(1)
    pytest.fail("original Pod container failed to restart")


def await_release_retirement(namespace: str) -> None:
    """Wait for actual release Pods/controllers to retire without shortening Dapr shutdown."""
    for _ in range(120):
        inventory = json.loads(
            kube(
                namespace,
                "get",
                "pods,deployments,replicasets,statefulsets,daemonsets,jobs,cronjobs",
                "-l",
                "app.kubernetes.io/instance=custos",
                "-o",
                "json",
            )
        )
        assert isinstance(inventory, dict) and isinstance(inventory.get("items"), list)
        if not inventory["items"]:
            return
        time.sleep(5)
    pytest.fail("retained release workloads did not retire before cleanup")


@pytest.fixture(scope="module")
def packaged_chart(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Refuse any cluster except the explicitly named hosted kind and save the exact chart archive."""
    cluster = os.environ["BINDING_CLUSTER"]
    assert (
        command(["kubectl", "config", "current-context"]).strip() == f"kind-{cluster}"
    )
    assert cluster in command(["kind", "get", "clusters"]).splitlines()
    assert command(["helm", "version", "--short"]).startswith("v3.16.2")
    directory = tmp_path_factory.mktemp("binding-chart")
    command(["helm", "dependency", "build", str(CHART)])
    command(["helm", "package", str(CHART), "--destination", str(directory)])
    return next(directory.glob("custos-*.tgz"))


@pytest.mark.parametrize("count", [51, 200, 1000, 1024])
def test_instrumented_file_loader(count: int, tmp_path: Path) -> None:
    """Record real bounded-file parse duration and peak memory without inventing an SLO."""
    raw = fixture(count, maximum=True)
    path = tmp_path / "bindings.json"
    path.write_bytes(raw)
    tracemalloc.start()
    started = time.perf_counter()
    loaded = operator.ExecutionBindings.from_file(
        str(path), hashlib.sha256(raw).hexdigest()
    )
    elapsed = time.perf_counter() - started
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert len(loaded.bindings) == count
    evidence(
        **{
            f"loader_{count}": {
                "registryBytes": len(raw),
                "instrumentedParseSeconds": elapsed,
                "instrumentedPeakBytes": peak,
            }
        }
    )


def test_actual_maximum_storage_and_file_rollback(
    packaged_chart: Path, tmp_path: Path
) -> None:
    """Install 1000, upgrade 1024, restart pinned Pods, rollback both reused/recreated snapshots."""
    namespace = "binding-files"
    config = settings(namespace)
    namespace_uid = foundation(namespace, config, tmp_path)
    raw = fixture(1000, maximum=True)
    next_raw = fixture(1024, maximum=True)
    bundles = [
        operator.prepare(value, config, "custos", namespace)
        for value in (raw, next_raw)
    ]
    retained_uids = {}
    for value, bundle in zip((raw, next_raw), bundles, strict=True):
        if bundle == bundles[0]:
            with pytest.raises(ValueError, match="missing"):
                operator.provision(bundle, create=False)
        execution_secrets(namespace, value)
        operator.provision(bundle, create=True)
        operator.provision(bundle, create=False)
        retained_uids.update(object_uids(bundle["objects"]))
    assert len(retained_uids) == sum(len(bundle["objects"]) for bundle in bundles)
    files = [
        save_values(tmp_path / f"file-{index}.yaml", file_config(config, bundle))
        for index, bundle in enumerate(bundles)
    ]
    helm(namespace, "install", packaged_chart, files[0])
    original = assert_consumers(namespace, raw)
    sizes = storage_budget(namespace, raw)
    wrong = file_config(config, bundles[0])
    wrong["global"]["receiverRuntime"]["executionBindingsBundle"] = copy.deepcopy(
        bundles[0]["descriptor"]
    )
    wrong["global"]["receiverRuntime"]["executionBindingsBundle"]["sha256"] = "0" * 64
    wrong_file = save_values(tmp_path / "wrong-digest.yaml", wrong)
    helm(
        namespace, "upgrade", packaged_chart, wrong_file, succeeds=False, timeout="30s"
    )
    for service in operator.CONSUMERS:
        items = json.loads(
            kube(
                namespace,
                "get",
                "pods",
                "-l",
                f"app.kubernetes.io/name={service}",
                "-o",
                "json",
            )
        )["items"]
        rejected = next(
            row
            for row in items
            if row["metadata"]["annotations"].get("checksum/execution-bindings")
            == "0" * 64
        )
        logs = kube(
            namespace, "logs", rejected["metadata"]["name"], "-c", service, "--tail=20"
        )
        assert "execution binding file SHA-256 mismatch" in logs
        assert json.loads(raw)["bindings"][0]["workspaceId"] not in logs
    command(
        [
            "helm",
            "rollback",
            "custos",
            "1",
            "-n",
            namespace,
            "--wait",
            "--timeout",
            "5m",
        ]
    )
    original = assert_consumers(namespace, raw)
    for service in operator.CONSUMERS:
        kube(namespace, "rollout", "pause", f"deployment/custos-{service}")
    command(
        [
            "helm",
            "upgrade",
            "custos",
            str(packaged_chart),
            "-n",
            namespace,
            "--reset-values",
            "-f",
            str(files[1]),
        ]
    )
    for service, pod in original.items():
        restart_container(namespace, pod, service)
        assert (
            probe(namespace, pod, service, "/snapshot")["sha256"]
            == hashlib.sha256(raw).hexdigest()
        )
    command(["helm", "rollback", "custos", "1", "-n", namespace])
    for service in operator.CONSUMERS:
        kube(namespace, "rollout", "resume", f"deployment/custos-{service}")
    reused = assert_consumers(namespace, raw)
    assert all(
        reused[key]["metadata"]["uid"] == value["metadata"]["uid"]
        for key, value in original.items()
    )
    helm(namespace, "upgrade", packaged_chart, files[1])
    assert_consumers(namespace, next_raw)
    sizes.extend(storage_budget(namespace, next_raw))
    changed = json.loads(next_raw)
    original_workspace = changed["bindings"][1000]["workspaceId"]
    changed["defaults"].pop(original_workspace)
    changed["bindings"][1000]["workspaceId"] = changed["bindings"][0]["workspaceId"]
    changed["defaults"][changed["bindings"][0]["workspaceId"]] = changed["bindings"][
        1000
    ]["bindingId"]
    changed_raw = operator.canonical(changed)
    changed_bundle = operator.prepare(changed_raw, config, "custos", namespace)
    operator.provision(changed_bundle, create=True)
    bundles.append(changed_bundle)
    retained_uids.update(object_uids(changed_bundle["objects"]))
    changed_file = save_values(
        tmp_path / "changed-default.yaml", file_config(config, changed_bundle)
    )
    helm(namespace, "upgrade", packaged_chart, changed_file)
    assert_consumers(namespace, changed_raw)
    command(
        [
            "helm",
            "rollback",
            "custos",
            "1",
            "-n",
            namespace,
            "--wait",
            "--timeout",
            "5m",
        ]
    )
    recreated = assert_consumers(namespace, raw)
    assert all(
        recreated[key]["metadata"]["uid"] != value["metadata"]["uid"]
        for key, value in original.items()
    )
    cm = bundles[0]["objects"][0]
    kube(
        namespace,
        "patch",
        "configmap",
        cm["metadata"]["name"],
        "--type=merge",
        "-p",
        '{"data":{"bindings.json":"tampered"}}',
        succeeds=False,
    )
    with pytest.raises(ValueError, match="retained"):
        operator.cleanup_resources(bundles[1], confirmed_no_retained_work=True)
    command(
        ["helm", "uninstall", "custos", "-n", namespace, "--wait", "--timeout", "5m"]
    )
    await_release_retirement(namespace)
    assert (
        json.loads(command(["kubectl", "get", "namespace", namespace, "-o", "json"]))[
            "metadata"
        ]["uid"]
        == namespace_uid
    )
    standalone = copy.deepcopy(
        next(row for row in bundles[0]["objects"] if row["kind"] == "RoleBinding")
    )
    standalone["metadata"] = {
        "name": "retained-execution-reader",
        "namespace": namespace,
    }
    command(["kubectl", "create", "-f", "-"], standalone)
    with pytest.raises(ValueError, match="RoleBinding"):
        operator.cleanup_resources(bundles[0], confirmed_no_retained_work=True)
    kube(namespace, "delete", "rolebinding", standalone["metadata"]["name"])
    for bundle in bundles:
        operator.provision(bundle, create=False)
        assert object_uids(bundle["objects"]) == {
            key: value
            for key, value in retained_uids.items()
            if key
            in {f"{row['kind']}/{row['metadata']['name']}" for row in bundle["objects"]}
        }
        operator.cleanup_resources(bundle, confirmed_no_retained_work=True)
    evidence(
        maximum_registry_bytes=[len(raw), len(next_raw)],
        helm_secret_data_bytes=sizes,
        file_rollback_reused_and_recreated=True,
        operator_namespace_uninstall_survived=True,
        standalone_rolebinding_retained_role=True,
        changed_default_kept_original_identity=True,
    )


def test_actual_inline_bridge_timeout_recovery(
    packaged_chart: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify chart-owned Namespace handoff and recover updated-but-timed-out first cutover."""
    namespace = "binding-bridge"
    config = settings(namespace, chart_owned=True)
    namespace_uid = foundation(namespace, config, tmp_path)
    raw = operator.canonical(config["global"]["receiverRuntime"]["executionBindings"])
    execution_secrets(namespace, raw)
    bundle = operator.prepare(raw, config, "custos", namespace)
    legacy = save_values(tmp_path / "legacy.yaml", config)
    helm(namespace, "install", packaged_chart, legacy)
    with pytest.raises(ValueError):
        operator.provision(bundle, create=True)
    config["global"]["receiverRuntime"]["retainLegacyExecutionResources"] = True
    bridge = save_values(tmp_path / "bridge.yaml", config)
    helm(namespace, "upgrade", packaged_chart, bridge)
    inventory = operator.record_bridge(packaged_chart, [bridge], "custos", namespace)
    assert len(inventory["resources"]) == 13
    unverified = copy.deepcopy(config)
    unverified["auth-service"]["image"]["tag"] = "intentionally-unverified"
    unverified_bridge = save_values(tmp_path / "unverified-bridge.yaml", unverified)
    with pytest.raises(ValueError, match="full rendering"):
        operator.record_bridge(packaged_chart, [unverified_bridge], "custos", namespace)
    with pytest.raises(ValueError, match="atomic"):
        operator.transition_preflight(bundle, inventory, atomic=True)
    with pytest.raises(ValueError, match="native inline"):
        operator.transition_preflight(
            bundle, inventory, rollback_revision=inventory["revision"]
        )
    operator.provision(bundle, create=True, inventory=inventory)
    bundle_uids = object_uids(bundle["objects"])
    original = assert_consumers(namespace, raw)
    target = file_config(config, bundle)
    for service in SERVICES:
        target[service]["image"]["tag"] = "intentionally-missing"
    failed = save_values(tmp_path / "timeout.yaml", target)
    helm(namespace, "upgrade", packaged_chart, failed, succeeds=False, timeout="30s")
    for service, pod in original.items():
        live = json.loads(
            kube(namespace, "get", "deployment", f"custos-{service}", "-o", "json")
        )
        assert any(
            row["name"] == "CUSTOS_EXECUTION_BINDINGS_FILE"
            for row in live["spec"]["template"]["spec"]["containers"][0]["env"]
        )
        restart_container(namespace, pod, service)
        restart_container(namespace, pod, "daprd")
        assert probe(namespace, pod, service, "/authority")[:4] == [200, 200, 200, 200]
    bridge_docs = operator.rendered_bridge(
        packaged_chart, [bridge], "custos", namespace
    )
    deployment = json.loads(
        kube(namespace, "get", "deployment", "custos-api-gateway", "-o", "json")
    )
    target_doc = next(
        row
        for row in bridge_docs
        if row["kind"] == "Deployment"
        and row["metadata"]["name"] == "custos-api-gateway"
    )
    stale_patch = operator.reconcile_source(deployment, target_doc)
    kube(
        namespace,
        "annotate",
        "deployment",
        "custos-api-gateway",
        "binding-concurrent=preserved",
    )
    kube(
        namespace,
        "patch",
        "deployment",
        "custos-api-gateway",
        "--type=json",
        "-p",
        json.dumps(stale_patch),
        succeeds=False,
    )
    original_run = operator.run

    def fail_second_patch(args: list[str], payload: Any = None) -> str:
        """Inject a bounded tool failure after the first real reconciliation to test safe retry."""
        if args[:4] == ["kubectl", "patch", "deployment", "custos-trigger-service"]:
            raise ValueError("injected partial reconciliation failure")
        result = original_run(args, payload)
        assert isinstance(result, str)
        return result

    with monkeypatch.context() as patch:
        patch.setattr(operator, "run", fail_second_patch)
        with pytest.raises(ValueError, match="partial"):
            operator.recover_bridge(inventory, packaged_chart, [bridge])
    gateway = json.loads(
        kube(namespace, "get", "deployment", "custos-api-gateway", "-o", "json")
    )
    trigger = json.loads(
        kube(namespace, "get", "deployment", "custos-trigger-service", "-o", "json")
    )
    assert not operator.source_fields(gateway)["volumes"]
    assert operator.source_fields(trigger)["volumes"]
    operator.recover_bridge(inventory, packaged_chart, [bridge])
    assert_consumers(namespace, raw)
    for service in operator.CONSUMERS:
        live = json.loads(
            kube(namespace, "get", "deployment", f"custos-{service}", "-o", "json")
        )
        fields = operator.source_fields(live)
        assert not fields["mounts"] and not fields["volumes"]
        assert all(row["name"] not in operator.FILE_ENVS for row in fields["env"])
        assert {"name": "BINDING_UNRELATED", "value": "preserved"} in live["spec"][
            "template"
        ]["spec"]["containers"][0]["env"]
    valid = save_values(tmp_path / "remigrate.yaml", file_config(config, bundle))
    operator.provision(bundle, create=False, inventory=inventory)
    helm(namespace, "upgrade", packaged_chart, valid)
    assert_consumers(namespace, raw)
    kube(
        namespace,
        "annotate",
        "namespace",
        namespace,
        "meta.helm.sh/release-namespace=another-namespace",
        "--overwrite",
    )
    try:
        with pytest.raises(ValueError, match="release namespace"):
            operator.transition_preflight(bundle)
    finally:
        kube(
            namespace,
            "annotate",
            "namespace",
            namespace,
            f"meta.helm.sh/release-namespace={namespace}",
            "--overwrite",
        )
    command(
        ["helm", "uninstall", "custos", "-n", namespace, "--wait", "--timeout", "5m"]
    )
    await_release_retirement(namespace)
    for record in inventory["resources"]:
        live = operator.get_resource(record["resource"])
        assert live is not None and live["metadata"]["uid"] == record["uid"]
    assert (
        json.loads(command(["kubectl", "get", "namespace", namespace, "-o", "json"]))[
            "metadata"
        ]["uid"]
        == namespace_uid
    )
    assert object_uids(bundle["objects"]) == bundle_uids
    operator.cleanup_resources(bundle, confirmed_no_retained_work=True)
    operator.cleanup_resources(inventory, confirmed_no_retained_work=True)
    evidence(
        bridge_namespace_uninstall_survived=True,
        legacy_application_and_sidecar_restarts=True,
        timed_out_first_cutover_recovered_and_remigrated=True,
        concurrent_patch_refused_and_partial_recovery_retried=True,
        full_recovery_render_mismatch_refused=True,
        foreign_helm_release_namespace_refused=True,
    )
