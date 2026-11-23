"""Regress the smoke-only runtime activation and pre-admission identity bootstrap."""

import asyncio
import io
import json
import runpy
import ssl
import subprocess
import sys
import urllib.error
import urllib.request
from email.message import Message
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
smoke = SimpleNamespace(**runpy.run_path(str(ROOT / "tests/helm/smoke/runtime.py")))
public = SimpleNamespace(
    **runpy.run_path(str(ROOT / "tests/helm/smoke/public_workflow.py"))
)
TAG = "a" * 40
SIDECAR = "localhost:5001/smoke-sidecar@sha256:" + "b" * 64
HELPER = "localhost:5001/smoke-hook-helper@sha256:" + "c" * 64


@pytest.fixture(autouse=True)
def smoke_cli_import_path(monkeypatch: pytest.MonkeyPatch) -> None:
    """Give runpy-loaded scripts the sibling imports supplied by normal CLI execution."""
    monkeypatch.syspath_prepend(str(ROOT / "tests/helm/smoke"))


def managed_render(*, public_execution: bool = False) -> list[dict[str, Any]]:
    """Render the actual profile with the exact overlay used by hosted smoke setup."""
    overlay = smoke.settings(TAG, "custos-system", ["10.96.0.1"], SIDECAR, HELPER)
    if public_execution:
        public.configure_public_execution(overlay)
    result = subprocess.run(
        [
            "helm",
            "template",
            "custos",
            str(ROOT / "deploy/helm/custos"),
            "-n",
            "custos-system",
            "-f",
            str(ROOT / "deploy/helm/custos/values-connected-eval.yaml"),
            "-f",
            "-",
        ],
        input=yaml.safe_dump(overlay),
        text=True,
        capture_output=True,
        check=True,
    )
    return [row for row in yaml.safe_load_all(result.stdout) if row]


def test_smoke_activates_actual_scheduler_with_only_secret_references() -> None:
    """The failed default-off/S3 projection becomes real CSI/identity/TLS runtime wiring."""
    documents = managed_render()
    arm = next(
        row
        for row in documents
        if row["kind"] == "Deployment"
        and row["metadata"]["name"] == "custos-activity-runtime-manager"
    )
    pod = arm["spec"]["template"]["spec"]
    container = pod["containers"][0]
    env = {entry["name"]: entry for entry in container["env"]}
    assert env["ARM_RUNTIME_ENABLED"]["value"] == "true"
    assert env["ARM_ARTIFACT_STORE"]["value"] == "csi"
    assert env["ARM_SANDBOX_NAMESPACE"]["value"] == "custos-system-activities"
    assert (
        env["ARM_METADATA_STORE"]["valueFrom"]["secretKeyRef"]["name"] == "custos-app"
    )
    assert env["APP_API_TOKEN"]["valueFrom"]["secretKeyRef"]["name"].startswith(
        "smoke-"
    )
    assert container["readinessProbe"]["httpGet"] == {
        "path": "/readyz",
        "port": "https",
        "scheme": "HTTPS",
    }
    assert any(
        row.get("persistentVolumeClaim", {}).get("claimName") == "smoke-artifacts"
        for row in pod["volumes"]
    )
    refs = json.loads(env["CUSTOS_EXECUTION_BINDINGS"]["value"])
    assert refs["bindings"][0]["secretRef"] == smoke.EXECUTION_SECRET
    trigger = next(
        row
        for row in documents
        if row["kind"] == "Deployment"
        and row["metadata"]["name"] == "custos-trigger-service"
    )
    trigger_env = {
        row["name"]: row
        for row in trigger["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    assert trigger_env["TRIGGER_RECEIVER_WORKERS_ENABLED"]["value"] == "false"
    workflow = next(
        row
        for row in documents
        if row["kind"] == "Deployment"
        and row["metadata"]["name"] == "custos-workflow-service"
    )
    workflow_env = {
        row["name"]: row
        for row in workflow["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    assert workflow_env["WF_GATEWAY_NAMESPACE"]["value"] == "custos-system"
    assert workflow_env["WF_AUTHZ_ENDPOINT"]["value"].endswith("/custos-auth/method")
    assert workflow_env["WF_REQUIRE_CALL_CONTEXT"]["value"] == "1"
    assert workflow_env["WF_PUBLIC_EXECUTION_ENABLED"]["value"] == "false"
    assert all(not row.get("stringData") for row in documents)


def test_managed_smoke_client_has_only_dns_and_public_gateway_access() -> None:
    """The actual Helm client escapes deny-all for its public checks, not backend/mesh access."""
    documents = managed_render()
    pod = next(
        row
        for row in documents
        if row["kind"] == "Pod" and row["metadata"]["name"] == "custos-smoke-test"
    )
    policies = {
        row["metadata"]["name"]: row
        for row in documents
        if row["kind"] == "NetworkPolicy"
    }
    egress = policies["custos-smoke-test-egress"]["spec"]
    ingress = policies["custos-smoke-test-gateway"]["spec"]
    assert all(
        pod["metadata"]["labels"][key] == value
        for key, value in egress["podSelector"]["matchLabels"].items()
    )
    assert egress["policyTypes"] == ["Egress"]
    assert len(egress["egress"]) == 2
    dns, gateway = egress["egress"]
    assert dns["to"] == [
        {
            "namespaceSelector": {
                "matchLabels": {
                    "kubernetes.io/metadata.name": "kube-system",
                }
            }
        }
    ]
    assert dns["ports"] == [
        {"protocol": "UDP", "port": 53},
        {"protocol": "TCP", "port": 53},
    ]
    assert gateway["to"] == [{"podSelector": ingress["podSelector"]}]
    assert gateway["ports"] == [{"protocol": "TCP", "port": 8080}]
    assert ingress["policyTypes"] == ["Ingress"]
    assert ingress["ingress"] == [
        {
            "from": [{"podSelector": egress["podSelector"]}],
            "ports": [{"protocol": "TCP", "port": 8080}],
        }
    ]
    assert "annotations" not in policies["custos-smoke-test-egress"]["metadata"]


@pytest.mark.parametrize(
    "tag,sidecar,addresses",
    [
        ("dev", SIDECAR, ["10.96.0.1"]),
        (TAG, "sidecar:mutable", ["10.96.0.1"]),
        (TAG, SIDECAR, []),
        (TAG, SIDECAR, ["0.0.0.0/0"]),
    ],
)
def test_smoke_refuses_unpinned_or_unscoped_inputs(
    tag: str,
    sidecar: str,
    addresses: list[str],
) -> None:
    """Mutable builds and unspecified API scope cannot silently produce a smoke configuration."""
    with pytest.raises(ValueError):
        smoke.settings(tag, "custos-system", addresses, sidecar, HELPER)


def test_preseed_uses_real_bootstrap_image_and_secret_projection() -> None:
    """Pre-admission seeding preserves production database/permissions inputs and hook behavior."""
    documents = managed_render()
    original = next(
        row
        for row in documents
        if row["kind"] == "Job" and row["metadata"]["name"] == "custos-bootstrap"
    )
    job = smoke.seed_job(documents, TAG)
    container = job["spec"]["template"]["spec"]["containers"][0]
    assert (
        container["image"]
        == original["spec"]["template"]["spec"]["containers"][0]["image"]
    )
    assert container["command"] == ["python", "/smoke/runtime.py", "seed"]
    assert container["envFrom"] == [{"secretRef": {"name": "custos-app"}}]
    assert next(
        row for row in container["env"] if row["name"] == "SMOKE_EXECUTION_TOKEN"
    ) == {
        "name": "SMOKE_EXECUTION_TOKEN",
        "valueFrom": {"secretKeyRef": {"name": smoke.EXECUTION_SECRET, "key": "token"}},
    }
    assert "annotations" not in job["metadata"]
    assert (
        original["metadata"]["annotations"]["helm.sh/hook"]
        == "post-install,post-upgrade"
    )
    assert job["spec"]["activeDeadlineSeconds"] == 180


def test_prepare_migrates_before_identity_and_keeps_secrets_out_of_values(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Pre-admission jobs run in dependency order; only Secret references reach the overlay."""
    documents = managed_render()
    calls: list[tuple[list[str], Any]] = []

    def run(args: list[str], payload: Any = None) -> str:
        """Stub cluster I/O and certificate files, leaving actual render/domain data intact."""
        calls.append((args, payload))
        if args[:4] == ["kubectl", "get", "service", "kubernetes"]:
            return json.dumps({"spec": {"clusterIP": "10.96.0.1"}})
        if args[:4] == ["kubectl", "get", "endpoints", "kubernetes"]:
            return json.dumps({"subsets": [{"addresses": [{"ip": "172.18.0.2"}]}]})
        if args[0] == "openssl":
            Path(args[args.index("-out") + 1]).write_text("synthetic certificate")
            Path(args[args.index("-keyout") + 1]).write_text("synthetic private key")
        if args[:2] == ["helm", "template"]:
            return yaml.safe_dump_all(documents)
        return ""

    monkeypatch.setitem(smoke.prepare.__globals__, "command", run)
    for key, value in {
        "NS": "custos-system",
        "GITHUB_SHA": TAG,
        "SMOKE_SIDECAR_IMAGE": SIDECAR,
        "SMOKE_HOOK_HELPER_IMAGE": HELPER,
        "CHART": "unused-chart",
        "VALUES": "unused-profile",
    }.items():
        monkeypatch.setenv(key, value)
    output = tmp_path / "values.yaml"
    smoke.prepare(output)
    jobs = [
        payload
        for _, payload in calls
        if isinstance(payload, dict) and payload.get("kind") == "Job"
    ]
    assert [job["metadata"]["name"] for job in jobs] == [
        "smoke-schema-migrate",
        "smoke-execution-bootstrap",
    ]
    migration_wait = next(
        index
        for index, (args, _) in enumerate(calls)
        if "job/smoke-schema-migrate" in args
    )
    identity_create = next(
        index for index, (_, payload) in enumerate(calls) if payload is jobs[1]
    )
    assert migration_wait < identity_create
    for _, payload in calls:
        if isinstance(payload, dict) and payload.get("kind") == "Secret":
            assert "stringData" in payload
            assert all(
                value not in output.read_text()
                for value in payload["stringData"].values()
            )


def test_command_failure_does_not_echo_private_process_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Secret-bearing stdout/stderr cannot be included in the raised operator error."""
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args[0],
            1,
            stdout="sensitive fixture output",
            stderr="sensitive fixture error",
        ),
    )
    with pytest.raises(
        RuntimeError, match="smoke prerequisite command failed"
    ) as result:
        smoke.command(
            ["kubectl", "create", "-f", "-"], {"stringData": {"token": "private"}}
        )
    assert "sensitive" not in str(result.value)
    assert "private" not in str(result.value)


def test_readiness_failure_retains_probe_cause_and_ready_arm_logs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Keep a real child probe's HTTP failure and both log streams, never private content."""
    from contextlib import redirect_stdout

    monkeypatch.setenv("NS", "custos-system")
    monkeypatch.setenv("GITHUB_SHA", TAG)
    monkeypatch.setenv("ARM_RUNTIME_ENABLED", "true")
    monkeypatch.setenv("ARM_ARTIFACT_STORE", "csi")
    monkeypatch.setattr(ssl, "create_default_context", Mock())
    body = io.BytesIO(b"syntheticPrivateBody")
    calls: list[list[str]] = []

    def reject(*args: Any, **kwargs: Any) -> None:
        """Simulate a real HTTP rejection with private URL, headers and body."""
        raise urllib.error.HTTPError(
            "https://private/?token=syntheticPrivateURL",
            503,
            "syntheticPrivateMessage",
            Message(),
            body,
        )

    def run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        """Execute the actual serialized probe; supply logs without a Pod-readiness filter."""
        calls.append(args)
        if "exec" in args:
            stream = io.StringIO()
            program = tmp_path / "readiness_probe.py"
            program.write_text(args[-1])
            with redirect_stdout(stream), pytest.raises(SystemExit) as exited:
                runpy.run_path(str(program))
            code = exited.value.code
            assert isinstance(code, int)
            return subprocess.CompletedProcess(
                args, code, stream.getvalue(), "syntheticPrivateStderr"
            )
        assert "logs" in args and "activity-runtime-manager" in args
        assert "--tail=200" in args and "--limit-bytes=32768" in args
        return subprocess.CompletedProcess(
            args,
            0,
            'File "/private/resources.py", line 221, in probe_dependencies\n'
            "syntheticPrivateMessage",
            "",
        )

    monkeypatch.setattr(urllib.request, "urlopen", reject)
    monkeypatch.setattr(subprocess, "run", run)
    output = tmp_path / "runtime-evidence.json"
    with pytest.raises(RuntimeError):
        smoke.verify(output)
    report = json.loads((tmp_path / "runtime-readiness-diagnostics.json").read_text())
    assert report["sha"] == TAG and report["readinessProofFailed"] is True
    assert report["probe"]["category"] == "http_status"
    assert report["probe"]["httpStatus"] == 503
    assert report["probe"]["errorType"] == "HTTPError"
    assert report["probe"]["frames"]
    assert report["collectionErrors"] == []
    assert set(report["serviceLogs"]) == {"arm-current", "arm-previous"}
    assert all(
        row["frames"]
        == [{"file": "resources.py", "line": 221, "function": "probe_dependencies"}]
        for row in report["serviceLogs"].values()
    )
    assert len(calls) == 3 and "--previous" in calls[-1]
    assert body.closed
    assert not output.exists()
    retained = json.dumps(report) + capsys.readouterr().out
    assert "syntheticPrivate" not in retained and "/private/" not in retained


@pytest.mark.parametrize(
    "error,category",
    [
        (
            subprocess.TimeoutExpired(["private"], 30, output="syntheticPrivate"),
            "exec_timeout",
        ),
        (FileNotFoundError("syntheticPrivate"), "exec_unavailable"),
        (None, "exec_exit"),
    ],
)
def test_readiness_exec_failures_survive_secondary_collection_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    error: Exception | None,
    category: str,
) -> None:
    """Retain the primary exec failure when current/previous log collection also fails."""
    monkeypatch.setenv("NS", "custos-system")
    monkeypatch.setenv("GITHUB_SHA", TAG)

    def run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        """Reject exec separately from the independently failing log commands."""
        if "exec" in args:
            if error is not None:
                raise error
            return subprocess.CompletedProcess(args, 1, "", "syntheticPrivate")
        raise OSError("syntheticPrivate")

    monkeypatch.setattr(subprocess, "run", run)
    with pytest.raises((RuntimeError, OSError, subprocess.TimeoutExpired)) as raised:
        smoke.verify(tmp_path / "runtime-evidence.json")
    if error is not None:
        assert raised.value is error
    text = (tmp_path / "runtime-readiness-diagnostics.json").read_text()
    report = json.loads(text)
    assert report["failure"]["category"] == category
    assert report["probe"] is None
    assert [row["source"] for row in report["collectionErrors"]] == [
        "arm-current",
        "arm-previous",
    ]
    assert "syntheticPrivate" not in text


@pytest.mark.parametrize(
    "kind,category,error_type",
    [
        ("tls", "tls", "SSLCertVerificationError"),
        ("timeout", "timeout", "TimeoutError"),
        ("ca-file", "tls", "FileNotFoundError"),
        ("configuration", "configuration", "AssertionError"),
        ("json", "invalid_response", "JSONDecodeError"),
        ("status", "invalid_response", "AssertionError"),
        ("oversized", "invalid_response", "ValueError"),
    ],
)
def test_readiness_probe_categorizes_failures_without_response_or_exception_text(
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
    category: str,
    error_type: str,
) -> None:
    """Distinguish trusted-probe failures and cap response reads without retaining private data."""
    monkeypatch.setenv(
        "ARM_RUNTIME_ENABLED", "true" if kind != "configuration" else "false"
    )
    monkeypatch.setenv("ARM_ARTIFACT_STORE", "csi")
    tls = Mock()
    if kind == "ca-file":
        tls.side_effect = FileNotFoundError("syntheticPrivateCA")
    monkeypatch.setattr(ssl, "create_default_context", tls)
    response = MagicMock(status=200)
    response.__enter__.return_value = response
    response.read.return_value = {
        "json": b"syntheticPrivateBody",
        "status": b'{"status":"syntheticPrivateStatus"}',
        "oversized": b"x" * 4097,
    }.get(kind, b'{"status":"ready"}')
    urlopen = Mock(return_value=response)
    if kind in {"tls", "timeout"}:
        root = (
            ssl.SSLCertVerificationError("syntheticPrivateCertificate")
            if kind == "tls"
            else TimeoutError("syntheticPrivateTimeout")
        )
        urlopen.side_effect = urllib.error.URLError(root)
    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    report = smoke.readiness_probe()
    assert report["ready"] is False
    assert report["category"] == category and report["errorType"] == error_type
    assert report["frames"] and len(report["frames"]) <= 5
    if kind in {"json", "status", "oversized"}:
        response.read.assert_called_once_with(4097)
        response.__exit__.assert_called_once()
    assert "syntheticPrivate" not in json.dumps(report)


def test_successful_readiness_keeps_original_evidence_and_skips_failure_collection(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A trusted ready response emits only the existing success artifact and executes once."""
    monkeypatch.setenv("NS", "custos-system")
    monkeypatch.setenv("GITHUB_SHA", TAG)
    monkeypatch.setenv("ARM_RUNTIME_ENABLED", "true")
    monkeypatch.setenv("ARM_ARTIFACT_STORE", "csi")
    monkeypatch.setattr(ssl, "create_default_context", Mock())
    response = MagicMock(status=200)
    response.__enter__.return_value = response
    body = b'{"status":"ready","private":"syntheticPrivate"}'
    response.read.return_value = body + b" " * (4096 - len(body))
    monkeypatch.setattr(urllib.request, "urlopen", Mock(return_value=response))
    probe = smoke.readiness_probe()
    assert probe["ready"] is True
    run = Mock(
        return_value=subprocess.CompletedProcess(
            [], 0, "RUNTIME_READINESS_PROBE " + json.dumps(probe), ""
        )
    )
    monkeypatch.setattr(subprocess, "run", run)
    output = tmp_path / "runtime-evidence.json"
    smoke.verify(output)
    report = json.loads(output.read_text())
    assert report["actualSchedulerReady"] is True and report["sha"] == TAG
    assert run.call_count == 1
    assert "syntheticPrivate" not in output.read_text()
    assert not (tmp_path / "runtime-readiness-diagnostics.json").exists()


@pytest.mark.parametrize(
    "change",
    [
        {"httpStatus": True},
        {"phase": ["syntheticPrivate"]},
        {"errorType": "syntheticPrivate"},
        {"frames": [{"file": "/private/source.py", "function": "probe", "line": 1}]},
        {"private": "syntheticPrivate" * 700},
    ],
)
def test_malformed_readiness_record_is_an_explicit_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    change: dict[str, Any],
) -> None:
    """Reject malformed producer records without retaining their unsafe fields or claiming success."""
    monkeypatch.setenv("NS", "custos-system")
    monkeypatch.setenv("GITHUB_SHA", TAG)
    row = {
        "ready": False,
        "phase": "request",
        "category": "http_status",
        "errorType": "HTTPError",
        "httpStatus": 503,
        "frames": [],
    } | change
    run = Mock(
        return_value=subprocess.CompletedProcess(
            [], 0, "RUNTIME_READINESS_PROBE " + json.dumps(row), ""
        )
    )
    monkeypatch.setattr(subprocess, "run", run)
    with pytest.raises(ValueError):
        smoke.verify(tmp_path / "runtime-evidence.json")
    text = (tmp_path / "runtime-readiness-diagnostics.json").read_text()
    report = json.loads(text)
    assert report["failure"]["category"] == "invalid_probe_record"
    assert report["probe"] is None
    assert "syntheticPrivate" not in text and "/private/" not in text
    assert not (tmp_path / "runtime-evidence.json").exists()


def test_readiness_artifact_write_failure_preserves_the_original_probe_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Keep the original error and safe stdout evidence when the artifact cannot be written."""
    monkeypatch.setenv("NS", "custos-system")
    monkeypatch.setenv("GITHUB_SHA", TAG)
    error = subprocess.TimeoutExpired(["private"], 30, stderr="syntheticPrivate")
    monkeypatch.setattr(subprocess, "run", Mock(side_effect=error))
    with pytest.raises(subprocess.TimeoutExpired) as raised:
        smoke.verify(tmp_path / "absent" / "runtime-evidence.json")
    assert raised.value is error
    text = capsys.readouterr().out
    report = json.loads(text)
    assert report["failure"]["category"] == "exec_timeout"
    assert report["collectionErrors"][-1] == {
        "source": "artifact",
        "errorType": "OSError",
    }
    assert "syntheticPrivate" not in text


@pytest.mark.parametrize(
    ("invalid", "count"),
    [
        (None, 1),
        (None, 51),
        ({"component": ["syntheticPrivate"]}, 1),
        ({"httpStatus": True}, 1),
        (
            {
                "frames": [
                    {"file": "/private/source.py", "function": "probe", "line": 1}
                ]
            },
            1,
        ),
        ({"message": "syntheticPrivate" * 400}, 1),
    ],
)
def test_ready_arm_component_evidence_is_bounded_and_collection_errors_are_explicit(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    invalid: dict[str, Any] | None,
    count: int,
) -> None:
    """Collect components from both Ready-ARM streams, excluding private fields and bad records."""
    monkeypatch.setenv("NS", "custos-system")
    monkeypatch.setenv("GITHUB_SHA", TAG)
    record = {
        "probeId": "a" * 32,
        "phase": "readiness",
        "component": "artifacts",
        "reason": "error",
        "errorType": "BackendUnavailable",
        "httpStatus": None,
        "frames": [
            {"file": "resources.py", "function": "probe_artifacts", "line": 125}
        ],
        "message": "syntheticPrivateMessage",
        "url": "https://private/?token=syntheticPrivate",
    }

    def run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        """Return primary exec failure, then bounded current/previous structured logs."""
        if "exec" in args:
            return subprocess.CompletedProcess(args, 1, "", "syntheticPrivate")
        row = {**record, "phase": "startup"} if "--previous" in args else record
        if invalid is not None:
            row = {**row, **invalid}
        logs = "\n".join(
            "WARNING ARM_READINESS_FAILURE "
            + json.dumps(row if count == 1 else {**row, "probeId": f"{index:032x}"})
            for index in range(count)
        )
        return subprocess.CompletedProcess(args, 0, logs)

    monkeypatch.setattr(subprocess, "run", run)
    with pytest.raises(RuntimeError):
        smoke.verify(tmp_path / "runtime-evidence.json")
    text = (tmp_path / "runtime-readiness-diagnostics.json").read_text()
    report = json.loads(text)
    assert report["failure"] == {"category": "exec_exit"}
    assert "syntheticPrivate" not in text and "https://" not in text
    if invalid is not None:
        assert [row["source"] for row in report["collectionErrors"]] == [
            "arm-current",
            "arm-previous",
        ]
    else:
        assert report["collectionErrors"] == []
        evidence = report["serviceLogs"]["arm-current"]
        rows = evidence["componentFailures"]
        assert len(rows) == min(count, 50)
        assert evidence["componentFailuresTruncated"] is (count > 50)
        current = rows[0]
        previous = report["serviceLogs"]["arm-previous"]["componentFailures"][0]
        assert current["component"] == "artifacts" and current["phase"] == "readiness"
        assert previous["phase"] == "startup"
        assert current["frames"] == record["frames"]
        assert current["probeId"] == (
            record["probeId"] if count == 1 else f"{count - 50:032x}"
        )


def test_preseed_grants_only_workspace_execution_and_closes_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The real adapter boundary receives a scoped executor, not a platform-admin shortcut."""
    monkeypatch.syspath_prepend(str(ROOT / "src/jobs/bootstrap/src"))
    import custos_bootstrap.__main__ as bootstrap
    from custos_auth.roles import ROLE_WORKSPACE_EXECUTOR
    from custos_auth.tokens import mint_token
    from custos_pg import PgAuthAdapter
    from custos_pg.pool import LazyPool
    from custos_spl.interfaces.auth_store import WorkspaceScope

    seed = AsyncMock()
    grant, principal, credential, close = (
        AsyncMock(),
        AsyncMock(),
        AsyncMock(),
        AsyncMock(),
    )
    monkeypatch.setattr(bootstrap, "seed_platform", seed)
    monkeypatch.setattr(PgAuthAdapter, "put_role_binding", grant)
    monkeypatch.setattr(PgAuthAdapter, "put_principal", principal)
    monkeypatch.setattr(PgAuthAdapter, "put_service_token", credential)
    monkeypatch.setattr(LazyPool, "aclose", close)
    pool_factory = Mock(wraps=LazyPool)
    monkeypatch.setattr("custos_pg.pool.LazyPool", pool_factory)
    token, hashed = mint_token()
    monkeypatch.setenv("SMOKE_EXECUTION_TOKEN", token)
    monkeypatch.setenv("CUSTOS_PG_DSN", "postgresql://unused/synthetic")
    asyncio.run(smoke.seed_execution())
    binding = grant.call_args.args[0]
    assert binding.role_id == ROLE_WORKSPACE_EXECUTOR
    assert binding.scope == WorkspaceScope(smoke.WORKSPACE)
    assert principal.call_args.args[0].workspace_id == smoke.WORKSPACE
    assert binding.bound_by == principal.call_args.args[0].principal_id
    assert credential.call_args.args[0].hash == hashed
    assert token not in repr(credential.call_args.args[0])
    assert seed.call_args.kwargs["admin_oidc_issuer"] is None
    close.assert_awaited_once()
    connection = SimpleNamespace(set_type_codec=AsyncMock())
    asyncio.run(pool_factory.call_args.kwargs["init"](connection))
    codec = connection.set_type_codec.call_args
    assert codec.args == ("jsonb",)
    scope = {"type": "workspace", "workspace_id": smoke.WORKSPACE}
    encoded = codec.kwargs["encoder"](scope)
    assert isinstance(encoded, str)
    assert codec.kwargs["decoder"](encoded) == scope


def test_real_gateway_minted_metadata_reaches_real_workflow_list_handler() -> None:
    """Keep actual minter/signer/verifier defaults compatible without legacy identity headers."""
    import base64

    from custos_auth.callctx_signer import (
        CallContextSigner,
        SigningKey,
        StaticSigningKeyResolver,
    )
    from custos_callctx import CallContextVerifier, JwksCache
    from custos_gateway.clients.auth import (
        CallctxSignRequest,
        CallctxSignResponse,
        FakeAuthServiceClient,
    )
    from custos_gateway.middleware.auth import AuthorizedCaller
    from custos_gateway.middleware.callctx_mint import mint_call_context
    from custos_workflow import create_app
    from custos_workflow.providers import load_run_components
    from custos_workflow.runtime import FakeWorkflowRuntime
    from fastapi import FastAPI, Request
    from fastapi.testclient import TestClient

    key = SigningKey.generate()
    signer = CallContextSigner(StaticSigningKeyResolver(key))

    class SigningClient(FakeAuthServiceClient):
        """Replace only the remote signing RPC with its actual domain signer."""

        async def callctx_sign(
            self, request: CallctxSignRequest
        ) -> CallctxSignResponse:
            """Honor the Gateway's exact authorized sign request and signer defaults."""
            result = await signer.sign(
                principal_id=request.principal_id,
                workspace_id=request.workspace_id,
                caller_component=request.caller_component,
                permissions=request.permissions,
            )
            return CallctxSignResponse(
                token=result.token,
                kid=result.kid,
                jti=result.jti,
                iat=result.iat,
                exp=result.exp,
            )

    gateway = FastAPI()
    gateway.state.auth_client = SigningClient()
    outbound = Request(
        {"type": "http", "method": "GET", "path": "/", "headers": [], "app": gateway}
    )
    outbound.state.auth = AuthorizedCaller(
        principal_id="operator",
        audit_event_id="synthetic",
        permission="run:read",
        workspace_id="ws",
    )
    outbound.state.correlation_id = "synthetic-correlation"
    minted = asyncio.run(mint_call_context(outbound))
    assert set(minted.metadata) == {"x-custos-callctx", "x-correlation-id"}

    async def fetch(url: str) -> tuple[dict[str, str], dict[str, Any]]:
        """Expose the real signing key's public portion to the shared JWKS verifier."""
        encoded = (
            base64.urlsafe_b64encode(key.public_key.public_bytes_raw())
            .decode()
            .rstrip("=")
        )
        return {}, {
            "keys": [{"kty": "OKP", "crv": "Ed25519", "kid": key.kid, "x": encoded}]
        }

    app = create_app(
        require_call_context=True,
        run_components=load_run_components(
            env={}, workflow_runtime=FakeWorkflowRuntime()
        ),
        call_context_verifier=CallContextVerifier(
            jwks_cache=JwksCache(
                jwks_url="http://auth/.well-known/jwks.json", fetcher=fetch
            ),
        ),
        app_api_token="synthetic-app-token",
        gateway_namespace="custos",
    )
    with TestClient(app) as client:
        response = client.get(
            "/v1/workspaces/ws/runs",
            headers={
                **minted.metadata,
                "dapr-api-token": "synthetic-app-token",
                "dapr-caller-app-id": "api-gateway",
                "dapr-caller-namespace": "custos",
            },
        )
    assert response.status_code == 200
    assert response.json() == {"items": [], "nextCursor": None}


def test_public_execution_opt_in_keeps_background_default_and_separate_secret_scope() -> (
    None
):
    """The actual rendered public opt-in adds its caller pin without sharing its client token."""
    documents = managed_render(public_execution=True)
    workflow = next(
        row
        for row in documents
        if row["kind"] == "Deployment"
        and row["metadata"]["name"] == "custos-workflow-service"
    )
    env = {
        row["name"]: row
        for row in workflow["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    assert env["WF_PUBLIC_EXECUTION_ENABLED"]["value"] == "true"
    bindings = json.loads(env["CUSTOS_EXECUTION_BINDINGS"]["value"])
    assert bindings["defaults"] == {smoke.WORKSPACE: "smoke-v1"}
    caller = next(
        row for row in bindings["bindings"] if row["principalId"] == public.OPERATOR
    )
    assert caller["secretRef"] == public.EXECUTION_RESOURCE
    configuration = next(
        row
        for row in documents
        if row["kind"] == "Configuration"
        and row["metadata"]["name"] == "custos-receivers-workflow-service"
    )
    scopes = configuration["spec"]["secrets"]["scopes"]
    secrets = next(row for row in scopes if row["storeName"] == "kubernetes")
    assert public.EXECUTION_RESOURCE in secrets["allowedSecrets"]
    assert public.RESOURCE not in secrets["allowedSecrets"]


def test_public_preparation_preserves_dsn_and_separates_credentials(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Seed before activation without dropping DSN settings or sharing execution credentials."""
    documents = managed_render(public_execution=True)
    overlay = tmp_path / "values.yaml"
    overlay.write_text(
        yaml.safe_dump(
            smoke.settings(TAG, "custos-system", ["10.96.0.1"], SIDECAR, HELPER)
        )
    )
    wait = Mock()
    monkeypatch.setenv("NS", "custos-system")
    monkeypatch.setenv("GITHUB_SHA", TAG)
    monkeypatch.setitem(
        sys.modules, "runtime", SimpleNamespace(command=Mock(), seed_job=smoke.seed_job)
    )
    monkeypatch.setitem(public.prepare.__globals__, "rendered", lambda path: documents)
    monkeypatch.setitem(public.prepare.__globals__, "wait_job", wait)
    public.prepare(overlay)
    seed = wait.call_args.args[0]
    container = seed["spec"]["template"]["spec"]["containers"][0]
    original = smoke.seed_job(documents, TAG)["spec"]["template"]["spec"]["containers"][
        0
    ]
    for row in original["env"]:
        if row["name"] != "SMOKE_EXECUTION_TOKEN":
            assert row in container["env"]
    env = {row["name"]: row for row in container["env"]}
    assert (
        env["CUSTOS_TEST_TOKEN"]["valueFrom"]["secretKeyRef"]["name"] == public.RESOURCE
    )
    assert (
        env["CUSTOS_TEST_EXECUTION_TOKEN"]["valueFrom"]["secretKeyRef"]["name"]
        == public.EXECUTION_RESOURCE
    )
    assert "SMOKE_EXECUTION_TOKEN" not in env
    assert yaml.safe_load(overlay.read_text())["global"]["receiverRuntime"][
        "publicExecutionEnabled"
    ]


@pytest.mark.parametrize("run_id", [None, "cancelled-root"])
def test_public_workflow_client_inherits_only_public_policy_identity(
    monkeypatch: pytest.MonkeyPatch,
    run_id: str | None,
) -> None:
    """The lifecycle probe has no seed/mesh authority despite using a production image."""
    monkeypatch.setenv("GITHUB_SHA", TAG)
    original = next(
        row
        for row in managed_render()
        if row["kind"] == "Pod" and row["metadata"]["name"] == "custos-smoke-test"
    )
    job = public.client_job(original, "public-client:" + TAG, run_id=run_id)
    pod = job["spec"]["template"]
    assert set(pod) == {"metadata", "spec"}
    assert pod["metadata"] == {"labels": original["metadata"]["labels"]}
    spec = pod["spec"]
    assert spec["automountServiceAccountToken"] is False
    assert "serviceAccountName" not in spec
    container = spec["containers"][0]
    env = {row["name"]: row for row in container["env"]}
    assert set(env) == {"GITHUB_SHA", "PYTHONDONTWRITEBYTECODE", "CUSTOS_TEST_TOKEN"}
    assert "envFrom" not in container
    assert container["securityContext"]["readOnlyRootFilesystem"] is True
    assert container["resources"]["limits"]


def test_public_start_acceptance_does_not_count_as_runtime_completion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An accepted start that never completes cannot produce a successful proof."""
    calls = 0

    def request(method: str, path: str, body: object = None) -> dict[str, str]:
        """Return HTTP-shaped acceptance and pending state without real completion."""
        nonlocal calls
        calls += 1
        return (
            {"runId": "074171de-bc84-5ea4-b636-1135477620e1"}
            if method == "POST"
            else {"status": "pending"}
        )

    monkeypatch.setitem(public.exercise.__globals__, "request", request)
    times = iter((0.0, 121.0))
    monkeypatch.setitem(
        public.exercise.__globals__,
        "time",
        SimpleNamespace(monotonic=lambda: next(times)),
    )
    with pytest.raises(RuntimeError, match="did not complete"):
        public.exercise()
    assert calls == 2


@pytest.mark.parametrize(
    "field,value",
    [
        ("sha", "stale"),
        ("publicWorkflowCompleted", False),
        ("publicReadbackVerified", False),
        ("fullReceiverCertification", True),
        ("runId", None),
        ("cancellationRunId", None),
    ],
)
def test_public_lifecycle_evidence_rejects_stale_or_incomplete_reports(
    field: str,
    value: object,
) -> None:
    """Job success cannot hide stale commit attribution or missing actual effects."""
    report = {
        "sha": TAG,
        "publicWorkflowCompleted": True,
        "publicReadbackVerified": True,
        "fullReceiverCertification": False,
        "runId": "synthetic",
        "cancellationRunId": "synthetic-waiting",
    }
    report[field] = value
    with pytest.raises(ValueError, match="stale or incomplete"):
        public.report_from_logs(public.REPORT_PREFIX + json.dumps(report), TAG)


@pytest.mark.parametrize("status", ["cancelled", "cancelling"])
def test_private_probe_uses_real_execution_client_and_requires_terminal_cancellation(
    monkeypatch: pytest.MonkeyPatch, status: str
) -> None:
    """Keep actual credential exchange and reject in-progress cancellation without forging peers."""
    import time

    import httpx

    trigger = next(
        row
        for row in managed_render(public_execution=True)
        if row["kind"] == "Deployment"
        and row["metadata"]["name"] == "custos-trigger-service"
    )
    for row in trigger["spec"]["template"]["spec"]["containers"][0]["env"]:
        if "value" in row:
            monkeypatch.setenv(row["name"], row["value"])
    monkeypatch.setenv("DAPR_API_TOKEN", "synthetic-sidecar-api-token")
    calls: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        """Double only local Dapr I/O; no caller app/namespace headers may originate in the client."""
        calls.append(request)
        assert request.url.host == "127.0.0.1"
        assert request.headers["dapr-api-token"] == "synthetic-sidecar-api-token"
        assert "dapr-caller-app-id" not in request.headers
        assert "dapr-caller-namespace" not in request.headers
        if request.method == "GET":
            assert public.EXECUTION_RESOURCE in request.url.path
            return httpx.Response(200, json={"token": "synthetic-server-credential"})
        if request.url.path.endswith("/rpc/callctx.exchangeExecution"):
            assert json.loads(request.content) == {
                "token": "synthetic-server-credential",
                "workspaceId": public.WORKSPACE,
                "principalId": public.OPERATOR,
                "operation": "workflow-cancel",
            }
            now = int(time.time())
            return httpx.Response(
                200,
                json={"token": "e30.e30.test", "iat": now, "exp": now + 300},
            )
        assert request.url.path.endswith("/internal/runs/root:cancel")
        return httpx.Response(
            202,
            json={"runId": "root", "workspaceId": public.WORKSPACE, "status": status},
        )

    original = httpx.AsyncClient

    def client(**kwargs: Any) -> httpx.AsyncClient:
        """Retain the production client's ownership/timeouts while replacing only its sockets."""
        return original(transport=httpx.MockTransport(respond), **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", client)
    if status == "cancelling":
        with pytest.raises(ValueError, match="terminal run"):
            asyncio.run(public.private_cancellation("root", TAG))
    else:
        result = asyncio.run(public.private_cancellation("root", TAG))
        assert result["privateCancellationVerified"] is True
    assert len(calls) == 3


def test_failed_public_job_reports_only_payload_free_rejection_locations(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Failed candidate state and ready-service logs are retained without payloads or success evidence."""
    monkeypatch.setenv("NS", "custos-system")
    monkeypatch.setenv("GITHUB_SHA", TAG)
    monkeypatch.setenv("IMAGE_PREFIX", "localhost:5001/custos")
    monkeypatch.setitem(
        public.run.__globals__,
        "rendered",
        lambda _: [{"kind": "Pod", "metadata": {"name": "custos-smoke-test"}}],
    )
    monkeypatch.setitem(public.run.__globals__, "client_job", lambda *args: {})

    def wait(job: dict[str, Any]) -> None:
        """Fail at the same orchestration boundary as an unsuccessful initial public Job."""
        raise RuntimeError("public Job failed")

    diagnostic = (
        "PUBLIC_START_REJECTED run=00000000-0000-5000-8000-000000000000 "
        "cause=OutboundRpcStatusError origin=reject:123 status=503"
    )
    run_id = "00000000-0000-5000-8000-000000000000"
    terminal = {
        "sha": TAG,
        "runId": run_id,
        "status": "failed",
        "token": "private-candidate-payload",
    }
    causal = {
        "runId": run_id,
        "stepId": "await-cancellation",
        "operation": "register",
        "phase": "activity",
        "errorType": "OutboundRpcStatusError",
        "kind": "workflow.client.status",
        "httpStatus": 403,
        "frames": [{"file": "trigger.py", "function": "register", "line": 10}],
        "message": "private-message",
    }
    inspected: list[list[str]] = []

    def command(args: list[str]) -> str:
        """Expose ready-service and client logs with private data that must not leave the collector."""
        inspected.append(args)
        if "job/" + public.RESOURCE in args:
            return str(public.DIAGNOSTIC_PREFIX) + json.dumps(terminal)
        return (
            "unrelated-private-value\nWARNING "
            + diagnostic
            + '\n  File "/app/runner.py", line 42, in execute\n'
            + "TypeError: private-tail"
            + "\nWORKFLOW_RESUME_FAILURE "
            + json.dumps(causal)
        )

    def probe(service: str, phase: str, candidate: str, sha: str) -> dict[str, Any]:
        """Return the sanitized runtime diagnostic from the exact Workflow container."""
        assert (service, phase, candidate, sha) == (
            "workflow-service",
            "diagnose",
            run_id,
            TAG,
        )
        return {"sha": TAG, "runId": run_id, "runtimeStatus": "FAILED"}

    monkeypatch.setitem(public.run.__globals__, "wait_job", wait)
    monkeypatch.setitem(public.run.__globals__, "service_probe", probe)
    monkeypatch.setitem(sys.modules, "runtime", SimpleNamespace(command=command))
    output = tmp_path / "evidence.json"
    with pytest.raises(RuntimeError, match="public Job failed"):
        public.run(output, tmp_path / "values.yaml")
    captured = capsys.readouterr().out
    report = json.loads(captured)
    assert report["publicStartRejections"] == [diagnostic]
    assert report["terminalRuns"][0]["status"] == "failed"
    assert report["terminalRuns"][0]["runtime"]["runtimeStatus"] == "FAILED"
    assert set(report["serviceLogs"]) == {
        "api-gateway",
        "api-gateway-daprd",
        "auth-service",
        "auth-service-daprd",
        "workflow-service",
        "daprd",
    }
    assert report["serviceLogs"]["workflow-service"]["frames"] == [
        {"file": "runner.py", "line": 42, "function": "execute"}
    ]
    assert len(inspected) == 8
    assert report["causalFailures"][0]["operation"] == "register"
    assert report["causalFailures"][0]["httpStatus"] == 403
    assert "private" not in captured
    assert (
        json.loads((tmp_path / "public-workflow-diagnostics.json").read_text())
        == report
    )
    assert not output.exists()


def test_terminal_candidate_logs_only_identity_and_status(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """An early terminal observation emits a correlation record, never the public response body."""
    monkeypatch.setenv("GITHUB_SHA", TAG)
    run_id = "00000000-0000-5000-8000-000000000000"
    responses = iter(
        [
            {"runId": run_id},
            {"status": "failed", "inputs": {"token": "private-input"}},
        ]
    )
    monkeypatch.setitem(
        public.start_waiting_run.__globals__, "request", lambda *args: next(responses)
    )
    with pytest.raises(RuntimeError, match="terminated before"):
        public.start_waiting_run()
    captured = capsys.readouterr().out
    assert (
        captured
        == public.CANDIDATE_PREFIX
        + json.dumps({"sha": TAG, "runId": run_id})
        + "\n"
        + public.DIAGNOSTIC_PREFIX
        + json.dumps({"sha": TAG, "runId": run_id, "status": "failed"})
        + "\n"
    )
    assert "private-input" not in captured


@pytest.mark.parametrize(
    "phase",
    [
        "runtime-wait",
        "private-cancellation",
        "runtime-state",
        "public-readback",
        "snapshot-change",
    ],
)
def test_later_proof_failure_retains_nonterminal_candidate(
    phase: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Every later proof failure retains candidate metadata, never terminal or success fabrication."""
    monkeypatch.setenv("NS", "custos-system")
    monkeypatch.setenv("GITHUB_SHA", TAG)
    monkeypatch.setenv("IMAGE_PREFIX", "localhost:5001/custos")
    run_id = "00000000-0000-5000-8000-000000000000"
    initial = {
        "sha": TAG,
        "runId": "completed",
        "cancellationRunId": run_id,
        "publicWorkflowCompleted": True,
        "publicReadbackVerified": True,
        "fullReceiverCertification": False,
    }
    error = ValueError("private-proof-detail")
    monkeypatch.setitem(
        public.run.__globals__,
        "rendered",
        lambda _: [{"kind": "Pod", "metadata": {"name": "custos-smoke-test"}}],
    )
    monkeypatch.setitem(
        public.run.__globals__, "client_job", lambda *args, **kwargs: kwargs
    )

    def wait(job: dict[str, Any]) -> None:
        """Fail only the final readback Job when that phase is under test."""
        if phase == "public-readback" and job.get("run_id"):
            raise error

    def probe(
        service: str, probe_phase: str, candidate: str, sha: str
    ) -> dict[str, Any]:
        """Separate real proof failure from the collector's metadata-only runtime observation."""
        assert candidate == run_id
        if probe_phase == "diagnose":
            return {"sha": sha, "runId": candidate, "runtimeStatus": "RUNNING"}
        if probe_phase == ("cancel" if phase == "private-cancellation" else phase):
            raise error
        if probe_phase == "runtime-wait":
            return {"liveWaitVerified": True, "acceptedInputHash": "a" * 64}
        return {
            "privateCancellationVerified": True,
            "runtimeTerminationVerified": True,
            "acceptedInputHash": ("b" if phase == "snapshot-change" else "a") * 64,
        }

    monkeypatch.setitem(public.run.__globals__, "wait_job", wait)
    monkeypatch.setitem(public.run.__globals__, "service_probe", probe)
    monkeypatch.setitem(
        sys.modules,
        "runtime",
        SimpleNamespace(
            command=lambda args: (
                public.REPORT_PREFIX + json.dumps(initial)
                if "job/" + public.RESOURCE in args
                else ""
            )
        ),
    )
    output = tmp_path / "evidence.json"
    with pytest.raises(ValueError) as raised:
        public.run(output, tmp_path / "values.yaml")
    if phase == "snapshot-change":
        assert "changed the accepted wait snapshot" in str(raised.value)
    else:
        assert raised.value is error
    report = json.loads((tmp_path / "public-workflow-diagnostics.json").read_text())
    assert report["failedPhase"] == (
        "runtime-state" if phase == "snapshot-change" else phase
    )
    assert report["candidateRuns"] == [
        {
            "runId": run_id,
            "status": None,
            "runtime": {"sha": TAG, "runId": run_id, "runtimeStatus": "RUNNING"},
        }
    ]
    assert report["terminalRuns"] == []
    assert not output.exists()
    assert "private-proof-detail" not in json.dumps(report)


@pytest.mark.parametrize("ttl_seconds", [300, 299])
def test_live_wait_snapshot_requires_registered_exact_ttl_and_preserves_pin(
    ttl_seconds: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Only an exact live caller-bound mirror permits proof; output is a digest, not runtime input."""
    import hashlib
    from datetime import UTC, datetime, timedelta

    from custos_callctx.execution import ExecutionIdentity
    from custos_spl.interfaces.delivery import DeliveryStateProvider
    from custos_workflow.providers import MetadataStorePool
    from custos_workflow.runs import RunInput
    from custos_workflow.steps.resume.durable import DurableResumeMirrorRepository
    from custos_workflow.steps.resume.mirror import ResumeSubscriptionMirror

    monkeypatch.setenv("WF_METADATA_STORE", "postgresql://localhost/synthetic")
    now = datetime(2026, 11, 21, tzinfo=UTC)
    run_id = "00000000-0000-5000-8000-000000000000"
    identity = ExecutionIdentity(public.WORKSPACE, public.OPERATOR, "public-context-v1")
    accepted = RunInput(
        workspace_id=public.WORKSPACE,
        workflow_version_id=f"{public.WORKSPACE}/{public.WAITING_WORKFLOW}@1",
        compiled_graph_json="{}",
        inputs={},
        delivery_protocol=3,
        execution_identity=identity,
    )
    encoded = json.dumps(accepted.to_dict())
    provider = Mock(spec=DeliveryStateProvider)
    provider.get_workflow_start = AsyncMock(
        return_value=SimpleNamespace(input_json=encoded)
    )
    provider.delivery_now = AsyncMock(return_value=now)
    opened = AsyncMock(return_value=provider)
    closed = AsyncMock()
    monkeypatch.setattr(MetadataStorePool, "open", opened)
    monkeypatch.setattr(MetadataStorePool, "aclose", closed)
    mirror = ResumeSubscriptionMirror(
        mirror_id="synthetic-mirror",
        run_id=run_id,
        step_id="await-cancellation",
        event_key="never-sent",
        ts_subscription_id="registered",
        registered_at=now,
        expires_at=now + timedelta(seconds=ttl_seconds),
        workspace_id=public.WORKSPACE,
        delivery_protocol=2,
        execution_identity=identity,
    )
    monkeypatch.setattr(
        DurableResumeMirrorRepository,
        "list_open_for_step",
        AsyncMock(return_value=(mirror,)),
    )
    if ttl_seconds == 300:
        digest = asyncio.run(
            public.accepted_wait_snapshot(run_id, require_live_mirror=True)
        )
        assert digest == hashlib.sha256(encoded.encode()).hexdigest()
        assert identity.principal_id not in digest
    else:
        with pytest.raises(ValueError, match="bounded caller wait"):
            asyncio.run(public.accepted_wait_snapshot(run_id, require_live_mirror=True))
    opened.assert_awaited_once()
    closed.assert_awaited_once()


def test_collector_failure_does_not_replace_original_proof_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Explicit collector failure still propagates the original proof exception and never success."""
    monkeypatch.setenv("NS", "custos-system")
    monkeypatch.setenv("GITHUB_SHA", TAG)
    monkeypatch.setitem(sys.modules, "runtime", SimpleNamespace())
    monkeypatch.setitem(
        public.run.__globals__,
        "rendered",
        Mock(side_effect=ValueError("private-proof")),
    )
    monkeypatch.setitem(
        public.run.__globals__,
        "collect_failure_diagnostics",
        Mock(side_effect=RuntimeError("private-collector")),
    )
    with pytest.raises(ValueError) as raised:
        public.run(tmp_path / "evidence.json", tmp_path / "values.yaml")
    assert str(raised.value) == "private-proof"
    report = json.loads(capsys.readouterr().out)
    assert report["collectionErrors"] == [
        {"source": "collector", "errorType": "RuntimeError"}
    ]
    assert report["lifecycleProofFailed"] is True
    assert "private" not in json.dumps(report)
    assert not (tmp_path / "evidence.json").exists()


@pytest.mark.parametrize(
    "error_type,expected_type",
    [("TypeError", "TypeError"), ("syntheticStructuredError", "unrecognized")],
)
def test_runtime_diagnostic_excludes_payloads_and_failure_messages(
    monkeypatch: pytest.MonkeyPatch,
    error_type: str,
    expected_type: str,
) -> None:
    """Fetch only metadata and keep stack locations/type, excluding exception values and raw paths."""
    from dapr.ext import workflow

    client = Mock(spec=workflow.DaprWorkflowClient)
    client.get_workflow_state.return_value = SimpleNamespace(
        runtime_status=SimpleNamespace(name="FAILED"),
        failure_details=SimpleNamespace(
            error_type=error_type,
            message="credential=syntheticCredentialError",
            stack_trace=(
                '  File "/private/worker.py", line 123, in run\n'
                "TypeError: payload=syntheticPayloadException"
            ),
        ),
        serialized_input="private-input",
        serialized_output="private-output",
    )
    monkeypatch.setattr(workflow, "DaprWorkflowClient", lambda: client)
    result = asyncio.run(public.runtime_diagnostics("root", TAG))
    client.get_workflow_state.assert_called_once_with("root", fetch_payloads=False)
    client.close.assert_called_once_with()
    assert result["runtimeStatus"] == "FAILED"
    assert result["failureType"] == expected_type
    assert result["historyCapability"] == "unsupported-client-api"
    assert result["failureLocations"]["frames"] == [
        {"file": "worker.py", "line": 123, "function": "run"}
    ]
    assert "exceptionTypes" not in result["failureLocations"]
    assert "syntheticCredentialError" not in json.dumps(result)
    assert "syntheticPayloadException" not in json.dumps(result)
    assert "syntheticStructuredError" not in json.dumps(result)
    assert "private" not in json.dumps(result)


def test_service_log_diagnostics_excludes_error_suffixed_private_values(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Ready-pod collection never derives exception names from credential or payload text."""
    monkeypatch.setenv("NS", "custos-system")
    logs = (
        "credential=syntheticCredentialError payload=syntheticPayloadException\n"
        '  File "/app/worker.py", line 12, in execute\n'
        "PUBLIC_START_REJECTED run=00000000-0000-5000-8000-000000000000 "
        "cause=syntheticCredentialError origin=execute:12 status=503\n"
    )
    monkeypatch.setitem(sys.modules, "runtime", SimpleNamespace(command=lambda _: logs))
    output = tmp_path / "evidence.json"
    public.collect_failure_diagnostics(output, TAG)
    report = json.loads((tmp_path / "public-workflow-diagnostics.json").read_text())
    assert report["collectionErrors"] == []
    assert report["publicStartRejections"] == []
    assert report["serviceLogs"]["workflow-service"]["frames"] == [
        {"file": "worker.py", "line": 12, "function": "execute"}
    ]
    assert "exceptionTypes" not in json.dumps(report)
    assert "syntheticCredentialError" not in json.dumps(report)
    assert "syntheticPayloadException" not in json.dumps(report)
    assert not output.exists()


def test_first_public_503_retains_request_correlated_gateway_cause(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A503 before acceptance is correlated without a run ID and preserves the original failure."""
    import urllib.error
    import urllib.request

    monkeypatch.setenv("NS", "custos-system")
    monkeypatch.setenv("GITHUB_SHA", TAG)
    monkeypatch.setenv("CUSTOS_TEST_TOKEN", "syntheticTokenError")
    outbound: list[Any] = []

    def reject(request: urllib.request.Request, *, timeout: int) -> Any:
        """Expose a real HTTP error at the public transport seam, not a synthesized run result."""
        outbound.append(request)
        raise urllib.error.HTTPError(
            request.full_url,
            503,
            "privateRemoteException",
            Message(),
            io.BytesIO(
                b'{"code":"downstream-unavailable","detail":"syntheticPayloadException"}'
            ),
        )

    monkeypatch.setattr(urllib.request, "urlopen", reject)
    with pytest.raises(RuntimeError, match="public POST: HTTP 503"):
        public.request("POST", "/v1/workspaces/workspace-default/runs", {"inputs": {}})
    client_logs = capsys.readouterr().out
    request_rows = [
        json.loads(line.removeprefix(public.REQUEST_PREFIX))
        for line in client_logs.splitlines()
    ]
    correlation = request_rows[0]["correlationId"]
    assert outbound[0].get_header("X-correlation-id") == correlation
    assert request_rows[-1]["httpStatus"] == 503
    causal = {
        "correlationId": correlation,
        "phase": "dapr-route",
        "kind": "status",
        "httpStatus": 500,
        "message": "syntheticCredentialError",
    }
    inspected = []

    def command(args: list[str]) -> str:
        """Return correlated Ready-Gateway records and unrelated ready-service errors."""
        inspected.append(args)
        if "job/" + public.RESOURCE in args:
            return client_logs
        if (
            "deployment/custos-api-gateway" in args
            and args[args.index("-c") + 1] == "api-gateway"
        ):
            return "WARNING GATEWAY_DEPENDENCY_FAILURE " + json.dumps(causal)
        return "unrelated syntheticCredentialError"

    monkeypatch.setitem(sys.modules, "runtime", SimpleNamespace(command=command))
    output = tmp_path / "evidence.json"
    public.collect_failure_diagnostics(output, TAG)
    report = json.loads((tmp_path / "public-workflow-diagnostics.json").read_text())
    assert report["collectionErrors"] == []
    assert report["candidateRuns"] == []
    assert report["publicRequests"] == [
        {"correlationId": correlation, "method": "POST", "httpStatus": 503}
    ]
    assert len(report["dependencyFailures"]) == 1
    assert report["dependencyFailures"][0]["source"] == "api-gateway"
    assert report["dependencyFailures"][0]["phase"] == "dapr-route"
    assert report["dependencyFailures"][0]["httpStatus"] == 500
    assert any("deployment/custos-auth-service" in args for args in inspected)
    assert "synthetic" not in json.dumps(report)
    assert "synthetic" not in client_logs
    assert not output.exists()


def test_dependency_records_reject_invalid_fields_and_ignore_other_requests() -> None:
    """Only the selected request's closed taxonomy and source fields can reach the artifact."""
    correlation = "00000000-0000-5000-8000-000000000000"
    valid = {
        "correlationId": correlation,
        "phase": "auth-verify",
        "kind": "transport",
        "httpStatus": None,
        "token": "syntheticCredentialError",
    }
    unrelated = {**valid, "correlationId": "unrelated"}
    prefix = "GATEWAY_DEPENDENCY_FAILURE "
    captured = public.dependency_failure_records(
        prefix + json.dumps(unrelated) + "\n" + prefix + json.dumps(valid),
        {correlation},
        "api-gateway",
    )
    assert len(captured) == 1
    assert captured[0]["kind"] == "transport"
    assert "synthetic" not in json.dumps(captured)
    workflow = public.dependency_failure_records(
        "WORKFLOW_PUBLIC_START_FAILURE "
        + json.dumps(
            {
                "correlationId": correlation,
                "phase": "durable-start",
                "errorType": "OSError",
                "runId": correlation,
                "frames": [
                    {
                        "file": "runtime.py",
                        "function": "schedule",
                        "line": 10,
                        "token": "private",
                    }
                ],
            }
        ),
        {correlation},
        "workflow-service",
    )
    assert workflow[0]["attemptedRunId"] == correlation
    assert "private" not in json.dumps(workflow)
    for field, value in (("kind", "syntheticCredentialError"), ("httpStatus", True)):
        with pytest.raises(ValueError, match="invalid taxonomy"):
            public.dependency_failure_records(
                prefix + json.dumps({**valid, field: value}),
                {correlation},
                "api-gateway",
            )


def test_prehandler_jwks_failure_is_retained_from_ready_service_without_a_run(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Collect a real producer's pre-handler cause without a run, ignoring private extra fields."""
    import httpx
    from custos_workflow._public_diagnostics import log_public_start_failure

    monkeypatch.setenv("NS", "custos-system")
    correlation = "00000000-0000-5000-8000-000000000000"
    url = "http://private/?credential=syntheticCredentialError"
    with pytest.raises(httpx.HTTPStatusError) as raised:
        httpx.Response(
            500,
            request=httpx.Request("GET", url),
            text="syntheticPayloadException",
        ).raise_for_status()
    log_public_start_failure(
        None,
        correlation,
        "jwks",
        raised.value,
        reason="jwks_unavailable",
        response_status=503,
    )
    assert "private" not in caplog.text and "synthetic" not in caplog.text
    record = json.loads(
        caplog.records[-1].getMessage().split("WORKFLOW_PUBLIC_START_FAILURE ", 1)[1]
    )
    record.update(
        message="syntheticPayloadException",
        url=url,
        kid="syntheticCredentialError",
    )

    def command(args: list[str]) -> str:
        """Supply Ready-Workflow and client logs without accepting any private extra fields."""
        if "job/" + public.RESOURCE in args:
            prefix = public.REQUEST_PREFIX
            assert isinstance(prefix, str)
            return prefix + json.dumps(
                {
                    "sha": TAG,
                    "correlationId": correlation,
                    "method": "POST",
                    "httpStatus": 503,
                }
            )
        if "deployment/custos-workflow-service" in args and "workflow-service" in args:
            return "\n".join(
                "WARNING WORKFLOW_PUBLIC_START_FAILURE " + json.dumps(row)
                for row in ({**record, "correlationId": "unrelated"}, record)
            )
        return ""

    monkeypatch.setitem(sys.modules, "runtime", SimpleNamespace(command=command))
    public.collect_failure_diagnostics(tmp_path / "evidence.json", TAG)
    report = json.loads((tmp_path / "public-workflow-diagnostics.json").read_text())
    assert report["collectionErrors"] == [] and report["candidateRuns"] == []
    assert len(report["dependencyFailures"]) == 1
    retained = report["dependencyFailures"][0]
    assert (
        retained["source"] == "workflow-service" and retained["attemptedRunId"] is None
    )
    assert retained["correlationId"] == correlation
    assert retained["reason"] == "jwks_unavailable"
    assert retained["responseStatus"] == 503 and retained["httpStatus"] == 500
    assert retained["frames"] == record["frames"]
    assert "synthetic" not in json.dumps(report) and "private" not in json.dumps(report)
    assert not (tmp_path / "evidence.json").exists()


@pytest.mark.parametrize(
    "field,value",
    [
        ("reason", "syntheticCredentialError"),
        ("reason", None),
        ("responseStatus", True),
        ("responseStatus", 200),
        ("runId", "00000000-0000-5000-8000-000000000000"),
    ],
)
def test_auth_diagnostics_reject_malformed_reason_status_or_premature_run(
    field: str, value: Any
) -> None:
    """Fail collection explicitly instead of retaining an invalid pre-handler causal record."""
    correlation = "00000000-0000-5000-8000-000000000000"
    record = {
        "correlationId": correlation,
        "phase": "jwks",
        "reason": "jwks_unavailable",
        "responseStatus": 503,
        "errorType": "HTTPStatusError",
        "httpStatus": 500,
        field: value,
    }
    with pytest.raises(ValueError, match="invalid"):
        public.dependency_failure_records(
            "WORKFLOW_PUBLIC_START_FAILURE " + json.dumps(record),
            {correlation},
            "workflow-service",
        )


def test_resume_causal_records_are_correlated_and_allowlisted() -> None:
    """Retain only the candidate's typed status/source record, not extra remote/private fields."""
    run_id = "00000000-0000-5000-8000-000000000000"
    record = {
        "runId": run_id,
        "stepId": "await-cancellation",
        "operation": "register",
        "phase": "activity",
        "attempt": None,
        "errorType": "OutboundRpcStatusError",
        "kind": "workflow.client.status",
        "httpStatus": 403,
        "grpcStatus": None,
        "frames": [
            {
                "file": "trigger.py",
                "function": "register",
                "line": 10,
                "token": "private-frame",
            }
        ],
        "message": "private-error",
        "code": "private-code",
    }
    unrelated = {**record, "runId": "unrelated"}
    logs = "\n".join(
        "WARNING WORKFLOW_RESUME_FAILURE " + json.dumps(row)
        for row in (unrelated, record)
    )
    captured = public.resume_failure_records(logs, {run_id})
    assert len(captured) == 1
    assert captured[0]["operation"] == "register"
    assert captured[0]["httpStatus"] == 403
    assert captured[0]["frames"] == [
        {"file": "trigger.py", "function": "register", "line": 10}
    ]
    assert "private" not in json.dumps(captured)
    record["httpStatus"] = "private-status"
    with pytest.raises(ValueError, match="invalid fields"):
        public.resume_failure_records(
            "WORKFLOW_RESUME_FAILURE " + json.dumps(record), {run_id}
        )


def test_private_proof_executes_only_in_an_actual_ready_tested_container(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Wrong images fail before exec; the valid program is Python source, not a quoted JSON input."""
    monkeypatch.setenv("NS", "custos-system")
    monkeypatch.setenv("IMAGE_PREFIX", "localhost:5001/custos")
    image = "localhost:5001/custos/trigger-service:" + TAG
    pod: dict[str, Any] = {
        "metadata": {"name": "actual-trigger"},
        "spec": {"containers": [{"name": "trigger-service", "image": "unreviewed"}]},
        "status": {"containerStatuses": [{"name": "trigger-service", "ready": True}]},
    }
    executed: list[list[str]] = []

    def command(args: list[str], payload: object = None, *, timeout: int = 360) -> str:
        """Serve deployment/pod discovery and inspect the bounded exact-container exec."""
        if "deployment/custos-trigger-service" in args:
            return json.dumps(
                {"spec": {"selector": {"matchLabels": {"app": "trigger"}}}}
            )
        if "pods" in args:
            return json.dumps({"items": [pod]})
        assert payload is None and timeout == 150
        assert args[3:7] == ["exec", "actual-trigger", "-c", "trigger-service"]
        compile(args[args.index("--") + 3], "<actual-container-proof>", "exec")
        executed.append(args)
        return f"{public.REPORT_PREFIX}{json.dumps({'sha': TAG, 'runId': 'root'})}"

    monkeypatch.setitem(sys.modules, "runtime", SimpleNamespace(command=command))
    with pytest.raises(RuntimeError, match="no ready actual"):
        public.service_probe("trigger-service", "cancel", "root", TAG)
    assert not executed
    pod["spec"]["containers"][0]["image"] = image
    assert (
        public.service_probe("trigger-service", "cancel", "root", TAG)["runId"]
        == "root"
    )
    assert len(executed) == 1
