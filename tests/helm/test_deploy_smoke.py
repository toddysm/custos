"""Regress the smoke-only runtime activation and pre-admission identity bootstrap."""

import asyncio
import json
import runpy
import subprocess
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
smoke = SimpleNamespace(**runpy.run_path(str(ROOT / "tests/helm/smoke/runtime.py")))
TAG = "a" * 40
SIDECAR = "localhost:5001/smoke-sidecar@sha256:" + "b" * 64
HELPER = "localhost:5001/smoke-hook-helper@sha256:" + "c" * 64


def managed_render() -> list[dict[str, Any]]:
    """Render the actual profile with the exact overlay used by hosted smoke setup."""
    overlay = smoke.settings(TAG, "custos-system", ["10.96.0.1"], SIDECAR, HELPER)
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
    assert all(not row.get("stringData") for row in documents)


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
