"""Regress managed smoke activation, scoped bootstrap and real receiver evidence acceptance."""

import asyncio
import json
import runpy
import subprocess
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Literal
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
smoke = SimpleNamespace(**runpy.run_path(str(ROOT / "tests/helm/smoke/runtime.py")))
receivers = SimpleNamespace(
    **runpy.run_path(str(ROOT / "tests/helm/smoke/receivers.py"))
)
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


@pytest.mark.parametrize(
    "waiting,delivery_mode", [(False, "push"), (True, "push"), (True, "pull")]
)
def test_receiver_certification_definitions_use_actual_workflow_contracts(
    waiting: bool,
    delivery_mode: Literal["push", "pull"],
) -> None:
    """Synthetic start/wait definitions must pass the real compiler, not just JSON construction."""
    from datetime import UTC, datetime

    from custos_workflow.bindings.registry import InMemoryActivityTypeRegistry
    from custos_workflow.compiler import RunMeta, compile
    from custos_workflow.document import WorkflowDocument

    document = WorkflowDocument.model_validate(
        receivers.workflow_document(waiting=waiting, delivery_mode=delivery_mode)
    )
    graph = compile(
        document,
        RunMeta(
            workspace_id=receivers.WORKSPACE,
            workflow_version_id="synthetic",
            workflow_name=document.metadata.name,
            workflow_version_label="1",
            started_at_default=datetime(2026, 1, 1, tzinfo=UTC),
        ),
        InMemoryActivityTypeRegistry({}),
    )
    assert graph.topological_order == (
        ("await-source", "effect") if waiting else ("effect",)
    )


def test_receiver_client_cannot_access_backend_secrets_or_kubernetes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only one workspace token and upstream test keys are projected, never backend credentials."""
    monkeypatch.setenv("GITHUB_SHA", TAG)
    job = receivers.client_job("synthetic:tested", "10.244.0.12")
    pod = job["spec"]["template"]["spec"]
    container = pod["containers"][0]
    assert pod["automountServiceAccountToken"] is False
    assert "serviceAccountName" not in pod
    projected = [
        row["valueFrom"]["secretKeyRef"]
        for row in container["env"]
        if "valueFrom" in row
    ]
    assert {(row["name"], row["key"]) for row in projected} == {
        (receivers.OPERATOR_SECRET, key)
        for key in ("token", "webhook-key", "rotated-webhook-key")
    }
    assert "envFrom" not in container
    assert container["securityContext"]["readOnlyRootFilesystem"] is True
    assert pod["volumes"] == [
        {"name": "script", "configMap": {"name": "receiver-certification-script"}}
    ]


def test_receiver_client_refuses_a_successful_job_without_effect_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Job completion alone is never reported as receiver-to-Workflow certification."""
    import sys

    monkeypatch.setenv("NS", "synthetic")
    monkeypatch.setenv("GITHUB_SHA", TAG)
    monkeypatch.setenv("IMAGE_PREFIX", "synthetic")
    monkeypatch.setitem(
        sys.modules,
        "runtime",
        SimpleNamespace(
            command=Mock(
                side_effect=[
                    "created",
                    json.dumps({"status": {"succeeded": 1}}),
                    "no evidence",
                ]
            )
        ),
    )
    with pytest.raises(
        receivers.CertificationFailure, match="no certification claimed"
    ):
        receivers.run_client("synthetic:tested", "10.244.0.12", phase="exercise")


@pytest.mark.parametrize("outcome", ["reachable", "refused", "dns-failure"])
def test_receiver_isolation_does_not_confuse_unavailability_with_filtering(
    monkeypatch: pytest.MonkeyPatch,
    outcome: str,
) -> None:
    """An absent listener, bad address or successful TCP session cannot certify policy denial."""
    connect = Mock()
    if outcome == "reachable":
        connect.return_value = MagicMock()
    elif outcome == "refused":
        connect.side_effect = ConnectionRefusedError()
    else:
        import socket

        connect.side_effect = socket.gaierror()
    monkeypatch.setattr("socket.create_connection", connect)
    with pytest.raises(receivers.CertificationFailure, match="network-policy"):
        receivers.check_network_isolation("10.244.0.12")


def test_receiver_isolation_requires_every_backend_port_to_be_filtered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One filtered connection cannot hide an exposed app or Dapr listener."""
    connect = Mock(side_effect=[TimeoutError(), MagicMock()])
    monkeypatch.setattr("socket.create_connection", connect)
    with pytest.raises(receivers.CertificationFailure, match="3500 is reachable"):
        receivers.check_network_isolation("10.244.0.12")
    connect.side_effect = TimeoutError()
    connect.reset_mock()
    receivers.check_network_isolation("10.244.0.12")
    assert connect.call_count == 4


def test_receiver_webhook_signs_the_exact_anonymous_request_bytes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Public upstream signatures must cover transmitted JSON without leaking the operator token."""
    import hashlib
    import hmac

    monkeypatch.setenv("CUSTOS_TEST_TOKEN", "synthetic-operator")
    monkeypatch.setenv("CUSTOS_GATEWAY_URL", "http://custos-api-gateway:8080")
    monkeypatch.setenv("CUSTOS_TEST_PHASE", "exercise")
    response = MagicMock()
    response.__enter__.return_value = response
    response.status = 200
    response.read.return_value = b'{"accepted": 1, "rejected": 0}'
    send = Mock(return_value=response)
    monkeypatch.setattr("urllib.request.urlopen", send)
    gateway = receivers.Gateway()
    gateway.webhook(
        [{"eventId": "synthetic", "value": "quoted \\"}], "synthetic-upstream"
    )
    request = send.call_args.args[0]
    headers = {key.lower(): value for key, value in request.header_items()}
    assert "authorization" not in headers
    assert (
        headers["x-custos-signature"]
        == "sha256="
        + hmac.new(
            b"synthetic-upstream",
            request.data,
            hashlib.sha256,
        ).hexdigest()
    )


def test_receiver_http_failure_records_only_a_closed_problem_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep enough evidence to classify a real 503 without leaking upstream detail fields."""
    import io
    import urllib.error
    from email.message import Message

    monkeypatch.setenv("CUSTOS_TEST_TOKEN", "synthetic-operator")
    monkeypatch.setenv("CUSTOS_GATEWAY_URL", "http://custos-api-gateway:8080")
    monkeypatch.setenv("CUSTOS_TEST_PHASE", "exercise")
    failure = urllib.error.HTTPError(
        "http://custos-api-gateway:8080/synthetic",
        503,
        "unavailable",
        Message(),
        io.BytesIO(
            b'{"code":"trigger.dispatch_unavailable","detail":"sensitive-upstream"}'
        ),
    )
    monkeypatch.setattr("urllib.request.urlopen", Mock(side_effect=failure))
    with pytest.raises(receivers.CertificationFailure) as caught:
        receivers.Gateway().request("POST", "/synthetic", {})
    assert (
        str(caught.value) == "POST /synthetic: HTTP 503 (trigger.dispatch_unavailable)"
    )
    assert "sensitive-upstream" not in str(caught.value)
    assert failure.closed


def test_receiver_effect_reads_page_past_the_first_workflow_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A redelivery or missing deep-page run must not disappear behind the public page boundary."""
    from urllib.parse import parse_qs, urlsplit

    monkeypatch.setenv("CUSTOS_TEST_TOKEN", "synthetic-operator")
    monkeypatch.setenv("CUSTOS_GATEWAY_URL", "http://custos-api-gateway:8080")
    monkeypatch.setenv("CUSTOS_TEST_PHASE", "exercise")
    gateway = receivers.Gateway()
    request = Mock(
        side_effect=[
            {"items": [{"runId": "first"}], "nextCursor": "cursor+with/slash"},
            {"items": [{"runId": "second"}], "nextCursor": None},
        ]
    )
    monkeypatch.setattr(gateway, "request", request)
    assert [row["runId"] for row in gateway.runs("synthetic")] == ["first", "second"]
    query = parse_qs(urlsplit(request.call_args.args[1]).query)
    assert query == {
        "cursor": ["cursor+with/slash"],
        "workflowVersionId": [receivers.WORKSPACE + "/synthetic@1"],
    }


@pytest.mark.parametrize(
    "overrides",
    [
        {"fullReceiverCertification": True},
        {"completedScenarios": []},
        {"sha": "b" * 40},
        {"phase": "rotation"},
    ],
)
def test_receiver_evidence_cannot_promote_partial_or_mismatched_execution(
    monkeypatch: pytest.MonkeyPatch,
    overrides: dict[str, Any],
) -> None:
    """A completed Job is insufficient if its report overclaims scope or names a different run."""
    import sys

    monkeypatch.setenv("NS", "synthetic")
    monkeypatch.setenv("GITHUB_SHA", TAG)
    report = {
        "sha": TAG,
        "phase": "exercise",
        "fullReceiverCertification": False,
        "completedScenarios": ["synthetic-completed-scenario"],
        "elapsedSeconds": 1,
        **overrides,
    }
    monkeypatch.setitem(
        sys.modules,
        "runtime",
        SimpleNamespace(
            command=Mock(
                side_effect=[
                    "created",
                    json.dumps({"status": {"succeeded": 1}}),
                    receivers.REPORT_PREFIX + json.dumps(report),
                ]
            )
        ),
    )
    with pytest.raises(receivers.CertificationFailure, match="receiver evidence"):
        receivers.run_client("synthetic:tested", "10.244.0.12", phase="exercise")


def test_vendor_public_effect_deadline_does_not_accept_a_producer_acknowledgement(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """No effect report is emitted when a real-producer phase is followed by no Workflow start."""
    monkeypatch.setenv("CUSTOS_WAIT_RUN_ID", "synthetic-wait")
    gateway = Mock()
    gateway.runs.return_value = []
    monkeypatch.setitem(
        receivers.exercise_vendor.__globals__,
        "Gateway",
        Mock(return_value=gateway),
    )
    monkeypatch.setitem(
        receivers.exercise_vendor.__globals__,
        "time",
        SimpleNamespace(
            monotonic=Mock(side_effect=[0, 0, 0, 181]),
            sleep=Mock(),
        ),
    )
    with pytest.raises(receivers.CertificationFailure, match="no workflow effect"):
        receivers.exercise_vendor(redelivery=False)
    gateway.wait_terminal.assert_not_called()
    assert receivers.REPORT_PREFIX not in capsys.readouterr().out


def test_pull_effect_report_requires_the_real_resume_to_complete(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Acknowledged unpause and polling cannot certify a wait that failed to resume."""
    monkeypatch.setenv("CUSTOS_WAIT_RUN_ID", "synthetic-pull-wait")
    gateway = Mock()
    gateway.wait_terminal.side_effect = receivers.CertificationFailure(
        "synthetic resume failed"
    )
    monkeypatch.setitem(
        receivers.exercise_pull.__globals__, "Gateway", Mock(return_value=gateway)
    )
    with pytest.raises(receivers.CertificationFailure, match="resume failed"):
        receivers.exercise_pull("pull-resume")
    gateway.pull_cursor.assert_not_called()
    assert receivers.REPORT_PREFIX not in capsys.readouterr().out


def test_pull_pause_rejects_a_cursor_advance(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A public pause acknowledgement cannot hide a later committed upstream effect."""
    gateway = Mock()
    gateway.pull_cursor.side_effect = ["synthetic-before", "synthetic-after"]
    monkeypatch.setitem(
        receivers.exercise_pull.__globals__, "Gateway", Mock(return_value=gateway)
    )
    with pytest.raises(receivers.CertificationFailure, match="operator-paused"):
        receivers.exercise_pull("pull-pause")
    assert receivers.REPORT_PREFIX not in capsys.readouterr().out


def test_pull_cursor_requires_a_real_committed_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An acquired but never-committed cursor remains uninitialized, not progress proof."""
    gateway = object.__new__(receivers.Gateway)
    monkeypatch.setattr(
        gateway,
        "request",
        Mock(
            return_value={
                "workspaceId": receivers.WORKSPACE,
                "instanceId": receivers.PULL_CONNECTOR,
                "encoding": "oci-list-tags-v1",
                "valueFingerprint": None,
                "valueLength": None,
                "leaseHolder": "synthetic-holder",
            }
        ),
    )
    monkeypatch.setitem(
        receivers.Gateway.pull_cursor.__globals__,
        "time",
        SimpleNamespace(monotonic=Mock(side_effect=[0, 0, 181]), sleep=Mock()),
    )
    with pytest.raises(receivers.CertificationFailure, match="cursor observation"):
        gateway.pull_cursor()


def test_vendor_completion_requires_the_registered_wait_to_resume(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A successful started Workflow cannot hide a missing source-associated resume effect."""
    monkeypatch.setenv("CUSTOS_WAIT_RUN_ID", "synthetic-wait")
    gateway = Mock()
    gateway.runs.return_value = [{"runId": "synthetic-start"}]
    gateway.wait_terminal.side_effect = [
        None,
        receivers.CertificationFailure("synthetic wait did not complete"),
    ]
    monkeypatch.setitem(
        receivers.exercise_vendor.__globals__,
        "Gateway",
        Mock(return_value=gateway),
    )
    with pytest.raises(receivers.CertificationFailure, match="wait did not complete"):
        receivers.exercise_vendor(redelivery=False)
    assert [call.args[0] for call in gateway.wait_terminal.call_args_list] == [
        "synthetic-start",
        "synthetic-wait",
    ]
    assert receivers.REPORT_PREFIX not in capsys.readouterr().out
