"""Tests for the custosctl CLI root group and ``doctor`` (DEVCLI-IMPL-001)."""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from click.testing import CliRunner

from custosctl import cli as cli_module
from custosctl import lifecycle as lifecycle_module
from custosctl.cli import cli
from custosctl.shell import ToolStatus


@pytest.fixture
def runner() -> Iterator[CliRunner]:
    r = CliRunner()
    with r.isolated_filesystem():
        yield r


def _all_present(name: str, *_args: object) -> ToolStatus:
    return ToolStatus(name=name, found=True, version=f"{name} v1.0.0")


def test_version(runner: CliRunner) -> None:
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0, result.output
    assert "custosctl" in result.output


def test_doctor_local_all_present(runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_module, "probe_tool", _all_present)
    result = runner.invoke(cli, ["--target", "local", "doctor"])
    assert result.exit_code == 0, result.output
    assert "preflight OK" in result.output
    assert "target: local" in result.output


def test_doctor_local_missing_tool_fails(
    runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    def _probe(name: str, *_args: object) -> ToolStatus:
        if name == "kind":
            return ToolStatus(name=name, found=False)
        return _all_present(name)

    monkeypatch.setattr(cli_module, "probe_tool", _probe)
    result = runner.invoke(cli, ["--target", "local", "doctor"])
    assert result.exit_code != 0
    assert "MISS" in result.output
    assert "kind" in result.output


def test_doctor_remote_reachable(runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_module, "probe_tool", _all_present)
    monkeypatch.setattr(cli_module, "kube_context_reachable", lambda ctx, **kw: True)
    result = runner.invoke(
        cli,
        ["--target", "remote", "doctor"],
        env={"CUSTOS_KUBE_CONTEXT": "prod-ctx"},
    )
    assert result.exit_code == 0, result.output
    assert "prod-ctx" in result.output
    assert "preflight OK" in result.output


def test_doctor_remote_unreachable_fails(
    runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(cli_module, "probe_tool", _all_present)
    monkeypatch.setattr(cli_module, "kube_context_reachable", lambda ctx, **kw: False)
    result = runner.invoke(
        cli,
        ["--target", "remote", "doctor"],
        env={"CUSTOS_KUBE_CONTEXT": "prod-ctx"},
    )
    assert result.exit_code != 0


def test_doctor_remote_without_explicit_context_probes_current(
    runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    # No CUSTOS_KUBE_CONTEXT: doctor falls back to kubectl's current context
    # (effective_kube_context() is None). Here the current context is reachable.
    monkeypatch.setattr(cli_module, "probe_tool", _all_present)
    seen: list[str | None] = []

    def _reachable(ctx: str | None, **_kw: object) -> bool:
        seen.append(ctx)
        return True

    monkeypatch.setattr(cli_module, "kube_context_reachable", _reachable)
    result = runner.invoke(cli, ["--target", "remote", "doctor"], env={})
    assert result.exit_code == 0, result.output
    assert "current kubectl context" in result.output
    assert seen == [None]


def test_doctor_remote_unreachable_current_context_fails(
    runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(cli_module, "probe_tool", _all_present)
    monkeypatch.setattr(cli_module, "kube_context_reachable", lambda ctx, **kw: False)
    result = runner.invoke(cli, ["--target", "remote", "doctor"], env={})
    assert result.exit_code != 0
    assert "current kubectl context" in result.output


def test_target_flag_overrides_env(runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_module, "probe_tool", _all_present)
    monkeypatch.setattr(cli_module, "kube_context_reachable", lambda ctx, **kw: True)
    # env says local; the flag forces remote (which runs the kube-context probe).
    result = runner.invoke(
        cli,
        ["--target", "remote", "doctor"],
        env={"CUSTOS_TARGET": "local"},
    )
    assert result.exit_code == 0, result.output
    assert "target: remote" in result.output


# --- lifecycle commands (DEVCLI-IMPL-002 / 003) ---------------------------


def test_up_local_invokes_lifecycle(runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    called: list[str] = []
    monkeypatch.setattr(lifecycle_module, "up", lambda s, echo: called.append("up"))
    result = runner.invoke(cli, ["up"], env={})
    assert result.exit_code == 0, result.output
    assert called == ["up"]


def test_up_remote_invokes_lifecycle(runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    called: list[str] = []
    monkeypatch.setattr(lifecycle_module, "up", lambda s, echo: called.append("up"))
    result = runner.invoke(cli, ["--target", "remote", "up"], env={"CUSTOS_KUBE_CONTEXT": "x"})
    assert result.exit_code == 0, result.output
    assert called == ["up"]


def test_down_aborts_without_yes(runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    called: list[str] = []
    monkeypatch.setattr(
        lifecycle_module, "down", lambda s, echo, force=False: called.append("down")
    )
    result = runner.invoke(cli, ["down"], input="n\n", env={})
    assert result.exit_code != 0  # aborted
    assert called == []


def test_down_with_yes_invokes_lifecycle(
    runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: list[bool] = []
    monkeypatch.setattr(lifecycle_module, "down", lambda s, echo, force=False: seen.append(force))
    result = runner.invoke(cli, ["--yes", "down"], env={})
    assert result.exit_code == 0, result.output
    assert seen == [False]


def test_down_remote_force_passthrough(runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[bool] = []
    monkeypatch.setattr(lifecycle_module, "down", lambda s, echo, force=False: seen.append(force))
    result = runner.invoke(
        cli,
        ["--target", "remote", "--yes", "down", "--force"],
        env={"CUSTOS_KUBE_CONTEXT": "prod"},
    )
    assert result.exit_code == 0, result.output
    assert seen == [True]


def test_down_force_local_errors(runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    called: list[str] = []
    monkeypatch.setattr(
        lifecycle_module, "down", lambda s, echo, force=False: called.append("down")
    )
    result = runner.invoke(cli, ["--yes", "down", "--force"], env={})
    assert result.exit_code != 0
    assert "only to --target remote" in result.output
    assert called == []


def test_status_exit_code_reflects_health(
    runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(lifecycle_module, "status", lambda s, echo: True)
    assert runner.invoke(cli, ["status"], env={}).exit_code == 0
    monkeypatch.setattr(lifecycle_module, "status", lambda s, echo: False)
    assert runner.invoke(cli, ["status"], env={}).exit_code == 1


# --- connector commands (DEVCLI-IMPL-005) ---------------------------------


def test_connector_register_prints_ref(runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    from custosctl import connectors as connectors_module

    monkeypatch.setattr(
        connectors_module,
        "register",
        lambda s, *, path, image_ref: {
            "type": "dockerhub",
            "version": "1.2.0",
            "digest": "sha256:ab",
        },
    )
    result = runner.invoke(
        cli, ["connector", "register", "extensions/connectors/dockerhub"], env={}
    )
    assert result.exit_code == 0, result.output
    assert "registered dockerhub@1.2.0 (sha256:ab)" in result.output


def test_connector_list_prints_versions(runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    from custosctl import connectors as connectors_module

    monkeypatch.setattr(
        connectors_module,
        "list_versions",
        lambda s, *, connector_type: [
            {"type": "dockerhub", "version": "1.0.0", "digest": "sha256:1"},
            {"type": "dockerhub", "version": "1.1.0", "digest": "sha256:2"},
        ],
    )
    result = runner.invoke(cli, ["connector", "list", "dockerhub"], env={})
    assert result.exit_code == 0, result.output
    assert "1.0.0" in result.output
    assert "1.1.0" in result.output


def test_connector_list_empty(runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    from custosctl import connectors as connectors_module

    monkeypatch.setattr(connectors_module, "list_versions", lambda s, *, connector_type: [])
    result = runner.invoke(cli, ["connector", "list", "nope"], env={})
    assert result.exit_code == 0, result.output
    assert "no versions registered" in result.output


def test_connector_register_maps_errors(runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    from custosctl import connectors as connectors_module

    def _boom(s: object, *, path: str, image_ref: str | None) -> dict[str, str]:
        raise RuntimeError("CUSTOS_GATEWAY is required for API commands")

    monkeypatch.setattr(connectors_module, "register", _boom)
    result = runner.invoke(cli, ["connector", "register", "x"], env={})
    assert result.exit_code != 0
    assert "CUSTOS_GATEWAY is required" in result.output


# --- activity commands (DEVCLI-IMPL-006) ----------------------------------


def test_activity_register_prints_ref(runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    from custosctl import activities as activities_module

    monkeypatch.setattr(
        activities_module,
        "register",
        lambda s, *, path, image_ref, workspace: {
            "namespace": "custos.builtin",
            "type": "copy-image",
            "version": "0.1.0",
            "digest": "sha256:ab",
        },
    )
    result = runner.invoke(
        cli, ["activity", "register", "extensions/activities/copy-image"], env={}
    )
    assert result.exit_code == 0, result.output
    assert "registered custos.builtin/copy-image@0.1.0 (sha256:ab)" in result.output


def test_activity_list_prints_versions(runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    from custosctl import activities as activities_module

    monkeypatch.setattr(
        activities_module,
        "list_versions",
        lambda s, *, namespace, activity_type, workspace: [
            {
                "namespace": namespace,
                "type": activity_type,
                "version": "0.1.0",
                "digest": "sha256:1",
            },
        ],
    )
    result = runner.invoke(cli, ["activity", "list", "custos.builtin", "copy-image"], env={})
    assert result.exit_code == 0, result.output
    assert "0.1.0" in result.output


def test_activity_list_empty(runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    from custosctl import activities as activities_module

    monkeypatch.setattr(
        activities_module, "list_versions", lambda s, *, namespace, activity_type, workspace: []
    )
    result = runner.invoke(cli, ["activity", "list", "ns", "nope"], env={})
    assert result.exit_code == 0, result.output
    assert "no versions registered" in result.output


# --- workflow commands (DEVCLI-IMPL-007) ----------------------------------


def test_workflow_apply_prints_ref(
    runner: CliRunner, monkeypatch: pytest.MonkeyPatch, tmp_path: object
) -> None:
    from custosctl import workflows as workflows_module

    monkeypatch.setattr(
        workflows_module,
        "apply",
        lambda s, *, path, workspace: {
            "workspaceId": "ws-prod",
            "workflowName": "demo",
            "version": 3,
        },
    )
    result = runner.invoke(cli, ["workflow", "apply", "wf.yaml", "--workspace", "ws-prod"], env={})
    assert result.exit_code == 0, result.output
    assert "published demo@3 (workspace ws-prod)" in result.output


def test_workflow_run_parses_inputs(runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    from custosctl import workflows as workflows_module

    seen: dict[str, object] = {}

    def _run(
        s: object,
        *,
        workflow_version_id: str,
        workspace: str | None,
        inputs: dict[str, object],
        idempotency_key: str | None,
    ) -> dict[str, str]:
        seen["inputs"] = inputs
        return {"runId": "run-1", "status": "running"}

    monkeypatch.setattr(workflows_module, "run", _run)
    result = runner.invoke(
        cli,
        ["workflow", "run", "wfv-1", "--input", "count=3", "--input", "name=demo"],
        env={"CUSTOS_WORKSPACE": "ws-prod"},
    )
    assert result.exit_code == 0, result.output
    assert "started run-1 (status running)" in result.output
    # count parses as JSON int; name is a bare string.
    assert seen["inputs"] == {"count": 3, "name": "demo"}


def test_workflow_run_rejects_bad_input(runner: CliRunner) -> None:
    result = runner.invoke(
        cli, ["workflow", "run", "wfv-1", "--input", "noequals"], env={"CUSTOS_WORKSPACE": "w"}
    )
    assert result.exit_code != 0
    assert "KEY=VALUE" in result.output


def test_workflow_run_rejects_empty_key(runner: CliRunner) -> None:
    result = runner.invoke(
        cli, ["workflow", "run", "wfv-1", "--input", "=3"], env={"CUSTOS_WORKSPACE": "w"}
    )
    assert result.exit_code != 0
    assert "non-empty key" in result.output


def test_workflow_status_prints(runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    from custosctl import workflows as workflows_module

    monkeypatch.setattr(
        workflows_module,
        "get_status",
        lambda s, *, run_id, workspace: {"runId": run_id, "status": "succeeded"},
    )
    result = runner.invoke(cli, ["workflow", "status", "run-9"], env={"CUSTOS_WORKSPACE": "ws"})
    assert result.exit_code == 0, result.output
    assert "run-9: succeeded" in result.output


def test_workflow_status_watch_nonzero_on_failure(
    runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    from custosctl import workflows as workflows_module

    monkeypatch.setattr(
        workflows_module,
        "wait_for",
        lambda s, *, run_id, workspace, timeout, interval: {
            "runId": run_id,
            "status": "failed",
            "reason": "boom",
        },
    )
    monkeypatch.setattr(workflows_module, "is_success", lambda r: False)
    result = runner.invoke(
        cli, ["workflow", "status", "run-9", "--watch"], env={"CUSTOS_WORKSPACE": "ws"}
    )
    assert result.exit_code == 1
    assert "failed" in result.output
    assert "reason: boom" in result.output
