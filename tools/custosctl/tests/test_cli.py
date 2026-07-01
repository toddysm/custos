"""Tests for the custosctl CLI root group and ``doctor`` (DEVCLI-IMPL-001)."""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from click.testing import CliRunner

from custosctl import cli as cli_module
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
