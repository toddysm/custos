"""Tests for the shell subprocess helpers (DEVCLI-IMPL-010 coverage)."""

from __future__ import annotations

import shutil
import subprocess
from collections.abc import Sequence
from pathlib import Path

import pytest

from custosctl import shell


def _completed(
    returncode: int = 0, stdout: str = "", stderr: str = ""
) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args=[], returncode=returncode, stdout=stdout, stderr=stderr)


class _RunSpy:
    """Patch ``shell.run`` and script its return per call; record invocations."""

    def __init__(
        self, monkeypatch: pytest.MonkeyPatch, results: list[subprocess.CompletedProcess[str]]
    ) -> None:
        self.calls: list[tuple[list[str], dict[str, object]]] = []
        self._results = list(results)

        def _run(argv: Sequence[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
            self.calls.append((list(argv), kwargs))
            return self._results.pop(0) if self._results else _completed()

        monkeypatch.setattr(shell, "run", _run)

    @property
    def last_argv(self) -> list[str]:
        return self.calls[-1][0]


# --- run primitive --------------------------------------------------------


def test_run_success_capture(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, object] = {}

    def _sub(argv: object, **kw: object) -> subprocess.CompletedProcess[str]:
        seen.update(kw)
        seen["argv"] = argv
        return _completed(0, "out")

    monkeypatch.setattr(subprocess, "run", _sub)
    result = shell.run(["echo", "hi"], capture=True, env={"A": "1"})
    assert result.stdout == "out"
    assert seen["argv"] == ["echo", "hi"]
    assert seen["env"] == {"A": "1"}
    assert seen["capture_output"] is True


def test_run_nonzero_raises_command_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: _completed(2, "", "boom"))
    with pytest.raises(shell.CommandError) as excinfo:
        shell.run(["false"], capture=True)
    assert excinfo.value.returncode == 2
    assert "boom" in str(excinfo.value)


def test_run_nonzero_check_false_returns(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: _completed(1))
    result = shell.run(["x"], check=False)
    assert result.returncode == 1


# --- which / probe_tool / reachability ------------------------------------


def test_which(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(shutil, "which", lambda t: "/usr/bin/" + t)
    assert shell.which("kubectl") == "/usr/bin/kubectl"


def test_probe_tool_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(shell, "which", lambda t: None)
    status = shell.probe_tool("nope")
    assert status.found is False and status.ok is False


def test_probe_tool_version_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(shell, "which", lambda t: "/bin/tool")
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: _completed(0, "tool v1.2\nextra"))
    status = shell.probe_tool("tool")
    assert status.found is True
    assert status.version == "tool v1.2"


def test_probe_tool_version_failure_is_advisory(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(shell, "which", lambda t: "/bin/tool")
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: _completed(1, "", "unknown flag"))
    status = shell.probe_tool("tool")
    assert status.found is True and status.version is None


def test_probe_tool_oserror(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(shell, "which", lambda t: "/bin/tool")

    def _raise(*a: object, **k: object) -> subprocess.CompletedProcess[str]:
        raise OSError("nope")

    monkeypatch.setattr(subprocess, "run", _raise)
    assert shell.probe_tool("tool").version is None


def test_kube_context_reachable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(shell, "which", lambda t: "/bin/kubectl")
    calls: list[list[str]] = []

    def _sub(argv: list[str], **k: object) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        return _completed(0)

    monkeypatch.setattr(subprocess, "run", _sub)
    assert shell.kube_context_reachable("ctx") is True
    assert "--context" in calls[0] and "ctx" in calls[0]


def test_kube_context_reachable_no_kubectl(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(shell, "which", lambda t: None)
    assert shell.kube_context_reachable(None) is False


def test_kube_context_reachable_oserror(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(shell, "which", lambda t: "/bin/kubectl")

    def _raise(*a: object, **k: object) -> subprocess.CompletedProcess[str]:
        raise subprocess.TimeoutExpired(cmd="kubectl", timeout=1)

    monkeypatch.setattr(subprocess, "run", _raise)
    assert shell.kube_context_reachable("ctx") is False


# --- kind / helm / make / scripts (argv builders) -------------------------


def test_kind_cluster_exists(monkeypatch: pytest.MonkeyPatch) -> None:
    _RunSpy(monkeypatch, [_completed(0, "a\ncustos-local\n")])
    assert shell.kind_cluster_exists("custos-local") is True


def test_kind_cluster_exists_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    _RunSpy(monkeypatch, [_completed(0, "other\n")])
    assert shell.kind_cluster_exists("custos-local") is False


def test_kind_cluster_exists_error(monkeypatch: pytest.MonkeyPatch) -> None:
    _RunSpy(monkeypatch, [_completed(1)])
    assert shell.kind_cluster_exists("x") is False


def test_kind_create_delete(monkeypatch: pytest.MonkeyPatch) -> None:
    spy = _RunSpy(monkeypatch, [])
    shell.kind_create("c1", "kindest/node:v1")
    assert spy.last_argv == [
        "kind",
        "create",
        "cluster",
        "--name",
        "c1",
        "--image",
        "kindest/node:v1",
    ]
    shell.kind_delete("c1")
    assert spy.last_argv == ["kind", "delete", "cluster", "--name", "c1"]


def test_run_script_and_make(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    spy = _RunSpy(monkeypatch, [])
    shell.run_script(tmp_path / "x.sh", cwd=tmp_path, args=("--flag",))
    assert spy.last_argv == [str(tmp_path / "x.sh"), "--flag"]
    shell.make_target("deps", cwd=tmp_path)
    assert spy.last_argv == ["make", "-C", str(tmp_path), "deps"]


def test_helm_release_exists(monkeypatch: pytest.MonkeyPatch) -> None:
    _RunSpy(monkeypatch, [_completed(0)])
    assert shell.helm_release_exists("custos", "ns", context="ctx") is True


def test_helm_repo_and_install(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    spy = _RunSpy(monkeypatch, [])
    shell.helm_repo_add("cnpg", "https://example")
    assert spy.last_argv == ["helm", "repo", "add", "cnpg", "https://example", "--force-update"]
    shell.helm_repo_update()
    assert spy.last_argv == ["helm", "repo", "update"]
    shell.helm_install(
        "custos",
        tmp_path / "chart",
        namespace="ns",
        values=tmp_path / "v.yaml",
        sets=["a=b"],
        version="1.0.0",
        create_namespace=True,
        timeout="9m",
        context="ctx",
    )
    argv = spy.last_argv
    assert argv[:5] == ["helm", "upgrade", "--install", "custos", str(tmp_path / "chart")]
    assert "--version" in argv and "1.0.0" in argv
    assert "--create-namespace" in argv
    assert "--set" in argv and "a=b" in argv
    assert "--wait" in argv and "9m" in argv
    assert "--kube-context" in argv and "ctx" in argv


def test_helm_template_and_uninstall(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    spy = _RunSpy(monkeypatch, [_completed(0, "rendered")])
    out = shell.helm_template("custos", tmp_path, namespace="ns", show_only="charts/x.yaml")
    assert out == "rendered"
    assert "--show-only" in spy.last_argv
    shell.helm_uninstall("custos", namespace="ns", context="ctx")
    assert spy.last_argv[:3] == ["helm", "uninstall", "custos"]
    assert "--ignore-not-found" in spy.last_argv


def test_kubectl_helpers(monkeypatch: pytest.MonkeyPatch) -> None:
    spy = _RunSpy(monkeypatch, [_completed(0, "manifest"), _completed(0)])
    shell.kubectl_ensure_namespace("ns", context="ctx")
    # second call is the apply.
    assert spy.calls[-1][0][:2] == ["kubectl", "--context"]
    shell.kubectl_apply_stdin("doc", namespace="ns")
    assert spy.last_argv[-3:] == ["-n", "ns", "-f"] or "-f" in spy.last_argv
    shell.kubectl_delete_namespace("ns")
    assert spy.last_argv == ["kubectl", "delete", "namespace", "ns", "--ignore-not-found"]
    shell.kubectl_wait("cluster/custos", namespace="ns", condition="Ready", timeout="1m")
    assert "wait" in spy.last_argv and "--for=condition=Ready" in spy.last_argv


def test_kubectl_current_context(monkeypatch: pytest.MonkeyPatch) -> None:
    _RunSpy(monkeypatch, [_completed(0, "kind-custos-local\n")])
    assert shell.kubectl_current_context() == "kind-custos-local"


def test_kubectl_current_context_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    _RunSpy(monkeypatch, [_completed(1)])
    assert shell.kubectl_current_context() is None


def test_kubectl_pod_phases(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = (
        '{"items":['
        '{"metadata":{"name":"a"},"status":{"phase":"Running"}},'
        '{"metadata":{"name":"b"},"status":{"phase":"Pending"}}'
        "]}"
    )
    _RunSpy(monkeypatch, [_completed(0, payload)])
    assert shell.kubectl_pod_phases("ns") == [("a", "Running"), ("b", "Pending")]


def test_kubectl_pod_phases_empty_on_error(monkeypatch: pytest.MonkeyPatch) -> None:
    _RunSpy(monkeypatch, [_completed(1, "")])
    assert shell.kubectl_pod_phases("ns") == []


def test_kubectl_pod_phases_bad_json(monkeypatch: pytest.MonkeyPatch) -> None:
    _RunSpy(monkeypatch, [_completed(0, "not json")])
    assert shell.kubectl_pod_phases("ns") == []


# --- resolve_image_digest -------------------------------------------------


def test_resolve_image_digest_docker(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(shell, "which", lambda t: "/bin/docker" if t == "docker" else None)
    _RunSpy(monkeypatch, [_completed(0, "sha256:" + "a" * 64)])
    assert shell.resolve_image_digest("ghcr.io/x:v1") == "sha256:" + "a" * 64


def test_resolve_image_digest_crane_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(shell, "which", lambda t: "/bin/crane" if t == "crane" else None)
    _RunSpy(monkeypatch, [_completed(0, "sha256:" + "b" * 64)])
    assert shell.resolve_image_digest("ghcr.io/x:v1").startswith("sha256:")


def test_resolve_image_digest_none_available(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(shell, "which", lambda t: None)
    with pytest.raises(shell.CommandError, match="could not resolve a digest"):
        shell.resolve_image_digest("ghcr.io/x:v1")
