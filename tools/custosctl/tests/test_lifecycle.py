"""Tests for the local (kind) lifecycle orchestration (DEVCLI-IMPL-002)."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from custosctl import lifecycle, shell
from custosctl.config import Settings, Target


def _fake_checkout(tmp_path: Path, *, profile: str = "connected-eval") -> Path:
    """Create a minimal directory that looks like a Custos checkout."""
    (tmp_path / "deploy" / "helm" / "custos").mkdir(parents=True)
    (tmp_path / "deploy" / "helm" / "custos" / "Chart.yaml").write_text("name: custos\n")
    (tmp_path / "deploy" / "helm" / "custos" / f"values-{profile}.yaml").write_text("{}\n")
    (tmp_path / "scripts").mkdir()
    (tmp_path / "scripts" / "install-prereqs.sh").write_text("#!/usr/bin/env bash\n")
    (tmp_path / "Makefile").write_text("deps:\n\t@true\n")
    return tmp_path


def _settings(root: Path, **overrides: object) -> Settings:
    base: dict[str, object] = {
        "target": Target.LOCAL,
        "repo_root": root,
        "cluster": "test-cluster",
        "namespace": "custos-system",
        "release": "custos",
        "profile": "connected-eval",
    }
    base.update(overrides)
    return Settings(**base)  # type: ignore[arg-type]


class _Recorder:
    """Patches every shell action lifecycle uses and records the call order."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, *, cluster_exists: bool) -> None:
        self.calls: list[str] = []
        self._cluster_exists = cluster_exists
        self.pods: list[tuple[str, str]] = [("api-gateway-0", "Running")]
        self.release_installed = True

        def rec(name: str, ret: object = None) -> Callable[..., object]:
            def _fn(*_a: object, **_k: object) -> object:
                self.calls.append(name)
                return ret

            return _fn

        monkeypatch.setattr(shell, "kind_cluster_exists", lambda *_a, **_k: self._cluster_exists)
        monkeypatch.setattr(shell, "kind_create", rec("kind_create"))
        monkeypatch.setattr(shell, "kind_delete", rec("kind_delete"))
        monkeypatch.setattr(shell, "run_script", rec("run_script"))
        monkeypatch.setattr(shell, "helm_repo_add", rec("helm_repo_add"))
        monkeypatch.setattr(shell, "helm_repo_update", rec("helm_repo_update"))
        monkeypatch.setattr(shell, "make_target", rec("make_target"))
        monkeypatch.setattr(shell, "kubectl_ensure_namespace", rec("kubectl_ensure_namespace"))
        monkeypatch.setattr(shell, "helm_template", rec("helm_template", "manifest"))
        monkeypatch.setattr(shell, "kubectl_apply_stdin", rec("kubectl_apply_stdin"))
        monkeypatch.setattr(shell, "kubectl_wait", rec("kubectl_wait"))
        monkeypatch.setattr(shell, "helm_uninstall", rec("helm_uninstall"))
        monkeypatch.setattr(shell, "helm_release_exists", lambda *_a, **_k: self.release_installed)
        monkeypatch.setattr(shell, "kubectl_pod_phases", lambda *_a, **_k: self.pods)

        def _helm_install(release: str, *_a: object, **_k: object) -> None:
            self.calls.append(f"helm_install:{release}")

        monkeypatch.setattr(shell, "helm_install", _helm_install)


def test_up_creates_cluster_and_installs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _fake_checkout(tmp_path)
    rec = _Recorder(monkeypatch, cluster_exists=False)
    echoes: list[str] = []
    lifecycle.up(_settings(root), echo=echoes.append)

    assert "kind_create" in rec.calls
    assert "run_script" in rec.calls
    assert "make_target" in rec.calls
    # CNPG operator installs before the platform release.
    assert rec.calls.index("helm_install:cnpg") < rec.calls.index("helm_install:custos")
    # Postgres is pre-provisioned before the platform install.
    assert rec.calls.index("kubectl_wait") < rec.calls.index("helm_install:custos")
    assert echoes[-1] == "platform up"


def test_up_skips_kind_when_cluster_exists(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _fake_checkout(tmp_path)
    rec = _Recorder(monkeypatch, cluster_exists=True)
    lifecycle.up(_settings(root), echo=lambda _m: None)
    assert "kind_create" not in rec.calls


def test_up_skips_prereqs_when_configured(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _fake_checkout(tmp_path)
    rec = _Recorder(monkeypatch, cluster_exists=True)
    lifecycle.up(_settings(root, prereqs="skip"), echo=lambda _m: None)
    assert "run_script" not in rec.calls


def test_up_unknown_profile_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _fake_checkout(tmp_path)
    _Recorder(monkeypatch, cluster_exists=True)
    with pytest.raises(RuntimeError, match="values file not found"):
        lifecycle.up(_settings(root, profile="does-not-exist"), echo=lambda _m: None)


def test_down_uninstalls_and_deletes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _fake_checkout(tmp_path)
    rec = _Recorder(monkeypatch, cluster_exists=True)
    echoes: list[str] = []
    lifecycle.down(_settings(root), echo=echoes.append)
    assert rec.calls == ["helm_uninstall", "kind_delete"]
    assert echoes[-1] == "platform down"


def test_status_all_running_is_true(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _fake_checkout(tmp_path)
    _Recorder(monkeypatch, cluster_exists=True)
    assert lifecycle.status(_settings(root), echo=lambda _m: None) is True


def test_status_absent_cluster_is_false(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _fake_checkout(tmp_path)
    _Recorder(monkeypatch, cluster_exists=False)
    assert lifecycle.status(_settings(root), echo=lambda _m: None) is False


def test_status_not_all_running_is_false(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _fake_checkout(tmp_path)
    rec = _Recorder(monkeypatch, cluster_exists=True)
    rec.pods = [("api-gateway-0", "Running"), ("auth-0", "Pending")]
    assert lifecycle.status(_settings(root), echo=lambda _m: None) is False
