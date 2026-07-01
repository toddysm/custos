"""Tests for the custosctl configuration model (DEVCLI-IMPL-001)."""

from __future__ import annotations

from pathlib import Path

import pytest

from custosctl.config import Settings, Target, resolve_repo_root

_CUSTOS_KEYS = (
    "CUSTOS_TARGET",
    "CUSTOS_KUBE_CONTEXT",
    "CUSTOS_CLUSTER",
    "CUSTOS_KIND_NODE_IMAGE",
    "CUSTOS_NAMESPACE",
    "CUSTOS_RELEASE",
    "CUSTOS_PROFILE",
    "CUSTOS_IMAGE_PREFIX",
    "CUSTOS_IMAGE_TAG",
    "CUSTOS_GATEWAY",
    "CUSTOS_TOKEN",
    "CUSTOS_INSECURE",
    "CUSTOS_PREREQS",
)


@pytest.fixture(autouse=True)
def _hermetic_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Clear CUSTOS_* env and run from a dir with no ``.env`` so config is hermetic."""
    for key in _CUSTOS_KEYS:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.chdir(tmp_path)


def test_defaults_target_local() -> None:
    settings = Settings()
    assert settings.target is Target.LOCAL
    assert settings.cluster == "custos-local"
    assert settings.namespace == "custos-system"
    assert settings.release == "custos"
    assert settings.image_prefix == "ghcr.io/toddysm/custos"


def test_env_overrides_with_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CUSTOS_TARGET", "remote")
    monkeypatch.setenv("CUSTOS_GATEWAY", "https://custos.example")
    monkeypatch.setenv("CUSTOS_TOKEN", "cst_secret")
    monkeypatch.setenv("CUSTOS_INSECURE", "true")
    settings = Settings()
    assert settings.target is Target.REMOTE
    assert settings.gateway == "https://custos.example"
    assert settings.token is not None
    assert settings.token.get_secret_value() == "cst_secret"
    assert settings.insecure is True


def test_token_is_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CUSTOS_TOKEN", "cst_topsecret")
    settings = Settings()
    assert "cst_topsecret" not in repr(settings)


def test_effective_kube_context_local_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CUSTOS_CLUSTER", "demo")
    settings = Settings()
    assert settings.effective_kube_context() == "kind-demo"


def test_effective_kube_context_remote_defaults_to_current(monkeypatch: pytest.MonkeyPatch) -> None:
    # For remote with no explicit context, None means "kubectl's current context".
    monkeypatch.setenv("CUSTOS_TARGET", "remote")
    settings = Settings()
    assert settings.effective_kube_context() is None


def test_effective_kube_context_explicit_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CUSTOS_KUBE_CONTEXT", "prod-ctx")
    settings = Settings()
    assert settings.effective_kube_context() == "prod-ctx"


def _make_checkout(root: Path) -> Path:
    (root / "deploy" / "helm" / "custos").mkdir(parents=True)
    (root / "deploy" / "helm" / "custos" / "Chart.yaml").write_text("name: custos\n")
    (root / "scripts").mkdir()
    (root / "scripts" / "install-prereqs.sh").write_text("#!/usr/bin/env bash\n")
    (root / "Makefile").write_text("deps:\n\t@true\n")
    return root


def test_resolve_repo_root_autodetects_from_subdir(tmp_path: Path) -> None:
    root = _make_checkout(tmp_path / "repo")
    nested = root / "a" / "b"
    nested.mkdir(parents=True)
    assert resolve_repo_root(None, start=nested) == root.resolve()


def test_resolve_repo_root_uses_configured(tmp_path: Path) -> None:
    root = _make_checkout(tmp_path / "repo")
    assert resolve_repo_root(root) == root.resolve()


def test_resolve_repo_root_configured_invalid_raises(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="does not look like a Custos checkout"):
        resolve_repo_root(tmp_path)


def test_resolve_repo_root_not_found_raises(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="could not locate the Custos repository root"):
        resolve_repo_root(None, start=tmp_path)
