"""Tests for the seed-ootb wrapper + sample workflow fixture (DEVCLI-IMPL-008)."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path

import pytest
import yaml

from custosctl import seed, shell
from custosctl.config import Settings, Target
from custosctl.fixtures import sample_workflow_path


def _checkout(tmp_path: Path) -> Path:
    (tmp_path / "deploy" / "helm" / "custos").mkdir(parents=True)
    (tmp_path / "deploy" / "helm" / "custos" / "Chart.yaml").write_text("name: custos\n")
    (tmp_path / "scripts").mkdir()
    (tmp_path / "scripts" / "install-prereqs.sh").write_text("#!/usr/bin/env bash\n")
    (tmp_path / "scripts" / "seed-ootb.sh").write_text("#!/usr/bin/env bash\n")
    (tmp_path / "Makefile").write_text("deps:\n\t@true\n")
    return tmp_path


def _settings(root: Path, **overrides: object) -> Settings:
    base: dict[str, object] = {
        "target": Target.REMOTE,
        "repo_root": root,
        "gateway": "https://gw.example",
        "token": "cst_secret",
        "image_prefix": "ghcr.io/acme/custos",
    }
    base.update(overrides)
    return Settings(**base)  # type: ignore[arg-type]


class _RunRecorder:
    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.argv: list[str] = []
        self.cwd: object = None
        self.env: dict[str, str] = {}

        def _run(
            argv: Sequence[str],
            *,
            cwd: object = None,
            env: Mapping[str, str] | None = None,
            **_kw: object,
        ) -> None:
            self.argv = list(argv)
            self.cwd = cwd
            self.env = dict(env) if env else {}

        monkeypatch.setattr(shell, "run", _run)


def test_seed_runs_script_with_gateway_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _checkout(tmp_path)
    rec = _RunRecorder(monkeypatch)
    seed.seed_ootb(_settings(root))

    assert rec.argv[0].endswith("scripts/seed-ootb.sh")
    assert "--allow-existing" not in rec.argv
    assert str(rec.cwd) == str(root)
    assert rec.env["GATEWAY"] == "https://gw.example"
    assert rec.env["TOKEN"] == "cst_secret"
    assert rec.env["IMAGE_PREFIX"] == "ghcr.io/acme/custos"
    assert "INSECURE" not in rec.env
    # inherited env is preserved.
    assert "PATH" in rec.env


def test_seed_allow_existing_and_insecure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _checkout(tmp_path)
    rec = _RunRecorder(monkeypatch)
    seed.seed_ootb(_settings(root, insecure=True), allow_existing=True)
    assert rec.argv[-1] == "--allow-existing"
    assert rec.env["INSECURE"] == "1"


def test_seed_requires_gateway(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _checkout(tmp_path)
    _RunRecorder(monkeypatch)
    with pytest.raises(RuntimeError, match="CUSTOS_GATEWAY is required"):
        seed.seed_ootb(_settings(root, gateway=None))


def test_seed_requires_token(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _checkout(tmp_path)
    _RunRecorder(monkeypatch)
    with pytest.raises(RuntimeError, match="CUSTOS_TOKEN is required"):
        seed.seed_ootb(_settings(root, token=None))


def test_seed_script_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _checkout(tmp_path)
    (root / "scripts" / "seed-ootb.sh").unlink()
    _RunRecorder(monkeypatch)
    # resolve_repo_root uses Chart.yaml/install-prereqs.sh/Makefile markers, so the
    # checkout is still valid even without seed-ootb.sh; the wrapper checks it too.
    with pytest.raises(RuntimeError, match="seed-ootb script not found"):
        seed.seed_ootb(_settings(root))


def test_seed_maps_oserror_to_runtimeerror(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _checkout(tmp_path)

    def _raise(*_a: object, **_k: object) -> None:
        raise PermissionError(13, "Permission denied")

    monkeypatch.setattr(shell, "run", _raise)
    with pytest.raises(RuntimeError, match="could not run"):
        seed.seed_ootb(_settings(root))


# --- fixture --------------------------------------------------------------


def test_sample_workflow_is_valid_yaml_copy_image() -> None:
    path = sample_workflow_path()
    assert path.is_file()
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert doc["kind"] == "Workflow"
    step = doc["spec"]["steps"][0]
    assert step["activity"] == "custos.builtin/copy-image@0"
    assert set(step["connectors"]) == {"source", "dest"}
