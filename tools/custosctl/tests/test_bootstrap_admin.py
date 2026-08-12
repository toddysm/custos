from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from custosctl import bootstrap_admin, shell
from custosctl.cli import cli
from custosctl.config import Settings


@pytest.fixture
def runner() -> Iterator[CliRunner]:
    with CliRunner().isolated_filesystem():
        yield CliRunner()


def test_init_displays_token_only_when_requested(
    runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    token = "custos_secret-value"
    monkeypatch.setattr(bootstrap_admin, "run_ceremony", lambda *a, **kw: token)
    result = runner.invoke(cli, ["bootstrap-admin", "init", "--show-token"])
    assert result.exit_code == 0, result.output
    assert token in result.output


def test_init_redacts_token_by_default(runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    token = "custos_secret-value"
    monkeypatch.setattr(bootstrap_admin, "run_ceremony", lambda *a, **kw: token)
    result = runner.invoke(cli, ["bootstrap-admin", "init", "--keep-secret"])
    assert result.exit_code == 0, result.output
    assert token not in result.output


def test_recovery_requires_confirmation(runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    called: list[bool] = []

    def _run(*_args: object, **_kwargs: object) -> str:
        called.append(True)
        return "custos_secret"

    monkeypatch.setattr(bootstrap_admin, "run_ceremony", _run)
    result = runner.invoke(cli, ["bootstrap-admin", "recover", "--keep-secret"], input="n\n")
    assert result.exit_code != 0
    assert called == []


def test_recovery_global_yes_skips_confirmation(
    runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    called: list[bool] = []

    def _run(*_args: object, **_kwargs: object) -> str:
        called.append(True)
        return "custos_secret"

    monkeypatch.setattr(bootstrap_admin, "run_ceremony", _run)
    result = runner.invoke(cli, ["--yes", "bootstrap-admin", "recover", "--keep-secret"])
    assert result.exit_code == 0, result.output
    assert called == [True]


class _FakeClient:
    def __init__(self, events: list[str], *, fail: bool = False, **_kwargs: Any) -> None:
        self.events = events
        self.fail = fail

    def __enter__(self) -> _FakeClient:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def get(self, path: str) -> None:
        self.events.append(f"verify:{path}")
        if self.fail:
            raise RuntimeError("verification failed")


def _settings() -> Settings:
    return Settings(
        gateway="https://gateway.example",
        repo_root=Path(__file__).resolve().parents[3],
    )


def test_ceremony_cleans_up_only_after_verification(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []
    manifests: list[str] = []
    helm_sets: list[list[str]] = []
    monkeypatch.setattr(bootstrap_admin, "mint_token", lambda: "custos_secret-value")

    def _apply(manifest: str, **_kwargs: object) -> None:
        manifests.append(manifest)
        events.append("apply")

    def _helm(*_args: object, **kwargs: Any) -> None:
        helm_sets.append(kwargs["sets"])
        events.append(f"helm:{len(events)}")

    monkeypatch.setattr(shell, "kubectl_apply_stdin", _apply)
    monkeypatch.setattr(shell, "helm_install", _helm)
    monkeypatch.setattr(
        bootstrap_admin,
        "ApiClient",
        lambda **kwargs: _FakeClient(events, **kwargs),
    )
    monkeypatch.setattr(
        shell,
        "kubectl_delete_secret",
        lambda *args, **kwargs: events.append("delete"),
    )

    token = bootstrap_admin.run_ceremony(
        _settings(),
        mode=bootstrap_admin.BootstrapMode.INIT,
        show_token=True,
        keep_secret=False,
        echo=lambda _message: None,
    )

    assert token == "custos_secret-value"
    assert manifests and "custos_secret-value" in manifests[0]
    assert "custos_secret-value" not in " ".join(events)
    assert events[-1] == "delete"
    assert events.index("delete") > next(
        index for index, event in enumerate(events) if event.startswith("verify:")
    )
    assert sum(event.startswith("helm:") for event in events) == 2
    assert "bootstrap.adminToken.mode=disabled" in helm_sets[1]
    assert "bootstrap.adminToken.secretName=" in helm_sets[1]


def test_ceremony_retains_secret_and_redacts_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    monkeypatch.setattr(bootstrap_admin, "mint_token", lambda: "custos_secret-value")

    monkeypatch.setattr(
        shell,
        "kubectl_apply_stdin",
        lambda *args, **kwargs: events.append("apply"),
    )
    monkeypatch.setattr(
        shell,
        "helm_install",
        lambda *args, **kwargs: events.append("helm"),
    )
    monkeypatch.setattr(
        bootstrap_admin,
        "ApiClient",
        lambda **kwargs: _FakeClient(events, fail=True, **kwargs),
    )
    monkeypatch.setattr(
        shell,
        "kubectl_delete_secret",
        lambda *args, **kwargs: events.append("delete"),
    )

    with pytest.raises(RuntimeError) as caught:
        bootstrap_admin.run_ceremony(
            _settings(),
            mode=bootstrap_admin.BootstrapMode.RECOVER,
            show_token=True,
            keep_secret=False,
            echo=lambda _message: None,
        )

    assert "retained for recovery" in str(caught.value)
    assert "custos_secret-value" not in str(caught.value)
    assert "delete" not in events
