"""Unit tests for the ``custos-migrate-job`` wrapper.

The job delegates to the SPL CLI; these tests stub
``custos_spl.migrations.cli.main`` so the success and abort (revision
mismatch) paths are exercised without a live Postgres, plus the DSN-resolution
and missing-DSN behaviours.
"""

from __future__ import annotations

import os

import pytest
from custos_spl.migrations import cli as spl_cli

from custos_migrate import main, resolve_dsn


def test_resolve_dsn_prefers_explicit() -> None:
    env = {
        "CUSTOS_PG_DSN": "postgresql://u:p@explicit:5432/custos",
        "DATABASE_URL": "postgresql://u:p@fallback:5432/custos",
        "uri": "postgresql://u:p@cnpg:5432/custos",
    }
    assert resolve_dsn(env) == "postgresql://u:p@explicit:5432/custos"


def test_resolve_dsn_database_url_fallback() -> None:
    env = {"DATABASE_URL": "postgresql://u:p@fallback:5432/custos"}
    assert resolve_dsn(env) == "postgresql://u:p@fallback:5432/custos"


def test_resolve_dsn_cnpg_uri_fallback() -> None:
    env = {"uri": "postgresql://u:p@cnpg:5432/custos"}
    assert resolve_dsn(env) == "postgresql://u:p@cnpg:5432/custos"


def test_resolve_dsn_none_when_absent() -> None:
    assert resolve_dsn({}) is None


def test_main_success_path(monkeypatch: pytest.MonkeyPatch) -> None:
    """`migrate up` succeeds → exit 0, DSN exported, CLI called with ['up']."""
    monkeypatch.setenv("CUSTOS_PG_DSN", "postgresql://u:p@db:5432/custos")
    calls: list[list[str]] = []

    def fake_cli(argv: list[str]) -> int:
        calls.append(argv)
        return 0

    monkeypatch.setattr(spl_cli, "main", fake_cli)

    assert main([]) == 0
    assert calls == [["up"]]


def test_main_abort_on_revision_mismatch(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remaining revision gap → SPL CLI returns 1, job propagates non-zero."""
    monkeypatch.setenv("CUSTOS_PG_DSN", "postgresql://u:p@db:5432/custos")

    def fake_cli(argv: list[str]) -> int:
        return 1

    monkeypatch.setattr(spl_cli, "main", fake_cli)

    assert main([]) == 1


def test_main_missing_dsn_returns_nonzero(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """No DSN resolvable → exit 1 without invoking the SPL CLI."""
    monkeypatch.delenv("CUSTOS_PG_DSN", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("uri", raising=False)

    called = False

    def fake_cli(argv: list[str]) -> int:
        nonlocal called
        called = True
        return 0

    monkeypatch.setattr(spl_cli, "main", fake_cli)

    assert main([]) == 1
    assert called is False
    assert "no Postgres DSN available" in capsys.readouterr().err


def test_main_exports_fallback_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    """A `uri`-only environment is exported as CUSTOS_PG_DSN before delegating."""
    monkeypatch.delenv("CUSTOS_PG_DSN", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("uri", "postgresql://u:p@cnpg:5432/custos")
    seen: dict[str, str | None] = {}

    def fake_cli(argv: list[str]) -> int:
        seen["dsn"] = os.environ.get("CUSTOS_PG_DSN")
        return 0

    monkeypatch.setattr(spl_cli, "main", fake_cli)

    assert main([]) == 0
    assert seen["dsn"] == "postgresql://u:p@cnpg:5432/custos"


def test_main_passes_through_adapter_and_check(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CUSTOS_PG_DSN", "postgresql://u:p@db:5432/custos")
    calls: list[list[str]] = []

    def fake_cli(argv: list[str]) -> int:
        calls.append(argv)
        return 0

    monkeypatch.setattr(spl_cli, "main", fake_cli)

    assert main(["--adapter", "postgres-metadata", "--check"]) == 0
    assert calls == [["up", "--adapter", "postgres-metadata", "--check"]]
