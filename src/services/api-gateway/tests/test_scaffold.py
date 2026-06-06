"""Scaffold smoke tests for the API Gateway package (AGW-IMPL-001).

These assert the package is importable, the application factory returns a
FastAPI instance, and the ``python -m custos_gateway`` CLI parses arguments
without importing the optional server stack.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI

import custos_gateway
from custos_gateway.__main__ import main
from custos_gateway.app import create_app
from custos_gateway.settings import Settings


def test_package_exposes_version() -> None:
    assert custos_gateway.__version__ == "0.1.0"


def test_create_app_returns_fastapi_instance(settings: Settings) -> None:
    app = create_app(settings=settings)
    assert isinstance(app, FastAPI)
    assert app.title == "Custos API Gateway"
    assert app.version == "0.1.0"


def test_cli_help_exits_clean(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as excinfo:
        main(["--help"])
    assert excinfo.value.code == 0
    captured = capsys.readouterr()
    assert "Custos API Gateway" in captured.out


def test_cli_rejects_unknown_argument() -> None:
    with pytest.raises(SystemExit) as excinfo:
        main(["--definitely-not-a-flag"])
    assert excinfo.value.code != 0
