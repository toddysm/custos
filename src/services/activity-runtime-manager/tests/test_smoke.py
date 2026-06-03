"""Smoke tests for the Activity Runtime Manager package (ARM-IMPL-001)."""

from __future__ import annotations

import custos_arm
from custos_arm import create_app
from custos_arm._version import __version__


def test_package_imports_as_custos_arm() -> None:
    assert custos_arm.__name__ == "custos_arm"


def test_create_app_is_re_exported() -> None:
    assert callable(create_app)


def test_app_version_matches_package_version() -> None:
    app = create_app()
    assert app.version == __version__
