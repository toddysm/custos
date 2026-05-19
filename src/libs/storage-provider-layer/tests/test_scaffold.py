"""Smoke test confirming the package is importable and CI is wired correctly."""

import custos_spl


def test_package_imports() -> None:
    assert custos_spl.__version__ == "0.1.0"


def test_subpackages_importable() -> None:
    from custos_spl import adapters, interfaces, middleware, migrations  # noqa: F401
