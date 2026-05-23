"""Smoke tests for the catalog-service scaffold (CS-IMPL-001).

These tests assert only that the package imports cleanly and that the
scaffold's documented placeholder contract holds. Real behaviour lands in
CS-IMPL-003 onwards.
"""

from __future__ import annotations

import pytest


def test_package_imports() -> None:
    import custos_catalog

    assert hasattr(custos_catalog, "__version__")
    assert isinstance(custos_catalog.__version__, str)
    assert custos_catalog.__version__ == "0.1.0"


def test_create_app_is_scaffold_stub() -> None:
    """``create_app`` is documented to raise until CS-IMPL-017 lands."""
    import custos_catalog

    with pytest.raises(NotImplementedError, match="CS-IMPL-017"):
        custos_catalog.create_app()


def test_main_module_is_importable() -> None:
    """``python -m custos_catalog`` must be wired even pre-CS-IMPL-017."""
    import importlib

    mod = importlib.import_module("custos_catalog.__main__")
    assert callable(mod.main)
