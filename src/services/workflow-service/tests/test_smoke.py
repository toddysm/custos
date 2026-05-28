"""Smoke tests for the workflow-service scaffold (WF-IMPL-013).

These tests assert only that the package imports cleanly and that the
scaffold's documented placeholder contract holds. Real behaviour lands in
WF-IMPL-014 onwards.
"""

from __future__ import annotations

import pytest


def test_package_imports() -> None:
    import custos_workflow

    assert hasattr(custos_workflow, "__version__")
    assert isinstance(custos_workflow.__version__, str)
    assert custos_workflow.__version__ == "0.1.0"


def test_create_app_is_scaffold_stub() -> None:
    """``create_app`` is documented to raise until WF-IMPL-015 lands."""
    import custos_workflow

    with pytest.raises(NotImplementedError, match="WF-IMPL-015"):
        custos_workflow.create_app()


def test_main_module_is_importable() -> None:
    """``python -m custos_workflow`` must be wired even pre-WF-IMPL-015."""
    import importlib

    mod = importlib.import_module("custos_workflow.__main__")
    assert callable(mod.main)
