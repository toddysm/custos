"""Smoke tests for the Workflow Service package surface (WF-IMPL-015).

These tests assert only the package-level invariants: the package
imports cleanly, ``__version__`` is the expected literal, ``create_app``
is exported and returns a real FastAPI instance, and the
``python -m custos_workflow`` entrypoint is importable. Behaviour is
covered by the focused test modules (``test_app.py``,
``test_healthz.py``, ``test_call_context.py``).
"""

from __future__ import annotations


def test_package_imports() -> None:
    import custos_workflow

    assert hasattr(custos_workflow, "__version__")
    assert isinstance(custos_workflow.__version__, str)
    assert custos_workflow.__version__ == "0.1.0"


def test_create_app_is_exported_and_returns_a_fastapi_instance() -> None:
    """``create_app`` is the canonical entry point used by ASGI servers."""
    from fastapi import FastAPI

    import custos_workflow

    app = custos_workflow.create_app(require_call_context=False)
    assert isinstance(app, FastAPI)


def test_main_module_is_importable() -> None:
    """``python -m custos_workflow`` must keep working as the entrypoint."""
    import importlib

    mod = importlib.import_module("custos_workflow.__main__")
    assert callable(mod.main)
