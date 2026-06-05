"""Scaffold smoke tests for the Trigger Service package (TS-IMPL-001).

These assert the package is importable and the entry-point wiring is in
place. They are replaced/expanded by real unit suites as the
``TS-IMPL-*`` phases land their runtime surfaces.
"""

from __future__ import annotations

import importlib


def test_package_imports_and_exposes_version() -> None:
    module = importlib.import_module("custos_trigger")
    assert module.__version__ == "0.1.0"
    assert module.__all__ == ["__version__", "create_app"]


def test_create_app_factory_target_builds_a_fastapi_app() -> None:
    # The ``python -m custos_trigger`` / console-script entry point asks
    # uvicorn to import ``custos_trigger:create_app`` (``factory=True``).
    # Assert that target resolves and constructs a FastAPI app so the
    # entry-point wiring cannot silently break (TS-IMPL-003).
    module = importlib.import_module("custos_trigger")
    assert callable(module.create_app)
    from fastapi import FastAPI

    assert isinstance(module.create_app(), FastAPI)


def test_main_entry_point_is_callable() -> None:
    main_module = importlib.import_module("custos_trigger.__main__")
    assert callable(main_module.main)
