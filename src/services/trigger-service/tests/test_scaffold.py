"""Scaffold smoke tests for the Trigger Service package (TS-IMPL-001).

These assert the package is importable and the entry-point wiring is in
place. They are replaced/expanded by real unit suites as the
``TS-IMPL-*`` phases land their runtime surfaces.
"""

from __future__ import annotations

import importlib

import pytest


def test_package_imports_and_exposes_version() -> None:
    module = importlib.import_module("custos_trigger")
    assert module.__version__ == "0.1.0"
    assert module.__all__ == ["__version__", "create_app"]


def test_create_app_factory_target_exists_and_is_stubbed() -> None:
    # The ``python -m custos_trigger`` / console-script entry point asks
    # uvicorn to import ``custos_trigger:create_app`` (``factory=True``).
    # Assert that target resolves so the entry-point wiring cannot silently
    # break; it is a scaffold stub raising ``NotImplementedError`` until the
    # FastAPI skeleton lands in TS-IMPL-003.
    module = importlib.import_module("custos_trigger")
    assert callable(module.create_app)
    with pytest.raises(NotImplementedError):
        module.create_app()


def test_main_entry_point_is_callable() -> None:
    main_module = importlib.import_module("custos_trigger.__main__")
    assert callable(main_module.main)
