"""Smoke tests for the catalog-service package (CS-IMPL-001 scaffold + CS-IMPL-003/004 wiring).

These tests assert the package imports cleanly and that the
:func:`create_app` factory returns a FastAPI instance with the
Phase B middleware + probes wired. Per-component behaviour is
covered in dedicated test modules (``test_providers.py``,
``test_callctx.py``, ``test_app.py``).
"""

from __future__ import annotations

import importlib


def test_package_imports() -> None:
    import custos_catalog

    assert hasattr(custos_catalog, "__version__")
    assert isinstance(custos_catalog.__version__, str)
    assert custos_catalog.__version__ == "0.1.0"


def test_create_app_builds_a_fastapi_instance() -> None:
    """``create_app`` is now a working factory (CS-IMPL-003 / CS-IMPL-004)."""
    from fastapi import FastAPI

    import custos_catalog
    from custos_catalog.providers import Providers
    from custos_catalog.settings import load_settings
    from tests._fakes import FakeCatalogAdapter, FakeDefinitionAdapter, FakeMetadataAdapter

    settings = load_settings(
        {
            "CAT_DEFINITION_STORE": "postgresql://u:p@h:5432/def",
            "CAT_CATALOG_STORE": "postgresql://u:p@h:5432/cat",
            "CAT_METADATA_STORE": "postgresql://u:p@h:5432/meta",
            "CAT_CONNECTOR_ENDPOINT": "http://connector-service:8080",
        },
    )
    providers = Providers(
        definition_store=FakeDefinitionAdapter(),  # type: ignore[arg-type]
        catalog_store=FakeCatalogAdapter(),  # type: ignore[arg-type]
        metadata_store=FakeMetadataAdapter(),  # type: ignore[arg-type]
    )
    app = custos_catalog.create_app(settings=settings, providers=providers)
    assert isinstance(app, FastAPI)


def test_main_module_is_importable() -> None:
    """``python -m custos_catalog`` must be wired."""
    mod = importlib.import_module("custos_catalog.__main__")
    assert callable(mod.main)
