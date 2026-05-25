"""Smoke tests for the connector-service package.

These tests assert the package imports cleanly. The richer behavioural
tests live in :mod:`tests.test_app` (healthz/readyz + schema gate),
:mod:`tests.test_callctx` (middleware), :mod:`tests.test_providers`
(schema gate logic), and :mod:`tests.test_settings` (env parsing).
"""

from __future__ import annotations


def test_package_imports() -> None:
    import custos_connector

    assert hasattr(custos_connector, "__version__")
    assert isinstance(custos_connector.__version__, str)
    assert custos_connector.__version__ == "0.1.0"


def test_create_app_is_callable() -> None:
    """`create_app` is exposed at the package root."""
    import custos_connector

    assert callable(custos_connector.create_app)
