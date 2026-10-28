"""Regression coverage for conformance fixtures without a running container engine."""

from __future__ import annotations

import inspect
from types import TracebackType
from typing import Self

import pytest

from custos_spl.conformance import fixtures
from custos_spl.conformance.base import AdapterConformanceBase


class _ReadyContainer:
    """Model the public context-manager API, deliberately omitting removed helpers."""

    def __init__(self, **kwargs: object) -> None:
        """Record construction without contacting a container engine."""
        self.started = False

    def __enter__(self) -> Self:
        """Represent testcontainers' startup and readiness wait."""
        self.started = True
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Release the fixture-owned container."""
        self.started = False

    def get_connection_url(self) -> str:
        """Return a non-secret test URL through the supported public API."""
        return "postgresql://localhost/conformance"


def test_postgres_fixture_uses_context_manager_readiness(monkeypatch: pytest.MonkeyPatch) -> None:
    """The fixture must not call the nonexistent get_connection_client method."""
    monkeypatch.setattr(fixtures, "PostgresContainer", _ReadyContainer)
    iterator = inspect.unwrap(fixtures.postgres_container)()
    container = next(iterator)
    assert isinstance(container, _ReadyContainer)
    assert container.started
    assert container.get_connection_url() == "postgresql://localhost/conformance"
    iterator.close()
    assert not container.started


def test_schema_revision_check_executes_instead_of_skipping() -> None:
    """Provided adapters must expose a nonnegative integer schema revision."""
    suite = AdapterConformanceBase()
    for value in (0, 1):
        adapter = type("Adapter", (), {"SCHEMA_REVISION": value})()
        suite.test_adapter_has_schema_revision(adapter)
    for invalid_revision in (None, -1, True, "1"):
        adapter = type("Adapter", (), {"SCHEMA_REVISION": invalid_revision})()
        with pytest.raises(AssertionError):
            suite.test_adapter_has_schema_revision(adapter)
