"""In-memory fakes for the SPL provider Protocols used by tests."""

from __future__ import annotations

from collections.abc import Mapping
from collections.abc import Set as AbstractSet
from types import MappingProxyType


class FakeDefinitionAdapter:
    """A minimal in-memory ``DefinitionStoreProvider`` for wiring tests.

    Implements only the ``MigrationCapable`` surface plus enough
    metadata for :func:`custos_spl.check_revisions` to read the
    ledger. The real adapter surface (``put_workflow_version`` etc.)
    is exercised by the conformance suite under
    ``src/libs/custos-postgres/tests/test_conformance.py``.
    """

    SCHEMA_REVISION = 1

    def __init__(self, *, applied_revisions: AbstractSet[int] | None = None) -> None:
        self._applied: set[int] = set({1} if applied_revisions is None else applied_revisions)
        self.refresh_calls = 0

    @property
    def declared_revisions(self) -> Mapping[str, AbstractSet[int]]:
        return MappingProxyType(
            {"DefinitionStoreProvider": frozenset(self._applied)},
        )

    async def apply_pending(self) -> list[str]:  # pragma: no cover - not exercised
        return []

    async def refresh_declared(self) -> None:
        self.refresh_calls += 1

    def set_applied(self, revisions: AbstractSet[int]) -> None:
        self._applied = set(revisions)


class FakeCatalogAdapter:
    """In-memory ``CatalogStoreProvider`` mirror of :class:`FakeDefinitionAdapter`."""

    SCHEMA_REVISION = 1

    def __init__(self, *, applied_revisions: AbstractSet[int] | None = None) -> None:
        self._applied: set[int] = set({1} if applied_revisions is None else applied_revisions)
        self.refresh_calls = 0

    @property
    def declared_revisions(self) -> Mapping[str, AbstractSet[int]]:
        return MappingProxyType(
            {"CatalogStoreProvider": frozenset(self._applied)},
        )

    async def apply_pending(self) -> list[str]:  # pragma: no cover - not exercised
        return []

    async def refresh_declared(self) -> None:
        self.refresh_calls += 1

    def set_applied(self, revisions: AbstractSet[int]) -> None:
        self._applied = set(revisions)


__all__ = ["FakeCatalogAdapter", "FakeDefinitionAdapter"]
