"""In-memory fakes for the SPL provider Protocols used by connector-service tests.

Each fake implements the slice of the SPL Protocol surface that
connector-service Phase B actually exercises:

* ``MigrationCapable``: ``declared_revisions`` + ``refresh_declared`` so the
  schema-revision startup gate can run.
* A minimal ``append_audit`` recorder on the metadata fake so audit-emission
  unit tests can assert event shape without standing up Postgres.

The full SPL Protocol surface (``put_connector_type_version`` /
``put_connector_cursor`` / ``acquire_cursor_lease`` / etc.) is exercised by
the conformance suite under ``src/libs/custos-postgres/tests/`` and the
Phase B integration test that drives the real ``PgCatalogAdapter`` /
``PgMetadataAdapter`` against a live database.
"""

from __future__ import annotations

from collections.abc import Mapping
from collections.abc import Set as AbstractSet
from types import MappingProxyType
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from custos_spl.interfaces.metadata_store import AuditEvent


class FakeCatalogAdapter:
    """In-memory ``CatalogStoreProvider`` for wiring tests.

    Implements only the ``MigrationCapable`` surface plus enough metadata
    for the schema gate to read the ledger.
    """

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


class FakeMetadataAdapter:
    """In-memory ``MetadataStoreProvider`` for wiring tests.

    Covers the migration surface plus a minimal ``append_audit`` recorder
    so middleware audit emission can be asserted without Postgres.
    """

    SCHEMA_REVISION = 4

    def __init__(self, *, applied_revisions: AbstractSet[int] | None = None) -> None:
        self._applied: set[int] = set(
            {1, 2, 3, 4} if applied_revisions is None else applied_revisions,
        )
        self.refresh_calls = 0
        self.append_audit_calls: list[tuple[str, object]] = []

    @property
    def declared_revisions(self) -> Mapping[str, AbstractSet[int]]:
        return MappingProxyType(
            {"MetadataStoreProvider": frozenset(self._applied)},
        )

    async def apply_pending(self) -> list[str]:  # pragma: no cover - not exercised
        return []

    async def refresh_declared(self) -> None:
        self.refresh_calls += 1

    def set_applied(self, revisions: AbstractSet[int]) -> None:
        self._applied = set(revisions)

    async def append_audit(
        self,
        workspace_id: object,
        event: AuditEvent,
        tx: object = None,
    ) -> None:
        self.append_audit_calls.append((str(workspace_id), event))


__all__ = [
    "FakeCatalogAdapter",
    "FakeMetadataAdapter",
]
