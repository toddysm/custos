"""In-memory fakes for the SPL provider Protocols used by tests."""

from __future__ import annotations

from collections.abc import Mapping
from collections.abc import Set as AbstractSet
from types import MappingProxyType
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from custos_spl.interfaces.metadata_store import AuditEvent


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


class FakeMetadataAdapter:
    """In-memory ``MetadataStoreProvider`` mirror used by wiring tests.

    Covers the migration surface plus a minimal ``append_audit`` hook
    so the catalog audit module can be unit-tested without depending
    on Postgres. The full Protocol surface (runs, steps, triggers,
    cursors, idempotency, leases, audit reads) is exercised by the
    conformance suite under
    ``src/libs/custos-postgres/tests/test_conformance.py``.
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

    async def append_audit(self, workspace_id: object, event: object, tx: object = None) -> None:
        self.append_audit_calls.append((str(workspace_id), event))


class FakeMetadataStore:
    """Compact audit-only fake used by manager-level unit tests.

    Stores every :meth:`append_audit` call so tests can assert event
    type, actor, subject, and payload. The schema-revision surface is
    populated so the same fake is also usable as a
    ``MetadataStoreProvider`` in :func:`load_providers`-style wiring
    tests; the richer wiring fake is :class:`FakeMetadataAdapter`.
    """

    SCHEMA_REVISION = 4

    def __init__(self) -> None:
        self.audit: list[AuditEvent] = []
        self.raise_on_append: BaseException | None = None
        self._applied: set[int] = {1, 2, 3, 4}

    @property
    def declared_revisions(self) -> Mapping[str, AbstractSet[int]]:
        return MappingProxyType(
            {"MetadataStoreProvider": frozenset(self._applied)},
        )

    async def apply_pending(self) -> list[str]:  # pragma: no cover
        return []

    async def refresh_declared(self) -> None:
        return None

    async def append_audit(
        self,
        workspace_id: object,
        event: AuditEvent,
        tx: object = None,
    ) -> None:
        if self.raise_on_append is not None:
            raise self.raise_on_append
        self.audit.append(event)


__all__ = [
    "FakeCatalogAdapter",
    "FakeDefinitionAdapter",
    "FakeMetadataAdapter",
    "FakeMetadataStore",
]
