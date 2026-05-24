"""In-memory fakes for the SPL provider Protocols used by auth-service tests."""

from __future__ import annotations

from collections.abc import Mapping
from collections.abc import Set as AbstractSet
from types import MappingProxyType


class FakeAuthAdapter:
    """A minimal in-memory ``AuthStoreProvider`` for wiring tests.

    Implements only the ``MigrationCapable`` surface plus enough
    metadata for :func:`custos_spl.check_revisions` to read the
    ledger. The real adapter surface (``put_principal`` etc.) is
    exercised by the SPL contract suite in
    ``src/libs/storage-provider-layer/tests/test_auth_store.py`` and
    by the integration suite in
    ``src/libs/custos-postgres/tests/test_integration.py``.
    """

    SCHEMA_REVISION = 1

    def __init__(self, *, applied_revisions: AbstractSet[int] | None = None) -> None:
        self._applied: set[int] = set({1} if applied_revisions is None else applied_revisions)
        self.refresh_calls = 0

    @property
    def declared_revisions(self) -> Mapping[str, AbstractSet[int]]:
        return MappingProxyType(
            {"AuthStoreProvider": frozenset(self._applied)},
        )

    async def apply_pending(self) -> list[str]:  # pragma: no cover - not exercised
        return []

    async def refresh_declared(self) -> None:
        self.refresh_calls += 1

    def set_applied(self, revisions: AbstractSet[int]) -> None:
        self._applied = set(revisions)


class FakeMetadataAdapter:
    """In-memory ``MetadataStoreProvider`` mirror used by auth-service wiring tests.

    Covers the migration surface plus a minimal ``append_audit`` hook so
    auth-service audit modules can be unit-tested without depending on
    Postgres. The full Protocol surface (runs, steps, triggers, cursors,
    idempotency, leases, audit reads) is exercised by the conformance
    suite under ``src/libs/custos-postgres/tests/test_integration.py``.
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


__all__ = [
    "FakeAuthAdapter",
    "FakeMetadataAdapter",
]
