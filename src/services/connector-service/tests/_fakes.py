"""In-memory fakes for the SPL provider Protocols used by connector-service tests.

Each fake implements the slice of the SPL Protocol surface that
connector-service Phase B and Phase D actually exercise:

* ``MigrationCapable``: ``declared_revisions`` + ``refresh_declared`` so the
  schema-revision startup gate can run.
* A minimal ``append_audit`` recorder on the metadata fake so audit-emission
  unit tests can assert event shape without standing up Postgres.
* The connector-type registry methods (``put_connector_type_version``,
  ``get_connector_type_version``, ``list_connector_type_versions``,
  ``set_connector_type_deprecated``) that the Plugin Loader (CONN-IMPL-008)
  drives. The fake enforces SPL's ``ConflictDigest`` contract on
  identical-key digest divergence.

The full SPL Protocol surface (cursors, leases, audit outbox, etc.) is
exercised by the conformance suite under ``src/libs/custos-postgres/tests/``
and the Phase B integration test that drives the real ``PgCatalogAdapter`` /
``PgMetadataAdapter`` against a live database.
"""

from __future__ import annotations

from collections.abc import Mapping
from collections.abc import Set as AbstractSet
from datetime import UTC, datetime
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

from custos_spl import ConflictDigest
from custos_spl.interfaces.catalog_store import ConnectorTypeVersion
from custos_spl.pagination import Cursor, Page

if TYPE_CHECKING:
    from custos_spl.interfaces.metadata_store import AuditEvent


class FakeCatalogAdapter:
    """In-memory ``CatalogStoreProvider`` for connector-service tests.

    Implements the ``MigrationCapable`` surface plus the connector-type
    registry slice the Plugin Loader (CONN-IMPL-008) drives.
    """

    SCHEMA_REVISION = 1

    def __init__(self, *, applied_revisions: AbstractSet[int] | None = None) -> None:
        self._applied: set[int] = set({1} if applied_revisions is None else applied_revisions)
        self.refresh_calls = 0
        # Per-(type, version) row store + parent-type deprecation flag.
        # The parent_deprecated flag is denormalised onto every returned
        # row to mirror SPL's read-side contract.
        self._rows: dict[tuple[str, str], ConnectorTypeVersion] = {}
        self._deprecated_types: set[str] = set()

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

    # ------------------------------------------------------------------
    # ConnectorTypeVersion surface (Plugin Loader, CONN-IMPL-008)
    # ------------------------------------------------------------------

    async def put_connector_type_version(
        self,
        type: str,
        version: str,
        digest: str,
        normalized_manifest: Mapping[str, Any],
    ) -> ConnectorTypeVersion:
        """Idempotent on identical ``(type, version, digest)``; raises
        :class:`ConflictDigest` on digest divergence."""
        key = (type, version)
        existing = self._rows.get(key)
        if existing is not None and existing.digest != digest:
            raise ConflictDigest(
                f"connector_type=({type!r},{version!r}) already registered "
                f"with digest={existing.digest!r}; refused new digest={digest!r}"
            )
        if existing is not None:
            # Idempotent re-put: refresh the parent_deprecated read.
            row = ConnectorTypeVersion(
                type=existing.type,
                version=existing.version,
                digest=existing.digest,
                normalized_manifest=existing.normalized_manifest,
                parent_deprecated=type in self._deprecated_types,
                published_at=existing.published_at,
            )
            self._rows[key] = row
            return row
        row = ConnectorTypeVersion(
            type=type,
            version=version,
            digest=digest,
            normalized_manifest=dict(normalized_manifest),
            parent_deprecated=type in self._deprecated_types,
            published_at=datetime.now(UTC),
        )
        self._rows[key] = row
        return row

    async def get_connector_type_version(
        self,
        type: str,
        version: str,
    ) -> ConnectorTypeVersion | None:
        row = self._rows.get((type, version))
        if row is None:
            return None
        # Refresh denormalised parent_deprecated on every read so a
        # deprecate-then-get returns the updated flag without needing
        # to re-put.
        return ConnectorTypeVersion(
            type=row.type,
            version=row.version,
            digest=row.digest,
            normalized_manifest=row.normalized_manifest,
            parent_deprecated=type in self._deprecated_types,
            published_at=row.published_at,
        )

    async def list_connector_type_versions(
        self,
        type: str,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[ConnectorTypeVersion]:
        # Deterministic order by version string ASC so callers can
        # assert order without a tie-breaker dance.
        all_rows = [
            ConnectorTypeVersion(
                type=row.type,
                version=row.version,
                digest=row.digest,
                normalized_manifest=row.normalized_manifest,
                parent_deprecated=type in self._deprecated_types,
                published_at=row.published_at,
            )
            for (t, _), row in sorted(self._rows.items())
            if t == type
        ]
        # Page through using a simple offset encoded in the cursor's
        # opaque token. Production SPL uses an opaque keyset cursor;
        # the loader's contract here only cares that next_cursor is
        # ``None`` once the walk completes.
        start = 0
        if cursor is not None:
            try:
                start = int(cursor.token)
            except ValueError:  # pragma: no cover - defensive
                start = 0
        end = len(all_rows) if limit is None else min(start + limit, len(all_rows))
        next_cursor = Cursor(token=str(end)) if end < len(all_rows) else None
        return Page(items=all_rows[start:end], next_cursor=next_cursor)

    async def set_connector_type_deprecated(
        self,
        type: str,
        deprecated: bool,
    ) -> None:
        if deprecated:
            self._deprecated_types.add(type)
        else:
            self._deprecated_types.discard(type)


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
