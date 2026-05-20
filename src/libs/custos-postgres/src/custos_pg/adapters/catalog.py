"""Postgres `CatalogStoreProvider` adapter.

Platform-wide (no `workspace_id`). Put semantics:

  - Re-puts with the same `(key, digest)` are idempotent and return
    the existing row.
  - Re-puts with a different digest on the same key raise
    `ConflictDigest` (subclass of `ImmutableViolation`).

`resolve(namespace, type, semver_range)` uses
`packaging.specifiers.SpecifierSet` to filter, then picks the latest
matching `Version`. Deprecated parents return `None`. The catalog is
expected to be modest in cardinality per `(namespace, type)`; we load
candidate rows and filter in Python rather than encoding semver
ordering in SQL.
"""

from __future__ import annotations

import base64
import json
import os
from collections.abc import Mapping
from collections.abc import Set as AbstractSet
from datetime import datetime
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

from custos_spl.errors import BackendUnavailable, ConflictDigest
from custos_spl.interfaces.catalog_store import ActivityTypeVersion, ConnectorTypeVersion
from custos_spl.pagination import Cursor, Page
from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.version import InvalidVersion, Version

from custos_pg.migrations.catalog import CATALOG_REV1
from custos_pg.pool import DSN_ENV_VAR, LazyPool
from custos_pg.revisions import ensure_ledger, read_declared, record_revision

if TYPE_CHECKING:
    from asyncpg import Record
    from asyncpg.pool import Pool

INTERFACE_NAME = "CatalogStoreProvider"
DEFAULT_LIST_LIMIT = 50
MAX_LIST_LIMIT = 200


def _encode_cursor(published_at: datetime, version: str) -> Cursor:
    raw = json.dumps([published_at.isoformat(), version]).encode("utf-8")
    return Cursor(token=base64.urlsafe_b64encode(raw).decode("ascii"))


def _decode_cursor(cursor: Cursor) -> tuple[datetime, str]:
    raw = base64.urlsafe_b64decode(cursor.token.encode("ascii"))
    published_at_iso, version = json.loads(raw)
    return datetime.fromisoformat(published_at_iso), str(version)


def _row_to_activity(row: Record, parent_deprecated: bool) -> ActivityTypeVersion:
    manifest = row["normalized_manifest"]
    if isinstance(manifest, (str, bytes, bytearray)):
        manifest = json.loads(manifest)
    return ActivityTypeVersion(
        namespace=row["namespace"],
        type=row["type"],
        version=row["version"],
        digest=row["digest"],
        normalized_manifest=MappingProxyType(dict(manifest)),
        parent_deprecated=parent_deprecated,
        published_at=row["published_at"],
    )


def _row_to_connector(row: Record, parent_deprecated: bool) -> ConnectorTypeVersion:
    manifest = row["normalized_manifest"]
    if isinstance(manifest, (str, bytes, bytearray)):
        manifest = json.loads(manifest)
    return ConnectorTypeVersion(
        type=row["type"],
        version=row["version"],
        digest=row["digest"],
        normalized_manifest=MappingProxyType(dict(manifest)),
        parent_deprecated=parent_deprecated,
        published_at=row["published_at"],
    )


def _resolve_limit(limit: int | None) -> int:
    if limit is None:
        return DEFAULT_LIST_LIMIT
    if limit < 1:
        return DEFAULT_LIST_LIMIT
    return min(limit, MAX_LIST_LIMIT)


class PgCatalogAdapter:
    """asyncpg implementation of `CatalogStoreProvider`."""

    SCHEMA_REVISION = 1

    def __init__(
        self,
        pool: Pool | None = None,
        *,
        lazy: LazyPool | None = None,
    ) -> None:
        if pool is None and lazy is None:
            raise ValueError(
                "PgCatalogAdapter requires either `pool` or `lazy`."
            )
        self._pool: Pool | None = pool
        self._lazy = lazy
        self._applied_revisions: set[int] = set()

    async def _pool_ref(self) -> Pool:
        if self._pool is None:
            assert self._lazy is not None
            self._pool = await self._lazy.get()
        return self._pool

    # ----- MigrationCapable -----

    @property
    def declared_revisions(self) -> Mapping[str, AbstractSet[int]]:
        return MappingProxyType({INTERFACE_NAME: frozenset(self._applied_revisions)})

    async def refresh_declared(self) -> None:
        pool = await self._pool_ref()
        declared = await read_declared(pool, (INTERFACE_NAME,))
        self._applied_revisions = set(declared.get(INTERFACE_NAME, set()))

    async def apply_pending(self) -> list[str]:
        summaries: list[str] = []
        pool = await self._pool_ref()
        async with pool.acquire() as conn, conn.transaction():
            await ensure_ledger(conn)
            applied = await conn.fetch(
                "SELECT revision FROM custos_meta.adapter_revisions "
                "WHERE interface_name = $1",
                INTERFACE_NAME,
            )
            already = {int(r["revision"]) for r in applied}
            for rev in (CATALOG_REV1,):
                if rev.number in already:
                    continue
                for stmt in rev.statements:
                    await conn.execute(stmt)
                await record_revision(conn, INTERFACE_NAME, rev.number)
                summaries.append(
                    f"applied {INTERFACE_NAME} rev{rev.number} "
                    f"({len(rev.statements)} statements)"
                )
        await self.refresh_declared()
        return summaries

    # ----- Activity types -----

    async def put_activity_type_version(
        self,
        namespace: str,
        type: str,
        version: str,
        digest: str,
        normalized_manifest: Mapping[str, Any],
    ) -> ActivityTypeVersion:
        pool = await self._pool_ref()
        try:
            async with pool.acquire() as conn, conn.transaction():
                await conn.execute(
                    "INSERT INTO catalog.activity_type (namespace, type) "
                    "VALUES ($1, $2) ON CONFLICT DO NOTHING",
                    namespace,
                    type,
                )
                existing = await conn.fetchrow(
                    "SELECT namespace, type, version, digest, normalized_manifest, published_at "
                    "FROM catalog.activity_type_version "
                    "WHERE namespace = $1 AND type = $2 AND version = $3 "
                    "FOR UPDATE",
                    namespace,
                    type,
                    version,
                )
                if existing is not None:
                    if existing["digest"] != digest:
                        raise ConflictDigest(
                            f"activity_type {namespace}/{type}@{version} already "
                            f"published with digest {existing['digest']!r}; "
                            f"refusing re-put with {digest!r}"
                        )
                    row = existing
                else:
                    row = await conn.fetchrow(
                        "INSERT INTO catalog.activity_type_version "
                        "(namespace, type, version, digest, normalized_manifest) "
                        "VALUES ($1, $2, $3, $4, $5::jsonb) "
                        "RETURNING namespace, type, version, digest, normalized_manifest, "
                        "published_at",
                        namespace,
                        type,
                        version,
                        digest,
                        json.dumps(dict(normalized_manifest)),
                    )
                parent_deprecated = await conn.fetchval(
                    "SELECT deprecated FROM catalog.activity_type "
                    "WHERE namespace = $1 AND type = $2",
                    namespace,
                    type,
                )
        except ConflictDigest:
            raise
        except Exception as exc:
            raise self._classify(exc) from exc
        assert row is not None
        return _row_to_activity(row, bool(parent_deprecated))

    async def get_activity_type_version(
        self,
        namespace: str,
        type: str,
        version: str,
    ) -> ActivityTypeVersion | None:
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT v.namespace, v.type, v.version, v.digest, v.normalized_manifest, "
                "v.published_at, a.deprecated "
                "FROM catalog.activity_type_version v "
                "JOIN catalog.activity_type a "
                "  ON a.namespace = v.namespace AND a.type = v.type "
                "WHERE v.namespace = $1 AND v.type = $2 AND v.version = $3",
                namespace,
                type,
                version,
            )
        if row is None:
            return None
        return _row_to_activity(row, bool(row["deprecated"]))

    async def list_activity_type_versions(
        self,
        namespace: str,
        type: str,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[ActivityTypeVersion]:
        eff_limit = _resolve_limit(limit)
        params: list[Any] = [namespace, type]
        where = ["v.namespace = $1", "v.type = $2"]
        if cursor is not None:
            ts, ver = _decode_cursor(cursor)
            params.extend([ts, ver])
            where.append(
                f"(v.published_at, v.version) < (${len(params) - 1}, ${len(params)})"
            )
        params.append(eff_limit + 1)
        sql = (
            "SELECT v.namespace, v.type, v.version, v.digest, v.normalized_manifest, "
            "v.published_at, a.deprecated "
            "FROM catalog.activity_type_version v "
            "JOIN catalog.activity_type a "
            "  ON a.namespace = v.namespace AND a.type = v.type "
            f"WHERE {' AND '.join(where)} "
            "ORDER BY v.published_at DESC, v.version DESC "
            f"LIMIT ${len(params)}"
        )
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
        next_cursor: Cursor | None = None
        if len(rows) > eff_limit:
            last = rows[eff_limit - 1]
            next_cursor = _encode_cursor(last["published_at"], last["version"])
            rows = rows[:eff_limit]
        items = [_row_to_activity(r, bool(r["deprecated"])) for r in rows]
        return Page(items=items, next_cursor=next_cursor)

    async def set_activity_type_deprecated(
        self,
        namespace: str,
        type: str,
        deprecated: bool,
    ) -> None:
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO catalog.activity_type (namespace, type, deprecated) "
                "VALUES ($1, $2, $3) "
                "ON CONFLICT (namespace, type) "
                "DO UPDATE SET deprecated = EXCLUDED.deprecated",
                namespace,
                type,
                deprecated,
            )

    # ----- Connector types -----

    async def put_connector_type_version(
        self,
        type: str,
        version: str,
        digest: str,
        normalized_manifest: Mapping[str, Any],
    ) -> ConnectorTypeVersion:
        pool = await self._pool_ref()
        try:
            async with pool.acquire() as conn, conn.transaction():
                await conn.execute(
                    "INSERT INTO catalog.connector_type (type) VALUES ($1) "
                    "ON CONFLICT DO NOTHING",
                    type,
                )
                existing = await conn.fetchrow(
                    "SELECT type, version, digest, normalized_manifest, published_at "
                    "FROM catalog.connector_type_version "
                    "WHERE type = $1 AND version = $2 FOR UPDATE",
                    type,
                    version,
                )
                if existing is not None:
                    if existing["digest"] != digest:
                        raise ConflictDigest(
                            f"connector_type {type}@{version} already published "
                            f"with digest {existing['digest']!r}; refusing "
                            f"re-put with {digest!r}"
                        )
                    row = existing
                else:
                    row = await conn.fetchrow(
                        "INSERT INTO catalog.connector_type_version "
                        "(type, version, digest, normalized_manifest) "
                        "VALUES ($1, $2, $3, $4::jsonb) "
                        "RETURNING type, version, digest, normalized_manifest, published_at",
                        type,
                        version,
                        digest,
                        json.dumps(dict(normalized_manifest)),
                    )
                parent_deprecated = await conn.fetchval(
                    "SELECT deprecated FROM catalog.connector_type WHERE type = $1",
                    type,
                )
        except ConflictDigest:
            raise
        except Exception as exc:
            raise self._classify(exc) from exc
        assert row is not None
        return _row_to_connector(row, bool(parent_deprecated))

    async def get_connector_type_version(
        self,
        type: str,
        version: str,
    ) -> ConnectorTypeVersion | None:
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT v.type, v.version, v.digest, v.normalized_manifest, "
                "v.published_at, c.deprecated "
                "FROM catalog.connector_type_version v "
                "JOIN catalog.connector_type c ON c.type = v.type "
                "WHERE v.type = $1 AND v.version = $2",
                type,
                version,
            )
        if row is None:
            return None
        return _row_to_connector(row, bool(row["deprecated"]))

    async def list_connector_type_versions(
        self,
        type: str,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[ConnectorTypeVersion]:
        eff_limit = _resolve_limit(limit)
        params: list[Any] = [type]
        where = ["v.type = $1"]
        if cursor is not None:
            ts, ver = _decode_cursor(cursor)
            params.extend([ts, ver])
            where.append(
                f"(v.published_at, v.version) < (${len(params) - 1}, ${len(params)})"
            )
        params.append(eff_limit + 1)
        sql = (
            "SELECT v.type, v.version, v.digest, v.normalized_manifest, "
            "v.published_at, c.deprecated "
            "FROM catalog.connector_type_version v "
            "JOIN catalog.connector_type c ON c.type = v.type "
            f"WHERE {' AND '.join(where)} "
            "ORDER BY v.published_at DESC, v.version DESC "
            f"LIMIT ${len(params)}"
        )
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
        next_cursor: Cursor | None = None
        if len(rows) > eff_limit:
            last = rows[eff_limit - 1]
            next_cursor = _encode_cursor(last["published_at"], last["version"])
            rows = rows[:eff_limit]
        items = [_row_to_connector(r, bool(r["deprecated"])) for r in rows]
        return Page(items=items, next_cursor=next_cursor)

    async def set_connector_type_deprecated(
        self,
        type: str,
        deprecated: bool,
    ) -> None:
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO catalog.connector_type (type, deprecated) VALUES ($1, $2) "
                "ON CONFLICT (type) DO UPDATE SET deprecated = EXCLUDED.deprecated",
                type,
                deprecated,
            )

    # ----- Resolution -----

    async def resolve(
        self,
        namespace: str,
        type: str,
        semver_range: str,
    ) -> ActivityTypeVersion | None:
        # Parse the spec up front so a bad input is a programming error,
        # not a database round-trip.
        try:
            spec = SpecifierSet(semver_range)
        except InvalidSpecifier:
            return None
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            parent = await conn.fetchrow(
                "SELECT deprecated FROM catalog.activity_type "
                "WHERE namespace = $1 AND type = $2",
                namespace,
                type,
            )
            if parent is None or bool(parent["deprecated"]):
                return None
            rows = await conn.fetch(
                "SELECT namespace, type, version, digest, normalized_manifest, published_at "
                "FROM catalog.activity_type_version "
                "WHERE namespace = $1 AND type = $2",
                namespace,
                type,
            )
        best_row: Record | None = None
        best_version: Version | None = None
        for row in rows:
            try:
                v = Version(row["version"])
            except InvalidVersion:
                # Non-PEP-440 versions cannot be ordered against a
                # SpecifierSet; skip them silently. Catalog Service is
                # expected to reject bad versions upstream.
                continue
            if v not in spec:
                continue
            if best_version is None or v > best_version:
                best_version = v
                best_row = row
        if best_row is None:
            return None
        return _row_to_activity(best_row, parent_deprecated=False)

    # ----- Error classification -----

    @staticmethod
    def _classify(exc: BaseException) -> Exception:
        cls_name = type(exc).__name__
        if "Connection" in cls_name or "Interface" in cls_name:
            return BackendUnavailable(str(exc), cause=exc)
        return exc if isinstance(exc, Exception) else RuntimeError(str(exc))


def make_adapter() -> PgCatalogAdapter:
    """Entry-point factory (synchronous). See definition.make_adapter."""
    dsn = os.environ.get(DSN_ENV_VAR)
    if not dsn:
        raise RuntimeError(
            f"{DSN_ENV_VAR} is not set; cannot construct Postgres pool. "
            "Set it to a libpq DSN such as "
            "'postgresql://user:pw@host:5432/custos'."
        )
    return PgCatalogAdapter(lazy=LazyPool(dsn))


__all__ = ["INTERFACE_NAME", "PgCatalogAdapter", "make_adapter"]
