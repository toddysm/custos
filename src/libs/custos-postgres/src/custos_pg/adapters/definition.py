"""Postgres `DefinitionStoreProvider` adapter.

Write-once contract on version rows: `put_*_version` translates the
Postgres unique-violation `23505` into `ImmutableViolation` so callers
get the SPL-typed error rather than a driver-specific one.

Pagination uses keyset cursors. The `Cursor.token` is a base64-encoded
`(published_at_iso, version)` tuple so paging is stable even when many
rows share the same `published_at`.

Migration: revision 1 owns the four tables in the `definition` schema
plus the parent rows. `apply_pending()` is idempotent — every DDL is
`CREATE … IF NOT EXISTS` and the ledger insert uses `ON CONFLICT`.

The adapter accepts either an `asyncpg.Pool` (tests, with a live
testcontainers instance) or a `LazyPool` (production, deferred
construction so the synchronous SPL CLI factory can build the adapter
outside an event loop).
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

from custos_spl.errors import BackendUnavailable, ImmutableViolation
from custos_spl.interfaces.definition_store import (
    DefinitionListFilter,
    WorkflowTemplateVersion,
    WorkflowVersion,
)
from custos_spl.pagination import Cursor, Page

from custos_pg.migrations.definition import DEFINITION_REV1
from custos_pg.pool import DSN_ENV_VAR, LazyPool
from custos_pg.revisions import ensure_ledger, read_declared, record_revision

if TYPE_CHECKING:
    from asyncpg import Record
    from asyncpg.pool import Pool

INTERFACE_NAME = "DefinitionStoreProvider"
DEFAULT_LIST_LIMIT = 50
MAX_LIST_LIMIT = 200


def _encode_cursor(published_at: datetime, version: str) -> Cursor:
    raw = json.dumps([published_at.isoformat(), version]).encode("utf-8")
    return Cursor(token=base64.urlsafe_b64encode(raw).decode("ascii"))


def _decode_cursor(cursor: Cursor) -> tuple[datetime, str]:
    raw = base64.urlsafe_b64decode(cursor.token.encode("ascii"))
    published_at_iso, version = json.loads(raw)
    return datetime.fromisoformat(published_at_iso), str(version)


def _row_to_workflow_version(row: Record, parent_deprecated: bool) -> WorkflowVersion:
    doc = row["normalized_doc"]
    if isinstance(doc, (str, bytes, bytearray)):
        doc = json.loads(doc)
    return WorkflowVersion(
        workspace_id=row["workspace_id"],
        workflow_id=row["workflow_id"],
        version=row["version"],
        normalized_doc=MappingProxyType(dict(doc)),
        derived_from_template_version_id=row["derived_from_template_version_id"],
        parent_deprecated=parent_deprecated,
        published_at=row["published_at"],
    )


def _row_to_template_version(row: Record, parent_deprecated: bool) -> WorkflowTemplateVersion:
    doc = row["normalized_doc"]
    if isinstance(doc, (str, bytes, bytearray)):
        doc = json.loads(doc)
    return WorkflowTemplateVersion(
        workspace_id=row["workspace_id"],
        template_id=row["template_id"],
        version=row["version"],
        normalized_doc=MappingProxyType(dict(doc)),
        derived_from_workflow_version_id=row["derived_from_workflow_version_id"],
        parent_deprecated=parent_deprecated,
        published_at=row["published_at"],
    )


def _resolve_limit(limit: int | None) -> int:
    if limit is None:
        return DEFAULT_LIST_LIMIT
    if limit < 1:
        return DEFAULT_LIST_LIMIT
    return min(limit, MAX_LIST_LIMIT)


class PgDefinitionAdapter:
    """asyncpg implementation of `DefinitionStoreProvider`.

    Tests should construct directly with `pool=`; production code
    receives an instance from `make_adapter()` which sets `lazy=` so
    the pool is built inside the CLI's event loop.
    """

    SCHEMA_REVISION = 1

    def __init__(
        self,
        pool: Pool | None = None,
        *,
        lazy: LazyPool | None = None,
    ) -> None:
        if pool is None and lazy is None:
            raise ValueError(
                "PgDefinitionAdapter requires either `pool` or `lazy`."
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
        # Synchronous by Protocol contract — return the cached snapshot
        # populated by `apply_pending()` / `refresh_declared()`.
        return MappingProxyType({INTERFACE_NAME: frozenset(self._applied_revisions)})

    async def refresh_declared(self) -> None:
        """Re-read the ledger and update the cached `declared_revisions`."""
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
            for rev in (DEFINITION_REV1,):
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

    # ----- Workflow versions -----

    async def put_workflow_version(
        self,
        workspace_id: str,
        workflow_id: str,
        version: str,
        normalized_doc: Mapping[str, Any],
        derived_from_template_version_id: str | None = None,
    ) -> WorkflowVersion:
        pool = await self._pool_ref()
        try:
            async with pool.acquire() as conn, conn.transaction():
                await conn.execute(
                    "INSERT INTO definition.workflow (workspace_id, workflow_id) "
                    "VALUES ($1, $2) ON CONFLICT DO NOTHING",
                    workspace_id,
                    workflow_id,
                )
                row = await conn.fetchrow(
                    "INSERT INTO definition.workflow_version "
                    "(workspace_id, workflow_id, version, normalized_doc, "
                    "derived_from_template_version_id) "
                    "VALUES ($1, $2, $3, $4::jsonb, $5) "
                    "RETURNING workspace_id, workflow_id, version, normalized_doc, "
                    "derived_from_template_version_id, published_at",
                    workspace_id,
                    workflow_id,
                    version,
                    json.dumps(dict(normalized_doc)),
                    derived_from_template_version_id,
                )
                parent_deprecated = await conn.fetchval(
                    "SELECT deprecated FROM definition.workflow "
                    "WHERE workspace_id = $1 AND workflow_id = $2",
                    workspace_id,
                    workflow_id,
                )
        except Exception as exc:
            raise self._classify(exc) from exc
        assert row is not None
        return _row_to_workflow_version(row, bool(parent_deprecated))

    async def get_workflow_version(
        self,
        workspace_id: str,
        workflow_id: str,
        version: str,
    ) -> WorkflowVersion | None:
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT v.workspace_id, v.workflow_id, v.version, v.normalized_doc, "
                "v.derived_from_template_version_id, v.published_at, w.deprecated "
                "FROM definition.workflow_version v "
                "JOIN definition.workflow w "
                "  ON w.workspace_id = v.workspace_id AND w.workflow_id = v.workflow_id "
                "WHERE v.workspace_id = $1 AND v.workflow_id = $2 AND v.version = $3",
                workspace_id,
                workflow_id,
                version,
            )
        if row is None:
            return None
        return _row_to_workflow_version(row, bool(row["deprecated"]))

    async def list_workflow_versions(
        self,
        workspace_id: str,
        workflow_id: str,
        filter: DefinitionListFilter | None = None,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[WorkflowVersion]:
        eff_limit = _resolve_limit(limit)
        params: list[Any] = [workspace_id, workflow_id]
        where = ["v.workspace_id = $1", "v.workflow_id = $2"]
        if filter is not None and filter.published_after is not None:
            params.append(filter.published_after)
            where.append(f"v.published_at >= ${len(params)}")
        if filter is not None and filter.published_before is not None:
            params.append(filter.published_before)
            where.append(f"v.published_at < ${len(params)}")
        if cursor is not None:
            ts, ver = _decode_cursor(cursor)
            params.extend([ts, ver])
            where.append(
                f"(v.published_at, v.version) < (${len(params) - 1}, ${len(params)})"
            )
        params.append(eff_limit + 1)
        sql = (
            "SELECT v.workspace_id, v.workflow_id, v.version, v.normalized_doc, "
            "v.derived_from_template_version_id, v.published_at, w.deprecated "
            "FROM definition.workflow_version v "
            "JOIN definition.workflow w "
            "  ON w.workspace_id = v.workspace_id AND w.workflow_id = v.workflow_id "
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
        items = [_row_to_workflow_version(r, bool(r["deprecated"])) for r in rows]
        return Page(items=items, next_cursor=next_cursor)

    async def get_latest_workflow_version(
        self,
        workspace_id: str,
        workflow_id: str,
    ) -> WorkflowVersion | None:
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT v.workspace_id, v.workflow_id, v.version, v.normalized_doc, "
                "v.derived_from_template_version_id, v.published_at, w.deprecated "
                "FROM definition.workflow_version v "
                "JOIN definition.workflow w "
                "  ON w.workspace_id = v.workspace_id AND w.workflow_id = v.workflow_id "
                "WHERE v.workspace_id = $1 AND v.workflow_id = $2 "
                "ORDER BY v.published_at DESC, v.version DESC LIMIT 1",
                workspace_id,
                workflow_id,
            )
        if row is None:
            return None
        return _row_to_workflow_version(row, bool(row["deprecated"]))

    async def set_workflow_deprecated(
        self,
        workspace_id: str,
        workflow_id: str,
        deprecated: bool,
    ) -> None:
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO definition.workflow (workspace_id, workflow_id, deprecated) "
                "VALUES ($1, $2, $3) "
                "ON CONFLICT (workspace_id, workflow_id) "
                "DO UPDATE SET deprecated = EXCLUDED.deprecated",
                workspace_id,
                workflow_id,
                deprecated,
            )

    # ----- Workflow template versions (mirror) -----

    async def put_workflow_template_version(
        self,
        workspace_id: str,
        template_id: str,
        version: str,
        normalized_doc: Mapping[str, Any],
        derived_from_workflow_version_id: str | None = None,
    ) -> WorkflowTemplateVersion:
        pool = await self._pool_ref()
        try:
            async with pool.acquire() as conn, conn.transaction():
                await conn.execute(
                    "INSERT INTO definition.workflow_template (workspace_id, template_id) "
                    "VALUES ($1, $2) ON CONFLICT DO NOTHING",
                    workspace_id,
                    template_id,
                )
                row = await conn.fetchrow(
                    "INSERT INTO definition.workflow_template_version "
                    "(workspace_id, template_id, version, normalized_doc, "
                    "derived_from_workflow_version_id) "
                    "VALUES ($1, $2, $3, $4::jsonb, $5) "
                    "RETURNING workspace_id, template_id, version, normalized_doc, "
                    "derived_from_workflow_version_id, published_at",
                    workspace_id,
                    template_id,
                    version,
                    json.dumps(dict(normalized_doc)),
                    derived_from_workflow_version_id,
                )
                parent_deprecated = await conn.fetchval(
                    "SELECT deprecated FROM definition.workflow_template "
                    "WHERE workspace_id = $1 AND template_id = $2",
                    workspace_id,
                    template_id,
                )
        except Exception as exc:
            raise self._classify(exc) from exc
        assert row is not None
        return _row_to_template_version(row, bool(parent_deprecated))

    async def get_workflow_template_version(
        self,
        workspace_id: str,
        template_id: str,
        version: str,
    ) -> WorkflowTemplateVersion | None:
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT v.workspace_id, v.template_id, v.version, v.normalized_doc, "
                "v.derived_from_workflow_version_id, v.published_at, t.deprecated "
                "FROM definition.workflow_template_version v "
                "JOIN definition.workflow_template t "
                "  ON t.workspace_id = v.workspace_id AND t.template_id = v.template_id "
                "WHERE v.workspace_id = $1 AND v.template_id = $2 AND v.version = $3",
                workspace_id,
                template_id,
                version,
            )
        if row is None:
            return None
        return _row_to_template_version(row, bool(row["deprecated"]))

    async def list_workflow_template_versions(
        self,
        workspace_id: str,
        template_id: str,
        filter: DefinitionListFilter | None = None,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[WorkflowTemplateVersion]:
        eff_limit = _resolve_limit(limit)
        params: list[Any] = [workspace_id, template_id]
        where = ["v.workspace_id = $1", "v.template_id = $2"]
        if filter is not None and filter.published_after is not None:
            params.append(filter.published_after)
            where.append(f"v.published_at >= ${len(params)}")
        if filter is not None and filter.published_before is not None:
            params.append(filter.published_before)
            where.append(f"v.published_at < ${len(params)}")
        if cursor is not None:
            ts, ver = _decode_cursor(cursor)
            params.extend([ts, ver])
            where.append(
                f"(v.published_at, v.version) < (${len(params) - 1}, ${len(params)})"
            )
        params.append(eff_limit + 1)
        sql = (
            "SELECT v.workspace_id, v.template_id, v.version, v.normalized_doc, "
            "v.derived_from_workflow_version_id, v.published_at, t.deprecated "
            "FROM definition.workflow_template_version v "
            "JOIN definition.workflow_template t "
            "  ON t.workspace_id = v.workspace_id AND t.template_id = v.template_id "
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
        items = [_row_to_template_version(r, bool(r["deprecated"])) for r in rows]
        return Page(items=items, next_cursor=next_cursor)

    async def set_workflow_template_deprecated(
        self,
        workspace_id: str,
        template_id: str,
        deprecated: bool,
    ) -> None:
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO definition.workflow_template "
                "(workspace_id, template_id, deprecated) VALUES ($1, $2, $3) "
                "ON CONFLICT (workspace_id, template_id) "
                "DO UPDATE SET deprecated = EXCLUDED.deprecated",
                workspace_id,
                template_id,
                deprecated,
            )

    # ----- Error classification -----

    @staticmethod
    def _classify(exc: BaseException) -> Exception:
        """Map asyncpg driver errors to SPL-typed errors.

        - `23505` unique-violation on a version PK → `ImmutableViolation`
          per the write-once contract.
        - Connection-level failures → `BackendUnavailable` (transient).
        - Anything else is re-raised unchanged via `from exc` upstream.
        """
        sqlstate = getattr(exc, "sqlstate", None)
        if sqlstate == "23505":
            return ImmutableViolation(str(exc))
        cls_name = type(exc).__name__
        if "Connection" in cls_name or "Interface" in cls_name:
            return BackendUnavailable(str(exc), cause=exc)
        return exc if isinstance(exc, Exception) else RuntimeError(str(exc))


def make_adapter() -> PgDefinitionAdapter:
    """Entry-point factory (synchronous).

    Captures `CUSTOS_PG_DSN` at call time and returns an adapter whose
    pool is built lazily on first async use. The SPL CLI invokes this
    outside an event loop and then awaits adapter methods.
    """
    dsn = os.environ.get(DSN_ENV_VAR)
    if not dsn:
        raise RuntimeError(
            f"{DSN_ENV_VAR} is not set; cannot construct Postgres pool. "
            "Set it to a libpq DSN such as "
            "'postgresql://user:pw@host:5432/custos'."
        )
    return PgDefinitionAdapter(lazy=LazyPool(dsn))


__all__ = ["INTERFACE_NAME", "PgDefinitionAdapter", "make_adapter"]
