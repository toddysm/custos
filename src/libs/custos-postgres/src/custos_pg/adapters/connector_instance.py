"""Postgres `ConnectorInstanceStoreProvider` adapter.

Workspace-scoped. Every method takes `workspace_id` as its first arg
and all SQL `WHERE` clauses pin it as a constant prefix, so the
adapter cannot leak rows across workspaces by construction.

Put semantics
-------------

* `put_connector_instance` is **create-only**: re-puts on an existing
  `(workspace_id, instance_id)` raise :class:`ImmutableViolation`,
  even when the row contents are byte-identical. Idempotence is the
  service layer's job — the adapter refuses to silently merge.

* `patch_connector_instance` applies a partial update to mutable
  fields and bumps `updated_at` to `now()`. Unknown keys in `updates`
  raise :class:`ValueError` so a typo in the service layer surfaces
  immediately rather than silently no-op-ing.

* `get_connector_instance` returns `None` on absent — both for "row
  does not exist anywhere" and for "row exists in another workspace".
  Disclosing cross-workspace existence would violate the SPL
  workspace-scoping contract.
"""

from __future__ import annotations

import base64
import json
from collections.abc import Mapping
from collections.abc import Set as AbstractSet
from datetime import UTC, datetime
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, ClassVar, Final

from custos_spl.errors import BackendUnavailable, ImmutableViolation
from custos_spl.ids import ConnectorInstanceId, WorkspaceId
from custos_spl.interfaces.connector_instance_store import (
    ConnectorInstance,
    ConnectorInstanceFilter,
)
from custos_spl.pagination import Cursor, Page

from custos_pg.migrations.connector_instance import CONNECTOR_INSTANCE_REV1
from custos_pg.pool import LazyPool, read_dsn_from_env
from custos_pg.revisions import ensure_ledger, read_declared, record_revision

if TYPE_CHECKING:
    from asyncpg import Record
    from asyncpg.pool import Pool

INTERFACE_NAME: Final[str] = "ConnectorInstanceStoreProvider"
DEFAULT_LIST_LIMIT: Final[int] = 50
MAX_LIST_LIMIT: Final[int] = 200

#: Mutable columns the adapter accepts on :meth:`patch_connector_instance`.
#: Anything outside this set raises :class:`ValueError`. ``status`` and
#: ``health_status`` are present so CONN-IMPL-013 can drive activation
#: + health transitions through the same patch path without revisiting
#: the adapter contract.
_PATCHABLE_COLUMNS: Final[frozenset[str]] = frozenset(
    {"name", "lease_ttl_seconds", "enabled", "status", "health_status"}
)


def _encode_cursor(created_at: datetime, instance_id: str) -> Cursor:
    raw = json.dumps([created_at.isoformat(), instance_id]).encode("utf-8")
    return Cursor(token=base64.urlsafe_b64encode(raw).decode("ascii"))


def _decode_cursor(cursor: Cursor) -> tuple[datetime, str]:
    raw = base64.urlsafe_b64decode(cursor.token.encode("ascii"))
    created_at_iso, instance_id = json.loads(raw)
    return datetime.fromisoformat(created_at_iso), str(instance_id)


def _row_to_instance(row: Record) -> ConnectorInstance:
    return ConnectorInstance(
        workspace_id=WorkspaceId(row["workspace_id"]),
        instance_id=ConnectorInstanceId(row["instance_id"]),
        type=row["type"],
        version=row["version"],
        name=row["name"],
        lease_ttl_seconds=row["lease_ttl_seconds"],
        enabled=bool(row["enabled"]),
        status=row["status"],
        health_status=row["health_status"],
        target_config=_json_payload(row["target_config"]) or {},
        credentials_authentication=_json_payload(row["credentials_authentication"]) or {},
        used_capabilities=_decode_capabilities(_json_payload(row["used_capabilities"])),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _json_payload(value: Any) -> Any:
    """Decode a JSONB column that asyncpg surfaces as either a Python
    object (when a codec is registered) or a JSON string."""

    if value is None:
        return None
    if isinstance(value, (str, bytes, bytearray)):
        return json.loads(value)
    return value


def _decode_capabilities(value: Any) -> tuple[str, ...] | None:
    if value is None:
        return None
    return tuple(str(item) for item in value)


def _resolve_limit(limit: int | None) -> int:
    if limit is None:
        return DEFAULT_LIST_LIMIT
    if limit < 1:
        return DEFAULT_LIST_LIMIT
    return min(limit, MAX_LIST_LIMIT)


class PgConnectorInstanceAdapter:
    """asyncpg implementation of `ConnectorInstanceStoreProvider`."""

    SCHEMA_REVISION: ClassVar[int] = 1

    def __init__(
        self,
        pool: Pool | None = None,
        *,
        lazy: LazyPool | None = None,
    ) -> None:
        if pool is None and lazy is None:
            raise ValueError("PgConnectorInstanceAdapter requires either `pool` or `lazy`.")
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
                "SELECT revision FROM custos_meta.adapter_revisions WHERE interface_name = $1",
                INTERFACE_NAME,
            )
            already = {int(r["revision"]) for r in applied}
            for rev in (CONNECTOR_INSTANCE_REV1,):
                if rev.number in already:
                    continue
                for stmt in rev.statements:
                    await conn.execute(stmt)
                await record_revision(conn, INTERFACE_NAME, rev.number)
                summaries.append(
                    f"applied {INTERFACE_NAME} rev{rev.number} ({len(rev.statements)} statements)"
                )
        await self.refresh_declared()
        return summaries

    # ----- CRUD -----

    async def put_connector_instance(
        self,
        workspace_id: WorkspaceId,
        instance: ConnectorInstance,
    ) -> ConnectorInstance:
        if instance.workspace_id != workspace_id:
            # The protocol's first arg is the source of truth for
            # workspace scoping; a mismatch on the dataclass field is
            # almost certainly a service-layer bug we'd rather surface
            # loudly than silently rebind.
            raise ValueError(
                f"instance.workspace_id {instance.workspace_id!r} does not match "
                f"workspace_id arg {workspace_id!r}"
            )
        pool = await self._pool_ref()
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    "INSERT INTO connector_instance.connector_instance "
                    "(workspace_id, instance_id, type, version, name, "
                    " lease_ttl_seconds, enabled, status, health_status, "
                    " target_config, credentials_authentication, used_capabilities, "
                    " created_at, updated_at) "
                    "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, "
                    "        $10::jsonb, $11::jsonb, $12::jsonb, $13, $14) "
                    "ON CONFLICT (workspace_id, instance_id) DO NOTHING "
                    "RETURNING workspace_id, instance_id, type, version, name, "
                    "lease_ttl_seconds, enabled, status, health_status, "
                    "target_config, credentials_authentication, used_capabilities, "
                    "created_at, updated_at",
                    str(workspace_id),
                    str(instance.instance_id),
                    instance.type,
                    instance.version,
                    instance.name,
                    instance.lease_ttl_seconds,
                    instance.enabled,
                    instance.status,
                    instance.health_status,
                    json.dumps(dict(instance.target_config)),
                    json.dumps(dict(instance.credentials_authentication)),
                    json.dumps(list(instance.used_capabilities))
                    if instance.used_capabilities is not None
                    else None,
                    instance.created_at,
                    instance.updated_at,
                )
                if row is None:
                    raise ImmutableViolation(
                        f"connector_instance "
                        f"({workspace_id}, {instance.instance_id}) already exists; "
                        f"use patch_connector_instance to update"
                    )
        except ImmutableViolation:
            raise
        except Exception as exc:
            raise self._classify(exc) from exc
        return _row_to_instance(row)

    async def get_connector_instance(
        self,
        workspace_id: WorkspaceId,
        instance_id: ConnectorInstanceId,
    ) -> ConnectorInstance | None:
        pool = await self._pool_ref()
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT workspace_id, instance_id, type, version, name, "
                    "lease_ttl_seconds, enabled, status, health_status, "
                    "target_config, credentials_authentication, used_capabilities, "
                    "created_at, updated_at "
                    "FROM connector_instance.connector_instance "
                    "WHERE workspace_id = $1 AND instance_id = $2",
                    str(workspace_id),
                    str(instance_id),
                )
        except Exception as exc:
            raise self._classify(exc) from exc
        if row is None:
            return None
        return _row_to_instance(row)

    async def patch_connector_instance(
        self,
        workspace_id: WorkspaceId,
        instance_id: ConnectorInstanceId,
        updates: Mapping[str, Any],
    ) -> ConnectorInstance | None:
        unknown = set(updates) - _PATCHABLE_COLUMNS
        if unknown:
            raise ValueError(
                f"unknown patch fields: {sorted(unknown)!r}; "
                f"allowed: {sorted(_PATCHABLE_COLUMNS)!r}"
            )
        # Build the SET clause from whatever the caller supplied. We
        # always touch updated_at so a no-field-supplied call still
        # behaves like a refresh.
        set_fragments: list[str] = []
        params: list[Any] = []
        for column in sorted(updates):
            params.append(updates[column])
            set_fragments.append(f"{column} = ${len(params)}")
        params.append(datetime.now(UTC))
        set_fragments.append(f"updated_at = ${len(params)}")
        params.append(str(workspace_id))
        ws_idx = len(params)
        params.append(str(instance_id))
        id_idx = len(params)
        sql = (
            "UPDATE connector_instance.connector_instance SET "
            f"{', '.join(set_fragments)} "
            f"WHERE workspace_id = ${ws_idx} AND instance_id = ${id_idx} "
            "RETURNING workspace_id, instance_id, type, version, name, "
            "lease_ttl_seconds, enabled, status, health_status, "
            "target_config, credentials_authentication, used_capabilities, "
            "created_at, updated_at"
        )
        pool = await self._pool_ref()
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(sql, *params)
        except Exception as exc:
            raise self._classify(exc) from exc
        if row is None:
            # "Not in this workspace" and "does not exist anywhere"
            # surface identically as `None` per the SPL protocol;
            # disclosing cross-workspace existence would violate
            # the workspace-scoping contract.
            return None
        return _row_to_instance(row)

    async def list_connector_instances(
        self,
        workspace_id: WorkspaceId,
        filter: ConnectorInstanceFilter | None = None,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[ConnectorInstance]:
        eff_limit = _resolve_limit(limit)
        where: list[str] = ["workspace_id = $1"]
        params: list[Any] = [str(workspace_id)]
        if filter is not None:
            if filter.type is not None:
                params.append(filter.type)
                where.append(f"type = ${len(params)}")
            if filter.enabled is not None:
                params.append(filter.enabled)
                where.append(f"enabled = ${len(params)}")
        if cursor is not None:
            ts, iid = _decode_cursor(cursor)
            params.append(ts)
            params.append(iid)
            # Lexicographic tuple comparison on (created_at DESC,
            # instance_id ASC). DESC on created_at flips the
            # inequality direction relative to a naive `<`; we
            # express the boundary as a CASE over equal timestamps
            # so the secondary tie-breaker stays ASC and pages don't
            # skip rows that share a microsecond.
            where.append(
                f"(created_at < ${len(params) - 1} "
                f"OR (created_at = ${len(params) - 1} "
                f"AND instance_id > ${len(params)}))"
            )
        params.append(eff_limit + 1)
        sql = (
            "SELECT workspace_id, instance_id, type, version, name, "
            "lease_ttl_seconds, enabled, status, health_status, "
            "target_config, credentials_authentication, used_capabilities, "
            "created_at, updated_at "
            "FROM connector_instance.connector_instance "
            f"WHERE {' AND '.join(where)} "
            "ORDER BY created_at DESC, instance_id ASC "
            f"LIMIT ${len(params)}"
        )
        pool = await self._pool_ref()
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch(sql, *params)
        except Exception as exc:
            raise self._classify(exc) from exc
        next_cursor: Cursor | None = None
        if len(rows) > eff_limit:
            last = rows[eff_limit - 1]
            next_cursor = _encode_cursor(last["created_at"], last["instance_id"])
            rows = rows[:eff_limit]
        items = [_row_to_instance(r) for r in rows]
        return Page(items=items, next_cursor=next_cursor)

    # ----- Error classification -----

    @staticmethod
    def _classify(exc: BaseException) -> Exception:
        cls_name = type(exc).__name__
        if "Connection" in cls_name or "Interface" in cls_name:
            return BackendUnavailable(str(exc), cause=exc)
        return exc if isinstance(exc, Exception) else RuntimeError(str(exc))


def make_adapter() -> PgConnectorInstanceAdapter:
    """Entry-point factory (synchronous). See definition.make_adapter."""
    return PgConnectorInstanceAdapter(lazy=LazyPool(read_dsn_from_env()))


__all__ = [
    "INTERFACE_NAME",
    "PgConnectorInstanceAdapter",
    "make_adapter",
]
