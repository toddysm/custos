"""Postgres `LeaseStoreProvider` adapter.

Workspace-scoped. Every method takes ``workspace_id`` as its first arg
and all SQL ``WHERE`` clauses pin it as a constant prefix, so the
adapter cannot leak rows across workspaces by construction.

Lifecycle
---------

* :meth:`put_lease` is **create-only**: re-puts on an existing
  ``(workspace_id, lease_id)`` raise :class:`ImmutableViolation`,
  even when the row contents are byte-identical. The Lease Manager
  mints a fresh ULID per ``issue`` so a duplicate put is a
  service-layer bug worth surfacing loudly.

* :meth:`refresh_lease` updates only ``expires_at`` and bumps
  ``updated_at`` to ``now()``. Returns the post-refresh row, or
  ``None`` when no live row exists for ``(workspace_id, lease_id)``
  (both for "row does not exist anywhere", "row exists in another
  workspace", and "row exists but is already released" — the last is
  the Lease Manager's clean refusal to extend a released lease).

* :meth:`release_lease` marks the row released without deleting it
  and is idempotent. A subsequent release returns the original row
  with its first-release timestamp unchanged.

* :meth:`count_active_for_step_attempt` powers the per-step-attempt
  cap check. ``as_of`` is supplied by the caller so the Lease
  Manager's clock seam threads through to the cap.

* :meth:`get_lease` returns ``None`` on absent — both for "row does
  not exist anywhere" and for "row exists in another workspace".
  Disclosing cross-workspace existence would violate the SPL
  workspace-scoping contract.
"""

from __future__ import annotations

import base64
import json
from collections.abc import Mapping
from collections.abc import Set as AbstractSet
from datetime import datetime
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, ClassVar, Final

from custos_spl.errors import BackendUnavailable, ImmutableViolation
from custos_spl.ids import ConnectorInstanceId, RunId, StepId, WorkspaceId
from custos_spl.interfaces.lease_store import Lease, LeaseFilter
from custos_spl.pagination import Cursor, Page

from custos_pg.migrations.lease import LEASE_REV1
from custos_pg.pool import LazyPool, read_dsn_from_env
from custos_pg.revisions import ensure_ledger, read_declared, record_revision

if TYPE_CHECKING:
    from asyncpg import Record
    from asyncpg.pool import Pool

INTERFACE_NAME: Final[str] = "LeaseStoreProvider"
DEFAULT_LIST_LIMIT: Final[int] = 50
MAX_LIST_LIMIT: Final[int] = 200


def _encode_cursor(issued_at: datetime, lease_id: str) -> Cursor:
    raw = json.dumps([issued_at.isoformat(), lease_id]).encode("utf-8")
    return Cursor(token=base64.urlsafe_b64encode(raw).decode("ascii"))


def _decode_cursor(cursor: Cursor) -> tuple[datetime, str]:
    raw = base64.urlsafe_b64decode(cursor.token.encode("ascii"))
    issued_at_iso, lease_id = json.loads(raw)
    return datetime.fromisoformat(issued_at_iso), str(lease_id)


def _row_to_lease(row: Record) -> Lease:
    return Lease(
        workspace_id=WorkspaceId(row["workspace_id"]),
        lease_id=str(row["lease_id"]),
        run_id=RunId(row["run_id"]),
        step_id=StepId(row["step_id"]),
        attempt=int(row["attempt"]),
        slot=row["slot"],
        capability=row["capability"],
        connector_instance_id=ConnectorInstanceId(row["connector_instance_id"]),
        token_type=row["token_type"],
        issued_at=row["issued_at"],
        expires_at=row["expires_at"],
        released_at=row["released_at"],
        revoked_at=row["revoked_at"],
        revoke_reason=row["revoke_reason"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _resolve_limit(limit: int | None) -> int:
    if limit is None:
        return DEFAULT_LIST_LIMIT
    if limit < 1:
        return DEFAULT_LIST_LIMIT
    return min(limit, MAX_LIST_LIMIT)


class PgLeaseAdapter:
    """asyncpg implementation of `LeaseStoreProvider`."""

    SCHEMA_REVISION: ClassVar[int] = 1

    def __init__(
        self,
        pool: Pool | None = None,
        *,
        lazy: LazyPool | None = None,
    ) -> None:
        if pool is None and lazy is None:
            raise ValueError("PgLeaseAdapter requires either `pool` or `lazy`.")
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
            for rev in (LEASE_REV1,):
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

    async def put_lease(
        self,
        workspace_id: WorkspaceId,
        lease: Lease,
    ) -> Lease:
        if lease.workspace_id != workspace_id:
            # Same belt-and-braces guard PgConnectorInstanceAdapter applies:
            # the protocol's first arg is the source of truth for workspace
            # scoping; a mismatch on the dataclass field is almost certainly
            # a service-layer bug we'd rather surface loudly than silently
            # rebind.
            raise ValueError(
                f"lease.workspace_id {lease.workspace_id!r} does not match "
                f"workspace_id arg {workspace_id!r}"
            )
        pool = await self._pool_ref()
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    "INSERT INTO lease.lease "
                    "(workspace_id, lease_id, run_id, step_id, attempt, slot, "
                    " capability, connector_instance_id, token_type, "
                    " issued_at, expires_at, released_at, revoked_at, "
                    " revoke_reason, created_at, updated_at) "
                    "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, "
                    "        $12, $13, $14, $15, $16) "
                    "ON CONFLICT (workspace_id, lease_id) DO NOTHING "
                    "RETURNING workspace_id, lease_id, run_id, step_id, attempt, "
                    "slot, capability, connector_instance_id, token_type, "
                    "issued_at, expires_at, released_at, revoked_at, "
                    "revoke_reason, created_at, updated_at",
                    str(workspace_id),
                    lease.lease_id,
                    str(lease.run_id),
                    str(lease.step_id),
                    lease.attempt,
                    lease.slot,
                    lease.capability,
                    str(lease.connector_instance_id),
                    lease.token_type,
                    lease.issued_at,
                    lease.expires_at,
                    lease.released_at,
                    lease.revoked_at,
                    lease.revoke_reason,
                    lease.created_at,
                    lease.updated_at,
                )
                if row is None:
                    raise ImmutableViolation(
                        f"lease ({workspace_id}, {lease.lease_id}) already exists; "
                        f"use refresh_lease to extend or release_lease to close"
                    )
        except ImmutableViolation:
            raise
        except Exception as exc:
            raise self._classify(exc) from exc
        return _row_to_lease(row)

    async def get_lease(
        self,
        workspace_id: WorkspaceId,
        lease_id: str,
    ) -> Lease | None:
        pool = await self._pool_ref()
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT workspace_id, lease_id, run_id, step_id, attempt, "
                    "slot, capability, connector_instance_id, token_type, "
                    "issued_at, expires_at, released_at, revoked_at, "
                    "revoke_reason, created_at, updated_at "
                    "FROM lease.lease "
                    "WHERE workspace_id = $1 AND lease_id = $2",
                    str(workspace_id),
                    lease_id,
                )
        except Exception as exc:
            raise self._classify(exc) from exc
        if row is None:
            return None
        return _row_to_lease(row)

    async def refresh_lease(
        self,
        workspace_id: WorkspaceId,
        lease_id: str,
        new_expires_at: datetime,
    ) -> Lease | None:
        pool = await self._pool_ref()
        try:
            async with pool.acquire() as conn:
                # Refusing released rows in the WHERE clause keeps the
                # adapter contract tight: refresh-after-release returns
                # None just like refresh of an unknown id, so the Lease
                # Manager does not need to fetch+check.
                row = await conn.fetchrow(
                    "UPDATE lease.lease "
                    "SET expires_at = $3, updated_at = $4 "
                    "WHERE workspace_id = $1 "
                    "  AND lease_id = $2 "
                    "  AND released_at IS NULL "
                    "RETURNING workspace_id, lease_id, run_id, step_id, attempt, "
                    "slot, capability, connector_instance_id, token_type, "
                    "issued_at, expires_at, released_at, revoked_at, "
                    "revoke_reason, created_at, updated_at",
                    str(workspace_id),
                    lease_id,
                    new_expires_at,
                    new_expires_at,
                )
        except Exception as exc:
            raise self._classify(exc) from exc
        if row is None:
            return None
        return _row_to_lease(row)

    async def release_lease(
        self,
        workspace_id: WorkspaceId,
        lease_id: str,
        released_at: datetime,
    ) -> Lease | None:
        pool = await self._pool_ref()
        try:
            async with pool.acquire() as conn:
                # COALESCE keeps release idempotent: a second call leaves
                # ``released_at`` at its original value and only refreshes
                # ``updated_at``. The Lease Manager treats either outcome
                # as success.
                row = await conn.fetchrow(
                    "UPDATE lease.lease "
                    "SET released_at = COALESCE(released_at, $3), "
                    "    updated_at = $3 "
                    "WHERE workspace_id = $1 AND lease_id = $2 "
                    "RETURNING workspace_id, lease_id, run_id, step_id, attempt, "
                    "slot, capability, connector_instance_id, token_type, "
                    "issued_at, expires_at, released_at, revoked_at, "
                    "revoke_reason, created_at, updated_at",
                    str(workspace_id),
                    lease_id,
                    released_at,
                )
        except Exception as exc:
            raise self._classify(exc) from exc
        if row is None:
            return None
        return _row_to_lease(row)

    async def count_active_for_step_attempt(
        self,
        workspace_id: WorkspaceId,
        run_id: RunId,
        step_id: StepId,
        attempt: int,
        as_of: datetime,
    ) -> int:
        pool = await self._pool_ref()
        try:
            async with pool.acquire() as conn:
                # Hits the ``lease_by_step_attempt`` partial index which
                # excludes released rows; the explicit ``released_at IS NULL``
                # filter in the WHERE makes the planner pick that index.
                count = await conn.fetchval(
                    "SELECT COUNT(*) FROM lease.lease "
                    "WHERE workspace_id = $1 "
                    "  AND run_id = $2 "
                    "  AND step_id = $3 "
                    "  AND attempt = $4 "
                    "  AND released_at IS NULL "
                    "  AND expires_at > $5",
                    str(workspace_id),
                    str(run_id),
                    str(step_id),
                    attempt,
                    as_of,
                )
        except Exception as exc:
            raise self._classify(exc) from exc
        return int(count or 0)

    async def list_active_leases(
        self,
        workspace_id: WorkspaceId,
        filter: LeaseFilter | None = None,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[Lease]:
        eff_limit = _resolve_limit(limit)
        where: list[str] = ["workspace_id = $1", "released_at IS NULL"]
        params: list[Any] = [str(workspace_id)]
        if filter is not None:
            if filter.run_id is not None:
                params.append(str(filter.run_id))
                where.append(f"run_id = ${len(params)}")
            if filter.step_id is not None:
                params.append(str(filter.step_id))
                where.append(f"step_id = ${len(params)}")
            if filter.attempt is not None:
                params.append(filter.attempt)
                where.append(f"attempt = ${len(params)}")
            if filter.connector_instance_id is not None:
                params.append(str(filter.connector_instance_id))
                where.append(f"connector_instance_id = ${len(params)}")
        if cursor is not None:
            ts, lid = _decode_cursor(cursor)
            params.append(ts)
            params.append(lid)
            # Tuple boundary matching the (issued_at DESC, lease_id ASC)
            # primary ordering — same construction as the connector_instance
            # adapter so paging never skips microsecond ties.
            where.append(
                f"(issued_at < ${len(params) - 1} "
                f"OR (issued_at = ${len(params) - 1} "
                f"AND lease_id > ${len(params)}))"
            )
        params.append(eff_limit + 1)
        sql = (
            "SELECT workspace_id, lease_id, run_id, step_id, attempt, "
            "slot, capability, connector_instance_id, token_type, "
            "issued_at, expires_at, released_at, revoked_at, "
            "revoke_reason, created_at, updated_at "
            "FROM lease.lease "
            f"WHERE {' AND '.join(where)} "
            "ORDER BY issued_at DESC, lease_id ASC "
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
            next_cursor = _encode_cursor(last["issued_at"], last["lease_id"])
            rows = rows[:eff_limit]
        items = [_row_to_lease(r) for r in rows]
        return Page(items=items, next_cursor=next_cursor)

    # ----- Error classification -----

    @staticmethod
    def _classify(exc: BaseException) -> Exception:
        cls_name = type(exc).__name__
        if "Connection" in cls_name or "Interface" in cls_name:
            return BackendUnavailable(str(exc), cause=exc)
        return exc if isinstance(exc, Exception) else RuntimeError(str(exc))


def make_adapter() -> PgLeaseAdapter:
    """Entry-point factory (synchronous). See definition.make_adapter."""
    return PgLeaseAdapter(lazy=LazyPool(read_dsn_from_env()))


__all__ = [
    "INTERFACE_NAME",
    "PgLeaseAdapter",
    "make_adapter",
]
