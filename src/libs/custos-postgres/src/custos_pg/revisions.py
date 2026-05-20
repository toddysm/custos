"""Postgres-side schema-revision ledger.

Each adapter records the revisions it has applied for each interface it
owns in `custos_meta.adapter_revisions`. The runtime `declared_revisions`
property reads this table; `apply_pending()` writes new rows after a
successful migration step.

The ledger is intentionally per-adapter and per-interface. A single
deployment can host several adapters (Definition + Catalog + Metadata
+ Auth) against the same Postgres database; each writes to its own
interface-name rows in the shared ledger.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from asyncpg import Connection
    from asyncpg.pool import Pool

LEDGER_SCHEMA = "custos_meta"
LEDGER_TABLE = "adapter_revisions"

LEDGER_DDL: tuple[str, ...] = (
    f"CREATE SCHEMA IF NOT EXISTS {LEDGER_SCHEMA}",
    f"""
    CREATE TABLE IF NOT EXISTS {LEDGER_SCHEMA}.{LEDGER_TABLE} (
        interface_name TEXT NOT NULL,
        revision INTEGER NOT NULL,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (interface_name, revision)
    )
    """,
)


async def ensure_ledger(conn: Connection) -> None:
    """Create the ledger schema + table if missing.

    Idempotent; safe to call on every `apply_pending()`.
    """
    for stmt in LEDGER_DDL:
        await conn.execute(stmt)


async def read_declared(pool: Pool, interface_names: tuple[str, ...]) -> dict[str, set[int]]:
    """Return `{interface_name: {revisions...}}` for the given interfaces.

    Interfaces missing from the ledger are reported as empty sets so
    the caller (and SPL's `check_revisions`) can detect gaps without
    `KeyError`. Returns empty sets if the ledger table itself is
    missing, since a fresh database has nothing declared.
    """
    declared: dict[str, set[int]] = {name: set() for name in interface_names}
    async with pool.acquire() as conn:
        exists = await conn.fetchval(
            "SELECT to_regclass($1)",
            f"{LEDGER_SCHEMA}.{LEDGER_TABLE}",
        )
        if exists is None:
            return declared
        rows = await conn.fetch(
            f"SELECT interface_name, revision FROM {LEDGER_SCHEMA}.{LEDGER_TABLE} "
            f"WHERE interface_name = ANY($1::text[])",
            list(interface_names),
        )
    for row in rows:
        declared.setdefault(row["interface_name"], set()).add(int(row["revision"]))
    return declared


async def record_revision(conn: Connection, interface_name: str, revision: int) -> None:
    """Insert a `(interface_name, revision)` row into the ledger.

    Run inside the same transaction as the DDL it represents so a
    crash mid-migration cannot leave the ledger out of sync with the
    actual schema. ON CONFLICT DO NOTHING makes the call idempotent.
    """
    await conn.execute(
        f"INSERT INTO {LEDGER_SCHEMA}.{LEDGER_TABLE} (interface_name, revision) "
        f"VALUES ($1, $2) ON CONFLICT DO NOTHING",
        interface_name,
        revision,
    )


__all__ = [
    "LEDGER_DDL",
    "LEDGER_SCHEMA",
    "LEDGER_TABLE",
    "ensure_ledger",
    "read_declared",
    "record_revision",
]
