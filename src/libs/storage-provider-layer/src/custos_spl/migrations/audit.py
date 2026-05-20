"""Audit Partition Enforcer — append-only DDL for `custos_audit`.

Emits the canonical DDL bundle that any audit-aware Postgres adapter
applies at migration time. The DDL enforces tamper-evidence on the
audit partition via two redundant layers (per ADR-010):

  1. **Role-based grants** — the platform role gets `INSERT, SELECT` on
     audit tables; `UPDATE`/`DELETE` are revoked. Only the dedicated
     `audit_retention` role gets `DELETE` (used by Observability
     Service's retention worker).
  2. **`BEFORE UPDATE` / `BEFORE DELETE` triggers** — a `RAISE
     EXCEPTION` function fires on any mutation of `audit_events` or
     `audit_outbox`. The trigger short-circuits when `current_user` is
     the `audit_retention` role for `DELETE`, so retention cleanup can
     still proceed. Grants alone would suffice, but the trigger catches
     superuser bypass and misconfigured grants.

The `audit_outbox_cursor` table lives in the same schema but is
mutable — it tracks per-pipeline drain progress and is not audit data
itself. Platform gets `INSERT, SELECT, UPDATE` on it; no trigger.

The function returns DDL as an ordered list of statements rather than
one big string so callers can execute them individually and report
errors per-statement. Statements are idempotent: schema/table creation
uses `IF NOT EXISTS`; the trigger function uses `CREATE OR REPLACE`;
trigger drops are guarded with `DROP TRIGGER IF EXISTS`.

See `design/components/storage-provider-layer/design.md` § Audit
Partition Enforcer.
"""

from __future__ import annotations

import re

DEFAULT_AUDIT_SCHEMA = "custos_audit"
"""Default value for `CUSTOS_AUDIT_SCHEMA` — keep in sync with design.md § Configuration."""

DEFAULT_PLATFORM_ROLE = "custos_platform"
"""DB role that the platform services use to write audit + read drain state."""

DEFAULT_AUDIT_RETENTION_ROLE = "audit_retention"
"""DB role used exclusively by Observability Service's retention worker."""

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(name: str, role: str) -> None:
    """Reject anything that isn't a bare SQL identifier.

    We interpolate `schema` and role names directly into DDL (Postgres
    has no parameter binding for identifiers), so we MUST reject any
    value that could break out of the identifier slot. Quoted/dotted
    identifiers are not supported.
    """
    if not _IDENT.match(name):
        raise ValueError(
            f"{role} must be a bare SQL identifier (letters, digits, "
            f"underscore; not starting with a digit); got {name!r}"
        )


def audit_partition_ddl(
    *,
    schema: str = DEFAULT_AUDIT_SCHEMA,
    platform_role: str = DEFAULT_PLATFORM_ROLE,
    audit_retention_role: str = DEFAULT_AUDIT_RETENTION_ROLE,
) -> list[str]:
    """Return the ordered DDL statements that establish the audit partition.

    Args:
        schema: Postgres schema name; default `custos_audit`.
        platform_role: DB role used by the platform services for normal
            audit writes. Granted `INSERT, SELECT` on append-only
            tables, plus `UPDATE` on `audit_outbox_cursor`.
        audit_retention_role: DB role used by the retention worker.
            Granted `DELETE` on the two append-only tables.

    Returns:
        A list of DDL statements, in the order they must be executed.
        Idempotent: re-running the bundle on an already-configured
        database is a no-op.

    Raises:
        ValueError: if any name is not a bare SQL identifier. The
            arguments are interpolated directly into DDL; quoted or
            dotted identifiers are rejected to keep the surface safe.
    """
    _validate_identifier(schema, "schema")
    _validate_identifier(platform_role, "platform_role")
    _validate_identifier(audit_retention_role, "audit_retention_role")

    s = schema
    plat = platform_role
    ret = audit_retention_role

    return [
        # ----- schema -----
        f"CREATE SCHEMA IF NOT EXISTS {s};",
        # ----- tables -----
        # audit_events: the materialized audit log. Append-only.
        f"""\
CREATE TABLE IF NOT EXISTS {s}.audit_events (
    event_id      UUID         PRIMARY KEY,
    workspace_id  TEXT         NOT NULL,
    event_type    TEXT         NOT NULL,
    actor         TEXT         NOT NULL,
    subject       JSONB        NOT NULL,
    payload       JSONB        NOT NULL,
    occurred_at   TIMESTAMPTZ  NOT NULL
);""",
        f"CREATE INDEX IF NOT EXISTS audit_events_workspace_occurred_idx "
        f"ON {s}.audit_events (workspace_id, occurred_at DESC);",
        # audit_outbox: outbox for Observability Service to drain. Append-only.
        f"""\
CREATE TABLE IF NOT EXISTS {s}.audit_outbox (
    id            BIGSERIAL    PRIMARY KEY,
    workspace_id  TEXT         NOT NULL,
    event_id      UUID         NOT NULL,
    event_type    TEXT         NOT NULL,
    payload       JSONB        NOT NULL,
    enqueued_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);""",
        # audit_outbox_cursor: per-pipeline high-water mark. MUTABLE — no trigger.
        f"""\
CREATE TABLE IF NOT EXISTS {s}.audit_outbox_cursor (
    pipeline_id   TEXT         PRIMARY KEY,
    cursor        BIGINT       NOT NULL,
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);""",
        # ----- tamper-evident trigger function -----
        f"""\
CREATE OR REPLACE FUNCTION {s}.deny_audit_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    -- Permit DELETEs only from the dedicated retention role.
    IF TG_OP = 'DELETE' AND lower(current_user) = lower('{ret}') THEN
        RETURN OLD;
    END IF;
    RAISE EXCEPTION
        'audit tamper attempt: % on %.% by role %',
        TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME, current_user
        USING ERRCODE = 'insufficient_privilege';
END;
$$;""",
        # ----- triggers on append-only tables -----
        f"DROP TRIGGER IF EXISTS deny_mutation ON {s}.audit_events;",
        f"""\
CREATE TRIGGER deny_mutation
BEFORE UPDATE OR DELETE ON {s}.audit_events
FOR EACH ROW EXECUTE FUNCTION {s}.deny_audit_mutation();""",
        f"DROP TRIGGER IF EXISTS deny_mutation ON {s}.audit_outbox;",
        f"""\
CREATE TRIGGER deny_mutation
BEFORE UPDATE OR DELETE ON {s}.audit_outbox
FOR EACH ROW EXECUTE FUNCTION {s}.deny_audit_mutation();""",
        # ----- role grants: platform role -----
        # Schema usage so the platform can resolve the qualified names.
        f"GRANT USAGE ON SCHEMA {s} TO {plat};",
        # Audit data is append-only for the platform: INSERT + SELECT only.
        f"REVOKE UPDATE, DELETE, TRUNCATE ON {s}.audit_events FROM {plat};",
        f"REVOKE UPDATE, DELETE, TRUNCATE ON {s}.audit_outbox FROM {plat};",
        f"GRANT SELECT, INSERT ON {s}.audit_events TO {plat};",
        f"GRANT SELECT, INSERT ON {s}.audit_outbox TO {plat};",
        f"GRANT USAGE ON SEQUENCE {s}.audit_outbox_id_seq TO {plat};",
        # Cursor table is mutable.
        f"GRANT SELECT, INSERT, UPDATE ON {s}.audit_outbox_cursor TO {plat};",
        # ----- role grants: audit_retention role -----
        # Retention worker connects with this role; the trigger lets its
        # DELETEs through. Grant only what is needed.
        f"GRANT USAGE ON SCHEMA {s} TO {ret};",
        f"GRANT SELECT, DELETE ON {s}.audit_events TO {ret};",
        f"GRANT SELECT, DELETE ON {s}.audit_outbox TO {ret};",
    ]


__all__ = [
    "DEFAULT_AUDIT_RETENTION_ROLE",
    "DEFAULT_AUDIT_SCHEMA",
    "DEFAULT_PLATFORM_ROLE",
    "audit_partition_ddl",
]
