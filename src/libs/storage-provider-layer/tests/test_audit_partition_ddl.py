"""Tests for the Audit Partition Enforcer DDL bundle.

No live Postgres is required — these tests inspect the rendered DDL
text to verify the contract (schema isolation, append-only triggers,
role grants). SPL-013 / SPL-015 will exercise the DDL against a real
database.
"""

from __future__ import annotations

import re

import pytest

from custos_spl import (
    DEFAULT_AUDIT_RETENTION_ROLE,
    DEFAULT_AUDIT_SCHEMA,
    DEFAULT_PLATFORM_ROLE,
    audit_partition_ddl,
)


@pytest.fixture
def ddl() -> list[str]:
    return audit_partition_ddl()


@pytest.fixture
def joined(ddl: list[str]) -> str:
    return "\n".join(ddl)


# ----- defaults -----


def test_default_constants_match_design() -> None:
    """Pinned to design.md § Configuration — keep these in sync."""
    assert DEFAULT_AUDIT_SCHEMA == "custos_audit"
    assert DEFAULT_PLATFORM_ROLE == "custos_platform"
    assert DEFAULT_AUDIT_RETENTION_ROLE == "audit_retention"


# ----- schema + tables -----


def test_creates_schema(joined: str) -> None:
    assert "CREATE SCHEMA IF NOT EXISTS custos_audit;" in joined


def test_creates_audit_events_table(joined: str) -> None:
    assert "CREATE TABLE IF NOT EXISTS custos_audit.audit_events" in joined
    assert "event_id" in joined
    assert "workspace_id" in joined
    assert "occurred_at" in joined


def test_creates_audit_outbox_table(joined: str) -> None:
    assert "CREATE TABLE IF NOT EXISTS custos_audit.audit_outbox" in joined
    # BIGSERIAL id is what the drain cursor compares against.
    assert "id            BIGSERIAL" in joined or "id BIGSERIAL" in joined


def test_creates_audit_outbox_cursor_table(joined: str) -> None:
    assert (
        "CREATE TABLE IF NOT EXISTS custos_audit.audit_outbox_cursor" in joined
    )
    assert "pipeline_id" in joined


# ----- trigger function + triggers -----


def test_trigger_function_uses_create_or_replace(joined: str) -> None:
    """Idempotent: re-running the bundle must not error on the function."""
    assert "CREATE OR REPLACE FUNCTION custos_audit.deny_audit_mutation" in joined


def test_trigger_function_raises_on_mutation(joined: str) -> None:
    assert "RAISE EXCEPTION" in joined
    assert "audit tamper attempt" in joined


def test_trigger_function_permits_retention_role_delete(joined: str) -> None:
    """`audit_retention` role's DELETEs must pass — that's the retention worker."""
    # Look for the conditional that permits the dedicated role.
    assert (
        "TG_OP = 'DELETE' AND current_user = 'audit_retention'" in joined
    )


def test_before_update_or_delete_trigger_on_audit_events(ddl: list[str]) -> None:
    assert any(
        "BEFORE UPDATE OR DELETE ON custos_audit.audit_events" in s for s in ddl
    )


def test_before_update_or_delete_trigger_on_audit_outbox(ddl: list[str]) -> None:
    assert any(
        "BEFORE UPDATE OR DELETE ON custos_audit.audit_outbox" in s for s in ddl
    )


def test_no_trigger_on_cursor_table(joined: str) -> None:
    """audit_outbox_cursor is mutable — no append-only trigger on it."""
    assert "ON custos_audit.audit_outbox_cursor" not in re.sub(
        r"GRANT[^;]*ON custos_audit\.audit_outbox_cursor", "", joined
    )


# ----- grants: platform role -----


def test_platform_role_loses_update_delete_on_events(joined: str) -> None:
    assert (
        "REVOKE UPDATE, DELETE, TRUNCATE ON custos_audit.audit_events "
        "FROM custos_platform" in joined
    )


def test_platform_role_loses_update_delete_on_outbox(joined: str) -> None:
    assert (
        "REVOKE UPDATE, DELETE, TRUNCATE ON custos_audit.audit_outbox "
        "FROM custos_platform" in joined
    )


def test_platform_role_keeps_insert_select_on_events(joined: str) -> None:
    assert (
        "GRANT SELECT, INSERT ON custos_audit.audit_events TO custos_platform"
        in joined
    )


def test_platform_role_keeps_insert_select_on_outbox(joined: str) -> None:
    assert (
        "GRANT SELECT, INSERT ON custos_audit.audit_outbox TO custos_platform"
        in joined
    )


def test_platform_role_can_update_cursor_table(joined: str) -> None:
    """Cursor commits require UPDATE on the cursor table."""
    assert (
        "GRANT SELECT, INSERT, UPDATE ON custos_audit.audit_outbox_cursor "
        "TO custos_platform" in joined
    )


# ----- grants: audit_retention role -----


def test_retention_role_can_delete_events(joined: str) -> None:
    assert (
        "GRANT SELECT, DELETE ON custos_audit.audit_events TO audit_retention"
        in joined
    )


def test_retention_role_can_delete_outbox(joined: str) -> None:
    assert (
        "GRANT SELECT, DELETE ON custos_audit.audit_outbox TO audit_retention"
        in joined
    )


def test_retention_role_never_receives_insert(joined: str) -> None:
    """Retention worker MUST NOT be able to forge audit rows."""
    # The retention role only appears on SELECT/DELETE grants.
    for stmt in audit_partition_ddl():
        if "audit_retention" in stmt and "GRANT" in stmt and "INSERT" in stmt:
            pytest.fail(
                f"audit_retention should never receive INSERT: {stmt!r}"
            )


# ----- parameterization -----


def test_custom_schema_and_roles_are_interpolated() -> None:
    ddl = audit_partition_ddl(
        schema="my_audit",
        platform_role="my_platform",
        audit_retention_role="my_retention",
    )
    joined = "\n".join(ddl)
    assert "CREATE SCHEMA IF NOT EXISTS my_audit;" in joined
    assert "ON my_audit.audit_events" in joined
    assert "TO my_platform" in joined
    assert "TO my_retention" in joined
    # No leakage of defaults.
    assert "custos_audit" not in joined
    assert "custos_platform" not in joined
    assert "audit_retention" not in joined


@pytest.mark.parametrize(
    "name",
    [
        "",
        "1bad",
        "drop table users",
        "custos_audit; DROP TABLE",
        '"custos_audit"',
        "schema.table",
        "with-dash",
    ],
)
def test_rejects_non_identifier_inputs(name: str) -> None:
    """Identifiers are interpolated directly — anything weird must be rejected."""
    with pytest.raises(ValueError, match="bare SQL identifier"):
        audit_partition_ddl(schema=name)
    with pytest.raises(ValueError, match="bare SQL identifier"):
        audit_partition_ddl(platform_role=name)
    with pytest.raises(ValueError, match="bare SQL identifier"):
        audit_partition_ddl(audit_retention_role=name)


# ----- idempotency markers -----


def test_table_creates_are_if_not_exists(ddl: list[str]) -> None:
    """Every CREATE TABLE must use IF NOT EXISTS — re-runs must be safe."""
    creates = [s for s in ddl if s.lstrip().upper().startswith("CREATE TABLE")]
    assert creates, "expected at least one CREATE TABLE"
    for s in creates:
        assert "IF NOT EXISTS" in s, f"non-idempotent CREATE TABLE: {s!r}"


def test_trigger_drops_are_if_exists(ddl: list[str]) -> None:
    drops = [s for s in ddl if s.lstrip().upper().startswith("DROP TRIGGER")]
    assert drops, "expected DROP TRIGGER guards before CREATE TRIGGER"
    for s in drops:
        assert "IF EXISTS" in s
