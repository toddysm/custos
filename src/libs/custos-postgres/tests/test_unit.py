"""Unit tests for the custos-postgres package — no live Postgres.

Covers:
- Protocol conformance (the adapters satisfy SPL Protocols + MigrationCapable).
- DDL bundle shape (revision-1 statements include the expected schemas).
- Factory error path (CUSTOS_PG_DSN unset → operator-actionable RuntimeError).
- Cursor encode/decode roundtrip.

Live-database behavior (apply_pending, write-once enforcement, digest
conflict, deprecation, pagination) lives in `test_integration.py`.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from custos_spl.interfaces.catalog_store import CatalogStoreProvider
from custos_spl.interfaces.definition_store import DefinitionStoreProvider
from custos_spl.migrations.runner import MigrationCapable

from custos_pg.adapters.catalog import (
    PgCatalogAdapter,
    _decode_cursor,
    _encode_cursor,
)
from custos_pg.adapters.catalog import make_adapter as make_catalog_adapter
from custos_pg.adapters.definition import PgDefinitionAdapter
from custos_pg.adapters.definition import make_adapter as make_definition_adapter
from custos_pg.migrations.catalog import CATALOG_REV1
from custos_pg.migrations.definition import DEFINITION_REV1
from custos_pg.pool import DSN_ENV_VAR, LazyPool, read_dsn_from_env

# ----- Protocol conformance -----


def test_definition_adapter_satisfies_definition_protocol() -> None:
    adapter = PgDefinitionAdapter(lazy=LazyPool("postgresql://noop"))
    assert isinstance(adapter, DefinitionStoreProvider)


def test_catalog_adapter_satisfies_catalog_protocol() -> None:
    adapter = PgCatalogAdapter(lazy=LazyPool("postgresql://noop"))
    assert isinstance(adapter, CatalogStoreProvider)


def test_definition_adapter_satisfies_migration_capable() -> None:
    adapter = PgDefinitionAdapter(lazy=LazyPool("postgresql://noop"))
    assert isinstance(adapter, MigrationCapable)


def test_catalog_adapter_satisfies_migration_capable() -> None:
    adapter = PgCatalogAdapter(lazy=LazyPool("postgresql://noop"))
    assert isinstance(adapter, MigrationCapable)


def test_adapter_requires_pool_or_lazy() -> None:
    with pytest.raises(ValueError, match="requires either"):
        PgDefinitionAdapter()
    with pytest.raises(ValueError, match="requires either"):
        PgCatalogAdapter()


# ----- Declared-revisions cache before any migration -----


def test_declared_revisions_starts_empty() -> None:
    """Before `apply_pending`/`refresh_declared`, the cache is empty.

    SPL's `check_revisions` will see this as a gap and surface
    `MigrationRequired` — exactly the behavior the design requires.
    """
    adapter = PgDefinitionAdapter(lazy=LazyPool("postgresql://noop"))
    assert adapter.declared_revisions == {"DefinitionStoreProvider": frozenset()}
    cat = PgCatalogAdapter(lazy=LazyPool("postgresql://noop"))
    assert cat.declared_revisions == {"CatalogStoreProvider": frozenset()}


# ----- DDL shape -----


def test_definition_rev1_owns_expected_tables() -> None:
    joined = " ".join(DEFINITION_REV1.statements)
    assert "CREATE SCHEMA IF NOT EXISTS definition" in joined
    assert "definition.workflow_version" in joined
    assert "definition.workflow_template_version" in joined
    assert "FOREIGN KEY" in joined  # parent reference
    assert "ORDER BY" not in joined  # DDL only; no DML


def test_catalog_rev1_owns_expected_tables() -> None:
    joined = " ".join(CATALOG_REV1.statements)
    assert "CREATE SCHEMA IF NOT EXISTS catalog" in joined
    assert "catalog.activity_type_version" in joined
    assert "catalog.connector_type_version" in joined
    assert "digest" in joined
    # No workspace scoping at the catalog layer.
    assert "workspace_id" not in joined


def test_revisions_are_numbered_one() -> None:
    assert DEFINITION_REV1.number == 1
    assert CATALOG_REV1.number == 1


# ----- Factories -----


def test_definition_factory_errors_when_dsn_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(DSN_ENV_VAR, raising=False)
    with pytest.raises(RuntimeError, match=DSN_ENV_VAR):
        make_definition_adapter()


def test_catalog_factory_errors_when_dsn_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(DSN_ENV_VAR, raising=False)
    with pytest.raises(RuntimeError, match=DSN_ENV_VAR):
        make_catalog_adapter()


def test_definition_factory_returns_lazy_adapter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Synchronous factory: returns an adapter without touching the network."""
    monkeypatch.setenv(DSN_ENV_VAR, "postgresql://noop")
    adapter = make_definition_adapter()
    assert isinstance(adapter, PgDefinitionAdapter)


def test_catalog_factory_returns_lazy_adapter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(DSN_ENV_VAR, "postgresql://noop")
    adapter = make_catalog_adapter()
    assert isinstance(adapter, PgCatalogAdapter)


def test_read_dsn_from_env_errors_when_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(DSN_ENV_VAR, raising=False)
    with pytest.raises(RuntimeError, match=DSN_ENV_VAR):
        read_dsn_from_env()


# ----- Cursor roundtrip -----


def test_cursor_roundtrip() -> None:
    ts = datetime(2026, 5, 19, 10, 30, tzinfo=UTC)
    c = _encode_cursor(ts, "1.2.3")
    rt_ts, rt_ver = _decode_cursor(c)
    assert rt_ts == ts
    assert rt_ver == "1.2.3"
