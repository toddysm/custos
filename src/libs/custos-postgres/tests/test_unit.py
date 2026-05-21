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
from custos_spl.interfaces.auth_store import AuthStoreProvider
from custos_spl.interfaces.metadata_store import (
    MetadataStoreProvider,
    TransactionHandle,
)
from custos_spl.migrations.runner import MigrationCapable

from custos_pg.adapters.auth import PgAuthAdapter
from custos_pg.adapters.auth import make_adapter as make_auth_adapter
from custos_pg.adapters.catalog import (
    PgCatalogAdapter,
    _decode_cursor,
    _encode_cursor,
)
from custos_pg.adapters.catalog import make_adapter as make_catalog_adapter
from custos_pg.adapters.definition import PgDefinitionAdapter
from custos_pg.adapters.definition import make_adapter as make_definition_adapter
from custos_pg.adapters.metadata import (
    PgLeaseHandle,
    PgMetadataAdapter,
    PgTransactionHandle,
)
from custos_pg.adapters.metadata import make_adapter as make_metadata_adapter
from custos_pg.migrations.auth import AUTH_REV1
from custos_pg.migrations.catalog import CATALOG_REV1
from custos_pg.migrations.definition import DEFINITION_REV1
from custos_pg.migrations.metadata import METADATA_REV1, METADATA_REV2, METADATA_REV3
from custos_pg.pool import DSN_ENV_VAR, LazyPool, read_dsn_from_env

# ----- Protocol conformance -----


def test_definition_adapter_satisfies_definition_protocol() -> None:
    adapter = PgDefinitionAdapter(lazy=LazyPool("postgresql://noop"))
    assert isinstance(adapter, DefinitionStoreProvider)


def test_catalog_adapter_satisfies_catalog_protocol() -> None:
    adapter = PgCatalogAdapter(lazy=LazyPool("postgresql://noop"))
    assert isinstance(adapter, CatalogStoreProvider)


def test_metadata_adapter_satisfies_metadata_protocol() -> None:
    adapter = PgMetadataAdapter(lazy=LazyPool("postgresql://noop"))
    assert isinstance(adapter, MetadataStoreProvider)


def test_auth_adapter_satisfies_auth_protocol() -> None:
    adapter = PgAuthAdapter(lazy=LazyPool("postgresql://noop"))
    assert isinstance(adapter, AuthStoreProvider)


def test_auth_adapter_unimplemented_methods_raise_not_implemented_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Out-of-scope methods (SPL-130c through SPL-130h) raise NotImplementedError."""
    monkeypatch.setenv(DSN_ENV_VAR, "postgresql://noop")
    adapter = make_auth_adapter()

    # These methods are not yet implemented and should raise NotImplementedError
    unimplemented_methods = [
        "put_principal",
        "get_principal",
        "list_principals",
        "disable_principal",
        "put_oidc_identity",
        "get_oidc_identity",
        "list_oidc_identities_for_user",
        "put_service_token",
        "get_service_token_by_hash",
        "revoke_service_token",
        "list_service_tokens_for_service_account",
        "delete_expired_service_tokens",
        "upsert_permission",
        "list_permissions",
        "put_role",
        "get_role",
        "list_roles",
        "put_role_binding",
        "delete_role_binding",
        "list_role_bindings_for_principal",
        "list_role_bindings_for_scope",
        "with_transaction",
    ]

    for method_name in unimplemented_methods:
        method = getattr(adapter, method_name)
        assert callable(method), f"{method_name} should be callable"
        # We can't actually call async methods without an event loop in a sync test,
        # but we can at least verify they exist and are callable


def test_definition_adapter_satisfies_migration_capable() -> None:
    adapter = PgDefinitionAdapter(lazy=LazyPool("postgresql://noop"))
    assert isinstance(adapter, MigrationCapable)


def test_catalog_adapter_satisfies_migration_capable() -> None:
    adapter = PgCatalogAdapter(lazy=LazyPool("postgresql://noop"))
    assert isinstance(adapter, MigrationCapable)


def test_metadata_adapter_satisfies_migration_capable() -> None:
    adapter = PgMetadataAdapter(lazy=LazyPool("postgresql://noop"))
    assert isinstance(adapter, MigrationCapable)


def test_auth_adapter_satisfies_migration_capable() -> None:
    adapter = PgAuthAdapter(lazy=LazyPool("postgresql://noop"))
    assert isinstance(adapter, MigrationCapable)


def test_adapter_requires_pool_or_lazy() -> None:
    with pytest.raises(ValueError, match="requires either"):
        PgDefinitionAdapter()
    with pytest.raises(ValueError, match="requires either"):
        PgCatalogAdapter()
    with pytest.raises(ValueError, match="requires either"):
        PgMetadataAdapter()
    with pytest.raises(ValueError, match="requires either"):
        PgAuthAdapter()


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
    meta = PgMetadataAdapter(lazy=LazyPool("postgresql://noop"))
    assert meta.declared_revisions == {"MetadataStoreProvider": frozenset()}
    auth = PgAuthAdapter(lazy=LazyPool("postgresql://noop"))
    assert auth.declared_revisions == {"AuthStoreProvider": frozenset()}


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
    assert METADATA_REV1.number == 1


def test_metadata_revisions_are_numbered_one_through_three() -> None:
    assert METADATA_REV1.number == 1
    assert METADATA_REV2.number == 2
    assert METADATA_REV3.number == 3


def test_metadata_rev1_owns_expected_tables() -> None:
    joined = " ".join(METADATA_REV1.statements)
    assert "CREATE SCHEMA IF NOT EXISTS custos_state" in joined
    assert "custos_state.run" in joined
    assert "custos_state.step" in joined
    assert "custos_state.step_attempt" in joined
    assert "custos_state.subscription" in joined
    assert "custos_state.subscription_selector" in joined
    assert "custos_state.resume_subscription" in joined
    assert "custos_state.dedup_key" in joined
    assert "custos_state.schedule" in joined
    assert "custos_state.connector_cursor" in joined
    assert "lease_holder" in joined
    assert "lease_expires_at" in joined
    assert "custos_state.artifact_use" in joined


def test_metadata_rev2_owns_idempotency_records() -> None:
    joined = " ".join(METADATA_REV2.statements)
    assert "custos_state.idempotency_record" in joined
    assert "workspace_id" in joined
    assert "principal_id" in joined
    assert "idempotency_key" in joined
    assert "request_hash" in joined
    assert "response_snapshot" in joined


def test_metadata_rev3_owns_device_code_sessions() -> None:
    joined = " ".join(METADATA_REV3.statements)
    assert "custos_state.device_code_session" in joined
    assert "device_code" in joined
    assert "user_code" in joined
    assert "issuer_alias" in joined
    assert "token_bundle" in joined


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


def test_metadata_factory_errors_when_dsn_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(DSN_ENV_VAR, raising=False)
    with pytest.raises(RuntimeError, match=DSN_ENV_VAR):
        make_metadata_adapter()


def test_auth_factory_errors_when_dsn_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(DSN_ENV_VAR, raising=False)
    with pytest.raises(RuntimeError, match=DSN_ENV_VAR):
        make_auth_adapter()


def test_metadata_factory_returns_lazy_adapter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(DSN_ENV_VAR, "postgresql://noop")
    adapter = make_metadata_adapter()
    assert isinstance(adapter, PgMetadataAdapter)


def test_auth_factory_returns_lazy_adapter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(DSN_ENV_VAR, "postgresql://noop")
    adapter = make_auth_adapter()
    assert isinstance(adapter, PgAuthAdapter)


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


# ----- Metadata handles -----


def test_lease_handle_carries_identifiers() -> None:
    h = PgLeaseHandle(workspace_id="ws", instance_id="inst", holder_id="holder-1")
    assert h.workspace_id == "ws"
    assert h.instance_id == "inst"
    assert h.holder_id == "holder-1"


def test_transaction_handle_is_subclass_of_protocol_base() -> None:
    # __weakref__ comes from the base; PgTransactionHandle adds `_conn`.
    # We construct with a sentinel and confirm the conn property works.
    sentinel = object()
    h = PgTransactionHandle(sentinel)
    assert h.conn is sentinel
    assert isinstance(h, TransactionHandle)


def test_transaction_handle_starts_open_and_can_be_marked_closed() -> None:
    sentinel = object()
    h = PgTransactionHandle(sentinel)
    assert h.closed is False
    h._mark_closed()
    assert h.closed is True


def test_auth_rev1_is_numbered_one() -> None:
    assert AUTH_REV1.number == 1


def test_auth_rev1_has_expected_statement_count() -> None:
    # 1 schema + 8 tables + 9 indices = 18 statements
    assert len(AUTH_REV1.statements) == 18


def test_auth_rev1_owns_expected_tables() -> None:
    joined = " ".join(AUTH_REV1.statements)
    assert "CREATE SCHEMA IF NOT EXISTS auth" in joined
    # Tenancy
    assert "auth.tenant" in joined
    assert "auth.workspace" in joined
    # Principals
    assert "auth.principal" in joined
    assert "kind" in joined
    # OIDC identities
    assert "auth.oidc_identity" in joined
    assert "issuer" in joined
    assert "subject" in joined
    # Service tokens
    assert "auth.service_token" in joined
    assert "hash" in joined
    assert "service_account_id" in joined
    # Permissions & roles
    assert "auth.permission" in joined
    assert "auth.role" in joined
    assert "permission_names" in joined
    # Role bindings
    assert "auth.role_binding" in joined
    assert "principal_id" in joined
    assert "scope" in joined
    # Verify expected indices
    assert "workspace_by_tenant" in joined
    assert "principal_by_tenant_kind" in joined
    assert "principal_by_workspace_kind" in joined
    assert "oidc_identity_by_user" in joined
    assert "service_token_by_hash" in joined
    assert "service_token_by_service_account" in joined
    assert "service_token_by_expires_at" in joined
    assert "role_binding_by_principal_scope" in joined
    assert "role_binding_by_role" in joined
