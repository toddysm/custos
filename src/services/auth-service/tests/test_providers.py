"""Tests for :mod:`custos_auth.providers` (AS-IMPL-003 + AS-IMPL-004)."""

from __future__ import annotations

import pytest
from custos_spl import MigrationRequired

from custos_auth.authz_cache import DEFAULT_AUTHZ_CACHE_TTL_SECONDS
from custos_auth.providers import (
    Providers,
    schema_gate_explainer,
    verify_schema_revisions,
)
from tests._fakes import FakeAuthAdapter, FakeMetadataAdapter


@pytest.fixture
def providers() -> Providers:
    return Providers(
        auth_store=FakeAuthAdapter(),  # type: ignore[arg-type]
        metadata_store=FakeMetadataAdapter(),  # type: ignore[arg-type]
    )


async def test_verify_schema_revisions_refreshes_each_adapter(providers: Providers) -> None:
    await verify_schema_revisions(providers)
    assert providers.auth_store.refresh_calls == 1  # type: ignore[attr-defined]
    assert providers.metadata_store.refresh_calls == 1  # type: ignore[attr-defined]


async def test_verify_schema_revisions_passes_when_ledger_is_current(
    providers: Providers,
) -> None:
    await verify_schema_revisions(providers)  # does not raise


async def test_verify_schema_revisions_raises_when_auth_store_is_behind() -> None:
    providers = Providers(
        auth_store=FakeAuthAdapter(applied_revisions=set()),  # type: ignore[arg-type]
        metadata_store=FakeMetadataAdapter(),  # type: ignore[arg-type]
    )
    with pytest.raises(MigrationRequired) as exc_info:
        await verify_schema_revisions(providers)
    assert ("AuthStoreProvider", 1) in exc_info.value.gaps


async def test_verify_schema_revisions_raises_when_metadata_store_is_behind() -> None:
    providers = Providers(
        auth_store=FakeAuthAdapter(),  # type: ignore[arg-type]
        metadata_store=FakeMetadataAdapter(applied_revisions=set()),  # type: ignore[arg-type]
    )
    with pytest.raises(MigrationRequired) as exc_info:
        await verify_schema_revisions(providers)
    assert ("MetadataStoreProvider", 4) in exc_info.value.gaps


def test_schema_gate_explainer_mentions_each_interface_and_remediation() -> None:
    err = MigrationRequired(
        [
            ("AuthStoreProvider", 1),
            ("MetadataStoreProvider", 4),
        ],
    )
    text = schema_gate_explainer(err)
    assert "AuthStoreProvider@rev1" in text
    assert "MetadataStoreProvider@rev4" in text
    assert "custos migrate up" in text
    assert "CUSTOS_AUTH_STORE_DSN" in text
    assert "CUSTOS_AUTH_METADATA_STORE_DSN" in text


def test_load_providers_constructs_postgres_adapters_from_settings() -> None:
    # load_providers imports the real PgAuthAdapter/PgMetadataAdapter which
    # require a DSN but do not connect synchronously thanks to LazyPool.
    from custos_auth.providers import load_providers
    from custos_auth.settings import load_settings

    settings = load_settings(
        {
            "CUSTOS_AUTH_STORE_DSN": "postgresql://u:p@h:5432/auth",
            "CUSTOS_AUTH_METADATA_STORE_DSN": "postgresql://u:p@h:5432/meta",
        },
    )
    bundle = load_providers(settings)
    assert bundle.auth_store is not None
    assert bundle.metadata_store is not None
    # Each adapter must declare its own interface name once refreshed.
    assert hasattr(bundle.auth_store, "declared_revisions")
    assert hasattr(bundle.metadata_store, "declared_revisions")


def test_default_authz_cache_uses_60_second_ttl() -> None:
    # The Providers dataclass default_factory matches the design's
    # 60-second TTL so tests that build a bare Providers() get the
    # expected behavior without touching settings.
    bundle = Providers(
        auth_store=FakeAuthAdapter(),  # type: ignore[arg-type]
        metadata_store=FakeMetadataAdapter(),  # type: ignore[arg-type]
    )
    assert bundle.authz_cache.ttl_seconds == DEFAULT_AUTHZ_CACHE_TTL_SECONDS
    assert bundle.authz_cache.enabled is True


def test_default_binding_changed_publisher_is_local_bus() -> None:
    # AS-IMPL-012 swaps the default from NoOp -> LocalBindingChangedBus
    # so single-replica deployments deliver invalidations to the
    # local cache synchronously.
    from custos_auth.binding_events import LocalBindingChangedBus

    bundle = Providers(
        auth_store=FakeAuthAdapter(),  # type: ignore[arg-type]
        metadata_store=FakeMetadataAdapter(),  # type: ignore[arg-type]
    )
    assert isinstance(bundle.binding_changed_publisher, LocalBindingChangedBus)


def test_default_binding_changed_subscriber_is_noop() -> None:
    # Single-replica deployments do not need a cross-replica
    # subscriber; the no-op default keeps the lifespan wiring uniform
    # without requiring a real transport.
    from custos_auth.binding_events import NoOpBindingChangedSubscriber

    bundle = Providers(
        auth_store=FakeAuthAdapter(),  # type: ignore[arg-type]
        metadata_store=FakeMetadataAdapter(),  # type: ignore[arg-type]
    )
    assert isinstance(bundle.binding_changed_subscriber, NoOpBindingChangedSubscriber)


def test_load_providers_threads_settings_ttl_into_cache() -> None:
    from custos_auth.providers import load_providers
    from custos_auth.settings import load_settings

    settings = load_settings(
        {
            "CUSTOS_AUTH_STORE_DSN": "postgresql://u:p@h:5432/auth",
            "CUSTOS_AUTH_METADATA_STORE_DSN": "postgresql://u:p@h:5432/meta",
            "CUSTOS_AUTH_AUTHZ_CACHE_TTL": "30",
        },
    )
    bundle = load_providers(settings)
    assert bundle.authz_cache.ttl_seconds == 30


def test_load_providers_propagates_disabled_cache_setting() -> None:
    from custos_auth.providers import load_providers
    from custos_auth.settings import load_settings

    settings = load_settings(
        {
            "CUSTOS_AUTH_STORE_DSN": "postgresql://u:p@h:5432/auth",
            "CUSTOS_AUTH_METADATA_STORE_DSN": "postgresql://u:p@h:5432/meta",
            "CUSTOS_AUTH_AUTHZ_CACHE_TTL": "0",
        },
    )
    bundle = load_providers(settings)
    assert bundle.authz_cache.enabled is False
