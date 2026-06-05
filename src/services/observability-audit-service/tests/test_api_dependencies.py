"""Tests for the Query API dependency factories (OBS-IMPL-012)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from custos_obs.api.dependencies import (
    get_log_query_provider,
    get_metadata_store,
    get_metrics_query_provider,
    get_providers,
    get_settings,
)
from custos_obs.providers import Providers
from custos_obs.settings import load_settings


class _FakeMetadataStore:
    async def query_audit(
        self, *args: object, **kwargs: object
    ) -> object:  # pragma: no cover - stub
        raise NotImplementedError


class _FakeLogQuery:
    async def query_run_logs(
        self, *args: object, **kwargs: object
    ) -> object:  # pragma: no cover - stub
        raise NotImplementedError


class _FakeMetricsQuery:
    async def query_run_metrics(
        self, *args: object, **kwargs: object
    ) -> object:  # pragma: no cover - stub
        raise NotImplementedError


def _request_with(providers: object | None) -> object:
    """Build a minimal stand-in exposing ``request.app.state.providers``."""
    state = SimpleNamespace()
    if providers is not None:
        state.providers = providers
    return SimpleNamespace(app=SimpleNamespace(state=state))


def _providers() -> Providers:
    return Providers(
        metadata_store=_FakeMetadataStore(),  # type: ignore[arg-type]
        log_query=_FakeLogQuery(),  # type: ignore[arg-type]
        metrics_query=_FakeMetricsQuery(),  # type: ignore[arg-type]
    )


def test_get_providers_returns_bundle() -> None:
    bundle = _providers()
    request = _request_with(bundle)
    assert get_providers(request) is bundle  # type: ignore[arg-type]


def test_get_providers_raises_when_unset() -> None:
    request = _request_with(None)
    with pytest.raises(RuntimeError, match=r"app\.state\.providers is not set"):
        get_providers(request)  # type: ignore[arg-type]


def test_provider_factories_return_singletons() -> None:
    bundle = _providers()
    request = _request_with(bundle)
    assert get_log_query_provider(request) is bundle.log_query  # type: ignore[arg-type]
    assert get_metrics_query_provider(request) is bundle.metrics_query  # type: ignore[arg-type]
    assert get_metadata_store(request) is bundle.metadata_store  # type: ignore[arg-type]


def _request_with_settings(settings: object | None) -> object:
    state = SimpleNamespace()
    if settings is not None:
        state.settings = settings
    return SimpleNamespace(app=SimpleNamespace(state=state))


def test_get_settings_returns_bundle() -> None:
    settings = load_settings(
        {
            "CUSTOS_LOG_QUERY_PROVIDER": "noop",
            "CUSTOS_LOGS_EXTERNAL_URL": "https://logs.example.com",
            "CUSTOS_METRICS_QUERY_PROVIDER": "noop",
            "CUSTOS_METRICS_EXTERNAL_URL": "https://metrics.example.com",
            "CUSTOS_OBS_METADATA_STORE_DSN": "postgresql://noop/noop",
        }
    )
    request = _request_with_settings(settings)
    assert get_settings(request) is settings  # type: ignore[arg-type]


def test_get_settings_raises_when_unset() -> None:
    request = _request_with_settings(None)
    with pytest.raises(RuntimeError, match=r"app\.state\.settings is not set"):
        get_settings(request)  # type: ignore[arg-type]
