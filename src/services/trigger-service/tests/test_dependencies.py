"""Dependency helpers + app-state provider wiring (TS-IMPL-008)."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest
from fastapi import Request
from fastapi.testclient import TestClient

from custos_trigger import create_app
from custos_trigger.dependencies import get_metadata_store, get_providers
from custos_trigger.providers import InMemoryTriggerMetadataStore, Providers
from custos_trigger.stores import (
    ResumeSubscriptionStore,
    ScheduleStore,
    SubscriptionStore,
)


def _request_with_providers(providers: Providers | None) -> Request:
    state = SimpleNamespace(providers=providers) if providers is not None else SimpleNamespace()
    return cast(Request, SimpleNamespace(app=SimpleNamespace(state=state)))


def test_get_providers_returns_bundle() -> None:
    providers = Providers(metadata_store=InMemoryTriggerMetadataStore())
    assert get_providers(_request_with_providers(providers)) is providers


def test_get_providers_raises_when_unset() -> None:
    with pytest.raises(RuntimeError, match="lifespan"):
        get_providers(_request_with_providers(None))


def test_get_metadata_store_unwraps_bundle() -> None:
    store = InMemoryTriggerMetadataStore()
    providers = Providers(metadata_store=store)
    assert get_metadata_store(providers) is store


def test_lifespan_wires_providers_and_stores() -> None:
    providers = Providers(metadata_store=InMemoryTriggerMetadataStore())
    app = create_app(providers=providers)
    with TestClient(app):
        assert app.state.providers is providers
        assert isinstance(app.state.subscription_store, SubscriptionStore)
        assert isinstance(app.state.resume_subscription_store, ResumeSubscriptionStore)
        assert isinstance(app.state.schedule_store, ScheduleStore)


def test_lifespan_builds_default_in_memory_providers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # No injected providers + no TRIGGER_METADATA_STORE env => in-memory.
    monkeypatch.delenv("TRIGGER_METADATA_STORE", raising=False)
    app = create_app()
    with TestClient(app):
        assert isinstance(app.state.providers.metadata_store, InMemoryTriggerMetadataStore)
