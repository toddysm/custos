"""Shared fixtures for the Trigger Service test-suite (TS-IMPL-008)."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from custos_trigger.providers import InMemoryTriggerMetadataStore, Providers

#: A fixed instant the in-memory store's clock returns so tests can assert on
#: ``updated_at`` / dedup ``expires_at`` deterministically.
FIXED_NOW: datetime = datetime(2026, 6, 4, 12, 0, 0, tzinfo=UTC)


@pytest.fixture
def metadata_store() -> InMemoryTriggerMetadataStore:
    """A fresh in-process metadata store with a frozen clock."""
    return InMemoryTriggerMetadataStore(now=lambda: FIXED_NOW)


@pytest.fixture
def providers(metadata_store: InMemoryTriggerMetadataStore) -> Providers:
    """A :class:`Providers` bundle wrapping the in-memory store."""
    return Providers(metadata_store=metadata_store)
