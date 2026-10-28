"""Scheduler leadership cadence, failover and explicit failure contracts."""

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest
from custos_spl.errors import BackendUnavailable, LeaseExpired, QueryUnsupported

from custos_trigger.providers import InMemoryTriggerMetadataStore
from custos_trigger.scheduler import SchedulerLeadership
from custos_trigger.settings import (
    SettingsError,
    load_settings,
    validate_scheduler_timing,
)

_START = datetime(2026, 10, 28, tzinfo=UTC)


@dataclass
class Clock:
    """Control provider and monotonic time without sleeping."""

    seconds: float = 0

    def now(self) -> datetime:
        """Return the deterministic provider clock."""
        return _START + timedelta(seconds=self.seconds)


async def test_default_cadence_and_first_scan_takeover(caplog: pytest.LogCaptureFixture) -> None:
    """Renew at ten seconds, retain epochs, and acquire exactly at the live owner's expiry."""
    clock = Clock()
    store = InMemoryTriggerMetadataStore(now=clock.now)
    a = SchedulerLeadership(
        store, deployment_id="deployment", holder_id="a", monotonic=lambda: clock.seconds
    )
    b = SchedulerLeadership(
        store, deployment_id="deployment", holder_id="b", monotonic=lambda: clock.seconds
    )
    first = await a.poll()
    assert first is not None and first.epoch == 1
    assert first.expires_at == _START + timedelta(seconds=30)
    assert await b.poll() is None
    clock.seconds = 9
    assert await a.poll() == first
    clock.seconds = 10
    renewed = await a.poll()
    assert renewed is not None and renewed.epoch == first.epoch
    assert renewed.expires_at == _START + timedelta(seconds=40)
    clock.seconds = 39
    assert await b.poll() is None
    clock.seconds = 40
    replacement = await b.poll()
    assert replacement is not None and replacement.epoch == 2
    assert await a.poll() is None
    assert "leadership lost" in caplog.text
    with pytest.raises(LeaseExpired):
        await store.release_scheduler_lease(first)
    await b.release()
    reacquired = await a.poll()
    assert reacquired is not None and reacquired.epoch == 3


async def test_backend_failure_discards_local_authority(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A failed renewal is visible and never leaves a success-shaped local leadership state."""
    clock = Clock()
    store = InMemoryTriggerMetadataStore(now=clock.now)
    leader = SchedulerLeadership(
        store, deployment_id="d", holder_id="a", monotonic=lambda: clock.seconds
    )
    await leader.poll()

    async def unavailable(*args: object, **kwargs: object) -> None:
        """Simulate a concrete provider outage."""
        raise BackendUnavailable("offline")

    monkeypatch.setattr(store, "renew_scheduler_lease", unavailable)
    clock.seconds = 10
    with pytest.raises(BackendUnavailable):
        await leader.poll()
    assert leader.lease is None
    assert "backend unavailable" in caplog.text
    await leader.release()


async def test_acquire_retry_does_not_assume_a_fresh_ttl() -> None:
    """Recovery of an already-live holder near expiry renews immediately, not ten seconds late."""
    clock = Clock()
    store = InMemoryTriggerMetadataStore(now=clock.now)
    original = await store.acquire_scheduler_lease("d", "a", ttl_seconds=30)
    assert original is not None
    clock.seconds = 29
    recovered = SchedulerLeadership(
        store, deployment_id="d", holder_id="a", monotonic=lambda: clock.seconds
    )
    existing = await recovered.poll()
    assert existing is not None and existing.expires_at == original.expires_at
    renewed = await recovered.poll()
    assert renewed is not None and renewed.epoch == original.epoch
    assert renewed.expires_at == _START + timedelta(seconds=59)


async def test_cancelled_renewal_discards_local_authority(monkeypatch: pytest.MonkeyPatch) -> None:
    """Cancellation of an in-flight renewal leaves no cached leadership claim."""
    clock = Clock()
    store = InMemoryTriggerMetadataStore(now=clock.now)
    leader = SchedulerLeadership(
        store, deployment_id="d", holder_id="a", monotonic=lambda: clock.seconds
    )
    await leader.poll()
    entered = asyncio.Event()

    async def blocked(*args: object, **kwargs: object) -> None:
        """Expose a deterministic cancellation point at the provider boundary."""
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(store, "renew_scheduler_lease", blocked)
    clock.seconds = 10
    task = asyncio.create_task(leader.poll())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert leader.lease is None


def test_missing_capability_is_not_a_ready_scheduler() -> None:
    """Unsupported providers fail explicitly rather than acting as an unfenced scheduler."""
    with pytest.raises(QueryUnsupported):
        SchedulerLeadership(object(), deployment_id="d", holder_id="a")


@pytest.mark.parametrize(
    ("lease", "renew"), [(0, 0), (30, 0), (30, 30), (10, 30), (True, 1), (30, True)]
)
def test_invalid_lease_renewal_combinations(lease: int, renew: int) -> None:
    """Renewal must be a positive whole interval strictly shorter than the lease."""
    with pytest.raises(SettingsError):
        validate_scheduler_timing(lease, renew)


def test_settings_expose_and_validate_renewal_interval() -> None:
    """The typed environment surface uses the approved 30/10 defaults."""
    env = {
        "TRIGGER_WEBHOOK_BASE_URL": "https://example.test",
        "TRIGGER_WF_ENDPOINT": "workflow",
        "TRIGGER_CONNECTOR_ENDPOINT": "connector",
        "TRIGGER_METADATA_STORE": "memory",
    }
    configured = load_settings(env)
    assert (
        configured.scheduler_leader_lease_seconds,
        configured.scheduler_leader_renew_seconds,
    ) == (30, 10)
    with pytest.raises(SettingsError):
        load_settings({**env, "TRIGGER_SCHEDULER_LEADER_RENEW_SECONDS": "30"})
