"""Lease maintenance for a scheduler scan loop; no worker is started by importing this module."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable

from custos_spl.errors import BackendUnavailable, LeaseExpired, QueryUnsupported
from custos_spl.interfaces.scheduler import SchedulerLease, SchedulerStateProvider, lease_duration

from custos_trigger.settings import (
    DEFAULT_SCHEDULER_LEADER_LEASE_SECONDS,
    DEFAULT_SCHEDULER_LEADER_RENEW_SECONDS,
    validate_scheduler_timing,
)

logger = logging.getLogger("custos_trigger.scheduler")


class SchedulerLeadership:
    """Maintain one deployment's fenced lease using an injected monotonic renewal clock.

    Call ``poll`` on each receiver scan. Standbys attempt acquisition each time;
    leaders renew at the configured interval. Provider time decides ownership,
    and every schedule mutation must still present the token to the provider.
    No background task, publication, or production worker enablement is implied.
    """

    def __init__(
        self,
        provider: object,
        *,
        deployment_id: str,
        holder_id: str,
        lease_seconds: int = DEFAULT_SCHEDULER_LEADER_LEASE_SECONDS,
        renew_seconds: int = DEFAULT_SCHEDULER_LEADER_RENEW_SECONDS,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        """Validate capability and timing without opening backend connections."""
        validate_scheduler_timing(lease_seconds, renew_seconds)
        lease_duration(deployment_id, holder_id, lease_seconds)
        if not isinstance(provider, SchedulerStateProvider):
            raise QueryUnsupported("metadata provider lacks fenced scheduler state capability")
        self._provider = provider
        self._deployment_id = deployment_id
        self._holder_id = holder_id
        self._lease_seconds = lease_seconds
        self._renew_seconds = renew_seconds
        self._monotonic = monotonic
        self._lease: SchedulerLease | None = None
        self._renew_at = 0.0
        self._ready = False
        self._lock = asyncio.Lock()

    @property
    def lease(self) -> SchedulerLease | None:
        """Expose the current token; only provider-side checks establish its liveness."""
        return self._lease

    async def check_ready(self) -> None:
        """Fail readiness explicitly when the additive scheduler schema is unavailable."""
        await self._provider.check_scheduler_ready()
        self._ready = True

    def _schedule_renewal(self, started: float, lease: SchedulerLease) -> None:
        """Retain the renewal safety margin when an idempotent acquire returns a partial TTL."""
        remaining = (lease.expires_at - lease.observed_at).total_seconds()
        delay = min(
            self._renew_seconds,
            max(0.0, remaining - (self._lease_seconds - self._renew_seconds)),
        )
        self._renew_at = started + delay

    async def poll(self) -> SchedulerLease | None:
        """Acquire or renew once, allowing first-scan takeover after expiry."""
        async with self._lock:
            if not self._ready:
                await self.check_ready()
            started = self._monotonic()
            try:
                if self._lease is not None and started >= self._renew_at:
                    try:
                        self._lease = await self._provider.renew_scheduler_lease(
                            self._lease, ttl_seconds=self._lease_seconds
                        )
                    except LeaseExpired:
                        logger.warning(
                            "scheduler leadership lost",
                            extra={
                                "deployment_id": self._deployment_id,
                                "holder_id": self._holder_id,
                            },
                        )
                        self._lease = None
                    else:
                        self._schedule_renewal(started, self._lease)
                if self._lease is None:
                    self._lease = await self._provider.acquire_scheduler_lease(
                        self._deployment_id, self._holder_id, ttl_seconds=self._lease_seconds
                    )
                    if self._lease is not None:
                        self._schedule_renewal(started, self._lease)
                        logger.info(
                            "scheduler leadership acquired",
                            extra={
                                "deployment_id": self._deployment_id,
                                "holder_id": self._holder_id,
                                "epoch": self._lease.epoch,
                            },
                        )
                return self._lease
            except BackendUnavailable:
                self._lease = None
                logger.warning(
                    "scheduler lease backend unavailable",
                    extra={
                        "deployment_id": self._deployment_id,
                        "holder_id": self._holder_id,
                    },
                )
                raise
            except asyncio.CancelledError:
                self._lease = None
                raise

    async def release(self) -> None:
        """Forget local authority before releasing; a stale release remains an explicit error."""
        async with self._lock:
            lease, self._lease = self._lease, None
            if lease is not None:
                await self._provider.release_scheduler_lease(lease)
