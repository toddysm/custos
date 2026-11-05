"""Own bounded scheduled, recovery, demand and retention passes for one application lifespan."""

from __future__ import annotations

import asyncio
import logging
import math
import time
from collections.abc import Awaitable, Callable
from functools import partial
from uuid import uuid4

from custos_spl.errors import LeaseExpired, SPLError
from custos_spl.ids import WorkspaceId
from custos_spl.interfaces.receiver_progress import (
    ReceiverScan,
    ReceiverScanProgressProvider,
    ScanContinuation,
    ScanOperation,
    scan_delay,
)
from custos_spl.interfaces.receiver_workspaces import ReceiverWorkspaceProvider
from opentelemetry import metrics

from custos_trigger.clients.pull_demand import DaprPullDemandClient
from custos_trigger.dead_letters import ADMIN_TIMEOUT_SECONDS, DeadLetterAdmin, DeadLetterAdminError
from custos_trigger.errors import TriggerError
from custos_trigger.events import NormalizedEvent
from custos_trigger.pipeline.dispatch import AuditSink
from custos_trigger.pipeline.reliable import (
    MAX_DISPATCH_SECONDS,
    SOURCE_RECONCILE_QUERY_SECONDS,
    ReliableDispatcher,
)
from custos_trigger.receivers.scheduled import ScheduledReceiver
from custos_trigger.receivers.webhook import WebhookIngressError
from custos_trigger.scheduler import SchedulerLeadership

logger = logging.getLogger(__name__)
_meter = metrics.get_meter("custos_trigger.receivers")
_passes = _meter.create_counter("custos_trigger_receiver_passes_total")
_duration = _meter.create_histogram("custos_trigger_receiver_pass_seconds", unit="s")
_checkpoints = _meter.create_counter("custos_trigger_receiver_checkpoints_total")
_expired = _meter.create_counter("custos_trigger_receiver_claim_expirations_total")
_discovered = _meter.create_counter("custos_trigger_receiver_discovered_total")
_pruned = _meter.create_counter("custos_trigger_receiver_progress_pruned_total")
SCAN_LEASE_SECONDS = 360
DISCOVERY_LEASE_SECONDS = 30


class ReceiverRuntime:
    """One task per independent operation; all effects use existing fenced service collaborators."""

    def __init__(
        self,
        workspaces: ReceiverWorkspaceProvider,
        reliable: ReliableDispatcher,
        demand: DaprPullDemandClient,
        publish: Callable[[NormalizedEvent], Awaitable[None]],
        audit: AuditSink,
        *,
        deployment_id: str,
        interval_seconds: float = 1,
        drain_seconds: float = 330,
        lease_seconds: int = 30,
        renew_seconds: int = 10,
    ) -> None:
        """Bind deployment identity and borrowed transports without starting tasks."""
        for value in (interval_seconds, drain_seconds):
            if not math.isfinite(value) or value <= 0:
                raise ValueError("receiver timing must be finite and positive")
        scan_delay(interval_seconds)
        if not isinstance(reliable.store, ReceiverScanProgressProvider):
            raise ValueError("receiver workers require durable scan-progress storage")
        if SCAN_LEASE_SECONDS <= MAX_DISPATCH_SECONDS + 2 * SOURCE_RECONCILE_QUERY_SECONDS:
            raise ValueError("scan lease must cover claim, page and checkpoint deadlines")
        self.workspaces, self.reliable, self.demand = workspaces, reliable, demand
        self.progress = reliable.store
        self.deployment_id, self.holder = deployment_id, uuid4().hex
        self.publish, self.audit = publish, audit
        self.interval, self.drain_seconds = interval_seconds, drain_seconds
        self.leadership = SchedulerLeadership(
            reliable.store,
            deployment_id=deployment_id,
            holder_id=self.holder,
            lease_seconds=lease_seconds,
            renew_seconds=renew_seconds,
        )
        self.admin = DeadLetterAdmin(reliable)
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []
        self._failures: dict[str, bool] = {}
        self._close_task: asyncio.Task[None] | None = None

    @property
    def healthy(self) -> bool:
        """A failed or draining task cannot be reported as a ready receiver runtime."""
        return (
            bool(self._tasks)
            and not self._stop.is_set()
            and not any(self._failures.values())
            and all(not task.done() for task in self._tasks)
        )

    async def start(self) -> None:
        """Check every required storage capability before launching any background task."""
        if self._tasks or self._stop.is_set():
            raise RuntimeError("receiver runtime cannot be restarted")
        async with asyncio.timeout(SOURCE_RECONCILE_QUERY_SECONDS):
            await self.workspaces.check_receiver_workspaces_ready()
            await self.progress.check_receiver_scan_progress_ready()
            await self.reliable.store.check_delivery_ready()
            await self.reliable.store.check_dead_letters_ready()
            await self.leadership.check_ready()
        operations: list[tuple[str, Callable[[], Awaitable[None]]]] = [
            ("discovery", self._discover),
            *(
                (operation.value, partial(self._run_operation, operation))
                for operation in ScanOperation
            ),
        ]
        for name, operation in operations:
            self._failures[name] = False
            task = asyncio.create_task(self._work(name, operation), name=f"trigger-{name}")
            task.add_done_callback(self._completed)
            self._tasks.append(task)

    def _completed(self, task: asyncio.Task[None]) -> None:
        """Observe fatal exits without exposing messages or retained payloads."""
        if not task.cancelled() and (error := task.exception()) is not None:
            logger.error("Receiver task %s failed: %s", task.get_name(), type(error).__name__)

    async def _work(self, name: str, operation: Callable[[], Awaitable[None]]) -> None:
        """Drive one bounded pass independently; durable rows retain all workspace progress."""
        while not self._stop.is_set():
            started = time.monotonic()
            outcome = "success"
            try:
                await operation()
            except (
                SPLError,
                TriggerError,
                DeadLetterAdminError,
                WebhookIngressError,
                TimeoutError,
            ) as exc:
                self._failures[name] = True
                outcome = "unavailable"
                logger.warning("Receiver %s unavailable: %s", name, type(exc).__name__)
            else:
                self._failures[name] = False
            _passes.add(1, {"receiver": name, "outcome": outcome})
            _duration.record(time.monotonic() - started, {"receiver": name})
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.interval)
            except TimeoutError:
                continue

    async def _discover(self) -> None:
        """Register a bounded page independently; failed discovery never resets progress."""
        async with asyncio.timeout(SOURCE_RECONCILE_QUERY_SECONDS):
            scan = await self.progress.claim_receiver_discovery(
                self.deployment_id,
                self.holder,
                lease_seconds=DISCOVERY_LEASE_SECONDS,
            )
            if scan is None:
                return
            page = await self.workspaces.query_receiver_workspaces(cursor=scan.cursor, limit=50)
            await self.progress.commit_receiver_discovery(scan, page.items, page.next_cursor)
        _discovered.add(len(page.items))
        completed = scan.sweep if page.next_cursor is None else scan.completed_sweep
        if completed:
            async with asyncio.timeout(SOURCE_RECONCILE_QUERY_SECONDS):
                count = await self.progress.prune_receiver_scans(
                    self.deployment_id,
                    completed,
                    limit=50,
                )
            _pruned.add(count)

    async def _run_operation(self, operation: ScanOperation) -> None:
        """Claim one fair row, run a bounded page, then fence its acknowledged checkpoint."""
        async with asyncio.timeout(SOURCE_RECONCILE_QUERY_SECONDS):
            lease = None
            if operation is ScanOperation.SCHEDULED:
                lease = await self.leadership.poll(force_renew=True)
                if lease is None:
                    return
            scan = await self.progress.claim_receiver_scan(
                self.deployment_id,
                operation,
                self.holder,
                lease_seconds=SCAN_LEASE_SECONDS,
                scheduler_lease=lease,
            )
        if scan is None:
            return
        committed = False
        try:
            async with asyncio.timeout(MAX_DISPATCH_SECONDS):
                continuation = await self._page(scan)
            try:
                async with asyncio.timeout(SOURCE_RECONCILE_QUERY_SECONDS):
                    await self.progress.checkpoint_receiver_scan(
                        scan,
                        continuation,
                        interval_seconds=self.interval,
                    )
            except (SPLError, TimeoutError):
                _checkpoints.add(1, {"receiver": operation.value, "outcome": "unavailable"})
                raise
            committed = True
            _checkpoints.add(1, {"receiver": operation.value, "outcome": "success"})
        finally:
            if not committed:
                cleanup = asyncio.create_task(self._release(scan))
                interrupted = False
                while not cleanup.done():
                    try:
                        await asyncio.shield(cleanup)
                    except asyncio.CancelledError:
                        interrupted = True
                cleanup.result()
                if interrupted:
                    raise asyncio.CancelledError

    async def _release(self, scan: ReceiverScan) -> None:
        """Join bounded failure cleanup; uncertain committed checkpoints cannot be overwritten."""
        try:
            async with asyncio.timeout(SOURCE_RECONCILE_QUERY_SECONDS):
                await self.progress.release_receiver_scan(scan)
        except LeaseExpired:
            _expired.add(1, {"receiver": str(scan.claim.operation)})
            logger.info("Receiver progress claim no longer owned during release")
        except (SPLError, TimeoutError) as exc:
            logger.warning("Receiver progress release unavailable: %s", type(exc).__name__)
            raise

    async def _page(self, scan: ReceiverScan) -> ScanContinuation:
        """Restore claimed scope only; existing collaborators retain effect authority."""
        workspace, operation = scan.claim.workspace_id, scan.claim.operation
        if workspace is None or operation is None or scan.continuation.operation is not operation:
            raise ValueError("receiver progress claim has invalid operation scope")
        cursor = scan.continuation.cursor
        if operation is ScanOperation.SCHEDULED:
            scheduler = ScheduledReceiver(
                self.reliable.store,
                workspace_id=workspace,
                leadership=self.leadership,
                publish=self.publish,
                audit=self.audit,
                continuation=scan.continuation,
            )
            result = await scheduler.tick()
            if not result.leader:
                raise LeaseExpired("scheduler leadership lost after progress claim")
            return scheduler.snapshot_continuation()
        if operation is ScanOperation.DELIVERY:
            delivery = await self.reliable.reconcile_pending(workspace, cursor=cursor, limit=50)
            return ScanContinuation(operation, delivery.next_cursor)
        if operation is ScanOperation.PULL_DEMAND:
            await self.demand.reconcile(workspace)
            return ScanContinuation(operation)
        async with asyncio.timeout(ADMIN_TIMEOUT_SECONDS):
            retention = await self.admin.sweep(
                workspace,
                cursor=cursor.token if cursor else None,
                limit=50,
            )
            await self.reliable.store.clear_expired_delivery_requests(
                WorkspaceId(workspace), limit=50
            )
        return ScanContinuation(operation, retention.next_cursor)

    async def _drain(self) -> None:
        """Join owned tasks before releasing leadership or borrowed clients."""
        failed = False
        if self._tasks:
            _, pending = await asyncio.wait(self._tasks, timeout=self.drain_seconds)
            if pending:
                logger.warning("Receiver drain deadline reached; joining owned cancellation")
                for task in pending:
                    task.cancel()
            outcomes = await asyncio.gather(*self._tasks, return_exceptions=True)
            failed = any(isinstance(result, Exception) for result in outcomes)
        try:
            async with asyncio.timeout(SOURCE_RECONCILE_QUERY_SECONDS):
                await self.leadership.release()
        except LeaseExpired:
            logger.info("Scheduler leadership expired while draining receiver tasks")
        if failed:
            raise RuntimeError("trigger receiver task failed; inspect safe worker diagnostics")

    async def aclose(self) -> None:
        """Shutdown cancellation never detaches a running pass or closes its transport early."""
        self._stop.set()
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._drain())
        interrupted = False
        while not self._close_task.done():
            try:
                await asyncio.shield(self._close_task)
            except asyncio.CancelledError:
                interrupted = True
        self._close_task.result()
        if interrupted:
            raise asyncio.CancelledError
