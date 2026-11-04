"""Own bounded scheduled, recovery, demand and retention passes for one application lifespan."""

from __future__ import annotations

import asyncio
import logging
import math
import time
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from uuid import uuid4

from custos_spl.errors import LeaseExpired, SPLError
from custos_spl.ids import WorkspaceId
from custos_spl.interfaces.receiver_workspaces import ReceiverWorkspaceProvider
from custos_spl.pagination import Cursor
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


@dataclass
class _Progress:
    """Keep one admitted workspace's paging state inside the bounded active queue."""

    cursor: Cursor | None = None
    scheduler: ScheduledReceiver | None = None


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
        self.workspaces, self.reliable, self.demand = workspaces, reliable, demand
        self.publish, self.audit = publish, audit
        self.interval, self.drain_seconds = interval_seconds, drain_seconds
        self.leadership = SchedulerLeadership(
            reliable.store,
            deployment_id=deployment_id,
            holder_id=uuid4().hex,
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
            await self.reliable.store.check_delivery_ready()
            await self.reliable.store.check_dead_letters_ready()
            await self.leadership.check_ready()
        operations = (
            ("scheduled", self._schedule),
            ("delivery", self._deliver),
            ("pull-demand", self._pull),
            ("retention", self._retain),
        )
        for name, operation in operations:
            self._failures[name] = False
            task = asyncio.create_task(self._work(name, operation), name=f"trigger-{name}")
            task.add_done_callback(self._completed)
            self._tasks.append(task)

    def _completed(self, task: asyncio.Task[None]) -> None:
        """Observe fatal exits without exposing messages or retained payloads."""
        if not task.cancelled() and (error := task.exception()) is not None:
            logger.error("Receiver task %s failed: %s", task.get_name(), type(error).__name__)

    async def _work(
        self, name: str, operation: Callable[[str, _Progress], Awaitable[bool]]
    ) -> None:
        """Rotate after each page, retaining at most 50 active workspace continuations."""
        cursor: Cursor | None = None
        workspaces: deque[tuple[str, _Progress]] = deque()
        while not self._stop.is_set():
            started = time.monotonic()
            outcome = "success"
            try:
                if len(workspaces) < 50:
                    async with asyncio.timeout(SOURCE_RECONCILE_QUERY_SECONDS):
                        page = await self.workspaces.query_receiver_workspaces(
                            cursor=cursor, limit=50 - len(workspaces)
                        )
                    active = {workspace for workspace, _ in workspaces}
                    workspaces.extend(
                        (str(ws), _Progress()) for ws in page.items if str(ws) not in active
                    )
                    cursor = page.next_cursor
                if workspaces:
                    workspace, progress = workspaces.popleft()
                    if await operation(workspace, progress):
                        workspaces.append((workspace, progress))
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

    async def _schedule(self, workspace: str, progress: _Progress) -> bool:
        """Preserve due-page progress and publish through the normal acknowledged broker path."""
        if progress.scheduler is None:
            progress.scheduler = ScheduledReceiver(
                self.reliable.store,
                workspace_id=workspace,
                leadership=self.leadership,
                publish=self.publish,
                audit=self.audit,
            )
        async with asyncio.timeout(MAX_DISPATCH_SECONDS):
            result = await progress.scheduler.tick()
        return result.has_more

    async def _deliver(self, workspace: str, progress: _Progress) -> bool:
        """A null cursor ends a pass; future passes still retry pending targets."""
        result = await self.reliable.reconcile_pending(workspace, cursor=progress.cursor, limit=50)
        progress.cursor = result.next_cursor
        return progress.cursor is not None

    async def _pull(self, workspace: str, progress: _Progress) -> bool:
        """Send a bounded hint only; Connector retains all upstream polling authority."""
        await self.demand.reconcile(workspace)
        return False

    async def _retain(self, workspace: str, progress: _Progress) -> bool:
        """Run audited retention without deleting receipt identities or schedule replay proofs."""
        async with asyncio.timeout(ADMIN_TIMEOUT_SECONDS):
            result = await self.admin.sweep(
                workspace, cursor=progress.cursor.token if progress.cursor else None, limit=50
            )
            await self.reliable.store.clear_expired_delivery_requests(
                WorkspaceId(workspace), limit=50
            )
        progress.cursor = result.next_cursor
        return progress.cursor is not None

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
