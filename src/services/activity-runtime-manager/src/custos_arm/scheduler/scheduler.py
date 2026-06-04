"""The Activity Scheduler — the attempt state machine (ARM-IMPL-017).

The Scheduler orchestrates one activity attempt end-to-end:

    resolve → limit → prepare → materialize inputs → inject secrets →
    run via driver → finalize outputs → map result → persist

It owns the execution state machine (:class:`~custos_arm.store.ExecutionState`),
idempotent replay, and crash reconciliation. Every collaborator is a pure or
single-purpose sub-module; the Scheduler is the only place that wires them in
order and the only place domain errors become an
:class:`~custos_arm.result.ActivityResultEnvelope`.

**Idempotent replay.** ``ScheduleActivity`` is at-least-once; the
``(runId, stepId, attempt)`` triple is the dedup key. When the Execution Store
already holds a terminal record for the triple, the Scheduler returns the
cached result envelope without launching a second sandbox.

**Crash reconciliation.** When a record exists in a non-terminal state, the
Scheduler reconciles against the live sandbox: if it still holds the in-flight
context it resumes monitoring; otherwise it relaunches the attempt. (Durable
cross-restart adoption of an orphaned ``Job`` additionally depends on the
deterministic ``Job`` name — see :func:`~custos_arm.runtime.oci.job.job_name`.)

The driver lifecycle methods are synchronous and blocking; the Scheduler
offloads each onto a worker thread via :func:`asyncio.to_thread` so the event
loop is never blocked while a sandbox runs.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Final

from custos_arm.config import Settings
from custos_arm.contract import (
    ActivitySpec,
    CtxEnvelope,
    ErrorEnvelope,
    ImageRef,
    OutputsEnvelope,
)
from custos_arm.io import IOBroker
from custos_arm.limit import EffectiveResources, ResourceLimiter
from custos_arm.resolve import ActivityResolver, ActivityTypeVersion
from custos_arm.result import ActivityResultEnvelope, ResultClass, ResultMapper
from custos_arm.runtime import (
    RuntimeDriver,
    RuntimeDriverDispatcher,
    SandboxHandle,
    SandboxOutcome,
    SandboxPlan,
    SidecarSpec,
    TmpfsMount,
)
from custos_arm.runtime.oci import SYSTEM_SANDBOX_FAILURE, signal_error_code
from custos_arm.secrets import SecretInjector
from custos_arm.store import ActivityExecution, ExecutionRepository, ExecutionState

from .errors import synthesize_failure
from .fsio import (
    FilesystemArtifactReader,
    FilesystemSecretSink,
    read_outputs,
    write_ctx,
    write_inputs,
)
from .request import ScheduleRequest

#: The idempotency key — ``(workspaceId, runId, stepId, attempt)``.
ExecutionKey = tuple[str, str, str, int]

#: Contract mount paths realized as ``tmpfs`` in the sandbox.
CUSTOS_IN: Final[str] = "/custos/in"
CUSTOS_OUT: Final[str] = "/custos/out"

#: The forward (happy-path) progression of live states.
_NEXT: Final[dict[ExecutionState, ExecutionState]] = {
    ExecutionState.PENDING: ExecutionState.RESOLVING,
    ExecutionState.RESOLVING: ExecutionState.MATERIALIZING,
    ExecutionState.MATERIALIZING: ExecutionState.RUNNING,
    ExecutionState.RUNNING: ExecutionState.FINALIZING,
}
_HAPPY_ORDER: Final[tuple[ExecutionState, ...]] = (
    ExecutionState.PENDING,
    ExecutionState.RESOLVING,
    ExecutionState.MATERIALIZING,
    ExecutionState.RUNNING,
    ExecutionState.FINALIZING,
)

#: The terminal state each result class persists as.
_TERMINAL_BY_CLASS: Final[dict[ResultClass, ExecutionState]] = {
    ResultClass.SUCCESS: ExecutionState.SUCCEEDED,
    ResultClass.RETRYABLE: ExecutionState.FAILED,
    ResultClass.PERMANENT: ExecutionState.FAILED,
    ResultClass.CANCELLED: ExecutionState.CANCELLED,
}


@dataclass(frozen=True, slots=True)
class _AttemptContext:
    """The in-process state needed to resume monitoring a live attempt."""

    handle: SandboxHandle
    resolved: ActivityTypeVersion
    driver: RuntimeDriver
    deadline: datetime


class ActivityScheduler:
    """Drives one activity attempt through its lifecycle state machine."""

    def __init__(
        self,
        *,
        resolver: ActivityResolver,
        limiter: ResourceLimiter,
        broker: IOBroker,
        injector: SecretInjector,
        mapper: ResultMapper,
        dispatcher: RuntimeDriverDispatcher,
        repository: ExecutionRepository,
        settings: Settings,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._resolver = resolver
        self._limiter = limiter
        self._broker = broker
        self._injector = injector
        self._mapper = mapper
        self._dispatcher = dispatcher
        self._repo = repository
        self._settings = settings
        self._now = now or (lambda: datetime.now(UTC))
        self._context: dict[ExecutionKey, _AttemptContext] = {}
        self._cache: dict[ExecutionKey, ActivityResultEnvelope] = {}

    async def schedule(self, request: ScheduleRequest) -> ActivityResultEnvelope:
        """Run (or replay / reconcile) one attempt and return its result."""
        key = self._key(request)
        existing = await self._repo.get(*key)
        if existing is not None:
            if existing.is_terminal:
                return self._replay(key, existing)
            return await self._reconcile(key, request)

        started = self._now()
        deadline = self._clamp(started + self._settings.max_timeout, request.step_deadline)
        execution = ActivityExecution(
            workspace_id=request.workspace_id,
            run_id=request.step.run_id,
            step_id=request.step.step_id,
            attempt=request.step.attempt,
            activity_ref=request.activity_ref,
            deadline=deadline,
            started_at=started,
        )
        await self._repo.insert(execution)
        return await self._drive(key, request)

    # -- Pipeline ---------------------------------------------------------

    async def _drive(self, key: ExecutionKey, request: ScheduleRequest) -> ActivityResultEnvelope:
        driver: RuntimeDriver | None = None
        handle: SandboxHandle | None = None
        try:
            await self._ensure(key, ExecutionState.RESOLVING)
            resolved = await self._resolver.resolve(
                workspace_id=request.workspace_id, activity_ref=request.activity_ref
            )
            effective = self._limiter.limit(
                resources=resolved.resources,
                isolation_floor=resolved.isolation_floor,
                override=request.override,
                cluster_ceiling=request.cluster_ceiling,
            )
            current = await self._require(key)
            deadline = self._clamp(current.started_at + effective.timeout, request.step_deadline)
            driver = self._dispatcher.select(resolved.runtime.kind)
            plan = self._build_plan(request, resolved, effective, deadline)
            handle = await asyncio.to_thread(driver.prepare, plan)
            await self._ensure(
                key,
                ExecutionState.MATERIALIZING,
                resolved_digest=resolved.digest,
                isolation_tier=effective.tier.value,
                runtime_class=effective.runtime_class or None,
                deadline=deadline,
                sandbox_ref=handle.reference,
            )
            self._context[key] = _AttemptContext(
                handle=handle, resolved=resolved, driver=driver, deadline=deadline
            )
            self._materialize(request, resolved, handle, deadline)
            await self._inject(request, resolved, handle)
            await self._ensure(key, ExecutionState.RUNNING)
            await asyncio.to_thread(driver.start, handle)
            outcome = await asyncio.to_thread(driver.await_terminal, handle, deadline)
            return await self._finalize(key, request, resolved, driver, handle, outcome)
        except Exception as exc:
            envelope = synthesize_failure(exc, attempt=request.step.attempt)
            return await self._complete(key, envelope, driver, handle)

    async def _finalize(
        self,
        key: ExecutionKey,
        request: ScheduleRequest,
        resolved: ActivityTypeVersion,
        driver: RuntimeDriver,
        handle: SandboxHandle,
        outcome: SandboxOutcome,
    ) -> ActivityResultEnvelope:
        await self._ensure(key, ExecutionState.FINALIZING)
        bundle = await asyncio.to_thread(driver.collect, handle)
        raw = read_outputs(bundle.root)
        finalized: OutputsEnvelope | None = None
        if raw is not None:
            finalized = await self._broker.finalize_outputs(
                raw_outputs=raw,
                manifest=resolved.manifest,
                step=request.step,
                workspace_id=request.workspace_id,
                artifacts=FilesystemArtifactReader(bundle.root),
            )
        envelope = self._resolve_result(outcome, finalized, request.step.attempt)
        return await self._complete(key, envelope, driver, handle)

    # -- Replay & reconciliation -----------------------------------------

    def _replay(self, key: ExecutionKey, existing: ActivityExecution) -> ActivityResultEnvelope:
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        return self._reconstruct(existing)

    @staticmethod
    def _reconstruct(existing: ActivityExecution) -> ActivityResultEnvelope:
        if existing.state is ExecutionState.SUCCEEDED:
            return ActivityResultEnvelope.model_validate(
                {"class": ResultClass.SUCCESS.value, "attempt": existing.attempt, "outputs": {}}
            )
        result_class = existing.result_class or ResultClass.RETRYABLE.value
        error = ErrorEnvelope.model_validate(
            {
                "code": existing.error_code or SYSTEM_SANDBOX_FAILURE,
                "class": result_class,
                "message": "replayed terminal execution record",
            }
        )
        return ActivityResultEnvelope.model_validate(
            {"class": result_class, "attempt": existing.attempt, "error": error}
        )

    async def _reconcile(
        self, key: ExecutionKey, request: ScheduleRequest
    ) -> ActivityResultEnvelope:
        context = self._context.get(key)
        if context is None:
            # No live sandbox to resume (e.g. lost after a restart): relaunch.
            return await self._drive(key, request)
        try:
            outcome = await asyncio.to_thread(
                context.driver.await_terminal, context.handle, context.deadline
            )
        except Exception as exc:
            envelope = synthesize_failure(exc, attempt=request.step.attempt)
            return await self._complete(key, envelope, context.driver, context.handle)
        return await self._finalize(
            key, request, context.resolved, context.driver, context.handle, outcome
        )

    # -- State machine ----------------------------------------------------

    async def _ensure(
        self, key: ExecutionKey, target: ExecutionState, **changes: object
    ) -> ActivityExecution:
        """Walk the live record forward to ``target``, applying ``changes`` last.

        Forward-only: when the record is already at or past ``target`` (a
        relaunch resuming a partially-advanced attempt) this is a no-op.
        """
        current = await self._require(key)
        target_index = _HAPPY_ORDER.index(target)
        while current.state in _NEXT and _HAPPY_ORDER.index(current.state) < target_index:
            nxt = _NEXT[current.state]
            applied = changes if nxt == target else {}
            current = await self._repo.transition(current, nxt, **applied)
        return current

    async def _complete(
        self,
        key: ExecutionKey,
        envelope: ActivityResultEnvelope,
        driver: RuntimeDriver | None,
        handle: SandboxHandle | None,
    ) -> ActivityResultEnvelope:
        current = await self._require(key)
        await self._repo.transition(
            current,
            _TERMINAL_BY_CLASS[envelope.class_],
            result_class=envelope.class_.value,
            error_code=envelope.error.code if envelope.error else None,
            finished_at=self._now(),
        )
        self._cache[key] = envelope
        self._context.pop(key, None)
        if driver is not None and handle is not None:
            await self._safe_cleanup(driver, handle)
        return envelope

    async def _require(self, key: ExecutionKey) -> ActivityExecution:
        current = await self._repo.get(*key)
        if current is None:  # pragma: no cover - defensive; insert precedes drive
            raise RuntimeError(f"execution record vanished for {key}")
        return current

    # -- Sub-module wiring ------------------------------------------------

    def _build_plan(
        self,
        request: ScheduleRequest,
        resolved: ActivityTypeVersion,
        effective: EffectiveResources,
        deadline: datetime,
    ) -> SandboxPlan:
        image = ImageRef.model_validate(
            {"ref": resolved.runtime.image, "digest": resolved.runtime.digest}
        )
        return SandboxPlan(
            step=request.step,
            namespace=self._settings.sandbox_namespace,
            image=image,
            resources=effective,
            tmpfs_mounts=(
                TmpfsMount(mount_path=CUSTOS_IN, read_only=True),
                TmpfsMount(mount_path=CUSTOS_OUT),
            ),
            sidecar=SidecarSpec(
                image=self._settings.sidecar_image,
                endpoint=self._settings.connector_endpoint,
            ),
            deadline=deadline,
        )

    def _materialize(
        self,
        request: ScheduleRequest,
        resolved: ActivityTypeVersion,
        handle: SandboxHandle,
        deadline: datetime,
    ) -> None:
        activity = ActivitySpec(type=resolved.type, version=resolved.version)
        inputs_envelope = self._broker.materialize_inputs(
            activity=activity,
            step=request.step,
            inputs=dict(request.inputs),
            input_schema=resolved.input_schema,
        )
        write_inputs(handle.input_root, inputs_envelope)
        ctx = CtxEnvelope.model_validate(
            {
                "runId": request.step.run_id,
                "stepId": request.step.step_id,
                "attempt": request.step.attempt,
                "workspaceId": request.workspace_id,
                "activity": activity,
                "connectors": dict(request.connectors),
                "deadline": deadline,
            }
        )
        write_ctx(handle.input_root, ctx)

    async def _inject(
        self,
        request: ScheduleRequest,
        resolved: ActivityTypeVersion,
        handle: SandboxHandle,
    ) -> None:
        await self._injector.inject(
            sink=FilesystemSecretSink(handle.input_root),
            step=request.step,
            connectors=resolved.connectors,
            contexts=request.connector_contexts,
        )

    def _resolve_result(
        self,
        outcome: SandboxOutcome,
        finalized: OutputsEnvelope | None,
        attempt: int,
    ) -> ActivityResultEnvelope:
        signal = signal_error_code(outcome.signal)
        if signal is not None:
            code, error_class = signal
            error = ErrorEnvelope.model_validate(
                {
                    "code": code,
                    "class": error_class.value,
                    "message": f"sandbox terminated by {outcome.signal.value}",
                }
            )
            return ActivityResultEnvelope.model_validate(
                {
                    "class": ResultClass.from_error_class(error_class).value,
                    "attempt": attempt,
                    "error": error,
                }
            )
        return self._mapper.map_result(
            exit_code=outcome.exit_code,
            finalized_outputs=finalized,
            attempt=attempt,
        )

    @staticmethod
    async def _safe_cleanup(driver: RuntimeDriver, handle: SandboxHandle) -> None:
        try:
            await asyncio.to_thread(driver.cleanup, handle)
        except Exception:
            # Reaping is best-effort; a cleanup fault must not mask the result.
            return

    @staticmethod
    def _clamp(deadline: datetime, step_deadline: datetime | None) -> datetime:
        if step_deadline is not None and step_deadline < deadline:
            return step_deadline
        return deadline

    @staticmethod
    def _key(request: ScheduleRequest) -> ExecutionKey:
        return (
            request.workspace_id,
            request.step.run_id,
            request.step.step_id,
            request.step.attempt,
        )


__all__ = ["ActivityScheduler", "ExecutionKey"]
