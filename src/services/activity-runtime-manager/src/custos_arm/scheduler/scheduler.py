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
``(workspaceId, runId, stepId, attempt)`` quadruple is the dedup key. When the
Execution Store already holds a terminal record for the key, the Scheduler
returns the cached result envelope without launching a second sandbox. A
per-key :class:`asyncio.Lock` serializes concurrent calls for the same key so
the get/insert/drive sequence cannot race two sandboxes into existence.

**Crash reconciliation.** Durable ownership never expires into permission to
relaunch an unknown effect. The original process may resume monitoring, but a
replacement process must retain the pending fence until termination is proven.
Only explicitly non-durable legacy test adapters retain the earlier relaunch path.
An unconfirmed owned attempt loses credential admission before its worker returns.
Revocation outages retain the worker/context for retry; shutdown also drains
retained contexts whose original request workers have already finished.

The driver lifecycle methods are synchronous and blocking; the Scheduler
offloads each onto a worker thread via :func:`asyncio.to_thread` so the event
loop is never blocked while a sandbox runs.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import secrets
from collections.abc import Callable
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from enum import StrEnum
from itertools import chain
from typing import Final
from weakref import WeakValueDictionary

from custos_spl.errors import SPLError

from custos_arm.config import Settings
from custos_arm.contract import (
    ActivitySpec,
    CtxEnvelope,
    ErrorEnvelope,
    ImageRef,
    OutputsEnvelope,
)
from custos_arm.contract.errors import ErrorClass
from custos_arm.io import IOBroker
from custos_arm.limit import EffectiveResources, ResourceLimiter
from custos_arm.observe import (
    STAGE_FINALIZE,
    STAGE_MATERIALIZE,
    STAGE_RESOLVE,
    STAGE_RUN,
    observe_attempt,
    observe_stage,
    record_result,
)
from custos_arm.resolve import ActivityResolver, ActivityTypeVersion
from custos_arm.resolve.resolver import ScopedCatalogActivityResolver
from custos_arm.result import ActivityResultEnvelope, ResultClass, ResultMapper
from custos_arm.runtime import (
    CancelReason,
    RuntimeDriver,
    RuntimeDriverDispatcher,
    SandboxHandle,
    SandboxOutcome,
    SandboxPlan,
    SidecarSpec,
    TmpfsMount,
)
from custos_arm.runtime.models import SIDECAR_PRIVATE_MOUNT
from custos_arm.runtime.oci import SYSTEM_SANDBOX_FAILURE, signal_error_code
from custos_arm.runtime.oci.errors import SandboxFailureError
from custos_arm.secrets import SecretInjector, SidecarToken
from custos_arm.store import ActivityExecution, ExecutionRepository, ExecutionState
from custos_arm.store.execution import UnsafeExecutionError

from .errors import synthesize_failure
from .fsio import (
    FilesystemArtifactReader,
    FilesystemInputArtifactWriter,
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
logger = logging.getLogger(__name__)
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


class CancelOutcome(StrEnum):
    """The idempotent result of a ``CancelActivity`` lookup.

    Mirrors the design's § Internal RPC cancel semantics so the RPC adapter
    can render the documented status codes without leaking Scheduler
    internals:

    * :attr:`ACCEPTED` — a live (non-terminal) attempt exists; the cancel is
      accepted (``200``/``204``).
    * :attr:`TERMINATED` — the attempt already reached a terminal state; the
      cancel is a no-op (``409``).
    * :attr:`UNKNOWN` — the step is unknown to this Scheduler (``404``).
    """

    ACCEPTED = "accepted"
    TERMINATED = "terminated"
    UNKNOWN = "unknown"


class RuntimeAdmissionClosed(RuntimeError):
    """A shutdown/capacity refusal is retryable and has not accepted an activity effect."""

    code = "activity.runtime_unavailable"
    error_class = ErrorClass.RETRYABLE


@dataclass(frozen=True, slots=True)
class _AttemptContext:
    """The in-process state needed to resume monitoring a live attempt."""

    handle: SandboxHandle
    resolved: ActivityTypeVersion
    driver: RuntimeDriver
    deadline: datetime
    bootstrap: SidecarToken | None = None


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
        self._closing = False
        self._workers: set[asyncio.Task[ActivityResultEnvelope]] = set()
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
        self._locks: WeakValueDictionary[ExecutionKey, asyncio.Lock] = WeakValueDictionary()

    async def schedule(self, request: ScheduleRequest) -> ActivityResultEnvelope:
        """Run (or replay / reconcile) one attempt and return its result.

        A per-key lock serializes concurrent calls for the same execution key
        so the get/insert/drive sequence cannot race two sandboxes into
        existence and the idempotent-replay guarantee holds under at-least-once
        delivery.
        The lifespan owns the worker even if the HTTP caller disconnects, and
        admission is bounded to 64 concurrent requests.
        """
        if self._closing or len(self._workers) + self._repo.pending_count >= 64:
            logger.warning("Activity runtime admission is closed or at capacity")
            return synthesize_failure(
                RuntimeAdmissionClosed("runtime admission unavailable"),
                attempt=request.step.attempt,
            )
        worker = asyncio.create_task(self._admitted(request), name="activity-attempt")
        self._workers.add(worker)
        worker.add_done_callback(self._worker_done)
        return await asyncio.shield(worker)

    def _worker_done(self, worker: asyncio.Task[ActivityResultEnvelope]) -> None:
        """Observe detached request failures without serializing credential-bearing exceptions."""
        self._workers.discard(worker)
        if not worker.cancelled() and worker.exception() is not None:
            logger.error("Owned activity worker failed; durable ownership must be reconciled")

    async def drain(self) -> None:
        """Close authority and request stops for workers and retained contexts before closure.

        Cancellation of shutdown waits for that drain; stop requests never prove
        termination or remove the durable unknown-effect fence.
        """
        self._closing = True
        draining = asyncio.create_task(self._drain_workers(), name="activity-runtime-drain")
        cancelled = False
        while True:
            try:
                await asyncio.shield(draining)
                break
            except asyncio.CancelledError:
                if draining.cancelled():
                    raise
                cancelled = True
        if cancelled:
            raise asyncio.CancelledError

    async def _drain_workers(self) -> None:
        """Retry revocation/stop for retained contexts even without a request worker to join."""
        cancelled: set[ExecutionKey] = set()
        reported: set[ExecutionKey] = set()
        stop_reported: set[ExecutionKey] = set()
        try:
            while self._workers or self._context.keys() - cancelled:
                for key, context in tuple(self._context.items()):
                    if key in cancelled:
                        continue
                    try:
                        await self._repo.revoke_credentials(key)
                    except (SPLError, UnsafeExecutionError):
                        if key not in reported:
                            logger.error(
                                "Credential revocation failed; stop deferred and ownership retained"
                            )
                            reported.add(key)
                        continue
                    if self._context.get(key) is not context:
                        continue
                    try:
                        await asyncio.to_thread(
                            context.driver.cancel, context.handle, CancelReason.SHUTDOWN
                        )
                    except SandboxFailureError:
                        if key not in stop_reported:
                            logger.error("Sandbox stop request failed; retaining context for retry")
                            stop_reported.add(key)
                        continue
                    cancelled.add(key)
                if self._workers:
                    await asyncio.wait(tuple(self._workers), timeout=0.1)
                elif self._context.keys() - cancelled:
                    await asyncio.sleep(0.1)
        finally:
            if self._workers:
                await asyncio.gather(*tuple(self._workers), return_exceptions=True)

    async def _admitted(self, request: ScheduleRequest) -> ActivityResultEnvelope:
        """Close unknown-effect credential admission before releasing the owned worker."""
        key = self._key(request)
        try:
            async with self._locks.setdefault(key, asyncio.Lock()):
                try:
                    return await self._schedule(key, request)
                finally:
                    if self._repo.durable and key in self._context:
                        await self._close_credential_admission(key)
        except UnsafeExecutionError as exc:
            return synthesize_failure(exc, attempt=request.step.attempt)

    async def _close_credential_admission(self, key: ExecutionKey) -> None:
        """Retain the worker until revocation succeeds, without claiming termination."""
        reported = False
        while True:
            try:
                await self._repo.revoke_credentials(key)
                return
            except (SPLError, UnsafeExecutionError):
                if not reported:
                    logger.error("Credential revocation failed; owned attempt retained for retry")
                    reported = True
                await asyncio.sleep(0.1)

    async def _schedule(
        self, key: ExecutionKey, request: ScheduleRequest
    ) -> ActivityResultEnvelope:
        existing = await self._repo.get(*key)
        if existing is not None:
            if self._repo.durable and existing.request_hash != self._request_hash(request):
                raise UnsafeExecutionError("activity coordinates are bound to different inputs")
            if existing.is_terminal:
                return self._replay(key, existing)
            return await self._reconcile(key, request)

        started = self._now()
        deadline = self._clamp(started + self._settings.max_timeout, request.step_deadline)
        grant_hash = None
        scopes: dict[str, dict[str, object]] = {}
        if self._settings.credential_proxy_url:
            identity = request.execution_identity
            if (
                not self._repo.durable
                or identity is None
                or identity.workspace_id != request.workspace_id
                or identity.principal_id != request.principal_id
                or any(context.binding is None for context in request.connector_contexts)
            ):
                raise UnsafeExecutionError("durable scoped activity authority is required")
            proof = secrets.token_urlsafe(32)
            request = replace(request, credential_proof=proof)
            grant_hash = hashlib.sha256(proof.encode()).hexdigest()
            scopes = {
                context.slot_name: {
                    "instanceId": context.connector_instance_id,
                    "kind": context.connector_type,
                    "capabilities": list(context.capabilities),
                }
                for context in request.connector_contexts
            }
        execution = ActivityExecution(
            workspace_id=request.workspace_id,
            run_id=request.step.run_id,
            step_id=request.step.step_id,
            attempt=request.step.attempt,
            activity_ref=request.activity_ref,
            deadline=deadline,
            started_at=started,
            request_hash=self._request_hash(request),
            execution_identity=(
                request.execution_identity.to_dict() if request.execution_identity else None
            ),
            credential_scopes=scopes,
            credential_grant_hash=grant_hash,
        )
        await self._repo.insert(execution)
        return await self._drive(key, request)

    async def cancel(self, *, workspace_id: str, run_id: str, step_id: str) -> CancelOutcome:
        """Cancel the live attempt for a step, idempotently.

        ``CancelActivity`` carries only ``(runId, stepId)`` on the wire; the
        workspace comes from the verified call context. The Scheduler inspects
        the attempts it is tracking for the step and classifies the request:

        * a live (non-terminal) attempt is driven to ``cancelled`` through its
          runtime driver (``cancel(reason=cancelled)``) and yields
          :attr:`CancelOutcome.ACCEPTED`. The in-flight ``await_terminal``
          observes the recorded cancellation and returns a ``cancelled``
          outcome, so the original ``schedule`` call finalizes the attempt with
          class ``cancelled`` (``activity.cancelled``);
        * an already-terminal attempt yields :attr:`CancelOutcome.TERMINATED`;
        * an unknown step yields :attr:`CancelOutcome.UNKNOWN`.

        Cancellation is idempotent: :meth:`RuntimeDriver.cancel` records the
        reason and tolerates a missing sandbox, so repeated requests (or a
        request racing the natural termination) converge on the same outcome.

        The lookup is scoped to the attempts this replica is tracking
        in-memory. ``CancelActivity`` is idempotent and the caller collapses
        both ``404`` and ``409`` to a no-op, so a request that lands on a
        replica which never saw the matching ``ScheduleActivity`` (Dapr
        load-balancing, or a restart) is safely reported ``UNKNOWN``.

        The deadline path is enforced separately: ``schedule`` clamps the
        attempt deadline by ``ARM_MAX_TIMEOUT`` and the step deadline, and the
        driver's ``await_terminal`` self-cancels with ``reason=deadline`` when
        it elapses, synthesizing ``activity.timeout``.
        """
        prefix = (workspace_id, run_id, step_id)
        keys = {key for key in chain(self._context, self._cache) if key[:3] == prefix}
        for key in keys:
            context = self._context.get(key)
            if context is None:
                continue
            record = await self._repo.get(*key)
            if record is not None and not record.is_terminal:
                await self._repo.revoke_credentials(key)
                await asyncio.to_thread(
                    context.driver.cancel, context.handle, CancelReason.CANCELLED
                )
                return CancelOutcome.ACCEPTED
        for key in keys:
            if key in self._cache:
                return CancelOutcome.TERMINATED
            record = await self._repo.get(*key)
            if record is not None and record.is_terminal:
                return CancelOutcome.TERMINATED
        return CancelOutcome.UNKNOWN

    # -- Pipeline ---------------------------------------------------------

    async def _drive(self, key: ExecutionKey, request: ScheduleRequest) -> ActivityResultEnvelope:
        driver: RuntimeDriver | None = None
        handle: SandboxHandle | None = None
        start_requested = False
        with observe_attempt(request):
            try:
                with observe_stage(STAGE_RESOLVE):
                    await self._ensure(key, ExecutionState.RESOLVING)
                    if isinstance(self._resolver, ScopedCatalogActivityResolver):
                        if request.execution_identity is None:
                            raise UnsafeExecutionError("pinned execution identity is required")
                        resolved = await self._resolver.resolve_execution(
                            workspace_id=request.workspace_id,
                            activity_ref=request.activity_ref,
                            identity=request.execution_identity,
                        )
                    else:
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
                    deadline = self._clamp(
                        current.started_at + effective.timeout, request.step_deadline
                    )
                    driver = self._dispatcher.select(resolved.runtime.kind)
                    plan = self._build_plan(request, resolved, effective, deadline)
                with observe_stage(STAGE_MATERIALIZE):
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
                    await self._materialize_input_artifacts(request, handle)
                    bootstrap = await self._inject(request, resolved, handle, deadline)
                    self._context[key] = replace(self._context[key], bootstrap=bootstrap)
                with observe_stage(STAGE_RUN):
                    await self._ensure(key, ExecutionState.RUNNING)
                    start_requested = True
                    await asyncio.to_thread(driver.start, handle)
                    outcome = await asyncio.to_thread(driver.await_terminal, handle, deadline)
                    if self._repo.durable and outcome.signal.value in {"deadline", "cancelled"}:
                        raise UnsafeExecutionError(
                            "activity termination is unconfirmed; ownership remains fenced"
                        )
                return await self._finalize(key, request, resolved, driver, handle, outcome)
            except (UnsafeExecutionError, SPLError):
                raise
            except Exception as exc:
                if self._repo.durable and start_requested:
                    raise UnsafeExecutionError(
                        "activity result is unconfirmed; ownership remains fenced"
                    ) from None
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
        with observe_stage(STAGE_FINALIZE):
            await self._ensure(key, ExecutionState.FINALIZING)
            bundle = await asyncio.to_thread(driver.collect, handle)
            raw = read_outputs(bundle.root, max_bytes=self._settings.output_max_bytes)
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
        if existing.result is not None:
            return existing.result
        if existing.state is ExecutionState.SUCCEEDED:
            raise UnsafeExecutionError("legacy successful attempt has no retained result")
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
        """Resume owned monitoring without relaunch; close admission if its local handle is lost."""
        context = self._context.get(key)
        if context is None:
            if self._repo.durable:
                await self._close_credential_admission(key)
                raise UnsafeExecutionError("activity ownership cannot be reconstructed safely")
            # No live sandbox to resume (e.g. lost after a restart): relaunch.
            return await self._drive(key, request)
        try:
            outcome = await asyncio.to_thread(
                context.driver.await_terminal, context.handle, context.deadline
            )
            if self._repo.durable:
                if outcome.signal.value in {"deadline", "cancelled"}:
                    raise UnsafeExecutionError(
                        "activity termination is unconfirmed; ownership remains fenced"
                    )
                return await self._finalize(
                    key, request, context.resolved, context.driver, context.handle, outcome
                )
        except Exception as exc:
            if self._repo.durable:
                raise UnsafeExecutionError(
                    "activity result is unconfirmed; ownership remains fenced"
                ) from None
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
            result=envelope,
        )
        if not self._repo.durable:
            self._cache[key] = envelope
        context = self._context.pop(key, None)
        if context is not None and context.bootstrap is not None:
            self._injector.revoke(token=context.bootstrap)
        record_result(envelope)
        if driver is not None and handle is not None:
            await self._safe_cleanup(driver, handle)
        return envelope

    @staticmethod
    def _request_hash(request: ScheduleRequest) -> str:
        """Bind accepted inputs, caller and opaque contexts without persisting secret material."""
        payload = {
            "activity_ref": request.activity_ref,
            "inputs": dict(request.inputs),
            "principal_id": request.principal_id,
            "execution_identity": (
                request.execution_identity.to_dict() if request.execution_identity else None
            ),
            "connectors": {
                name: ref.model_dump(mode="json") for name, ref in request.connectors.items()
            },
            "contexts": [
                {
                    "slot": context.slot_name,
                    "type": context.connector_type,
                    "instance": context.connector_instance_id,
                    "lease": context.lease_id,
                    "secrets": dict(context.secrets),
                    "binding": dict(context.binding) if context.binding is not None else None,
                    "capabilities": context.capabilities,
                }
                for context in sorted(request.connector_contexts, key=lambda value: value.slot_name)
            ],
            "deadline": request.step_deadline.isoformat() if request.step_deadline else None,
            "override": asdict(request.override) if request.override is not None else None,
            "ceiling": asdict(request.cluster_ceiling)
            if request.cluster_ceiling is not None
            else None,
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        ).hexdigest()

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
        # Production renders every activity as ``image@digest`` so the running
        # bits are content-addressed. When the operator opts into unpinned
        # images (``ARM_ALLOW_UNPINNED_IMAGES``, test/dev only) the activity may
        # run from a locally loaded image (e.g. a ``kind load``ed tag) that no
        # registry digest can satisfy, so we render the tag only. Provenance is
        # unaffected — the resolved digest is still recorded from the manifest.
        if self._settings.allow_unpinned_images:
            image = ImageRef.model_validate({"ref": resolved.runtime.image})
        else:
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
                TmpfsMount(mount_path=SIDECAR_PRIVATE_MOUNT, read_only=True, size_limit="16Ki"),
                *(
                    (TmpfsMount(mount_path="/custos/run", size_limit="1Mi"),)
                    if self._settings.credential_proxy_url
                    else ()
                ),
            ),
            sidecar=SidecarSpec(
                image=self._settings.sidecar_image,
                endpoint=self._settings.connector_endpoint,
                credential_broker=bool(self._settings.credential_proxy_url),
            ),
            io_bridge_image=self._settings.io_bridge_image,
            deadline=deadline,
            allow_unpinned_images=self._settings.allow_unpinned_images,
            workspace_id=request.workspace_id,
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

    async def _materialize_input_artifacts(
        self, request: ScheduleRequest, handle: SandboxHandle
    ) -> None:
        """Stage every consumed ``ArtifactRef`` input under ``/custos/in/artifacts``.

        Runs before ``start()`` so the input bridge streams the materialized
        files into the pod alongside ``inputs.json``.
        """
        await self._broker.materialize_input_artifacts(
            inputs=dict(request.inputs),
            workspace_id=request.workspace_id,
            writer=FilesystemInputArtifactWriter(handle.input_root),
        )

    async def _inject(
        self,
        request: ScheduleRequest,
        resolved: ActivityTypeVersion,
        handle: SandboxHandle,
        deadline: datetime,
    ) -> SidecarToken:
        """Bind bootstrap issuance to the actual workspace and clamped execution deadline."""
        result = await self._injector.inject(
            sink=FilesystemSecretSink(handle.input_root, private_root=handle.sidecar_root),
            step=request.step,
            connectors=resolved.connectors,
            contexts=request.connector_contexts,
            workspace_id=request.workspace_id,
            deadline=deadline,
        )
        if request.credential_proof is not None:
            bundle = {
                "workspaceId": request.workspace_id,
                "runId": request.step.run_id,
                "stepId": request.step.step_id,
                "attempt": request.step.attempt,
                "proof": request.credential_proof,
                "endpoint": self._settings.credential_proxy_url,
            }
            await FilesystemSecretSink(
                handle.input_root,
                private_root=handle.sidecar_root,
            ).write_private_secret(
                relative_path="runtime.json",
                content=json.dumps(bundle).encode(),
                mode=0o400,
            )
        return result.token

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


__all__ = ["ActivityScheduler", "CancelOutcome", "ExecutionKey"]
