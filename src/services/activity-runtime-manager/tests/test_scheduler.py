"""Tests for the Activity Scheduler (ARM-IMPL-017)."""

from __future__ import annotations

import asyncio
import json
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from custos_spl.errors import ImmutableViolation
from custos_spl.ids import RunId, StepId, WorkspaceId
from custos_spl.interfaces.metadata_store import MetadataStoreProvider, StepAttempt

from custos_arm.config import Settings, load_settings
from custos_arm.contract import ConnectorRef, ErrorClass, StepRef
from custos_arm.io import IOBroker
from custos_arm.limit import ResourceLimiter
from custos_arm.manifest import parse_manifest
from custos_arm.resolve import ActivityTypeVersion
from custos_arm.resolve.errors import ActivityUnresolvedError, CatalogUnavailableError
from custos_arm.result import ResultClass, ResultMapper
from custos_arm.runtime import (
    OCI_CONTAINER_KIND,
    CancelReason,
    OutputBundle,
    RuntimeDriverDispatcher,
    SandboxHandle,
    SandboxOutcome,
    SandboxPlan,
    SandboxSignal,
)
from custos_arm.scheduler import (
    ActivityScheduler,
    CancelOutcome,
    FilesystemArtifactReader,
    FilesystemSecretSink,
    ScheduleRequest,
    error_envelope_for,
    read_outputs,
    synthesize_failure,
)
from custos_arm.secrets import SecretInjector, SidecarTokenMinter
from custos_arm.store import ActivityExecution, ExecutionRepository, ExecutionState

_NOW = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
_OK = SandboxOutcome(0, SandboxSignal.NONE)
_DIGEST = "sha256:" + "ab" * 32


# ---------------------------------------------------------------------------
# Fakes and builders
# ---------------------------------------------------------------------------


class _FakeMetadataStore:
    """Append-only stand-in for the SPL ``MetadataStoreProvider``."""

    def __init__(self) -> None:
        self.buckets: dict[tuple[str, str, str], list[StepAttempt]] = {}

    async def append_step_attempt(
        self,
        workspace_id: WorkspaceId,
        run_id: RunId,
        step_id: StepId,
        attempt: StepAttempt,
    ) -> StepAttempt:
        bucket = self.buckets.setdefault((workspace_id, run_id, step_id), [])
        if any(existing.attempt == attempt.attempt for existing in bucket):
            raise ImmutableViolation("duplicate step attempt")
        bucket.append(attempt)
        return attempt

    async def get_step_attempts(
        self, workspace_id: WorkspaceId, run_id: RunId, step_id: StepId
    ) -> tuple[StepAttempt, ...]:
        bucket = self.buckets.get((workspace_id, run_id, step_id), [])
        return tuple(sorted(bucket, key=lambda a: a.attempt))


class _StubArtifactStore:
    """An artifact store that must never be touched (no artifacts declared)."""

    async def upload(self, **_kwargs: Any) -> Any:  # pragma: no cover - guard
        raise AssertionError("artifact upload should not be called")


class _FakeDriver:
    """A synchronous :class:`RuntimeDriver` writing a configurable outputs blob."""

    kind = OCI_CONTAINER_KIND

    def __init__(
        self,
        root: Path,
        *,
        outputs: bytes | None,
        outcome: SandboxOutcome = _OK,
        await_error: Exception | None = None,
        cleanup_error: Exception | None = None,
    ) -> None:
        self._root = root
        self._outputs = outputs
        self._outcome = outcome
        self._await_error = await_error
        self._cleanup_error = cleanup_error
        self.prepared = 0
        self.started = 0
        self.awaited = 0
        self.collected = 0
        self.cleaned = 0
        self.cancellations: list[CancelReason] = []
        self.handle: SandboxHandle | None = None

    def prepare(self, plan: SandboxPlan) -> SandboxHandle:
        self.prepared += 1
        in_root = self._root / "in"
        out_root = self._root / "out"
        in_root.mkdir(parents=True, exist_ok=True)
        out_root.mkdir(parents=True, exist_ok=True)
        self.handle = _handle(in_root, out_root)
        return self.handle

    def start(self, handle: SandboxHandle) -> None:
        self.started += 1

    def await_terminal(self, handle: SandboxHandle, deadline: datetime) -> SandboxOutcome:
        self.awaited += 1
        if self._await_error is not None:
            raise self._await_error
        if self._outputs is not None:
            (handle.output_root / "outputs.json").write_bytes(self._outputs)
        return self._outcome

    def cancel(self, handle: SandboxHandle, reason: CancelReason) -> None:
        self.cancellations.append(reason)

    def collect(self, handle: SandboxHandle) -> OutputBundle:
        self.collected += 1
        return OutputBundle(root=handle.output_root)

    def cleanup(self, handle: SandboxHandle) -> None:
        self.cleaned += 1
        if self._cleanup_error is not None:
            raise self._cleanup_error


class _FakeResolver:
    def __init__(
        self, resolved: ActivityTypeVersion | None, *, error: Exception | None = None
    ) -> None:
        self._resolved = resolved
        self._error = error
        self.calls = 0

    async def resolve(self, *, workspace_id: str, activity_ref: str) -> ActivityTypeVersion:
        self.calls += 1
        if self._error is not None:
            raise self._error
        assert self._resolved is not None
        return self._resolved


def _handle(in_root: Path, out_root: Path) -> SandboxHandle:
    return SandboxHandle(
        kind=OCI_CONTAINER_KIND,
        reference="custos-activities/job-x",
        input_root=in_root,
        output_root=out_root,
    )


def _settings(**overrides: str) -> Settings:
    base = {
        "ARM_ARTIFACT_STORE": "artifacts",
        "ARM_METADATA_STORE": "metadata",
        "ARM_CATALOG_ENDPOINT": "http://catalog.svc:8080",
        "ARM_CONNECTOR_ENDPOINT": "http://connector.svc:8080",
        "ARM_SANDBOX_NAMESPACE": "custos-activities",
        "ARM_SIDECAR_IMAGE": "ghcr.io/custos/connector-sidecar:0.1.0",
        "ENVIRONMENT": "development",
    }
    base.update(overrides)
    return load_settings(base)


def _resolved() -> ActivityTypeVersion:
    raw: dict[str, Any] = {
        "apiVersion": "custos.dev/v1",
        "kind": "ActivityManifest",
        "metadata": {
            "type": "echo",
            "version": "1.0.0",
            "namespace": "acme",
            "description": "Echo the input.",
            "owner": "team-acme",
        },
        "spec": {
            "contractVersion": "1",
            "runtime": {
                "kind": "oci-container",
                "image": "ghcr.io/acme/echo:1.0.0",
                "digest": _DIGEST,
            },
            "inputs": {"schema": {"type": "object"}},
            "outputs": {"schema": {"type": "object"}},
            "resources": {"timeout": "PT15M"},
        },
    }
    return ActivityTypeVersion(
        namespace="acme",
        type="echo",
        version="1.0.0",
        digest=_DIGEST,
        manifest=parse_manifest(raw),
    )


def _success_outputs(payload: dict[str, Any] | None = None) -> bytes:
    return json.dumps(
        {
            "schemaVersion": "1",
            "contractVersion": "1",
            "status": "success",
            "outputs": payload if payload is not None else {"ok": True},
        }
    ).encode("utf-8")


def _repo() -> ExecutionRepository:
    store: MetadataStoreProvider = _FakeMetadataStore()  # type: ignore[assignment]
    return ExecutionRepository(store, idempotency_ttl=timedelta(hours=24))


def _scheduler(
    driver: _FakeDriver,
    *,
    resolver: _FakeResolver | None = None,
    repo: ExecutionRepository | None = None,
) -> ActivityScheduler:
    settings = _settings()
    return ActivityScheduler(
        resolver=resolver or _FakeResolver(_resolved()),
        limiter=ResourceLimiter(settings),
        broker=IOBroker(_StubArtifactStore(), output_max_bytes=1_000_000),  # type: ignore[arg-type]
        injector=SecretInjector(token_minter=SidecarTokenMinter()),
        mapper=ResultMapper(),
        dispatcher=RuntimeDriverDispatcher((driver,)),
        repository=repo or _repo(),
        settings=settings,
        now=lambda: _NOW,
    )


def _request(**overrides: Any) -> ScheduleRequest:
    base: dict[str, Any] = {
        "workspace_id": "ws-1",
        "step": StepRef(runId="run-1", stepId="step-1", attempt=1),
        "activity_ref": "acme/echo@1.0.0",
        "inputs": {"message": "hi"},
    }
    base.update(overrides)
    return ScheduleRequest(**base)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


async def test_happy_path_drives_all_submodules_and_persists(tmp_path: Path) -> None:
    driver = _FakeDriver(tmp_path, outputs=_success_outputs())
    repo = _repo()
    scheduler = _scheduler(driver, repo=repo)

    result = await scheduler.schedule(_request())

    assert result.class_ is ResultClass.SUCCESS
    assert result.outputs == {"ok": True}
    assert driver.prepared == 1
    assert driver.started == 1
    assert driver.awaited == 1
    assert driver.collected == 1
    assert driver.cleaned == 1
    record = await repo.get("ws-1", "run-1", "step-1", 1)
    assert record is not None
    assert record.state is ExecutionState.SUCCEEDED
    assert record.resolved_digest == _DIGEST
    # The contract envelopes and bootstrap token were materialized.
    assert driver.handle is not None
    written = json.loads((driver.handle.input_root / "inputs.json").read_text())
    assert written["inputs"] == {"message": "hi"}
    assert (driver.handle.input_root / "ctx.json").is_file()
    assert (driver.handle.input_root / "sidecar-token").is_file()


async def test_ctx_carries_connector_handles(tmp_path: Path) -> None:
    driver = _FakeDriver(tmp_path, outputs=_success_outputs())
    scheduler = _scheduler(driver)
    connectors = {"registry": ConnectorRef(host="h", endpoint="e", type="oci-registry")}

    await scheduler.schedule(_request(connectors=connectors))

    assert driver.handle is not None
    ctx = json.loads((driver.handle.input_root / "ctx.json").read_text())
    assert ctx["connectors"]["registry"]["type"] == "oci-registry"


# ---------------------------------------------------------------------------
# Idempotent replay
# ---------------------------------------------------------------------------


async def test_replay_returns_cached_envelope_without_second_sandbox(tmp_path: Path) -> None:
    driver = _FakeDriver(tmp_path, outputs=_success_outputs())
    scheduler = _scheduler(driver)

    first = await scheduler.schedule(_request())
    second = await scheduler.schedule(_request())

    assert second is first
    assert driver.prepared == 1  # no second sandbox


async def test_replay_reconstructs_from_terminal_record(tmp_path: Path) -> None:
    repo = _repo()
    await _scheduler(_FakeDriver(tmp_path, outputs=_success_outputs()), repo=repo).schedule(
        _request()
    )

    # A fresh Scheduler shares the repo but has an empty in-process cache.
    fresh_driver = _FakeDriver(tmp_path, outputs=None)
    result = await _scheduler(fresh_driver, repo=repo).schedule(_request())

    assert result.class_ is ResultClass.SUCCESS
    assert result.outputs == {}  # outputs are not part of the persisted record
    assert fresh_driver.prepared == 0


async def test_replay_reconstructs_failure_record() -> None:
    repo = _repo()
    terminal = ActivityExecution(
        workspace_id="ws-1",
        run_id="run-1",
        step_id="step-1",
        attempt=1,
        activity_ref="acme/echo@1.0.0",
        deadline=_NOW + timedelta(minutes=10),
        started_at=_NOW,
        state=ExecutionState.FAILED,
        result_class="permanent",
        error_code="activity.unresolved",
    )
    await repo.insert(terminal)
    driver = _FakeDriver(Path("/tmp"), outputs=None)
    result = await _scheduler(driver, repo=repo).schedule(_request())

    assert result.class_ is ResultClass.PERMANENT
    assert result.error is not None
    assert result.error.code == "activity.unresolved"
    assert driver.prepared == 0


# ---------------------------------------------------------------------------
# Crash reconciliation
# ---------------------------------------------------------------------------


async def _insert_running(repo: ExecutionRepository) -> ActivityExecution:
    record = ActivityExecution(
        workspace_id="ws-1",
        run_id="run-1",
        step_id="step-1",
        attempt=1,
        activity_ref="acme/echo@1.0.0",
        deadline=_NOW + timedelta(minutes=10),
        started_at=_NOW,
        state=ExecutionState.RUNNING,
    )
    return await repo.insert(record)


def _seed_context(handle: SandboxHandle, driver: _FakeDriver) -> Any:
    from custos_arm.scheduler.scheduler import _AttemptContext

    return _AttemptContext(
        handle=handle,
        resolved=_resolved(),
        driver=driver,
        deadline=_NOW + timedelta(minutes=10),
    )


async def test_reconcile_resumes_inflight_attempt(tmp_path: Path) -> None:
    repo = _repo()
    await _insert_running(repo)
    out_root = tmp_path / "out"
    out_root.mkdir()
    (out_root / "outputs.json").write_bytes(_success_outputs())
    driver = _FakeDriver(tmp_path, outputs=None)
    handle = _handle(tmp_path / "in", out_root)
    scheduler = _scheduler(driver, repo=repo)
    # Seed the in-flight context as if a prior call had prepared the sandbox.
    scheduler._context[("ws-1", "run-1", "step-1", 1)] = _seed_context(handle, driver)

    result = await scheduler.schedule(_request())

    assert result.class_ is ResultClass.SUCCESS
    assert driver.prepared == 0  # resumed, not relaunched
    assert driver.awaited == 1
    record = await repo.get("ws-1", "run-1", "step-1", 1)
    assert record is not None
    assert record.state is ExecutionState.SUCCEEDED


async def test_reconcile_resume_failure_synthesizes_envelope(tmp_path: Path) -> None:
    repo = _repo()
    await _insert_running(repo)
    boom = CatalogUnavailableError("acme/echo@1.0.0", "monitor lost the pod")
    driver = _FakeDriver(tmp_path, outputs=None, await_error=boom)
    handle = _handle(tmp_path, tmp_path)
    scheduler = _scheduler(driver, repo=repo)
    scheduler._context[("ws-1", "run-1", "step-1", 1)] = _seed_context(handle, driver)

    result = await scheduler.schedule(_request())

    assert result.class_ is ResultClass.RETRYABLE
    assert driver.cleaned == 1


async def test_reconcile_relaunches_when_no_context(tmp_path: Path) -> None:
    repo = _repo()
    await _insert_running(repo)
    driver = _FakeDriver(tmp_path, outputs=_success_outputs())
    scheduler = _scheduler(driver, repo=repo)

    result = await scheduler.schedule(_request())

    assert result.class_ is ResultClass.SUCCESS
    assert driver.prepared == 1  # relaunched from scratch
    record = await repo.get("ws-1", "run-1", "step-1", 1)
    assert record is not None
    assert record.state is ExecutionState.SUCCEEDED


# ---------------------------------------------------------------------------
# Failure modes
# ---------------------------------------------------------------------------


async def test_resolve_failure_maps_permanent(tmp_path: Path) -> None:
    driver = _FakeDriver(tmp_path, outputs=None)
    resolver = _FakeResolver(None, error=ActivityUnresolvedError("acme/echo@1.0.0"))
    repo = _repo()
    result = await _scheduler(driver, resolver=resolver, repo=repo).schedule(_request())

    assert result.class_ is ResultClass.PERMANENT
    assert result.error is not None
    assert result.error.code == "activity.unresolved"
    assert driver.prepared == 0  # never reached the sandbox
    record = await repo.get("ws-1", "run-1", "step-1", 1)
    assert record is not None
    assert record.state is ExecutionState.FAILED


async def test_deadline_signal_maps_to_timeout(tmp_path: Path) -> None:
    outcome = SandboxOutcome(124, SandboxSignal.DEADLINE)
    driver = _FakeDriver(tmp_path, outputs=None, outcome=outcome)
    result = await _scheduler(driver).schedule(_request())

    assert result.class_ is ResultClass.CANCELLED
    assert result.error is not None
    assert result.error.code == "activity.timeout"


async def test_oom_signal_maps_to_retryable(tmp_path: Path) -> None:
    outcome = SandboxOutcome(137, SandboxSignal.OOM)
    driver = _FakeDriver(tmp_path, outputs=None, outcome=outcome)
    result = await _scheduler(driver).schedule(_request())

    assert result.class_ is ResultClass.RETRYABLE
    assert result.error is not None
    assert result.error.code == "activity.oom_killed"


async def test_missing_outputs_maps_contract_violation(tmp_path: Path) -> None:
    driver = _FakeDriver(tmp_path, outputs=None)
    result = await _scheduler(driver).schedule(_request())

    assert result.class_ is ResultClass.PERMANENT
    assert result.error is not None
    assert result.error.code == "activity.contract_violation"


async def test_cleanup_failure_does_not_mask_result(tmp_path: Path) -> None:
    driver = _FakeDriver(
        tmp_path,
        outputs=_success_outputs(),
        cleanup_error=RuntimeError("kube down"),
    )
    result = await _scheduler(driver).schedule(_request())

    assert result.class_ is ResultClass.SUCCESS
    assert driver.cleaned == 1


async def test_step_deadline_clamps_below_manifest_timeout(tmp_path: Path) -> None:
    captured: dict[str, datetime] = {}

    class _CapturingDriver(_FakeDriver):
        def await_terminal(self, handle: SandboxHandle, deadline: datetime) -> SandboxOutcome:
            captured["deadline"] = deadline
            return super().await_terminal(handle, deadline)

    driver = _CapturingDriver(tmp_path, outputs=_success_outputs())
    step_deadline = _NOW + timedelta(minutes=2)
    await _scheduler(driver).schedule(_request(step_deadline=step_deadline))

    assert captured["deadline"] == step_deadline  # clamped below the PT15M manifest timeout


# ---------------------------------------------------------------------------
# Filesystem adapters
# ---------------------------------------------------------------------------


async def test_filesystem_secret_sink_writes_with_mode(tmp_path: Path) -> None:
    sink = FilesystemSecretSink(tmp_path)
    await sink.write_secret(relative_path="secrets/registry/token", content=b"s3cr3t", mode=0o400)

    path = tmp_path / "secrets" / "registry" / "token"
    assert path.read_bytes() == b"s3cr3t"
    assert (path.stat().st_mode & 0o777) == 0o400


async def test_filesystem_artifact_reader_streams(tmp_path: Path) -> None:
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    (artifacts / "report").write_bytes(b"hello world")
    reader = FilesystemArtifactReader(tmp_path)

    assert reader.has("report") is True
    assert reader.has("missing") is False
    chunks = [chunk async for chunk in reader.open("report")]
    assert b"".join(chunks) == b"hello world"


def test_read_outputs_returns_none_when_absent(tmp_path: Path) -> None:
    assert read_outputs(tmp_path, max_bytes=1024) is None
    (tmp_path / "outputs.json").write_bytes(b"{}")
    assert read_outputs(tmp_path, max_bytes=1024) == b"{}"


def test_read_outputs_rejects_oversized_blob(tmp_path: Path) -> None:
    from custos_arm.io.errors import OutputTooLargeError

    (tmp_path / "outputs.json").write_bytes(b"x" * 64)
    with pytest.raises(OutputTooLargeError):
        read_outputs(tmp_path, max_bytes=8)


async def test_oversized_outputs_maps_permanent(tmp_path: Path) -> None:
    # The cap is enforced before the blob is read, surfacing as a permanent
    # ``output.too_large`` failure synthesized by the Scheduler.
    big = _success_outputs({"blob": "x" * 4096})
    driver = _FakeDriver(tmp_path, outputs=big)
    settings = _settings(ARM_OUTPUT_MAX_BYTES="8")
    scheduler = ActivityScheduler(
        resolver=_FakeResolver(_resolved()),
        limiter=ResourceLimiter(settings),
        broker=IOBroker(_StubArtifactStore(), output_max_bytes=8),  # type: ignore[arg-type]
        injector=SecretInjector(token_minter=SidecarTokenMinter()),
        mapper=ResultMapper(),
        dispatcher=RuntimeDriverDispatcher((driver,)),
        repository=_repo(),
        settings=settings,
        now=lambda: _NOW,
    )

    result = await scheduler.schedule(_request())

    assert result.class_ is ResultClass.PERMANENT
    assert result.error is not None
    assert result.error.code == "output.too_large"


# ---------------------------------------------------------------------------
# Error translation
# ---------------------------------------------------------------------------


def test_synthesize_failure_uses_code_and_class() -> None:
    result = synthesize_failure(ActivityUnresolvedError("acme/echo@1.0.0"), attempt=2)
    assert result.class_ is ResultClass.PERMANENT
    assert result.attempt == 2
    assert result.error is not None
    assert result.error.code == "activity.unresolved"


def test_error_envelope_for_unknown_exception_is_retryable() -> None:
    envelope = error_envelope_for(ValueError("boom"))
    assert envelope.error_class is ErrorClass.RETRYABLE
    assert envelope.code == "system.sandbox_failure"


def test_error_envelope_for_io_error_uses_to_envelope() -> None:
    from custos_arm.io.errors import OutputTooLargeError

    envelope = error_envelope_for(OutputTooLargeError("too big"))
    assert envelope.error_class is ErrorClass.PERMANENT
    assert envelope.code == "output.too_large"


# ---------------------------------------------------------------------------
# Cancellation (status reporting — ARM-IMPL-018)
# ---------------------------------------------------------------------------


async def test_cancel_accepts_live_attempt(tmp_path: Path) -> None:
    repo = _repo()
    await _insert_running(repo)
    driver = _FakeDriver(tmp_path, outputs=None)
    scheduler = _scheduler(driver, repo=repo)
    scheduler._context[("ws-1", "run-1", "step-1", 1)] = _seed_context(
        _handle(tmp_path / "in", tmp_path / "out"), driver
    )

    outcome = await scheduler.cancel(workspace_id="ws-1", run_id="run-1", step_id="step-1")

    assert outcome is CancelOutcome.ACCEPTED
    # The live attempt is driven to cancellation through its runtime driver.
    assert driver.cancellations == [CancelReason.CANCELLED]


async def test_cancel_reports_terminated_after_completion(tmp_path: Path) -> None:
    driver = _FakeDriver(tmp_path, outputs=_success_outputs())
    scheduler = _scheduler(driver)
    await scheduler.schedule(_request())

    outcome = await scheduler.cancel(workspace_id="ws-1", run_id="run-1", step_id="step-1")

    assert outcome is CancelOutcome.TERMINATED


async def test_cancel_reports_terminated_from_terminal_record(tmp_path: Path) -> None:
    repo = _repo()
    terminal = ActivityExecution(
        workspace_id="ws-1",
        run_id="run-1",
        step_id="step-1",
        attempt=1,
        activity_ref="acme/echo@1.0.0",
        deadline=_NOW + timedelta(minutes=10),
        started_at=_NOW,
        state=ExecutionState.SUCCEEDED,
    )
    await repo.insert(terminal)
    driver = _FakeDriver(tmp_path, outputs=None)
    scheduler = _scheduler(driver, repo=repo)
    # A live context entry whose record has already terminated (a transient
    # state between completion and context eviction) reports TERMINATED.
    scheduler._context[("ws-1", "run-1", "step-1", 1)] = _seed_context(
        _handle(tmp_path / "in", tmp_path / "out"), driver
    )

    outcome = await scheduler.cancel(workspace_id="ws-1", run_id="run-1", step_id="step-1")

    assert outcome is CancelOutcome.TERMINATED


async def test_cancel_reports_unknown_for_unseen_step(tmp_path: Path) -> None:
    scheduler = _scheduler(_FakeDriver(tmp_path, outputs=None))

    outcome = await scheduler.cancel(workspace_id="ws-1", run_id="nope", step_id="nope")

    assert outcome is CancelOutcome.UNKNOWN


# ---------------------------------------------------------------------------
# End-to-end cancel + deadline (ARM-IMPL-019)
# ---------------------------------------------------------------------------


class _CancellableDriver(_FakeDriver):
    """A driver whose ``await_terminal`` blocks until cancelled.

    Mirrors the OCI driver contract: ``cancel`` records the reason and
    unblocks the in-flight ``await_terminal``, which then reports the matching
    :class:`SandboxSignal` so the Scheduler synthesizes the cancelled result.
    """

    def __init__(self, root: Path) -> None:
        super().__init__(root, outputs=None)
        self._cancelled = threading.Event()

    def await_terminal(self, handle: SandboxHandle, deadline: datetime) -> SandboxOutcome:
        self.awaited += 1
        if not self._cancelled.wait(timeout=5):  # pragma: no cover - safety timeout
            raise AssertionError("await_terminal was never cancelled")
        return SandboxOutcome(exit_code=137, signal=SandboxSignal.CANCELLED)

    def cancel(self, handle: SandboxHandle, reason: CancelReason) -> None:
        self.cancellations.append(reason)
        self._cancelled.set()


async def _wait_for_live_attempt(scheduler: ActivityScheduler) -> None:
    for _ in range(500):
        if scheduler._context:
            return
        await asyncio.sleep(0.01)
    raise AssertionError("attempt never became live")  # pragma: no cover - safety


async def test_run_cancel_terminates_live_attempt_as_cancelled(tmp_path: Path) -> None:
    driver = _CancellableDriver(tmp_path)
    repo = _repo()
    scheduler = _scheduler(driver, repo=repo)

    run = asyncio.create_task(scheduler.schedule(_request()))
    await _wait_for_live_attempt(scheduler)

    outcome = await scheduler.cancel(workspace_id="ws-1", run_id="run-1", step_id="step-1")
    result = await run

    assert outcome is CancelOutcome.ACCEPTED
    assert driver.cancellations == [CancelReason.CANCELLED]
    assert result.class_ is ResultClass.CANCELLED
    assert result.error is not None
    assert result.error.code == "activity.cancelled"
    record = await repo.get("ws-1", "run-1", "step-1", 1)
    assert record is not None
    assert record.state is ExecutionState.CANCELLED


async def test_run_cancel_is_idempotent(tmp_path: Path) -> None:
    driver = _CancellableDriver(tmp_path)
    scheduler = _scheduler(driver)

    run = asyncio.create_task(scheduler.schedule(_request()))
    await _wait_for_live_attempt(scheduler)

    first = await scheduler.cancel(workspace_id="ws-1", run_id="run-1", step_id="step-1")
    result = await run
    # A second cancel after the attempt has terminated is a no-op (409).
    second = await scheduler.cancel(workspace_id="ws-1", run_id="run-1", step_id="step-1")

    assert first is CancelOutcome.ACCEPTED
    assert second is CancelOutcome.TERMINATED
    assert result.class_ is ResultClass.CANCELLED


async def test_deadline_clamped_by_step_deadline_yields_timeout(tmp_path: Path) -> None:
    # The OCI driver self-cancels on the deadline; here the fake driver reports
    # the DEADLINE signal directly to assert the Scheduler synthesizes timeout.
    outcome = SandboxOutcome(124, SandboxSignal.DEADLINE)
    driver = _FakeDriver(tmp_path, outputs=None, outcome=outcome)
    repo = _repo()
    scheduler = _scheduler(driver, repo=repo)
    step_deadline = _NOW + timedelta(minutes=1)

    result = await scheduler.schedule(_request(step_deadline=step_deadline))

    assert result.class_ is ResultClass.CANCELLED
    assert result.error is not None
    assert result.error.code == "activity.timeout"
    record = await repo.get("ws-1", "run-1", "step-1", 1)
    assert record is not None
    # The persisted deadline is clamped to the (earlier) step deadline.
    assert record.deadline == step_deadline
    assert record.state is ExecutionState.CANCELLED
