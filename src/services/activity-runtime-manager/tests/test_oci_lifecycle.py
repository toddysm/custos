"""Unit tests for the OCI Container Driver lifecycle monitor (ARM-IMPL-016).

The Kubernetes API clients are faked, so these exercise the lifecycle logic
(suspend/start, Pod-status → outcome mapping, deadline enforcement, idempotent
cancel/cleanup) without a cluster. The real ``kind`` path lives in the
``integration``-marked suite.
"""

from __future__ import annotations

import io
import tarfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from custos_arm.contract import ErrorClass, ImageRef, StepRef
from custos_arm.limit import EffectiveResources
from custos_arm.manifest import IsolationTier
from custos_arm.runtime.driver import RuntimeDriver
from custos_arm.runtime.models import (
    OCI_CONTAINER_KIND,
    CancelReason,
    OutputBundle,
    SandboxHandle,
    SandboxOutcome,
    SandboxPlan,
    SandboxSignal,
    SidecarSpec,
    TmpfsMount,
)
from custos_arm.runtime.oci import (
    ACTIVITY_CANCELLED,
    ACTIVITY_CONTAINER_NAME,
    ACTIVITY_OOM_KILLED,
    ACTIVITY_SANDBOX_FAILURE,
    ACTIVITY_TIMEOUT,
    INPUT_BRIDGE_CONTAINER_NAME,
    INPUT_READY_SENTINEL,
    ImagePullError,
    OciContainerDriver,
    SandboxFailureError,
    classify_signal,
    is_image_pull_waiting_reason,
    job_name,
    signal_error_code,
)
from custos_arm.runtime.oci.lifecycle import (
    DEADLINE_EXIT_CODE,
    ExecResult,
    _init_container_state,
)

_DIGEST = "sha256:" + "a" * 64
_DEADLINE = datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)


# --------------------------------------------------------------------------- #
# Plan + fake-Pod construction helpers
# --------------------------------------------------------------------------- #
def _plan(*, namespace: str = "custos-sandboxes") -> SandboxPlan:
    return SandboxPlan(
        step=StepRef.model_validate({"runId": "run-1", "stepId": "step-a", "attempt": 1}),
        namespace=namespace,
        image=ImageRef(ref="registry.example/act", digest=_DIGEST),
        resources=EffectiveResources(
            cpu_request="250m",
            cpu_limit="500m",
            memory_request="128Mi",
            memory_limit="256Mi",
            ephemeral_storage_limit="512Mi",
            timeout=timedelta(seconds=120),
            tier=IsolationTier.PROCESS,
            runtime_class="",
        ),
        tmpfs_mounts=(
            TmpfsMount(mount_path="/custos/in", read_only=True, size_limit="64Mi"),
            TmpfsMount(mount_path="/custos/out"),
        ),
        sidecar=SidecarSpec(image="registry.example/sidecar:1", endpoint="http://c:8080"),
        io_bridge_image="registry.example/io-bridge:1",
        deadline=_DEADLINE,
    )


def _running_status() -> SimpleNamespace:
    return SimpleNamespace(
        name=ACTIVITY_CONTAINER_NAME,
        state=SimpleNamespace(waiting=None, terminated=None, running=SimpleNamespace()),
    )


def _waiting_status(reason: str) -> SimpleNamespace:
    return SimpleNamespace(
        name=ACTIVITY_CONTAINER_NAME,
        state=SimpleNamespace(
            waiting=SimpleNamespace(reason=reason), terminated=None, running=None
        ),
    )


def _terminated_status(exit_code: int, reason: str | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        name=ACTIVITY_CONTAINER_NAME,
        state=SimpleNamespace(
            waiting=None,
            terminated=SimpleNamespace(exit_code=exit_code, reason=reason),
            running=None,
        ),
    )


def _pod_list(container_statuses: list[SimpleNamespace] | None) -> SimpleNamespace:
    if container_statuses is None:
        return SimpleNamespace(items=[])
    return SimpleNamespace(
        items=[SimpleNamespace(status=SimpleNamespace(container_statuses=container_statuses))]
    )


def _init_running(name: str = INPUT_BRIDGE_CONTAINER_NAME) -> SimpleNamespace:
    return SimpleNamespace(
        name=name,
        state=SimpleNamespace(running=SimpleNamespace(), terminated=None, waiting=None),
    )


def _init_terminated(name: str = INPUT_BRIDGE_CONTAINER_NAME) -> SimpleNamespace:
    return SimpleNamespace(
        name=name,
        state=SimpleNamespace(
            running=None,
            terminated=SimpleNamespace(exit_code=0, reason="Completed"),
            waiting=None,
        ),
    )


def _pod_with_init(
    init_statuses: list[SimpleNamespace] | None,
    *,
    pod_name: str = "sandbox-pod-abc12",
) -> SimpleNamespace:
    if init_statuses is None:
        return SimpleNamespace(items=[])
    return SimpleNamespace(
        items=[
            SimpleNamespace(
                metadata=SimpleNamespace(name=pod_name),
                status=SimpleNamespace(
                    container_statuses=[], init_container_statuses=init_statuses
                ),
            )
        ]
    )


class _RecordingExec:
    """A fake :class:`PodExec` that records calls and returns canned results."""

    def __init__(self, results: list[ExecResult] | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self._results = results or []

    def __call__(
        self,
        *,
        namespace: str,
        pod: str,
        container: str,
        command: list[str],
        stdin: bytes | None,
    ) -> ExecResult:
        self.calls.append(
            {
                "namespace": namespace,
                "pod": pod,
                "container": container,
                "command": command,
                "stdin": stdin,
            }
        )
        if self._results:
            return self._results.pop(0)
        return ExecResult(exit_code=0)


class _ApiError(Exception):
    """Mimics ``kubernetes.client.exceptions.ApiException`` (carries ``status``)."""

    def __init__(self, status: int) -> None:
        super().__init__(f"status={status}")
        self.status = status


def _driver(
    *,
    batch: Any = None,
    core: Any = None,
    staging_root: Path,
    now: datetime = _DEADLINE - timedelta(seconds=1),
    pod_exec: Any = None,
    start_timeout: float = 120.0,
) -> OciContainerDriver:
    return OciContainerDriver(
        batch_api=batch or MagicMock(),
        core_api=core or MagicMock(),
        staging_root=staging_root,
        poll_interval=0.0,
        sleep=MagicMock(),
        now=lambda: now,
        pod_exec=pod_exec,
        start_timeout=start_timeout,
    )


def _handle(plan: SandboxPlan, staging_root: Path) -> SandboxHandle:
    name = job_name(plan.step)
    return SandboxHandle(
        kind=OCI_CONTAINER_KIND,
        reference=f"{plan.namespace}/{name}",
        input_root=staging_root / name / "in",
        output_root=staging_root / name / "out",
    )


# --------------------------------------------------------------------------- #
# Pure mapping functions
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    ("exit_code", "terminated_reason", "cancel_reason", "expected"),
    [
        (0, None, None, SandboxSignal.NONE),
        (2, None, None, SandboxSignal.NONE),
        (137, "OOMKilled", None, SandboxSignal.OOM),
        (137, None, None, SandboxSignal.KILLED),
        (1, None, CancelReason.DEADLINE, SandboxSignal.DEADLINE),
        (1, None, CancelReason.CANCELLED, SandboxSignal.CANCELLED),
        (1, None, CancelReason.SHUTDOWN, SandboxSignal.CANCELLED),
        # Driver-initiated termination wins over the kernel signal.
        (137, "OOMKilled", CancelReason.DEADLINE, SandboxSignal.DEADLINE),
    ],
)
def test_classify_signal(
    exit_code: int,
    terminated_reason: str | None,
    cancel_reason: CancelReason | None,
    expected: SandboxSignal,
) -> None:
    assert (
        classify_signal(
            exit_code=exit_code,
            terminated_reason=terminated_reason,
            cancel_reason=cancel_reason,
        )
        == expected
    )


@pytest.mark.parametrize(
    ("signal", "code", "error_class"),
    [
        (SandboxSignal.OOM, ACTIVITY_OOM_KILLED, ErrorClass.RETRYABLE),
        (SandboxSignal.KILLED, ACTIVITY_SANDBOX_FAILURE, ErrorClass.RETRYABLE),
        (SandboxSignal.DEADLINE, ACTIVITY_TIMEOUT, ErrorClass.CANCELLED),
        (SandboxSignal.CANCELLED, ACTIVITY_CANCELLED, ErrorClass.CANCELLED),
    ],
)
def test_signal_error_code(signal: SandboxSignal, code: str, error_class: ErrorClass) -> None:
    assert signal_error_code(signal) == (code, error_class)


def test_signal_error_code_none_for_clean_exit() -> None:
    assert signal_error_code(SandboxSignal.NONE) is None


@pytest.mark.parametrize(
    ("reason", "expected"),
    [
        ("ImagePullBackOff", True),
        ("ErrImagePull", True),
        ("InvalidImageName", True),
        ("ErrImageNeverPull", True),
        ("RegistryUnavailable", True),
        ("ContainerCreating", False),
        ("CrashLoopBackOff", False),
        (None, False),
    ],
)
def test_is_image_pull_waiting_reason(reason: str | None, expected: bool) -> None:
    assert is_image_pull_waiting_reason(reason) is expected


# --------------------------------------------------------------------------- #
# Protocol conformance
# --------------------------------------------------------------------------- #
def test_driver_conforms_to_protocol(tmp_path: Path) -> None:
    driver = _driver(staging_root=tmp_path)
    assert isinstance(driver, RuntimeDriver)
    assert driver.kind == OCI_CONTAINER_KIND


# --------------------------------------------------------------------------- #
# prepare
# --------------------------------------------------------------------------- #
def test_prepare_creates_suspended_job_and_staging_dirs(tmp_path: Path) -> None:
    batch = MagicMock()
    plan = _plan()
    driver = _driver(batch=batch, staging_root=tmp_path)

    handle = driver.prepare(plan)

    name = job_name(plan.step)
    assert handle.reference == f"{plan.namespace}/{name}"
    assert handle.input_root == tmp_path / name / "in"
    assert handle.output_root == tmp_path / name / "out"
    assert handle.input_root.is_dir()
    assert handle.output_root.is_dir()

    batch.create_namespaced_job.assert_called_once()
    _, kwargs = batch.create_namespaced_job.call_args
    assert kwargs["namespace"] == plan.namespace
    assert kwargs["body"]["spec"]["suspend"] is True


def test_prepare_wraps_client_failure(tmp_path: Path) -> None:
    batch = MagicMock()
    batch.create_namespaced_job.side_effect = _ApiError(500)
    plan = _plan()
    driver = _driver(batch=batch, staging_root=tmp_path)

    with pytest.raises(SandboxFailureError):
        driver.prepare(plan)

    # The staging tree must not leak when the Job never lands.
    assert not (tmp_path / job_name(plan.step)).exists()


# --------------------------------------------------------------------------- #
# start
# --------------------------------------------------------------------------- #
def test_start_unsuspends_then_streams_inputs_and_writes_sentinel(tmp_path: Path) -> None:
    batch = MagicMock()
    core = MagicMock()
    core.list_namespaced_pod.return_value = _pod_with_init([_init_running()])
    exec_ = _RecordingExec()
    plan = _plan()
    driver = _driver(batch=batch, core=core, staging_root=tmp_path, pod_exec=exec_)

    handle = _handle(plan, tmp_path)
    handle.input_root.mkdir(parents=True, exist_ok=True)
    (handle.input_root / "inputs.json").write_text('{"x": 1}')
    (handle.input_root / "nested").mkdir()
    (handle.input_root / "nested" / "ctx.json").write_text("{}")

    driver.start(handle)

    # The Job is un-suspended so its Pod can be scheduled.
    _, kwargs = batch.patch_namespaced_job.call_args
    assert kwargs["name"] == job_name(plan.step)
    assert kwargs["namespace"] == plan.namespace
    assert kwargs["body"] == {"spec": {"suspend": False}}

    # Exactly two execs target the input bridge of the discovered Pod.
    assert len(exec_.calls) == 2
    stream_call, sentinel_call = exec_.calls
    assert stream_call["pod"] == "sandbox-pod-abc12"
    assert stream_call["namespace"] == plan.namespace
    assert stream_call["container"] == INPUT_BRIDGE_CONTAINER_NAME
    assert stream_call["command"] == ["tar", "-x", "-C", "/custos/in"]

    # The streamed raw tar round-trips the host input tree.
    with tarfile.open(fileobj=io.BytesIO(stream_call["stdin"])) as tar:
        assert sorted(tar.getnames()) == ["inputs.json", "nested", "nested/ctx.json"]

    # The sentinel that releases the input gate is written last.
    assert sentinel_call["command"] == ["touch", INPUT_READY_SENTINEL]
    assert sentinel_call["stdin"] is None


def test_start_waits_for_the_input_bridge_to_start(tmp_path: Path) -> None:
    core = MagicMock()
    core.list_namespaced_pod.side_effect = [
        _pod_with_init(None),  # Pod not scheduled yet.
        _pod_with_init([_init_running()]),  # Bridge now running.
    ]
    exec_ = _RecordingExec()
    sleep = MagicMock()
    driver = OciContainerDriver(
        batch_api=MagicMock(),
        core_api=core,
        staging_root=tmp_path,
        poll_interval=0.0,
        sleep=sleep,
        now=lambda: _DEADLINE - timedelta(seconds=1),
        pod_exec=exec_,
        start_timeout=120.0,
    )

    handle = _handle(_plan(), tmp_path)
    handle.input_root.mkdir(parents=True, exist_ok=True)

    driver.start(handle)

    assert sleep.call_count == 1
    assert len(exec_.calls) == 2


def test_start_raises_when_input_bridge_exited_before_staging(tmp_path: Path) -> None:
    core = MagicMock()
    core.list_namespaced_pod.return_value = _pod_with_init([_init_terminated()])
    driver = _driver(core=core, staging_root=tmp_path, pod_exec=_RecordingExec())

    handle = _handle(_plan(), tmp_path)
    handle.input_root.mkdir(parents=True, exist_ok=True)

    with pytest.raises(SandboxFailureError):
        driver.start(handle)


def test_start_raises_when_input_bridge_never_starts(tmp_path: Path) -> None:
    core = MagicMock()
    core.list_namespaced_pod.return_value = _pod_with_init(None)
    driver = _driver(
        core=core,
        staging_root=tmp_path,
        pod_exec=_RecordingExec(),
        start_timeout=0.0,
    )

    handle = _handle(_plan(), tmp_path)
    handle.input_root.mkdir(parents=True, exist_ok=True)

    with pytest.raises(SandboxFailureError):
        driver.start(handle)


def test_start_raises_when_streaming_inputs_fails(tmp_path: Path) -> None:
    core = MagicMock()
    core.list_namespaced_pod.return_value = _pod_with_init([_init_running()])
    exec_ = _RecordingExec([ExecResult(exit_code=1, stderr="tar: write error")])
    driver = _driver(core=core, staging_root=tmp_path, pod_exec=exec_)

    handle = _handle(_plan(), tmp_path)
    handle.input_root.mkdir(parents=True, exist_ok=True)

    with pytest.raises(SandboxFailureError):
        driver.start(handle)


def test_start_raises_when_writing_sentinel_fails(tmp_path: Path) -> None:
    core = MagicMock()
    core.list_namespaced_pod.return_value = _pod_with_init([_init_running()])
    exec_ = _RecordingExec([ExecResult(exit_code=0), ExecResult(exit_code=2)])
    driver = _driver(core=core, staging_root=tmp_path, pod_exec=exec_)

    handle = _handle(_plan(), tmp_path)
    handle.input_root.mkdir(parents=True, exist_ok=True)

    with pytest.raises(SandboxFailureError):
        driver.start(handle)


def test_start_wraps_client_failure(tmp_path: Path) -> None:
    batch = MagicMock()
    batch.patch_namespaced_job.side_effect = _ApiError(500)
    driver = _driver(batch=batch, staging_root=tmp_path)

    with pytest.raises(SandboxFailureError):
        driver.start(_handle(_plan(), tmp_path))


def test_init_container_state_handles_missing_status_and_container() -> None:
    # A Pod without a status block yields no state.
    assert _init_container_state(SimpleNamespace(status=None), INPUT_BRIDGE_CONTAINER_NAME) is None
    # A Pod whose init statuses omit the bridge yields no state.
    pod = SimpleNamespace(status=SimpleNamespace(init_container_statuses=[_init_running("other")]))
    assert _init_container_state(pod, INPUT_BRIDGE_CONTAINER_NAME) is None


# --------------------------------------------------------------------------- #
# await_terminal
# --------------------------------------------------------------------------- #
def test_await_terminal_clean_exit(tmp_path: Path) -> None:
    core = MagicMock()
    core.list_namespaced_pod.return_value = _pod_list([_terminated_status(0)])
    driver = _driver(core=core, staging_root=tmp_path)

    outcome = driver.await_terminal(_handle(_plan(), tmp_path), _DEADLINE)

    assert outcome == SandboxOutcome(exit_code=0, signal=SandboxSignal.NONE)


def test_await_terminal_oom(tmp_path: Path) -> None:
    core = MagicMock()
    core.list_namespaced_pod.return_value = _pod_list([_terminated_status(137, reason="OOMKilled")])
    driver = _driver(core=core, staging_root=tmp_path)

    outcome = driver.await_terminal(_handle(_plan(), tmp_path), _DEADLINE)

    assert outcome.signal is SandboxSignal.OOM
    assert outcome.exit_code == 137


def test_await_terminal_sigkill(tmp_path: Path) -> None:
    core = MagicMock()
    core.list_namespaced_pod.return_value = _pod_list([_terminated_status(137)])
    driver = _driver(core=core, staging_root=tmp_path)

    outcome = driver.await_terminal(_handle(_plan(), tmp_path), _DEADLINE)

    assert outcome.signal is SandboxSignal.KILLED


def test_await_terminal_image_pull_failure(tmp_path: Path) -> None:
    core = MagicMock()
    core.list_namespaced_pod.return_value = _pod_list([_waiting_status("ImagePullBackOff")])
    driver = _driver(core=core, staging_root=tmp_path)

    with pytest.raises(ImagePullError) as excinfo:
        driver.await_terminal(_handle(_plan(), tmp_path), _DEADLINE)
    assert excinfo.value.error_class is ErrorClass.RETRYABLE


def test_await_terminal_polls_until_terminal(tmp_path: Path) -> None:
    core = MagicMock()
    sleep = MagicMock()
    core.list_namespaced_pod.side_effect = [
        _pod_list(None),  # pod not scheduled yet
        _pod_list([_running_status()]),  # running
        _pod_list([_terminated_status(0)]),  # done
    ]
    driver = OciContainerDriver(
        batch_api=MagicMock(),
        core_api=core,
        staging_root=tmp_path,
        poll_interval=0.0,
        sleep=sleep,
        now=lambda: _DEADLINE - timedelta(seconds=1),
    )

    outcome = driver.await_terminal(_handle(_plan(), tmp_path), _DEADLINE)

    assert outcome.exit_code == 0
    assert sleep.call_count == 2


def test_await_terminal_enforces_deadline(tmp_path: Path) -> None:
    batch = MagicMock()
    core = MagicMock()
    core.list_namespaced_pod.return_value = _pod_list([_running_status()])
    plan = _plan()
    driver = OciContainerDriver(
        batch_api=batch,
        core_api=core,
        staging_root=tmp_path,
        poll_interval=0.0,
        sleep=MagicMock(),
        now=lambda: _DEADLINE + timedelta(seconds=1),
    )

    outcome = driver.await_terminal(_handle(plan, tmp_path), _DEADLINE)

    assert outcome == SandboxOutcome(exit_code=DEADLINE_EXIT_CODE, signal=SandboxSignal.DEADLINE)
    batch.delete_namespaced_job.assert_called_once()


def test_await_terminal_ignores_non_activity_container(tmp_path: Path) -> None:
    sidecar_only = SimpleNamespace(
        name="connector-sidecar",
        state=SimpleNamespace(waiting=None, terminated=None, running=SimpleNamespace()),
    )
    core = MagicMock()
    core.list_namespaced_pod.return_value = _pod_list([sidecar_only])
    driver = _driver(core=core, staging_root=tmp_path, now=_DEADLINE + timedelta(seconds=1))

    outcome = driver.await_terminal(_handle(_plan(), tmp_path), _DEADLINE)

    assert outcome.signal is SandboxSignal.DEADLINE


def test_await_terminal_handles_container_without_state(tmp_path: Path) -> None:
    stateless = SimpleNamespace(name=ACTIVITY_CONTAINER_NAME, state=None)
    core = MagicMock()
    core.list_namespaced_pod.return_value = _pod_list([stateless])
    driver = _driver(core=core, staging_root=tmp_path, now=_DEADLINE + timedelta(seconds=1))

    outcome = driver.await_terminal(_handle(_plan(), tmp_path), _DEADLINE)

    assert outcome.signal is SandboxSignal.DEADLINE


def test_await_terminal_handles_pod_without_status(tmp_path: Path) -> None:
    pod = SimpleNamespace(status=None)
    core = MagicMock()
    core.list_namespaced_pod.return_value = SimpleNamespace(items=[pod])
    driver = _driver(core=core, staging_root=tmp_path, now=_DEADLINE + timedelta(seconds=1))

    outcome = driver.await_terminal(_handle(_plan(), tmp_path), _DEADLINE)

    assert outcome.signal is SandboxSignal.DEADLINE


def test_await_terminal_observes_scheduler_cancel(tmp_path: Path) -> None:
    core = MagicMock()
    core.list_namespaced_pod.return_value = _pod_list([_running_status()])
    driver = _driver(staging_root=tmp_path, core=core)
    handle = _handle(_plan(), tmp_path)

    # A concurrent cancel must short-circuit the wait with the matching signal,
    # not block to the deadline or report a raw SIGKILL.
    driver.cancel(handle, CancelReason.CANCELLED)
    outcome = driver.await_terminal(handle, _DEADLINE)

    assert outcome.signal is SandboxSignal.CANCELLED


def test_await_terminal_wraps_pod_list_failure(tmp_path: Path) -> None:
    core = MagicMock()
    core.list_namespaced_pod.side_effect = _ApiError(503)
    driver = _driver(core=core, staging_root=tmp_path)

    with pytest.raises(SandboxFailureError):
        driver.await_terminal(_handle(_plan(), tmp_path), _DEADLINE)


# --------------------------------------------------------------------------- #
# cancel / cleanup idempotency
# --------------------------------------------------------------------------- #
def test_cancel_deletes_job(tmp_path: Path) -> None:
    batch = MagicMock()
    plan = _plan()
    driver = _driver(batch=batch, staging_root=tmp_path)

    driver.cancel(_handle(plan, tmp_path), CancelReason.CANCELLED)

    _, kwargs = batch.delete_namespaced_job.call_args
    assert kwargs["name"] == job_name(plan.step)
    assert kwargs["namespace"] == plan.namespace
    assert kwargs["propagation_policy"] == "Background"


def test_cancel_is_idempotent_when_job_missing(tmp_path: Path) -> None:
    batch = MagicMock()
    batch.delete_namespaced_job.side_effect = _ApiError(404)
    driver = _driver(batch=batch, staging_root=tmp_path)

    # Must not raise even though the Job is already gone.
    driver.cancel(_handle(_plan(), tmp_path), CancelReason.SHUTDOWN)


def test_cancel_wraps_unexpected_delete_failure(tmp_path: Path) -> None:
    batch = MagicMock()
    batch.delete_namespaced_job.side_effect = _ApiError(500)
    driver = _driver(batch=batch, staging_root=tmp_path)

    with pytest.raises(SandboxFailureError):
        driver.cancel(_handle(_plan(), tmp_path), CancelReason.CANCELLED)


def test_collect_exposes_output_root(tmp_path: Path) -> None:
    plan = _plan()
    handle = _handle(plan, tmp_path)
    driver = _driver(staging_root=tmp_path)

    assert driver.collect(handle) == OutputBundle(root=handle.output_root)


def test_cleanup_reaps_job_and_staging(tmp_path: Path) -> None:
    batch = MagicMock()
    plan = _plan()
    driver = _driver(batch=batch, staging_root=tmp_path)
    handle = driver.prepare(plan)
    assert handle.output_root.is_dir()

    driver.cleanup(handle)

    batch.delete_namespaced_job.assert_called_once()
    assert not (tmp_path / job_name(plan.step)).exists()


def test_cleanup_is_idempotent(tmp_path: Path) -> None:
    batch = MagicMock()
    batch.delete_namespaced_job.side_effect = _ApiError(404)
    plan = _plan()
    driver = _driver(batch=batch, staging_root=tmp_path)
    handle = driver.prepare(plan)

    driver.cleanup(handle)
    # A second cleanup after the tree is already gone is still a no-op.
    driver.cleanup(handle)
