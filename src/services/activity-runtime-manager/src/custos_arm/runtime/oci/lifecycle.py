"""OCI Container Driver — lifecycle monitor (ARM-IMPL-016).

Drive the sandbox lifecycle for ``runtime.kind == "oci-container"`` against a
real Kubernetes cluster: ``prepare`` → ``start`` → ``await_terminal`` →
``collect`` → ``cleanup`` (plus idempotent ``cancel``). The Job *spec* is built
by :func:`custos_arm.runtime.oci.job.build_activity_job` (ARM-IMPL-015); this
module owns the cluster interactions and the translation of Kubernetes Pod
status into a runtime-agnostic :class:`~custos_arm.runtime.models.SandboxOutcome`.

Per the design § Runtime Driver dispatcher contract the driver:

* is **synchronous and blocking** — ``await_terminal`` blocks until the Pod
  terminates, the deadline elapses, or a cancel is observed; the Scheduler
  offloads it off the event loop;
* **never interprets** ``outputs.json`` and **never classifies** a finished
  attempt — it returns the raw ``exitCode`` + ``signal`` and lets the Result
  Mapper classify. The only exception is a failure with *no* exit code (an
  un-pullable image, a Kubernetes-level failure), surfaced as a typed
  :class:`~custos_arm.runtime.oci.errors.OciDriverError`;
* makes ``cancel`` and ``cleanup`` **idempotent** — safe after exit or twice.

The Kubernetes API clients are injected so the lifecycle logic is unit-testable
with fakes; the real ``kind``-cluster path is exercised by the
``integration``-marked suite.
"""

from __future__ import annotations

import io
import shutil
import tarfile
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Final, Protocol

from custos_arm.contract import ErrorClass
from custos_arm.runtime.models import (
    OCI_CONTAINER_KIND,
    CancelReason,
    OutputBundle,
    SandboxHandle,
    SandboxOutcome,
    SandboxPlan,
    SandboxSignal,
)
from custos_arm.runtime.oci.errors import ImagePullError, SandboxFailureError
from custos_arm.runtime.oci.job import (
    ACTIVITY_CONTAINER_NAME,
    INPUT_BRIDGE_CONTAINER_NAME,
    INPUT_READY_SENTINEL,
    build_activity_job,
    job_name,
)

__all__ = [
    "DEADLINE_EXIT_CODE",
    "IMAGE_PULL_WAITING_REASONS",
    "OOM_TERMINATED_REASON",
    "SIGKILL_EXIT_CODE",
    "ExecResult",
    "OciContainerDriver",
    "PodExec",
    "classify_signal",
    "is_image_pull_waiting_reason",
    "signal_error_code",
]

#: Pod container ``waiting.reason`` values meaning the image cannot be pulled.
IMAGE_PULL_WAITING_REASONS: Final[frozenset[str]] = frozenset(
    {
        "ImagePullBackOff",
        "ErrImagePull",
        "InvalidImageName",
        "ErrImageNeverPull",
        "RegistryUnavailable",
    }
)
#: ``terminated.reason`` the kubelet reports for an OOM-killed container.
OOM_TERMINATED_REASON: Final[str] = "OOMKilled"
#: Exit code of a SIGKILL-terminated process (128 + 9).
SIGKILL_EXIT_CODE: Final[int] = 137
#: Synthetic exit code recorded when an attempt is killed on its deadline.
DEADLINE_EXIT_CODE: Final[int] = 124
#: Pod label the Job controller stamps with the owning Job's name.
_JOB_NAME_LABEL: Final[str] = "job-name"
#: Default poll interval (seconds) while awaiting terminal Pod state.
_DEFAULT_POLL_INTERVAL: Final[float] = 1.0
#: Default ceiling (seconds) for the input bridge to reach ``Running`` so the
#: monitor can stream ``/custos/in`` in; exceeding it surfaces a sandbox failure.
_DEFAULT_START_TIMEOUT: Final[float] = 120.0


@dataclass(frozen=True, slots=True)
class ExecResult:
    """The outcome of a single ``pods/exec`` invocation.

    ``exit_code`` is the remote command's status (``0`` on success) and
    ``stderr`` carries whatever the command wrote to standard error, surfaced in
    the :class:`~custos_arm.runtime.oci.errors.SandboxFailureError` message when
    the command fails.
    """

    exit_code: int
    stderr: str = ""


class PodExec(Protocol):
    """Runs a command in a pod container, optionally feeding it ``stdin``.

    Injected into :class:`OciContainerDriver` so the streaming logic is
    unit-testable with a fake channel; the default implementation drives the
    Kubernetes ``connect_get_namespaced_pod_exec`` websocket.
    """

    def __call__(
        self,
        *,
        namespace: str,
        pod: str,
        container: str,
        command: list[str],
        stdin: bytes | None,
    ) -> ExecResult: ...


def is_image_pull_waiting_reason(reason: str | None) -> bool:
    """Whether a container ``waiting.reason`` indicates an image-pull failure."""
    return reason in IMAGE_PULL_WAITING_REASONS


def classify_signal(
    *,
    exit_code: int,
    terminated_reason: str | None,
    cancel_reason: CancelReason | None,
) -> SandboxSignal:
    """Translate raw Pod termination facts into a :class:`SandboxSignal`.

    A driver-initiated termination (deadline / cancel) wins over the kernel
    signal; otherwise an ``OOMKilled`` reason or a SIGKILL exit code is
    surfaced, and a clean self-exit reports :attr:`SandboxSignal.NONE`.
    """
    if cancel_reason is CancelReason.DEADLINE:
        return SandboxSignal.DEADLINE
    if cancel_reason in (CancelReason.CANCELLED, CancelReason.SHUTDOWN):
        return SandboxSignal.CANCELLED
    if terminated_reason == OOM_TERMINATED_REASON:
        return SandboxSignal.OOM
    if exit_code == SIGKILL_EXIT_CODE:
        return SandboxSignal.KILLED
    return SandboxSignal.NONE


def signal_error_code(signal: SandboxSignal) -> tuple[str, ErrorClass] | None:
    """Map a non-clean :class:`SandboxSignal` to its ``(code, class)``.

    Returns ``None`` for :attr:`SandboxSignal.NONE` (the activity exited on its
    own — the Result Mapper classifies by exit code + ``outputs.json``).
    """
    from custos_arm.runtime.oci.errors import (
        ACTIVITY_CANCELLED,
        ACTIVITY_OOM_KILLED,
        ACTIVITY_SANDBOX_FAILURE,
        ACTIVITY_TIMEOUT,
    )

    mapping: dict[SandboxSignal, tuple[str, ErrorClass]] = {
        SandboxSignal.OOM: (ACTIVITY_OOM_KILLED, ErrorClass.RETRYABLE),
        SandboxSignal.KILLED: (ACTIVITY_SANDBOX_FAILURE, ErrorClass.RETRYABLE),
        SandboxSignal.DEADLINE: (ACTIVITY_TIMEOUT, ErrorClass.CANCELLED),
        SandboxSignal.CANCELLED: (ACTIVITY_CANCELLED, ErrorClass.CANCELLED),
    }
    return mapping.get(signal)


class OciContainerDriver:
    """Kubernetes-``Job``-backed :class:`~custos_arm.runtime.driver.RuntimeDriver`.

    Args:
        batch_api: a ``kubernetes.client.BatchV1Api`` (Job CRUD).
        core_api: a ``kubernetes.client.CoreV1Api`` (Pod status).
        staging_root: host directory under which per-attempt ``in`` / ``out``
            staging trees are created and exposed via the
            :class:`~custos_arm.runtime.models.SandboxHandle`.
        poll_interval: seconds between Pod status polls in ``await_terminal``.
        sleep: injectable sleep (defaults to :func:`time.sleep`).
        now: injectable clock returning an aware UTC ``datetime``.
    """

    kind: str = OCI_CONTAINER_KIND

    def __init__(
        self,
        *,
        batch_api: Any,
        core_api: Any,
        staging_root: Path,
        poll_interval: float = _DEFAULT_POLL_INTERVAL,
        sleep: Callable[[float], None] = time.sleep,
        now: Callable[[], datetime] | None = None,
        pod_exec: PodExec | None = None,
        start_timeout: float = _DEFAULT_START_TIMEOUT,
    ) -> None:
        self._batch = batch_api
        self._core = core_api
        self._staging_root = staging_root
        self._poll_interval = poll_interval
        self._sleep = sleep
        self._now = now or (lambda: datetime.now(UTC))
        self._exec = pod_exec or self._default_pod_exec
        self._start_timeout = start_timeout
        # Records a scheduler-driven cancel keyed by handle reference so the
        # blocking ``await_terminal`` loop (running on another thread) observes
        # it and reports the right signal instead of blocking to the deadline.
        self._cancellations: dict[str, CancelReason] = {}

    # -- lifecycle ---------------------------------------------------------- #

    def prepare(self, plan: SandboxPlan) -> SandboxHandle:
        """Create the (suspended) Job and the in/out staging trees.

        The Job is created ``suspend: true`` so the Pod is not started until
        :meth:`start`, giving the I/O Broker and Secret Injector a window to
        populate ``/custos/in`` first.
        """
        name = job_name(plan.step)
        manifest = build_activity_job(plan)
        manifest["spec"]["suspend"] = True

        input_root = self._staging_root / name / "in"
        output_root = self._staging_root / name / "out"
        input_root.mkdir(parents=True, exist_ok=True)
        output_root.mkdir(parents=True, exist_ok=True)

        try:
            self._batch.create_namespaced_job(namespace=plan.namespace, body=manifest)
        except Exception as exc:
            # Don't leak the staging tree we just created if the Job never lands.
            shutil.rmtree(self._staging_root / name, ignore_errors=True)
            raise SandboxFailureError(f"failed to create sandbox Job {name!r}: {exc}") from exc

        return SandboxHandle(
            kind=self.kind,
            reference=f"{plan.namespace}/{name}",
            input_root=input_root,
            output_root=output_root,
        )

    def start(self, handle: SandboxHandle) -> None:
        """Un-suspend the Job, then stream ``/custos/in`` in and release the gate.

        After the Pod is scheduled the input bridge init container blocks on the
        readiness sentinel. This method ``tar``-streams ARM's host-local staging
        ``in/`` tree into that container over ``pods/exec`` and then writes the
        sentinel, which lets the init container complete and the activity
        container start behind a fully-staged ``/custos/in``.

        Raises:
            SandboxFailureError: the Job could not be un-suspended, the input
                bridge never started, or the streaming/sentinel exec failed.
        """
        namespace, name = _split_reference(handle.reference)
        try:
            self._batch.patch_namespaced_job(
                name=name,
                namespace=namespace,
                body={"spec": {"suspend": False}},
            )
        except Exception as exc:
            raise SandboxFailureError(f"failed to start sandbox Job {name!r}: {exc}") from exc

        self._stream_inputs(handle, namespace, name)

    def _stream_inputs(self, handle: SandboxHandle, namespace: str, name: str) -> None:
        """Stream the host ``in/`` tree into the input bridge and drop the gate."""
        pod_name = self._await_input_bridge(namespace, name)
        archive = _tar_directory(handle.input_root)

        # The raw tar is fed straight to ``tar -x``: the archive's end-of-archive
        # marker lets ``tar`` finish and exit on its own, so we never have to
        # half-close the exec stdin channel (which the client cannot do).
        extracted = self._exec(
            namespace=namespace,
            pod=pod_name,
            container=INPUT_BRIDGE_CONTAINER_NAME,
            command=["tar", "-x", "-C", "/custos/in"],
            stdin=archive,
        )
        if extracted.exit_code != 0:
            raise SandboxFailureError(
                f"failed to stream inputs into sandbox {name!r} "
                f"(exit {extracted.exit_code}): {extracted.stderr}"
            )

        released = self._exec(
            namespace=namespace,
            pod=pod_name,
            container=INPUT_BRIDGE_CONTAINER_NAME,
            command=["touch", INPUT_READY_SENTINEL],
            stdin=None,
        )
        if released.exit_code != 0:
            raise SandboxFailureError(
                f"failed to release input bridge for sandbox {name!r} "
                f"(exit {released.exit_code}): {released.stderr}"
            )

    def _await_input_bridge(self, namespace: str, name: str) -> str:
        """Block until the input bridge container is ``Running``; return its Pod.

        Raises:
            SandboxFailureError: the bridge terminated before inputs were staged
                or did not start within ``start_timeout``.
        """
        deadline = self._now() + timedelta(seconds=self._start_timeout)
        while True:
            pod = self._activity_pod(namespace, name)
            if pod is not None:
                state = _init_container_state(pod, INPUT_BRIDGE_CONTAINER_NAME)
                if state is not None and getattr(state, "running", None) is not None:
                    return str(pod.metadata.name)
                if state is not None and getattr(state, "terminated", None) is not None:
                    raise SandboxFailureError(
                        f"input bridge for sandbox {name!r} exited before inputs were staged"
                    )
            if self._now() >= deadline:
                raise SandboxFailureError(
                    f"input bridge for sandbox {name!r} did not start within {self._start_timeout}s"
                )
            self._sleep(self._poll_interval)

    def await_terminal(self, handle: SandboxHandle, deadline: datetime) -> SandboxOutcome:
        """Block until the activity container terminates or the deadline passes.

        A scheduler-driven :meth:`cancel` (observed via the recorded reason) or
        the elapsed ``deadline`` short-circuits the wait so the caller is not
        blocked past a requested cancellation.

        Raises:
            ImagePullError: the image could not be pulled (no exit code).
        """
        namespace, name = _split_reference(handle.reference)
        while True:
            cancel_reason = self._cancellations.get(handle.reference)
            if cancel_reason is not None:
                signal = classify_signal(
                    exit_code=SIGKILL_EXIT_CODE,
                    terminated_reason=None,
                    cancel_reason=cancel_reason,
                )
                return SandboxOutcome(exit_code=SIGKILL_EXIT_CODE, signal=signal)

            pod = self._activity_pod(namespace, name)
            container = _activity_container_status(pod) if pod is not None else None

            if container is not None:
                waiting_reason = _waiting_reason(container)
                if is_image_pull_waiting_reason(waiting_reason):
                    raise ImagePullError(
                        f"sandbox {name!r} could not pull its image: {waiting_reason}"
                    )
                terminated = _terminated_state(container)
                if terminated is not None:
                    exit_code = int(terminated.exit_code)
                    signal = classify_signal(
                        exit_code=exit_code,
                        terminated_reason=terminated.reason,
                        cancel_reason=None,
                    )
                    return SandboxOutcome(exit_code=exit_code, signal=signal)

            if self._now() >= deadline:
                self.cancel(handle, CancelReason.DEADLINE)
                return SandboxOutcome(exit_code=DEADLINE_EXIT_CODE, signal=SandboxSignal.DEADLINE)

            self._sleep(self._poll_interval)

    def cancel(self, handle: SandboxHandle, reason: CancelReason) -> None:
        """Idempotently delete the Job (and its Pod). A missing Job is a no-op.

        The ``reason`` is recorded so a concurrent :meth:`await_terminal` reports
        the matching signal (deadline / cancelled) rather than the raw kernel
        SIGKILL that deleting the Job induces.
        """
        namespace, name = _split_reference(handle.reference)
        self._cancellations[handle.reference] = reason
        self._delete_job(namespace, name)

    def collect(self, handle: SandboxHandle) -> OutputBundle:
        """Expose the ``/custos/out`` staging tree to the I/O Broker."""
        return OutputBundle(root=handle.output_root)

    def cleanup(self, handle: SandboxHandle) -> None:
        """Reap the Job and remove the per-attempt staging trees (idempotent)."""
        namespace, name = _split_reference(handle.reference)
        self._delete_job(namespace, name)
        shutil.rmtree(self._staging_root / name, ignore_errors=True)
        self._cancellations.pop(handle.reference, None)

    # -- internals ---------------------------------------------------------- #

    def _delete_job(self, namespace: str, name: str) -> None:
        try:
            self._batch.delete_namespaced_job(
                name=name,
                namespace=namespace,
                propagation_policy="Background",
            )
        except Exception as exc:
            if _is_not_found(exc):
                return
            raise SandboxFailureError(f"failed to delete sandbox Job {name!r}: {exc}") from exc

    def _activity_pod(self, namespace: str, name: str) -> Any | None:
        try:
            pods = self._core.list_namespaced_pod(
                namespace=namespace,
                label_selector=f"{_JOB_NAME_LABEL}={name}",
            )
        except Exception as exc:
            raise SandboxFailureError(
                f"failed to list sandbox Pods for Job {name!r}: {exc}"
            ) from exc
        items = list(pods.items)
        return items[0] if items else None

    def _default_pod_exec(  # pragma: no cover - exercised against a real cluster
        self,
        *,
        namespace: str,
        pod: str,
        container: str,
        command: list[str],
        stdin: bytes | None,
    ) -> ExecResult:
        """Drive ``connect_get_namespaced_pod_exec`` over a websocket stream."""
        from kubernetes.stream import stream

        client = stream(
            self._core.connect_get_namespaced_pod_exec,
            pod,
            namespace,
            container=container,
            command=command,
            stderr=True,
            stdin=stdin is not None,
            stdout=True,
            tty=False,
            _preload_content=False,
        )
        stderr_parts: list[str] = []
        try:
            if stdin is not None:
                client.write_stdin(stdin)
            while client.is_open():
                client.update(timeout=1)
                if client.peek_stdout():
                    client.read_stdout()
                if client.peek_stderr():
                    stderr_parts.append(client.read_stderr())
            returncode = client.returncode
        finally:
            client.close()
        return ExecResult(
            exit_code=returncode if returncode is not None else 0,
            stderr="".join(stderr_parts),
        )


def _split_reference(reference: str) -> tuple[str, str]:
    namespace, _, name = reference.partition("/")
    return namespace, name


def _tar_directory(root: Path) -> bytes:
    """Pack ``root``'s contents into an uncompressed tar (members relative to it).

    Entries are added under paths relative to ``root`` so the archive extracts
    directly into ``/custos/in``. An empty tree yields a valid empty archive.
    """
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as archive:
        for path in sorted(root.rglob("*")):
            archive.add(path, arcname=str(path.relative_to(root)), recursive=False)
    return buffer.getvalue()


def _init_container_state(pod: Any, container_name: str) -> Any | None:
    status = getattr(pod, "status", None)
    if status is None:
        return None
    statuses = getattr(status, "init_container_statuses", None) or []
    for container_status in statuses:
        if container_status.name == container_name:
            return getattr(container_status, "state", None)
    return None


def _activity_container_status(pod: Any) -> Any | None:
    status = getattr(pod, "status", None)
    if status is None:
        return None
    statuses = getattr(status, "container_statuses", None) or []
    for container_status in statuses:
        if container_status.name == ACTIVITY_CONTAINER_NAME:
            return container_status
    return None


def _waiting_reason(container_status: Any) -> str | None:
    state = getattr(container_status, "state", None)
    waiting = getattr(state, "waiting", None) if state is not None else None
    if waiting is None:
        return None
    reason: str | None = waiting.reason
    return reason


def _terminated_state(container_status: Any) -> Any | None:
    state = getattr(container_status, "state", None)
    if state is None:
        return None
    return getattr(state, "terminated", None)


def _is_not_found(exc: Exception) -> bool:
    return getattr(exc, "status", None) == 404
