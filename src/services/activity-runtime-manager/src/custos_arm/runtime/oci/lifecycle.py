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

import shutil
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

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
from custos_arm.runtime.oci.job import ACTIVITY_CONTAINER_NAME, build_activity_job, job_name

__all__ = [
    "DEADLINE_EXIT_CODE",
    "IMAGE_PULL_WAITING_REASONS",
    "OOM_TERMINATED_REASON",
    "SIGKILL_EXIT_CODE",
    "OciContainerDriver",
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
    ) -> None:
        self._batch = batch_api
        self._core = core_api
        self._staging_root = staging_root
        self._poll_interval = poll_interval
        self._sleep = sleep
        self._now = now or (lambda: datetime.now(UTC))

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
            raise SandboxFailureError(f"failed to create sandbox Job {name!r}: {exc}") from exc

        return SandboxHandle(
            kind=self.kind,
            reference=f"{plan.namespace}/{name}",
            input_root=input_root,
            output_root=output_root,
        )

    def start(self, handle: SandboxHandle) -> None:
        """Un-suspend the Job so the activity Pod is scheduled and run."""
        namespace, name = _split_reference(handle.reference)
        try:
            self._batch.patch_namespaced_job(
                name=name,
                namespace=namespace,
                body={"spec": {"suspend": False}},
            )
        except Exception as exc:
            raise SandboxFailureError(f"failed to start sandbox Job {name!r}: {exc}") from exc

    def await_terminal(self, handle: SandboxHandle, deadline: datetime) -> SandboxOutcome:
        """Block until the activity container terminates or the deadline passes.

        Raises:
            ImagePullError: the image could not be pulled (no exit code).
        """
        namespace, name = _split_reference(handle.reference)
        while True:
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
        """Idempotently delete the Job (and its Pod). A missing Job is a no-op."""
        namespace, name = _split_reference(handle.reference)
        self._delete_job(namespace, name)

    def collect(self, handle: SandboxHandle) -> OutputBundle:
        """Expose the ``/custos/out`` staging tree to the I/O Broker."""
        return OutputBundle(root=handle.output_root)

    def cleanup(self, handle: SandboxHandle) -> None:
        """Reap the Job and remove the per-attempt staging trees (idempotent)."""
        namespace, name = _split_reference(handle.reference)
        self._delete_job(namespace, name)
        shutil.rmtree(self._staging_root / name, ignore_errors=True)

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
        pods = self._core.list_namespaced_pod(
            namespace=namespace,
            label_selector=f"{_JOB_NAME_LABEL}={name}",
        )
        items = list(pods.items)
        return items[0] if items else None


def _split_reference(reference: str) -> tuple[str, str]:
    namespace, _, name = reference.partition("/")
    return namespace, name


def _activity_container_status(pod: Any) -> Any | None:
    statuses = getattr(pod.status, "container_statuses", None) or []
    for status in statuses:
        if status.name == ACTIVITY_CONTAINER_NAME:
            return status
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
