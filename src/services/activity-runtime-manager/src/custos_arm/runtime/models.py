"""Runtime-driver domain types (ARM-IMPL-013).

These are the runtime-kind-agnostic value objects exchanged between the
Activity Scheduler and a concrete :class:`~custos_arm.runtime.driver.RuntimeDriver`.
They are internal domain objects — not wire contracts — so they are plain
frozen dataclasses (no pydantic aliases), per the ARM persistence-model
convention.

The split mirrors the design § Runtime Driver dispatcher contract: the
Scheduler owns the cross-cutting concerns (I/O, secrets, artifacts, result
mapping) and hands the driver a fully-resolved :class:`SandboxPlan`; the
driver hands back an opaque :class:`SandboxHandle`, a raw
:class:`SandboxOutcome`, and a read-only :class:`OutputBundle`. The driver
never interprets ``outputs.json``, never classifies errors, and never
touches the artifact store.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Final

from custos_arm.contract import ImageRef, StepRef
from custos_arm.limit import EffectiveResources

__all__ = [
    "OCI_CONTAINER_KIND",
    "CancelReason",
    "OutputBundle",
    "SandboxHandle",
    "SandboxOutcome",
    "SandboxPlan",
    "SandboxSignal",
    "SidecarSpec",
    "TmpfsMount",
]

#: The only runtime kind ARM registers in v1. ``http`` (M3) and ``wasm``
#: (M4+) are reserved; their drivers slot into the same dispatcher.
OCI_CONTAINER_KIND: Final[str] = "oci-container"


class SandboxSignal(StrEnum):
    """How a sandbox terminated, as reported by the driver.

    The Result Mapper — not the driver — turns this plus the exit code into
    an :class:`~custos_arm.result.ResultClass`. ``NONE`` means the process
    exited on its own with no externally-induced signal.
    """

    NONE = "none"
    OOM = "oom"
    KILLED = "killed"
    DEADLINE = "deadline"
    CANCELLED = "cancelled"


class CancelReason(StrEnum):
    """Why the Scheduler asked the driver to terminate a sandbox."""

    DEADLINE = "deadline"
    CANCELLED = "cancelled"
    SHUTDOWN = "shutdown"


@dataclass(frozen=True, slots=True)
class TmpfsMount:
    """A ``tmpfs`` (``medium: Memory``) mount in the sandbox.

    The contract paths (``/custos/in/*``, ``/custos/out``) are the only
    writable mounts. ``read_only`` marks a mount the activity may read but
    not write (the input tree); ``size_limit`` is an optional Kubernetes
    quantity (e.g. ``"64Mi"``) capping the mount.
    """

    mount_path: str
    read_only: bool = False
    size_limit: str | None = None


@dataclass(frozen=True, slots=True)
class SidecarSpec:
    """The connector sidecar injected alongside every activity container.

    ``endpoint`` is the Connector Service address the sidecar dials on the
    activity's behalf; the activity itself only ever talks to the sidecar.
    """

    image: str
    endpoint: str


@dataclass(frozen=True, slots=True)
class SandboxPlan:
    """The fully-resolved request to stand up a single activity sandbox.

    Everything a driver needs is pre-computed by the Scheduler: the pinned
    activity ``image`` (with digest), the effective resource envelope plus
    isolation tier / ``RuntimeClass`` (:class:`EffectiveResources`), the
    ``tmpfs`` mounts realizing the contract filesystem, the connector
    ``sidecar``, the ``io_bridge_image`` backing the input-injector init
    container and the output-collector native sidecar, the target
    ``namespace``, and the attempt ``deadline``.
    """

    step: StepRef
    namespace: str
    image: ImageRef
    resources: EffectiveResources
    tmpfs_mounts: tuple[TmpfsMount, ...]
    sidecar: SidecarSpec
    io_bridge_image: str
    deadline: datetime


@dataclass(frozen=True, slots=True)
class SandboxHandle:
    """An opaque, driver-owned reference to a prepared sandbox.

    ``reference`` is meaningful only to the driver that produced it (for the
    OCI driver: the ``namespace/job-name``). ``input_root`` and
    ``output_root`` are the host-side paths the I/O Broker writes
    ``/custos/in`` to and reads ``/custos/out`` from.
    """

    kind: str
    reference: str
    input_root: Path
    output_root: Path


@dataclass(frozen=True, slots=True)
class SandboxOutcome:
    """The raw termination signal of a finished sandbox.

    Classification (success / retryable / permanent / cancelled) is the
    Result Mapper's job, not the driver's.
    """

    exit_code: int
    signal: SandboxSignal


@dataclass(frozen=True, slots=True)
class OutputBundle:
    """A read-only view over the materialized ``/custos/out`` tree.

    ``root`` is the host-side directory the I/O Broker reads outputs and
    artifacts from after the sandbox exits.
    """

    root: Path
