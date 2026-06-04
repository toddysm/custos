"""The ``RuntimeDriver`` Protocol and its dispatcher (ARM-IMPL-013).

Every runtime kind (``oci-container`` in v1; ``http`` / ``wasm`` reserved)
implements the :class:`RuntimeDriver` Protocol. The
:class:`RuntimeDriverDispatcher` selects the concrete driver by
``manifest.spec.runtime.kind``; adding a runtime kind is just registering a
new driver, so nothing above the dispatcher changes.

The lifecycle methods are deliberately synchronous and blocking (notably
:meth:`RuntimeDriver.await_terminal`). The Activity Scheduler is responsible
for offloading them off the event loop (e.g. via :func:`asyncio.to_thread`).
"""

from __future__ import annotations

from datetime import datetime
from typing import Protocol, runtime_checkable

from .errors import DuplicateRuntimeKindError, UnknownRuntimeKindError
from .models import (
    CancelReason,
    OutputBundle,
    SandboxHandle,
    SandboxOutcome,
    SandboxPlan,
)

__all__ = [
    "RuntimeDriver",
    "RuntimeDriverDispatcher",
]


@runtime_checkable
class RuntimeDriver(Protocol):
    """The runtime-specific lifecycle for one ``runtime.kind``.

    A driver owns only the sandbox lifecycle; the Scheduler owns I/O,
    secrets, artifacts, and result mapping. A driver never interprets
    ``outputs.json``, never classifies errors, and never touches the
    artifact store.
    """

    #: Matches ``manifest.spec.runtime.kind`` (e.g. ``"oci-container"``).
    kind: str

    def prepare(self, plan: SandboxPlan) -> SandboxHandle:
        """Create the sandbox and its in/out volumes WITHOUT starting the
        activity process, so the I/O Broker and Secret Injector can
        populate ``/custos/in`` against the returned handle first."""
        ...

    def start(self, handle: SandboxHandle) -> None:
        """Start the activity process. Non-blocking."""
        ...

    def await_terminal(self, handle: SandboxHandle, deadline: datetime) -> SandboxOutcome:
        """Block until the process exits, the deadline elapses, or a cancel
        is observed, then return the raw exit signal."""
        ...

    def cancel(self, handle: SandboxHandle, reason: CancelReason) -> None:
        """Idempotently terminate the sandbox. Safe to call after exit."""
        ...

    def collect(self, handle: SandboxHandle) -> OutputBundle:
        """Expose the ``/custos/out`` tree to the I/O Broker after exit."""
        ...

    def cleanup(self, handle: SandboxHandle) -> None:
        """Reap all sandbox resources (Job, Pod, volumes, tmpfs)."""
        ...


class RuntimeDriverDispatcher:
    """Selects a concrete :class:`RuntimeDriver` by ``runtime.kind``.

    v1 registers only the OCI Container Driver, but the dispatcher itself is
    kind-agnostic: drivers are registered at wiring time and looked up by
    their ``kind``.
    """

    def __init__(self, drivers: tuple[RuntimeDriver, ...] = ()) -> None:
        self._by_kind: dict[str, RuntimeDriver] = {}
        for driver in drivers:
            self.register(driver)

    def register(self, driver: RuntimeDriver) -> None:
        """Register ``driver`` under its ``kind``.

        Raises:
            DuplicateRuntimeKindError: another driver already claims that kind.
        """
        if driver.kind in self._by_kind:
            raise DuplicateRuntimeKindError(driver.kind)
        self._by_kind[driver.kind] = driver

    def select(self, kind: str) -> RuntimeDriver:
        """Return the driver registered for ``kind``.

        Raises:
            UnknownRuntimeKindError: no driver is registered for ``kind``.
        """
        try:
            return self._by_kind[kind]
        except KeyError:
            raise UnknownRuntimeKindError(kind, self.registered_kinds) from None

    @property
    def registered_kinds(self) -> tuple[str, ...]:
        """The kinds with a registered driver, in registration order."""
        return tuple(self._by_kind)
