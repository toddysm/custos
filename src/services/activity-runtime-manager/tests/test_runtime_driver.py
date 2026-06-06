"""Tests for the RuntimeDriver Protocol, types, and dispatcher (ARM-IMPL-013)."""

from __future__ import annotations

import dataclasses
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from custos_arm.contract import ImageRef, StepRef
from custos_arm.limit import EffectiveResources
from custos_arm.manifest import IsolationTier
from custos_arm.runtime import (
    OCI_CONTAINER_KIND,
    CancelReason,
    DuplicateRuntimeKindError,
    OutputBundle,
    RuntimeDriver,
    RuntimeDriverDispatcher,
    SandboxHandle,
    SandboxOutcome,
    SandboxPlan,
    SandboxSignal,
    SidecarSpec,
    TmpfsMount,
    UnknownRuntimeKindError,
)


class _FakeDriver:
    """Minimal in-memory :class:`RuntimeDriver` for dispatcher tests."""

    def __init__(self, kind: str) -> None:
        self.kind = kind

    def prepare(self, plan: SandboxPlan) -> SandboxHandle:
        return SandboxHandle(
            kind=self.kind,
            reference="ns/job",
            input_root=Path("/in"),
            output_root=Path("/out"),
        )

    def start(self, handle: SandboxHandle) -> None:
        return None

    def await_terminal(self, handle: SandboxHandle, deadline: datetime) -> SandboxOutcome:
        return SandboxOutcome(exit_code=0, signal=SandboxSignal.NONE)

    def cancel(self, handle: SandboxHandle, reason: CancelReason) -> None:
        return None

    def collect(self, handle: SandboxHandle) -> OutputBundle:
        return OutputBundle(root=handle.output_root)

    def cleanup(self, handle: SandboxHandle) -> None:
        return None


def _effective_resources() -> EffectiveResources:
    return EffectiveResources(
        cpu_request="250m",
        cpu_limit="1",
        memory_request="256Mi",
        memory_limit="1Gi",
        ephemeral_storage_limit="2Gi",
        timeout=timedelta(minutes=30),
        tier=IsolationTier.PROCESS,
        runtime_class="",
    )


def _plan() -> SandboxPlan:
    return SandboxPlan(
        step=StepRef(runId="run-1", stepId="step-1", attempt=1),
        namespace="custos-activities",
        image=ImageRef(ref="ghcr.io/acme/scan@sha256:abc", digest="sha256:abc"),
        resources=_effective_resources(),
        tmpfs_mounts=(
            TmpfsMount(mount_path="/custos/in", read_only=True, size_limit="64Mi"),
            TmpfsMount(mount_path="/custos/out"),
        ),
        sidecar=SidecarSpec(image="ghcr.io/custos/sidecar:1", endpoint="connector:9090"),
        io_bridge_image="ghcr.io/custos/io-bridge:1",
        deadline=datetime(2026, 1, 1, tzinfo=UTC),
    )


# ---------------------------------------------------------------------------
# Constants / enums
# ---------------------------------------------------------------------------


def test_oci_kind_constant() -> None:
    assert OCI_CONTAINER_KIND == "oci-container"


def test_sandbox_signal_values() -> None:
    assert [s.value for s in SandboxSignal] == [
        "none",
        "oom",
        "killed",
        "deadline",
        "cancelled",
    ]


def test_cancel_reason_values() -> None:
    assert {r.value for r in CancelReason} == {"deadline", "cancelled", "shutdown"}


# ---------------------------------------------------------------------------
# Value-object typing / immutability
# ---------------------------------------------------------------------------


def test_sandbox_plan_is_frozen() -> None:
    plan = _plan()
    with pytest.raises(dataclasses.FrozenInstanceError):
        plan.namespace = "other"  # type: ignore[misc]


def test_sandbox_plan_round_trips_its_inputs() -> None:
    plan = _plan()
    assert plan.image.digest == "sha256:abc"
    assert plan.resources.tier is IsolationTier.PROCESS
    assert [m.mount_path for m in plan.tmpfs_mounts] == ["/custos/in", "/custos/out"]
    assert plan.sidecar.endpoint == "connector:9090"
    assert plan.step.run_id == "run-1"


def test_tmpfs_mount_defaults() -> None:
    mount = TmpfsMount(mount_path="/custos/out")
    assert mount.read_only is False
    assert mount.size_limit is None


def test_sandbox_outcome_is_frozen() -> None:
    outcome = SandboxOutcome(exit_code=137, signal=SandboxSignal.OOM)
    with pytest.raises(dataclasses.FrozenInstanceError):
        outcome.exit_code = 0  # type: ignore[misc]


def test_output_bundle_holds_root() -> None:
    bundle = OutputBundle(root=Path("/custos/out"))
    assert bundle.root == Path("/custos/out")


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


def test_fake_driver_satisfies_protocol() -> None:
    assert isinstance(_FakeDriver(OCI_CONTAINER_KIND), RuntimeDriver)


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------


def test_dispatcher_selects_oci_driver() -> None:
    driver = _FakeDriver(OCI_CONTAINER_KIND)
    dispatcher = RuntimeDriverDispatcher((driver,))
    assert dispatcher.select(OCI_CONTAINER_KIND) is driver


def test_dispatcher_raises_for_unregistered_kind() -> None:
    dispatcher = RuntimeDriverDispatcher((_FakeDriver(OCI_CONTAINER_KIND),))
    with pytest.raises(UnknownRuntimeKindError) as excinfo:
        dispatcher.select("wasm")
    assert excinfo.value.kind == "wasm"
    assert excinfo.value.registered == (OCI_CONTAINER_KIND,)


def test_dispatcher_empty_registry_raises() -> None:
    dispatcher = RuntimeDriverDispatcher()
    with pytest.raises(UnknownRuntimeKindError):
        dispatcher.select(OCI_CONTAINER_KIND)
    assert dispatcher.registered_kinds == ()


def test_dispatcher_register_after_construction() -> None:
    dispatcher = RuntimeDriverDispatcher()
    driver = _FakeDriver(OCI_CONTAINER_KIND)
    dispatcher.register(driver)
    assert dispatcher.select(OCI_CONTAINER_KIND) is driver


def test_dispatcher_rejects_duplicate_kind() -> None:
    dispatcher = RuntimeDriverDispatcher((_FakeDriver(OCI_CONTAINER_KIND),))
    with pytest.raises(DuplicateRuntimeKindError) as excinfo:
        dispatcher.register(_FakeDriver(OCI_CONTAINER_KIND))
    assert excinfo.value.kind == OCI_CONTAINER_KIND


def test_registered_kinds_preserves_registration_order() -> None:
    dispatcher = RuntimeDriverDispatcher(
        (_FakeDriver("oci-container"), _FakeDriver("http"), _FakeDriver("wasm"))
    )
    assert dispatcher.registered_kinds == ("oci-container", "http", "wasm")


def test_selected_driver_round_trips_a_plan() -> None:
    dispatcher = RuntimeDriverDispatcher((_FakeDriver(OCI_CONTAINER_KIND),))
    driver = dispatcher.select(OCI_CONTAINER_KIND)
    handle = driver.prepare(_plan())
    assert handle.kind == OCI_CONTAINER_KIND
    assert driver.collect(handle).root == handle.output_root
