"""End-to-end integration tests for the OCI Container Driver (ARM-IMPL-016).

These run only in the dedicated ``activity-runtime-manager-integration`` CI job
against a real ``kind`` cluster (they are ``integration``-marked and therefore
excluded from the default unit run). They exercise the full lifecycle —
``prepare`` → ``start`` → ``await_terminal`` → ``collect`` → ``cleanup`` — plus
image-pull failure detection and idempotent cancel/cleanup.

The CI job builds a tiny non-root test image (``ENTRYPOINT /bin/true``) and
``kind load``s it under ``CUSTOS_ARM_E2E_IMAGE`` (default ``custos-arm-e2e:test``)
so the hardened security context (``runAsNonRoot``/``readOnlyRootFilesystem``)
is satisfied without a registry pull.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from custos_arm.contract import ImageRef, StepRef
from custos_arm.limit import EffectiveResources
from custos_arm.manifest import IsolationTier
from custos_arm.runtime.models import (
    CancelReason,
    SandboxPlan,
    SandboxSignal,
    SidecarSpec,
    TmpfsMount,
)
from custos_arm.runtime.oci import ImagePullError, OciContainerDriver

pytestmark = pytest.mark.integration

_NAMESPACE = "custos-arm-e2e"
_E2E_IMAGE = os.environ.get("CUSTOS_ARM_E2E_IMAGE", "custos-arm-e2e:test")


@pytest.fixture(scope="module")
def _apis() -> Iterator[tuple[object, object]]:
    """Connect to the in-context cluster, ensure the namespace exists."""
    kubernetes = pytest.importorskip("kubernetes")
    from kubernetes.client.exceptions import ApiException

    try:
        kubernetes.config.load_kube_config()
    except Exception:
        try:
            kubernetes.config.load_incluster_config()
        except Exception as exc:
            pytest.skip(f"no reachable Kubernetes cluster: {exc}")

    batch = kubernetes.client.BatchV1Api()
    core = kubernetes.client.CoreV1Api()

    body = {"metadata": {"name": _NAMESPACE}}
    try:
        core.create_namespace(body=body)
    except ApiException as exc:
        if exc.status != 409:  # already exists is fine
            raise

    yield batch, core


def _driver(apis: tuple[object, object], staging_root: Path) -> OciContainerDriver:
    batch, core = apis
    return OciContainerDriver(
        batch_api=batch,
        core_api=core,
        staging_root=staging_root,
        poll_interval=2.0,
    )


def _plan(*, image: str, run_id: str) -> SandboxPlan:
    return SandboxPlan(
        step=StepRef.model_validate({"runId": run_id, "stepId": "step-a", "attempt": 1}),
        namespace=_NAMESPACE,
        image=ImageRef(ref=image),
        resources=EffectiveResources(
            cpu_request="50m",
            cpu_limit="200m",
            memory_request="32Mi",
            memory_limit="64Mi",
            ephemeral_storage_limit="64Mi",
            timeout=timedelta(seconds=120),
            tier=IsolationTier.PROCESS,
            runtime_class="",
        ),
        tmpfs_mounts=(
            TmpfsMount(mount_path="/custos/in", read_only=True, size_limit="16Mi"),
            TmpfsMount(mount_path="/custos/out", size_limit="16Mi"),
        ),
        sidecar=SidecarSpec(image=_E2E_IMAGE, endpoint="http://127.0.0.1:8080"),
        io_bridge_image=_E2E_IMAGE,
        deadline=datetime.now(UTC) + timedelta(seconds=120),
    )


def test_lifecycle_happy_path(_apis: tuple[object, object], tmp_path: Path) -> None:
    driver = _driver(_apis, tmp_path)
    plan = _plan(image=_E2E_IMAGE, run_id=f"ok-{uuid.uuid4().hex[:8]}")

    handle = driver.prepare(plan)
    try:
        driver.start(handle)
        outcome = driver.await_terminal(handle, plan.deadline)
        assert outcome.exit_code == 0
        assert outcome.signal is SandboxSignal.NONE

        bundle = driver.collect(handle)
        assert bundle.root.is_dir()
    finally:
        driver.cleanup(handle)

    # No orphaned Job after cleanup.
    from kubernetes.client.exceptions import ApiException

    batch, _ = _apis
    with pytest.raises(ApiException) as excinfo:
        batch.read_namespaced_job(  # type: ignore[attr-defined]
            name=handle.reference.split("/", 1)[1], namespace=_NAMESPACE
        )
    assert excinfo.value.status == 404


def test_image_pull_failure_is_surfaced(_apis: tuple[object, object], tmp_path: Path) -> None:
    driver = _driver(_apis, tmp_path)
    plan = _plan(
        image="registry.invalid.example/nope:absent",
        run_id=f"pull-{uuid.uuid4().hex[:8]}",
    )

    handle = driver.prepare(plan)
    try:
        driver.start(handle)
        with pytest.raises(ImagePullError):
            driver.await_terminal(handle, plan.deadline)
    finally:
        driver.cleanup(handle)


def test_cancel_and_cleanup_are_idempotent(_apis: tuple[object, object], tmp_path: Path) -> None:
    driver = _driver(_apis, tmp_path)
    plan = _plan(image=_E2E_IMAGE, run_id=f"idem-{uuid.uuid4().hex[:8]}")

    handle = driver.prepare(plan)
    driver.start(handle)

    driver.cancel(handle, CancelReason.CANCELLED)
    # Second cancel + double cleanup must not raise even though the Job is gone.
    driver.cancel(handle, CancelReason.CANCELLED)
    driver.cleanup(handle)
    driver.cleanup(handle)
