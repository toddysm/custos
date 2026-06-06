"""End-to-end Scheduler integration tests against a real ``kind`` cluster (ARM-IMPL-021).

These wire the **real** :class:`~custos_arm.scheduler.ActivityScheduler` to the
**real** :class:`~custos_arm.runtime.oci.OciContainerDriver` and drive whole
attempts against a live Kubernetes cluster — the Scheduler resolves, applies
limits, builds the plan, dispatches the driver (``prepare`` → ``start`` →
``await_terminal`` → ``collect`` → ``cleanup``), finalizes, classifies, and
persists. Only the Catalog resolver, metadata store, and artifact store are
in-memory stand-ins (there is no Catalog/SPL backend on ``kind``); everything
else is production code.

They run only in the dedicated ``activity-runtime-manager-integration`` CI job
(``integration``-marked, excluded from the default unit run) and reuse the tiny
non-root ``CUSTOS_ARM_E2E_IMAGE`` the job ``kind load``s.

**Scope (ARM-IMPL-021, Option A).** The scenarios exercised here are the ones
the current driver supports against a registry-less ``kind`` cluster:

* image-pull failure classification (retryable) — a true Scheduler+driver+kind
  round-trip through ``synthesize_failure``;
* idempotent replay — a second ``schedule`` for the same key returns the cached
  envelope and stands up no second sandbox;
* deadline/timeout — the driver self-cancels the in-flight attempt and the
  Scheduler classifies it ``cancelled`` / ``activity.timeout``.

A genuine output round-trip (happy-path outputs and downstream ``ArtifactRef``
materialization) needs two pieces of plumbing that do not exist yet: the
ARM↔pod I/O bridge (the pod writes ``/custos/out`` into a per-pod ``emptyDir``
that ARM's host-local staging tree never sees) and a way to pin a locally
``kind load``ed image by digest (the Scheduler always digest-pins, which a
registry-less image cannot satisfy). Those are tracked as a follow-up and are
intentionally out of scope here.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from custos_spl.errors import ImmutableViolation
from custos_spl.ids import RunId, StepId, WorkspaceId
from custos_spl.interfaces.metadata_store import StepAttempt

from custos_arm.config import Settings, load_settings
from custos_arm.contract import StepRef
from custos_arm.io import IOBroker
from custos_arm.limit import ResourceLimiter
from custos_arm.manifest import parse_manifest
from custos_arm.resolve import ActivityTypeVersion
from custos_arm.result import ResultClass, ResultMapper
from custos_arm.runtime import RuntimeDriverDispatcher, SandboxHandle, SandboxPlan
from custos_arm.runtime.oci import OciContainerDriver
from custos_arm.scheduler import ActivityScheduler, ScheduleRequest
from custos_arm.secrets import SecretInjector, SidecarTokenMinter
from custos_arm.store import ExecutionRepository, ExecutionState

pytestmark = pytest.mark.integration

_NAMESPACE = "custos-arm-e2e"
_E2E_IMAGE = os.environ.get("CUSTOS_ARM_E2E_IMAGE", "custos-arm-e2e:test")
_INVALID_IMAGE = "registry.invalid.example/nope:absent"
_DIGEST = "sha256:" + "ab" * 32


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


# ---------------------------------------------------------------------------
# In-memory stand-ins (no Catalog / SPL backend on kind) + a counting driver.
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


class _FakeResolver:
    """Returns a pre-built type version (no Catalog on kind)."""

    def __init__(self, resolved: ActivityTypeVersion) -> None:
        self._resolved = resolved
        self.calls = 0

    async def resolve(self, *, workspace_id: str, activity_ref: str) -> ActivityTypeVersion:
        self.calls += 1
        return self._resolved


class _CountingDriver(OciContainerDriver):
    """The real OCI driver, counting how many sandboxes it stood up."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.prepared = 0

    def prepare(self, plan: SandboxPlan) -> SandboxHandle:
        self.prepared += 1
        return super().prepare(plan)


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


def _settings() -> Settings:
    return load_settings(
        {
            "ARM_ARTIFACT_STORE": "artifacts",
            "ARM_METADATA_STORE": "metadata",
            "ARM_CATALOG_ENDPOINT": "http://catalog.svc:8080",
            "ARM_CONNECTOR_ENDPOINT": "http://connector.svc:8080",
            "ARM_SANDBOX_NAMESPACE": _NAMESPACE,
            "ARM_SIDECAR_IMAGE": _E2E_IMAGE,
            # Reuse the kind-loaded image for the io-bridge helpers so the
            # init containers never trigger an external pull (the default is a
            # Docker Hub busybox digest that is not loaded into the cluster).
            "ARM_IO_BRIDGE_IMAGE": _E2E_IMAGE,
            "ENVIRONMENT": "development",
        }
    )


def _resolved(*, image: str, digest: str = _DIGEST) -> ActivityTypeVersion:
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
            "runtime": {"kind": "oci-container", "image": image, "digest": digest},
            "inputs": {"schema": {"type": "object"}},
            "outputs": {"schema": {"type": "object"}},
            "resources": {
                "cpu": {"request": "50m", "limit": "200m"},
                "memory": {"request": "32Mi", "limit": "64Mi"},
                "ephemeralStorage": {"limit": "64Mi"},
                "timeout": "PT2M",
            },
        },
    }
    return ActivityTypeVersion(
        namespace="acme",
        type="echo",
        version="1.0.0",
        digest=digest,
        manifest=parse_manifest(raw),
    )


def _scheduler(
    driver: OciContainerDriver,
    resolved: ActivityTypeVersion,
    *,
    repo: ExecutionRepository | None = None,
) -> ActivityScheduler:
    settings = _settings()
    return ActivityScheduler(
        resolver=_FakeResolver(resolved),
        limiter=ResourceLimiter(settings),
        broker=IOBroker(_StubArtifactStore(), output_max_bytes=1_000_000),  # type: ignore[arg-type]
        injector=SecretInjector(token_minter=SidecarTokenMinter()),
        mapper=ResultMapper(),
        dispatcher=RuntimeDriverDispatcher((driver,)),
        repository=repo
        or ExecutionRepository(_FakeMetadataStore(), idempotency_ttl=timedelta(hours=24)),  # type: ignore[arg-type]
        settings=settings,
    )


def _driver(apis: tuple[object, object], staging_root: Path) -> _CountingDriver:
    batch, core = apis
    return _CountingDriver(
        batch_api=batch,
        core_api=core,
        staging_root=staging_root,
        poll_interval=1.0,
    )


def _request(**overrides: Any) -> ScheduleRequest:
    base: dict[str, Any] = {
        "workspace_id": "ws-1",
        "step": StepRef(runId=f"run-{uuid.uuid4().hex[:8]}", stepId="step-a", attempt=1),
        "activity_ref": "acme/echo@1.0.0",
        "inputs": {"message": "hi"},
    }
    base.update(overrides)
    return ScheduleRequest(**base)


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------


async def test_image_pull_failure_is_classified_retryable(
    _apis: tuple[object, object], tmp_path: Path
) -> None:
    driver = _driver(_apis, tmp_path)
    resolved = _resolved(image=_INVALID_IMAGE)
    repo = ExecutionRepository(_FakeMetadataStore(), idempotency_ttl=timedelta(hours=24))  # type: ignore[arg-type]
    scheduler = _scheduler(driver, resolved, repo=repo)
    request = _request()

    result = await scheduler.schedule(request)

    assert result.class_ is ResultClass.RETRYABLE
    assert result.error is not None
    assert result.error.code == "activity.image_pull_failed"

    record = await repo.get(
        request.workspace_id, request.step.run_id, request.step.step_id, request.step.attempt
    )
    assert record is not None
    assert record.state is ExecutionState.FAILED
    assert driver.prepared == 1


async def test_idempotent_replay_runs_a_single_sandbox(
    _apis: tuple[object, object], tmp_path: Path
) -> None:
    driver = _driver(_apis, tmp_path)
    resolved = _resolved(image=_INVALID_IMAGE)
    repo = ExecutionRepository(_FakeMetadataStore(), idempotency_ttl=timedelta(hours=24))  # type: ignore[arg-type]
    scheduler = _scheduler(driver, resolved, repo=repo)
    request = _request()

    first = await scheduler.schedule(request)
    second = await scheduler.schedule(request)

    assert second is first  # the cached terminal envelope, not a fresh attempt
    assert driver.prepared == 1  # no second sandbox stood up


async def test_deadline_exceeded_yields_timeout_cancelled(
    _apis: tuple[object, object], tmp_path: Path
) -> None:
    driver = _driver(_apis, tmp_path)
    resolved = _resolved(image=_E2E_IMAGE)
    repo = ExecutionRepository(_FakeMetadataStore(), idempotency_ttl=timedelta(hours=24))  # type: ignore[arg-type]
    scheduler = _scheduler(driver, resolved, repo=repo)
    # A step deadline already in the past forces the driver to self-cancel the
    # in-flight attempt on its first poll, before the pod can terminate.
    request = _request(step_deadline=datetime.now(UTC) - timedelta(seconds=30))

    result = await scheduler.schedule(request)

    assert result.class_ is ResultClass.CANCELLED
    assert result.error is not None
    assert result.error.code == "activity.timeout"

    record = await repo.get(
        request.workspace_id, request.step.run_id, request.step.step_id, request.step.attempt
    )
    assert record is not None
    assert record.state is ExecutionState.CANCELLED
