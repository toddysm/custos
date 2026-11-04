"""Durable attempt replay/ownership through the real repository and scheduler."""

import asyncio
import hashlib
import json
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import cast

import pytest
from custos_callctx.execution import ExecutionIdentity
from custos_spl.errors import BackendUnavailable, ImmutableViolation, LeaseBusy, LeaseExpired
from custos_spl.interfaces.activity_execution import (
    ActivityExecutionRecord,
    activity_snapshot,
    validate_activity_claim,
)
from custos_spl.interfaces.metadata_store import MetadataStoreProvider

from custos_arm.manifest import ConnectorSpec
from custos_arm.result import ResultClass
from custos_arm.runtime import CancelReason
from custos_arm.scheduler import ActivityScheduler
from custos_arm.secrets import ConnectorContext
from custos_arm.store import ExecutionRepository
from tests.test_scheduler import (
    _CancellableDriver,
    _FakeDriver,
    _FakeMetadataStore,
    _FakeResolver,
    _request,
    _resolved,
    _scheduler,
    _settings,
    _success_outputs,
    _wait_for_live_attempt,
)


class SharedAttempts:
    """Atomic external-store double shared across distinct process/repository instances."""

    def __init__(self) -> None:
        """Keep durable rows outside any repository's live/cache dictionaries."""
        self.rows: dict[tuple[str, str, str, int], ActivityExecutionRecord] = {}
        self.lock = asyncio.Lock()

    async def check_activity_execution_ready(self) -> None:
        """The explicit test capability has no external migration dependency."""

    async def claim_activity_execution(
        self,
        workspace_id: str,
        run_id: str,
        step_id: str,
        attempt: int,
        request_hash: str,
        owner: str,
        snapshot_json: str,
    ) -> ActivityExecutionRecord:
        """Match provider claim, step fence and immutable request semantics atomically."""
        validate_activity_claim(workspace_id, run_id, step_id, attempt, request_hash, owner)
        key = (workspace_id, run_id, step_id, attempt)
        async with self.lock:
            if key in self.rows:
                row = self.rows[key]
                if row.request_hash != request_hash:
                    raise ImmutableViolation("different request")
                return row
            for other, row in self.rows.items():
                if other[:3] == key[:3] and (
                    not row.completed
                    or json.loads(row.snapshot_json)["state"] == "succeeded"
                    or row.attempt >= attempt
                ):
                    raise LeaseBusy("unresolved or successful earlier attempt")
            row = ActivityExecutionRecord(
                *key, request_hash, owner, activity_snapshot(snapshot_json), False
            )
            self.rows[key] = row
            return row

    async def read_activity_execution(
        self, workspace_id: str, run_id: str, step_id: str, attempt: int
    ) -> ActivityExecutionRecord | None:
        """Return the retained row, never the caller's in-memory execution state."""
        return self.rows.get((workspace_id, run_id, step_id, attempt))

    async def write_activity_execution(
        self, record: ActivityExecutionRecord, snapshot_json: str, *, completed: bool
    ) -> ActivityExecutionRecord:
        """Reject stale owners/snapshots and any overwrite of a terminal result."""
        key = (record.workspace_id, record.run_id, record.step_id, record.attempt)
        async with self.lock:
            if self.rows.get(key) != record or record.completed:
                raise LeaseExpired("stale owner or state")
            row = replace(
                record, snapshot_json=activity_snapshot(snapshot_json), completed=completed
            )
            self.rows[key] = row
            return row

    async def revoke_activity_authority(
        self, record: ActivityExecutionRecord
    ) -> ActivityExecutionRecord:
        """Irreversibly revoke private credential admission while retaining pending execution."""
        key = (record.workspace_id, record.run_id, record.step_id, record.attempt)
        row = self.rows[key]
        if row.owner != record.owner:
            raise LeaseExpired("different owner")
        updated = replace(row, authority_revoked=True)
        self.rows[key] = updated
        return updated


def repository(store: SharedAttempts) -> ExecutionRepository:
    """Create an independent runtime process over shared durable rows."""
    return ExecutionRepository(
        cast(MetadataStoreProvider, _FakeMetadataStore()),
        idempotency_ttl=timedelta(days=31),
        durable_store=store,
    )


async def test_restart_replays_exact_outputs_without_second_execution(tmp_path: Path) -> None:
    """A fresh repository/scheduler returns the persisted result, not an empty success."""
    store = SharedAttempts()
    request = _request()
    first = await _scheduler(
        _FakeDriver(tmp_path / "first", outputs=_success_outputs({"nested": {"answer": 42}})),
        repo=repository(store),
    ).schedule(request)
    assert first.class_ is ResultClass.SUCCESS
    driver = _FakeDriver(tmp_path / "restart", outputs=None)
    replay = await _scheduler(driver, repo=repository(store)).schedule(request)
    assert replay == first and replay.outputs == {"nested": {"answer": 42}}
    assert driver.prepared == 0


async def test_pending_owner_blocks_restart_and_later_attempt(tmp_path: Path) -> None:
    """A process crash or transport timeout cannot turn pending ownership into a new effect."""
    from custos_arm.store import ActivityExecution
    from tests.test_scheduler import _NOW

    store = SharedAttempts()
    request = _request()
    first = repository(store)
    await first.insert(
        ActivityExecution(
            workspace_id=request.workspace_id,
            run_id=request.step.run_id,
            step_id=request.step.step_id,
            attempt=1,
            activity_ref=request.activity_ref,
            deadline=_NOW + timedelta(hours=1),
            started_at=_NOW,
            request_hash=ActivityScheduler._request_hash(request),
        )
    )
    for candidate in (
        request,
        replace(request, step=request.step.model_copy(update={"attempt": 2})),
    ):
        driver = _FakeDriver(tmp_path / str(candidate.step.attempt), outputs=None)
        result = await _scheduler(driver, repo=repository(store)).schedule(candidate)
        assert result.class_ is ResultClass.PERMANENT
        assert result.error is not None and result.error.code == "activity.execution_unconfirmed"
        assert driver.prepared == 0
    assert len(store.rows) == 1 and not next(iter(store.rows.values())).completed


async def test_replay_cannot_change_inputs_or_caller(tmp_path: Path) -> None:
    """Neither stale credentials nor a changed request can acquire a prior result."""
    store = SharedAttempts()
    request = _request(principal_id="original")
    await _scheduler(
        _FakeDriver(tmp_path / "first", outputs=_success_outputs()), repo=repository(store)
    ).schedule(request)
    for changed in (
        replace(request, inputs={"different": True}),
        replace(request, principal_id="other"),
    ):
        driver = _FakeDriver(tmp_path / "changed", outputs=None)
        result = await _scheduler(driver, repo=repository(store)).schedule(changed)
        assert result.class_ is ResultClass.PERMANENT and driver.prepared == 0


async def test_terminal_snapshot_is_immutable(tmp_path: Path) -> None:
    """Even the original owner cannot replace an acknowledged terminal result."""
    store = SharedAttempts()
    await _scheduler(
        _FakeDriver(tmp_path, outputs=_success_outputs()), repo=repository(store)
    ).schedule(_request())
    row = next(iter(store.rows.values()))
    with pytest.raises(LeaseExpired):
        await store.write_activity_execution(row, row.snapshot_json, completed=True)


def test_fingerprint_covers_binding_authority_and_original_identity() -> None:
    """Opaque contexts, capabilities and credential references cannot change during replay."""
    identity = ExecutionIdentity("ws-1", "account", "stable")
    context = ConnectorContext(
        slot_name="source",
        connector_type="registry",
        connector_instance_id="instance",
        binding={"handle": {"opaque": "one"}},
        capabilities=("oci.pull",),
    )
    request = replace(_request(), execution_identity=identity, connector_contexts=(context,))
    original = ActivityScheduler._request_hash(request)
    for changed in (
        replace(request, execution_identity=replace(identity, binding_id="replacement")),
        replace(request, connector_contexts=(replace(context, capabilities=("oci.push",)),)),
        replace(
            request, connector_contexts=(replace(context, binding={"handle": {"opaque": "two"}}),)
        ),
    ):
        assert ActivityScheduler._request_hash(changed) != original


async def test_disconnect_and_drain_keep_unknown_effect_fenced(tmp_path: Path) -> None:
    """Request cancellation retains the live worker and unknown-effect fence."""
    store = SharedAttempts()
    driver = _CancellableDriver(tmp_path)
    scheduler = _scheduler(driver, repo=repository(store))
    caller = asyncio.create_task(scheduler.schedule(_request()))
    await _wait_for_live_attempt(scheduler)
    caller.cancel()
    with pytest.raises(asyncio.CancelledError):
        await caller
    assert scheduler._workers
    await asyncio.wait_for(scheduler.drain(), timeout=5)
    assert not scheduler._workers
    assert driver.cancellations == [CancelReason.SHUTDOWN]
    assert not next(iter(store.rows.values())).completed
    assert next(iter(store.rows.values())).authority_revoked
    refusal = await scheduler.schedule(_request())
    assert refusal.class_ is ResultClass.RETRYABLE
    assert driver.prepared == 1


async def test_drain_retries_revocation_and_joins_even_when_cancelled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A failed revocation cannot close resources or stop the sandbox before authority is closed."""
    store = SharedAttempts()
    driver = _CancellableDriver(tmp_path)
    scheduler = _scheduler(driver, repo=repository(store))
    caller = asyncio.create_task(scheduler.schedule(_request()))
    await _wait_for_live_attempt(scheduler)
    attempted, recover = asyncio.Event(), asyncio.Event()
    revoke = store.revoke_activity_authority

    async def unavailable(record: ActivityExecutionRecord) -> ActivityExecutionRecord:
        """Hold the store unavailable until the test verifies the stop/join boundary."""
        attempted.set()
        if not recover.is_set():
            raise BackendUnavailable("private-backend-diagnostic")
        return await revoke(record)

    monkeypatch.setattr(store, "revoke_activity_authority", unavailable)
    draining = asyncio.create_task(scheduler.drain())
    await asyncio.wait_for(attempted.wait(), timeout=1)
    draining.cancel()
    await asyncio.sleep(0.02)
    assert not draining.done() and scheduler._workers and not driver.cancellations
    recover.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(draining, timeout=5)
    await caller
    assert not scheduler._workers
    assert driver.cancellations == [CancelReason.SHUTDOWN]
    assert next(iter(store.rows.values())).authority_revoked
    assert "private-backend-diagnostic" not in caplog.text


@pytest.mark.parametrize("with_slot", [False, True])
async def test_private_broker_bundle_is_separate_from_activity_inputs(
    tmp_path: Path,
    with_slot: bool,
) -> None:
    """Only proof hashes and stable references persist; the pod plan contains no raw proof."""
    store = SharedAttempts()
    identity = ExecutionIdentity("ws-1", "executor", "stable")
    driver = _FakeDriver(tmp_path, outputs=_success_outputs())
    settings = replace(_settings(), credential_proxy_url="https://runtime.internal")
    resolved = _resolved()
    contexts: tuple[ConnectorContext, ...] = ()
    if with_slot:
        requirement = ConnectorSpec(
            name="source", type="registry", required=True, capabilities=["oci.pull"]
        )
        manifest = resolved.manifest.model_copy(
            update={
                "spec": resolved.manifest.spec.model_copy(update={"connectors": [requirement]}),
            }
        )
        resolved = resolved.model_copy(update={"manifest": manifest})
        contexts = (
            ConnectorContext(
                slot_name="source",
                connector_type="registry",
                connector_instance_id="instance",
                capabilities=("oci.pull",),
                binding={
                    "endpoint": "https://registry",
                    "tokenTypeHint": None,
                    "handle": {"opaque": "reference"},
                    "extras": {},
                },
            ),
        )
    result = await _scheduler(
        driver,
        repo=repository(store),
        settings=settings,
        resolver=_FakeResolver(resolved),
    ).schedule(
        _request(principal_id="executor", execution_identity=identity, connector_contexts=contexts),
    )
    assert result.class_ is ResultClass.SUCCESS
    assert driver.handle is not None and driver.last_plan is not None
    bundle_path = driver.handle.sidecar_root / "runtime.json"
    bundle = json.loads(bundle_path.read_bytes())
    assert bundle_path.stat().st_mode & 0o777 == 0o400
    assert not (driver.handle.input_root / "runtime.json").exists()
    row = next(iter(store.rows.values()))
    assert bundle["proof"] not in row.snapshot_json
    assert json.loads(row.snapshot_json)["execution_identity"] == identity.to_dict()
    assert (
        json.loads(row.snapshot_json)["credential_grant_hash"]
        == hashlib.sha256(bundle["proof"].encode()).hexdigest()
    )
    assert json.loads(row.snapshot_json)["credential_scopes"] == (
        {"source": {"instanceId": "instance", "kind": "registry", "capabilities": ["oci.pull"]}}
        if with_slot
        else {}
    )
    assert driver.last_plan.sidecar.credential_broker
    assert any(mount.mount_path == "/custos/run" for mount in driver.last_plan.tmpfs_mounts)
