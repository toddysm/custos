"""Native durable attempt claims, cross-replica fencing and exact result replay."""

from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from asyncpg.pool import Pool
from custos_spl.errors import ImmutableViolation, LeaseBusy, LeaseExpired, MigrationRequired
from custos_spl.ids import ConnectorInstanceId, RunId, StepId, WorkspaceId
from custos_spl.interfaces.activity_execution import ActivityExecutionRecord
from custos_spl.interfaces.lease_store import Lease

from custos_pg.adapters.lease import PgLeaseAdapter
from custos_pg.adapters.metadata import PgMetadataAdapter

pytestmark = pytest.mark.integration
OWNER = "a" * 32
HASH = "b" * 64


def snapshot(
    *,
    workspace: str = "ws",
    run: str = "run",
    step: str = "step",
    attempt: int = 1,
    owner: str = OWNER,
) -> str:
    """Build the runtime snapshot shape without importing or replacing the production provider."""
    now = datetime.now(UTC)
    return json.dumps(
        {
            "workspace_id": workspace,
            "run_id": run,
            "step_id": step,
            "attempt": attempt,
            "request_hash": HASH,
            "owner_id": owner,
            "activity_ref": "test/echo@1.0.0",
            "started_at": now.isoformat(),
            "deadline": (now + timedelta(minutes=5)).isoformat(),
            "state": "pending",
            "execution_identity": {
                "workspaceId": workspace,
                "principalId": "executor",
                "bindingId": "original",
            },
        }
    )


async def claim(
    adapter: PgMetadataAdapter,
    *,
    workspace: str = "ws",
    run: str = "run",
    step: str = "step",
    attempt: int = 1,
    owner: str = OWNER,
) -> ActivityExecutionRecord:
    """Claim an actual adapter-owned row under exact opaque execution coordinates."""
    return await adapter.claim_activity_execution(
        workspace,
        run,
        step,
        attempt,
        HASH,
        owner,
        snapshot(workspace=workspace, run=run, step=step, attempt=attempt, owner=owner),
    )


async def test_attempt_claim_is_shared_and_never_automatically_reowned(pg_pool: Pool) -> None:
    """Two replicas share one claim; restarts and later attempts cannot relaunch pending effects."""
    first, restarted = PgMetadataAdapter(pool=pg_pool), PgMetadataAdapter(pool=pg_pool)
    with pytest.raises(MigrationRequired):
        await first.check_activity_execution_ready()
    await first.apply_pending()
    await restarted.check_activity_execution_ready()
    rows = await asyncio.gather(claim(first), claim(restarted, owner="c" * 32))
    assert rows[0] == rows[1]
    assert await restarted.read_activity_execution("ws", "run", "step", 1) == rows[0]
    with pytest.raises(LeaseBusy):
        await claim(restarted, attempt=2)
    assert await restarted.read_activity_execution("other", "run", "step", 1) is None
    with pytest.raises(ImmutableViolation):
        wire = json.loads(snapshot())
        wire["request_hash"] = "d" * 64
        await first.claim_activity_execution(
            "ws",
            "run",
            "step",
            1,
            "d" * 64,
            OWNER,
            json.dumps(wire),
        )


async def test_exact_result_survives_restart_and_terminal_state_cannot_change(
    pg_pool: Pool,
) -> None:
    """Every real CAS transition persists, and terminal output is returned losslessly."""
    adapter = PgMetadataAdapter(pool=pg_pool)
    await adapter.apply_pending()
    row = await claim(adapter)
    initial = row
    for state in ("resolving", "materializing", "running", "finalizing", "succeeded"):
        wire = json.loads(row.snapshot_json)
        wire["state"] = state
        if state == "succeeded":
            wire["finished_at"] = datetime.now(UTC).isoformat()
            wire["result"] = {
                "class_": "success",
                "attempt": 1,
                "outputs": {"nested": [1, {"ok": True}]},
                "error": None,
            }
        row = await adapter.write_activity_execution(
            row, json.dumps(wire), completed=state == "succeeded"
        )
    saved = await PgMetadataAdapter(pool=pg_pool).read_activity_execution("ws", "run", "step", 1)
    assert saved == row
    assert json.loads(row.snapshot_json)["result"]["outputs"] == {"nested": [1, {"ok": True}]}
    stale = json.loads(initial.snapshot_json)
    stale["state"] = "resolving"
    with pytest.raises(LeaseExpired):
        await adapter.write_activity_execution(initial, json.dumps(stale), completed=False)
    with pytest.raises(ValueError, match="terminal"):
        await adapter.write_activity_execution(row, row.snapshot_json, completed=True)
    with pytest.raises(LeaseBusy):
        await claim(adapter, attempt=2)


@pytest.mark.parametrize(
    "field,value",
    [
        ("owner_id", "d" * 32),
        ("request_hash", "d" * 64),
        ("workspace_id", "other"),
        ("execution_identity", {"workspaceId": "ws", "principalId": "other", "bindingId": "new"}),
        ("deadline", "2099-01-01T00:00:00+00:00"),
        ("result", {"class_": "success"}),
    ],
)
async def test_snapshot_cannot_change_authority_or_invent_completion(
    pg_pool: Pool,
    field: str,
    value: object,
) -> None:
    """Caller-supplied snapshot mutations fail before changing any durable state."""
    adapter = PgMetadataAdapter(pool=pg_pool)
    await adapter.apply_pending()
    row = await claim(adapter)
    wire = json.loads(row.snapshot_json)
    wire.update(state="resolving")
    wire[field] = value
    with pytest.raises(ValueError):
        await adapter.write_activity_execution(row, json.dumps(wire), completed=False)
    assert await adapter.read_activity_execution("ws", "run", "step", 1) == row


async def test_durable_step_fence_is_atomic_and_handles_large_opaque_ids(pg_pool: Pool) -> None:
    """Fixed-size step indexes preserve the documented identifier bound without index overflow."""
    adapter = PgMetadataAdapter(pool=pg_pool)
    await adapter.apply_pending()
    run, step = "parent/" + "x" * 4000, "step/" + "y" * 4000
    outcomes = await asyncio.gather(
        *(claim(adapter, run=run, step=step, attempt=attempt) for attempt in (1, 2)),
        return_exceptions=True,
    )
    assert sum(isinstance(row, ActivityExecutionRecord) for row in outcomes) == 1
    assert sum(isinstance(row, LeaseBusy) for row in outcomes) == 1


async def test_stale_snapshot_and_forged_owner_cannot_write(pg_pool: Pool) -> None:
    """Owner/fingerprint/prior-snapshot CAS is enforced by the actual database, not caller state."""
    adapter = PgMetadataAdapter(pool=pg_pool)
    await adapter.apply_pending()
    row = await claim(adapter)
    wire = json.loads(row.snapshot_json)
    wire["state"] = "resolving"
    await adapter.write_activity_execution(row, json.dumps(wire), completed=False)
    for candidate in (row, replace(row, owner="f" * 32)):
        candidate_wire = json.loads(candidate.snapshot_json)
        candidate_wire.update(state="resolving", owner_id=candidate.owner)
        if candidate.owner != row.owner:
            candidate = replace(
                candidate,
                snapshot_json=json.dumps(
                    {
                        **json.loads(candidate.snapshot_json),
                        "owner_id": candidate.owner,
                    }
                ),
            )
        with pytest.raises(LeaseExpired):
            await adapter.write_activity_execution(
                candidate, json.dumps(candidate_wire), completed=False
            )


async def test_retry_attempts_must_increase_after_confirmed_failure(pg_pool: Pool) -> None:
    """A completed failed attempt permits a later attempt, never an older delayed attempt."""
    adapter = PgMetadataAdapter(pool=pg_pool)
    await adapter.apply_pending()
    row = await claim(adapter, attempt=2)
    wire = json.loads(row.snapshot_json)
    wire.update(
        state="failed",
        finished_at=datetime.now(UTC).isoformat(),
        result={"class_": "retryable", "attempt": 2, "outputs": None, "error": {"code": "test"}},
    )
    await adapter.write_activity_execution(row, json.dumps(wire), completed=True)
    with pytest.raises(LeaseBusy):
        await claim(adapter, attempt=1)
    assert (await claim(adapter, attempt=3)).attempt == 3


async def test_revocation_retains_unknown_effect_and_cannot_be_undone(pg_pool: Pool) -> None:
    """Credential admission closes durably without claiming that the sandbox terminated."""
    adapter = PgMetadataAdapter(pool=pg_pool)
    await adapter.apply_pending()
    row = await claim(adapter)
    revoked = await adapter.revoke_activity_authority(row)
    assert revoked.authority_revoked and not revoked.completed
    with pytest.raises(LeaseBusy):
        await claim(adapter, attempt=2)
    wire = json.loads(row.snapshot_json)
    wire["state"] = "resolving"
    moved = await adapter.write_activity_execution(row, json.dumps(wire), completed=False)
    assert moved.authority_revoked
    assert (
        await PgMetadataAdapter(pool=pg_pool).read_activity_execution("ws", "run", "step", 1)
    ) == moved


async def test_credential_lease_capacity_is_atomic_across_adapters(pg_pool: Pool) -> None:
    """Concurrent real inserts never exceed the attempt cap; revocation releases capacity."""
    adapters = [PgLeaseAdapter(pool=pg_pool), PgLeaseAdapter(pool=pg_pool)]
    await adapters[0].apply_pending()
    now = datetime.now(UTC)

    def lease(number: int) -> Lease:
        """Use distinct leases for the same accepted attempt."""
        return Lease(
            workspace_id=WorkspaceId("ws"),
            lease_id=f"lease-{number}",
            run_id=RunId("run"),
            step_id=StepId("step"),
            attempt=1,
            slot="source",
            capability="oci.pull",
            connector_instance_id=ConnectorInstanceId("instance"),
            token_type="provider-material",
            issued_at=now,
            expires_at=now + timedelta(minutes=5),
            released_at=None,
            revoked_at=None,
            revoke_reason=None,
            created_at=now,
            updated_at=now,
        )

    results = await asyncio.gather(
        *(
            adapters[index % 2].put_lease_bounded(WorkspaceId("ws"), lease(index), 2)
            for index in range(10)
        ),
        return_exceptions=True,
    )
    admitted = [row for row in results if isinstance(row, Lease)]
    assert len(admitted) == 2
    assert sum(isinstance(row, LeaseBusy) for row in results) == 8
    await adapters[0].revoke_lease(WorkspaceId("ws"), admitted[0].lease_id, "operator", now)
    assert (
        await adapters[1].put_lease_bounded(WorkspaceId("ws"), lease(11), 2)
    ).lease_id == "lease-11"
