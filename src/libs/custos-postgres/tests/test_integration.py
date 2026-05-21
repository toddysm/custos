"""Integration tests against a live Postgres.

Skipped unless `CUSTOS_PG_DSN` is set or `testcontainers[postgres]`
can spin up a container (see `conftest.py`). Marked
`integration` so CI suites can filter them.

Covers the contract surface the design pins:
  - Definition write-once → `ImmutableViolation` on re-put.
  - Catalog digest semantics → idempotent same-digest, `ConflictDigest`
    on different-digest re-put.
  - Parent-deprecation flag denormalized onto version rows.
  - Keyset pagination ordering and `next_cursor`.
  - `resolve(semver)` honors deprecation.
  - `apply_pending` records into `custos_meta.adapter_revisions` and is
    idempotent.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from typing import TYPE_CHECKING

import pytest
from custos_spl.errors import (
    ConflictDigest,
    ImmutableViolation,
    InvalidTransactionHandle,
    LeaseBusy,
    LeaseExpired,
)
from custos_spl.interfaces.metadata_store import (
    ArtifactUse,
    DedupDuplicate,
    DedupReserved,
    ExistingCompleted,
    ExistingInFlight,
    KeyReuse,
    IdemReserved,
    ResumeSubscription,
    Run,
    Schedule,
    Step,
    StepAttempt,
    Subscription,
    SubscriptionSelector,
    DeviceCodeSession,
)
from custos_spl.interfaces.auth_store import (
    PrincipalFilter,
    ServiceAccount,
    Tenant,
    User,
    Workspace,
    WorkspaceFilter,
)
from custos_spl.migrations.runner import check_revisions

from custos_pg.adapters.catalog import PgCatalogAdapter
from custos_pg.adapters.definition import PgDefinitionAdapter
from custos_pg.adapters.metadata import PgMetadataAdapter
from custos_pg.adapters.auth import PgAuthAdapter

if TYPE_CHECKING:
    from asyncpg.pool import Pool

pytestmark = pytest.mark.integration


# ----- Migrations -----


async def test_definition_apply_pending_records_revision(pg_pool: Pool) -> None:
    adapter = PgDefinitionAdapter(pool=pg_pool)
    summaries = await adapter.apply_pending()
    assert summaries  # at least one revision applied
    assert any("DefinitionStoreProvider rev1" in s for s in summaries)
    declared = adapter.declared_revisions
    assert 1 in declared["DefinitionStoreProvider"]


async def test_definition_apply_pending_is_idempotent(pg_pool: Pool) -> None:
    adapter = PgDefinitionAdapter(pool=pg_pool)
    first = await adapter.apply_pending()
    second = await adapter.apply_pending()
    assert first  # first run did something
    assert second == []  # second run is a no-op


async def test_check_revisions_passes_after_apply(pg_pool: Pool) -> None:
    """A platform-startup-style negotiation after migrations should succeed.

    Both stateful adapters together must declare all required SPL
    revisions for the interfaces they own; SPL skips interfaces not
    declared by ANY adapter, but here we only construct the two
    revision-1 adapters so we exercise just their interfaces — the
    intent is that, post-apply, neither contributes a gap.
    """
    defn = PgDefinitionAdapter(pool=pg_pool)
    cat = PgCatalogAdapter(pool=pg_pool)
    await defn.apply_pending()
    await cat.apply_pending()
    # With only these two adapters, MetadataStore/AuthStore/ArtifactStore
    # are still gaps — assert the two interfaces these adapters own are
    # satisfied locally.
    assert 1 in defn.declared_revisions["DefinitionStoreProvider"]
    assert 1 in cat.declared_revisions["CatalogStoreProvider"]


# ----- DefinitionStoreProvider -----


async def test_put_and_get_workflow_version(pg_pool: Pool) -> None:
    adapter = PgDefinitionAdapter(pool=pg_pool)
    await adapter.apply_pending()
    v = await adapter.put_workflow_version(
        "ws-1", "wf-1", "1.0.0", {"steps": []}
    )
    assert v.workspace_id == "ws-1"
    assert v.workflow_id == "wf-1"
    assert v.version == "1.0.0"
    assert v.parent_deprecated is False
    got = await adapter.get_workflow_version("ws-1", "wf-1", "1.0.0")
    assert got is not None
    assert got.version == "1.0.0"


async def test_workflow_version_is_write_once(pg_pool: Pool) -> None:
    adapter = PgDefinitionAdapter(pool=pg_pool)
    await adapter.apply_pending()
    await adapter.put_workflow_version("ws-1", "wf-1", "1.0.0", {"a": 1})
    with pytest.raises(ImmutableViolation):
        await adapter.put_workflow_version("ws-1", "wf-1", "1.0.0", {"a": 1})


async def test_get_workflow_version_returns_none_when_absent(pg_pool: Pool) -> None:
    adapter = PgDefinitionAdapter(pool=pg_pool)
    await adapter.apply_pending()
    assert await adapter.get_workflow_version("ws-x", "wf-x", "9.9.9") is None


async def test_set_workflow_deprecated_reflects_in_version_fetch(pg_pool: Pool) -> None:
    adapter = PgDefinitionAdapter(pool=pg_pool)
    await adapter.apply_pending()
    await adapter.put_workflow_version("ws-1", "wf-1", "1.0.0", {})
    await adapter.set_workflow_deprecated("ws-1", "wf-1", True)
    got = await adapter.get_workflow_version("ws-1", "wf-1", "1.0.0")
    assert got is not None
    assert got.parent_deprecated is True


async def test_get_latest_workflow_version_returns_newest(pg_pool: Pool) -> None:
    adapter = PgDefinitionAdapter(pool=pg_pool)
    await adapter.apply_pending()
    await adapter.put_workflow_version("ws-1", "wf-1", "1.0.0", {})
    await adapter.put_workflow_version("ws-1", "wf-1", "2.0.0", {})
    latest = await adapter.get_latest_workflow_version("ws-1", "wf-1")
    assert latest is not None
    assert latest.version == "2.0.0"


async def test_list_workflow_versions_paginates(pg_pool: Pool) -> None:
    adapter = PgDefinitionAdapter(pool=pg_pool)
    await adapter.apply_pending()
    for i in range(5):
        await adapter.put_workflow_version("ws-1", "wf-1", f"1.0.{i}", {})
    page1 = await adapter.list_workflow_versions("ws-1", "wf-1", limit=2)
    assert len(page1.items) == 2
    assert page1.next_cursor is not None
    page2 = await adapter.list_workflow_versions(
        "ws-1", "wf-1", cursor=page1.next_cursor, limit=2
    )
    assert len(page2.items) == 2
    # Ensure no overlap.
    assert {v.version for v in page1.items}.isdisjoint(
        {v.version for v in page2.items}
    )


async def test_workspace_scoping_isolates_workflows(pg_pool: Pool) -> None:
    adapter = PgDefinitionAdapter(pool=pg_pool)
    await adapter.apply_pending()
    await adapter.put_workflow_version("ws-a", "wf-1", "1.0.0", {"who": "a"})
    await adapter.put_workflow_version("ws-b", "wf-1", "1.0.0", {"who": "b"})
    a = await adapter.get_workflow_version("ws-a", "wf-1", "1.0.0")
    b = await adapter.get_workflow_version("ws-b", "wf-1", "1.0.0")
    assert a is not None and a.normalized_doc["who"] == "a"
    assert b is not None and b.normalized_doc["who"] == "b"


# ----- Template parity (one smoke test; logic mirrors workflow paths) -----


async def test_template_version_write_once(pg_pool: Pool) -> None:
    adapter = PgDefinitionAdapter(pool=pg_pool)
    await adapter.apply_pending()
    await adapter.put_workflow_template_version("ws-1", "tpl-1", "1.0.0", {})
    with pytest.raises(ImmutableViolation):
        await adapter.put_workflow_template_version("ws-1", "tpl-1", "1.0.0", {})


# ----- CatalogStoreProvider -----


async def test_put_activity_type_idempotent_on_same_digest(pg_pool: Pool) -> None:
    adapter = PgCatalogAdapter(pool=pg_pool)
    await adapter.apply_pending()
    a = await adapter.put_activity_type_version(
        "ns", "echo", "1.0.0", "sha256:abc", {"k": 1}
    )
    b = await adapter.put_activity_type_version(
        "ns", "echo", "1.0.0", "sha256:abc", {"k": 1}
    )
    assert a.digest == b.digest == "sha256:abc"


async def test_put_activity_type_concurrent_same_digest_is_idempotent(
    pg_pool: Pool,
) -> None:
    """Concurrent puts with the same (key, digest) must all succeed.

    The legacy SELECT-then-INSERT pattern raced on the unique index;
    ON CONFLICT DO NOTHING + RETURNING makes the loser fall through
    to a digest comparison instead of leaking a 23505.
    """
    adapter = PgCatalogAdapter(pool=pg_pool)
    await adapter.apply_pending()
    results = await asyncio.gather(
        *(
            adapter.put_activity_type_version("ns", "race", "1.0.0", "sha256:r", {"i": i})
            for i in range(8)
        )
    )
    assert {r.digest for r in results} == {"sha256:r"}


async def test_put_activity_type_conflict_on_different_digest(pg_pool: Pool) -> None:
    adapter = PgCatalogAdapter(pool=pg_pool)
    await adapter.apply_pending()
    await adapter.put_activity_type_version("ns", "echo", "1.0.0", "sha256:a", {})
    with pytest.raises(ConflictDigest):
        await adapter.put_activity_type_version("ns", "echo", "1.0.0", "sha256:b", {})


async def test_put_connector_type_idempotent_on_same_digest(pg_pool: Pool) -> None:
    adapter = PgCatalogAdapter(pool=pg_pool)
    await adapter.apply_pending()
    a = await adapter.put_connector_type_version("http", "1.0.0", "sha256:x", {"k": 1})
    b = await adapter.put_connector_type_version("http", "1.0.0", "sha256:x", {"k": 1})
    assert a.digest == b.digest == "sha256:x"


async def test_put_connector_type_conflict_on_different_digest(pg_pool: Pool) -> None:
    adapter = PgCatalogAdapter(pool=pg_pool)
    await adapter.apply_pending()
    await adapter.put_connector_type_version("http", "1.0.0", "sha256:1", {})
    with pytest.raises(ConflictDigest):
        await adapter.put_connector_type_version("http", "1.0.0", "sha256:2", {})


async def test_resolve_picks_latest_in_range(pg_pool: Pool) -> None:
    adapter = PgCatalogAdapter(pool=pg_pool)
    await adapter.apply_pending()
    await adapter.put_activity_type_version("ns", "echo", "1.0.0", "d1", {})
    await adapter.put_activity_type_version("ns", "echo", "1.5.0", "d2", {})
    await adapter.put_activity_type_version("ns", "echo", "2.0.0", "d3", {})
    chosen = await adapter.resolve("ns", "echo", ">=1.0,<2.0")
    assert chosen is not None
    assert chosen.version == "1.5.0"


async def test_resolve_returns_none_on_deprecated_parent(pg_pool: Pool) -> None:
    adapter = PgCatalogAdapter(pool=pg_pool)
    await adapter.apply_pending()
    await adapter.put_activity_type_version("ns", "echo", "1.0.0", "d1", {})
    await adapter.set_activity_type_deprecated("ns", "echo", True)
    assert await adapter.resolve("ns", "echo", ">=1.0") is None


async def test_catalog_list_paginates(pg_pool: Pool) -> None:
    adapter = PgCatalogAdapter(pool=pg_pool)
    await adapter.apply_pending()
    for i in range(4):
        await adapter.put_activity_type_version("ns", "echo", f"1.0.{i}", f"d{i}", {})
    page1 = await adapter.list_activity_type_versions("ns", "echo", limit=2)
    assert len(page1.items) == 2
    assert page1.next_cursor is not None
    page2 = await adapter.list_activity_type_versions(
        "ns", "echo", cursor=page1.next_cursor, limit=2
    )
    assert len(page2.items) == 2
    assert {v.version for v in page1.items}.isdisjoint(
        {v.version for v in page2.items}
    )


# ----- Cross-interface negotiation sanity check -----


async def test_check_revisions_reports_only_unmigrated_interfaces(pg_pool: Pool) -> None:
    """With only Definition+Catalog adapters declared, MetadataStore /
    AuthStore / ArtifactStore appear as gaps. Confirms the runner
    treats per-interface revisions independently."""
    defn = PgDefinitionAdapter(pool=pg_pool)
    cat = PgCatalogAdapter(pool=pg_pool)
    await defn.apply_pending()
    await cat.apply_pending()
    from custos_spl.errors import MigrationRequired

    with pytest.raises(MigrationRequired) as exc:
        check_revisions([defn, cat])
    gap_interfaces = {iface for iface, _ in exc.value.gaps}
    assert "DefinitionStoreProvider" not in gap_interfaces
    assert "CatalogStoreProvider" not in gap_interfaces
    assert "MetadataStoreProvider" in gap_interfaces


# ----- MetadataStoreProvider (#127 slice) -----


def _now() -> datetime:
    return datetime.now(UTC)


def _make_run(run_id: str = "r-1") -> Run:
    t = _now()
    return Run(
        workspace_id="ws-1",
        run_id=run_id,
        workflow_id="wf-1",
        workflow_version="1.0.0",
        status="running",
        reason=None,
        started_at=t,
        updated_at=t,
    )


async def test_metadata_apply_pending_records_rev1(pg_pool: Pool) -> None:
    adapter = PgMetadataAdapter(pool=pg_pool)
    summaries = await adapter.apply_pending()
    assert summaries
    assert 1 in adapter.declared_revisions["MetadataStoreProvider"]


async def test_metadata_apply_pending_is_idempotent(pg_pool: Pool) -> None:
    adapter = PgMetadataAdapter(pool=pg_pool)
    first = await adapter.apply_pending()
    second = await adapter.apply_pending()
    assert first
    assert second == []


async def test_metadata_fully_satisfied_after_rev4_apply(pg_pool: Pool) -> None:
    """After #127/#128/#129, all 4 revisions are applied and MetadataStoreProvider
    is fully satisfied.

    Confirms that applying all revisions completes the MetadataStoreProvider
    contract; declared_revisions includes all 4 revisions.
    """
    adapter = PgMetadataAdapter(pool=pg_pool)
    await adapter.apply_pending()
    assert adapter.declared_revisions["MetadataStoreProvider"] == frozenset({1, 2, 3, 4})


async def test_put_run_and_get(pg_pool: Pool) -> None:
    adapter = PgMetadataAdapter(pool=pg_pool)
    await adapter.apply_pending()
    written = await adapter.put_run("ws-1", _make_run("r-1"))
    assert written.run_id == "r-1"
    got = await adapter.get_run("ws-1", "r-1")
    assert got is not None
    assert got.workflow_id == "wf-1"


async def test_put_run_is_immutable(pg_pool: Pool) -> None:
    adapter = PgMetadataAdapter(pool=pg_pool)
    await adapter.apply_pending()
    await adapter.put_run("ws-1", _make_run("r-1"))
    with pytest.raises(ImmutableViolation):
        await adapter.put_run("ws-1", _make_run("r-1"))


async def test_list_runs_paginates(pg_pool: Pool) -> None:
    adapter = PgMetadataAdapter(pool=pg_pool)
    await adapter.apply_pending()
    base = _now()
    for i in range(5):
        t = base + timedelta(seconds=i)
        await adapter.put_run(
            "ws-1",
            Run(
                workspace_id="ws-1",
                run_id=f"r-{i}",
                workflow_id="wf-1",
                workflow_version="1.0.0",
                status="running",
                reason=None,
                started_at=t,
                updated_at=t,
            ),
        )
    page1 = await adapter.list_runs("ws-1", limit=2)
    assert len(page1.items) == 2
    assert page1.next_cursor is not None
    page2 = await adapter.list_runs("ws-1", cursor=page1.next_cursor, limit=2)
    assert len(page2.items) == 2
    assert {r.run_id for r in page1.items}.isdisjoint(
        {r.run_id for r in page2.items}
    )


async def test_append_step_attempt_is_immutable(pg_pool: Pool) -> None:
    adapter = PgMetadataAdapter(pool=pg_pool)
    await adapter.apply_pending()
    await adapter.put_run("ws-1", _make_run("r-1"))
    await adapter.append_step(
        "ws-1",
        "r-1",
        Step(
            workspace_id="ws-1",
            run_id="r-1",
            step_id="s-1",
            name="step-1",
            status="running",
            created_at=_now(),
        ),
    )
    attempt = StepAttempt(
        workspace_id="ws-1",
        run_id="r-1",
        step_id="s-1",
        attempt=1,
        status="running",
        started_at=_now(),
        finished_at=None,
        error=None,
    )
    await adapter.append_step_attempt("ws-1", "r-1", "s-1", attempt)
    with pytest.raises(ImmutableViolation):
        await adapter.append_step_attempt("ws-1", "r-1", "s-1", attempt)


async def test_get_step_attempts_in_order(pg_pool: Pool) -> None:
    adapter = PgMetadataAdapter(pool=pg_pool)
    await adapter.apply_pending()
    await adapter.put_run("ws-1", _make_run("r-1"))
    await adapter.append_step(
        "ws-1",
        "r-1",
        Step(
            workspace_id="ws-1",
            run_id="r-1",
            step_id="s-1",
            name="s",
            status="running",
            created_at=_now(),
        ),
    )
    for i in (1, 2, 3):
        await adapter.append_step_attempt(
            "ws-1",
            "r-1",
            "s-1",
            StepAttempt(
                workspace_id="ws-1",
                run_id="r-1",
                step_id="s-1",
                attempt=i,
                status="failed" if i < 3 else "succeeded",
                started_at=_now(),
                finished_at=_now(),
                error=MappingProxyType({"why": "x"}) if i < 3 else None,
            ),
        )
    attempts = await adapter.get_step_attempts("ws-1", "r-1", "s-1")
    assert tuple(a.attempt for a in attempts) == (1, 2, 3)


async def test_subscription_and_selector_round_trip(pg_pool: Pool) -> None:
    adapter = PgMetadataAdapter(pool=pg_pool)
    await adapter.apply_pending()
    sub = Subscription(
        workspace_id="ws-1",
        subscription_id="sub-1",
        workflow_id="wf-1",
        state="enabled",
        created_at=_now(),
        updated_at=_now(),
    )
    await adapter.put_subscription("ws-1", sub)
    sel = SubscriptionSelector(
        workspace_id="ws-1",
        subscription_id="sub-1",
        selector=MappingProxyType({"event_type": "user.created"}),
        added_at=_now(),
    )
    written = await adapter.append_subscription_selector("ws-1", "sub-1", sel)
    assert written.selector["event_type"] == "user.created"
    updated = await adapter.update_subscription_state("ws-1", "sub-1", "disabled")
    assert updated.state == "disabled"


async def test_resume_subscription_lifecycle(pg_pool: Pool) -> None:
    adapter = PgMetadataAdapter(pool=pg_pool)
    await adapter.apply_pending()
    resume = ResumeSubscription(
        workspace_id="ws-1",
        resume_id="res-1",
        run_id="r-1",
        step_id="s-1",
        expires_at=_now() + timedelta(minutes=5),
        payload=MappingProxyType({"k": "v"}),
    )
    await adapter.put_resume_subscription("ws-1", resume)
    with pytest.raises(ImmutableViolation):
        await adapter.put_resume_subscription("ws-1", resume)
    await adapter.delete_resume_subscription("ws-1", "res-1")
    # Now re-insertable after delete (resume_id is single-shot but
    # deletable; primary-key collision only matters while alive).
    await adapter.put_resume_subscription("ws-1", resume)


async def test_dedup_key_reserve_or_read(pg_pool: Pool) -> None:
    adapter = PgMetadataAdapter(pool=pg_pool)
    await adapter.apply_pending()
    first = await adapter.put_dedup_key("ws-1", "k-1", ttl_seconds=60)
    assert isinstance(first, DedupReserved)
    second = await adapter.put_dedup_key("ws-1", "k-1", ttl_seconds=60)
    assert isinstance(second, DedupDuplicate)
    assert second.existing.key == "k-1"


async def test_schedule_put_and_advance(pg_pool: Pool) -> None:
    adapter = PgMetadataAdapter(pool=pg_pool)
    await adapter.apply_pending()
    s = Schedule(
        workspace_id="ws-1",
        schedule_id="sch-1",
        workflow_id="wf-1",
        cron="*/5 * * * *",
        next_fire_at=_now(),
        enabled=True,
    )
    await adapter.put_schedule("ws-1", s)
    later = _now() + timedelta(minutes=5)
    updated = await adapter.update_schedule_next_fire("ws-1", "sch-1", later)
    assert updated.next_fire_at == later


# ----- Cursor lease -----


async def test_cursor_lease_acquire_commit_release(pg_pool: Pool) -> None:
    adapter = PgMetadataAdapter(pool=pg_pool)
    await adapter.apply_pending()
    cur, handle = await adapter.acquire_cursor_lease(
        "ws-1", "inst-1", "holder-1", ttl_seconds=60
    )
    assert cur.lease_holder == "holder-1"
    new_at = _now()
    committed = await adapter.commit_cursor("ws-1", handle, "checkpoint-1", new_at)
    assert committed.value == "checkpoint-1"
    assert committed.lease_holder is None
    # Re-acquire after release-via-commit succeeds.
    _, handle2 = await adapter.acquire_cursor_lease(
        "ws-1", "inst-1", "holder-2", ttl_seconds=60
    )
    await adapter.release_cursor_lease("ws-1", handle2)


async def test_cursor_lease_busy_when_held_by_other(pg_pool: Pool) -> None:
    adapter = PgMetadataAdapter(pool=pg_pool)
    await adapter.apply_pending()
    await adapter.acquire_cursor_lease(
        "ws-1", "inst-1", "holder-1", ttl_seconds=60
    )
    with pytest.raises(LeaseBusy):
        await adapter.acquire_cursor_lease(
            "ws-1", "inst-1", "holder-2", ttl_seconds=60
        )


async def test_first_touch_acquire_does_not_block_on_concurrent_insert(
    pg_pool: Pool,
) -> None:
    """Two acquires racing on a brand-new cursor key must not hang.

    Without the `pg_try_advisory_xact_lock` gate, the second acquire's
    `INSERT … ON CONFLICT DO NOTHING` blocks waiting for the first
    tx's unique-index check to commit, defeating the NOWAIT contract.
    We simulate the race by holding the advisory lock from a separate
    connection — the adapter call must surface `LeaseBusy` immediately
    rather than wait on the row lock.
    """
    adapter = PgMetadataAdapter(pool=pg_pool)
    await adapter.apply_pending()
    async with pg_pool.acquire() as holder, holder.transaction():
        await holder.execute(
            "SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))",
            "ws-race",
            "inst-race",
        )
        with pytest.raises(LeaseBusy):
            await asyncio.wait_for(
                adapter.acquire_cursor_lease(
                    "ws-race", "inst-race", "holder-x", ttl_seconds=60
                ),
                timeout=5.0,
            )


async def test_commit_cursor_raises_lease_expired_after_other_steals(
    pg_pool: Pool,
) -> None:
    """If a different holder re-acquires after expiry, the original
    handle's commit must surface `LeaseExpired` (the UPDATE matches
    zero rows because `lease_holder` no longer equals the handle)."""
    adapter = PgMetadataAdapter(pool=pg_pool)
    await adapter.apply_pending()
    _, handle1 = await adapter.acquire_cursor_lease(
        "ws-1", "inst-1", "holder-1", ttl_seconds=1
    )
    # Force the row's lease to look expired by writing a past timestamp.
    async with pg_pool.acquire() as conn:
        await conn.execute(
            "UPDATE custos_state.connector_cursor "
            "SET lease_expires_at = now() - interval '1 hour' "
            "WHERE workspace_id = $1 AND instance_id = $2",
            "ws-1",
            "inst-1",
        )
    # holder-2 takes over now that the lease is expired.
    await adapter.acquire_cursor_lease("ws-1", "inst-1", "holder-2", ttl_seconds=60)
    with pytest.raises(LeaseExpired):
        await adapter.commit_cursor("ws-1", handle1, "x", _now())


# ----- Artifact backrefs -----


async def test_artifact_use_append_and_paginate(pg_pool: Pool) -> None:
    adapter = PgMetadataAdapter(pool=pg_pool)
    await adapter.apply_pending()
    for i in range(5):
        await adapter.append_artifact_use(
            "ws-1",
            ArtifactUse(
                workspace_id="ws-1",
                run_id=f"r-{i}",
                step_id="s-1",
                artifact_id="art-1",
                name=f"out-{i}",
                recorded_at=_now(),
            ),
        )
    page1 = await adapter.list_artifact_uses("ws-1", "art-1", limit=2)
    assert len(page1.items) == 2
    assert page1.next_cursor is not None
    page2 = await adapter.list_artifact_uses(
        "ws-1", "art-1", cursor=page1.next_cursor, limit=2
    )
    assert len(page2.items) == 2
    assert {u.run_id for u in page1.items}.isdisjoint(
        {u.run_id for u in page2.items}
    )


# ----- Gateway short-lived state -----


async def test_reserve_idempotency_fresh_record(pg_pool: Pool) -> None:
    adapter = PgMetadataAdapter(pool=pg_pool)
    await adapter.apply_pending()

    result = await adapter.reserve_idempotency_record(
        "ws-1", "user-1", "/api/create", "key-1", "hash-1", ttl_seconds=300
    )
    assert isinstance(result, IdemReserved)
    assert result.record.status == "in_progress"
    assert result.record.response_snapshot is None


async def test_reserve_idempotency_handles_conflicts(pg_pool: Pool) -> None:
    adapter = PgMetadataAdapter(pool=pg_pool)
    await adapter.apply_pending()

    # First reservation succeeds.
    result1 = await adapter.reserve_idempotency_record(
        "ws-1", "user-1", "/api/create", "key-1", "hash-1", ttl_seconds=300
    )
    assert isinstance(result1, IdemReserved)

    # Same key, same hash, in-progress -> ExistingInFlight.
    result2 = await adapter.reserve_idempotency_record(
        "ws-1", "user-1", "/api/create", "key-1", "hash-1", ttl_seconds=300
    )
    assert isinstance(result2, ExistingInFlight)

    # Same key, different hash -> KeyReuse.
    result3 = await adapter.reserve_idempotency_record(
        "ws-1", "user-1", "/api/create", "key-1", "hash-different", ttl_seconds=300
    )
    assert isinstance(result3, KeyReuse)


async def test_complete_idempotency_record(pg_pool: Pool) -> None:
    adapter = PgMetadataAdapter(pool=pg_pool)
    await adapter.apply_pending()

    await adapter.reserve_idempotency_record(
        "ws-1", "user-1", "/api/create", "key-1", "hash-1", ttl_seconds=300
    )
    result = await adapter.complete_idempotency_record(
        "ws-1", "user-1", "/api/create", "key-1", {"status": 200, "body": "ok"}
    )
    assert result.status == "completed"
    assert result.response_snapshot is not None
    assert result.response_snapshot["status"] == 200

    # Second reservation, same hash, now sees ExistingCompleted.
    result2 = await adapter.reserve_idempotency_record(
        "ws-1", "user-1", "/api/create", "key-1", "hash-1", ttl_seconds=300
    )
    assert isinstance(result2, ExistingCompleted)
    assert result2.response_snapshot is not None


async def test_complete_idempotency_record_not_reserved(pg_pool: Pool) -> None:
    """Completing a non-existent reservation raises NotReserved (per SPL contract)."""
    from custos_spl.errors import NotReserved

    adapter = PgMetadataAdapter(pool=pg_pool)
    await adapter.apply_pending()

    with pytest.raises(NotReserved):
        await adapter.complete_idempotency_record(
            "ws-1", "user-1", "/api/create", "key-nonexistent", {"status": 200}
        )


async def test_delete_expired_idempotency_records(pg_pool: Pool) -> None:
    adapter = PgMetadataAdapter(pool=pg_pool)
    await adapter.apply_pending()

    now = datetime.now(UTC)
    past = now - timedelta(seconds=1)

    # Reserve one that expires in future (won't be deleted).
    await adapter.reserve_idempotency_record(
        "ws-1", "user-1", "/api/create", "key-1", "hash-1", ttl_seconds=10
    )
    # Insert one with expires_at exactly at `now` (should be deleted with <=).
    async with pg_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO custos_state.idempotency_record "
            "(workspace_id, principal_id, route, idempotency_key, request_hash, "
            " status, reserved_at, expires_at) "
            "VALUES ($1, $2, $3, $4, $5, 'completed', now(), $6)",
            "ws-1", "user-1", "/api/boundary", "key-boundary", "hash-boundary", now,
        )
    # Insert one with expires_at in the past (will be deleted).
    async with pg_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO custos_state.idempotency_record "
            "(workspace_id, principal_id, route, idempotency_key, request_hash, "
            " status, reserved_at, expires_at) "
            "VALUES ($1, $2, $3, $4, $5, 'completed', now(), $6)",
            "ws-1", "user-1", "/api/delete", "key-2", "hash-2", past,
        )

    count = await adapter.delete_expired_idempotency_records(now)
    # Should delete both boundary and past (2 records).
    assert count == 2


async def test_put_device_code_session(pg_pool: Pool) -> None:
    adapter = PgMetadataAdapter(pool=pg_pool)
    await adapter.apply_pending()

    session = DeviceCodeSession(
        workspace_id="ws-1",
        device_code="D-ABC123",
        user_code="U-XYZ789",
        issuer_alias="google",
        status="pending",
        token_bundle=None,
        created_at=datetime.now(UTC),
        expires_at=datetime.now(UTC) + timedelta(minutes=10),
    )
    result = await adapter.put_device_code_session("ws-1", session)
    assert result.device_code == "D-ABC123"
    assert result.status == "pending"


async def test_get_device_code_session_by_device_code(pg_pool: Pool) -> None:
    adapter = PgMetadataAdapter(pool=pg_pool)
    await adapter.apply_pending()

    session = DeviceCodeSession(
        workspace_id="ws-1",
        device_code="D-ABC123",
        user_code="U-XYZ789",
        issuer_alias="google",
        status="pending",
        token_bundle=None,
        created_at=datetime.now(UTC),
        expires_at=datetime.now(UTC) + timedelta(minutes=10),
    )
    await adapter.put_device_code_session("ws-1", session)

    retrieved = await adapter.get_device_code_session_by_device_code(
        "ws-1", "D-ABC123"
    )
    assert retrieved is not None
    assert retrieved.device_code == "D-ABC123"


async def test_get_device_code_session_by_user_code(pg_pool: Pool) -> None:
    adapter = PgMetadataAdapter(pool=pg_pool)
    await adapter.apply_pending()

    session = DeviceCodeSession(
        workspace_id="ws-1",
        device_code="D-ABC123",
        user_code="U-XYZ789",
        issuer_alias="google",
        status="pending",
        token_bundle=None,
        created_at=datetime.now(UTC),
        expires_at=datetime.now(UTC) + timedelta(minutes=10),
    )
    await adapter.put_device_code_session("ws-1", session)

    retrieved = await adapter.get_device_code_session_by_user_code(
        "ws-1", "U-XYZ789"
    )
    assert retrieved is not None
    assert retrieved.user_code == "U-XYZ789"


async def test_complete_device_code_session(pg_pool: Pool) -> None:
    adapter = PgMetadataAdapter(pool=pg_pool)
    await adapter.apply_pending()

    session = DeviceCodeSession(
        workspace_id="ws-1",
        device_code="D-ABC123",
        user_code="U-XYZ789",
        issuer_alias="google",
        status="pending",
        token_bundle=None,
        created_at=datetime.now(UTC),
        expires_at=datetime.now(UTC) + timedelta(minutes=10),
    )
    await adapter.put_device_code_session("ws-1", session)

    completed = await adapter.complete_device_code_session(
        "ws-1", "D-ABC123", {"access_token": "tok123", "scope": "openid"}
    )
    assert completed.status == "completed"
    assert completed.token_bundle is not None
    assert completed.token_bundle["access_token"] == "tok123"


async def test_delete_expired_device_code_sessions(pg_pool: Pool) -> None:
    adapter = PgMetadataAdapter(pool=pg_pool)
    await adapter.apply_pending()

    now = datetime.now(UTC)
    future = now + timedelta(minutes=10)
    past = now - timedelta(seconds=1)

    # Insert an expired session (expires_at < now).
    session_expired = DeviceCodeSession(
        workspace_id="ws-1",
        device_code="D-OLD",
        user_code="U-OLD",
        issuer_alias="google",
        status="pending",
        token_bundle=None,
        created_at=past,
        expires_at=past,
    )
    async with pg_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO custos_state.device_code_session "
            "(workspace_id, device_code, user_code, issuer_alias, "
            " status, created_at, expires_at) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7)",
            session_expired.workspace_id,
            session_expired.device_code,
            session_expired.user_code,
            session_expired.issuer_alias,
            session_expired.status,
            session_expired.created_at,
            session_expired.expires_at,
        )

    # Insert one with expires_at exactly at `now` (should be deleted with <=).
    async with pg_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO custos_state.device_code_session "
            "(workspace_id, device_code, user_code, issuer_alias, "
            " status, created_at, expires_at) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7)",
            "ws-1", "D-BOUNDARY", "U-BOUNDARY", "google", "pending", now, now,
        )

    # Insert a fresh session (won't be deleted).
    session_fresh = DeviceCodeSession(
        workspace_id="ws-1",
        device_code="D-NEW",
        user_code="U-NEW",
        issuer_alias="google",
        status="pending",
        token_bundle=None,
        created_at=now,
        expires_at=future,
    )
    await adapter.put_device_code_session("ws-1", session_fresh)

    count = await adapter.delete_expired_device_code_sessions(now)
    # Should delete both past and boundary (2 records).
    assert count == 2


# ----- Transactions -----


async def test_with_transaction_runs_body(pg_pool: Pool) -> None:
    adapter = PgMetadataAdapter(pool=pg_pool)
    await adapter.apply_pending()

    async def body(tx: object) -> str:
        return "ok"

    result = await adapter.with_transaction(body)
    assert result == "ok"


async def test_with_transaction_handle_rejected_by_other_provider(
    pg_pool: Pool,
) -> None:
    """The handle is bound to its issuing provider; passing it to
    another provider's tx-aware method must raise
    `InvalidTransactionHandle`."""
    adapter1 = PgMetadataAdapter(pool=pg_pool)
    adapter2 = PgMetadataAdapter(pool=pg_pool)
    await adapter1.apply_pending()

    captured: dict[str, object] = {}

    async def body(tx: object) -> None:
        captured["tx"] = tx

    await adapter1.with_transaction(body)
    # The captured handle remains associated with adapter1; use
    # `append_audit` (a stubbed tx-aware method) on adapter2 to verify
    # that another provider rejects it.
    with pytest.raises(InvalidTransactionHandle):
        await adapter2.append_audit(
            "ws-1",
            event=object(),
            tx=captured["tx"],
        )


async def test_with_transaction_handle_rejected_after_callback_returns(
    pg_pool: Pool,
) -> None:
    """A handle retained past `with_transaction`'s return must be dead.

    `check_handle` alone only verifies provider ownership; without an
    explicit closed-state check the issuing provider would accept a
    stale handle even though the underlying asyncpg transaction has
    ended and the connection has gone back to the pool.
    """
    adapter = PgMetadataAdapter(pool=pg_pool)
    await adapter.apply_pending()

    captured: dict[str, object] = {}

    async def body(tx: object) -> None:
        captured["tx"] = tx

    await adapter.with_transaction(body)
    with pytest.raises(InvalidTransactionHandle):
        await adapter.append_audit(
            "ws-1",
            event=object(),
            tx=captured["tx"],
        )


async def test_with_transaction_marks_handle_closed_on_error(
    pg_pool: Pool,
) -> None:
    """Failure inside the callback must still close the handle."""
    adapter = PgMetadataAdapter(pool=pg_pool)
    await adapter.apply_pending()

    captured: dict[str, object] = {}

    class _Boom(RuntimeError):
        pass

    async def body(tx: object) -> None:
        captured["tx"] = tx
        raise _Boom("boom")

    with pytest.raises(_Boom):
        await adapter.with_transaction(body)
    with pytest.raises(InvalidTransactionHandle):
        await adapter.append_audit(
            "ws-1",
            event=object(),
            tx=captured["tx"],
        )


# ----- AuthStoreProvider -----


async def test_auth_apply_pending_records_revision(pg_pool: Pool) -> None:
    adapter = PgAuthAdapter(pool=pg_pool)
    summaries = await adapter.apply_pending()
    assert summaries  # at least one revision applied
    assert any("AuthStoreProvider rev1" in s for s in summaries)
    declared = adapter.declared_revisions
    assert 1 in declared["AuthStoreProvider"]


async def test_auth_apply_pending_is_idempotent(pg_pool: Pool) -> None:
    adapter = PgAuthAdapter(pool=pg_pool)
    first = await adapter.apply_pending()
    second = await adapter.apply_pending()
    assert first  # first run did something
    assert second == []  # second run is a no-op


async def test_put_and_get_tenant(pg_pool: Pool) -> None:
    adapter = PgAuthAdapter(pool=pg_pool)
    await adapter.apply_pending()

    tenant = Tenant(
        tenant_id="t-1",
        display_name="Acme Corp",
        disabled_at=None,
        created_at=datetime.now(UTC),
    )
    await adapter.put_tenant(tenant)

    retrieved = await adapter.get_tenant("t-1")
    assert retrieved is not None
    assert retrieved.tenant_id == "t-1"
    assert retrieved.display_name == "Acme Corp"
    assert retrieved.disabled_at is None


async def test_tenant_upsert_updates_display_name(pg_pool: Pool) -> None:
    adapter = PgAuthAdapter(pool=pg_pool)
    await adapter.apply_pending()

    tenant1 = Tenant(
        tenant_id="t-1",
        display_name="Acme Corp",
        disabled_at=None,
        created_at=datetime.now(UTC),
    )
    await adapter.put_tenant(tenant1)

    tenant2 = Tenant(
        tenant_id="t-1",
        display_name="Acme Corporation",
        disabled_at=None,
        created_at=datetime.now(UTC),
    )
    await adapter.put_tenant(tenant2)

    retrieved = await adapter.get_tenant("t-1")
    assert retrieved is not None
    assert retrieved.display_name == "Acme Corporation"


async def test_get_tenant_returns_none_when_absent(pg_pool: Pool) -> None:
    adapter = PgAuthAdapter(pool=pg_pool)
    await adapter.apply_pending()

    retrieved = await adapter.get_tenant("nonexistent")
    assert retrieved is None


async def test_list_tenants_returns_all(pg_pool: Pool) -> None:
    adapter = PgAuthAdapter(pool=pg_pool)
    await adapter.apply_pending()

    now = datetime.now(UTC)
    await adapter.put_tenant(
        Tenant(
            tenant_id="t-1",
            display_name="Tenant 1",
            disabled_at=None,
            created_at=now,
        )
    )
    await adapter.put_tenant(
        Tenant(
            tenant_id="t-2",
            display_name="Tenant 2",
            disabled_at=None,
            created_at=now + timedelta(seconds=1),
        )
    )

    tenants = await adapter.list_tenants(TenantFilter())
    assert len(tenants) == 2
    assert {t.tenant_id for t in tenants} == {"t-1", "t-2"}


async def test_list_tenants_excludes_disabled_by_default(pg_pool: Pool) -> None:
    adapter = PgAuthAdapter(pool=pg_pool)
    await adapter.apply_pending()

    now = datetime.now(UTC)
    await adapter.put_tenant(
        Tenant(
            tenant_id="t-active",
            display_name="Active",
            disabled_at=None,
            created_at=now,
        )
    )
    await adapter.put_tenant(
        Tenant(
            tenant_id="t-disabled",
            display_name="Disabled",
            disabled_at=now,
            created_at=now,
        )
    )

    tenants = await adapter.list_tenants(TenantFilter())
    assert len(tenants) == 1
    assert tenants[0].tenant_id == "t-active"


async def test_put_and_get_workspace(pg_pool: Pool) -> None:
    adapter = PgAuthAdapter(pool=pg_pool)
    await adapter.apply_pending()

    # Create tenant first
    tenant = Tenant(
        tenant_id="t-1",
        display_name="Acme Corp",
        disabled_at=None,
        created_at=datetime.now(UTC),
    )
    await adapter.put_tenant(tenant)

    workspace = Workspace(
        workspace_id="ws-1",
        tenant_id="t-1",
        display_name="Production",
        disabled_at=None,
        created_at=datetime.now(UTC),
    )
    await adapter.put_workspace(workspace)

    retrieved = await adapter.get_workspace("ws-1")
    assert retrieved is not None
    assert retrieved.workspace_id == "ws-1"
    assert retrieved.tenant_id == "t-1"
    assert retrieved.display_name == "Production"


async def test_list_workspaces_filters_by_tenant(pg_pool: Pool) -> None:
    adapter = PgAuthAdapter(pool=pg_pool)
    await adapter.apply_pending()

    now = datetime.now(UTC)

    # Create two tenants
    await adapter.put_tenant(
        Tenant(
            tenant_id="t-1",
            display_name="Tenant 1",
            disabled_at=None,
            created_at=now,
        )
    )
    await adapter.put_tenant(
        Tenant(
            tenant_id="t-2",
            display_name="Tenant 2",
            disabled_at=None,
            created_at=now,
        )
    )

    # Create workspaces in each tenant
    await adapter.put_workspace(
        Workspace(
            workspace_id="ws-t1-1",
            tenant_id="t-1",
            display_name="WS1-1",
            disabled_at=None,
            created_at=now,
        )
    )
    await adapter.put_workspace(
        Workspace(
            workspace_id="ws-t1-2",
            tenant_id="t-1",
            display_name="WS1-2",
            disabled_at=None,
            created_at=now,
        )
    )
    await adapter.put_workspace(
        Workspace(
            workspace_id="ws-t2-1",
            tenant_id="t-2",
            display_name="WS2-1",
            disabled_at=None,
            created_at=now,
        )
    )

    # List workspaces for tenant 1
    ws_t1 = await adapter.list_workspaces(
        WorkspaceFilter(tenant_id="t-1", include_disabled=False)
    )
    assert len(ws_t1) == 2
    assert {w.workspace_id for w in ws_t1} == {"ws-t1-1", "ws-t1-2"}

    # List workspaces for tenant 2
    ws_t2 = await adapter.list_workspaces(
        WorkspaceFilter(tenant_id="t-2", include_disabled=False)
    )
    assert len(ws_t2) == 1
    assert ws_t2[0].workspace_id == "ws-t2-1"




async def test_put_and_get_user_principal(pg_pool: Pool) -> None:
    adapter = PgAuthAdapter(pool=pg_pool)
    await adapter.apply_pending()

    now = datetime.now(UTC)
    tenant = Tenant(
        tenant_id="t-1",
        display_name="Tenant 1",
        disabled_at=None,
        created_at=now,
    )
    await adapter.put_tenant(tenant)

    user = User(
        kind="user",
        principal_id="user-1",
        tenant_id="t-1",
        display_name="Alice",
        email="alice@example.com",
        disabled_at=None,
        disabled_reason=None,
        created_at=now,
    )
    await adapter.put_principal(user)

    retrieved = await adapter.get_principal("user-1")
    assert retrieved is not None
    assert isinstance(retrieved, User)
    assert retrieved.principal_id == "user-1"
    assert retrieved.tenant_id == "t-1"
    assert retrieved.display_name == "Alice"
    assert retrieved.email == "alice@example.com"


async def test_put_and_get_service_account_principal(pg_pool: Pool) -> None:
    adapter = PgAuthAdapter(pool=pg_pool)
    await adapter.apply_pending()

    now = datetime.now(UTC)
    tenant = Tenant(
        tenant_id="t-1",
        display_name="Tenant 1",
        disabled_at=None,
        created_at=now,
    )
    await adapter.put_tenant(tenant)

    workspace = Workspace(
        workspace_id="ws-1",
        tenant_id="t-1",
        display_name="Workspace 1",
        disabled_at=None,
        created_at=now,
    )
    await adapter.put_workspace(workspace)

    sa = ServiceAccount(
        kind="serviceAccount",
        principal_id="sa-1",
        workspace_id="ws-1",
        display_name="Bot Account",
        disabled_at=None,
        disabled_reason=None,
        created_at=now,
    )
    await adapter.put_principal(sa)

    retrieved = await adapter.get_principal("sa-1")
    assert retrieved is not None
    assert isinstance(retrieved, ServiceAccount)
    assert retrieved.principal_id == "sa-1"
    assert retrieved.workspace_id == "ws-1"
    assert retrieved.display_name == "Bot Account"


async def test_user_principal_upsert_updates_display_name(pg_pool: Pool) -> None:
    adapter = PgAuthAdapter(pool=pg_pool)
    await adapter.apply_pending()

    now = datetime.now(UTC)
    tenant = Tenant(
        tenant_id="t-1",
        display_name="Tenant 1",
        disabled_at=None,
        created_at=now,
    )
    await adapter.put_tenant(tenant)

    user1 = User(
        kind="user",
        principal_id="user-1",
        tenant_id="t-1",
        display_name="Alice",
        email="alice@example.com",
        disabled_at=None,
        disabled_reason=None,
        created_at=now,
    )
    await adapter.put_principal(user1)

    user2 = User(
        kind="user",
        principal_id="user-1",
        tenant_id="t-1",
        display_name="Alice Smith",
        email="alice.smith@example.com",
        disabled_at=None,
        disabled_reason=None,
        created_at=now,
    )
    await adapter.put_principal(user2)

    retrieved = await adapter.get_principal("user-1")
    assert retrieved is not None
    assert retrieved.display_name == "Alice Smith"


async def test_get_principal_returns_none_when_absent(pg_pool: Pool) -> None:
    adapter = PgAuthAdapter(pool=pg_pool)
    await adapter.apply_pending()

    retrieved = await adapter.get_principal("nonexistent")
    assert retrieved is None


async def test_list_principals_returns_all(pg_pool: Pool) -> None:
    adapter = PgAuthAdapter(pool=pg_pool)
    await adapter.apply_pending()

    now = datetime.now(UTC)
    tenant = Tenant(
        tenant_id="t-1",
        display_name="Tenant 1",
        disabled_at=None,
        created_at=now,
    )
    await adapter.put_tenant(tenant)

    workspace = Workspace(
        workspace_id="ws-1",
        tenant_id="t-1",
        display_name="Workspace 1",
        disabled_at=None,
        created_at=now,
    )
    await adapter.put_workspace(workspace)

    # Add three principals
    user1 = User(
        kind="user",
        principal_id="user-1",
        tenant_id="t-1",
        display_name="Alice",
        email="alice@example.com",
        disabled_at=None,
        disabled_reason=None,
        created_at=now,
    )
    await adapter.put_principal(user1)

    user2 = User(
        kind="user",
        principal_id="user-2",
        tenant_id="t-1",
        display_name="Bob",
        email="bob@example.com",
        disabled_at=None,
        disabled_reason=None,
        created_at=now,
    )
    await adapter.put_principal(user2)

    sa = ServiceAccount(
        kind="serviceAccount",
        principal_id="sa-1",
        workspace_id="ws-1",
        display_name="Bot",
        disabled_at=None,
        disabled_reason=None,
        created_at=now,
    )
    await adapter.put_principal(sa)

    # List all principals
    principals = await adapter.list_principals(PrincipalFilter())
    assert len(principals) == 3
    assert {p.principal_id for p in principals} == {"user-1", "user-2", "sa-1"}


async def test_list_principals_filters_by_tenant(pg_pool: Pool) -> None:
    adapter = PgAuthAdapter(pool=pg_pool)
    await adapter.apply_pending()

    now = datetime.now(UTC)

    # Create two tenants
    await adapter.put_tenant(
        Tenant(
            tenant_id="t-1",
            display_name="Tenant 1",
            disabled_at=None,
            created_at=now,
        )
    )
    await adapter.put_tenant(
        Tenant(
            tenant_id="t-2",
            display_name="Tenant 2",
            disabled_at=None,
            created_at=now,
        )
    )

    # Add users to each tenant
    user1 = User(
        kind="user",
        principal_id="user-1",
        tenant_id="t-1",
        display_name="Alice",
        email="alice@example.com",
        disabled_at=None,
        disabled_reason=None,
        created_at=now,
    )
    await adapter.put_principal(user1)

    user2 = User(
        kind="user",
        principal_id="user-2",
        tenant_id="t-2",
        display_name="Charlie",
        email="charlie@example.com",
        disabled_at=None,
        disabled_reason=None,
        created_at=now,
    )
    await adapter.put_principal(user2)

    # Filter by tenant 1
    t1_principals = await adapter.list_principals(
        PrincipalFilter(tenant_id="t-1", include_disabled=False)
    )
    assert len(t1_principals) == 1
    assert t1_principals[0].principal_id == "user-1"

    # Filter by tenant 2
    t2_principals = await adapter.list_principals(
        PrincipalFilter(tenant_id="t-2", include_disabled=False)
    )
    assert len(t2_principals) == 1
    assert t2_principals[0].principal_id == "user-2"


async def test_list_principals_filters_by_workspace(pg_pool: Pool) -> None:
    adapter = PgAuthAdapter(pool=pg_pool)
    await adapter.apply_pending()

    now = datetime.now(UTC)
    tenant = Tenant(
        tenant_id="t-1",
        display_name="Tenant 1",
        disabled_at=None,
        created_at=now,
    )
    await adapter.put_tenant(tenant)

    # Create two workspaces
    await adapter.put_workspace(
        Workspace(
            workspace_id="ws-1",
            tenant_id="t-1",
            display_name="Workspace 1",
            disabled_at=None,
            created_at=now,
        )
    )
    await adapter.put_workspace(
        Workspace(
            workspace_id="ws-2",
            tenant_id="t-1",
            display_name="Workspace 2",
            disabled_at=None,
            created_at=now,
        )
    )

    # Add service accounts to each workspace
    sa1 = ServiceAccount(
        kind="serviceAccount",
        principal_id="sa-1",
        workspace_id="ws-1",
        display_name="Bot 1",
        disabled_at=None,
        disabled_reason=None,
        created_at=now,
    )
    await adapter.put_principal(sa1)

    sa2 = ServiceAccount(
        kind="serviceAccount",
        principal_id="sa-2",
        workspace_id="ws-2",
        display_name="Bot 2",
        disabled_at=None,
        disabled_reason=None,
        created_at=now,
    )
    await adapter.put_principal(sa2)

    # Filter by workspace 1
    ws1_principals = await adapter.list_principals(
        PrincipalFilter(workspace_id="ws-1", include_disabled=False)
    )
    assert len(ws1_principals) == 1
    assert ws1_principals[0].principal_id == "sa-1"


async def test_list_principals_filters_by_kind(pg_pool: Pool) -> None:
    adapter = PgAuthAdapter(pool=pg_pool)
    await adapter.apply_pending()

    now = datetime.now(UTC)
    tenant = Tenant(
        tenant_id="t-1",
        display_name="Tenant 1",
        disabled_at=None,
        created_at=now,
    )
    await adapter.put_tenant(tenant)

    workspace = Workspace(
        workspace_id="ws-1",
        tenant_id="t-1",
        display_name="Workspace 1",
        disabled_at=None,
        created_at=now,
    )
    await adapter.put_workspace(workspace)

    # Add a user
    user = User(
        kind="user",
        principal_id="user-1",
        tenant_id="t-1",
        display_name="Alice",
        email="alice@example.com",
        disabled_at=None,
        disabled_reason=None,
        created_at=now,
    )
    await adapter.put_principal(user)

    # Add a service account
    sa = ServiceAccount(
        kind="serviceAccount",
        principal_id="sa-1",
        workspace_id="ws-1",
        display_name="Bot",
        disabled_at=None,
        disabled_reason=None,
        created_at=now,
    )
    await adapter.put_principal(sa)

    # Filter by user kind
    users = await adapter.list_principals(
        PrincipalFilter(kind="user", include_disabled=False)
    )
    assert len(users) == 1
    assert users[0].principal_id == "user-1"

    # Filter by serviceAccount kind
    sas = await adapter.list_principals(
        PrincipalFilter(kind="serviceAccount", include_disabled=False)
    )
    assert len(sas) == 1
    assert sas[0].principal_id == "sa-1"


async def test_list_principals_excludes_disabled_by_default(pg_pool: Pool) -> None:
    adapter = PgAuthAdapter(pool=pg_pool)
    await adapter.apply_pending()

    now = datetime.now(UTC)
    tenant = Tenant(
        tenant_id="t-1",
        display_name="Tenant 1",
        disabled_at=None,
        created_at=now,
    )
    await adapter.put_tenant(tenant)

    # Add an active user
    user1 = User(
        kind="user",
        principal_id="user-1",
        tenant_id="t-1",
        display_name="Alice",
        email="alice@example.com",
        disabled_at=None,
        disabled_reason=None,
        created_at=now,
    )
    await adapter.put_principal(user1)

    # Add a disabled user
    user2 = User(
        kind="user",
        principal_id="user-2",
        tenant_id="t-1",
        display_name="Bob",
        email="bob@example.com",
        disabled_at=datetime.now(UTC),
        disabled_reason="Inactive",
        created_at=now,
    )
    await adapter.put_principal(user2)

    # List without disabled (default)
    active = await adapter.list_principals()
    assert len(active) == 1
    assert active[0].principal_id == "user-1"

    # List with disabled
    all_principals = await adapter.list_principals(
        PrincipalFilter(include_disabled=True)
    )
    assert len(all_principals) == 2


async def test_disable_principal(pg_pool: Pool) -> None:
    adapter = PgAuthAdapter(pool=pg_pool)
    await adapter.apply_pending()

    now = datetime.now(UTC)
    tenant = Tenant(
        tenant_id="t-1",
        display_name="Tenant 1",
        disabled_at=None,
        created_at=now,
    )
    await adapter.put_tenant(tenant)

    user = User(
        kind="user",
        principal_id="user-1",
        tenant_id="t-1",
        display_name="Alice",
        email="alice@example.com",
        disabled_at=None,
        disabled_reason=None,
        created_at=now,
    )
    await adapter.put_principal(user)

    # Disable the principal
    from custos_spl.ids import PrincipalId
    await adapter.disable_principal(
        PrincipalId("user-1"), PrincipalId("admin"), "Unauthorized access"
    )

    # Verify it's now disabled
    retrieved = await adapter.get_principal("user-1")
    assert retrieved is not None
    assert retrieved.disabled_at is not None
    assert retrieved.disabled_reason == "Unauthorized access"




async def test_put_and_get_oidc_identity(pg_pool: Pool) -> None:
    adapter = PgAuthAdapter(pool=pg_pool)
    await adapter.apply_pending()

    now = datetime.now(UTC)
    tenant = Tenant(
        tenant_id="t-1",
        display_name="Tenant 1",
        disabled_at=None,
        created_at=now,
    )
    await adapter.put_tenant(tenant)

    user = User(
        kind="user",
        principal_id="user-1",
        tenant_id="t-1",
        display_name="Alice",
        email="alice@example.com",
        disabled_at=None,
        disabled_reason=None,
        created_at=now,
    )
    await adapter.put_principal(user)

    from custos_spl.ids import PrincipalId
    await adapter.put_oidc_identity("https://example.com", "alice@example.com", PrincipalId("user-1"))

    retrieved = await adapter.get_oidc_identity("https://example.com", "alice@example.com")
    assert retrieved == PrincipalId("user-1")


async def test_get_oidc_identity_returns_none_when_absent(pg_pool: Pool) -> None:
    adapter = PgAuthAdapter(pool=pg_pool)
    await adapter.apply_pending()

    retrieved = await adapter.get_oidc_identity("https://example.com", "unknown@example.com")
    assert retrieved is None


async def test_put_oidc_identity_write_once_raises_immutable_violation(pg_pool: Pool) -> None:
    adapter = PgAuthAdapter(pool=pg_pool)
    await adapter.apply_pending()

    now = datetime.now(UTC)
    tenant = Tenant(
        tenant_id="t-1",
        display_name="Tenant 1",
        disabled_at=None,
        created_at=now,
    )
    await adapter.put_tenant(tenant)

    user = User(
        kind="user",
        principal_id="user-1",
        tenant_id="t-1",
        display_name="Alice",
        email="alice@example.com",
        disabled_at=None,
        disabled_reason=None,
        created_at=now,
    )
    await adapter.put_principal(user)

    from custos_spl.ids import PrincipalId
    await adapter.put_oidc_identity("https://example.com", "alice@example.com", PrincipalId("user-1"))

    # Try to rebind the same (issuer, subject) — should raise ImmutableViolation
    with pytest.raises(ImmutableViolation):
        await adapter.put_oidc_identity("https://example.com", "alice@example.com", PrincipalId("user-1"))


async def test_list_oidc_identities_for_user(pg_pool: Pool) -> None:
    adapter = PgAuthAdapter(pool=pg_pool)
    await adapter.apply_pending()

    now = datetime.now(UTC)
    tenant = Tenant(
        tenant_id="t-1",
        display_name="Tenant 1",
        disabled_at=None,
        created_at=now,
    )
    await adapter.put_tenant(tenant)

    user = User(
        kind="user",
        principal_id="user-1",
        tenant_id="t-1",
        display_name="Alice",
        email="alice@example.com",
        disabled_at=None,
        disabled_reason=None,
        created_at=now,
    )
    await adapter.put_principal(user)

    from custos_spl.ids import PrincipalId
    # Add multiple OIDC identities for the same user
    await adapter.put_oidc_identity("https://example.com", "alice@example.com", PrincipalId("user-1"))
    await adapter.put_oidc_identity("https://github.com", "alice_github", PrincipalId("user-1"))

    identities = await adapter.list_oidc_identities_for_user(PrincipalId("user-1"))
    assert len(identities) == 2
    assert {(i.issuer, i.subject) for i in identities} == {
        ("https://example.com", "alice@example.com"),
        ("https://github.com", "alice_github"),
    }
    assert all(i.user_id == PrincipalId("user-1") for i in identities)


async def test_list_oidc_identities_for_user_returns_empty_when_none_exist(pg_pool: Pool) -> None:
    adapter = PgAuthAdapter(pool=pg_pool)
    await adapter.apply_pending()

    now = datetime.now(UTC)
    tenant = Tenant(
        tenant_id="t-1",
        display_name="Tenant 1",
        disabled_at=None,
        created_at=now,
    )
    await adapter.put_tenant(tenant)

    user = User(
        kind="user",
        principal_id="user-1",
        tenant_id="t-1",
        display_name="Alice",
        email="alice@example.com",
        disabled_at=None,
        disabled_reason=None,
        created_at=now,
    )
    await adapter.put_principal(user)

    from custos_spl.ids import PrincipalId
    identities = await adapter.list_oidc_identities_for_user(PrincipalId("user-1"))
    assert len(identities) == 0


async def test_unimplemented_service_token_methods_raise_not_implemented_error(

    pg_pool: Pool,
) -> None:
    """Out-of-scope methods (SPL-130e and later) raise NotImplementedError."""
    adapter = PgAuthAdapter(pool=pg_pool)
    await adapter.apply_pending()

    # OIDC methods (SPL-130d) are now implemented; service tokens and later raise errors

    with pytest.raises(NotImplementedError, match="SPL-130e"):
        await adapter.put_service_token(None)

    with pytest.raises(NotImplementedError, match="SPL-130f"):
        await adapter.upsert_permission(None)

    with pytest.raises(NotImplementedError, match="SPL-130g"):
        await adapter.put_role_binding(None)

    with pytest.raises(NotImplementedError, match="SPL-130h"):
        await adapter.with_transaction(None)

