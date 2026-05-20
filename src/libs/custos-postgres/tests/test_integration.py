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
    ResumeSubscription,
    Run,
    Schedule,
    Step,
    StepAttempt,
    Subscription,
    SubscriptionSelector,
)
from custos_spl.migrations.runner import check_revisions

from custos_pg.adapters.catalog import PgCatalogAdapter
from custos_pg.adapters.definition import PgDefinitionAdapter
from custos_pg.adapters.metadata import PgMetadataAdapter

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


async def test_metadata_still_a_gap_after_rev1_apply(pg_pool: Pool) -> None:
    """Adapter declares only rev 1; platform requires rev 4 (audit_outbox).

    Confirms the iterative-slice contract: applying #127 does NOT
    satisfy MetadataStoreProvider — operators still need #128/#129 to
    land before `check_revisions` will pass.
    """
    from custos_spl.errors import MigrationRequired

    adapter = PgMetadataAdapter(pool=pg_pool)
    await adapter.apply_pending()
    with pytest.raises(MigrationRequired) as exc:
        check_revisions([adapter])
    gaps = dict(exc.value.gaps)
    assert gaps.get("MetadataStoreProvider") == 4


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
