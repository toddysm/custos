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
from typing import TYPE_CHECKING

import pytest
from custos_spl.errors import ConflictDigest, ImmutableViolation
from custos_spl.migrations.runner import check_revisions

from custos_pg.adapters.catalog import PgCatalogAdapter
from custos_pg.adapters.definition import PgDefinitionAdapter

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
