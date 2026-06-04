"""WF-IMPL-118 — Postgres-backed durable ``StartRun`` integration suite.

Issue: https://github.com/toddysm/custos/issues/621

These tests prove that the durable wiring stood up across WF-IMPL-114
(real Catalog client), WF-IMPL-116 (durable Run store) and WF-IMPL-117
(durable idempotency ledger) actually persists and dedups against a
*real* Postgres — not the in-process fakes the unit suites inject.

Every test is marked ``integration`` so the default ``pytest`` run (and
the coverage gate in CI) deselects it; the dedicated
``workflow-service-integration`` GitHub Actions job re-selects it with
``-m integration`` against a ``postgres:15-alpine`` service container.
Locally the suite spins up a throwaway Postgres via ``testcontainers``
when Docker is available, and **skips cleanly** otherwise.

The "restart" in each test name is simulated by minting a brand-new
``PgMetadataAdapter`` over a fresh ``asyncpg`` pool on the same DSN —
i.e. the only state that crosses the boundary is what Postgres durably
persisted, exactly as it would across an HA failover or pod restart.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import pytest
import yaml
from custos_spl.interfaces.metadata_store import MetadataStoreProvider

from custos_workflow.document.models import WorkflowDocument
from custos_workflow.runs import (
    InProcessRunStore,
    RunId,
    RunRecord,
    RunStatus,
    derive_run_id,
)
from custos_workflow.runs.controller import WorkflowVersion
from custos_workflow.validator import (
    DurableIdempotencyLedger,
    IdempotencyConflictError,
    StartRunValidator,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    ProviderFactory = Callable[[], Awaitable[MetadataStoreProvider]]


pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Constants / helpers
# ---------------------------------------------------------------------------


WORKSPACE = "ws-int-118"
WORKFLOW_ID = "wf-int-118"
WORKFLOW_VERSION_ID = "wfv-int-118"
VERSION_LABEL = "v1"
TTL = timedelta(hours=1)


def _ts(seconds: int = 0) -> datetime:
    return datetime(2026, 1, 1, 0, 0, seconds, tzinfo=UTC)


def _record(*, run_id: RunId, status: RunStatus = RunStatus.QUEUED) -> RunRecord:
    return RunRecord(
        workspace_id=WORKSPACE,
        run_id=run_id,
        workflow_id=WORKFLOW_ID,
        workflow_version=VERSION_LABEL,
        status=status,
        reason=None,
        started_at=_ts(0),
        updated_at=_ts(0),
        compiled_graph=None,
    )


def _workflow_version() -> WorkflowVersion:
    parsed: dict[str, Any] = yaml.safe_load(
        f"""
        apiVersion: custos.dev/v1
        kind: Workflow
        metadata:
          name: pipeline
          workspace: {WORKSPACE}
        spec:
          inputs:
            a: {{type: integer, required: false}}
          steps:
            - id: a
              let: {{x: '${{{{ true }}}}'}}
        """
    )
    doc = WorkflowDocument.model_validate(parsed)
    return WorkflowVersion(
        id=WORKFLOW_VERSION_ID,
        workflow_id=WORKFLOW_ID,
        name="pipeline",
        version_label=VERSION_LABEL,
        document=doc,
    )


class _FixedCatalogClient:
    """Catalog Protocol fake returning one fixed ``WorkflowVersion``."""

    def __init__(self, version: WorkflowVersion) -> None:
        self._version = version
        self.calls: list[tuple[str, str]] = []

    async def get_workflow_version(
        self, workspace_id: str, workflow_version_id: str
    ) -> WorkflowVersion:
        self.calls.append((workspace_id, workflow_version_id))
        return self._version


# ---------------------------------------------------------------------------
# Durable Run store
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_persists_and_survives_restart(
    metadata_provider_factory: ProviderFactory,
) -> None:
    """A run written by one adapter is recovered by a fresh one over Postgres."""
    provider_before = await metadata_provider_factory()
    store_before = InProcessRunStore(provider_before)
    run_id = derive_run_id(WORKSPACE, "restart-key")
    await store_before.put_run(_record(run_id=run_id, status=RunStatus.QUEUED))
    await store_before.update_run_status(WORKSPACE, run_id, RunStatus.RUNNING)

    # "Restart": brand-new adapter + pool over the same DSN.
    provider_after = await metadata_provider_factory()
    store_after = InProcessRunStore(provider_after)
    recovered = await store_after.get_run(WORKSPACE, run_id)

    assert recovered is not None
    assert recovered.run_id == run_id
    assert recovered.status is RunStatus.RUNNING


@pytest.mark.asyncio
async def test_list_runs_pagination_survives_restart(
    metadata_provider_factory: ProviderFactory,
) -> None:
    """``list_runs`` cursor pagination is identical after a restart."""
    provider_before = await metadata_provider_factory()
    store_before = InProcessRunStore(provider_before)
    run_ids = [derive_run_id(WORKSPACE, f"r{i}") for i in range(5)]
    for run_id in run_ids:
        await store_before.put_run(_record(run_id=run_id))

    provider_after = await metadata_provider_factory()
    store_after = InProcessRunStore(provider_after)
    first = await store_after.list_runs(WORKSPACE, limit=2)
    second = await store_after.list_runs(WORKSPACE, cursor=first.next_cursor, limit=2)
    third = await store_after.list_runs(WORKSPACE, cursor=second.next_cursor, limit=2)

    assert [len(p.items) for p in (first, second, third)] == [2, 2, 1]
    assert third.next_cursor is None
    seen = {r.run_id for r in (*first.items, *second.items, *third.items)}
    assert seen == set(run_ids)


@pytest.mark.asyncio
async def test_list_runs_empty_workspace_after_restart(
    metadata_provider_factory: ProviderFactory,
) -> None:
    """An empty workspace lists no runs and a None cursor over Postgres."""
    provider = await metadata_provider_factory()
    store = InProcessRunStore(provider)
    page = await store.list_runs("ws-empty", limit=10)
    assert page.items == ()
    assert page.next_cursor is None


# ---------------------------------------------------------------------------
# Durable idempotency ledger
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_idempotency_dedup_survives_restart(
    metadata_provider_factory: ProviderFactory,
) -> None:
    """A reservation written by one ledger replays through a fresh one."""
    ledger_before = DurableIdempotencyLedger(await metadata_provider_factory(), ttl=TTL)
    first = await ledger_before.record_or_replay(
        workspace_id=WORKSPACE,
        idempotency_key="dedup-key",
        request_fingerprint="fp-1",
    )
    assert first.replayed is False

    # "Restart": new adapter over the same DSN must see the prior row.
    ledger_after = DurableIdempotencyLedger(await metadata_provider_factory(), ttl=TTL)
    second = await ledger_after.record_or_replay(
        workspace_id=WORKSPACE,
        idempotency_key="dedup-key",
        request_fingerprint="fp-1",
    )
    assert second.replayed is True


@pytest.mark.asyncio
async def test_idempotency_conflict_on_divergent_fingerprint(
    metadata_provider_factory: ProviderFactory,
) -> None:
    """Reusing a key with a different fingerprint raises a conflict."""
    ledger = DurableIdempotencyLedger(await metadata_provider_factory(), ttl=TTL)
    await ledger.record_or_replay(
        workspace_id=WORKSPACE,
        idempotency_key="conflict-key",
        request_fingerprint="fp-1",
    )

    with pytest.raises(IdempotencyConflictError):
        await ledger.record_or_replay(
            workspace_id=WORKSPACE,
            idempotency_key="conflict-key",
            request_fingerprint="fp-2",
        )


@pytest.mark.asyncio
async def test_purge_expired_reaps_then_allows_fresh_reservation(
    metadata_provider_factory: ProviderFactory,
) -> None:
    """``purge_expired`` deletes a lapsed row so the key reserves fresh again."""
    ledger = DurableIdempotencyLedger(await metadata_provider_factory(), ttl=TTL)
    await ledger.record_or_replay(
        workspace_id=WORKSPACE,
        idempotency_key="ttl-key",
        request_fingerprint="fp-1",
    )

    # Sweep with a cutoff well past the reservation's expiry.
    purged = await ledger.purge_expired(before=datetime.now(UTC) + timedelta(days=1))
    assert purged == 1

    # The key is free again — a fresh reservation, not a replay.
    fresh = await ledger.record_or_replay(
        workspace_id=WORKSPACE,
        idempotency_key="ttl-key",
        request_fingerprint="fp-2",
    )
    assert fresh.replayed is False


# ---------------------------------------------------------------------------
# Full StartRun validator path over Postgres
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_run_validator_dedups_across_restart(
    metadata_provider_factory: ProviderFactory,
) -> None:
    """The full StartRun validator dedups an identical replay over Postgres."""
    version = _workflow_version()
    validator_before = StartRunValidator(
        catalog=_FixedCatalogClient(version),
        ledger=DurableIdempotencyLedger(await metadata_provider_factory(), ttl=TTL),
    )
    first = await validator_before.validate_start_run(
        workspace_id=WORKSPACE,
        workflow_version_id=WORKFLOW_VERSION_ID,
        idempotency_key="start-key",
    )
    assert first.replayed is False

    # "Restart": a fresh validator + ledger + adapter over the same DSN.
    validator_after = StartRunValidator(
        catalog=_FixedCatalogClient(version),
        ledger=DurableIdempotencyLedger(await metadata_provider_factory(), ttl=TTL),
    )
    second = await validator_after.validate_start_run(
        workspace_id=WORKSPACE,
        workflow_version_id=WORKFLOW_VERSION_ID,
        idempotency_key="start-key",
    )
    assert second.replayed is True
    assert second.request_fingerprint == first.request_fingerprint


@pytest.mark.asyncio
async def test_start_run_validator_conflict_across_restart(
    metadata_provider_factory: ProviderFactory,
) -> None:
    """A divergent replay of the same key surfaces a conflict over Postgres."""
    version = _workflow_version()
    validator_before = StartRunValidator(
        catalog=_FixedCatalogClient(version),
        ledger=DurableIdempotencyLedger(await metadata_provider_factory(), ttl=TTL),
    )
    await validator_before.validate_start_run(
        workspace_id=WORKSPACE,
        workflow_version_id=WORKFLOW_VERSION_ID,
        idempotency_key="start-key",
        inputs={"a": 1},
    )

    validator_after = StartRunValidator(
        catalog=_FixedCatalogClient(version),
        ledger=DurableIdempotencyLedger(await metadata_provider_factory(), ttl=TTL),
    )
    with pytest.raises(IdempotencyConflictError):
        await validator_after.validate_start_run(
            workspace_id=WORKSPACE,
            workflow_version_id=WORKFLOW_VERSION_ID,
            idempotency_key="start-key",
            inputs={"a": 2},
        )
