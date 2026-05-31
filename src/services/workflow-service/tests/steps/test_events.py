"""Tests for the WF-IMPL-056 ``step.*`` lifecycle event emission surface."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from typing import Any

import pytest

from custos_workflow.runs.controller import (
    InMemoryLifecycleEventPublisher,
    LifecycleEvent,
    LifecycleEventPublisher,
)
from custos_workflow.runs.ids import RunId, derive_run_id
from custos_workflow.steps import (
    DEFAULT_STEP_DEDUP_CACHE_SIZE,
    LIFECYCLE_KIND_STEP_COMPLETED,
    LIFECYCLE_KIND_STEP_FAILED,
    LIFECYCLE_KIND_STEP_RETRY_SCHEDULED,
    LIFECYCLE_KIND_STEP_SKIPPED,
    LIFECYCLE_KIND_STEP_STARTED,
    LIFECYCLE_KIND_STEP_WAITING,
    LOCKED_STEP_EVENT_KINDS,
    LifecycleEventPublisherAdapter,
    RetryNow,
    StepLifecyclePublisher,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_WORKSPACE: str = "ws-events"
_WORKFLOW_VERSION_ID: str = "wfv-events-1"
_RUN_ID: RunId = derive_run_id(_WORKSPACE, "events-key-primary")
_OCCURRED_AT: datetime = datetime(2026, 5, 30, 12, 34, 56, tzinfo=UTC)


def _adapter(
    *, max_seen_keys: int = DEFAULT_STEP_DEDUP_CACHE_SIZE
) -> tuple[LifecycleEventPublisherAdapter, InMemoryLifecycleEventPublisher]:
    """Construct an adapter wrapping a fresh in-memory inner publisher."""
    inner = InMemoryLifecycleEventPublisher()
    adapter = LifecycleEventPublisherAdapter(inner, max_seen_keys=max_seen_keys)
    return adapter, inner


# ---------------------------------------------------------------------------
# Locked taxonomy
# ---------------------------------------------------------------------------


class TestLockedStepEventKinds:
    def test_is_a_frozenset(self) -> None:
        assert isinstance(LOCKED_STEP_EVENT_KINDS, frozenset)

    def test_contains_exactly_the_six_documented_kinds(self) -> None:
        assert {
            "step.started",
            "step.completed",
            "step.failed",
            "step.skipped",
            "step.waiting",
            "step.retry_scheduled",
        } == LOCKED_STEP_EVENT_KINDS

    def test_kind_constants_match_locked_set(self) -> None:
        # Each LIFECYCLE_KIND_STEP_* constant must live inside the
        # locked set; this catches a typo in either side of the pair.
        for kind in (
            LIFECYCLE_KIND_STEP_STARTED,
            LIFECYCLE_KIND_STEP_COMPLETED,
            LIFECYCLE_KIND_STEP_FAILED,
            LIFECYCLE_KIND_STEP_SKIPPED,
            LIFECYCLE_KIND_STEP_WAITING,
            LIFECYCLE_KIND_STEP_RETRY_SCHEDULED,
        ):
            assert kind in LOCKED_STEP_EVENT_KINDS


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


class TestStepLifecyclePublisherProtocol:
    def test_adapter_satisfies_protocol(self) -> None:
        adapter, _ = _adapter()
        assert isinstance(adapter, StepLifecyclePublisher)

    def test_every_locked_kind_has_an_emit_method(self) -> None:
        # The kind ↔ emit-method correspondence is part of the
        # adapter's public contract — if it drifts, downstream
        # callers can't dispatch on kind.
        kind_to_method = {
            "step.started": "emit_step_started",
            "step.completed": "emit_step_completed",
            "step.failed": "emit_step_failed",
            "step.skipped": "emit_step_skipped",
            "step.waiting": "emit_step_waiting",
            "step.retry_scheduled": "emit_step_retry_scheduled",
        }
        assert set(kind_to_method) == set(LOCKED_STEP_EVENT_KINDS)
        for method_name in kind_to_method.values():
            assert callable(getattr(LifecycleEventPublisherAdapter, method_name))
            assert callable(getattr(StepLifecyclePublisher, method_name))


# ---------------------------------------------------------------------------
# Per-kind round-trip + envelope shape
# ---------------------------------------------------------------------------


class TestEmitStepStarted:
    def test_round_trip_through_inner_publisher(self) -> None:
        adapter, inner = _adapter()
        asyncio.run(
            adapter.emit_step_started(
                workspace_id=_WORKSPACE,
                run_id=_RUN_ID,
                workflow_version_id=_WORKFLOW_VERSION_ID,
                step_id="hydrate",
                attempt=1,
                occurred_at=_OCCURRED_AT,
            )
        )
        assert len(inner.events) == 1
        event = inner.events[0]
        assert event.kind == LIFECYCLE_KIND_STEP_STARTED
        assert event.workspace_id == _WORKSPACE
        assert event.run_id == _RUN_ID
        assert event.workflow_version_id == _WORKFLOW_VERSION_ID
        assert event.occurred_at == _OCCURRED_AT
        assert dict(event.extra) == {"step_id": "hydrate", "attempt": 1}

    def test_wire_envelope_matches_design(self) -> None:
        adapter, inner = _adapter()
        asyncio.run(
            adapter.emit_step_started(
                workspace_id=_WORKSPACE,
                run_id=_RUN_ID,
                workflow_version_id=_WORKFLOW_VERSION_ID,
                step_id="hydrate",
                attempt=1,
                occurred_at=_OCCURRED_AT,
            )
        )
        assert inner.events[0].to_wire() == {
            "kind": "step.started",
            "workflowVersionId": _WORKFLOW_VERSION_ID,
            "runId": str(_RUN_ID),
            "workspace": _WORKSPACE,
            "occurredAt": _OCCURRED_AT.isoformat(),
            "stepId": "hydrate",
            "attempt": 1,
        }


class TestEmitStepCompleted:
    def test_round_trip_carries_outputs(self) -> None:
        adapter, inner = _adapter()
        asyncio.run(
            adapter.emit_step_completed(
                workspace_id=_WORKSPACE,
                run_id=_RUN_ID,
                workflow_version_id=_WORKFLOW_VERSION_ID,
                step_id="hydrate",
                attempt=1,
                outputs={"customerId": "c-42"},
                occurred_at=_OCCURRED_AT,
            )
        )
        assert inner.events[0].extra["outputs"] == {"customerId": "c-42"}

    def test_wire_envelope_matches_design(self) -> None:
        adapter, inner = _adapter()
        asyncio.run(
            adapter.emit_step_completed(
                workspace_id=_WORKSPACE,
                run_id=_RUN_ID,
                workflow_version_id=_WORKFLOW_VERSION_ID,
                step_id="hydrate",
                attempt=2,
                outputs={"customerId": "c-42"},
                occurred_at=_OCCURRED_AT,
            )
        )
        assert inner.events[0].to_wire() == {
            "kind": "step.completed",
            "workflowVersionId": _WORKFLOW_VERSION_ID,
            "runId": str(_RUN_ID),
            "workspace": _WORKSPACE,
            "occurredAt": _OCCURRED_AT.isoformat(),
            "stepId": "hydrate",
            "attempt": 2,
            "outputs": {"customerId": "c-42"},
        }

    def test_outputs_are_copied_not_aliased(self) -> None:
        adapter, inner = _adapter()
        outputs: dict[str, Any] = {"customerId": "c-42"}
        asyncio.run(
            adapter.emit_step_completed(
                workspace_id=_WORKSPACE,
                run_id=_RUN_ID,
                workflow_version_id=_WORKFLOW_VERSION_ID,
                step_id="hydrate",
                attempt=1,
                outputs=outputs,
                occurred_at=_OCCURRED_AT,
            )
        )
        outputs["mutated"] = True
        assert "mutated" not in inner.events[0].extra["outputs"]


class TestEmitStepFailed:
    def test_wire_envelope_matches_design(self) -> None:
        adapter, inner = _adapter()
        error_envelope = {
            "code": "RetryBudgetExhausted",
            "codePrefix": "ARM",
            "class": "transient",
        }
        asyncio.run(
            adapter.emit_step_failed(
                workspace_id=_WORKSPACE,
                run_id=_RUN_ID,
                workflow_version_id=_WORKFLOW_VERSION_ID,
                step_id="hydrate",
                attempt=3,
                error=error_envelope,
                occurred_at=_OCCURRED_AT,
            )
        )
        assert inner.events[0].to_wire() == {
            "kind": "step.failed",
            "workflowVersionId": _WORKFLOW_VERSION_ID,
            "runId": str(_RUN_ID),
            "workspace": _WORKSPACE,
            "occurredAt": _OCCURRED_AT.isoformat(),
            "stepId": "hydrate",
            "attempt": 3,
            "error": error_envelope,
        }

    def test_error_is_copied_not_aliased(self) -> None:
        adapter, inner = _adapter()
        error: dict[str, Any] = {"code": "X"}
        asyncio.run(
            adapter.emit_step_failed(
                workspace_id=_WORKSPACE,
                run_id=_RUN_ID,
                workflow_version_id=_WORKFLOW_VERSION_ID,
                step_id="hydrate",
                attempt=1,
                error=error,
                occurred_at=_OCCURRED_AT,
            )
        )
        error["mutated"] = True
        assert "mutated" not in inner.events[0].extra["error"]


class TestEmitStepSkipped:
    def test_wire_envelope_matches_design(self) -> None:
        adapter, inner = _adapter()
        asyncio.run(
            adapter.emit_step_skipped(
                workspace_id=_WORKSPACE,
                run_id=_RUN_ID,
                workflow_version_id=_WORKFLOW_VERSION_ID,
                step_id="hydrate",
                attempt=1,
                reason="on-error: skip",
                occurred_at=_OCCURRED_AT,
            )
        )
        assert inner.events[0].to_wire() == {
            "kind": "step.skipped",
            "workflowVersionId": _WORKFLOW_VERSION_ID,
            "runId": str(_RUN_ID),
            "workspace": _WORKSPACE,
            "occurredAt": _OCCURRED_AT.isoformat(),
            "stepId": "hydrate",
            "attempt": 1,
            "reason": "on-error: skip",
        }


class TestEmitStepWaiting:
    def test_wire_envelope_matches_design(self) -> None:
        adapter, inner = _adapter()
        asyncio.run(
            adapter.emit_step_waiting(
                workspace_id=_WORKSPACE,
                run_id=_RUN_ID,
                workflow_version_id=_WORKFLOW_VERSION_ID,
                step_id="approve",
                attempt=1,
                wait_token="external-event:manager-approval",
                occurred_at=_OCCURRED_AT,
            )
        )
        assert inner.events[0].to_wire() == {
            "kind": "step.waiting",
            "workflowVersionId": _WORKFLOW_VERSION_ID,
            "runId": str(_RUN_ID),
            "workspace": _WORKSPACE,
            "occurredAt": _OCCURRED_AT.isoformat(),
            "stepId": "approve",
            "attempt": 1,
            "waitToken": "external-event:manager-approval",
        }


class TestEmitStepRetryScheduled:
    def test_wire_envelope_packs_retry_block(self) -> None:
        adapter, inner = _adapter()
        decision = RetryNow(delay_seconds=2.5, next_attempt=2)
        envelope = {
            "code": "Throttled",
            "codePrefix": "ARM",
            "class": "transient",
        }
        asyncio.run(
            adapter.emit_step_retry_scheduled(
                workspace_id=_WORKSPACE,
                run_id=_RUN_ID,
                workflow_version_id=_WORKFLOW_VERSION_ID,
                step_id="hydrate",
                decision=decision,
                envelope=envelope,
                occurred_at=_OCCURRED_AT,
            )
        )
        wire = inner.events[0].to_wire()
        assert wire == {
            "kind": "step.retry_scheduled",
            "workflowVersionId": _WORKFLOW_VERSION_ID,
            "runId": str(_RUN_ID),
            "workspace": _WORKSPACE,
            "occurredAt": _OCCURRED_AT.isoformat(),
            "stepId": "hydrate",
            "attempt": 1,  # previous_attempt = next_attempt - 1
            "retry": {
                "nextAttempt": 2,
                "effectiveDelaySeconds": 2.5,
                "action": "retry",
                "previousCode": "Throttled",
                "previousCodePrefix": "ARM",
                "previousClass": "transient",
            },
        }

    def test_delegates_to_build_retry_scheduled_event(self) -> None:
        # Extra should carry WF-IMPL-053's flat audit-correlation
        # keys so the on-disk audit trail (LifecycleEvent.to_dict)
        # stays in lockstep with the direct emit_retry_scheduled
        # path.
        adapter, inner = _adapter()
        asyncio.run(
            adapter.emit_step_retry_scheduled(
                workspace_id=_WORKSPACE,
                run_id=_RUN_ID,
                workflow_version_id=_WORKFLOW_VERSION_ID,
                step_id="hydrate",
                decision=RetryNow(delay_seconds=1.0, next_attempt=2),
                envelope={"code": "X", "codePrefix": "ARM", "class": "transient"},
                occurred_at=_OCCURRED_AT,
            )
        )
        extra = dict(inner.events[0].extra)
        assert extra["step_id"] == "hydrate"
        assert extra["previous_attempt"] == 1
        assert extra["next_attempt"] == 2
        assert extra["effective_delay_seconds"] == 1.0
        assert extra["action"] == "retry"
        assert extra["previous_code"] == "X"


# ---------------------------------------------------------------------------
# Dedup
# ---------------------------------------------------------------------------


class TestDedup:
    def test_replay_of_same_event_is_absorbed(self) -> None:
        adapter, inner = _adapter()

        async def go() -> None:
            for _ in range(3):
                await adapter.emit_step_started(
                    workspace_id=_WORKSPACE,
                    run_id=_RUN_ID,
                    workflow_version_id=_WORKFLOW_VERSION_ID,
                    step_id="hydrate",
                    attempt=1,
                    occurred_at=_OCCURRED_AT,
                )

        asyncio.run(go())
        assert len(inner.events) == 1

    def test_distinct_step_ids_are_not_deduped(self) -> None:
        adapter, inner = _adapter()

        async def go() -> None:
            await adapter.emit_step_started(
                workspace_id=_WORKSPACE,
                run_id=_RUN_ID,
                workflow_version_id=_WORKFLOW_VERSION_ID,
                step_id="hydrate",
                attempt=1,
                occurred_at=_OCCURRED_AT,
            )
            await adapter.emit_step_started(
                workspace_id=_WORKSPACE,
                run_id=_RUN_ID,
                workflow_version_id=_WORKFLOW_VERSION_ID,
                step_id="charge",
                attempt=1,
                occurred_at=_OCCURRED_AT,
            )

        asyncio.run(go())
        assert len(inner.events) == 2

    def test_distinct_attempts_are_not_deduped(self) -> None:
        adapter, inner = _adapter()

        async def go() -> None:
            await adapter.emit_step_started(
                workspace_id=_WORKSPACE,
                run_id=_RUN_ID,
                workflow_version_id=_WORKFLOW_VERSION_ID,
                step_id="hydrate",
                attempt=1,
                occurred_at=_OCCURRED_AT,
            )
            await adapter.emit_step_started(
                workspace_id=_WORKSPACE,
                run_id=_RUN_ID,
                workflow_version_id=_WORKFLOW_VERSION_ID,
                step_id="hydrate",
                attempt=2,
                occurred_at=_OCCURRED_AT,
            )

        asyncio.run(go())
        assert len(inner.events) == 2

    def test_distinct_kinds_are_not_deduped(self) -> None:
        adapter, inner = _adapter()

        async def go() -> None:
            await adapter.emit_step_started(
                workspace_id=_WORKSPACE,
                run_id=_RUN_ID,
                workflow_version_id=_WORKFLOW_VERSION_ID,
                step_id="hydrate",
                attempt=1,
                occurred_at=_OCCURRED_AT,
            )
            await adapter.emit_step_completed(
                workspace_id=_WORKSPACE,
                run_id=_RUN_ID,
                workflow_version_id=_WORKFLOW_VERSION_ID,
                step_id="hydrate",
                attempt=1,
                outputs={},
                occurred_at=_OCCURRED_AT,
            )

        asyncio.run(go())
        assert len(inner.events) == 2

    def test_distinct_runs_are_not_deduped(self) -> None:
        adapter, inner = _adapter()
        other_run: RunId = derive_run_id(_WORKSPACE, "events-key-other")

        async def go() -> None:
            await adapter.emit_step_started(
                workspace_id=_WORKSPACE,
                run_id=_RUN_ID,
                workflow_version_id=_WORKFLOW_VERSION_ID,
                step_id="hydrate",
                attempt=1,
                occurred_at=_OCCURRED_AT,
            )
            await adapter.emit_step_started(
                workspace_id=_WORKSPACE,
                run_id=other_run,
                workflow_version_id=_WORKFLOW_VERSION_ID,
                step_id="hydrate",
                attempt=1,
                occurred_at=_OCCURRED_AT,
            )

        asyncio.run(go())
        assert len(inner.events) == 2

    def test_retry_scheduled_dedup_keyed_on_previous_attempt(self) -> None:
        # The same RetryNow decision firing twice on replay must
        # only publish once; a DIFFERENT decision (different
        # next_attempt) from the same step is a distinct event.
        adapter, inner = _adapter()
        d1 = RetryNow(delay_seconds=1.0, next_attempt=2)
        d2 = RetryNow(delay_seconds=2.0, next_attempt=3)

        async def go() -> None:
            for _ in range(2):
                await adapter.emit_step_retry_scheduled(
                    workspace_id=_WORKSPACE,
                    run_id=_RUN_ID,
                    workflow_version_id=_WORKFLOW_VERSION_ID,
                    step_id="hydrate",
                    decision=d1,
                    envelope={},
                    occurred_at=_OCCURRED_AT,
                )
            await adapter.emit_step_retry_scheduled(
                workspace_id=_WORKSPACE,
                run_id=_RUN_ID,
                workflow_version_id=_WORKFLOW_VERSION_ID,
                step_id="hydrate",
                decision=d2,
                envelope={},
                occurred_at=_OCCURRED_AT,
            )

        asyncio.run(go())
        assert len(inner.events) == 2

    def test_lru_eviction_at_cap(self) -> None:
        # max_seen_keys=2: after publishing keys K1, K2, K3 the
        # oldest (K1) must be evicted, so re-publishing K1 forwards
        # again.
        adapter, inner = _adapter(max_seen_keys=2)

        async def go() -> None:
            for attempt in (1, 2, 3):
                await adapter.emit_step_started(
                    workspace_id=_WORKSPACE,
                    run_id=_RUN_ID,
                    workflow_version_id=_WORKFLOW_VERSION_ID,
                    step_id="hydrate",
                    attempt=attempt,
                    occurred_at=_OCCURRED_AT,
                )
            # Re-publish K1 (attempt=1); should forward because K1
            # was evicted.
            await adapter.emit_step_started(
                workspace_id=_WORKSPACE,
                run_id=_RUN_ID,
                workflow_version_id=_WORKFLOW_VERSION_ID,
                step_id="hydrate",
                attempt=1,
                occurred_at=_OCCURRED_AT,
            )

        asyncio.run(go())
        assert len(inner.events) == 4

    def test_replay_after_cap_hit_still_deduped_within_window(self) -> None:
        # Recently-seen keys remain deduped after touching them.
        adapter, inner = _adapter(max_seen_keys=2)

        async def go() -> None:
            await adapter.emit_step_started(
                workspace_id=_WORKSPACE,
                run_id=_RUN_ID,
                workflow_version_id=_WORKFLOW_VERSION_ID,
                step_id="hydrate",
                attempt=1,
                occurred_at=_OCCURRED_AT,
            )
            await adapter.emit_step_started(
                workspace_id=_WORKSPACE,
                run_id=_RUN_ID,
                workflow_version_id=_WORKFLOW_VERSION_ID,
                step_id="hydrate",
                attempt=2,
                occurred_at=_OCCURRED_AT,
            )
            # Touch attempt=1 again — still in cache (LRU re-orders).
            await adapter.emit_step_started(
                workspace_id=_WORKSPACE,
                run_id=_RUN_ID,
                workflow_version_id=_WORKFLOW_VERSION_ID,
                step_id="hydrate",
                attempt=1,
                occurred_at=_OCCURRED_AT,
            )
            # Now publish attempt=3 — should evict attempt=2 (the
            # least-recently-touched), not attempt=1.
            await adapter.emit_step_started(
                workspace_id=_WORKSPACE,
                run_id=_RUN_ID,
                workflow_version_id=_WORKFLOW_VERSION_ID,
                step_id="hydrate",
                attempt=3,
                occurred_at=_OCCURRED_AT,
            )
            # Re-publish attempt=1: deduped (still in cache).
            await adapter.emit_step_started(
                workspace_id=_WORKSPACE,
                run_id=_RUN_ID,
                workflow_version_id=_WORKFLOW_VERSION_ID,
                step_id="hydrate",
                attempt=1,
                occurred_at=_OCCURRED_AT,
            )
            # Re-publish attempt=2: should forward (evicted).
            await adapter.emit_step_started(
                workspace_id=_WORKSPACE,
                run_id=_RUN_ID,
                workflow_version_id=_WORKFLOW_VERSION_ID,
                step_id="hydrate",
                attempt=2,
                occurred_at=_OCCURRED_AT,
            )

        asyncio.run(go())
        # Forwarded: attempt=1, attempt=2, attempt=3, attempt=2 (re-forwarded) = 4
        assert len(inner.events) == 4

    def test_inner_publish_failure_drops_reservation(self) -> None:
        # A failed inner.publish must NOT permanently mark the key
        # as seen — otherwise transient HTTP errors silently
        # swallow lifecycle events.
        class _ExplodingPublisher:
            def __init__(self) -> None:
                self.calls: int = 0

            async def publish(self, event: LifecycleEvent) -> None:
                self.calls += 1
                if self.calls == 1:
                    raise RuntimeError("transient")

        exploding: LifecycleEventPublisher = _ExplodingPublisher()
        adapter = LifecycleEventPublisherAdapter(exploding)

        async def go() -> None:
            with pytest.raises(RuntimeError, match=r"transient"):
                await adapter.emit_step_started(
                    workspace_id=_WORKSPACE,
                    run_id=_RUN_ID,
                    workflow_version_id=_WORKFLOW_VERSION_ID,
                    step_id="hydrate",
                    attempt=1,
                    occurred_at=_OCCURRED_AT,
                )
            # Retry succeeds: same key, reservation was dropped.
            await adapter.emit_step_started(
                workspace_id=_WORKSPACE,
                run_id=_RUN_ID,
                workflow_version_id=_WORKFLOW_VERSION_ID,
                step_id="hydrate",
                attempt=1,
                occurred_at=_OCCURRED_AT,
            )

        asyncio.run(go())
        assert isinstance(exploding, _ExplodingPublisher)
        assert exploding.calls == 2


# ---------------------------------------------------------------------------
# JSON stability
# ---------------------------------------------------------------------------


class TestJsonStability:
    def test_envelope_is_lexically_orderable(self) -> None:
        adapter, inner = _adapter()
        asyncio.run(
            adapter.emit_step_completed(
                workspace_id=_WORKSPACE,
                run_id=_RUN_ID,
                workflow_version_id=_WORKFLOW_VERSION_ID,
                step_id="hydrate",
                attempt=1,
                outputs={"a": 1, "z": 2},
                occurred_at=_OCCURRED_AT,
            )
        )
        wire = inner.events[0].to_wire()
        # json.dumps with sort_keys must produce a stable byte
        # sequence regardless of how the envelope was constructed.
        canonical = json.dumps(wire, sort_keys=True)
        assert canonical == json.dumps(json.loads(canonical), sort_keys=True)
        # Round-trips: no non-JSON values, no ordering surprises.
        assert json.loads(canonical) == wire
