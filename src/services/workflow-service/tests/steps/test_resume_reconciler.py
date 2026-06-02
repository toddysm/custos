"""Tests for the :class:`ResumeSubscriptionReplayReconciler` (WF-IMPL-105).

The reconciler re-registers a run's open
:class:`~custos_workflow.steps.resume.ResumeSubscriptionMirror` rows on
every orchestrator entry (``design.md`` § *Resume Subscription Replay
Protocol*). Coverage targets every acceptance criterion from #544:

* Re-register of an identical key returns the existing id (no
  duplicate).
* A divergent selector keeps the original + emits the audit event.
* A new id after TTL expiry updates the mirror row.

plus the supporting edges: empty open set, per-mirror failure isolation,
the sync ``on_replay`` bridge (runs the async core + swallows), TTL
fallback when a node is gone, and selector re-evaluation failure.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from typing import Any, cast

import pytest
from custos_cel import FixedClock, SchemaBindings, parse, type_check

from custos_workflow.clients.trigger import (
    FakeTriggerServiceClient,
    RegisterResumeSubscriptionRequest,
    RegisterResumeSubscriptionResponse,
    TriggerServiceClient,
)
from custos_workflow.document import LetStep, WaitForStep
from custos_workflow.graph import (
    CallSiteKind,
    ExecutionGraph,
    ExecutionNode,
    GraphMetadata,
    PrimitiveHandler,
    StepKind,
    TypedCallSite,
)
from custos_workflow.runs import RunId, StepExecutionContext
from custos_workflow.runtime import FakeWorkflowContext
from custos_workflow.steps.resume import (
    InMemoryResumeSubscriptionMirrorRepository,
    NoopResumeSubscriptionAuditPublisher,
    ReplayReconcileReport,
    ResumeSubscriptionAuditPublisher,
    ResumeSubscriptionMirror,
    ResumeSubscriptionReplayReconciler,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 1, 1, tzinfo=UTC)
_CLOCK = FixedClock(_NOW)
_EXPIRES = _NOW + timedelta(hours=24)
_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"key": {"type": "string"}, "sel": {"type": "string"}},
}


def _metadata() -> GraphMetadata:
    return GraphMetadata(
        workflow_name="pipeline",
        workflow_workspace="ws-1",
        document_api_version="custos.dev/v1",
    )


def _call_site(cel: str, kind: CallSiteKind, document_path: str) -> TypedCallSite:
    return TypedCallSite(
        source=f"${{{{ {cel} }}}}",
        typed_ast=type_check(parse(cel), SchemaBindings(inputs=_SCHEMA)),
        kind=kind,
        document_path=document_path,
    )


def _wait_for_node(
    *,
    step_id: str = "await-event",
    event_key_cel: str = "inputs.key",
    selector_cel: str | None = None,
    ttl: str | None = None,
) -> ExecutionNode:
    call_sites: dict[str, TypedCallSite] = {
        "waitFor.eventKey": _call_site(
            event_key_cel,
            CallSiteKind.WAIT_FOR_EVENT_KEY,
            "spec.steps[0].waitFor.eventKey",
        )
    }
    spec: dict[str, Any] = {"eventKey": f"${{{{ {event_key_cel} }}}}"}
    if selector_cel is not None:
        call_sites["waitFor.selector"] = _call_site(
            selector_cel,
            CallSiteKind.WAIT_FOR_SELECTOR,
            "spec.steps[0].waitFor.selector",
        )
        spec["selector"] = f"${{{{ {selector_cel} }}}}"
    if ttl is not None:
        spec["ttl"] = ttl
    return ExecutionNode(
        step_id=step_id,
        kind=StepKind.WAIT_FOR,
        primitive_handler=PrimitiveHandler.RESUME_SUBSCRIPTION,
        retry_policy=None,
        on_error_routes=(),
        call_sites=call_sites,
        step_source=WaitForStep.model_validate({"id": step_id, "waitFor": spec}),
    )


def _let_node(*, step_id: str = "compute") -> ExecutionNode:
    return ExecutionNode(
        step_id=step_id,
        kind=StepKind.LET,
        primitive_handler=PrimitiveHandler.EXPRESSION_INLINE,
        retry_policy=None,
        on_error_routes=(),
        call_sites={},
        step_source=LetStep.model_validate({"id": step_id, "let": {"x": "literal"}}),
    )


def _graph(*nodes: ExecutionNode) -> ExecutionGraph:
    return ExecutionGraph(
        nodes=tuple(nodes),
        edges=(),
        topological_order=tuple(n.step_id for n in nodes),
        metadata=_metadata(),
    )


def _ctx(*, inputs: dict[str, Any] | None = None, run_id: str = "run-1") -> StepExecutionContext:
    return StepExecutionContext(
        run_id=RunId(run_id),
        workspace_id="ws-1",
        workflow_version_id="wfv-1",
        inputs=MappingProxyType(dict(inputs or {})),
        workflow_context=FakeWorkflowContext(instance_id=run_id, now=_NOW),
        outputs=MappingProxyType({}),
        clock=_CLOCK,
    )


def _mirror(
    *,
    step_id: str = "await-event",
    event_key: str = "order-approved",
    ts_subscription_id: str = "ts-sub-1",
    selector: str | None = None,
    run_id: str = "run-1",
    mirror_id: str | None = None,
) -> ResumeSubscriptionMirror:
    return ResumeSubscriptionMirror(
        mirror_id=mirror_id or f"rsm-{step_id}",
        run_id=run_id,
        step_id=step_id,
        event_key=event_key,
        ts_subscription_id=ts_subscription_id,
        registered_at=_NOW,
        expires_at=_EXPIRES,
        selector=selector,
    )


async def _seed(
    repo: InMemoryResumeSubscriptionMirrorRepository, *mirrors: ResumeSubscriptionMirror
) -> None:
    for mirror in mirrors:
        await repo.put(mirror)


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class _RecordingAuditPublisher:
    """Captures every divergence event emitted."""

    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []

    async def emit_resume_subscription_divergent(
        self, *, workspace_id: str, occurred_at: datetime, envelope: Any
    ) -> None:
        self.events.append(
            {
                "workspace_id": workspace_id,
                "occurred_at": occurred_at,
                "envelope": dict(envelope),
            }
        )


class _RaisingAuditPublisher:
    """Audit sink whose publish always fails."""

    async def emit_resume_subscription_divergent(
        self, *, workspace_id: str, occurred_at: datetime, envelope: Any
    ) -> None:
        raise RuntimeError("audit sink down")


class _OneStepFailsTrigger:
    """Trigger double whose register raises for one ``step_id``."""

    def __init__(self, failing_step_id: str) -> None:
        self._failing = failing_step_id
        self._fake = FakeTriggerServiceClient()
        self.register_calls: list[RegisterResumeSubscriptionRequest] = []

    def register_resume_subscription(
        self, request: RegisterResumeSubscriptionRequest
    ) -> RegisterResumeSubscriptionResponse:
        self.register_calls.append(request)
        if request.step_id == self._failing:
            raise RuntimeError("trigger service unreachable")
        return self._fake.register_resume_subscription(request)

    def cancel_resume_subscription(self, request: Any) -> None:  # pragma: no cover
        self._fake.cancel_resume_subscription(request)


class _ExplodingRepo:
    """Mirror repo whose ``list_open`` always raises."""

    async def put(
        self, mirror: ResumeSubscriptionMirror
    ) -> ResumeSubscriptionMirror:  # pragma: no cover
        return mirror

    async def list_open(self, run_id: str) -> tuple[ResumeSubscriptionMirror, ...]:
        raise RuntimeError("store down")

    async def list_open_for_step(
        self, run_id: str, step_id: str
    ) -> tuple[ResumeSubscriptionMirror, ...]:  # pragma: no cover
        return ()

    async def delete(self, mirror_id: str) -> None:  # pragma: no cover
        return None

    async def list_expired(
        self, before: datetime
    ) -> tuple[ResumeSubscriptionMirror, ...]:  # pragma: no cover
        return ()


# ---------------------------------------------------------------------------
# Construction / module surface
# ---------------------------------------------------------------------------


class TestConstruction:
    def test_defaults_to_noop_audit_publisher(self) -> None:
        reconciler = ResumeSubscriptionReplayReconciler(
            InMemoryResumeSubscriptionMirrorRepository(),
            FakeTriggerServiceClient(),
        )
        assert isinstance(reconciler._audit_publisher, NoopResumeSubscriptionAuditPublisher)

    def test_mirror_repo_property_exposes_injected_repo(self) -> None:
        repo = InMemoryResumeSubscriptionMirrorRepository()
        reconciler = ResumeSubscriptionReplayReconciler(repo, FakeTriggerServiceClient())
        assert reconciler.mirror_repo is repo

    def test_noop_publisher_satisfies_protocol_and_drops(self) -> None:
        publisher = NoopResumeSubscriptionAuditPublisher()
        assert isinstance(publisher, ResumeSubscriptionAuditPublisher)
        # Dropping the event returns None without raising.
        result = asyncio.run(
            publisher.emit_resume_subscription_divergent(
                workspace_id="ws-1", occurred_at=_NOW, envelope={}
            )
        )
        assert result is None

    def test_noop_publisher_instances_compare_equal(self) -> None:
        assert NoopResumeSubscriptionAuditPublisher() == NoopResumeSubscriptionAuditPublisher()

    def test_report_defaults_are_empty(self) -> None:
        report = ReplayReconcileReport()
        assert report.reregistered == ()
        assert report.divergent == ()
        assert report.mirror_updated == ()
        assert report.failed == ()


# ---------------------------------------------------------------------------
# reconcile — async core
# ---------------------------------------------------------------------------


class TestReconcileEmpty:
    async def test_no_open_mirrors_is_a_noop(self) -> None:
        trigger = FakeTriggerServiceClient()
        reconciler = ResumeSubscriptionReplayReconciler(
            InMemoryResumeSubscriptionMirrorRepository(), trigger
        )
        report = await reconciler.reconcile(_ctx(), _graph(_wait_for_node()))
        assert report == ReplayReconcileReport()
        assert trigger.register_calls == []


class TestReconcileIdempotent:
    async def test_identical_key_returns_existing_id_no_duplicate(self) -> None:
        # Pre-seed the Trigger Service so the key already maps to an id;
        # a re-registration returns that same id (no duplicate, no
        # mirror update).
        repo = InMemoryResumeSubscriptionMirrorRepository()
        mirror = _mirror(ts_subscription_id="ts-sub-1", selector="region == 'eu'")
        await _seed(repo, mirror)
        trigger = FakeTriggerServiceClient()
        trigger.subscriptions[(mirror.run_id, mirror.step_id, mirror.event_key)] = "ts-sub-1"

        node = _wait_for_node(selector_cel="inputs.sel")
        ctx = _ctx(inputs={"key": "order-approved", "sel": "region == 'eu'"})
        reconciler = ResumeSubscriptionReplayReconciler(repo, trigger)

        report = await reconciler.reconcile(ctx, _graph(node))

        assert report.reregistered == (mirror.mirror_id,)
        assert report.divergent == ()
        assert report.mirror_updated == ()
        # Exactly one register, no duplicate subscription minted.
        assert len(trigger.register_calls) == 1
        assert len(trigger.subscriptions) == 1
        # The re-registration carried the original (matching) selector.
        assert trigger.register_calls[0].selector == "region == 'eu'"
        # Mirror row is untouched.
        stored = await repo.list_open(mirror.run_id)
        assert stored == (mirror,)


class _AsyncTrigger:
    """Trigger double whose ``register_resume_subscription`` is ``async``.

    Mirrors the production ``DaprTriggerServiceClient`` (whose register
    method is a coroutine) so the WF-IMPL-108 wiring path — where the
    async core must ``await`` the call rather than drop an un-awaited
    coroutine — is exercised end-to-end.
    """

    def __init__(self) -> None:
        self._fake = FakeTriggerServiceClient()
        self.register_calls: list[RegisterResumeSubscriptionRequest] = []

    async def register_resume_subscription(
        self, request: RegisterResumeSubscriptionRequest
    ) -> RegisterResumeSubscriptionResponse:
        self.register_calls.append(request)
        return self._fake.register_resume_subscription(request)

    async def cancel_resume_subscription(self, request: Any) -> None:  # pragma: no cover
        self._fake.cancel_resume_subscription(request)


class TestReconcileAsyncTriggerClient:
    async def test_awaits_async_register_and_updates_mirror(self) -> None:
        """An async trigger client is awaited; the minted id reaches the row.

        With the production async ``DaprTriggerServiceClient`` the
        register call returns a coroutine. The reconciler must await it
        so the freshly-minted ``tsSubscriptionId`` is read off the
        resolved response and written back to the mirror — without the
        bridge the coroutine would be dropped and the run silently
        un-reconciled.
        """
        repo = InMemoryResumeSubscriptionMirrorRepository()
        # Pre-seed with the "pending" sentinel so the minted id differs
        # and the mirror-update branch fires off the awaited response.
        mirror = _mirror(ts_subscription_id="pending", selector="region == 'eu'")
        await _seed(repo, mirror)
        trigger = _AsyncTrigger()

        node = _wait_for_node(selector_cel="inputs.sel")
        ctx = _ctx(inputs={"key": "order-approved", "sel": "region == 'eu'"})
        reconciler = ResumeSubscriptionReplayReconciler(
            repo, cast(TriggerServiceClient, trigger)
        )

        report = await reconciler.reconcile(ctx, _graph(node))

        assert report.reregistered == (mirror.mirror_id,)
        assert report.mirror_updated == (mirror.mirror_id,)
        assert len(trigger.register_calls) == 1
        # The awaited response's minted id was persisted to the row.
        stored = await repo.list_open(mirror.run_id)
        assert len(stored) == 1
        assert stored[0].ts_subscription_id != "pending"


class TestReconcileDivergence:
    async def test_divergent_selector_keeps_original_and_emits_audit(self) -> None:
        repo = InMemoryResumeSubscriptionMirrorRepository()
        mirror = _mirror(ts_subscription_id="ts-sub-1", selector="region == 'eu'")
        await _seed(repo, mirror)
        trigger = FakeTriggerServiceClient()
        trigger.subscriptions[(mirror.run_id, mirror.step_id, mirror.event_key)] = "ts-sub-1"
        audit = _RecordingAuditPublisher()

        # Replay evaluates the selector to a *different* value.
        node = _wait_for_node(selector_cel="inputs.sel")
        ctx = _ctx(inputs={"key": "order-approved", "sel": "region == 'us'"})
        reconciler = ResumeSubscriptionReplayReconciler(repo, trigger, audit_publisher=audit)

        report = await reconciler.reconcile(ctx, _graph(node))

        assert report.divergent == (mirror.mirror_id,)
        assert report.reregistered == (mirror.mirror_id,)
        # Original wins — the re-registration used the persisted selector.
        assert trigger.register_calls[0].selector == "region == 'eu'"
        # One audit event with the divergence envelope.
        assert len(audit.events) == 1
        envelope = audit.events[0]["envelope"]
        assert envelope["kind"] == "step.resume_subscription_divergent"
        assert envelope["original_selector"] == "region == 'eu'"
        assert envelope["replay_selector"] == "region == 'us'"
        assert audit.events[0]["workspace_id"] == "ws-1"
        assert audit.events[0]["occurred_at"] == _NOW

    async def test_audit_publish_failure_is_swallowed(self) -> None:
        repo = InMemoryResumeSubscriptionMirrorRepository()
        mirror = _mirror(ts_subscription_id="ts-sub-1", selector="region == 'eu'")
        await _seed(repo, mirror)
        trigger = FakeTriggerServiceClient()
        trigger.subscriptions[(mirror.run_id, mirror.step_id, mirror.event_key)] = "ts-sub-1"

        node = _wait_for_node(selector_cel="inputs.sel")
        ctx = _ctx(inputs={"key": "order-approved", "sel": "region == 'us'"})
        reconciler = ResumeSubscriptionReplayReconciler(
            repo, trigger, audit_publisher=_RaisingAuditPublisher()
        )

        # The flaky audit sink must not fail the reconcile.
        report = await reconciler.reconcile(ctx, _graph(node))
        assert report.divergent == (mirror.mirror_id,)
        assert report.reregistered == (mirror.mirror_id,)

    async def test_selector_reevaluation_failure_keeps_original(self) -> None:
        # A selector that evaluates to an empty string makes the CEL
        # resolver raise; the reconciler keeps the original selector and
        # records no divergence.
        repo = InMemoryResumeSubscriptionMirrorRepository()
        mirror = _mirror(ts_subscription_id="ts-sub-1", selector="region == 'eu'")
        await _seed(repo, mirror)
        trigger = FakeTriggerServiceClient()
        trigger.subscriptions[(mirror.run_id, mirror.step_id, mirror.event_key)] = "ts-sub-1"
        audit = _RecordingAuditPublisher()

        node = _wait_for_node(selector_cel="''")  # resolves to empty string → raises
        ctx = _ctx(inputs={"key": "order-approved"})
        reconciler = ResumeSubscriptionReplayReconciler(repo, trigger, audit_publisher=audit)

        report = await reconciler.reconcile(ctx, _graph(node))
        assert report.divergent == ()
        assert report.reregistered == (mirror.mirror_id,)
        assert audit.events == []
        assert trigger.register_calls[0].selector == "region == 'eu'"


class TestReconcileTtlExpiry:
    async def test_new_id_after_expiry_updates_the_mirror_row(self) -> None:
        # Trigger Service has forgotten the key (TTL GC), so it mints a
        # fresh id. The reconciler points the mirror row at the new id.
        repo = InMemoryResumeSubscriptionMirrorRepository()
        mirror = _mirror(ts_subscription_id="old-id", selector="region == 'eu'")
        await _seed(repo, mirror)
        trigger = FakeTriggerServiceClient()  # empty → mints ts-sub-1

        node = _wait_for_node(selector_cel="inputs.sel")
        ctx = _ctx(inputs={"key": "order-approved", "sel": "region == 'eu'"})
        reconciler = ResumeSubscriptionReplayReconciler(repo, trigger)

        report = await reconciler.reconcile(ctx, _graph(node))

        assert report.mirror_updated == (mirror.mirror_id,)
        assert report.divergent == ()
        stored = await repo.list_open(mirror.run_id)
        assert len(stored) == 1
        assert stored[0].ts_subscription_id == "ts-sub-1"
        # Only the subscription id moved; everything else is preserved.
        assert stored[0].selector == "region == 'eu'"
        assert stored[0].registered_at == mirror.registered_at


class TestReconcileTtlResolution:
    async def test_step_ttl_is_used_when_present(self) -> None:
        repo = InMemoryResumeSubscriptionMirrorRepository()
        mirror = _mirror()
        await _seed(repo, mirror)
        trigger = FakeTriggerServiceClient()
        trigger.subscriptions[(mirror.run_id, mirror.step_id, mirror.event_key)] = (
            mirror.ts_subscription_id
        )

        node = _wait_for_node(ttl="PT1H")
        reconciler = ResumeSubscriptionReplayReconciler(repo, trigger)
        await reconciler.reconcile(_ctx(inputs={"key": "order-approved"}), _graph(node))

        assert trigger.register_calls[0].ttl == "PT1H"

    async def test_missing_node_falls_back_to_default_ttl(self) -> None:
        # A mirror whose step is no longer in the graph is still
        # re-registered (kept alive) using the default TTL.
        repo = InMemoryResumeSubscriptionMirrorRepository()
        mirror = _mirror(step_id="gone")
        await _seed(repo, mirror)
        trigger = FakeTriggerServiceClient()
        trigger.subscriptions[(mirror.run_id, mirror.step_id, mirror.event_key)] = (
            mirror.ts_subscription_id
        )

        reconciler = ResumeSubscriptionReplayReconciler(repo, trigger, default_ttl="PT12H")
        report = await reconciler.reconcile(_ctx(), _graph(_let_node()))

        assert report.reregistered == (mirror.mirror_id,)
        assert report.divergent == ()
        assert trigger.register_calls[0].ttl == "PT12H"

    async def test_non_waitfor_node_falls_back_to_default_ttl(self) -> None:
        # A mirror whose step_id now maps to a non-waitFor node (graph
        # drift) also falls back to the default TTL and skips divergence
        # detection.
        repo = InMemoryResumeSubscriptionMirrorRepository()
        mirror = _mirror(step_id="compute", selector="region == 'eu'")
        await _seed(repo, mirror)
        trigger = FakeTriggerServiceClient()
        trigger.subscriptions[(mirror.run_id, mirror.step_id, mirror.event_key)] = (
            mirror.ts_subscription_id
        )

        reconciler = ResumeSubscriptionReplayReconciler(repo, trigger, default_ttl="PT6H")
        report = await reconciler.reconcile(_ctx(), _graph(_let_node(step_id="compute")))

        assert report.reregistered == (mirror.mirror_id,)
        assert report.divergent == ()
        assert trigger.register_calls[0].ttl == "PT6H"
        assert trigger.register_calls[0].selector == "region == 'eu'"


class TestReconcileFailureIsolation:
    async def test_one_mirror_failure_does_not_block_the_others(self) -> None:
        repo = InMemoryResumeSubscriptionMirrorRepository()
        ok_a = _mirror(step_id="await-a", event_key="evt-a", mirror_id="rsm-a")
        bad = _mirror(step_id="await-bad", event_key="evt-bad", mirror_id="rsm-bad")
        ok_c = _mirror(step_id="await-c", event_key="evt-c", mirror_id="rsm-c")
        await _seed(repo, ok_a, bad, ok_c)
        trigger = _OneStepFailsTrigger("await-bad")

        graph = _graph(
            _wait_for_node(step_id="await-a"),
            _wait_for_node(step_id="await-bad"),
            _wait_for_node(step_id="await-c"),
        )
        reconciler = ResumeSubscriptionReplayReconciler(repo, trigger)
        report = await reconciler.reconcile(_ctx(inputs={"key": "k"}), graph)

        assert report.failed == ("rsm-bad",)
        assert set(report.reregistered) == {"rsm-a", "rsm-c"}


# ---------------------------------------------------------------------------
# on_replay — sync bridge
# ---------------------------------------------------------------------------


class TestOnReplaySyncBridge:
    def test_on_replay_runs_the_async_core(self) -> None:
        repo = InMemoryResumeSubscriptionMirrorRepository()
        mirror = _mirror(ts_subscription_id="old-id")
        asyncio.run(_seed(repo, mirror))
        trigger = FakeTriggerServiceClient()  # mints a fresh id → mirror updated

        reconciler = ResumeSubscriptionReplayReconciler(repo, trigger)
        # Sync call (no running loop) — the bridge drives reconcile.
        reconciler.on_replay(_ctx(inputs={"key": "order-approved"}), _graph(_wait_for_node()))

        assert len(trigger.register_calls) == 1
        stored = asyncio.run(repo.list_open("run-1"))
        assert stored[0].ts_subscription_id == "ts-sub-1"

    def test_on_replay_swallows_reconcile_failure(self) -> None:
        reconciler = ResumeSubscriptionReplayReconciler(
            _ExplodingRepo(), FakeTriggerServiceClient()
        )
        # list_open raises → reconcile raises → on_replay swallows it.
        reconciler.on_replay(_ctx(), _graph(_wait_for_node()))

    async def test_reconcile_propagates_list_open_failure(self) -> None:
        # The async core itself does not swallow an infrastructure
        # failure — on_replay is the no-raise boundary.
        reconciler = ResumeSubscriptionReplayReconciler(
            _ExplodingRepo(), FakeTriggerServiceClient()
        )
        with pytest.raises(RuntimeError, match="store down"):
            await reconciler.reconcile(_ctx(), _graph(_wait_for_node()))
