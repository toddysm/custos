"""WF-IMPL-111 — Resume Subscription Manager end-to-end integration suite.

Where the per-component unit suites
(:mod:`tests.steps.test_resume_handler`,
:mod:`tests.steps.test_resume_reconciler`,
:mod:`tests.steps.test_resume_canceller`,
:mod:`tests.steps.test_resume_sweeper`) each exercise one collaborator
against its own bespoke fakes, this suite wires the **real**
:class:`~custos_workflow.steps.resume.WaitForStepHandler`,
:class:`~custos_workflow.steps.resume.ResumeSubscriptionReplayReconciler`,
:class:`~custos_workflow.steps.resume.ResumeSubscriptionCanceller`, and
:class:`~custos_workflow.steps.resume.ResumeSubscriptionTtlSweeper`
around **one shared**
:class:`~custos_workflow.steps.resume.InMemoryResumeSubscriptionMirrorRepository`
and **one shared**
:class:`~custos_workflow.clients.trigger.FakeTriggerServiceClient`, the
same way ``providers.load_run_components`` shares them in the running
worker. The goal is to pin the cross-component contracts the design's
*Resume Subscription Replay Protocol* depends on:

1. **register → park → resume** — the registration driver parks the run
   on the external event (mirror persisted with the real
   ``ts_subscription_id``, Trigger Service registered once); a later
   replay drives a fresh generator to completion with the delivered
   payload, cancels the subscription, and deletes the mirror row.
2. **replay re-registration is idempotent** — re-running the reconciler
   over the shared repo returns the existing subscription id (no
   duplicate) and leaves the row untouched.
3. **replay divergence** — a selector that re-evaluates to a different
   value on replay keeps the original registration and emits the audit
   event (Replay Protocol rule 2).
4. **register exhaustion** — a wedged Trigger Service exhausts the retry
   budget, fails the step retryably, and leaves only the *pending*
   mirror row for the TTL sweeper to reap.
5. **cancel-run cleanup** — the canceller sweeps every open subscription
   for a run (idempotent Trigger Service cancel + mirror-row delete).
6. **TTL sweep** — the periodic sweeper reaps an expired (orphaned)
   mirror row.

Everything runs against the in-process fakes (no Trigger Service / Dapr
dependency); ``asyncio_mode = "auto"`` wraps each coroutine test.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from typing import Any

from custos_cel import FixedClock, SchemaBindings, parse, type_check

from custos_workflow.clients.trigger import (
    CancelResumeSubscriptionRequest,
    FakeTriggerServiceClient,
    RegisterResumeSubscriptionRequest,
)
from custos_workflow.document import WaitForStep
from custos_workflow.graph import (
    CallSiteKind,
    ExecutionGraph,
    ExecutionNode,
    GraphMetadata,
    PrimitiveHandler,
    StepKind,
    TypedCallSite,
)
from custos_workflow.runs import (
    RunId,
    StepExecutionContext,
    StepFailed,
    StepSucceeded,
)
from custos_workflow.runtime import FakeWorkflowContext
from custos_workflow.steps.resume import (
    InMemoryResumeSubscriptionMirrorRepository,
    ResumeSubscriptionCanceller,
    ResumeSubscriptionMirror,
    ResumeSubscriptionReplayReconciler,
    ResumeSubscriptionTtlSweeper,
    WaitForStepHandler,
    drive_resume_generator,
    drive_resume_registration_to_wait,
)
from custos_workflow.steps.resume.handler import (
    PENDING_TS_SUBSCRIPTION_ID,
    WaitForExternalEventCall,
)

# ---------------------------------------------------------------------------
# Shared fixtures (mirror the per-component unit suites verbatim so the
# integration narrative reads against the same document / context shape)
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
_CLOCK = FixedClock(_NOW)
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


class _RecordingAuditPublisher:
    """Captures every divergence event the reconciler emits."""

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


class _AlwaysFailRegisterTrigger:
    """Trigger double whose register always raises (Trigger Service down)."""

    def __init__(self) -> None:
        self.register_calls: list[RegisterResumeSubscriptionRequest] = []
        self.cancel_calls: list[CancelResumeSubscriptionRequest] = []

    def register_resume_subscription(self, request: RegisterResumeSubscriptionRequest) -> Any:
        self.register_calls.append(request)
        raise RuntimeError("trigger service unreachable")

    def cancel_resume_subscription(self, request: CancelResumeSubscriptionRequest) -> None:
        self.cancel_calls.append(request)


# ---------------------------------------------------------------------------
# Scenario 1 — register → park → resume across one shared repo
# ---------------------------------------------------------------------------


class TestRegisterParkResume:
    async def test_park_then_replay_resumes_and_cleans_up(self) -> None:
        trigger = FakeTriggerServiceClient()
        repo = InMemoryResumeSubscriptionMirrorRepository()
        handler = WaitForStepHandler(repo)
        graph = _graph(_wait_for_node(selector_cel="inputs.sel", ttl="PT2H"))
        ctx = _ctx(inputs={"key": "resume-evt", "sel": "tenant-7"})

        # --- Phase 1: register + park on the external event. ---
        park = await drive_resume_registration_to_wait(
            handler.iter_resume(ctx, graph, "await-event"),
            trigger,
            repo,
        )
        assert isinstance(park, WaitForExternalEventCall)
        assert park.event_key == "resume-evt"

        # The Trigger Service was registered exactly once and the mirror
        # row carries the real (non-pending) subscription id.
        assert len(trigger.register_calls) == 1
        assert trigger.register_calls[0].selector == "tenant-7"
        assert trigger.register_calls[0].ttl == "PT2H"
        open_rows = await repo.list_open("run-1")
        assert len(open_rows) == 1
        registered_id = open_rows[0].ts_subscription_id
        assert registered_id != PENDING_TS_SUBSCRIPTION_ID

        # --- Phase 2: the event arrives; replay drives a fresh
        # generator to completion (idempotent re-register, resume,
        # cancel, delete) against the SAME repo + trigger. ---
        result = await drive_resume_generator(
            handler.iter_resume(ctx, graph, "await-event"),
            trigger,
            repo,
            resume_payload={"approved": True, "by": "alice"},
        )

        assert isinstance(result, StepSucceeded)
        assert dict(result.outputs) == {"approved": True, "by": "alice"}
        # Idempotent re-register on replay reused the same subscription id:
        # the fake only ever minted ONE id across the park + the replay
        # re-registration (``_next_id`` advances once, from 1 to 2).
        assert trigger._next_id == 2
        assert registered_id == "ts-sub-1"
        # The subscription was cancelled and the mirror row deleted.
        assert len(trigger.cancel_calls) == 1
        assert trigger.cancel_calls[0].event_key == "resume-evt"
        assert await repo.list_open("run-1") == ()


# ---------------------------------------------------------------------------
# Scenario 2 + 3 — replay re-registration idempotency + divergence
# ---------------------------------------------------------------------------


class TestReplayReconciliation:
    async def test_replay_reregister_is_idempotent(self) -> None:
        trigger = FakeTriggerServiceClient()
        repo = InMemoryResumeSubscriptionMirrorRepository()
        handler = WaitForStepHandler(repo)
        node = _wait_for_node(selector_cel="inputs.sel")
        graph = _graph(node)
        ctx = _ctx(inputs={"key": "evt-x", "sel": "tenant-7"})

        # Park a subscription so the shared repo holds one open row.
        await drive_resume_registration_to_wait(
            handler.iter_resume(ctx, graph, "await-event"), trigger, repo
        )
        before = await repo.list_open("run-1")
        original_id = before[0].ts_subscription_id

        # Replay: the reconciler re-registers every open mirror. With a
        # matching selector the existing id is returned (no duplicate),
        # no divergence, and the row is untouched.
        audit = _RecordingAuditPublisher()
        reconciler = ResumeSubscriptionReplayReconciler(repo, trigger, audit_publisher=audit)
        report = await reconciler.reconcile(ctx, graph)

        assert len(report.reregistered) == 1
        assert report.divergent == ()
        assert audit.events == []
        # No new subscription was minted on replay (id reused).
        assert trigger._next_id == 2
        after = await repo.list_open("run-1")
        assert len(after) == 1
        assert after[0].ts_subscription_id == original_id

    async def test_replay_divergent_selector_keeps_original_and_audits(self) -> None:
        trigger = FakeTriggerServiceClient()
        repo = InMemoryResumeSubscriptionMirrorRepository()
        handler = WaitForStepHandler(repo)
        # Park with the selector evaluating to "tenant-7".
        park_node = _wait_for_node(selector_cel="inputs.sel")
        await drive_resume_registration_to_wait(
            handler.iter_resume(
                _ctx(inputs={"key": "evt-x", "sel": "tenant-7"}), _graph(park_node), "await-event"
            ),
            trigger,
            repo,
        )
        original = (await repo.list_open("run-1"))[0]
        assert original.selector == "tenant-7"

        # Replay re-evaluates the selector to a DIFFERENT value.
        audit = _RecordingAuditPublisher()
        reconciler = ResumeSubscriptionReplayReconciler(repo, trigger, audit_publisher=audit)
        replay_ctx = _ctx(inputs={"key": "evt-x", "sel": "tenant-99"})
        report = await reconciler.reconcile(replay_ctx, _graph(park_node))

        # Original registration wins; divergence reported + audited.
        assert len(report.divergent) == 1
        assert len(audit.events) == 1
        kept = (await repo.list_open("run-1"))[0]
        assert kept.selector == "tenant-7"


# ---------------------------------------------------------------------------
# Scenario 4 — register exhaustion fails retryably, pending row remains
# ---------------------------------------------------------------------------


class TestRegisterExhaustion:
    async def test_exhaustion_fails_step_and_leaves_pending_row_for_sweep(self) -> None:
        trigger = _AlwaysFailRegisterTrigger()
        repo = InMemoryResumeSubscriptionMirrorRepository()
        handler = WaitForStepHandler(repo, max_register_retries=3)
        graph = _graph(_wait_for_node())
        ctx = _ctx(inputs={"key": "evt-x"})

        park = await drive_resume_registration_to_wait(
            handler.iter_resume(ctx, graph, "await-event"),
            trigger,
            repo,
        )

        assert isinstance(park, StepFailed)
        assert park.envelope["kind"] == "step.resume_registration_failed"
        assert park.envelope["max_retries"] == 3
        assert len(trigger.register_calls) == 3

        # The pending mirror persisted before the (doomed) registration
        # survives as an orphaned row — the TTL sweeper is the safety net
        # that reaps it once it expires.
        open_rows = await repo.list_open("run-1")
        assert len(open_rows) == 1
        assert open_rows[0].ts_subscription_id == PENDING_TS_SUBSCRIPTION_ID


# ---------------------------------------------------------------------------
# Scenario 5 — cancel-run sweeps every open subscription
# ---------------------------------------------------------------------------


class TestCancelRunCleanup:
    async def test_cancel_run_cancels_and_deletes_all_open(self) -> None:
        trigger = FakeTriggerServiceClient()
        repo = InMemoryResumeSubscriptionMirrorRepository()
        handler = WaitForStepHandler(repo)

        # Park two subscriptions for the same run (two waitFor steps).
        for step_id, key in (("await-a", "evt-a"), ("await-b", "evt-b")):
            node = _wait_for_node(step_id=step_id, event_key_cel="inputs.key")
            await drive_resume_registration_to_wait(
                handler.iter_resume(_ctx(inputs={"key": key}), _graph(node), step_id),
                trigger,
                repo,
            )
        assert len(await repo.list_open("run-1")) == 2

        canceller = ResumeSubscriptionCanceller(repo, trigger)
        report = await canceller.cancel_run("run-1")

        assert len(report.cancelled) == 2
        assert len(report.deleted) == 2
        assert report.failed == ()
        assert len(trigger.cancel_calls) == 2
        assert await repo.list_open("run-1") == ()


# ---------------------------------------------------------------------------
# Scenario 6 — TTL sweep reaps an expired (orphaned) mirror
# ---------------------------------------------------------------------------


class TestTtlSweep:
    async def test_sweep_once_reaps_expired_row(self) -> None:
        repo = InMemoryResumeSubscriptionMirrorRepository()
        expired = ResumeSubscriptionMirror(
            mirror_id="rsm-expired",
            run_id="run-1",
            step_id="await-event",
            event_key="evt-x",
            ts_subscription_id="ts-sub-1",
            registered_at=_NOW - timedelta(hours=25),
            expires_at=_NOW - timedelta(hours=1),
        )
        fresh = ResumeSubscriptionMirror(
            mirror_id="rsm-fresh",
            run_id="run-1",
            step_id="await-later",
            event_key="evt-y",
            ts_subscription_id="ts-sub-2",
            registered_at=_NOW,
            expires_at=_NOW + timedelta(hours=24),
        )
        await repo.put(expired)
        await repo.put(fresh)

        sweeper = ResumeSubscriptionTtlSweeper(repo, clock=lambda: _NOW)
        report = await sweeper.sweep_once()

        assert report.deleted == ("rsm-expired",)
        assert report.failed == ()
        remaining = await repo.list_open("run-1")
        assert [m.mirror_id for m in remaining] == ["rsm-fresh"]
