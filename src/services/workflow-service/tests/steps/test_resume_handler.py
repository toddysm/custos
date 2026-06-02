"""Tests for the :class:`WaitForStepHandler` (WF-IMPL-104).

The handler drives a ``waitFor:`` step's full register / wait /
resume / cancel / delete-mirror lifecycle
(``design.md`` § *Operation: Step Resume on External Event*,
REQ-081). Coverage targets every acceptance criterion from #543:

* Happy path: register → wait → resume → cancel → delete mirror.
* Trigger Service unreachable exhausts the retry budget and returns
  a retryable :class:`StepFailed`.
* Replay-safe: no double registration within one logical attempt.

plus the supporting edge cases: ``eventKey`` resolution failures,
mirror-persist failure, the *mirror-before-TS* sequencing rule,
selector / TTL plumbing, non-mapping payload binding, and the
dispatch guards.
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any

import pytest
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
    ResumeSubscriptionMirror,
    WaitForStepHandler,
    drive_resume_generator,
    drive_resume_registration_to_wait,
)
from custos_workflow.steps.resume.handler import (
    DEFAULT_REGISTER_SUB_MAX_RETRIES,
    DEFAULT_RESUME_SUB_TTL,
    PENDING_TS_SUBSCRIPTION_ID,
    PersistMirrorCall,
    WaitForExternalEventCall,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 1, 1, tzinfo=UTC)
_CLOCK = FixedClock(_NOW)
_STRING_KEY_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"key": {"type": "string"}, "sel": {"type": "string"}},
}


def _metadata() -> GraphMetadata:
    return GraphMetadata(
        workflow_name="pipeline",
        workflow_workspace="ws-1",
        document_api_version="custos.dev/v1",
    )


def _call_site(
    cel: str,
    kind: CallSiteKind,
    document_path: str,
    inputs_schema: dict[str, Any],
) -> TypedCallSite:
    return TypedCallSite(
        source=f"${{{{ {cel} }}}}",
        typed_ast=type_check(parse(cel), SchemaBindings(inputs=inputs_schema)),
        kind=kind,
        document_path=document_path,
    )


def _wait_for_node(
    *,
    step_id: str = "await-event",
    event_key_cel: str = "inputs.key",
    selector_cel: str | None = None,
    ttl: str | None = None,
    inputs_schema: dict[str, Any] | None = None,
    include_event_key_site: bool = True,
) -> ExecutionNode:
    schema = inputs_schema if inputs_schema is not None else _STRING_KEY_SCHEMA
    call_sites: dict[str, TypedCallSite] = {}
    if include_event_key_site:
        call_sites["waitFor.eventKey"] = _call_site(
            event_key_cel,
            CallSiteKind.WAIT_FOR_EVENT_KEY,
            "spec.steps[0].waitFor.eventKey",
            schema,
        )
    spec: dict[str, Any] = {"eventKey": f"${{{{ {event_key_cel} }}}}"}
    if selector_cel is not None:
        call_sites["waitFor.selector"] = _call_site(
            selector_cel,
            CallSiteKind.WAIT_FOR_SELECTOR,
            "spec.steps[0].waitFor.selector",
            schema,
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


def _activity_like_node(*, step_id: str = "scan") -> ExecutionNode:
    # A non-WAIT_FOR node reusing a WaitForStep source is impossible
    # (the kind drives dispatch), so build a LET node to exercise the
    # "wrong kind" guard.
    from custos_workflow.document import LetStep

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


def _ctx(
    *,
    inputs: dict[str, Any] | None = None,
    run_id: str = "run-1",
    timer_calls: list[Any] | None = None,
) -> StepExecutionContext:
    wf_ctx = FakeWorkflowContext(instance_id=run_id, now=_NOW)
    if timer_calls is not None:
        real_create = wf_ctx.create_timer

        def _record(fire_at: Any) -> Any:
            timer_calls.append(fire_at)
            return real_create(fire_at)

        wf_ctx.create_timer = _record  # type: ignore[method-assign]
    return StepExecutionContext(
        run_id=RunId(run_id),
        workspace_id="ws-1",
        workflow_version_id="wfv-1",
        inputs=MappingProxyType(dict(inputs or {})),
        workflow_context=wf_ctx,
        outputs=MappingProxyType({}),
        clock=_CLOCK,
    )


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class _AlwaysFailRegisterTrigger:
    """Trigger double whose register always raises (TS unreachable)."""

    def __init__(self) -> None:
        self.register_calls: list[RegisterResumeSubscriptionRequest] = []
        self.cancel_calls: list[CancelResumeSubscriptionRequest] = []

    def register_resume_subscription(self, request: RegisterResumeSubscriptionRequest) -> Any:
        self.register_calls.append(request)
        raise RuntimeError("trigger service unreachable")

    def cancel_resume_subscription(self, request: CancelResumeSubscriptionRequest) -> None:
        self.cancel_calls.append(request)


class _RecordingMirrorRepo(InMemoryResumeSubscriptionMirrorRepository):
    """In-memory repo that records the order of ``put`` calls."""

    def __init__(self) -> None:
        super().__init__()
        self.put_order: list[ResumeSubscriptionMirror] = []

    async def put(self, mirror: ResumeSubscriptionMirror) -> ResumeSubscriptionMirror:
        self.put_order.append(mirror)
        return await super().put(mirror)


class _FailingPutRepo(InMemoryResumeSubscriptionMirrorRepository):
    """In-memory repo whose ``put`` always raises (store down)."""

    async def put(self, mirror: ResumeSubscriptionMirror) -> ResumeSubscriptionMirror:
        raise RuntimeError("metadata store unavailable")


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


def test_construction_rejects_non_positive_retry_ceiling() -> None:
    with pytest.raises(ValueError, match="max_register_retries must be >= 1"):
        WaitForStepHandler(InMemoryResumeSubscriptionMirrorRepository(), max_register_retries=0)


def test_construction_rejects_invalid_default_ttl() -> None:
    with pytest.raises(ValueError):
        WaitForStepHandler(InMemoryResumeSubscriptionMirrorRepository(), default_ttl="nope")


def test_construction_exposes_mirror_repo() -> None:
    repo = InMemoryResumeSubscriptionMirrorRepository()
    handler = WaitForStepHandler(repo)
    assert handler.mirror_repo is repo


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


async def test_happy_path_register_wait_resume_cancel_delete() -> None:
    trigger = FakeTriggerServiceClient()
    repo = InMemoryResumeSubscriptionMirrorRepository()
    handler = WaitForStepHandler(repo)
    node = _wait_for_node(selector_cel="inputs.sel", ttl="PT2H")
    graph = _graph(node)
    ctx = _ctx(inputs={"key": "resume-evt", "sel": "tenant-7"})

    result = await drive_resume_generator(
        handler.iter_resume(ctx, graph, "await-event"),
        trigger,
        repo,
        resume_payload={"approved": True, "by": "alice"},
    )

    assert isinstance(result, StepSucceeded)
    assert dict(result.outputs) == {"approved": True, "by": "alice"}
    # Registered exactly once on the happy path.
    assert len(trigger.register_calls) == 1
    req = trigger.register_calls[0]
    assert req.event_key == "resume-evt"
    assert req.selector == "tenant-7"
    assert req.ttl == "PT2H"
    # Cancelled on resume and the mirror was deleted.
    assert len(trigger.cancel_calls) == 1
    assert trigger.cancel_calls[0].event_key == "resume-evt"
    assert await repo.list_open("run-1") == ()


async def test_happy_path_without_selector_registers_none_selector() -> None:
    trigger = FakeTriggerServiceClient()
    repo = InMemoryResumeSubscriptionMirrorRepository()
    handler = WaitForStepHandler(repo)
    graph = _graph(_wait_for_node())
    ctx = _ctx(inputs={"key": "evt-x"})

    result = await drive_resume_generator(
        handler.iter_resume(ctx, graph, "await-event"),
        trigger,
        repo,
        resume_payload={"ok": 1},
    )

    assert isinstance(result, StepSucceeded)
    assert trigger.register_calls[0].selector is None


async def test_default_ttl_applied_when_step_omits_ttl() -> None:
    trigger = FakeTriggerServiceClient()
    repo = InMemoryResumeSubscriptionMirrorRepository()
    handler = WaitForStepHandler(repo)
    graph = _graph(_wait_for_node())
    ctx = _ctx(inputs={"key": "evt-x"})

    await drive_resume_generator(
        handler.iter_resume(ctx, graph, "await-event"),
        trigger,
        repo,
        resume_payload={},
    )

    assert trigger.register_calls[0].ttl == DEFAULT_RESUME_SUB_TTL == "PT24H"


async def test_non_mapping_payload_bound_under_payload_key() -> None:
    trigger = FakeTriggerServiceClient()
    repo = InMemoryResumeSubscriptionMirrorRepository()
    handler = WaitForStepHandler(repo)
    graph = _graph(_wait_for_node())
    ctx = _ctx(inputs={"key": "evt-x"})

    result = await drive_resume_generator(
        handler.iter_resume(ctx, graph, "await-event"),
        trigger,
        repo,
        resume_payload="just-a-string",
    )

    assert isinstance(result, StepSucceeded)
    assert dict(result.outputs) == {"payload": "just-a-string"}


# ---------------------------------------------------------------------------
# Mirror-before-TS sequencing (Replay Protocol rule 4)
# ---------------------------------------------------------------------------


async def test_mirror_persisted_pending_before_registration() -> None:
    trigger = FakeTriggerServiceClient()
    repo = _RecordingMirrorRepo()
    handler = WaitForStepHandler(repo)
    graph = _graph(_wait_for_node())
    ctx = _ctx(inputs={"key": "evt-x"})

    await drive_resume_registration_to_wait(
        handler.iter_resume(ctx, graph, "await-event"),
        trigger,
        repo,
    )

    # First put is the pending mirror (before the TS register), the
    # second stamps the real subscription id the TS returned.
    assert len(repo.put_order) == 2
    assert repo.put_order[0].ts_subscription_id == PENDING_TS_SUBSCRIPTION_ID
    assert (
        repo.put_order[1].ts_subscription_id
        == trigger.subscriptions[("run-1", "await-event", "evt-x")]
    )
    # Exactly one row survives (deterministic mirror id, upsert).
    open_rows = await repo.list_open("run-1")
    assert len(open_rows) == 1
    assert open_rows[0].ts_subscription_id != PENDING_TS_SUBSCRIPTION_ID


# ---------------------------------------------------------------------------
# Register exhaustion → retryable failure
# ---------------------------------------------------------------------------


async def test_register_unreachable_exhausts_retries_then_fails_retryable() -> None:
    trigger = _AlwaysFailRegisterTrigger()
    repo = InMemoryResumeSubscriptionMirrorRepository()
    handler = WaitForStepHandler(repo, max_register_retries=3)
    graph = _graph(_wait_for_node())
    timer_calls: list[Any] = []
    ctx = _ctx(inputs={"key": "evt-x"}, timer_calls=timer_calls)

    result = await drive_resume_generator(
        handler.iter_resume(ctx, graph, "await-event"),
        trigger,
        repo,
        resume_payload={},
    )

    assert isinstance(result, StepFailed)
    assert result.envelope["kind"] == "step.resume_registration_failed"
    assert result.envelope["max_retries"] == 3
    assert result.envelope["event_key"] == "evt-x"
    assert result.envelope["attempt"] == 3
    # Registered max_register_retries times, with a backoff timer
    # opened between each failed attempt (one fewer than attempts).
    assert len(trigger.register_calls) == 3
    assert len(timer_calls) == 2


async def test_register_exhaustion_uses_default_retry_ceiling() -> None:
    trigger = _AlwaysFailRegisterTrigger()
    repo = InMemoryResumeSubscriptionMirrorRepository()
    handler = WaitForStepHandler(repo)
    graph = _graph(_wait_for_node())
    ctx = _ctx(inputs={"key": "evt-x"})

    result = await drive_resume_generator(
        handler.iter_resume(ctx, graph, "await-event"),
        trigger,
        repo,
        resume_payload={},
    )

    assert isinstance(result, StepFailed)
    assert len(trigger.register_calls) == DEFAULT_REGISTER_SUB_MAX_RETRIES == 5


# ---------------------------------------------------------------------------
# Mirror persist failure
# ---------------------------------------------------------------------------


async def test_mirror_persist_failure_fails_step_and_skips_registration() -> None:
    trigger = FakeTriggerServiceClient()
    repo = _FailingPutRepo()
    handler = WaitForStepHandler(repo)
    graph = _graph(_wait_for_node())
    ctx = _ctx(inputs={"key": "evt-x"})

    result = await drive_resume_generator(
        handler.iter_resume(ctx, graph, "await-event"),
        trigger,
        repo,
        resume_payload={},
    )

    assert isinstance(result, StepFailed)
    assert result.envelope["kind"] == "step.resume_mirror_persist_error"
    assert result.envelope["event_key"] == "evt-x"
    # Registration must NOT be attempted when the mirror write fails.
    assert trigger.register_calls == []


# ---------------------------------------------------------------------------
# Replay safety
# ---------------------------------------------------------------------------


async def test_registration_phase_driver_surfaces_mirror_failure() -> None:
    repo = _FailingPutRepo()
    trigger = FakeTriggerServiceClient()
    handler = WaitForStepHandler(repo)
    graph = _graph(_wait_for_node())
    ctx = _ctx(inputs={"key": "evt-x"})

    result = await drive_resume_registration_to_wait(
        handler.iter_resume(ctx, graph, "await-event"), trigger, repo
    )

    assert isinstance(result, StepFailed)
    assert result.envelope["kind"] == "step.resume_mirror_persist_error"
    assert trigger.register_calls == []


async def test_registration_phase_driver_surfaces_register_exhaustion() -> None:
    repo = InMemoryResumeSubscriptionMirrorRepository()
    trigger = _AlwaysFailRegisterTrigger()
    handler = WaitForStepHandler(repo, max_register_retries=2)
    graph = _graph(_wait_for_node())
    ctx = _ctx(inputs={"key": "evt-x"})

    result = await drive_resume_registration_to_wait(
        handler.iter_resume(ctx, graph, "await-event"), trigger, repo
    )

    assert isinstance(result, StepFailed)
    assert result.envelope["kind"] == "step.resume_registration_failed"
    assert len(trigger.register_calls) == 2


async def test_replay_safe_no_double_register_within_one_logical_attempt() -> None:
    trigger = FakeTriggerServiceClient()
    repo = InMemoryResumeSubscriptionMirrorRepository()
    handler = WaitForStepHandler(repo)
    graph = _graph(_wait_for_node())
    ctx = _ctx(inputs={"key": "evt-x"})

    # First pass parks at the wait. A Dapr replay re-runs the
    # generator from the start up to the same suspend point.
    token1 = await drive_resume_registration_to_wait(
        handler.iter_resume(ctx, graph, "await-event"), trigger, repo
    )
    token2 = await drive_resume_registration_to_wait(
        handler.iter_resume(ctx, graph, "await-event"), trigger, repo
    )

    assert isinstance(token1, WaitForExternalEventCall)
    assert isinstance(token2, WaitForExternalEventCall)
    assert token1.event_key == token2.event_key == "evt-x"
    # Re-registered on replay, but the idempotent TS returns the SAME
    # subscription id and the deterministic mirror id keeps one row.
    assert len(trigger.register_calls) == 2
    assert len(trigger.subscriptions) == 1
    open_rows = await repo.list_open("run-1")
    assert len(open_rows) == 1
    assert (
        open_rows[0].ts_subscription_id == trigger.subscriptions[("run-1", "await-event", "evt-x")]
    )


# ---------------------------------------------------------------------------
# eventKey / selector resolution failures
# ---------------------------------------------------------------------------


async def test_event_key_resolution_error_fails_step() -> None:
    trigger = FakeTriggerServiceClient()
    repo = InMemoryResumeSubscriptionMirrorRepository()
    handler = WaitForStepHandler(repo)
    graph = _graph(_wait_for_node())
    # The typed AST references inputs.key, but the run inputs omit it
    # → a runtime unbound-name CelError on evaluate.
    ctx = _ctx(inputs={})

    result = await drive_resume_generator(
        handler.iter_resume(ctx, graph, "await-event"),
        trigger,
        repo,
        resume_payload={},
    )

    assert isinstance(result, StepFailed)
    assert result.envelope["kind"] == "step.with_input_resolution_error"
    assert result.envelope["cause_kind"] is not None
    assert trigger.register_calls == []


async def test_empty_event_key_fails_step() -> None:
    trigger = FakeTriggerServiceClient()
    repo = InMemoryResumeSubscriptionMirrorRepository()
    handler = WaitForStepHandler(repo)
    graph = _graph(_wait_for_node())
    ctx = _ctx(inputs={"key": ""})

    result = await drive_resume_generator(
        handler.iter_resume(ctx, graph, "await-event"),
        trigger,
        repo,
        resume_payload={},
    )

    assert isinstance(result, StepFailed)
    assert result.envelope["kind"] == "step.with_input_resolution_error"
    assert trigger.register_calls == []


async def test_missing_event_key_call_site_fails_step() -> None:
    trigger = FakeTriggerServiceClient()
    repo = InMemoryResumeSubscriptionMirrorRepository()
    handler = WaitForStepHandler(repo)
    graph = _graph(_wait_for_node(include_event_key_site=False))
    ctx = _ctx(inputs={"key": "evt-x"})

    result = await drive_resume_generator(
        handler.iter_resume(ctx, graph, "await-event"),
        trigger,
        repo,
        resume_payload={},
    )

    assert isinstance(result, StepFailed)
    assert result.envelope["kind"] == "step.with_input_resolution_error"
    assert "missing the TypedAST" in result.envelope["message"]


async def test_call_site_kind_mismatch_fails_step() -> None:
    # A malformed graph: the eventKey slot label carries a call site
    # tagged as the selector kind. The kind guard must reject it.
    repo = InMemoryResumeSubscriptionMirrorRepository()
    trigger = FakeTriggerServiceClient()
    handler = WaitForStepHandler(repo)
    bad_node = ExecutionNode(
        step_id="await-event",
        kind=StepKind.WAIT_FOR,
        primitive_handler=PrimitiveHandler.RESUME_SUBSCRIPTION,
        retry_policy=None,
        on_error_routes=(),
        call_sites={
            "waitFor.eventKey": _call_site(
                "inputs.key",
                CallSiteKind.WAIT_FOR_SELECTOR,  # wrong kind
                "spec.steps[0].waitFor.eventKey",
                _STRING_KEY_SCHEMA,
            )
        },
        step_source=WaitForStep.model_validate(
            {"id": "await-event", "waitFor": {"eventKey": "${{ inputs.key }}"}}
        ),
    )
    ctx = _ctx(inputs={"key": "evt-x"})

    result = await drive_resume_generator(
        handler.iter_resume(ctx, _graph(bad_node), "await-event"),
        trigger,
        repo,
        resume_payload={},
    )

    assert isinstance(result, StepFailed)
    assert result.envelope["kind"] == "step.with_input_resolution_error"
    assert "slot-label collision" in result.envelope["message"]
    assert trigger.register_calls == []


async def test_post_registration_mirror_update_failure_fails_step() -> None:
    # The pre-register persist succeeds; the re-stamp (second put)
    # fails → structured StepFailed, not an uncaught exception.
    class _FailSecondPutRepo(InMemoryResumeSubscriptionMirrorRepository):
        def __init__(self) -> None:
            super().__init__()
            self._puts = 0

        async def put(self, mirror: ResumeSubscriptionMirror) -> ResumeSubscriptionMirror:
            self._puts += 1
            if self._puts == 2:
                raise RuntimeError("store down mid-registration")
            return await super().put(mirror)

    repo = _FailSecondPutRepo()
    trigger = FakeTriggerServiceClient()
    handler = WaitForStepHandler(repo)
    graph = _graph(_wait_for_node())
    ctx = _ctx(inputs={"key": "evt-x"})

    result = await drive_resume_generator(
        handler.iter_resume(ctx, graph, "await-event"),
        trigger,
        repo,
        resume_payload={},
    )

    assert isinstance(result, StepFailed)
    assert result.envelope["kind"] == "step.resume_mirror_persist_error"
    # Registration did happen before the failing re-stamp.
    assert len(trigger.register_calls) == 1


async def test_cleanup_failures_are_best_effort_step_still_succeeds() -> None:
    # Cancel + delete both fail AFTER the payload is received; the
    # step must still succeed (cleanup is best-effort so the workflow
    # cannot wedge on an un-redeliverable event).
    class _FailCleanupTrigger(FakeTriggerServiceClient):
        def cancel_resume_subscription(self, request: CancelResumeSubscriptionRequest) -> None:
            raise RuntimeError("cancel unreachable")

    class _FailDeleteRepo(InMemoryResumeSubscriptionMirrorRepository):
        async def delete(self, mirror_id: str) -> None:
            raise RuntimeError("delete unreachable")

    repo = _FailDeleteRepo()
    trigger = _FailCleanupTrigger()
    handler = WaitForStepHandler(repo)
    graph = _graph(_wait_for_node())
    ctx = _ctx(inputs={"key": "evt-x"})

    result = await drive_resume_generator(
        handler.iter_resume(ctx, graph, "await-event"),
        trigger,
        repo,
        resume_payload={"approved": True},
    )

    assert isinstance(result, StepSucceeded)
    assert dict(result.outputs) == {"approved": True}


# ---------------------------------------------------------------------------
# Dispatch guards
# ---------------------------------------------------------------------------


def test_wrong_kind_raises_not_implemented() -> None:
    repo = InMemoryResumeSubscriptionMirrorRepository()
    handler = WaitForStepHandler(repo)
    graph = _graph(_activity_like_node(step_id="scan"))
    ctx = _ctx()

    gen = handler.iter_resume(ctx, graph, "scan")
    with pytest.raises(NotImplementedError, match=r"only StepKind\.WAIT_FOR is supported"):
        next(gen)


def test_unknown_step_id_raises_key_error() -> None:
    repo = InMemoryResumeSubscriptionMirrorRepository()
    handler = WaitForStepHandler(repo)
    graph = _graph(_wait_for_node())
    ctx = _ctx(inputs={"key": "evt-x"})

    gen = handler.iter_resume(ctx, graph, "does-not-exist")
    with pytest.raises(KeyError):
        next(gen)


# ---------------------------------------------------------------------------
# Driver guards
# ---------------------------------------------------------------------------


async def test_driver_rejects_unknown_token() -> None:
    trigger = FakeTriggerServiceClient()
    repo = InMemoryResumeSubscriptionMirrorRepository()

    def _bad_gen() -> Any:
        yield "not-a-resume-call"
        return StepSucceeded(outputs=MappingProxyType({}))

    with pytest.raises(TypeError, match="unsupported token"):
        await drive_resume_generator(_bad_gen(), trigger, repo, resume_payload={})


def test_resume_call_token_equality() -> None:
    # The frozen tokens are value objects; identical fields compare
    # equal (used by replay-state diffing in WF-IMPL-108).
    assert PersistMirrorCall(
        mirror=ResumeSubscriptionMirror(
            mirror_id="m1",
            run_id="run-1",
            step_id="s1",
            event_key="e1",
            ts_subscription_id="ts-1",
            registered_at=_NOW,
            expires_at=_NOW,
        )
    ) == PersistMirrorCall(
        mirror=ResumeSubscriptionMirror(
            mirror_id="m1",
            run_id="run-1",
            step_id="s1",
            event_key="e1",
            ts_subscription_id="ts-1",
            registered_at=_NOW,
            expires_at=_NOW,
        )
    )
