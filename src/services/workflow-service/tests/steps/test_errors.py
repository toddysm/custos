"""Tests for the Step Coordinator error taxonomy (WF-IMPL-048)."""

from __future__ import annotations

import json

import pytest

from custos_workflow.steps import (
    LOCKED_STEP_KINDS,
    ActivityScheduleError,
    ApprovalTimeoutError,
    ConnectorBindError,
    LoopExpansionError,
    ResumeMirrorPersistError,
    ResumeRegistrationFailedError,
    ResumeSubscriptionDivergentError,
    RetryBudgetExhaustedError,
    StepCoordinatorError,
    StepKindNotImplementedError,
    SubOrchestrationSpawnError,
    SubWorkflowFailedError,
    WithInputResolutionError,
)

# ---------------------------------------------------------------------------
# LOCKED_STEP_KINDS
# ---------------------------------------------------------------------------


def test_locked_step_kinds_is_a_frozenset() -> None:
    """The WF-IMPL-058 OTel counter relies on this being a frozenset
    (it gets used as a closed label set)."""

    assert isinstance(LOCKED_STEP_KINDS, frozenset)


def test_locked_step_kinds_pins_published_strings() -> None:
    """If anyone ever edits a ``KIND`` constant, this test must fail."""

    assert (
        frozenset(
            {
                "step.kind_not_implemented",
                "step.with_input_resolution_error",
                "step.connector_bind_error",
                "step.activity_schedule_error",
                "step.retry_budget_exhausted",
                "step.loop_expansion_error",
                "step.sub_orchestration_spawn_error",
                "step.sub_workflow_failed",
                "step.approval_timeout",
                "step.resume_registration_failed",
                "step.resume_subscription_divergent",
                "step.resume_mirror_persist_error",
            }
        )
        == LOCKED_STEP_KINDS
    )


def test_locked_step_kinds_exhaustively_covers_class_hierarchy() -> None:
    """Every concrete subclass must contribute its KIND to the locked
    set, and the locked set must contain nothing else. This is the
    invariant the WF-IMPL-058 build-time check relies on."""

    subclass_kinds = {cls.KIND for cls in StepCoordinatorError.__subclasses__() if cls.KIND}
    assert subclass_kinds == set(LOCKED_STEP_KINDS)


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------


def test_base_step_coordinator_error_is_abstract() -> None:
    """Direct instantiation must fail; the empty KIND would defeat
    the taxonomy."""

    with pytest.raises(TypeError, match="abstract"):
        StepCoordinatorError("not allowed")


def test_base_subclasses_runtime_error() -> None:
    """Callers using broad ``except RuntimeError:`` must still catch
    every taxonomy error."""

    assert issubclass(StepCoordinatorError, RuntimeError)


# ---------------------------------------------------------------------------
# Concrete subclass identities + builtin parent classes
# ---------------------------------------------------------------------------


def test_step_kind_not_implemented_kind_and_builtin() -> None:
    err = StepKindNotImplementedError(
        "no handler",
        run_id="r-1",
        step_id="s-1",
        attempt=1,
        step_kind="workflow",
        primitive_handler="sub_orchestration",
    )
    assert err.kind == StepKindNotImplementedError.KIND == "step.kind_not_implemented"
    assert isinstance(err, NotImplementedError)
    assert isinstance(err, StepCoordinatorError)
    assert isinstance(err, RuntimeError)


def test_with_input_resolution_error_kind_and_builtin() -> None:
    err = WithInputResolutionError(
        "parse failed",
        run_id="r-1",
        step_id="s-1",
        attempt=1,
        binding_name="payload",
        cause_kind="cel.parse_error",
        source="steps.foo.outputs",
    )
    assert err.kind == WithInputResolutionError.KIND == "step.with_input_resolution_error"
    assert isinstance(err, ValueError)
    assert isinstance(err, StepCoordinatorError)


def test_connector_bind_error_kind_and_builtin() -> None:
    err = ConnectorBindError(
        "bind failed",
        run_id="r-1",
        step_id="s-1",
        attempt=2,
        slot_name="primary",
        connector_ref="conn-x",
        cause="ConnectionRefusedError(...)",
    )
    assert err.kind == ConnectorBindError.KIND == "step.connector_bind_error"
    assert isinstance(err, RuntimeError)


def test_activity_schedule_error_kind_and_builtin() -> None:
    err = ActivityScheduleError(
        "schedule failed",
        run_id="r-1",
        step_id="s-1",
        attempt=1,
        activity_ref="arm/echo",
        cause="DeadlineExceeded",
    )
    assert err.kind == ActivityScheduleError.KIND == "step.activity_schedule_error"
    assert isinstance(err, RuntimeError)


def test_retry_budget_exhausted_kind_and_builtin() -> None:
    err = RetryBudgetExhaustedError(
        "budget exhausted",
        run_id="r-1",
        step_id="s-1",
        attempt=3,
        max_attempts=3,
        last_code="conn.timeout",
        last_code_prefix="conn",
        last_class="retryable",
    )
    assert err.kind == RetryBudgetExhaustedError.KIND == "step.retry_budget_exhausted"
    assert isinstance(err, RuntimeError)


# ---------------------------------------------------------------------------
# to_dict() shape + stability
# ---------------------------------------------------------------------------


def test_step_kind_not_implemented_to_dict_shape() -> None:
    err = StepKindNotImplementedError(
        "no handler",
        run_id="r-1",
        step_id="s-1",
        attempt=1,
        step_kind="workflow",
        primitive_handler="sub_orchestration",
    )
    assert err.to_dict() == {
        "kind": "step.kind_not_implemented",
        "message": "no handler",
        "run_id": "r-1",
        "step_id": "s-1",
        "attempt": 1,
        "step_kind": "workflow",
        "primitive_handler": "sub_orchestration",
    }


def test_with_input_resolution_to_dict_shape() -> None:
    err = WithInputResolutionError(
        "parse failed",
        run_id="r-1",
        step_id="s-1",
        attempt=1,
        binding_name="payload",
        cause_kind="cel.parse_error",
        source="steps.foo.outputs",
    )
    assert err.to_dict() == {
        "kind": "step.with_input_resolution_error",
        "message": "parse failed",
        "run_id": "r-1",
        "step_id": "s-1",
        "attempt": 1,
        "binding_name": "payload",
        "cause_kind": "cel.parse_error",
        "source": "steps.foo.outputs",
    }


def test_connector_bind_to_dict_shape() -> None:
    err = ConnectorBindError(
        "bind failed",
        run_id="r-1",
        step_id="s-1",
        attempt=2,
        slot_name="primary",
        connector_ref="conn-x",
        cause="ConnectionRefusedError(...)",
    )
    assert err.to_dict() == {
        "kind": "step.connector_bind_error",
        "message": "bind failed",
        "run_id": "r-1",
        "step_id": "s-1",
        "attempt": 2,
        "slot_name": "primary",
        "connector_ref": "conn-x",
        "cause": "ConnectionRefusedError(...)",
    }


def test_activity_schedule_to_dict_shape() -> None:
    err = ActivityScheduleError(
        "schedule failed",
        run_id="r-1",
        step_id="s-1",
        attempt=1,
        activity_ref="arm/echo",
        cause="DeadlineExceeded",
    )
    assert err.to_dict() == {
        "kind": "step.activity_schedule_error",
        "message": "schedule failed",
        "run_id": "r-1",
        "step_id": "s-1",
        "attempt": 1,
        "activity_ref": "arm/echo",
        "cause": "DeadlineExceeded",
    }


def test_retry_budget_exhausted_to_dict_shape() -> None:
    err = RetryBudgetExhaustedError(
        "budget exhausted",
        run_id="r-1",
        step_id="s-1",
        attempt=3,
        max_attempts=3,
        last_code="conn.timeout",
        last_code_prefix="conn",
        last_class="retryable",
    )
    assert err.to_dict() == {
        "kind": "step.retry_budget_exhausted",
        "message": "budget exhausted",
        "run_id": "r-1",
        "step_id": "s-1",
        "attempt": 3,
        "max_attempts": 3,
        "last_code": "conn.timeout",
        "last_code_prefix": "conn",
        "last_class": "retryable",
    }


def test_to_dict_round_trips_through_json() -> None:
    """All ``to_dict()`` results must be JSON-safe with no custom encoder."""

    err = WithInputResolutionError(
        "parse failed",
        run_id="r-1",
        step_id="s-1",
        attempt=2,
        binding_name="payload",
        cause_kind="cel.type_error",
    )
    payload = json.dumps(err.to_dict(), sort_keys=False)
    parsed = json.loads(payload)
    assert parsed["kind"] == "step.with_input_resolution_error"
    assert parsed["binding_name"] == "payload"
    assert parsed["cause_kind"] == "cel.type_error"


def test_to_dict_key_order_is_deterministic() -> None:
    """Audit consumers may rely on byte-stable serialization
    without a separate canonicalization step. ``to_dict()`` keeps
    insertion order, so two calls on equal instances must produce
    identical key sequences."""

    a = ConnectorBindError("x", run_id="r", step_id="s", attempt=1, slot_name="p")
    b = ConnectorBindError("x", run_id="r", step_id="s", attempt=1, slot_name="p")
    assert list(a.to_dict().keys()) == list(b.to_dict().keys())


# ---------------------------------------------------------------------------
# Optional-field defaults
# ---------------------------------------------------------------------------


def test_omitted_optional_fields_default_to_none() -> None:
    err = ConnectorBindError("bind failed")
    assert err.run_id is None
    assert err.step_id is None
    assert err.attempt is None
    assert err.slot_name is None
    assert err.connector_ref is None
    assert err.cause is None
    assert err.to_dict()["run_id"] is None
    assert err.to_dict()["attempt"] is None


# ---------------------------------------------------------------------------
# Equality + hashability + repr
# ---------------------------------------------------------------------------


def test_equal_instances_compare_equal_and_hash_identically() -> None:
    a = WithInputResolutionError(
        "boom",
        run_id="r",
        step_id="s",
        attempt=1,
        binding_name="payload",
        cause_kind="cel.parse_error",
    )
    b = WithInputResolutionError(
        "boom",
        run_id="r",
        step_id="s",
        attempt=1,
        binding_name="payload",
        cause_kind="cel.parse_error",
    )
    assert a == b
    assert hash(a) == hash(b)
    assert {a, b} == {a}


def test_different_subclasses_are_never_equal_even_with_same_message() -> None:
    a = ConnectorBindError("boom", run_id="r", step_id="s", attempt=1)
    b = ActivityScheduleError("boom", run_id="r", step_id="s", attempt=1)
    assert a != b


def test_equality_against_non_taxonomy_returns_not_implemented() -> None:
    err = ConnectorBindError("boom")
    assert err.__eq__(object()) is NotImplemented
    assert (err == 42) is False


def test_repr_contains_kind_message_and_extra_fields() -> None:
    err = ActivityScheduleError(
        "schedule failed",
        run_id="r-1",
        step_id="s-1",
        attempt=1,
        activity_ref="arm/echo",
    )
    text = repr(err)
    assert "ActivityScheduleError(" in text
    assert "kind='step.activity_schedule_error'" in text
    assert "activity_ref='arm/echo'" in text
    assert "run_id='r-1'" in text


# ---------------------------------------------------------------------------
# Raise + catch contract
# ---------------------------------------------------------------------------


def test_can_be_raised_and_caught_by_base() -> None:
    with pytest.raises(StepCoordinatorError) as excinfo:
        raise ConnectorBindError("bind failed", slot_name="primary")
    assert excinfo.value.kind == "step.connector_bind_error"
    assert isinstance(excinfo.value, ConnectorBindError)
    assert excinfo.value.slot_name == "primary"


def test_can_be_caught_by_builtin_for_with_input_resolution() -> None:
    """``ValueError`` consumers must catch ``WithInputResolutionError``."""

    with pytest.raises(ValueError):
        raise WithInputResolutionError("bad")


def test_can_be_caught_by_builtin_for_step_kind_not_implemented() -> None:
    """``NotImplementedError`` consumers must catch
    ``StepKindNotImplementedError``."""

    with pytest.raises(NotImplementedError):
        raise StepKindNotImplementedError("no handler", step_kind="workflow")


# ---------------------------------------------------------------------------
# Sub-Orchestration Manager subclasses (WF-IMPL-086)
# ---------------------------------------------------------------------------


def test_loop_expansion_error_kind_builtin_and_to_dict() -> None:
    err = LoopExpansionError(
        "forEach evaluation failed",
        run_id="r-1",
        step_id="s-1",
        attempt=1,
        cause_kind="cel.evaluation_error",
        source="steps.fetch.outputs.items",
        colliding_key=None,
    )
    assert err.kind == LoopExpansionError.KIND == "step.loop_expansion_error"
    assert isinstance(err, ValueError)
    assert isinstance(err, StepCoordinatorError)
    assert err.to_dict() == {
        "kind": "step.loop_expansion_error",
        "message": "forEach evaluation failed",
        "run_id": "r-1",
        "step_id": "s-1",
        "attempt": 1,
        "cause_kind": "cel.evaluation_error",
        "source": "steps.fetch.outputs.items",
        "colliding_key": None,
    }


def test_loop_expansion_error_collision_records_key() -> None:
    err = LoopExpansionError(
        "duplicate iteration key",
        run_id="r-1",
        step_id="s-1",
        colliding_key="dup",
    )
    assert err.colliding_key == "dup"
    assert err.cause_kind is None
    assert json.loads(json.dumps(err.to_dict()))["colliding_key"] == "dup"


def test_sub_orchestration_spawn_error_kind_builtin_and_to_dict() -> None:
    err = SubOrchestrationSpawnError(
        "start_child_workflow rejected",
        run_id="r-1",
        step_id="s-1",
        attempt=1,
        child_instance_id="r-1/s-1/0",
        iteration_key="0",
        cause="RuntimeError(...)",
    )
    assert err.kind == SubOrchestrationSpawnError.KIND == "step.sub_orchestration_spawn_error"
    assert isinstance(err, StepCoordinatorError)
    assert isinstance(err, RuntimeError)
    assert err.to_dict() == {
        "kind": "step.sub_orchestration_spawn_error",
        "message": "start_child_workflow rejected",
        "run_id": "r-1",
        "step_id": "s-1",
        "attempt": 1,
        "child_instance_id": "r-1/s-1/0",
        "iteration_key": "0",
        "cause": "RuntimeError(...)",
    }


def test_sub_workflow_failed_error_kind_builtin_and_to_dict() -> None:
    err = SubWorkflowFailedError(
        "child returned a failure envelope",
        run_id="r-1",
        step_id="s-1",
        attempt=1,
        child_instance_id="r-1/s-1/2",
        iteration_key="2",
        child_kind="step.retry_budget_exhausted",
    )
    assert err.kind == SubWorkflowFailedError.KIND == "step.sub_workflow_failed"
    assert isinstance(err, StepCoordinatorError)
    assert isinstance(err, RuntimeError)
    assert err.to_dict() == {
        "kind": "step.sub_workflow_failed",
        "message": "child returned a failure envelope",
        "run_id": "r-1",
        "step_id": "s-1",
        "attempt": 1,
        "child_instance_id": "r-1/s-1/2",
        "iteration_key": "2",
        "child_kind": "step.retry_budget_exhausted",
    }


def test_approval_timeout_error_kind_builtin_and_to_dict() -> None:
    err = ApprovalTimeoutError(
        "approval gate timed out",
        run_id="r-1",
        step_id="s-1",
        attempt=1,
        child_instance_id="r-1/s-1/approval",
        timeout="PT1H",
    )
    assert err.kind == ApprovalTimeoutError.KIND == "step.approval_timeout"
    assert isinstance(err, StepCoordinatorError)
    assert isinstance(err, RuntimeError)
    assert err.to_dict() == {
        "kind": "step.approval_timeout",
        "message": "approval gate timed out",
        "run_id": "r-1",
        "step_id": "s-1",
        "attempt": 1,
        "child_instance_id": "r-1/s-1/approval",
        "timeout": "PT1H",
    }


# ---------------------------------------------------------------------------
# Resume Subscription Manager subclasses (WF-IMPL-100)
# ---------------------------------------------------------------------------


def test_resume_registration_failed_error_kind_builtin_and_to_dict() -> None:
    err = ResumeRegistrationFailedError(
        "trigger service unreachable",
        run_id="r-1",
        step_id="await-event",
        attempt=1,
        event_key="order.shipped",
        max_retries=5,
        cause="ConnectionRefusedError(...)",
    )
    assert err.kind == ResumeRegistrationFailedError.KIND == "step.resume_registration_failed"
    assert isinstance(err, StepCoordinatorError)
    assert isinstance(err, RuntimeError)
    assert err.to_dict() == {
        "kind": "step.resume_registration_failed",
        "message": "trigger service unreachable",
        "run_id": "r-1",
        "step_id": "await-event",
        "attempt": 1,
        "event_key": "order.shipped",
        "max_retries": 5,
        "cause": "ConnectionRefusedError(...)",
    }


def test_resume_subscription_divergent_error_kind_builtin_and_to_dict() -> None:
    err = ResumeSubscriptionDivergentError(
        "selector diverged on replay",
        run_id="r-1",
        step_id="await-event",
        attempt=1,
        event_key="order.shipped",
        original_selector="order.id == '123'",
        replay_selector="order.id == '456'",
    )
    assert err.kind == ResumeSubscriptionDivergentError.KIND == "step.resume_subscription_divergent"
    assert isinstance(err, StepCoordinatorError)
    assert isinstance(err, RuntimeError)
    assert err.to_dict() == {
        "kind": "step.resume_subscription_divergent",
        "message": "selector diverged on replay",
        "run_id": "r-1",
        "step_id": "await-event",
        "attempt": 1,
        "event_key": "order.shipped",
        "original_selector": "order.id == '123'",
        "replay_selector": "order.id == '456'",
    }


def test_resume_mirror_persist_error_kind_builtin_and_to_dict() -> None:
    err = ResumeMirrorPersistError(
        "metadata store write failed",
        run_id="r-1",
        step_id="await-event",
        attempt=1,
        event_key="order.shipped",
        cause="TimeoutError(...)",
    )
    assert err.kind == ResumeMirrorPersistError.KIND == "step.resume_mirror_persist_error"
    assert isinstance(err, StepCoordinatorError)
    assert isinstance(err, RuntimeError)
    assert err.to_dict() == {
        "kind": "step.resume_mirror_persist_error",
        "message": "metadata store write failed",
        "run_id": "r-1",
        "step_id": "await-event",
        "attempt": 1,
        "event_key": "order.shipped",
        "cause": "TimeoutError(...)",
    }


def test_sub_orchestration_errors_optional_fields_default_to_none() -> None:
    for err in (
        LoopExpansionError("x"),
        SubOrchestrationSpawnError("x"),
        SubWorkflowFailedError("x"),
        ApprovalTimeoutError("x"),
        ResumeRegistrationFailedError("x"),
        ResumeSubscriptionDivergentError("x"),
        ResumeMirrorPersistError("x"),
    ):
        payload = err.to_dict()
        assert payload["run_id"] is None
        assert payload["step_id"] is None
        assert payload["attempt"] is None
        # JSON-safe with the default encoder.
        assert json.loads(json.dumps(payload))["kind"] == err.kind
