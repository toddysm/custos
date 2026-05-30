"""Tests for the ``ActivityRuntimeClient`` Protocol + envelopes (WF-IMPL-049)."""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any, Literal, get_args

import pytest

from custos_workflow.clients import (
    ACTIVITY_RESULT_CLASSES,
    ActivityResultClass,
    ActivityResultEnvelope,
    ActivityRuntimeClient,
    FakeActivityRuntimeClient,
    NoopActivityRuntimeClient,
    ScheduleActivityRequest,
)
from custos_workflow.steps.idempotency import IdempotencyTripleError

# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------


def _success_envelope(attempt: int = 1) -> ActivityResultEnvelope:
    return ActivityResultEnvelope(
        class_="success",
        outputs=MappingProxyType({"echo": "hi"}),
        error=None,
        attempt=attempt,
    )


def _retryable_envelope(attempt: int = 1) -> ActivityResultEnvelope:
    return ActivityResultEnvelope(
        class_="retryable",
        outputs=None,
        error=MappingProxyType(
            {
                "kind": "conn.timeout",
                "message": "timed out",
                "code": "conn.timeout",
                "codePrefix": "conn",
            }
        ),
        attempt=attempt,
    )


def _request(attempt: int = 1) -> ScheduleActivityRequest:
    return ScheduleActivityRequest(
        run_id="run-1",
        step_id="step-1",
        attempt=attempt,
        activity_ref="arm/echo",
        inputs=MappingProxyType({"message": "hi"}),
        connector_contexts=MappingProxyType({}),
        deadline=datetime(2026, 5, 30, tzinfo=UTC),
    )


# ---------------------------------------------------------------------------
# Outcome class set
# ---------------------------------------------------------------------------


def test_activity_result_class_literal_matches_design() -> None:
    """The Literal alias must match the four design.md outcome classes."""

    assert set(get_args(ActivityResultClass)) == {
        "success",
        "retryable",
        "permanent",
        "cancelled",
    }


def test_activity_result_classes_is_frozenset_mirror() -> None:
    assert isinstance(ACTIVITY_RESULT_CLASSES, frozenset)
    assert frozenset(get_args(ActivityResultClass)) == ACTIVITY_RESULT_CLASSES


# ---------------------------------------------------------------------------
# Protocol runtime check
# ---------------------------------------------------------------------------


def test_activity_runtime_client_is_runtime_checkable() -> None:
    assert isinstance(NoopActivityRuntimeClient(), ActivityRuntimeClient)
    assert isinstance(FakeActivityRuntimeClient(), ActivityRuntimeClient)


def test_objects_missing_methods_do_not_satisfy_protocol() -> None:
    class _NotAClient:
        pass

    assert not isinstance(_NotAClient(), ActivityRuntimeClient)


def test_object_with_only_one_method_does_not_satisfy_protocol() -> None:
    class _OnlyScheduler:
        def schedule_activity(
            self, request: ScheduleActivityRequest
        ) -> ActivityResultEnvelope:  # pragma: no cover - protocol check
            return _success_envelope()

    assert not isinstance(_OnlyScheduler(), ActivityRuntimeClient)


# ---------------------------------------------------------------------------
# ScheduleActivityRequest immutability
# ---------------------------------------------------------------------------


class TestScheduleActivityRequestImmutability:
    def test_round_trip_through_dataclass_replace(self) -> None:
        req = _request(attempt=1)
        replaced = dataclasses.replace(req, attempt=2)
        assert replaced.attempt == 2
        # Original is untouched.
        assert req.attempt == 1
        # Other fields survive byte-equal.
        assert replaced.run_id == req.run_id
        assert replaced.step_id == req.step_id
        assert replaced.activity_ref == req.activity_ref
        assert replaced.inputs == req.inputs
        assert replaced.connector_contexts == req.connector_contexts
        assert replaced.deadline == req.deadline

    def test_attribute_mutation_raises_frozen_instance_error(self) -> None:
        req = _request()
        with pytest.raises(dataclasses.FrozenInstanceError):
            req.attempt = 99  # type: ignore[misc]

    def test_slots_blocks_new_attributes(self) -> None:
        req = _request()
        with pytest.raises((AttributeError, TypeError, dataclasses.FrozenInstanceError)):
            req.new_attr = "nope"  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# ActivityResultEnvelope immutability + shape
# ---------------------------------------------------------------------------


class TestActivityResultEnvelope:
    def test_round_trip_through_dataclass_replace(self) -> None:
        env = _success_envelope(attempt=1)
        replaced = dataclasses.replace(env, attempt=2)
        assert replaced.attempt == 2
        assert env.attempt == 1
        assert replaced.class_ == env.class_
        assert replaced.outputs == env.outputs
        assert replaced.error == env.error

    def test_attribute_mutation_raises_frozen_instance_error(self) -> None:
        env = _success_envelope()
        with pytest.raises(dataclasses.FrozenInstanceError):
            env.attempt = 99  # type: ignore[misc]

    def test_slots_blocks_new_attributes(self) -> None:
        env = _success_envelope()
        with pytest.raises((AttributeError, TypeError, dataclasses.FrozenInstanceError)):
            env.new_attr = "nope"  # type: ignore[attr-defined]

    def test_success_envelope_carries_outputs(self) -> None:
        env = _success_envelope()
        assert env.class_ == "success"
        assert env.outputs is not None
        assert env.error is None

    def test_retryable_envelope_carries_error(self) -> None:
        env = _retryable_envelope()
        assert env.class_ == "retryable"
        assert env.outputs is None
        assert env.error is not None
        assert env.error["kind"] == "conn.timeout"

    def test_permanent_envelope_construct(self) -> None:
        env = ActivityResultEnvelope(
            class_="permanent",
            outputs=None,
            error=MappingProxyType({"kind": "activity.permanent"}),
            attempt=1,
        )
        assert env.class_ == "permanent"

    def test_cancelled_envelope_construct(self) -> None:
        env = ActivityResultEnvelope(
            class_="cancelled",
            outputs=None,
            error=MappingProxyType({"kind": "run.cancelled"}),
            attempt=1,
        )
        assert env.class_ == "cancelled"

    def test_class_field_typed_as_activity_result_class(self) -> None:
        """``mypy --strict`` would reject this at type-check time; at
        runtime we just confirm the annotation is the Literal alias."""

        hints = dataclasses.fields(ActivityResultEnvelope)
        class_field = next(f for f in hints if f.name == "class_")
        # The annotation is stored as the alias name string when
        # ``from __future__ import annotations`` is active.
        assert class_field.type in (
            ActivityResultClass,
            "ActivityResultClass",
            Literal["success", "retryable", "permanent", "cancelled"],
        )


# ---------------------------------------------------------------------------
# Boundary validation — ScheduleActivityRequest
# ---------------------------------------------------------------------------


class TestScheduleActivityRequestValidation:
    """The dataclass must reject malformed idempotency keys at the
    boundary so adapters and tests can never propagate them
    downstream (mirrors WF-IMPL-047 ``IdempotencyTriple`` rules)."""

    def _kwargs(self, **overrides: Any) -> dict[str, Any]:
        base: dict[str, Any] = {
            "run_id": "run-1",
            "step_id": "step-1",
            "attempt": 1,
            "activity_ref": "arm/echo",
            "inputs": MappingProxyType({}),
            "connector_contexts": MappingProxyType({}),
            "deadline": datetime(2026, 5, 30, tzinfo=UTC),
        }
        base.update(overrides)
        return base

    def test_rejects_empty_run_id(self) -> None:
        with pytest.raises(IdempotencyTripleError, match="run_id"):
            ScheduleActivityRequest(**self._kwargs(run_id=""))

    def test_rejects_run_id_with_canonical_separator(self) -> None:
        with pytest.raises(IdempotencyTripleError, match="run_id"):
            ScheduleActivityRequest(**self._kwargs(run_id="bad|id"))

    def test_rejects_empty_step_id(self) -> None:
        with pytest.raises(IdempotencyTripleError, match="step_id"):
            ScheduleActivityRequest(**self._kwargs(step_id=""))

    def test_rejects_step_id_with_canonical_separator(self) -> None:
        with pytest.raises(IdempotencyTripleError, match="step_id"):
            ScheduleActivityRequest(**self._kwargs(step_id="bad|step"))

    def test_rejects_attempt_zero(self) -> None:
        with pytest.raises(ValueError, match=r"attempt must be >= 1"):
            ScheduleActivityRequest(**self._kwargs(attempt=0))

    def test_rejects_attempt_negative(self) -> None:
        with pytest.raises(ValueError, match=r"attempt must be >= 1"):
            ScheduleActivityRequest(**self._kwargs(attempt=-1))

    def test_rejects_attempt_bool(self) -> None:
        with pytest.raises(ValueError, match=r"attempt must be an int"):
            ScheduleActivityRequest(**self._kwargs(attempt=True))

    def test_rejects_empty_activity_ref(self) -> None:
        with pytest.raises(ValueError, match=r"activity_ref must be a non-empty string"):
            ScheduleActivityRequest(**self._kwargs(activity_ref=""))

    def test_accepts_minimal_valid_request(self) -> None:
        # Sanity check that the validation pipeline doesn't reject
        # the canonical happy-path shape used everywhere else in
        # this suite.
        req = ScheduleActivityRequest(**self._kwargs())
        assert req.run_id == "run-1"
        assert req.attempt == 1


# ---------------------------------------------------------------------------
# Boundary validation — ActivityResultEnvelope
# ---------------------------------------------------------------------------


class TestActivityResultEnvelopeValidation:
    """The dataclass must enforce the ``class_`` / outputs / error
    invariant from ``design.md`` § *Activity Result Envelope* and
    reject ``attempt < 1`` so retry/audit consumers never see a
    contradictory envelope."""

    _SUCCESS_OUTPUTS: Mapping[str, Any] = MappingProxyType({"echo": "hi"})
    _ERROR_PAYLOAD: Mapping[str, Any] = MappingProxyType({"kind": "x", "message": "x"})

    def test_rejects_success_without_outputs(self) -> None:
        with pytest.raises(ValueError, match="must carry outputs"):
            ActivityResultEnvelope(class_="success", outputs=None, error=None, attempt=1)

    def test_rejects_success_with_error(self) -> None:
        with pytest.raises(ValueError, match="must not carry error"):
            ActivityResultEnvelope(
                class_="success",
                outputs=self._SUCCESS_OUTPUTS,
                error=self._ERROR_PAYLOAD,
                attempt=1,
            )

    @pytest.mark.parametrize("cls", ["retryable", "permanent", "cancelled"])
    def test_rejects_non_success_without_error(self, cls: ActivityResultClass) -> None:
        with pytest.raises(ValueError, match="must carry error"):
            ActivityResultEnvelope(class_=cls, outputs=None, error=None, attempt=1)

    @pytest.mark.parametrize("cls", ["retryable", "permanent", "cancelled"])
    def test_rejects_non_success_with_outputs(self, cls: ActivityResultClass) -> None:
        with pytest.raises(ValueError, match="must not carry outputs"):
            ActivityResultEnvelope(
                class_=cls,
                outputs=self._SUCCESS_OUTPUTS,
                error=self._ERROR_PAYLOAD,
                attempt=1,
            )

    def test_rejects_attempt_zero(self) -> None:
        with pytest.raises(ValueError, match=r"attempt must be >= 1"):
            ActivityResultEnvelope(
                class_="success",
                outputs=self._SUCCESS_OUTPUTS,
                error=None,
                attempt=0,
            )

    def test_rejects_attempt_negative(self) -> None:
        with pytest.raises(ValueError, match=r"attempt must be >= 1"):
            ActivityResultEnvelope(
                class_="success",
                outputs=self._SUCCESS_OUTPUTS,
                error=None,
                attempt=-1,
            )

    def test_rejects_attempt_bool(self) -> None:
        with pytest.raises(ValueError, match=r"attempt must be an int"):
            ActivityResultEnvelope(
                class_="success",
                outputs=self._SUCCESS_OUTPUTS,
                error=None,
                attempt=True,
            )


# ---------------------------------------------------------------------------
# NoopActivityRuntimeClient — explicit refusal
# ---------------------------------------------------------------------------


class TestNoopActivityRuntimeClient:
    def test_schedule_activity_raises_not_implemented(self) -> None:
        client = NoopActivityRuntimeClient()
        with pytest.raises(NotImplementedError, match="deferred sub-module"):
            client.schedule_activity(_request())

    def test_cancel_activity_raises_not_implemented(self) -> None:
        client = NoopActivityRuntimeClient()
        with pytest.raises(NotImplementedError, match="deferred sub-module"):
            client.cancel_activity("run-1", "step-1")


# ---------------------------------------------------------------------------
# FakeActivityRuntimeClient — canned envelopes
# ---------------------------------------------------------------------------


class TestFakeActivityRuntimeClient:
    def test_returns_canned_envelopes_in_order(self) -> None:
        client = FakeActivityRuntimeClient(
            results=[_retryable_envelope(attempt=1), _success_envelope(attempt=2)]
        )
        first = client.schedule_activity(_request(attempt=1))
        second = client.schedule_activity(_request(attempt=2))
        assert first.class_ == "retryable"
        assert second.class_ == "success"

    def test_records_every_call_in_order(self) -> None:
        client = FakeActivityRuntimeClient(
            results=[_success_envelope(attempt=1), _success_envelope(attempt=2)]
        )
        req1 = _request(attempt=1)
        req2 = _request(attempt=2)
        client.schedule_activity(req1)
        client.schedule_activity(req2)
        assert client.calls == [req1, req2]

    def test_raises_index_error_when_results_exhausted(self) -> None:
        client = FakeActivityRuntimeClient(results=[])
        with pytest.raises(IndexError, match="no more canned envelopes queued"):
            client.schedule_activity(_request())

    def test_records_cancellations(self) -> None:
        client = FakeActivityRuntimeClient()
        client.cancel_activity("run-1", "step-1")
        client.cancel_activity("run-2", "step-2")
        assert client.cancellations == [("run-1", "step-1"), ("run-2", "step-2")]

    def test_default_factories_isolate_instances(self) -> None:
        """Mutable default-factory lists must not be shared across instances."""

        a = FakeActivityRuntimeClient()
        b = FakeActivityRuntimeClient()
        a.cancellations.append(("run-x", "step-x"))
        assert b.cancellations == []
        a.results.append(_success_envelope())
        assert b.results == []


# ---------------------------------------------------------------------------
# Static typing — Literal narrowing
# ---------------------------------------------------------------------------


def test_class_field_accepts_all_literal_values_at_runtime() -> None:
    """Round-trip every Literal value through the dataclass to catch
    any future tightening that would break a published outcome.
    Each outcome class is built with the minimal valid shape its
    invariant requires (success → outputs, others → error)."""

    success_outputs: Mapping[str, Any] = MappingProxyType({"echo": "hi"})
    error_payload: Mapping[str, Any] = MappingProxyType({"kind": "x", "message": "x"})

    for value in get_args(ActivityResultClass):
        outputs = success_outputs if value == "success" else None
        error = None if value == "success" else error_payload
        env = ActivityResultEnvelope(
            class_=value,
            outputs=outputs,
            error=error,
            attempt=1,
        )
        assert env.class_ == value


def _consumer_that_requires_literal(value: ActivityResultClass) -> Any:
    """Forces mypy to confirm the dataclass attribute narrows to the alias."""

    return value


def test_envelope_class_narrows_to_activity_result_class_for_mypy() -> None:
    env = _success_envelope()
    # If ``class_`` were typed ``str`` instead of ``ActivityResultClass``
    # this call would be flagged by ``mypy --strict``.
    assert _consumer_that_requires_literal(env.class_) == "success"
