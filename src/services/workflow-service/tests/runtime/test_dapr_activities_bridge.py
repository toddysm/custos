"""WF-IMPL-079 — Dapr activity registration + outbound RPC bridge tests.

Covers the activity-task surface registered on the Dapr worker for
the ARM + Connector outbound RPCs:

* Serializer round-trips for every wire shape
  (:class:`ScheduleActivityRequest`,
  :class:`ActivityResultEnvelope`, :class:`BindForStepRequest`,
  :class:`BindForStepResponse`).
* :class:`OutboundRpcError` round-trips through the activity-task
  envelope without losing class / kind / detail / status_code /
  cause information (the locked acceptance criterion).
* :class:`build_arm_schedule_activity` /
  :class:`build_connector_bind_for_step_activity` activity
  factories bridge both sync and async injected clients.
* :class:`WorkflowRuntime.start` registers the two bridge
  activities when both clients are supplied; the
  :attr:`registered_activities` introspection surface lists both.
* :class:`FakeWorkflowRuntime` mirrors the same registration so
  in-process tests see the same activity surface as production.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any
from unittest.mock import MagicMock

import pytest

from custos_workflow.clients._errors import (
    LOCKED_OUTBOUND_RPC_KINDS,
    MAX_CAUSE_DEPTH,
    OutboundRpcCancelledError,
    OutboundRpcDecodeError,
    OutboundRpcStatusError,
    OutboundRpcTransportError,
)
from custos_workflow.clients.activity_runtime import (
    ActivityResultEnvelope,
    FakeActivityRuntimeClient,
    ScheduleActivityRequest,
)
from custos_workflow.clients.connector import (
    BindForStepRequest,
    BindForStepResponse,
    ConnectorContext,
    FakeConnectorClient,
    SlotSpec,
)
from custos_workflow.runtime import (
    FakeWorkflowRuntime,
    WorkflowRuntime,
)
from custos_workflow.runtime.dapr_activities import (
    BIND_FOR_STEP_ACTIVITY_NAME,
    SCHEDULE_ACTIVITY_ACTIVITY_NAME,
    build_arm_schedule_activity,
    build_connector_bind_for_step_activity,
    parse_arm_schedule_activity_result,
    parse_connector_bind_for_step_result,
    serialize_bind_for_step_request,
    serialize_schedule_activity_request,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


_EXPIRES = datetime(2026, 5, 17, 12, 0, tzinfo=UTC)
_DEADLINE = datetime(2026, 5, 17, 12, 5, tzinfo=UTC)


def _make_connector_context(
    *, slot_name: str = "registry", handle: str = "ctx-abc"
) -> ConnectorContext:
    return ConnectorContext(
        slot_name=slot_name,
        handle=handle,
        expires_at=_EXPIRES,
        connector_kind="oci",
    )


def _make_bind_request(
    *, step_key: str = "s1", capabilities: tuple[str, ...] = ("oci.pull", "oci.inspect")
) -> BindForStepRequest:
    return BindForStepRequest(
        step_key=step_key,
        slots=(SlotSpec(name="registry", connector_ref="oci/main", capabilities=capabilities),),
    )


def _make_bind_response(*, slot_name: str = "registry") -> BindForStepResponse:
    return BindForStepResponse(
        contexts=MappingProxyType(
            {slot_name: _make_connector_context(slot_name=slot_name)},
        )
    )


def _make_schedule_request(*, attempt: int = 1) -> ScheduleActivityRequest:
    return ScheduleActivityRequest(
        run_id="run-1",
        step_id="step-7",
        attempt=attempt,
        activity_ref="docker.scan@v1",
        inputs=MappingProxyType({"image": "alpine:3.20"}),
        connector_contexts=MappingProxyType({"registry": _make_connector_context()}),
        deadline=_DEADLINE,
    )


def _make_envelope(class_: str = "success") -> ActivityResultEnvelope:
    if class_ == "success":
        return ActivityResultEnvelope(
            class_="success",
            outputs=MappingProxyType({"digest": "sha256:abc"}),
            error=None,
            attempt=1,
        )
    return ActivityResultEnvelope(
        class_="permanent",
        outputs=None,
        error=MappingProxyType({"code": "X", "message": "boom"}),
        attempt=2,
    )


# ---------------------------------------------------------------------------
# Bind request / response wire shape
# ---------------------------------------------------------------------------


class TestBindForStepWire:
    def test_request_serialization_preserves_capability_order(self) -> None:
        req = _make_bind_request(capabilities=("cap.b", "cap.a", "cap.c"))
        wire = serialize_bind_for_step_request(req)
        assert wire == {
            "stepKey": "s1",
            "slots": [
                {
                    "name": "registry",
                    "connectorRef": "oci/main",
                    "capabilities": ["cap.b", "cap.a", "cap.c"],
                },
            ],
        }

    def test_request_round_trip(self) -> None:
        from custos_workflow.runtime.dapr_activities import (
            _deserialize_bind_for_step_request,
        )

        req = _make_bind_request()
        wire = serialize_bind_for_step_request(req)
        restored = _deserialize_bind_for_step_request(wire)
        assert restored == req

    def test_response_round_trip(self) -> None:
        from custos_workflow.runtime.dapr_activities import (
            _deserialize_bind_for_step_response,
            _serialize_bind_for_step_response,
        )

        resp = _make_bind_response()
        wire = _serialize_bind_for_step_response(resp)
        restored = _deserialize_bind_for_step_response(wire)
        assert dict(restored.contexts) == dict(resp.contexts)

    @pytest.mark.parametrize("missing", ["stepKey", "slots"])
    def test_request_missing_required_field_rejected(self, missing: str) -> None:
        from custos_workflow.runtime.dapr_activities import (
            _deserialize_bind_for_step_request,
        )

        wire = serialize_bind_for_step_request(_make_bind_request())
        wire.pop(missing)
        with pytest.raises(ValueError, match=f"required field.*{missing}"):
            _deserialize_bind_for_step_request(wire)

    def test_request_slots_must_be_array(self) -> None:
        from custos_workflow.runtime.dapr_activities import (
            _deserialize_bind_for_step_request,
        )

        with pytest.raises(ValueError, match="slots must be a JSON array"):
            _deserialize_bind_for_step_request({"stepKey": "s1", "slots": {}})

    def test_request_slot_must_be_object(self) -> None:
        from custos_workflow.runtime.dapr_activities import (
            _deserialize_bind_for_step_request,
        )

        with pytest.raises(ValueError, match=r"slots\[\*\].*JSON object"):
            _deserialize_bind_for_step_request({"stepKey": "s1", "slots": ["not-a-dict"]})

    def test_slot_spec_capabilities_non_string_rejected(self) -> None:
        from custos_workflow.runtime.dapr_activities import (
            _deserialize_bind_for_step_request,
        )

        with pytest.raises(ValueError, match="capabilities entries"):
            _deserialize_bind_for_step_request(
                {
                    "stepKey": "s1",
                    "slots": [{"name": "n", "connectorRef": "r", "capabilities": [1]}],
                }
            )

    def test_slot_spec_capabilities_must_be_array(self) -> None:
        from custos_workflow.runtime.dapr_activities import (
            _deserialize_bind_for_step_request,
        )

        with pytest.raises(ValueError, match="capabilities must be a JSON array"):
            _deserialize_bind_for_step_request(
                {
                    "stepKey": "s1",
                    "slots": [{"name": "n", "connectorRef": "r", "capabilities": "x"}],
                }
            )

    def test_slot_spec_missing_field_rejected(self) -> None:
        from custos_workflow.runtime.dapr_activities import (
            _deserialize_bind_for_step_request,
        )

        with pytest.raises(ValueError, match=r"SlotSpec.*missing required field"):
            _deserialize_bind_for_step_request({"stepKey": "s1", "slots": [{"name": "n"}]})

    def test_response_non_object_payload_rejected(self) -> None:
        from custos_workflow.runtime.dapr_activities import (
            _deserialize_bind_for_step_response,
        )

        with pytest.raises(ValueError, match="must be a JSON object"):
            _deserialize_bind_for_step_response("not-a-dict")  # type: ignore[arg-type]

    def test_response_missing_contexts_field(self) -> None:
        from custos_workflow.runtime.dapr_activities import (
            _deserialize_bind_for_step_response,
        )

        with pytest.raises(ValueError, match="missing 'contexts' field"):
            _deserialize_bind_for_step_response({})

    def test_response_contexts_not_mapping(self) -> None:
        from custos_workflow.runtime.dapr_activities import (
            _deserialize_bind_for_step_response,
        )

        with pytest.raises(ValueError, match=r"contexts.*JSON object"):
            _deserialize_bind_for_step_response({"contexts": []})


# ---------------------------------------------------------------------------
# ConnectorContext wire shape
# ---------------------------------------------------------------------------


class TestConnectorContextWire:
    def test_round_trip(self) -> None:
        from custos_workflow.runtime.dapr_activities import (
            _deserialize_connector_context,
            _serialize_connector_context,
        )

        ctx = _make_connector_context()
        wire = _serialize_connector_context(ctx)
        assert wire == {
            "slotName": "registry",
            "handle": "ctx-abc",
            "expiresAt": "2026-05-17T12:00:00Z",
            "connectorKind": "oci",
        }
        assert _deserialize_connector_context(wire) == ctx

    @pytest.mark.parametrize("missing", ["slotName", "handle", "expiresAt", "connectorKind"])
    def test_missing_required_field(self, missing: str) -> None:
        from custos_workflow.runtime.dapr_activities import (
            _deserialize_connector_context,
        )

        wire = {
            "slotName": "registry",
            "handle": "h",
            "expiresAt": "2026-05-17T12:00:00Z",
            "connectorKind": "oci",
        }
        wire.pop(missing)
        with pytest.raises(ValueError, match=f"missing required field.*{missing}"):
            _deserialize_connector_context(wire)

    @pytest.mark.parametrize("field_name", ["slotName", "handle", "connectorKind"])
    def test_non_string_field_rejected(self, field_name: str) -> None:
        from custos_workflow.runtime.dapr_activities import (
            _deserialize_connector_context,
        )

        wire: dict[str, Any] = {
            "slotName": "registry",
            "handle": "h",
            "expiresAt": "2026-05-17T12:00:00Z",
            "connectorKind": "oci",
        }
        wire[field_name] = 42
        with pytest.raises(ValueError, match=f"{field_name} must be a string"):
            _deserialize_connector_context(wire)

    def test_format_iso_utc_rejects_naive(self) -> None:
        from custos_workflow.runtime.dapr_activities import _format_iso_utc

        with pytest.raises(ValueError, match="timezone-aware"):
            _format_iso_utc(datetime(2026, 5, 17, 12, 0))

    def test_parse_iso_utc_rejects_non_string(self) -> None:
        from custos_workflow.runtime.dapr_activities import _parse_iso_utc

        with pytest.raises(ValueError, match="ISO 8601 string"):
            _parse_iso_utc(12345)

    def test_parse_iso_utc_rejects_naive(self) -> None:
        from custos_workflow.runtime.dapr_activities import _parse_iso_utc

        with pytest.raises(ValueError, match="naïve"):
            _parse_iso_utc("2026-05-17T12:00:00")

    def test_parse_iso_utc_accepts_explicit_offset(self) -> None:
        from custos_workflow.runtime.dapr_activities import _parse_iso_utc

        parsed = _parse_iso_utc("2026-05-17T08:00:00-04:00")
        assert parsed.tzinfo is not None


# ---------------------------------------------------------------------------
# ScheduleActivity request / ActivityResultEnvelope wire shape
# ---------------------------------------------------------------------------


class TestScheduleActivityWire:
    def test_request_round_trip(self) -> None:
        from custos_workflow.runtime.dapr_activities import (
            _deserialize_schedule_activity_request,
        )

        req = _make_schedule_request()
        wire = serialize_schedule_activity_request(req)
        # Top-level shape locked.
        assert set(wire) == {
            "runId",
            "stepId",
            "attempt",
            "activityRef",
            "inputs",
            "connectorContexts",
            "deadline",
        }
        restored = _deserialize_schedule_activity_request(wire)
        assert restored == req

    def test_envelope_success_round_trip(self) -> None:
        from custos_workflow.runtime.dapr_activities import (
            _deserialize_activity_result_envelope,
            _serialize_activity_result_envelope,
        )

        env = _make_envelope("success")
        wire = _serialize_activity_result_envelope(env)
        restored = _deserialize_activity_result_envelope(wire)
        assert restored == env

    def test_envelope_failure_round_trip(self) -> None:
        from custos_workflow.runtime.dapr_activities import (
            _deserialize_activity_result_envelope,
            _serialize_activity_result_envelope,
        )

        env = _make_envelope("permanent")
        wire = _serialize_activity_result_envelope(env)
        restored = _deserialize_activity_result_envelope(wire)
        assert restored == env

    def test_request_non_mapping_rejected(self) -> None:
        from custos_workflow.runtime.dapr_activities import (
            _deserialize_schedule_activity_request,
        )

        with pytest.raises(ValueError, match="must be a JSON object"):
            _deserialize_schedule_activity_request("nope")  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        "missing",
        [
            "runId",
            "stepId",
            "attempt",
            "activityRef",
            "inputs",
            "connectorContexts",
            "deadline",
        ],
    )
    def test_request_missing_required_field(self, missing: str) -> None:
        from custos_workflow.runtime.dapr_activities import (
            _deserialize_schedule_activity_request,
        )

        wire = serialize_schedule_activity_request(_make_schedule_request())
        wire.pop(missing)
        with pytest.raises(ValueError, match=f"missing required field.*{missing}"):
            _deserialize_schedule_activity_request(wire)

    def test_request_inputs_must_be_object(self) -> None:
        from custos_workflow.runtime.dapr_activities import (
            _deserialize_schedule_activity_request,
        )

        wire = serialize_schedule_activity_request(_make_schedule_request())
        wire["inputs"] = [1, 2]
        with pytest.raises(ValueError, match="inputs must be a JSON object"):
            _deserialize_schedule_activity_request(wire)

    def test_request_connector_contexts_must_be_object(self) -> None:
        from custos_workflow.runtime.dapr_activities import (
            _deserialize_schedule_activity_request,
        )

        wire = serialize_schedule_activity_request(_make_schedule_request())
        wire["connectorContexts"] = []
        with pytest.raises(ValueError, match="connectorContexts must be a JSON object"):
            _deserialize_schedule_activity_request(wire)

    def test_envelope_non_mapping_rejected(self) -> None:
        from custos_workflow.runtime.dapr_activities import (
            _deserialize_activity_result_envelope,
        )

        with pytest.raises(ValueError, match="must be a JSON object"):
            _deserialize_activity_result_envelope("nope")  # type: ignore[arg-type]

    def test_envelope_class_must_be_string(self) -> None:
        from custos_workflow.runtime.dapr_activities import (
            _deserialize_activity_result_envelope,
        )

        with pytest.raises(ValueError, match="class must be a string"):
            _deserialize_activity_result_envelope(
                {"class": 1, "outputs": None, "error": None, "attempt": 1}
            )

    def test_envelope_outputs_invalid_type(self) -> None:
        from custos_workflow.runtime.dapr_activities import (
            _deserialize_activity_result_envelope,
        )

        with pytest.raises(ValueError, match="outputs must be a JSON object or null"):
            _deserialize_activity_result_envelope(
                {"class": "success", "outputs": "no", "error": None, "attempt": 1}
            )

    def test_envelope_error_invalid_type(self) -> None:
        from custos_workflow.runtime.dapr_activities import (
            _deserialize_activity_result_envelope,
        )

        with pytest.raises(ValueError, match="error must be a JSON object or null"):
            _deserialize_activity_result_envelope(
                {"class": "permanent", "outputs": None, "error": "x", "attempt": 1}
            )

    def test_envelope_missing_fields(self) -> None:
        from custos_workflow.runtime.dapr_activities import (
            _deserialize_activity_result_envelope,
        )

        with pytest.raises(ValueError, match="missing required field"):
            _deserialize_activity_result_envelope({"class": "success"})


# ---------------------------------------------------------------------------
# OutboundRpcError envelope round-trip
# ---------------------------------------------------------------------------


class TestOutboundRpcErrorEnvelope:
    def test_every_locked_kind_has_subclass(self) -> None:
        from custos_workflow.runtime.dapr_activities import _kind_to_subclass

        assert set(_kind_to_subclass()) == LOCKED_OUTBOUND_RPC_KINDS

    def test_transport_round_trip_preserves_cause_chain(self) -> None:
        try:
            try:
                raise ConnectionResetError("conn reset")
            except ConnectionResetError as inner:
                raise TimeoutError("timed out") from inner
        except TimeoutError as exc:
            base = OutboundRpcTransportError("Transport failed")
            base.__cause__ = exc
            from custos_workflow.runtime.dapr_activities import (
                _raise_outbound_rpc_error_from_envelope,
                _serialize_outbound_rpc_error,
            )

            wire = _serialize_outbound_rpc_error(base)
            assert wire["kind"] == OutboundRpcTransportError.kind
            assert wire["detail"] == "Transport failed"
            assert len(wire["cause"]) == 2
            assert wire["cause"][0]["type"] == "TimeoutError"
            assert wire["cause"][1]["type"] == "ConnectionResetError"
            with pytest.raises(OutboundRpcTransportError) as info:
                _raise_outbound_rpc_error_from_envelope(wire)
            rebuilt = info.value
            # Cause chain rebuilt and walkable.
            assert rebuilt.__cause__ is not None
            assert "TimeoutError" in str(rebuilt.__cause__)
            assert rebuilt.__cause__.__cause__ is not None
            assert "ConnectionResetError" in str(rebuilt.__cause__.__cause__)

    def test_status_round_trip_preserves_code_and_status(self) -> None:
        from custos_workflow.runtime.dapr_activities import (
            _raise_outbound_rpc_error_from_envelope,
            _serialize_outbound_rpc_error,
        )

        exc = OutboundRpcStatusError("HTTP 503", status_code=503, code="UPSTREAM_DOWN")
        wire = _serialize_outbound_rpc_error(exc)
        assert wire["statusCode"] == 503
        assert wire["code"] == "UPSTREAM_DOWN"
        with pytest.raises(OutboundRpcStatusError) as info:
            _raise_outbound_rpc_error_from_envelope(wire)
        rebuilt = info.value
        assert rebuilt.status_code == 503
        assert rebuilt.code == "UPSTREAM_DOWN"
        assert rebuilt.detail == "HTTP 503"

    def test_status_without_code_round_trip(self) -> None:
        from custos_workflow.runtime.dapr_activities import (
            _raise_outbound_rpc_error_from_envelope,
            _serialize_outbound_rpc_error,
        )

        exc = OutboundRpcStatusError("HTTP 404", status_code=404)
        wire = _serialize_outbound_rpc_error(exc)
        assert "code" not in wire
        with pytest.raises(OutboundRpcStatusError) as info:
            _raise_outbound_rpc_error_from_envelope(wire)
        assert info.value.code is None

    def test_decode_round_trip(self) -> None:
        from custos_workflow.runtime.dapr_activities import (
            _raise_outbound_rpc_error_from_envelope,
            _serialize_outbound_rpc_error,
        )

        exc = OutboundRpcDecodeError("malformed body")
        wire = _serialize_outbound_rpc_error(exc)
        with pytest.raises(OutboundRpcDecodeError) as info:
            _raise_outbound_rpc_error_from_envelope(wire)
        assert info.value.detail == "malformed body"

    def test_cancelled_round_trip(self) -> None:
        from custos_workflow.runtime.dapr_activities import (
            _raise_outbound_rpc_error_from_envelope,
            _serialize_outbound_rpc_error,
        )

        exc = OutboundRpcCancelledError("client cancelled")
        wire = _serialize_outbound_rpc_error(exc)
        with pytest.raises(OutboundRpcCancelledError):
            _raise_outbound_rpc_error_from_envelope(wire)

    def test_cause_chain_truncated_to_max_depth(self) -> None:
        from custos_workflow.runtime.dapr_activities import _serialize_outbound_rpc_error

        # Build a chain deeper than MAX_CAUSE_DEPTH.
        e: BaseException = ValueError("root")
        for level in range(MAX_CAUSE_DEPTH + 3):
            wrapper = RuntimeError(f"level-{level}")
            wrapper.__cause__ = e
            e = wrapper
        base = OutboundRpcTransportError("top")
        base.__cause__ = e
        wire = _serialize_outbound_rpc_error(base)
        assert len(wire["cause"]) == MAX_CAUSE_DEPTH

    def test_envelope_non_mapping_rejected(self) -> None:
        from custos_workflow.runtime.dapr_activities import (
            _raise_outbound_rpc_error_from_envelope,
        )

        with pytest.raises(OutboundRpcDecodeError, match="must be a JSON object"):
            _raise_outbound_rpc_error_from_envelope("nope")  # type: ignore[arg-type]

    def test_envelope_missing_kind_detail(self) -> None:
        from custos_workflow.runtime.dapr_activities import (
            _raise_outbound_rpc_error_from_envelope,
        )

        with pytest.raises(OutboundRpcDecodeError, match=r"kind.*detail"):
            _raise_outbound_rpc_error_from_envelope({"kind": "x"})

    def test_envelope_unknown_kind_rejected(self) -> None:
        from custos_workflow.runtime.dapr_activities import (
            _raise_outbound_rpc_error_from_envelope,
        )

        with pytest.raises(OutboundRpcDecodeError, match="unknown kind"):
            _raise_outbound_rpc_error_from_envelope({"kind": "bogus.kind", "detail": "msg"})

    def test_envelope_status_missing_status_code(self) -> None:
        from custos_workflow.runtime.dapr_activities import (
            _raise_outbound_rpc_error_from_envelope,
        )

        with pytest.raises(OutboundRpcDecodeError, match="statusCode"):
            _raise_outbound_rpc_error_from_envelope(
                {"kind": OutboundRpcStatusError.kind, "detail": "x"}
            )

    def test_envelope_status_code_must_not_be_bool(self) -> None:
        from custos_workflow.runtime.dapr_activities import (
            _raise_outbound_rpc_error_from_envelope,
        )

        with pytest.raises(OutboundRpcDecodeError, match="statusCode"):
            _raise_outbound_rpc_error_from_envelope(
                {
                    "kind": OutboundRpcStatusError.kind,
                    "detail": "x",
                    "statusCode": True,
                }
            )

    def test_envelope_status_code_not_string_rejected(self) -> None:
        from custos_workflow.runtime.dapr_activities import (
            _raise_outbound_rpc_error_from_envelope,
        )

        with pytest.raises(OutboundRpcDecodeError, match="'code' must be a string"):
            _raise_outbound_rpc_error_from_envelope(
                {
                    "kind": OutboundRpcStatusError.kind,
                    "detail": "x",
                    "statusCode": 500,
                    "code": 42,
                }
            )

    def test_cause_must_be_list(self) -> None:
        from custos_workflow.runtime.dapr_activities import _build_cause_chain

        with pytest.raises(ValueError, match="must be a JSON array"):
            _build_cause_chain({"not": "a list"})

    def test_cause_entries_must_be_mappings(self) -> None:
        from custos_workflow.runtime.dapr_activities import _build_cause_chain

        with pytest.raises(ValueError, match="JSON objects"):
            _build_cause_chain(["not-a-dict"])

    def test_cause_entries_must_have_string_fields(self) -> None:
        from custos_workflow.runtime.dapr_activities import _build_cause_chain

        with pytest.raises(ValueError, match="string 'type' \\+ 'message'"):
            _build_cause_chain([{"type": "T"}])

    def test_build_cause_chain_returns_none_for_empty(self) -> None:
        from custos_workflow.runtime.dapr_activities import _build_cause_chain

        assert _build_cause_chain(None) is None
        assert _build_cause_chain([]) is None


# ---------------------------------------------------------------------------
# Activity factories — sync and async clients
# ---------------------------------------------------------------------------


@dataclass
class _AsyncFakeActivityClient:
    envelopes: list[ActivityResultEnvelope]
    calls: list[ScheduleActivityRequest]

    def __init__(self, envelopes: list[ActivityResultEnvelope]) -> None:
        self.envelopes = envelopes
        self.calls = []

    async def schedule_activity(self, request: ScheduleActivityRequest) -> ActivityResultEnvelope:
        self.calls.append(request)
        return self.envelopes.pop(0)


@dataclass
class _AsyncFakeConnectorClient:
    responses: list[BindForStepResponse]
    calls: list[BindForStepRequest]

    def __init__(self, responses: list[BindForStepResponse]) -> None:
        self.responses = responses
        self.calls = []

    async def bind_for_step(self, request: BindForStepRequest) -> BindForStepResponse:
        self.calls.append(request)
        return self.responses.pop(0)


class TestArmScheduleActivityBridge:
    def test_factory_sets_activity_name(self) -> None:
        fake = FakeActivityRuntimeClient(results=[])
        fn = build_arm_schedule_activity(fake)
        assert fn.__name__ == SCHEDULE_ACTIVITY_ACTIVITY_NAME

    def test_success_round_trip_sync_client(self) -> None:
        env = _make_envelope("success")
        fake = FakeActivityRuntimeClient(results=[env])
        fn = build_arm_schedule_activity(fake)
        req = _make_schedule_request()
        wire = serialize_schedule_activity_request(req)
        envelope = fn(None, wire)
        assert envelope["ok"] is True
        assert envelope["error"] is None
        restored = parse_arm_schedule_activity_result(envelope)
        assert restored == env
        # Client received the deserialized request.
        assert fake.calls == [req]

    def test_success_round_trip_async_client(self) -> None:
        env = _make_envelope("success")
        client = _AsyncFakeActivityClient([env])
        fn = build_arm_schedule_activity(client)  # type: ignore[arg-type]
        wire = serialize_schedule_activity_request(_make_schedule_request())
        envelope = fn(None, wire)
        restored = parse_arm_schedule_activity_result(envelope)
        assert restored == env

    def test_outbound_status_error_round_trip(self) -> None:
        exc = OutboundRpcStatusError("HTTP 502", status_code=502)

        class _RaisingClient:
            def schedule_activity(self, request: ScheduleActivityRequest) -> ActivityResultEnvelope:
                raise exc

        fn = build_arm_schedule_activity(_RaisingClient())  # type: ignore[arg-type]
        wire = serialize_schedule_activity_request(_make_schedule_request())
        envelope = fn(None, wire)
        assert envelope["ok"] is False
        with pytest.raises(OutboundRpcStatusError) as info:
            parse_arm_schedule_activity_result(envelope)
        assert info.value.status_code == 502
        assert info.value.detail == "HTTP 502"

    def test_decode_failure_in_activity_returns_decode_error(self) -> None:
        fake = FakeActivityRuntimeClient(results=[])
        fn = build_arm_schedule_activity(fake)
        envelope = fn(None, {"runId": "r"})  # missing required fields
        assert envelope["ok"] is False
        with pytest.raises(OutboundRpcDecodeError):
            parse_arm_schedule_activity_result(envelope)

    def test_non_mapping_payload_returns_decode_error(self) -> None:
        fake = FakeActivityRuntimeClient(results=[])
        fn = build_arm_schedule_activity(fake)
        envelope = fn(None, "not-a-dict")
        assert envelope["ok"] is False
        with pytest.raises(OutboundRpcDecodeError):
            parse_arm_schedule_activity_result(envelope)


class TestConnectorBindForStepBridge:
    def test_factory_sets_activity_name(self) -> None:
        fake = FakeConnectorClient()
        fn = build_connector_bind_for_step_activity(fake)
        assert fn.__name__ == BIND_FOR_STEP_ACTIVITY_NAME

    def test_success_round_trip_sync_client(self) -> None:
        resp = _make_bind_response()
        fake = FakeConnectorClient(responses=[resp])
        fn = build_connector_bind_for_step_activity(fake)
        req = _make_bind_request()
        wire = serialize_bind_for_step_request(req)
        envelope = fn(None, wire)
        restored = parse_connector_bind_for_step_result(envelope)
        assert dict(restored.contexts) == dict(resp.contexts)
        assert fake.calls == [req]

    def test_success_round_trip_async_client(self) -> None:
        resp = _make_bind_response()
        client = _AsyncFakeConnectorClient([resp])
        fn = build_connector_bind_for_step_activity(client)  # type: ignore[arg-type]
        wire = serialize_bind_for_step_request(_make_bind_request())
        envelope = fn(None, wire)
        restored = parse_connector_bind_for_step_result(envelope)
        assert dict(restored.contexts) == dict(resp.contexts)

    def test_outbound_cancelled_round_trip(self) -> None:
        exc = OutboundRpcCancelledError("cancelled")

        class _RaisingClient:
            def bind_for_step(self, request: BindForStepRequest) -> BindForStepResponse:
                raise exc

        fn = build_connector_bind_for_step_activity(_RaisingClient())
        envelope = fn(None, serialize_bind_for_step_request(_make_bind_request()))
        assert envelope["ok"] is False
        with pytest.raises(OutboundRpcCancelledError):
            parse_connector_bind_for_step_result(envelope)

    def test_decode_failure_in_activity_returns_decode_error(self) -> None:
        fake = FakeConnectorClient()
        fn = build_connector_bind_for_step_activity(fake)
        envelope = fn(None, {"stepKey": "s"})  # missing slots
        assert envelope["ok"] is False
        with pytest.raises(OutboundRpcDecodeError):
            parse_connector_bind_for_step_result(envelope)


# ---------------------------------------------------------------------------
# Result-parser envelope validation
# ---------------------------------------------------------------------------


class TestResultParser:
    def test_non_mapping_envelope_rejected(self) -> None:
        with pytest.raises(OutboundRpcDecodeError, match="must be a JSON object"):
            parse_arm_schedule_activity_result("nope")

    def test_missing_ok_field_rejected(self) -> None:
        with pytest.raises(OutboundRpcDecodeError, match="missing required 'ok'"):
            parse_arm_schedule_activity_result({"result": {}})

    def test_success_result_must_be_mapping(self) -> None:
        with pytest.raises(OutboundRpcDecodeError, match="success payload"):
            parse_arm_schedule_activity_result({"ok": True, "result": "x", "error": None})

    def test_failure_error_must_be_mapping(self) -> None:
        with pytest.raises(OutboundRpcDecodeError, match="failure payload"):
            parse_arm_schedule_activity_result({"ok": False, "result": None, "error": "x"})

    def test_success_payload_malformed_inner_raises_decode(self) -> None:
        # Success branch but the inner ActivityResultEnvelope is malformed.
        with pytest.raises(OutboundRpcDecodeError):
            parse_arm_schedule_activity_result(
                {"ok": True, "result": {"class": "success"}, "error": None}
            )

    def test_bind_success_payload_malformed_inner_raises_decode(self) -> None:
        with pytest.raises(OutboundRpcDecodeError):
            parse_connector_bind_for_step_result(
                {"ok": True, "result": {"contexts": "no"}, "error": None}
            )


# ---------------------------------------------------------------------------
# WorkflowRuntime.start() registers bridge activities + introspection
# ---------------------------------------------------------------------------


class TestWorkflowRuntimeRegistration:
    async def test_start_without_clients_does_not_register_bridges(self) -> None:
        inner = MagicMock()
        runtime = WorkflowRuntime(runtime=inner)
        await runtime.start()
        assert runtime.registered_activities == ()
        inner.register_activity.assert_not_called()

    async def test_start_with_both_clients_registers_both_bridges(self) -> None:
        inner = MagicMock()
        fake_arm = FakeActivityRuntimeClient(results=[])
        fake_conn = FakeConnectorClient()
        runtime = WorkflowRuntime(
            runtime=inner,
            activity_runtime_client=fake_arm,
            connector_client=fake_conn,
        )
        await runtime.start()
        assert set(runtime.registered_activities) == {
            SCHEDULE_ACTIVITY_ACTIVITY_NAME,
            BIND_FOR_STEP_ACTIVITY_NAME,
        }
        # Two underlying SDK calls, one per bridge.
        assert inner.register_activity.call_count == 2

    async def test_start_with_only_arm_client_registers_only_arm(self) -> None:
        inner = MagicMock()
        runtime = WorkflowRuntime(
            runtime=inner,
            activity_runtime_client=FakeActivityRuntimeClient(results=[]),
        )
        await runtime.start()
        assert runtime.registered_activities == (SCHEDULE_ACTIVITY_ACTIVITY_NAME,)

    async def test_start_with_only_connector_client_registers_only_connector(self) -> None:
        inner = MagicMock()
        runtime = WorkflowRuntime(runtime=inner, connector_client=FakeConnectorClient())
        await runtime.start()
        assert runtime.registered_activities == (BIND_FOR_STEP_ACTIVITY_NAME,)

    async def test_start_is_idempotent_does_not_double_register(self) -> None:
        inner = MagicMock()
        runtime = WorkflowRuntime(
            runtime=inner,
            activity_runtime_client=FakeActivityRuntimeClient(results=[]),
            connector_client=FakeConnectorClient(),
        )
        await runtime.start()
        await runtime.start()
        assert inner.register_activity.call_count == 2

    async def test_manually_registered_bridge_not_double_registered_by_start(self) -> None:
        inner = MagicMock()
        runtime = WorkflowRuntime(
            runtime=inner,
            activity_runtime_client=FakeActivityRuntimeClient(results=[]),
            connector_client=FakeConnectorClient(),
        )
        # Pre-register one of the bridges manually.
        runtime.register_activity(
            build_arm_schedule_activity(FakeActivityRuntimeClient(results=[])),
            name=SCHEDULE_ACTIVITY_ACTIVITY_NAME,
        )
        before = inner.register_activity.call_count
        await runtime.start()
        # Only the connector bridge should have been added by start().
        assert inner.register_activity.call_count == before + 1
        assert set(runtime.registered_activities) == {
            SCHEDULE_ACTIVITY_ACTIVITY_NAME,
            BIND_FOR_STEP_ACTIVITY_NAME,
        }

    def test_register_activity_tracks_name(self) -> None:
        inner = MagicMock()
        runtime = WorkflowRuntime(runtime=inner)

        def my_fn(_ctx: Any, _payload: Any) -> Any:
            return None

        runtime.register_activity(my_fn)
        assert runtime.registered_activities == ("my_fn",)

    def test_register_activity_explicit_name_overrides_dunder(self) -> None:
        inner = MagicMock()
        runtime = WorkflowRuntime(runtime=inner)

        def my_fn(_ctx: Any, _payload: Any) -> Any:
            return None

        runtime.register_activity(my_fn, name="explicit.name")
        assert runtime.registered_activities == ("explicit.name",)


# ---------------------------------------------------------------------------
# FakeWorkflowRuntime mirrors the registration surface
# ---------------------------------------------------------------------------


class TestFakeWorkflowRuntimeRegistration:
    async def test_start_without_clients_no_bridges(self) -> None:
        runtime = FakeWorkflowRuntime()
        await runtime.start()
        assert runtime.registered_activities == ()

    async def test_start_with_both_clients_registers_both_bridges(self) -> None:
        runtime = FakeWorkflowRuntime(
            activity_runtime_client=FakeActivityRuntimeClient(results=[]),
            connector_client=FakeConnectorClient(),
        )
        await runtime.start()
        assert set(runtime.registered_activities) == {
            SCHEDULE_ACTIVITY_ACTIVITY_NAME,
            BIND_FOR_STEP_ACTIVITY_NAME,
        }

    async def test_start_with_only_arm_client_registers_only_arm(self) -> None:
        runtime = FakeWorkflowRuntime(
            activity_runtime_client=FakeActivityRuntimeClient(results=[]),
        )
        await runtime.start()
        assert runtime.registered_activities == (SCHEDULE_ACTIVITY_ACTIVITY_NAME,)

    async def test_register_activity_tracks_order(self) -> None:
        runtime = FakeWorkflowRuntime()

        def a(_ctx: Any, _p: Any) -> Any:
            return None

        def b(_ctx: Any, _p: Any) -> Any:
            return None

        runtime.register_activity(a)
        runtime.register_activity(b)
        assert runtime.registered_activities == ("a", "b")

    async def test_register_activity_same_name_does_not_duplicate(self) -> None:
        runtime = FakeWorkflowRuntime()

        def a(_ctx: Any, _p: Any) -> Any:
            return None

        runtime.register_activity(a)
        runtime.register_activity(a)
        assert runtime.registered_activities == ("a",)

    async def test_start_is_idempotent(self) -> None:
        runtime = FakeWorkflowRuntime(
            activity_runtime_client=FakeActivityRuntimeClient(results=[]),
            connector_client=FakeConnectorClient(),
        )
        await runtime.start()
        await runtime.start()
        # Only one of each.
        names = list(runtime.registered_activities)
        assert names.count(SCHEDULE_ACTIVITY_ACTIVITY_NAME) == 1
        assert names.count(BIND_FOR_STEP_ACTIVITY_NAME) == 1


# ---------------------------------------------------------------------------
# Cross-process integration: activity round-trip through fake registration
# ---------------------------------------------------------------------------


class TestEndToEndRoundTrip:
    async def test_arm_activity_invocable_via_fake_registration(self) -> None:
        # The fake runtime's registered_activities surface reflects
        # the bridge — but more importantly, the bridge function the
        # caller looks up by name still resolves the deserialization
        # path against the injected client.
        env = _make_envelope("success")
        arm = FakeActivityRuntimeClient(results=[env])
        runtime = FakeWorkflowRuntime(activity_runtime_client=arm)
        await runtime.start()
        # Look up the registered function via the internal table.
        fn = runtime._activities[SCHEDULE_ACTIVITY_ACTIVITY_NAME]
        wire = serialize_schedule_activity_request(_make_schedule_request())
        envelope = fn(MagicMock(), wire)
        restored = parse_arm_schedule_activity_result(envelope)
        assert restored == env

    async def test_connector_activity_invocable_via_fake_registration(self) -> None:
        resp = _make_bind_response()
        conn = FakeConnectorClient(responses=[resp])
        runtime = FakeWorkflowRuntime(connector_client=conn)
        await runtime.start()
        fn = runtime._activities[BIND_FOR_STEP_ACTIVITY_NAME]
        wire = serialize_bind_for_step_request(_make_bind_request())
        envelope = fn(MagicMock(), wire)
        restored = parse_connector_bind_for_step_result(envelope)
        assert dict(restored.contexts) == dict(resp.contexts)

    def test_async_client_event_loop_isolation(self) -> None:
        # The bridge uses asyncio.run internally; ensure invoking the
        # activity does not pollute an outer running loop. Run from a
        # plain sync test so asyncio.run inside the activity is the
        # only loop active.
        env = _make_envelope("success")
        client = _AsyncFakeActivityClient([env])
        fn = build_arm_schedule_activity(client)  # type: ignore[arg-type]
        wire = serialize_schedule_activity_request(_make_schedule_request())
        # First invocation succeeds.
        first = fn(None, wire)
        assert first["ok"] is True
        # Subsequent invocations work too (transport pool agnostic).
        client.envelopes.append(env)
        second = fn(None, wire)
        assert second["ok"] is True
