"""WF-IMPL-074 — activity-task yield protocol for ``ActivityStepHandler``.

Decouples the Step Coordinator's
:class:`~custos_workflow.steps.activity_step.ActivityStepHandler`
from the I/O substrate of its two outbound RPCs
(``ConnectorClient.bind_for_step`` and
``ActivityRuntimeClient.schedule_activity``). The handler exposes a
generator method (``iter_calls``) that yields
:data:`ActivityCallToken` value objects in place of calling the
underlying clients inline; the surrounding driver (production Dapr
worker, :class:`~custos_workflow.runtime.FakeWorkflowRuntime`, or
the in-process :class:`FakeDaprActivityDispatcher` defined below)
resolves each yielded token and sends the response back into the
generator via ``gen.send(response)``.

This is the prerequisite that makes a production HTTP-backed
adapter wireable: without it, calling the real ARM / Connector
adapters from inside the Run Controller orchestrator function would
violate Dapr Workflow determinism (every outbound RPC must be a
durable activity, not an inline ``requests.post``). The production
Dapr-Workflow activity registration that resolves these tokens via
``ctx.call_activity(...)`` lands in WF-IMPL-079; this module is
purely the foundation, plus the in-process driver tests and the
synchronous Step Coordinator path use to keep the existing
:meth:`StepHandler.execute` ↦ :class:`StepResult` contract intact
while the production wiring catches up.

Wire-stable activity names
--------------------------

:data:`BIND_FOR_STEP_ACTIVITY_NAME` and
:data:`SCHEDULE_ACTIVITY_ACTIVITY_NAME` are the Dapr activity
function names WF-IMPL-079 will register the resolver activities
under. Pinning the names here (rather than in WF-IMPL-079) lets
this module's tests assert the surface that the production wiring
will key off, and keeps the value-object types and the activity
names in the same file so a future rename only touches one module.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Generator, Mapping
from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Final, NoReturn, cast

if TYPE_CHECKING:
    # Eagerly importing ``custos_workflow.clients`` would close a
    # circular import via the package-level ``custos_workflow``
    # init chain (``app`` -> ``providers`` -> ``clients`` ->
    # ``steps`` -> ``runs`` -> ``runtime`` -> ``runtime.dapr``
    # -> ``runtime.dapr_activities``). The serializers /
    # activity-factories below lazy-import the request / response
    # / error types inside their bodies; only static type-checkers
    # need the symbols at module load.
    from custos_workflow.clients.activity_runtime import (
        ActivityResultClass,
        ActivityResultEnvelope,
        ActivityRuntimeClient,
        ScheduleActivityRequest,
    )
    from custos_workflow.clients.connector import (
        BindForStepRequest,
        BindForStepResponse,
        ConnectorClient,
        ConnectorContext,
        SlotSpec,
    )
    from custos_workflow.runs.step_handler import StepResult

__all__ = [
    "BIND_FOR_STEP_ACTIVITY_NAME",
    "SCHEDULE_ACTIVITY_ACTIVITY_NAME",
    "ActivityCallToken",
    "BindForStepCallToken",
    "FakeDaprActivityDispatcher",
    "ScheduleActivityCallToken",
    "build_arm_schedule_activity",
    "build_connector_bind_for_step_activity",
    "drive_activity_generator",
    "parse_arm_schedule_activity_result",
    "parse_connector_bind_for_step_result",
    "serialize_bind_for_step_request",
    "serialize_schedule_activity_request",
]


# ---------------------------------------------------------------------------
# Wire-stable Dapr activity names
# ---------------------------------------------------------------------------


#: Dapr activity name that resolves :class:`BindForStepCallToken`
#: yields. WF-IMPL-079 registers the corresponding activity function
#: (which calls ``ConnectorClient.bind_for_step`` against the real
#: HTTP adapter) under this name.
BIND_FOR_STEP_ACTIVITY_NAME: Final[str] = "custos.workflow.connector.bind_for_step"

#: Dapr activity name that resolves :class:`ScheduleActivityCallToken`
#: yields. WF-IMPL-079 registers the corresponding activity function
#: (which calls ``ActivityRuntimeClient.schedule_activity`` against
#: the real HTTP adapter) under this name.
SCHEDULE_ACTIVITY_ACTIVITY_NAME: Final[str] = "custos.workflow.arm.schedule_activity"


# ---------------------------------------------------------------------------
# Token value objects
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class BindForStepCallToken:
    """Yielded value object representing a deferred ``bind_for_step`` call.

    Carries the fully-constructed
    :class:`~custos_workflow.clients.BindForStepRequest` so the
    driver can dispatch the call without rebuilding the request.
    The expected ``gen.send(...)`` reply is the
    :class:`~custos_workflow.clients.BindForStepResponse` returned
    by the resolved call.

    :param request: The bind request the handler would otherwise
        have passed inline to
        :meth:`ConnectorClient.bind_for_step`.
    """

    request: BindForStepRequest


@dataclass(frozen=True, slots=True)
class ScheduleActivityCallToken:
    """Yielded value object representing a deferred ``schedule_activity`` call.

    Carries the fully-constructed
    :class:`~custos_workflow.clients.ScheduleActivityRequest` so
    the driver can dispatch the call without rebuilding it. The
    expected ``gen.send(...)`` reply is the
    :class:`~custos_workflow.clients.ActivityResultEnvelope`
    returned by the resolved call.

    :param request: The schedule request the handler would
        otherwise have passed inline to
        :meth:`ActivityRuntimeClient.schedule_activity`.
    """

    request: ScheduleActivityRequest


#: Union of value-object tokens
#: :meth:`ActivityStepHandler.iter_calls` may yield. Driver
#: implementations dispatch on this union via ``isinstance``.
ActivityCallToken = BindForStepCallToken | ScheduleActivityCallToken


# ---------------------------------------------------------------------------
# In-process driver
# ---------------------------------------------------------------------------


def drive_activity_generator(
    gen: Generator[ActivityCallToken, object, StepResult],
    activity_client: ActivityRuntimeClient,
    connector_client: ConnectorClient,
) -> StepResult:
    """Drive an activity-handler generator to completion in-process.

    Pumps ``gen`` forward, dispatching each yielded
    :data:`ActivityCallToken` to the matching client method and
    sending the response back into the generator. Exceptions
    raised by the client methods are propagated back into the
    generator via :meth:`Generator.throw`, so the handler's own
    ``try`` / ``except`` blocks around the yield sites observe
    the same exception types they observed when the calls were
    inline (e.g.
    :class:`~custos_workflow.steps.errors.ConnectorBindError`).

    :param gen: The generator returned by
        :meth:`~custos_workflow.steps.activity_step.ActivityStepHandler.iter_calls`.
    :param activity_client: The
        :class:`~custos_workflow.clients.ActivityRuntimeClient`
        used to resolve :class:`ScheduleActivityCallToken` yields.
    :param connector_client: The
        :class:`~custos_workflow.clients.ConnectorClient` used to
        resolve :class:`BindForStepCallToken` yields.

    :returns: The :class:`StepResult` returned by the generator on
        :class:`StopIteration`.

    :raises TypeError: If a yielded value is not an
        :data:`ActivityCallToken` instance.
    """
    sent: object = None
    pending_exc: Exception | None = None
    while True:
        try:
            if pending_exc is not None:
                exc_to_throw, pending_exc = pending_exc, None
                token = gen.throw(exc_to_throw)
            else:
                token = gen.send(sent)
        except StopIteration as stop:
            # Generators returning a non-default value carry it on
            # ``StopIteration.value``; the handler's
            # ``return StepResult`` lands here.
            return stop.value  # type: ignore[no-any-return]

        sent = None
        if isinstance(token, BindForStepCallToken):
            try:
                sent = connector_client.bind_for_step(token.request)
            except Exception as exc:
                pending_exc = exc
        elif isinstance(token, ScheduleActivityCallToken):
            try:
                sent = activity_client.schedule_activity(token.request)
            except Exception as exc:
                pending_exc = exc
        else:
            raise TypeError(
                "ActivityStepHandler.iter_calls yielded an unsupported token "
                f"type: {type(token).__name__}; expected BindForStepCallToken "
                "or ScheduleActivityCallToken",
            )


class FakeDaprActivityDispatcher:
    """In-process resolver for :data:`ActivityCallToken` yields.

    Wraps :func:`drive_activity_generator` in a stateful class so
    test fixtures can preserve a single dispatcher instance across
    multiple ``handler.iter_calls(...)`` invocations against the
    same pair of in-process fakes (mirroring the production wiring
    pattern where the worker constructs the dispatcher once at
    startup and reuses it for every run).

    The class also serves as the dependency boundary
    :class:`~custos_workflow.runtime.FakeWorkflowRuntime` keys off
    so the fake's orchestrator-side ``yield from``-based dispatch
    of :class:`BindForStepCallToken` / :class:`ScheduleActivityCallToken`
    resolves against the same in-process fakes a test already
    constructed for direct handler exercise.

    :param activity_client: The
        :class:`~custos_workflow.clients.ActivityRuntimeClient`
        used to resolve :class:`ScheduleActivityCallToken` yields.
    :param connector_client: The
        :class:`~custos_workflow.clients.ConnectorClient` used to
        resolve :class:`BindForStepCallToken` yields.
    """

    __slots__ = ("_activity_client", "_connector_client")

    def __init__(
        self,
        activity_client: ActivityRuntimeClient,
        connector_client: ConnectorClient,
    ) -> None:
        self._activity_client = activity_client
        self._connector_client = connector_client

    @property
    def activity_client(self) -> ActivityRuntimeClient:
        """The :class:`ActivityRuntimeClient` this dispatcher resolves against."""
        return self._activity_client

    @property
    def connector_client(self) -> ConnectorClient:
        """The :class:`ConnectorClient` this dispatcher resolves against."""
        return self._connector_client

    def drive(
        self,
        gen: Generator[ActivityCallToken, object, StepResult],
    ) -> StepResult:
        """Drive ``gen`` to completion. See :func:`drive_activity_generator`."""
        return drive_activity_generator(
            gen,
            self._activity_client,
            self._connector_client,
        )

    def resolve(self, token: ActivityCallToken) -> object:
        """Resolve a single :data:`ActivityCallToken` against the wired clients.

        Used by drivers (e.g. :class:`FakeWorkflowRuntime`) that
        prefer to interleave token resolution with their own
        generator-driving loop rather than delegate the whole
        generator to :meth:`drive`.

        :raises TypeError: If ``token`` is not an
            :data:`ActivityCallToken` instance.
        """
        if isinstance(token, BindForStepCallToken):
            return self._connector_client.bind_for_step(token.request)
        if isinstance(token, ScheduleActivityCallToken):
            return self._activity_client.schedule_activity(token.request)
        raise TypeError(
            "FakeDaprActivityDispatcher.resolve received an unsupported token "
            f"type: {type(token).__name__}; expected BindForStepCallToken or "
            "ScheduleActivityCallToken",
        )


# ---------------------------------------------------------------------------
# WF-IMPL-079 — Dapr-activity wiring for the production HTTP adapters
# ---------------------------------------------------------------------------
#
# The :data:`BIND_FOR_STEP_ACTIVITY_NAME` / :data:`SCHEDULE_ACTIVITY_ACTIVITY_NAME`
# activities the Dapr worker registers at startup. Each activity:
#
#   1. Receives the camelCase wire envelope Dapr Workflow delivered.
#   2. Deserializes it back into the matching frozen request
#      dataclass (using the dataclass's own ``__post_init__``
#      invariants — a malformed payload fails loudly at the
#      activity boundary instead of leaking into the injected
#      client).
#   3. Calls the injected sync or async ``ConnectorClient`` /
#      ``ActivityRuntimeClient`` method.
#   4. Serializes the success response (or the structured
#      :class:`OutboundRpcError` failure) into a JSON-friendly
#      envelope that round-trips back through Dapr's activity-task
#      return path without losing class / kind / detail / cause
#      information.
#
# The orchestrator side (Run Controller, WF-IMPL-080) consumes the
# returned envelope via :func:`parse_arm_schedule_activity_result`
# / :func:`parse_connector_bind_for_step_result`, which either
# return the deserialized response or re-raise the original
# :class:`OutboundRpcError` subclass with ``__cause__`` walked back
# to ``MAX_CAUSE_DEPTH``.


#: Top-level envelope key flagging success vs. structured failure.
_ENVELOPE_OK: Final[str] = "ok"

#: Top-level envelope key carrying the serialized success payload.
_ENVELOPE_RESULT: Final[str] = "result"

#: Top-level envelope key carrying the serialized
#: :class:`OutboundRpcError` failure payload.
_ENVELOPE_ERROR: Final[str] = "error"


# Concrete-subclass dispatch table for the error envelope round-trip.
# Indexed by :class:`OutboundRpcError.kind` (one of
# :data:`LOCKED_OUTBOUND_RPC_KINDS`); the cancelled / transport /
# decode constructors take only ``detail`` while the status
# constructor takes ``detail`` + ``status_code`` + optional ``code``.
# Built lazily on first use to keep the module load-order free of
# the ``custos_workflow.clients`` package init cycle.
_kind_to_subclass_cache: Mapping[str, type[Any]] | None = None


def _kind_to_subclass() -> Mapping[str, type[Any]]:
    global _kind_to_subclass_cache
    if _kind_to_subclass_cache is None:
        from custos_workflow.clients._errors import (
            OutboundRpcCancelledError,
            OutboundRpcDecodeError,
            OutboundRpcStatusError,
            OutboundRpcTransportError,
        )

        _kind_to_subclass_cache = MappingProxyType(
            {
                OutboundRpcTransportError.kind: OutboundRpcTransportError,
                OutboundRpcStatusError.kind: OutboundRpcStatusError,
                OutboundRpcDecodeError.kind: OutboundRpcDecodeError,
                OutboundRpcCancelledError.kind: OutboundRpcCancelledError,
            }
        )
    return _kind_to_subclass_cache


def _format_iso_utc(value: datetime) -> str:
    """Render a timezone-aware :class:`datetime` to ISO 8601 with ``Z`` suffix.

    Matches the wire format the production HTTP adapters
    (WF-IMPL-076 / 078) already emit so the activity-task
    envelopes share the same datetime shape and ``parse`` /
    ``serialize`` round-trips through Python ``json`` without
    losing precision or timezone information.
    """
    if value.tzinfo is None:
        raise ValueError(
            "datetime must be timezone-aware to serialize to the Dapr activity-task wire envelope"
        )
    return value.isoformat().replace("+00:00", "Z")


def _parse_iso_utc(value: object) -> datetime:
    """Parse an ISO 8601 string emitted by :func:`_format_iso_utc`.

    Accepts the ``Z`` suffix (normalised to ``+00:00``) or an
    explicit offset. Rejects naïve / non-string / garbage inputs
    with :class:`ValueError` so callers can wrap into
    :class:`OutboundRpcDecodeError`.
    """
    if not isinstance(value, str):
        raise ValueError(f"expected ISO 8601 string for datetime field, got {type(value).__name__}")
    normalised = value.replace("Z", "+00:00") if value.endswith("Z") else value
    parsed = datetime.fromisoformat(normalised)
    if parsed.tzinfo is None:
        raise ValueError(f"datetime string {value!r} is naïve; an explicit offset is required")
    return parsed


def _unwrap_mapping(value: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """Deep-copy a (potentially nested) :class:`Mapping` into plain ``dict`` form.

    The ``json`` stdlib happily serializes :class:`MappingProxyType`
    via duck typing, but Dapr's activity-task return path round-trips
    payloads through its own JSON codec; emitting a plain ``dict``
    keeps the wire envelope predictable across SDK versions.
    """
    if value is None:
        return None
    return {k: _unwrap_value(v) for k, v in value.items()}


def _unwrap_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {k: _unwrap_value(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [_unwrap_value(v) for v in value]
    return value


# ---------------------------------------------------------------------------
# ConnectorContext / SlotSpec wire form
# ---------------------------------------------------------------------------


def _serialize_connector_context(ctx: ConnectorContext) -> dict[str, Any]:
    return {
        "slotName": ctx.slot_name,
        "handle": ctx.handle,
        "expiresAt": _format_iso_utc(ctx.expires_at),
        "connectorKind": ctx.connector_kind,
    }


def _deserialize_connector_context(payload: Mapping[str, Any]) -> ConnectorContext:
    from custos_workflow.clients.connector import ConnectorContext as _ConnectorContext

    missing = {"slotName", "handle", "expiresAt", "connectorKind"} - set(payload)
    if missing:
        raise ValueError(
            f"ConnectorContext wire payload missing required field(s): {sorted(missing)!r}"
        )
    for field_name in ("slotName", "handle", "connectorKind"):
        field_value = payload[field_name]
        if not isinstance(field_value, str):
            raise ValueError(
                f"ConnectorContext.{field_name} must be a string, got {type(field_value).__name__}"
            )
    return _ConnectorContext(
        slot_name=payload["slotName"],
        handle=payload["handle"],
        expires_at=_parse_iso_utc(payload["expiresAt"]),
        connector_kind=payload["connectorKind"],
    )


def _serialize_slot_spec(spec: SlotSpec) -> dict[str, Any]:
    return {
        "name": spec.name,
        "connectorRef": spec.connector_ref,
        "capabilities": list(spec.capabilities),
    }


def _deserialize_slot_spec(payload: Mapping[str, Any]) -> SlotSpec:
    from custos_workflow.clients.connector import SlotSpec as _SlotSpec

    missing = {"name", "connectorRef", "capabilities"} - set(payload)
    if missing:
        raise ValueError(f"SlotSpec wire payload missing required field(s): {sorted(missing)!r}")
    capabilities_raw = payload["capabilities"]
    if not isinstance(capabilities_raw, list | tuple):
        raise ValueError(
            f"SlotSpec.capabilities must be a JSON array, got {type(capabilities_raw).__name__}"
        )
    for cap in capabilities_raw:
        if not isinstance(cap, str):
            raise ValueError(
                f"SlotSpec.capabilities entries must be strings, got {type(cap).__name__}"
            )
    return _SlotSpec(
        name=payload["name"],
        connector_ref=payload["connectorRef"],
        capabilities=tuple(capabilities_raw),
    )


# ---------------------------------------------------------------------------
# BindForStep request / response
# ---------------------------------------------------------------------------


def serialize_bind_for_step_request(request: BindForStepRequest) -> dict[str, Any]:
    """Render a :class:`BindForStepRequest` to its activity-input wire form.

    The wire envelope mirrors the camelCase HTTP body shape the
    production :class:`~custos_workflow.clients.connector.DaprConnectorClient`
    posts to Connector Service so an audit consumer reading the
    activity-task payload sees the same JSON shape as the
    downstream HTTP RPC body.
    """
    return {
        "stepKey": request.step_key,
        "slots": [_serialize_slot_spec(spec) for spec in request.slots],
    }


def _deserialize_bind_for_step_request(payload: Mapping[str, Any]) -> BindForStepRequest:
    from custos_workflow.clients.connector import BindForStepRequest as _BindForStepRequest

    if not isinstance(payload, Mapping):
        raise ValueError(
            f"BindForStepRequest wire payload must be a JSON object, got {type(payload).__name__}"
        )
    missing = {"stepKey", "slots"} - set(payload)
    if missing:
        raise ValueError(
            f"BindForStepRequest wire payload missing required field(s): {sorted(missing)!r}"
        )
    slots_raw = payload["slots"]
    if not isinstance(slots_raw, list | tuple):
        raise ValueError(
            f"BindForStepRequest.slots must be a JSON array, got {type(slots_raw).__name__}"
        )
    slots = tuple(
        _deserialize_slot_spec(_require_mapping(slot, "BindForStepRequest.slots[*]"))
        for slot in slots_raw
    )
    return _BindForStepRequest(step_key=payload["stepKey"], slots=slots)


def _serialize_bind_for_step_response(response: BindForStepResponse) -> dict[str, Any]:
    return {
        "contexts": {
            slot_name: _serialize_connector_context(ctx)
            for slot_name, ctx in response.contexts.items()
        },
    }


def _deserialize_bind_for_step_response(payload: Mapping[str, Any]) -> BindForStepResponse:
    from custos_workflow.clients.connector import BindForStepResponse as _BindForStepResponse

    if not isinstance(payload, Mapping):
        raise ValueError(
            f"BindForStepResponse wire payload must be a JSON object, got {type(payload).__name__}"
        )
    contexts_raw = payload.get("contexts")
    if contexts_raw is None:
        raise ValueError("BindForStepResponse wire payload missing 'contexts' field")
    if not isinstance(contexts_raw, Mapping):
        raise ValueError(
            "BindForStepResponse 'contexts' must be a JSON object, "
            f"got {type(contexts_raw).__name__}"
        )
    contexts = {
        slot_name: _deserialize_connector_context(
            _require_mapping(ctx, f"BindForStepResponse.contexts[{slot_name!r}]")
        )
        for slot_name, ctx in contexts_raw.items()
    }
    return _BindForStepResponse(contexts=contexts)


# ---------------------------------------------------------------------------
# ScheduleActivity request / response
# ---------------------------------------------------------------------------


def serialize_schedule_activity_request(request: ScheduleActivityRequest) -> dict[str, Any]:
    """Render a :class:`ScheduleActivityRequest` to its activity-input wire form.

    The ``connector_contexts`` mapping is serialized via
    :func:`_serialize_connector_context` so the per-slot
    :class:`ConnectorContext` invariants round-trip — the
    activity boundary is the only point where a malformed
    context would otherwise slip into the activity worker.
    """
    return {
        "runId": request.run_id,
        "stepId": request.step_id,
        "attempt": request.attempt,
        "activityRef": request.activity_ref,
        "inputs": _unwrap_mapping(request.inputs) or {},
        "connectorContexts": {
            slot_name: _serialize_connector_context(ctx)
            for slot_name, ctx in request.connector_contexts.items()
        },
        "deadline": _format_iso_utc(request.deadline),
    }


def _deserialize_schedule_activity_request(
    payload: Mapping[str, Any],
) -> ScheduleActivityRequest:
    from custos_workflow.clients.activity_runtime import (
        ScheduleActivityRequest as _ScheduleActivityRequest,
    )

    if not isinstance(payload, Mapping):
        raise ValueError(
            "ScheduleActivityRequest wire payload must be a JSON object, "
            f"got {type(payload).__name__}"
        )
    required = {
        "runId",
        "stepId",
        "attempt",
        "activityRef",
        "inputs",
        "connectorContexts",
        "deadline",
    }
    missing = required - set(payload)
    if missing:
        raise ValueError(
            f"ScheduleActivityRequest wire payload missing required field(s): {sorted(missing)!r}"
        )
    inputs_raw = payload["inputs"]
    if not isinstance(inputs_raw, Mapping):
        raise ValueError(
            f"ScheduleActivityRequest.inputs must be a JSON object, got {type(inputs_raw).__name__}"
        )
    contexts_raw = payload["connectorContexts"]
    if not isinstance(contexts_raw, Mapping):
        raise ValueError(
            "ScheduleActivityRequest.connectorContexts must be a JSON object, "
            f"got {type(contexts_raw).__name__}"
        )
    connector_contexts = {
        slot_name: _deserialize_connector_context(
            _require_mapping(ctx, f"ScheduleActivityRequest.connectorContexts[{slot_name!r}]")
        )
        for slot_name, ctx in contexts_raw.items()
    }
    return _ScheduleActivityRequest(
        run_id=payload["runId"],
        step_id=payload["stepId"],
        attempt=payload["attempt"],
        activity_ref=payload["activityRef"],
        inputs=MappingProxyType(_unwrap_mapping(inputs_raw) or {}),
        connector_contexts=MappingProxyType(connector_contexts),
        deadline=_parse_iso_utc(payload["deadline"]),
    )


def _serialize_activity_result_envelope(envelope: ActivityResultEnvelope) -> dict[str, Any]:
    return {
        "class": envelope.class_,
        "outputs": _unwrap_mapping(envelope.outputs),
        "error": _unwrap_mapping(envelope.error),
        "attempt": envelope.attempt,
    }


def _deserialize_activity_result_envelope(payload: Mapping[str, Any]) -> ActivityResultEnvelope:
    from custos_workflow.clients.activity_runtime import (
        ActivityResultEnvelope as _ActivityResultEnvelope,
    )

    if not isinstance(payload, Mapping):
        raise ValueError(
            "ActivityResultEnvelope wire payload must be a JSON object, "
            f"got {type(payload).__name__}"
        )
    missing = {"class", "outputs", "error", "attempt"} - set(payload)
    if missing:
        raise ValueError(
            f"ActivityResultEnvelope wire payload missing required field(s): {sorted(missing)!r}"
        )
    class_raw = payload["class"]
    if not isinstance(class_raw, str):
        raise ValueError(
            f"ActivityResultEnvelope.class must be a string, got {type(class_raw).__name__}"
        )
    outputs_raw = payload["outputs"]
    if outputs_raw is not None and not isinstance(outputs_raw, Mapping):
        raise ValueError(
            "ActivityResultEnvelope.outputs must be a JSON object or null, "
            f"got {type(outputs_raw).__name__}"
        )
    error_raw = payload["error"]
    if error_raw is not None and not isinstance(error_raw, Mapping):
        raise ValueError(
            "ActivityResultEnvelope.error must be a JSON object or null, "
            f"got {type(error_raw).__name__}"
        )
    outputs_unwrapped = _unwrap_mapping(outputs_raw)
    error_unwrapped = _unwrap_mapping(error_raw)
    outputs = MappingProxyType(outputs_unwrapped) if outputs_unwrapped is not None else None
    error = MappingProxyType(error_unwrapped) if error_unwrapped is not None else None
    return _ActivityResultEnvelope(
        class_=cast("ActivityResultClass", class_raw),
        outputs=outputs,
        error=error,
        attempt=payload["attempt"],
    )


# ---------------------------------------------------------------------------
# OutboundRpcError envelope round-trip
# ---------------------------------------------------------------------------


def _serialize_cause_chain(exc: BaseException | None, depth: int) -> list[dict[str, str]]:
    """Render an exception's ``__cause__`` chain to a depth-bounded list.

    Mirrors the cap (:data:`MAX_CAUSE_DEPTH`) the envelope
    mapper in :mod:`custos_workflow.clients._errors` uses so the
    wire envelope and the audit envelope can never disagree on
    how many cause levels survive.
    """
    chain: list[dict[str, str]] = []
    current = exc
    while current is not None and len(chain) < depth:
        chain.append({"type": type(current).__name__, "message": str(current)})
        current = current.__cause__
    return chain


def _serialize_outbound_rpc_error(exc: Any) -> dict[str, Any]:
    """Render an :class:`OutboundRpcError` to the activity-task error envelope.

    Preserves class (via :attr:`OutboundRpcError.kind`), detail,
    optional :class:`OutboundRpcStatusError` status code +
    structured code, and the ``__cause__`` chain. The envelope is
    pure JSON so it round-trips through Dapr's activity-task
    return path without re-raising into a generic Dapr error.
    """
    from custos_workflow.clients._errors import (
        MAX_CAUSE_DEPTH,
        OutboundRpcStatusError,
    )

    payload: dict[str, Any] = {
        "kind": exc.kind,
        "detail": exc.detail,
    }
    if isinstance(exc, OutboundRpcStatusError):
        payload["statusCode"] = exc.status_code
        if exc.code is not None:
            payload["code"] = exc.code
    cause = _serialize_cause_chain(exc.__cause__, MAX_CAUSE_DEPTH)
    if cause:
        payload["cause"] = cause
    return payload


def _build_cause_chain(cause_raw: object) -> BaseException | None:
    """Rebuild a chained exception tree from the serialized cause list.

    Each entry becomes a plain :class:`Exception` (the original
    cause type is unknown on the consuming side because the
    activity worker may live in a different process); the
    rebuilt chain still satisfies ``walk via __cause__`` so
    audit emitters preserve the diagnostic.
    """
    if not cause_raw:
        return None
    if not isinstance(cause_raw, list):
        raise ValueError(
            "OutboundRpcError envelope 'cause' must be a JSON array, "
            f"got {type(cause_raw).__name__}"
        )
    root: BaseException | None = None
    previous: BaseException | None = None
    for entry in cause_raw:
        if not isinstance(entry, Mapping):
            raise ValueError(
                "OutboundRpcError envelope 'cause' entries must be JSON objects, "
                f"got {type(entry).__name__}"
            )
        exc_type = entry.get("type")
        message = entry.get("message")
        if not isinstance(exc_type, str) or not isinstance(message, str):
            raise ValueError(
                "OutboundRpcError envelope 'cause' entry must carry string 'type' + 'message'"
            )
        new_exc = Exception(f"{exc_type}: {message}")
        if previous is None:
            root = new_exc
        else:
            previous.__cause__ = new_exc
        previous = new_exc
    return root


def _raise_outbound_rpc_error_from_envelope(payload: Mapping[str, Any]) -> NoReturn:
    """Reconstruct and ``raise`` the appropriate :class:`OutboundRpcError`.

    Surfaces decode-time issues (unknown kind, missing fields,
    malformed status code) as :class:`OutboundRpcDecodeError` so
    the orchestrator's exception-handling path always sees a
    locked-taxonomy exception regardless of whether the original
    activity-side failure was decodable.
    """
    from custos_workflow.clients._errors import (
        LOCKED_OUTBOUND_RPC_KINDS,
        OutboundRpcDecodeError,
        OutboundRpcError,
        OutboundRpcStatusError,
    )

    if not isinstance(payload, Mapping):
        raise OutboundRpcDecodeError(
            f"Dapr activity-task error envelope must be a JSON object, got {type(payload).__name__}"
        )
    kind = payload.get("kind")
    detail = payload.get("detail")
    if not isinstance(kind, str) or not isinstance(detail, str):
        raise OutboundRpcDecodeError(
            "Dapr activity-task error envelope must carry string 'kind' + 'detail' fields"
        )
    if kind not in LOCKED_OUTBOUND_RPC_KINDS:
        raise OutboundRpcDecodeError(
            f"Dapr activity-task error envelope carries unknown kind {kind!r}; "
            f"expected one of {sorted(LOCKED_OUTBOUND_RPC_KINDS)!r}"
        )
    cause = _build_cause_chain(payload.get("cause"))
    subclass = _kind_to_subclass()[kind]
    exc: OutboundRpcError
    if subclass is OutboundRpcStatusError:
        status_code = payload.get("statusCode")
        if not isinstance(status_code, int) or isinstance(status_code, bool):
            raise OutboundRpcDecodeError(
                "Dapr activity-task error envelope for OutboundRpcStatusError "
                "must carry integer 'statusCode'"
            )
        code_raw = payload.get("code")
        if code_raw is not None and not isinstance(code_raw, str):
            raise OutboundRpcDecodeError(
                "Dapr activity-task error envelope 'code' must be a string when present"
            )
        exc = OutboundRpcStatusError(detail, status_code=status_code, code=code_raw)
    else:
        exc = subclass(detail)
    if cause is not None:
        raise exc from cause
    raise exc


# ---------------------------------------------------------------------------
# Activity factories
# ---------------------------------------------------------------------------


def _require_mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be a JSON object, got {type(value).__name__}")
    return value


def _ok_envelope(result_payload: Mapping[str, Any]) -> dict[str, Any]:
    return {_ENVELOPE_OK: True, _ENVELOPE_RESULT: dict(result_payload), _ENVELOPE_ERROR: None}


def _error_envelope(exc: Any) -> dict[str, Any]:
    return {
        _ENVELOPE_OK: False,
        _ENVELOPE_RESULT: None,
        _ENVELOPE_ERROR: _serialize_outbound_rpc_error(exc),
    }


def _call_sync_or_async(
    fn: Callable[..., Any] | Callable[..., Awaitable[Any]],
    *args: Any,
) -> Any:
    """Invoke ``fn`` sync, or run the coroutine to completion if async.

    Bridges the documented gap from WF-IMPL-076 / WF-IMPL-078:
    the production
    :class:`~custos_workflow.clients.DaprActivityRuntimeClient`
    and :class:`~custos_workflow.clients.DaprConnectorClient`
    expose ``async`` methods, but the
    :class:`~custos_workflow.clients.ActivityRuntimeClient` /
    :class:`~custos_workflow.clients.ConnectorClient` Protocol the
    Step Coordinator depends on (and that test fakes implement)
    is synchronous. The Dapr activity function itself is sync —
    ``dapr-ext-workflow``'s activity worker invokes us off the
    event loop — so the bridge lives here, in the activity body,
    where we can pick the right call style per request without
    forcing every caller to wrap its client.

    Uses :func:`asyncio.run` so the coroutine executes on a
    fresh event loop owned by the activity invocation; the
    production adapters' ``httpx.AsyncClient`` instances are
    constructed at worker startup and survive across calls
    because the underlying transport pool is loop-agnostic
    (httpx documents this as the supported pattern).
    """
    result = fn(*args)
    if inspect.isawaitable(result):
        return asyncio.run(_await(result))
    return result


async def _await(coro: Awaitable[Any]) -> Any:
    return await coro


def build_arm_schedule_activity(
    client: ActivityRuntimeClient,
) -> Callable[[Any, Any], dict[str, Any]]:
    """Build the ``custos.workflow.arm.schedule_activity`` Dapr activity.

    The returned callable's ``__name__`` is set to
    :data:`SCHEDULE_ACTIVITY_ACTIVITY_NAME` so callers may register
    it via ``runtime.register_activity(fn)`` without passing an
    explicit ``name=`` (and we also exercise the explicit-``name=``
    path in :meth:`WorkflowRuntime.start` for symmetry with the
    other registered activities).

    :param client: The injected
        :class:`ActivityRuntimeClient` (sync test fake or the
        async production
        :class:`~custos_workflow.clients.DaprActivityRuntimeClient`).
        Async clients are bridged via :func:`asyncio.run` inside
        the activity body.

    :returns: The Dapr activity function.
    """

    def arm_schedule_activity(_ctx: Any, raw_payload: Any) -> dict[str, Any]:
        from custos_workflow.clients._errors import (
            OutboundRpcDecodeError,
            OutboundRpcError,
        )

        try:
            request = _deserialize_schedule_activity_request(
                _require_mapping(raw_payload, "schedule_activity raw payload"),
            )
        except ValueError as exc:
            # Decode failures are activity-side contract violations;
            # surface them as OutboundRpcDecodeError so the
            # orchestrator-side parser re-raises the locked subclass
            # rather than letting Dapr swallow them into a generic
            # error string.
            return _error_envelope(OutboundRpcDecodeError(str(exc)))
        try:
            envelope = _call_sync_or_async(client.schedule_activity, request)
        except OutboundRpcError as exc:
            return _error_envelope(exc)
        return _ok_envelope(_serialize_activity_result_envelope(envelope))

    arm_schedule_activity.__name__ = SCHEDULE_ACTIVITY_ACTIVITY_NAME
    return arm_schedule_activity


def build_connector_bind_for_step_activity(
    client: ConnectorClient,
) -> Callable[[Any, Any], dict[str, Any]]:
    """Build the ``custos.workflow.connector.bind_for_step`` Dapr activity.

    See :func:`build_arm_schedule_activity` — the symmetry is
    deliberate. The returned callable's ``__name__`` is set to
    :data:`BIND_FOR_STEP_ACTIVITY_NAME`.

    :param client: The injected
        :class:`ConnectorClient` (sync test fake or the async
        production
        :class:`~custos_workflow.clients.DaprConnectorClient`).

    :returns: The Dapr activity function.
    """

    def connector_bind_for_step(_ctx: Any, raw_payload: Any) -> dict[str, Any]:
        from custos_workflow.clients._errors import (
            OutboundRpcDecodeError,
            OutboundRpcError,
        )

        try:
            request = _deserialize_bind_for_step_request(
                _require_mapping(raw_payload, "bind_for_step raw payload"),
            )
        except ValueError as exc:
            return _error_envelope(OutboundRpcDecodeError(str(exc)))
        try:
            response = _call_sync_or_async(client.bind_for_step, request)
        except OutboundRpcError as exc:
            return _error_envelope(exc)
        return _ok_envelope(_serialize_bind_for_step_response(response))

    connector_bind_for_step.__name__ = BIND_FOR_STEP_ACTIVITY_NAME
    return connector_bind_for_step


# ---------------------------------------------------------------------------
# Orchestrator-side result parsers
# ---------------------------------------------------------------------------


def _unpack_envelope(envelope: Any, activity_name: str) -> Mapping[str, Any]:
    """Validate the activity-task envelope shape; re-raise on failure or return ``result``.

    Raises :class:`OutboundRpcDecodeError` if the envelope is
    malformed; raises the reconstructed :class:`OutboundRpcError`
    if the envelope is well-formed and reports failure.
    """
    from custos_workflow.clients._errors import OutboundRpcDecodeError

    if not isinstance(envelope, Mapping):
        raise OutboundRpcDecodeError(
            f"{activity_name} activity-task envelope must be a JSON object, "
            f"got {type(envelope).__name__}"
        )
    if _ENVELOPE_OK not in envelope:
        raise OutboundRpcDecodeError(
            f"{activity_name} activity-task envelope missing required {_ENVELOPE_OK!r} field"
        )
    if envelope[_ENVELOPE_OK]:
        result = envelope.get(_ENVELOPE_RESULT)
        if not isinstance(result, Mapping):
            raise OutboundRpcDecodeError(
                f"{activity_name} activity-task envelope success payload "
                f"must be a JSON object, got {type(result).__name__}"
            )
        return result
    error = envelope.get(_ENVELOPE_ERROR)
    if not isinstance(error, Mapping):
        raise OutboundRpcDecodeError(
            f"{activity_name} activity-task envelope failure payload "
            f"must be a JSON object, got {type(error).__name__}"
        )
    _raise_outbound_rpc_error_from_envelope(error)


def parse_arm_schedule_activity_result(envelope: Any) -> ActivityResultEnvelope:
    """Parse the ``arm_schedule_activity`` return envelope.

    On success returns the deserialized
    :class:`ActivityResultEnvelope`; on failure raises the
    original :class:`OutboundRpcError` subclass with class /
    kind / detail / status_code / cause preserved.

    :raises OutboundRpcError: If the envelope reports failure.
    :raises OutboundRpcDecodeError: If the envelope itself is
        malformed or the embedded payload fails shape validation.
    """
    result = _unpack_envelope(envelope, SCHEDULE_ACTIVITY_ACTIVITY_NAME)
    from custos_workflow.clients._errors import OutboundRpcDecodeError

    try:
        return _deserialize_activity_result_envelope(result)
    except ValueError as exc:
        raise OutboundRpcDecodeError(str(exc)) from exc


def parse_connector_bind_for_step_result(envelope: Any) -> BindForStepResponse:
    """Parse the ``connector_bind_for_step`` return envelope.

    See :func:`parse_arm_schedule_activity_result`.
    """
    result = _unpack_envelope(envelope, BIND_FOR_STEP_ACTIVITY_NAME)
    from custos_workflow.clients._errors import OutboundRpcDecodeError

    try:
        return _deserialize_bind_for_step_response(result)
    except ValueError as exc:
        raise OutboundRpcDecodeError(str(exc)) from exc
