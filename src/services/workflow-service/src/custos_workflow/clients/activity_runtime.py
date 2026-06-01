"""``ActivityRuntimeClient`` Protocol + result envelope (WF-IMPL-049).

The Step Coordinator's :class:`ActivityStepHandler` (WF-IMPL-054)
schedules each ``ACTIVITY`` step through an
:class:`ActivityRuntimeClient` and reacts to the returned
:class:`ActivityResultEnvelope`. The Protocol is the only thing
the handler talks to — production wires the real
Dapr-Workflow-backed adapter behind the Protocol (deferred
sub-module: *Real ARM Client + Connector Client adapters*),
and unit tests wire :class:`FakeActivityRuntimeClient` to drive
deterministic scenarios.

The Protocol's method signatures match the synchronous
:meth:`custos_workflow.runs.step_handler.StepHandler.execute`
contract: the production adapter is what calls Dapr Workflow's
``ctx.call_activity()`` and yields on the orchestrator's behalf,
hiding the generator dance from every downstream consumer.

Acceptance criteria (mirrored from #420):

* Protocol is ``runtime_checkable``.
* :attr:`ActivityResultEnvelope.class_` is constrained to the four
  ``design.md`` values
  (``"success"``, ``"retryable"``, ``"permanent"``, ``"cancelled"``)
  via a :data:`typing.Literal` alias that mypy enforces in tests.
* 100 % coverage on this module.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Final, Literal, Protocol, get_args, runtime_checkable

import httpx

from custos_workflow.clients._dapr_invoke import (
    DEFAULT_OUTBOUND_RPC_TIMEOUT_SECONDS,
    DaprInvokeEndpoint,
    build_invoke_url,
)
from custos_workflow.steps.idempotency import IdempotencyTriple

__all__ = [
    "ACTIVITY_RESULT_CLASSES",
    "CANCEL_ACTIVITY_DAPR_METHOD",
    "SCHEDULE_ACTIVITY_DAPR_METHOD",
    "ActivityResultClass",
    "ActivityResultEnvelope",
    "ActivityRuntimeClient",
    "DaprActivityRuntimeClient",
    "FakeActivityRuntimeClient",
    "NoopActivityRuntimeClient",
    "ScheduleActivityRequest",
]

_LOGGER = logging.getLogger(__name__)


#: HTTP header name carrying the canonical
#: :class:`IdempotencyTriple` wire form on every outbound
#: ``ScheduleActivity`` request. The Activity Runtime Manager
#: dedupes on this header per ``design.md`` § Internal RPC
#: (outbound) so the production adapter and ARM stay in lockstep.
IDEMPOTENCY_HEADER: Final[str] = "Idempotency-Key"

#: HTTP status code the Dapr sidecar surfaces when an upstream
#: cancelled the request (nginx-style ``client-closed-request``).
#: Mapped to :class:`OutboundRpcCancelledError` rather than
#: :class:`OutboundRpcStatusError` so the retry-decision driver
#: short-circuits the attempt without consuming a retry slot.
_CLIENT_CLOSED_REQUEST_STATUS: Final[int] = 499

#: Dapr Service-Invocation ``method`` name for ARM's
#: ``ScheduleActivity`` RPC. Pinned here so the adapter and any
#: smoke-test fixture key off the same constant.
SCHEDULE_ACTIVITY_DAPR_METHOD: Final[str] = "ScheduleActivity"

#: Dapr Service-Invocation ``method`` name for ARM's
#: ``CancelActivity`` RPC. Cancellation is idempotent end-to-end:
#: ARM responds 404 when the step is unknown and 409 when the
#: step has already terminated, both of which the adapter
#: collapses into a no-op (see
#: :meth:`DaprActivityRuntimeClient.cancel_activity`).
CANCEL_ACTIVITY_DAPR_METHOD: Final[str] = "CancelActivity"


# ---------------------------------------------------------------------------
# Outcome classification
# ---------------------------------------------------------------------------

ActivityResultClass = Literal["success", "retryable", "permanent", "cancelled"]
"""Closed set of outcome classes the Activity Runtime Manager can return.

Pinned to ``design.md`` § *Activity Result Envelope*; the
WF-IMPL-053 retry decision driver and the WF-IMPL-054
``ActivityStepHandler`` dispatch on this set exhaustively.
"""

ACTIVITY_RESULT_CLASSES: Final[frozenset[str]] = frozenset(get_args(ActivityResultClass))
"""Runtime-introspectable mirror of :data:`ActivityResultClass`.

Audit consumers and the WF-IMPL-058 OTel counter use this
frozenset as the closed label set.
"""


# ---------------------------------------------------------------------------
# Request / response envelopes
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ScheduleActivityRequest:
    """Frozen request envelope passed to :meth:`ActivityRuntimeClient.schedule_activity`.

    Immutable on purpose so the Step Coordinator can stash the
    request alongside the lifecycle event it emits without fear
    of any downstream consumer mutating it.

    The ``(run_id, step_id, attempt)`` triple is the same
    idempotency key the Activity Runtime Manager uses to
    deduplicate retries (see WF-IMPL-047 ``IdempotencyTriple``).
    Construction re-uses the WF-IMPL-047 validation pipeline so
    the same rules apply here: ``run_id`` and ``step_id`` must be
    non-empty and free of the canonical ``|`` separator, and
    ``attempt`` must be a positive integer (``bool`` rejected
    explicitly). ``activity_ref`` must also be non-empty.

    :raises IdempotencyTripleError: If any of the triple
        components is malformed.
    :raises ValueError: If ``attempt`` is not a positive int or
        ``activity_ref`` is empty.
    """

    run_id: str
    step_id: str
    attempt: int
    activity_ref: str
    inputs: Mapping[str, Any]
    # ``connector_contexts`` maps ``slot_name -> ConnectorContext``.
    # WF-IMPL-050 introduces the concrete ``ConnectorContext``
    # frozen dataclass; until that lands we keep the value type
    # loose so this module stays dependency-free per the
    # implementation plan.
    connector_contexts: Mapping[str, Any]
    deadline: datetime

    def __post_init__(self) -> None:
        # Re-use the WF-IMPL-047 idempotency-triple validation so
        # ``ScheduleActivityRequest`` and ``IdempotencyTriple`` agree
        # byte-for-byte on what counts as a valid scheduling key:
        # non-empty ``run_id`` / ``step_id`` free of the canonical
        # ``|`` separator, integer ``attempt >= 1`` (``bool`` rejected
        # explicitly). Any failure surfaces as
        # :class:`IdempotencyTripleError` (a ``ValueError`` subclass),
        # which the Step Coordinator's
        # :class:`ActivityScheduleError` adapter wraps on the way out.
        IdempotencyTriple(run_id=self.run_id, step_id=self.step_id, attempt=self.attempt)
        if not self.activity_ref:
            raise ValueError("activity_ref must be a non-empty string")


@dataclass(frozen=True, slots=True)
class ActivityResultEnvelope:
    """Frozen response envelope returned by :meth:`ActivityRuntimeClient.schedule_activity`.

    Mirrors the ``design.md`` *Activity Result Envelope* shape so
    a single object can flow unchanged from the activity worker
    → ARM → Step Coordinator → ``step.completed`` /
    ``step.failed`` audit event.

    Construction enforces the documented invariants so adapters
    and tests can never accidentally synthesize a malformed
    envelope:

    * ``"success"``  — :attr:`outputs` populated, :attr:`error` is ``None``.
    * ``"retryable"`` / ``"permanent"`` / ``"cancelled"`` —
      :attr:`error` populated, :attr:`outputs` is ``None``.
    * :attr:`attempt` must be a positive integer (``bool`` rejected
      explicitly).

    The retry decision driver (WF-IMPL-053) consumes
    :attr:`class_` + :attr:`error` to choose between scheduling
    a fresh attempt and tipping the step into terminal failure.

    :raises ValueError: If the ``outputs``/``error`` shape does
        not match :attr:`class_` or ``attempt < 1``.
    """

    class_: ActivityResultClass
    outputs: Mapping[str, Any] | None
    error: Mapping[str, Any] | None
    attempt: int

    def __post_init__(self) -> None:
        # ``attempt`` must mirror the per-step attempt counter the
        # Step Coordinator passed in. Reject 0 / negative / bool so
        # an invalid envelope can never be confused with a fresh
        # first attempt downstream.
        if isinstance(self.attempt, bool) or not isinstance(self.attempt, int):
            raise ValueError(f"attempt must be an int, got {type(self.attempt).__name__}")
        if self.attempt < 1:
            raise ValueError(f"attempt must be >= 1, got {self.attempt}")
        # The ``design.md`` § *Activity Result Envelope* contract:
        # success carries outputs (no error), every other class
        # carries an error (no outputs). The retry decision driver
        # (WF-IMPL-053) and the audit emitter (WF-IMPL-056) both
        # rely on this invariant; enforce it at the boundary so
        # malformed envelopes fail fast instead of silently
        # corrupting downstream behavior.
        if self.class_ == "success":
            if self.outputs is None:
                raise ValueError("ActivityResultEnvelope(class_='success') must carry outputs")
            if self.error is not None:
                raise ValueError("ActivityResultEnvelope(class_='success') must not carry error")
        else:
            if self.error is None:
                raise ValueError(f"ActivityResultEnvelope(class_={self.class_!r}) must carry error")
            if self.outputs is not None:
                raise ValueError(
                    f"ActivityResultEnvelope(class_={self.class_!r}) must not carry outputs"
                )


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class ActivityRuntimeClient(Protocol):
    """Runtime-checkable Protocol the Step Coordinator depends on.

    The Step Coordinator only ever calls these two methods; the
    production adapter (Dapr Workflow ``ctx.call_activity()``
    bridge — deferred sub-module) and the
    :class:`FakeActivityRuntimeClient` test double both satisfy
    this Protocol structurally.
    """

    def schedule_activity(self, request: ScheduleActivityRequest) -> ActivityResultEnvelope:
        """Schedule one activity attempt and return its result envelope.

        The call is synchronous from the handler's perspective: the
        production adapter is the layer that suspends the
        orchestrator on the Dapr Workflow generator's behalf so the
        handler signature can stay flat.
        """
        ...

    def cancel_activity(self, run_id: str, step_id: str) -> None:
        """Cancel any in-flight attempt for the given step.

        Idempotent: cancelling an already-finished step is a no-op
        for the production adapter, and tests rely on that.
        """
        ...


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class NoopActivityRuntimeClient:
    """Safe default that explicitly :class:`NotImplementedError`-s every call.

    Wired by the FastAPI lifespan (WF-IMPL-057) at startup so the
    process does *not* silently accept activity scheduling
    requests before the real adapter is installed.
    """

    def schedule_activity(self, request: ScheduleActivityRequest) -> ActivityResultEnvelope:
        raise NotImplementedError(
            "NoopActivityRuntimeClient.schedule_activity: "
            "no production ActivityRuntimeClient adapter is wired yet "
            "(deferred sub-module: Real ARM Client adapter)."
        )

    def cancel_activity(self, run_id: str, step_id: str) -> None:
        raise NotImplementedError(
            "NoopActivityRuntimeClient.cancel_activity: "
            "no production ActivityRuntimeClient adapter is wired yet "
            "(deferred sub-module: Real ARM Client adapter)."
        )


@dataclass(slots=True)
class FakeActivityRuntimeClient:
    """In-memory test double that returns canned envelopes.

    Pass a list of pre-built :class:`ActivityResultEnvelope`
    instances on :attr:`results`; each call to
    :meth:`schedule_activity` pops the next envelope in order.
    Every call is recorded on :attr:`calls` and every cancellation
    on :attr:`cancellations` so tests can assert call patterns
    without monkey-patching.

    Raises :class:`IndexError` if a test schedules more activities
    than it queued — that almost always means the test is missing
    a canned envelope, so failing loud beats returning a default.
    """

    results: list[ActivityResultEnvelope] = field(default_factory=list)
    calls: list[ScheduleActivityRequest] = field(default_factory=list)
    cancellations: list[tuple[str, str]] = field(default_factory=list)

    def schedule_activity(self, request: ScheduleActivityRequest) -> ActivityResultEnvelope:
        self.calls.append(request)
        if not self.results:
            raise IndexError(
                "FakeActivityRuntimeClient.schedule_activity: "
                "no more canned envelopes queued "
                f"(called for run_id={request.run_id!r} "
                f"step_id={request.step_id!r} attempt={request.attempt!r})."
            )
        return self.results.pop(0)

    def cancel_activity(self, run_id: str, step_id: str) -> None:
        self.cancellations.append((run_id, step_id))


# ---------------------------------------------------------------------------
# Production adapter: Dapr Service-Invocation HTTP transport
# ---------------------------------------------------------------------------


def _iso_utc(value: datetime) -> str:
    """Render a ``datetime`` as a canonical ISO-8601 UTC string.

    The wire envelope ARM consumes must use UTC with a trailing
    ``Z`` per ``design.md`` § *Internal RPCs*. Neither
    :class:`ScheduleActivityRequest.__post_init__` nor
    :class:`IdempotencyTriple` validate the ``deadline`` field's
    ``tzinfo``, so this helper is the enforcement point that
    keeps the wire format deterministic — a naïve datetime
    surfaces as a ``ValueError`` here rather than producing an
    ambiguous timestamp on the wire.
    """
    if value.tzinfo is None:
        # Reject naïve datetimes rather than silently treating
        # them as UTC: the Step Coordinator builds ``deadline``
        # from ``workflow_context.current_utc_datetime`` (already
        # tz-aware), so a naïve value almost always means a unit
        # test or caller built one without ``tzinfo`` by mistake.
        # Failing fast surfaces the bug instead of corrupting the
        # wire timestamp.
        raise ValueError(
            "DaprActivityRuntimeClient requires deadline to be timezone-aware "
            "(use datetime.UTC for absolute deadlines)."
        )
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _serialize_connector_context(value: Any) -> Mapping[str, Any]:
    """Render one ``connector_contexts`` value to its wire shape.

    Production passes :class:`~custos_workflow.clients.ConnectorContext`
    instances (the response of
    :meth:`ConnectorClient.bind_for_step`); tests sometimes pass
    plain dicts. Accept both so the adapter is symmetrical with
    the in-memory fakes — anything else is a programming bug and
    surfaces as :class:`TypeError`.
    """
    # Import lazily to avoid a top-level cycle: ``connector``
    # imports from :mod:`_errors`, and :mod:`_errors` imports
    # ``ActivityResultEnvelope`` / ``ActivityResultClass`` from
    # this module.
    from custos_workflow.clients.connector import ConnectorContext

    if isinstance(value, ConnectorContext):
        return {
            "slotName": value.slot_name,
            "handle": value.handle,
            "expiresAt": _iso_utc(value.expires_at),
            "connectorKind": value.connector_kind,
        }
    if isinstance(value, Mapping):
        # Tests may construct the request with plain-dict
        # contexts; pass them through verbatim so the wire-shape
        # assertion stays at the test boundary.
        return dict(value)
    raise TypeError(
        "ScheduleActivityRequest.connector_contexts values must be "
        f"ConnectorContext or Mapping, got {type(value).__name__}"
    )


def _request_to_wire(request: ScheduleActivityRequest) -> Mapping[str, Any]:
    """Marshal a :class:`ScheduleActivityRequest` into the ARM wire envelope.

    Keys are camelCase per the ARM design § *Internal RPCs* so the
    sidecar's JSON parser does not have to translate.
    """
    return {
        "runId": request.run_id,
        "stepId": request.step_id,
        "attempt": request.attempt,
        "activityRef": request.activity_ref,
        "inputs": dict(request.inputs),
        "connectorContexts": {
            slot: _serialize_connector_context(ctx)
            for slot, ctx in request.connector_contexts.items()
        },
        "deadline": _iso_utc(request.deadline),
    }


def _envelope_from_wire(body: Any, *, expected_attempt: int) -> ActivityResultEnvelope:
    """Parse an ARM response body into an :class:`ActivityResultEnvelope`.

    The wire envelope mirrors :class:`ActivityResultEnvelope` with
    one rename: ``class_`` is sent as ``class`` (Python keyword
    avoidance lives on the dataclass, not on the wire).

    :raises OutboundRpcDecodeError: If the body is not a mapping,
        is missing a required field, has an unsupported ``class``
        value, or the constructed envelope violates
        :meth:`ActivityResultEnvelope.__post_init__`'s invariants.
    """
    # Import lazily — see ``_serialize_connector_context``.
    from custos_workflow.clients._errors import OutboundRpcDecodeError

    if not isinstance(body, Mapping):
        raise OutboundRpcDecodeError(
            f"ARM response body must be a JSON object, got {type(body).__name__}"
        )
    if "class" not in body:
        raise OutboundRpcDecodeError("ARM response body missing required 'class' field")
    if "attempt" not in body:
        raise OutboundRpcDecodeError("ARM response body missing required 'attempt' field")

    class_value = body["class"]
    if class_value not in ACTIVITY_RESULT_CLASSES:
        raise OutboundRpcDecodeError(
            f"ARM response 'class' must be one of {sorted(ACTIVITY_RESULT_CLASSES)}, "
            f"got {class_value!r}"
        )

    attempt_value = body["attempt"]
    if isinstance(attempt_value, bool) or not isinstance(attempt_value, int):
        raise OutboundRpcDecodeError(
            f"ARM response 'attempt' must be an int, got {type(attempt_value).__name__}"
        )
    if attempt_value != expected_attempt:
        raise OutboundRpcDecodeError(
            f"ARM response 'attempt' is {attempt_value}, expected {expected_attempt} "
            "(must echo the request's attempt counter)"
        )

    outputs = body.get("outputs")
    error = body.get("error")
    if outputs is not None and not isinstance(outputs, Mapping):
        raise OutboundRpcDecodeError(
            f"ARM response 'outputs' must be a JSON object or null, got {type(outputs).__name__}"
        )
    if error is not None and not isinstance(error, Mapping):
        raise OutboundRpcDecodeError(
            f"ARM response 'error' must be a JSON object or null, got {type(error).__name__}"
        )

    try:
        return ActivityResultEnvelope(
            class_=class_value,
            outputs=outputs,
            error=error,
            attempt=attempt_value,
        )
    except ValueError as exc:
        raise OutboundRpcDecodeError(f"ARM response failed envelope invariants: {exc}") from exc


@dataclass(slots=True)
class DaprActivityRuntimeClient:
    """Production :class:`ActivityRuntimeClient` adapter over Dapr Service Invocation.

    Posts each :meth:`schedule_activity` call as
    ``Content-Type: application/json`` to
    ``…/v1.0/invoke/<arm-app-id>/method/ScheduleActivity`` against
    the local Dapr sidecar. Failure modes are normalised through
    the WF-IMPL-075 :class:`~custos_workflow.clients._errors.OutboundRpcError`
    taxonomy and rendered into the canonical
    :class:`ActivityResultEnvelope` via
    :func:`~custos_workflow.clients._errors.map_to_activity_envelope`,
    so the retry-decision driver always observes a shape-valid
    envelope regardless of which transport-layer error fired.

    The adapter does **not** own the :class:`httpx.AsyncClient`
    — the FastAPI lifespan hook (wired in WF-IMPL-080) is
    responsible for building and ``aclose``-ing the client. This
    mirrors the
    :class:`~custos_workflow.runs.events.DaprPubSubLifecyclePublisher`
    precedent.

    Idempotency
    -----------

    Every outbound request carries an ``Idempotency-Key`` header
    with the canonical
    :meth:`IdempotencyTriple.to_str` encoding of
    ``(run_id, step_id, attempt)`` so ARM can dedupe retries
    byte-for-byte against the Workflow Service's own keying.

    Method exposure
    ---------------

    Both :meth:`schedule_activity` and :meth:`cancel_activity`
    are exposed as ``async`` because the underlying transport is
    async; the orchestrator-side bridge that registers this
    client as a Dapr Workflow activity (deferred WF-IMPL-079) is
    the layer that adapts to the sync
    :class:`ActivityRuntimeClient` Protocol.

    :param http_client: Lifespan-owned async HTTP client.
    :param endpoint: Resolved Dapr Service-Invocation endpoint for
        the ARM app-id (built by
        :func:`~custos_workflow.clients._dapr_invoke.read_dapr_env`).
    :param timeout: Per-request timeout in seconds. Defaults to
        :data:`~custos_workflow.clients._dapr_invoke.DEFAULT_OUTBOUND_RPC_TIMEOUT_SECONDS`.
    """

    http_client: httpx.AsyncClient
    endpoint: DaprInvokeEndpoint
    timeout: float = DEFAULT_OUTBOUND_RPC_TIMEOUT_SECONDS

    async def schedule_activity(self, request: ScheduleActivityRequest) -> ActivityResultEnvelope:
        """Post one ``ScheduleActivity`` call through the Dapr sidecar.

        Always returns a shape-valid :class:`ActivityResultEnvelope`:
        success envelopes are passed through verbatim; every
        transport-layer failure mode (transport / status / decode /
        cancelled) is mapped to the corresponding
        :class:`ActivityResultClass` via
        :func:`~custos_workflow.clients._errors.map_to_activity_envelope`.
        """
        # Lazy import to break the top-level cycle: ``_errors``
        # imports ``ActivityResultClass`` / ``ActivityResultEnvelope``
        # from this module.
        from custos_workflow.clients._errors import (
            OutboundRpcCancelledError,
            OutboundRpcError,
            OutboundRpcStatusError,
            OutboundRpcTransportError,
            map_to_activity_envelope,
        )

        url = build_invoke_url(self.endpoint, SCHEDULE_ACTIVITY_DAPR_METHOD)
        idempotency_key = IdempotencyTriple(
            run_id=request.run_id,
            step_id=request.step_id,
            attempt=request.attempt,
        ).to_str()
        wire = _request_to_wire(request)

        try:
            try:
                response = await self.http_client.post(
                    url,
                    json=wire,
                    timeout=self.timeout,
                    headers={
                        "Content-Type": "application/json",
                        IDEMPOTENCY_HEADER: idempotency_key,
                    },
                )
            except httpx.HTTPError as exc:
                # No response observed — transport-layer failure.
                # Preserved on ``__cause__`` so the envelope mapper
                # renders the original ``httpx`` exception class +
                # message into the ``cause`` chain.
                raise OutboundRpcTransportError(
                    f"Dapr ScheduleActivity transport failure: {exc!r}"
                ) from exc

            status_code = response.status_code
            if status_code == _CLIENT_CLOSED_REQUEST_STATUS:
                raise OutboundRpcCancelledError(
                    f"Dapr ScheduleActivity cancelled upstream (HTTP {status_code})"
                )
            if status_code // 100 != 2:
                body_preview = response.text[:200] if response.text else ""
                raise OutboundRpcStatusError(
                    f"Dapr ScheduleActivity returned HTTP {status_code}: {body_preview!r}",
                    status_code=status_code,
                )

            try:
                body = response.json()
            except ValueError as exc:
                # ``ValueError`` covers both
                # :class:`json.JSONDecodeError` and any
                # httpx-internal decoding failure.
                from custos_workflow.clients._errors import OutboundRpcDecodeError

                raise OutboundRpcDecodeError(
                    f"Dapr ScheduleActivity response is not valid JSON: {exc!r}"
                ) from exc

            return _envelope_from_wire(body, expected_attempt=request.attempt)
        except OutboundRpcError as exc:
            return map_to_activity_envelope(exc, attempt=request.attempt)

    async def cancel_activity(self, run_id: str, step_id: str) -> None:
        """Post one ``CancelActivity`` call through the Dapr sidecar.

        Cancellation is idempotent end-to-end: the Workflow
        Service may issue the same cancel multiple times (e.g.
        retried during a shutdown drain), and ARM may have
        already terminated the step on its own. To keep callers'
        retry loops simple, this method silently absorbs the two
        "already-gone" outcomes:

        * ``200`` / ``204`` — ARM accepted the cancel; return.
        * ``404`` — ARM has no record of ``step_id`` (already
          purged or never started); logged at ``INFO`` and
          returned as a no-op.
        * ``409`` — ARM reports the step has already terminated;
          logged at ``INFO`` and returned as a no-op.
        * Any other ``4xx`` or ``5xx`` — raised as
          :class:`~custos_workflow.clients._errors.OutboundRpcStatusError`
          so the run-cancel path can surface it through
          :class:`~custos_workflow.runs.errors.RunControllerError`.
        * :class:`httpx.HTTPError` — raised as
          :class:`~custos_workflow.clients._errors.OutboundRpcTransportError`.

        Unlike :meth:`schedule_activity`, ``cancel_activity``
        does not return an :class:`ActivityResultEnvelope`; its
        Protocol surface is ``None`` and the caller distinguishes
        "cancel succeeded" from "cancel failed" by whether an
        exception escapes.
        """
        # Lazy import — see ``schedule_activity`` for rationale.
        from custos_workflow.clients._errors import (
            OutboundRpcStatusError,
            OutboundRpcTransportError,
        )

        url = build_invoke_url(self.endpoint, CANCEL_ACTIVITY_DAPR_METHOD)
        wire = {"runId": run_id, "stepId": step_id}

        try:
            response = await self.http_client.post(
                url,
                json=wire,
                timeout=self.timeout,
                headers={"Content-Type": "application/json"},
            )
        except httpx.HTTPError as exc:
            raise OutboundRpcTransportError(
                f"Dapr CancelActivity transport failure: {exc!r}"
            ) from exc

        status_code = response.status_code
        if status_code in (200, 204):
            return
        if status_code == 404:
            _LOGGER.info(
                "CancelActivity: ARM has no record of step (HTTP 404, treated as no-op)",
                extra={"run_id": run_id, "step_id": step_id},
            )
            return
        if status_code == 409:
            _LOGGER.info(
                "CancelActivity: ARM reports step already terminated (HTTP 409, treated as no-op)",
                extra={"run_id": run_id, "step_id": step_id},
            )
            return

        # Fallthrough: every status outside the contracted set
        # (200/204 success, 404/409 idempotent no-op) is an
        # unexpected response — 4xx, 5xx, redirects, and any
        # non-200/204 2xx alike — and is surfaced as a status
        # error so the bug is visible rather than silently
        # swallowed.
        body_preview = response.text[:200] if response.text else ""
        raise OutboundRpcStatusError(
            f"Dapr CancelActivity returned HTTP {status_code}: {body_preview!r}",
            status_code=status_code,
        )
