"""``ConnectorClient`` Protocol + ``ConnectorContext`` (WF-IMPL-050).

The Step Coordinator's :class:`ActivityStepHandler` (WF-IMPL-054)
must acquire named connector slot handles **before** scheduling
each activity attempt: the workflow declares which connector
slots the step needs, the Step Coordinator turns those slot
declarations into a :class:`BindForStepRequest`, and the
production :class:`ConnectorClient` adapter (Dapr Service
Invocation bridge — deferred sub-module) round-trips them
through Connector Service. The response is a frozen mapping of
``slot_name -> ConnectorContext`` which is then handed straight
to :class:`custos_workflow.clients.ScheduleActivityRequest.connector_contexts`.

This module ships only the contract surface and two test
doubles. The production adapter lives in the deferred *Real
Connector Client adapter* sub-module per
``design/components/workflow-service/todos.md``.

Acceptance criteria (mirrored from #421):

* :class:`ConnectorClient` is ``runtime_checkable``.
* :attr:`BindForStepResponse.contexts` is a
  :class:`types.MappingProxyType` snapshot the caller cannot
  mutate.
* 100 % coverage on this module.

Design references:

* ``design.md`` § Internal RPC (outbound) — locks the
  ``BindForStep(stepKey, slots[])`` signature.
* ``design.md`` § Operation: Execute Step — pins the
  *bind-before-schedule* ordering.
* ``design/components/workflow-service/changes/2026-05-18-002-bundle-g-binding-completion.md``
  — Bundle G binding lock-in (named ``ConnectorContext`` slot
  handles, sidecar bootstrap token stays out of band).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from types import MappingProxyType
from typing import Any, Final, Protocol, runtime_checkable

import httpx

from custos_workflow.clients._dapr_invoke import (
    DEFAULT_OUTBOUND_RPC_TIMEOUT_SECONDS,
    DaprInvokeEndpoint,
    build_invoke_url,
)
from custos_workflow.clients._errors import OutboundRpcError

__all__ = [
    "BIND_FOR_STEP_DAPR_METHOD",
    "BindForStepRequest",
    "BindForStepResponse",
    "ConnectorClient",
    "ConnectorContext",
    "DaprConnectorClient",
    "FakeConnectorClient",
    "NoopConnectorClient",
    "SlotSpec",
]

#: Dapr Service-Invocation ``method`` name for Connector Service's
#: ``BindForStep`` RPC. Pinned here so the adapter and any
#: smoke-test fixture key off the same constant.
BIND_FOR_STEP_DAPR_METHOD: Final[str] = "BindForStep"

#: HTTP status code the Dapr sidecar surfaces when an upstream
#: cancelled the request (nginx-style ``client-closed-request``).
#: Mapped to :class:`OutboundRpcCancelledError` rather than
#: :class:`OutboundRpcStatusError` so callers can short-circuit
#: cleanly instead of retrying a request that no longer matters.
_CLIENT_CLOSED_REQUEST_STATUS: Final[int] = 499


# ---------------------------------------------------------------------------
# Connector slot declarations
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SlotSpec:
    """Single connector slot the step references.

    The Step Coordinator builds one :class:`SlotSpec` per
    ``connectorRef`` slot the compiled step declares; Connector
    Service uses :attr:`connector_ref` to resolve the concrete
    connector instance and :attr:`capabilities` to validate that
    the bound connector covers every capability the step's
    activity will call (per ``design.md`` § Internal RPC
    (outbound)).

    :attr:`capabilities` is stored as a ``tuple`` (not a ``list``
    or ``set``) so the whole spec is hashable and the caller can
    stash it in a cache key without copying. Order is preserved
    so test assertions can pin it. An empty tuple is valid — it
    means *bind without an explicit capability check*; the
    connector adapter still enforces its own.

    :raises ValueError: If :attr:`name` or :attr:`connector_ref`
        is empty.
    """

    name: str
    connector_ref: str
    capabilities: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("SlotSpec.name must be a non-empty string")
        if not self.connector_ref:
            raise ValueError("SlotSpec.connector_ref must be a non-empty string")


@dataclass(frozen=True, slots=True)
class ConnectorContext:
    """Opaque slot handle Connector Service hands back to the workflow.

    The Step Coordinator never inspects :attr:`handle` — it just
    forwards the context unchanged in
    :class:`ScheduleActivityRequest.connector_contexts` so the
    Activity Runtime Manager + connector sidecar can dereference
    it inside the activity container.

    :attr:`expires_at` is informational at the Step Coordinator
    layer: the retry decision driver (WF-IMPL-053) compares it
    against the clock when deciding whether a stale context
    needs to be re-bound on the next attempt.

    The dataclass is :func:`dataclasses.dataclass(frozen=True,
    slots=True)` which makes it hashable as long as every field
    is hashable — :class:`datetime.datetime` is, so a
    :class:`ConnectorContext` can land in a ``set`` or a ``dict``
    key. Hashability is part of the locked contract (see
    acceptance criteria for #421).

    :raises ValueError: If :attr:`slot_name`, :attr:`handle`, or
        :attr:`connector_kind` is empty, or :attr:`expires_at` is
        naive (no tzinfo).
    """

    slot_name: str
    handle: str
    expires_at: datetime
    connector_kind: str

    def __post_init__(self) -> None:
        if not self.slot_name:
            raise ValueError("ConnectorContext.slot_name must be a non-empty string")
        if not self.handle:
            raise ValueError("ConnectorContext.handle must be a non-empty string")
        if not self.connector_kind:
            raise ValueError("ConnectorContext.connector_kind must be a non-empty string")
        if self.expires_at.tzinfo is None:
            raise ValueError(
                "ConnectorContext.expires_at must be timezone-aware "
                "(use datetime.UTC for absolute deadlines)"
            )


# ---------------------------------------------------------------------------
# Request / response envelopes
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class BindForStepRequest:
    """Frozen request envelope passed to :meth:`ConnectorClient.bind_for_step`.

    :attr:`slots` is a ``tuple`` so the whole request is hashable
    and the production adapter can stash the request alongside
    its tracing span without defensive copying.

    :raises ValueError: If :attr:`step_key` is empty or
        :attr:`slots` contains two entries with the same
        :attr:`SlotSpec.name` (slot names must be unique within
        a single bind call — the response is keyed by them).
    """

    step_key: str
    slots: tuple[SlotSpec, ...]

    def __post_init__(self) -> None:
        if not self.step_key:
            raise ValueError("BindForStepRequest.step_key must be a non-empty string")
        seen: set[str] = set()
        for spec in self.slots:
            if spec.name in seen:
                raise ValueError(
                    f"BindForStepRequest.slots contains duplicate slot name {spec.name!r}; "
                    "Connector Service keys the response by slot name so duplicates would "
                    "shadow each other."
                )
            seen.add(spec.name)


@dataclass(frozen=True, slots=True)
class BindForStepResponse:
    """Frozen response envelope returned by :meth:`ConnectorClient.bind_for_step`.

    :attr:`contexts` is exposed as a :class:`types.MappingProxyType`
    snapshot so the Step Coordinator (and any test that captures
    a response) cannot mutate the mapping in place. The mapping
    keys are slot names (matching
    :attr:`SlotSpec.name` on the corresponding request) and the
    values are the opaque :class:`ConnectorContext` handles.

    Construction normalises whatever :class:`Mapping` the caller
    hands in into a :class:`MappingProxyType` so the contract
    holds regardless of how the production adapter assembles the
    response.

    :raises ValueError: If any context's
        :attr:`ConnectorContext.slot_name` does not match its
        mapping key.
    """

    contexts: Mapping[str, ConnectorContext]

    def __post_init__(self) -> None:
        # Validate slot_name ↔ key alignment first so the error
        # message points at the offender before we freeze the
        # snapshot.
        for slot_name, ctx in self.contexts.items():
            if ctx.slot_name != slot_name:
                raise ValueError(
                    f"BindForStepResponse.contexts[{slot_name!r}].slot_name "
                    f"is {ctx.slot_name!r}; the key and the context's slot_name "
                    "must agree so the Step Coordinator can index by either."
                )
        # Snapshot into a MappingProxyType so the consumer cannot
        # mutate the response after the fact. Bypass the frozen
        # dataclass guard with object.__setattr__ — this is the
        # documented escape hatch for __post_init__ normalisation
        # on frozen dataclasses.
        if not isinstance(self.contexts, MappingProxyType):
            object.__setattr__(self, "contexts", MappingProxyType(dict(self.contexts)))


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class ConnectorClient(Protocol):
    """Runtime-checkable Protocol the Step Coordinator depends on.

    The Step Coordinator only ever calls :meth:`bind_for_step`;
    the production Dapr Service Invocation adapter (deferred
    sub-module) and the in-memory :class:`FakeConnectorClient`
    test double both satisfy this Protocol structurally.
    """

    def bind_for_step(self, request: BindForStepRequest) -> BindForStepResponse:
        """Bind every slot the step declares and return their contexts.

        The call is synchronous from the Step Coordinator's
        perspective — the production adapter is the layer that
        bridges to Dapr Service Invocation under the hood, hiding
        the async boundary from every consumer.
        """
        ...


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class NoopConnectorClient:
    """Safe default that explicitly :class:`NotImplementedError`-s every call.

    Wired by the FastAPI lifespan (WF-IMPL-057) at startup so the
    process does *not* silently accept bind requests before the
    real adapter is installed.
    """

    def bind_for_step(self, request: BindForStepRequest) -> BindForStepResponse:
        raise NotImplementedError(
            "NoopConnectorClient.bind_for_step: "
            "no production ConnectorClient adapter is wired yet "
            "(deferred sub-module: Real Connector Client adapter)."
        )


@dataclass(slots=True)
class FakeConnectorClient:
    """In-memory test double that returns canned responses.

    Pass a list of pre-built :class:`BindForStepResponse`
    instances on :attr:`responses`; each call to
    :meth:`bind_for_step` pops the next response in FIFO order.
    Every call is recorded on :attr:`calls` so tests can assert
    call patterns without monkey-patching.

    Raises :class:`IndexError` if a test binds more steps than it
    queued — almost always a sign the test is missing a canned
    response, so failing loud beats returning a default.
    """

    responses: list[BindForStepResponse] = field(default_factory=list)
    calls: list[BindForStepRequest] = field(default_factory=list)

    def bind_for_step(self, request: BindForStepRequest) -> BindForStepResponse:
        self.calls.append(request)
        if not self.responses:
            raise IndexError(
                "FakeConnectorClient.bind_for_step: "
                "no more canned responses queued "
                f"(called for step_key={request.step_key!r})."
            )
        return self.responses.pop(0)


# ---------------------------------------------------------------------------
# Client-layer error surface
# ---------------------------------------------------------------------------


# ``ConnectorBindError`` at the client layer is the structured error
# the future production :class:`ConnectorClient` adapter
# (WF-IMPL-078) raises when its outbound RPC fails. It extends
# :class:`~custos_workflow.clients._errors.OutboundRpcError` so the
# locked taxonomy applies — concrete failure modes are surfaced via
# the four concrete ``OutboundRpcError`` subclasses, which the
# adapter raises directly; the handler layer
# (:class:`custos_workflow.steps.errors.ConnectorBindError`,
# a distinct ``StepCoordinatorError``) is what wraps those into
# step-result envelopes. Deliberately omitted from ``__all__`` so
# this module's public surface doesn't gain a new name (the
# adapter wires it via a fully-qualified import).
class ConnectorBindError(OutboundRpcError):
    """Marker subclass for connector-bind transport failures.

    Concrete adapter code raises one of the four concrete
    :class:`OutboundRpcError` subclasses
    (:class:`OutboundRpcTransportError`,
    :class:`OutboundRpcStatusError`,
    :class:`OutboundRpcDecodeError`,
    :class:`OutboundRpcCancelledError`); this marker is reserved
    for cases where the adapter needs to wrap an already-classified
    structured error with bind-call context without inventing a
    fifth bucket. Inherits the locked ``kind`` enforcement from
    :class:`OutboundRpcError.__init_subclass__`, so a concrete
    bind-error subclass cannot ship with an unknown ``kind``.
    """


# ---------------------------------------------------------------------------
# Production adapter: Dapr Service-Invocation HTTP transport
# ---------------------------------------------------------------------------


def _request_to_wire(request: BindForStepRequest) -> Mapping[str, Any]:
    """Render a :class:`BindForStepRequest` to its camelCase wire form.

    The wire envelope is pinned in ``design.md`` § *Internal RPC
    outbound* — :attr:`SlotSpec.capabilities` order is preserved
    so Connector Service's audit log reflects exactly what the
    Step Coordinator declared.
    """
    return {
        "stepKey": request.step_key,
        "slots": [
            {
                "name": spec.name,
                "connectorRef": spec.connector_ref,
                "capabilities": list(spec.capabilities),
            }
            for spec in request.slots
        ],
    }


def _parse_iso_utc(value: Any) -> datetime:
    """Parse a wire ``expiresAt`` string into a tz-aware datetime.

    Accepts the canonical ``…Z`` suffix Connector Service emits
    (per ``design.md`` § *Internal RPCs*) as well as any explicit
    ``±HH:MM`` offset. Naïve timestamps and non-string values are
    rejected with :class:`ValueError` so the caller can surface
    them as :class:`OutboundRpcDecodeError`.
    """
    if not isinstance(value, str):
        raise ValueError(f"expiresAt must be an ISO-8601 string, got {type(value).__name__}")
    # ``datetime.fromisoformat`` rejects a trailing ``Z`` before
    # Python 3.11; normalise to ``+00:00`` so the adapter works
    # uniformly on the CI matrix.
    normalised = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalised)
    except ValueError as exc:
        raise ValueError(f"expiresAt is not a valid ISO-8601 timestamp: {value!r}") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"expiresAt must be timezone-aware (no trailing Z or offset): {value!r}")
    return parsed


def _response_from_wire(body: Any) -> BindForStepResponse:
    """Reconstruct a :class:`BindForStepResponse` from a wire body.

    Validates every contract the Step Coordinator depends on:

    * Body is a mapping with a single ``"contexts"`` key whose
      value is itself a mapping (per ``design.md`` § *Internal
      RPCs*).
    * Each context entry carries the four required keys
      (``slotName`` / ``handle`` / ``expiresAt`` /
      ``connectorKind``).
    * ``expiresAt`` parses to a tz-aware datetime; naïve values
      are rejected up-front (mirrored by
      :class:`ConnectorContext.__post_init__`).
    * Slot-name ↔ key alignment matches (mirrored by
      :class:`BindForStepResponse.__post_init__`).

    Any contract violation surfaces as
    :class:`OutboundRpcDecodeError` so the retry driver routes
    the failure as ``permanent`` (a malformed response is a
    contract violation, not a transient).
    """
    # Lazy import to keep ``_errors`` out of this module's top-level
    # imports — ``_errors`` already imports the activity-runtime
    # module and adding ``connector`` to its top imports would
    # close a circular ring.
    from custos_workflow.clients._errors import OutboundRpcDecodeError

    if not isinstance(body, Mapping):
        raise OutboundRpcDecodeError(
            f"Connector BindForStep response body must be a JSON object, got {type(body).__name__}"
        )
    contexts_raw = body.get("contexts")
    if contexts_raw is None:
        raise OutboundRpcDecodeError(
            "Connector BindForStep response is missing the required 'contexts' field"
        )
    if not isinstance(contexts_raw, Mapping):
        raise OutboundRpcDecodeError(
            f"Connector BindForStep response 'contexts' must be a JSON object, "
            f"got {type(contexts_raw).__name__}"
        )

    rebuilt: dict[str, ConnectorContext] = {}
    for slot_name, raw_ctx in contexts_raw.items():
        if not isinstance(raw_ctx, Mapping):
            raise OutboundRpcDecodeError(
                f"Connector BindForStep response contexts[{slot_name!r}] "
                f"must be a JSON object, got {type(raw_ctx).__name__}"
            )
        missing = {"slotName", "handle", "expiresAt", "connectorKind"} - set(raw_ctx)
        if missing:
            raise OutboundRpcDecodeError(
                f"Connector BindForStep response contexts[{slot_name!r}] "
                f"is missing required field(s): {sorted(missing)!r}"
            )
        try:
            expires_at = _parse_iso_utc(raw_ctx["expiresAt"])
        except ValueError as exc:
            raise OutboundRpcDecodeError(
                f"Connector BindForStep response contexts[{slot_name!r}].expiresAt "
                f"is invalid: {exc}"
            ) from exc
        try:
            ctx = ConnectorContext(
                slot_name=raw_ctx["slotName"],
                handle=raw_ctx["handle"],
                expires_at=expires_at,
                connector_kind=raw_ctx["connectorKind"],
            )
        except (TypeError, ValueError) as exc:
            raise OutboundRpcDecodeError(
                f"Connector BindForStep response contexts[{slot_name!r}] "
                f"failed ConnectorContext invariants: {exc}"
            ) from exc
        rebuilt[slot_name] = ctx

    try:
        return BindForStepResponse(contexts=rebuilt)
    except ValueError as exc:
        # Slot-name ↔ key mismatch enforced by
        # ``BindForStepResponse.__post_init__``.
        raise OutboundRpcDecodeError(
            f"Connector BindForStep response failed BindForStepResponse invariants: {exc}"
        ) from exc


@dataclass(slots=True)
class DaprConnectorClient:
    """Production :class:`ConnectorClient` adapter over Dapr Service Invocation.

    Posts each :meth:`bind_for_step` call as
    ``Content-Type: application/json`` to
    ``…/v1.0/invoke/<connector-app-id>/method/BindForStep`` against
    the local Dapr sidecar. Failure modes are normalised through
    the WF-IMPL-075
    :class:`~custos_workflow.clients._errors.OutboundRpcError`
    taxonomy so the retry-decision driver classifies bind failures
    the same way it classifies activity-scheduling failures.

    The adapter does **not** own the :class:`httpx.AsyncClient`
    — the FastAPI lifespan hook (wired in WF-IMPL-080) is
    responsible for building and ``aclose``-ing the client.

    Method exposure
    ---------------

    :meth:`bind_for_step` is exposed as ``async`` because the
    underlying transport is async; the Step Coordinator's
    activity-task bridge (WF-IMPL-079) adapts the async
    boundary to the sync :class:`ConnectorClient` Protocol.

    :param http_client: Lifespan-owned async HTTP client.
    :param endpoint: Resolved Dapr Service-Invocation endpoint for
        the Connector Service app-id (built by
        :func:`~custos_workflow.clients._dapr_invoke.read_dapr_env`).
    :param timeout: Per-request timeout in seconds. Defaults to
        :data:`~custos_workflow.clients._dapr_invoke.DEFAULT_OUTBOUND_RPC_TIMEOUT_SECONDS`.
    """

    http_client: httpx.AsyncClient
    endpoint: DaprInvokeEndpoint
    timeout: float = DEFAULT_OUTBOUND_RPC_TIMEOUT_SECONDS

    async def bind_for_step(self, request: BindForStepRequest) -> BindForStepResponse:
        """Post one ``BindForStep`` call through the Dapr sidecar.

        Always returns a :class:`BindForStepResponse` with a
        :class:`MappingProxyType`-frozen ``contexts`` mapping on
        success. Every transport-layer failure mode is raised as
        the appropriate
        :class:`~custos_workflow.clients._errors.OutboundRpcError`
        subclass:

        * Transport failure (no response observed) →
          :class:`OutboundRpcTransportError`.
        * HTTP 499 (upstream cancelled) →
          :class:`OutboundRpcCancelledError`.
        * Any other non-2xx →
          :class:`OutboundRpcStatusError` carrying the observed
          ``status_code`` (the WF-IMPL-075 mapper classifies
          408 / 429 / 5xx as retryable and the remaining 4xx as
          permanent).
        * Response body that isn't valid JSON, missing required
          fields, mismatched slot keys, or carrying a naïve
          ``expiresAt`` → :class:`OutboundRpcDecodeError`
          (always permanent — a malformed response is a contract
          violation).
        """
        # Lazy import to break the top-level cycle: ``_errors``
        # imports ``ActivityResultClass`` / ``ActivityResultEnvelope``
        # which keeps the dependency arrow pointing one way.
        from custos_workflow.clients._errors import (
            OutboundRpcCancelledError,
            OutboundRpcDecodeError,
            OutboundRpcStatusError,
            OutboundRpcTransportError,
        )

        url = build_invoke_url(self.endpoint, BIND_FOR_STEP_DAPR_METHOD)
        wire = _request_to_wire(request)

        try:
            response = await self.http_client.post(
                url,
                json=wire,
                timeout=self.timeout,
                headers={"Content-Type": "application/json"},
            )
        except httpx.HTTPError as exc:
            # No response observed — transport-layer failure.
            # Original ``httpx`` exception preserved on
            # ``__cause__`` so the envelope mapper renders it
            # into the ``cause`` chain.
            raise OutboundRpcTransportError(f"Dapr BindForStep transport failure: {exc!r}") from exc

        status_code = response.status_code
        if status_code == _CLIENT_CLOSED_REQUEST_STATUS:
            raise OutboundRpcCancelledError(
                f"Dapr BindForStep cancelled upstream (HTTP {status_code})"
            )
        if status_code // 100 != 2:
            body_preview = response.text[:200] if response.text else ""
            raise OutboundRpcStatusError(
                f"Dapr BindForStep returned HTTP {status_code}: {body_preview!r}",
                status_code=status_code,
            )

        try:
            body = response.json()
        except ValueError as exc:
            # Covers ``json.JSONDecodeError`` and any
            # httpx-internal decoding failure.
            raise OutboundRpcDecodeError(
                f"Dapr BindForStep response is not valid JSON: {exc!r}"
            ) from exc

        return _response_from_wire(body)
