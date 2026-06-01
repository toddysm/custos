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
from typing import Protocol, runtime_checkable

from custos_workflow.clients._errors import OutboundRpcError

__all__ = [
    "BindForStepRequest",
    "BindForStepResponse",
    "ConnectorClient",
    "ConnectorContext",
    "FakeConnectorClient",
    "NoopConnectorClient",
    "SlotSpec",
]


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
