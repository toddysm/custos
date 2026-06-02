"""``TriggerServiceClient`` Protocol + resume-subscription RPC models (WF-IMPL-101).

The Resume Subscription Manager (REQ-081) drives two outbound RPCs
against the Trigger Service (COMP-004) across each ``waitFor:``
step's lifecycle:

* ``RegisterResumeSubscription(runId, stepId, eventKey, selector, ttl)``
  — registered when the Step Coordinator parks a wait-style step,
  and **idempotently re-registered** on every Dapr Workflow replay
  (the ``(runId, stepId, eventKey)`` tuple is the idempotency key —
  see ``design.md`` § *Resume Subscription Replay Protocol*).
* ``CancelResumeSubscription(runId, stepId, eventKey)`` — issued
  on step/run terminal transition for every open mirror; the
  Trigger Service treats it as idempotent (cancelling an unknown
  or already-expired key is a no-op).

This module ships only the contract surface and two test doubles.
The Trigger Service is **not yet implemented** (COMP-004 has no
source), so the fake unblocks the rest of the sub-module; the
production Dapr Service-Invocation adapter lands separately in
``WF-IMPL-103``.

Acceptance criteria (mirrored from #540):

* :class:`TriggerServiceClient` is ``runtime_checkable``.
* The idempotent :class:`FakeTriggerServiceClient` returns the
  same ``tsSubscriptionId`` for a repeated
  ``(runId, stepId, eventKey)``.
* 100 % coverage on this module.

Design references:

* ``design.md`` § *Operation: Step Resume on External Event
  (REQ-081)* — locks the ``RegisterResumeSubscription`` /
  ``CancelResumeSubscription`` signatures.
* ``design.md`` § *Resume Subscription Replay Protocol* — pins the
  ``(runId, stepId, eventKey)`` idempotency key and the
  *original-wins* divergence policy the fake mirrors.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Final, Protocol, runtime_checkable

import httpx

from custos_workflow.clients._dapr_invoke import (
    DEFAULT_OUTBOUND_RPC_TIMEOUT_SECONDS,
    DaprInvokeEndpoint,
    build_invoke_url,
)

__all__ = [
    "CANCEL_RESUME_SUBSCRIPTION_DAPR_METHOD",
    "REGISTER_RESUME_SUBSCRIPTION_DAPR_METHOD",
    "CancelResumeSubscriptionRequest",
    "DaprTriggerServiceClient",
    "FakeTriggerServiceClient",
    "NoopTriggerServiceClient",
    "RegisterResumeSubscriptionRequest",
    "RegisterResumeSubscriptionResponse",
    "TriggerServiceClient",
]

#: Dapr Service-Invocation ``method`` name for the Trigger
#: Service's ``RegisterResumeSubscription`` RPC. Pinned here so the
#: production adapter (WF-IMPL-103) and any smoke-test fixture key
#: off the same constant.
REGISTER_RESUME_SUBSCRIPTION_DAPR_METHOD: Final[str] = "RegisterResumeSubscription"

#: Dapr Service-Invocation ``method`` name for the Trigger
#: Service's ``CancelResumeSubscription`` RPC.
CANCEL_RESUME_SUBSCRIPTION_DAPR_METHOD: Final[str] = "CancelResumeSubscription"

#: HTTP status the Dapr sidecar surfaces when an upstream cancelled
#: the request (nginx-style ``client-closed-request``). Mapped to
#: :class:`OutboundRpcCancelledError` so the caller short-circuits
#: instead of retrying a request that no longer matters. Mirrors
#: :data:`custos_workflow.clients.connector._CLIENT_CLOSED_REQUEST_STATUS`.
_CLIENT_CLOSED_REQUEST_STATUS: Final[int] = 499

#: HTTP statuses the Trigger Service returns when a
#: ``CancelResumeSubscription`` targets a key it no longer holds
#: (already-expired / never-registered). The cancel RPC is
#: idempotent (``design.md`` § *Resume Subscription Replay
#: Protocol*, cancellation rule), so the adapter treats both as a
#: clean no-op rather than an error.
_CANCEL_NOOP_STATUSES: Final[frozenset[int]] = frozenset({404, 409})


# ---------------------------------------------------------------------------
# Request / response envelopes
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RegisterResumeSubscriptionRequest:
    """Frozen request for :meth:`TriggerServiceClient.register_resume_subscription`.

    Carries the **resolved** values the Step Coordinator computes
    before parking a ``waitFor:`` step: :attr:`event_key` is the
    concrete resume-event key (the CEL ``eventKey`` already
    evaluated), :attr:`selector` is the optional narrowing
    predicate (``None`` means *match on event key alone*), and
    :attr:`ttl` is the resolved ISO-8601 duration the subscription
    stays live (the caller applies ``WF_RESUME_SUB_DEFAULT_TTL`` —
    ``PT24H`` — before constructing the request, so it is always a
    non-empty concrete value here).

    The ``(run_id, step_id, event_key)`` triple is the idempotency
    key the Trigger Service dedups on (``design.md`` § *Resume
    Subscription Replay Protocol*); the fields are stored on a
    frozen, hashable dataclass so a caller can stash the request
    in a replay cache without defensive copying.

    :raises ValueError: If :attr:`run_id`, :attr:`step_id`,
        :attr:`event_key`, or :attr:`ttl` is empty, or
        :attr:`selector` is an empty string (pass ``None`` to mean
        *no selector* — an empty string is almost always a bug
        where an unresolved optional leaked through).
    """

    run_id: str
    step_id: str
    event_key: str
    ttl: str
    selector: str | None = None

    def __post_init__(self) -> None:
        if not self.run_id:
            raise ValueError("RegisterResumeSubscriptionRequest.run_id must be a non-empty string")
        if not self.step_id:
            raise ValueError("RegisterResumeSubscriptionRequest.step_id must be a non-empty string")
        if not self.event_key:
            raise ValueError(
                "RegisterResumeSubscriptionRequest.event_key must be a non-empty string"
            )
        if not self.ttl:
            raise ValueError("RegisterResumeSubscriptionRequest.ttl must be a non-empty string")
        if self.selector is not None and not self.selector:
            raise ValueError(
                "RegisterResumeSubscriptionRequest.selector must be None or a non-empty "
                "string (an empty string usually means an unresolved optional leaked through)"
            )

    @property
    def idempotency_key(self) -> tuple[str, str, str]:
        """The ``(run_id, step_id, event_key)`` tuple the Trigger Service dedups on.

        Exposed so callers (and the fake) share one definition of
        the replay idempotency key instead of re-deriving it.
        """
        return (self.run_id, self.step_id, self.event_key)


@dataclass(frozen=True, slots=True)
class RegisterResumeSubscriptionResponse:
    """Frozen response for :meth:`TriggerServiceClient.register_resume_subscription`.

    :attr:`ts_subscription_id` is the Trigger Service's handle for
    the registration (``subscriptionId`` on the wire). The Step
    Coordinator stores it on the ``ResumeSubscriptionMirror`` so a
    later replay can detect a re-issued id (e.g. after TTL expiry)
    and update the mirror to point at the new one.

    :raises ValueError: If :attr:`ts_subscription_id` is empty.
    """

    ts_subscription_id: str

    def __post_init__(self) -> None:
        if not self.ts_subscription_id:
            raise ValueError(
                "RegisterResumeSubscriptionResponse.ts_subscription_id must be a non-empty string"
            )


@dataclass(frozen=True, slots=True)
class CancelResumeSubscriptionRequest:
    """Frozen request for :meth:`TriggerServiceClient.cancel_resume_subscription`.

    Cancellation keys off the same ``(run_id, step_id, event_key)``
    idempotency triple as registration — the Trigger Service does
    not need the ``tsSubscriptionId`` to cancel, and treats an
    unknown or already-expired key as a no-op (``design.md``
    § *Resume Subscription Replay Protocol*, cancellation rule).

    :raises ValueError: If :attr:`run_id`, :attr:`step_id`, or
        :attr:`event_key` is empty.
    """

    run_id: str
    step_id: str
    event_key: str

    def __post_init__(self) -> None:
        if not self.run_id:
            raise ValueError("CancelResumeSubscriptionRequest.run_id must be a non-empty string")
        if not self.step_id:
            raise ValueError("CancelResumeSubscriptionRequest.step_id must be a non-empty string")
        if not self.event_key:
            raise ValueError("CancelResumeSubscriptionRequest.event_key must be a non-empty string")

    @property
    def idempotency_key(self) -> tuple[str, str, str]:
        """The ``(run_id, step_id, event_key)`` tuple shared with registration."""
        return (self.run_id, self.step_id, self.event_key)


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class TriggerServiceClient(Protocol):
    """Runtime-checkable Protocol the Resume Subscription Manager depends on.

    The manager only ever calls
    :meth:`register_resume_subscription` and
    :meth:`cancel_resume_subscription`; the production Dapr
    Service-Invocation adapter (WF-IMPL-103) and the in-memory
    :class:`FakeTriggerServiceClient` test double both satisfy this
    Protocol structurally.
    """

    def register_resume_subscription(
        self, request: RegisterResumeSubscriptionRequest
    ) -> RegisterResumeSubscriptionResponse:
        """Register (or idempotently re-register) a resume subscription.

        Re-registering the same ``(run_id, step_id, event_key)``
        returns the existing ``tsSubscriptionId`` rather than
        creating a duplicate. The call is synchronous from the
        manager's perspective — the production adapter hides the
        Dapr async boundary.
        """
        ...

    def cancel_resume_subscription(self, request: CancelResumeSubscriptionRequest) -> None:
        """Cancel an open resume subscription; a no-op for unknown keys."""
        ...


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class NoopTriggerServiceClient:
    """Safe default that explicitly :class:`NotImplementedError`-s every call.

    Wired by the FastAPI lifespan at startup so the process does
    *not* silently accept resume-subscription registrations before
    the real adapter (WF-IMPL-103) is installed.
    """

    def register_resume_subscription(
        self, request: RegisterResumeSubscriptionRequest
    ) -> RegisterResumeSubscriptionResponse:
        raise NotImplementedError(
            "NoopTriggerServiceClient.register_resume_subscription: "
            "no production TriggerServiceClient adapter is wired yet "
            "(deferred sub-module: DaprTriggerServiceClient, WF-IMPL-103)."
        )

    def cancel_resume_subscription(self, request: CancelResumeSubscriptionRequest) -> None:
        raise NotImplementedError(
            "NoopTriggerServiceClient.cancel_resume_subscription: "
            "no production TriggerServiceClient adapter is wired yet "
            "(deferred sub-module: DaprTriggerServiceClient, WF-IMPL-103)."
        )


@dataclass(slots=True)
class FakeTriggerServiceClient:
    """In-memory idempotent test double for the Trigger Service.

    Mirrors the Trigger Service's replay contract so the rest of
    the Resume Subscription Manager can be tested without standing
    up Dapr:

    * :meth:`register_resume_subscription` mints a fresh
      ``tsSubscriptionId`` the first time it sees a
      ``(run_id, step_id, event_key)`` key and returns the **same**
      id for every subsequent registration of that key — exactly
      the idempotency the design pins. Minted ids are
      ``f"{id_prefix}{n}"`` with a monotonically increasing ``n``
      so tests can assert deterministic values.
    * :meth:`cancel_resume_subscription` forgets the key (so a
      later registration mints a fresh id, modelling TTL expiry /
      genuine re-registration) and is a no-op for an unknown key.

    Every call is recorded on :attr:`register_calls` /
    :attr:`cancel_calls` so tests can assert call patterns without
    monkey-patching. :attr:`subscriptions` exposes the current
    key → id map for direct inspection.
    """

    id_prefix: str = "ts-sub-"
    subscriptions: dict[tuple[str, str, str], str] = field(default_factory=dict)
    register_calls: list[RegisterResumeSubscriptionRequest] = field(default_factory=list)
    cancel_calls: list[CancelResumeSubscriptionRequest] = field(default_factory=list)
    _next_id: int = 1

    def register_resume_subscription(
        self, request: RegisterResumeSubscriptionRequest
    ) -> RegisterResumeSubscriptionResponse:
        self.register_calls.append(request)
        key = request.idempotency_key
        existing = self.subscriptions.get(key)
        if existing is not None:
            return RegisterResumeSubscriptionResponse(ts_subscription_id=existing)
        minted = f"{self.id_prefix}{self._next_id}"
        self._next_id += 1
        self.subscriptions[key] = minted
        return RegisterResumeSubscriptionResponse(ts_subscription_id=minted)

    def cancel_resume_subscription(self, request: CancelResumeSubscriptionRequest) -> None:
        self.cancel_calls.append(request)
        # Idempotent: cancelling an unknown / already-cancelled key
        # is a no-op, matching the Trigger Service contract.
        self.subscriptions.pop(request.idempotency_key, None)


# ---------------------------------------------------------------------------
# Production adapter: Dapr Service-Invocation HTTP transport
# ---------------------------------------------------------------------------


def _register_request_to_wire(request: RegisterResumeSubscriptionRequest) -> Mapping[str, Any]:
    """Render a register request to its camelCase wire form.

    The envelope matches ``design.md`` § *Operation: Step Resume on
    External Event* — ``selector`` is emitted as JSON ``null`` when
    the request carries no selector so the Trigger Service can tell
    *match on event key alone* apart from an empty narrowing
    predicate.
    """
    return {
        "runId": request.run_id,
        "stepId": request.step_id,
        "eventKey": request.event_key,
        "selector": request.selector,
        "ttl": request.ttl,
    }


def _cancel_request_to_wire(request: CancelResumeSubscriptionRequest) -> Mapping[str, Any]:
    """Render a cancel request to its camelCase wire form."""
    return {
        "runId": request.run_id,
        "stepId": request.step_id,
        "eventKey": request.event_key,
    }


def _parse_register_response(body: Any) -> RegisterResumeSubscriptionResponse:
    """Reconstruct a :class:`RegisterResumeSubscriptionResponse` from a wire body.

    The Trigger Service returns ``{"subscriptionId": "..."}`` (per
    ``design.md`` § *Operation: Step Resume on External Event*).
    Any contract violation — non-object body, missing / non-string /
    empty ``subscriptionId`` — surfaces as
    :class:`OutboundRpcDecodeError` so the retry driver routes the
    failure as ``permanent`` (a malformed response is a contract
    violation, not a transient).
    """
    from custos_workflow.clients._errors import OutboundRpcDecodeError

    if not isinstance(body, Mapping):
        raise OutboundRpcDecodeError(
            "Trigger RegisterResumeSubscription response body must be a JSON object, "
            f"got {type(body).__name__}"
        )
    subscription_id = body.get("subscriptionId")
    if subscription_id is None:
        raise OutboundRpcDecodeError(
            "Trigger RegisterResumeSubscription response is missing the required "
            "'subscriptionId' field"
        )
    if not isinstance(subscription_id, str):
        raise OutboundRpcDecodeError(
            "Trigger RegisterResumeSubscription response 'subscriptionId' must be a string, "
            f"got {type(subscription_id).__name__}"
        )
    try:
        return RegisterResumeSubscriptionResponse(ts_subscription_id=subscription_id)
    except ValueError as exc:
        # Empty ``subscriptionId`` rejected by the envelope invariant.
        raise OutboundRpcDecodeError(
            "Trigger RegisterResumeSubscription response failed "
            f"RegisterResumeSubscriptionResponse invariants: {exc}"
        ) from exc


@dataclass(slots=True)
class DaprTriggerServiceClient:
    """Production :class:`TriggerServiceClient` adapter over Dapr Service Invocation.

    Posts each RPC as ``Content-Type: application/json`` to
    ``…/v1.0/invoke/<trigger-app-id>/method/<Method>`` against the
    local Dapr sidecar. Failure modes are normalised through the
    WF-IMPL-075
    :class:`~custos_workflow.clients._errors.OutboundRpcError`
    taxonomy so the retry-decision driver classifies resume-
    subscription failures the same way it classifies activity-
    scheduling failures.

    The adapter does **not** own the :class:`httpx.AsyncClient` —
    the FastAPI lifespan hook is responsible for building and
    ``aclose``-ing the client, mirroring
    :class:`~custos_workflow.clients.connector.DaprConnectorClient`.

    Method exposure
    ---------------

    Both methods are exposed as ``async`` because the underlying
    transport is async; the Resume Subscription Manager adapts the
    async boundary to the sync :class:`TriggerServiceClient`
    Protocol, exactly as the Step Coordinator does for
    :class:`DaprConnectorClient`.

    :param http_client: Lifespan-owned async HTTP client.
    :param endpoint: Resolved Dapr Service-Invocation endpoint for
        the Trigger Service app-id (built by
        :func:`~custos_workflow.clients._dapr_invoke.read_dapr_env`).
    :param timeout: Per-request timeout in seconds. Defaults to
        :data:`~custos_workflow.clients._dapr_invoke.DEFAULT_OUTBOUND_RPC_TIMEOUT_SECONDS`.
    """

    http_client: httpx.AsyncClient
    endpoint: DaprInvokeEndpoint
    timeout: float = DEFAULT_OUTBOUND_RPC_TIMEOUT_SECONDS

    async def register_resume_subscription(
        self, request: RegisterResumeSubscriptionRequest
    ) -> RegisterResumeSubscriptionResponse:
        """Post one ``RegisterResumeSubscription`` call through the Dapr sidecar.

        Returns the :class:`RegisterResumeSubscriptionResponse`
        carrying the Trigger Service's ``subscriptionId`` on
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
          ``status_code``.
        * Response body that isn't valid JSON or violates the
          envelope contract → :class:`OutboundRpcDecodeError`.
        """
        from custos_workflow._telemetry import observe_outbound_rpc
        from custos_workflow.clients._errors import (
            OutboundRpcCancelledError,
            OutboundRpcDecodeError,
            OutboundRpcStatusError,
            OutboundRpcTransportError,
        )

        url = build_invoke_url(self.endpoint, REGISTER_RESUME_SUBSCRIPTION_DAPR_METHOD)
        wire = _register_request_to_wire(request)

        async with observe_outbound_rpc(
            client="trigger",
            method=REGISTER_RESUME_SUBSCRIPTION_DAPR_METHOD,
            run_id=request.run_id,
            step_id=request.step_id,
        ) as obs_ctx:
            try:
                response = await self.http_client.post(
                    url,
                    json=wire,
                    timeout=self.timeout,
                    headers={"Content-Type": "application/json"},
                )
            except httpx.HTTPError as exc:
                raise OutboundRpcTransportError(
                    f"Dapr RegisterResumeSubscription transport failure: {exc!r}"
                ) from exc

            status_code = response.status_code
            obs_ctx.set_status_code(status_code)
            if status_code == _CLIENT_CLOSED_REQUEST_STATUS:
                raise OutboundRpcCancelledError(
                    f"Dapr RegisterResumeSubscription cancelled upstream (HTTP {status_code})"
                )
            if status_code // 100 != 2:
                body_preview = response.text[:200] if response.text else ""
                raise OutboundRpcStatusError(
                    f"Dapr RegisterResumeSubscription returned HTTP {status_code}: "
                    f"{body_preview!r}",
                    status_code=status_code,
                )

            try:
                body = response.json()
            except ValueError as exc:
                raise OutboundRpcDecodeError(
                    f"Dapr RegisterResumeSubscription response is not valid JSON: {exc!r}"
                ) from exc

            return _parse_register_response(body)

    async def cancel_resume_subscription(self, request: CancelResumeSubscriptionRequest) -> None:
        """Post one ``CancelResumeSubscription`` call through the Dapr sidecar.

        Returns ``None`` on success. The RPC is idempotent: HTTP
        404 / 409 (the key is already-expired or was never
        registered) are treated as a clean no-op rather than an
        error, matching the Trigger Service cancellation contract.
        Every other transport-layer failure mode is raised as the
        appropriate
        :class:`~custos_workflow.clients._errors.OutboundRpcError`
        subclass:

        * Transport failure (no response observed) →
          :class:`OutboundRpcTransportError`.
        * HTTP 499 (upstream cancelled) →
          :class:`OutboundRpcCancelledError`.
        * Any other non-2xx (excluding the idempotent 404 / 409) →
          :class:`OutboundRpcStatusError` carrying the observed
          ``status_code``.
        """
        from custos_workflow._telemetry import observe_outbound_rpc
        from custos_workflow.clients._errors import (
            OutboundRpcCancelledError,
            OutboundRpcStatusError,
            OutboundRpcTransportError,
        )

        url = build_invoke_url(self.endpoint, CANCEL_RESUME_SUBSCRIPTION_DAPR_METHOD)
        wire = _cancel_request_to_wire(request)

        async with observe_outbound_rpc(
            client="trigger",
            method=CANCEL_RESUME_SUBSCRIPTION_DAPR_METHOD,
            run_id=request.run_id,
            step_id=request.step_id,
        ) as obs_ctx:
            try:
                response = await self.http_client.post(
                    url,
                    json=wire,
                    timeout=self.timeout,
                    headers={"Content-Type": "application/json"},
                )
            except httpx.HTTPError as exc:
                raise OutboundRpcTransportError(
                    f"Dapr CancelResumeSubscription transport failure: {exc!r}"
                ) from exc

            status_code = response.status_code
            obs_ctx.set_status_code(status_code)
            if status_code == _CLIENT_CLOSED_REQUEST_STATUS:
                raise OutboundRpcCancelledError(
                    f"Dapr CancelResumeSubscription cancelled upstream (HTTP {status_code})"
                )
            if status_code in _CANCEL_NOOP_STATUSES:
                # Idempotent no-op: the key is already gone. Record
                # the status on the telemetry context so the
                # histogram label reflects reality, then return
                # cleanly without touching the error counter.
                return
            if status_code // 100 != 2:
                body_preview = response.text[:200] if response.text else ""
                raise OutboundRpcStatusError(
                    f"Dapr CancelResumeSubscription returned HTTP {status_code}: {body_preview!r}",
                    status_code=status_code,
                )
