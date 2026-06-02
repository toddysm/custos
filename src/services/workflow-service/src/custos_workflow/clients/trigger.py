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

from dataclasses import dataclass, field
from typing import Final, Protocol, runtime_checkable

__all__ = [
    "CANCEL_RESUME_SUBSCRIPTION_DAPR_METHOD",
    "REGISTER_RESUME_SUBSCRIPTION_DAPR_METHOD",
    "CancelResumeSubscriptionRequest",
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
