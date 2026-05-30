"""Outbound client boundaries for the Step Coordinator (WF-IMPL-049+).

This package contains the runtime-checkable Protocols that the
Step Coordinator uses to talk to the rest of the platform without
hard-coupling to any specific transport. Each Protocol ships with
two test doubles next to it:

* a ``Noop*Client`` that explicitly :class:`NotImplementedError`-s
  every call (the safe default the Run Controller wiring picks up
  before the production adapter lands), and
* a ``Fake*Client`` that returns canned values, which the
  Step Coordinator's unit tests use to drive deterministic
  scenarios without standing up Dapr.

The real Dapr-backed adapters live in deferred sub-modules and
plug in behind the same Protocols.

* WF-IMPL-049 lands :class:`ActivityRuntimeClient` —
  the outbound boundary to the Activity Runtime Manager
  (``ScheduleActivity`` + ``CancelActivity``) — together with the
  :class:`ScheduleActivityRequest` / :class:`ActivityResultEnvelope`
  frozen dataclasses and the
  :class:`NoopActivityRuntimeClient` / :class:`FakeActivityRuntimeClient`
  test doubles.
"""

from __future__ import annotations

from custos_workflow.clients.activity_runtime import (
    ACTIVITY_RESULT_CLASSES,
    ActivityResultClass,
    ActivityResultEnvelope,
    ActivityRuntimeClient,
    FakeActivityRuntimeClient,
    NoopActivityRuntimeClient,
    ScheduleActivityRequest,
)

__all__ = [
    "ACTIVITY_RESULT_CLASSES",
    "ActivityResultClass",
    "ActivityResultEnvelope",
    "ActivityRuntimeClient",
    "FakeActivityRuntimeClient",
    "NoopActivityRuntimeClient",
    "ScheduleActivityRequest",
]
