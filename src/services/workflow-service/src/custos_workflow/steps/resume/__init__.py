"""Resume Subscription Manager sub-module (REQ-081, WF-IMPL-102+).

The Resume Subscription Manager owns the ``waitFor:`` step
lifecycle: it persists a :class:`ResumeSubscriptionMirror` before
registering the subscription with the Trigger Service, re-derives
the open subscription set on Dapr Workflow replay, and cancels
subscriptions on step/run terminal transitions.

WF-IMPL-102 lands the persistence foundation — the
:class:`ResumeSubscriptionMirror` entity, the
:class:`ResumeSubscriptionMirrorRepository` Protocol, and the
in-memory :class:`InMemoryResumeSubscriptionMirrorRepository`
adapter.
"""

from __future__ import annotations

from custos_workflow.steps.resume.mirror import (
    InMemoryResumeSubscriptionMirrorRepository,
    ResumeSubscriptionMirror,
    ResumeSubscriptionMirrorRepository,
)

__all__ = [
    "InMemoryResumeSubscriptionMirrorRepository",
    "ResumeSubscriptionMirror",
    "ResumeSubscriptionMirrorRepository",
]
