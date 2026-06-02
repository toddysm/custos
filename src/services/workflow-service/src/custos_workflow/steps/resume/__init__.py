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
adapter. WF-IMPL-104 lands the :class:`WaitForStepHandler` — the
register / wait / resume / cancel / delete-mirror lifecycle driver
for a ``waitFor:`` step — plus its in-process drivers and effect
tokens. WF-IMPL-105 lands the
:class:`ResumeSubscriptionReplayReconciler` — the production replay
hook that idempotently re-registers a run's open mirrors on every
orchestrator entry. WF-IMPL-106 lands the
:class:`ResumeSubscriptionCanceller` — the terminal-transition sweep
that cancels a run's (or a single step's) open subscriptions with the
Trigger Service and deletes their mirror rows. WF-IMPL-109 lands the
:class:`ResumeSubscriptionTtlSweeper` — the periodic, time-driven sweep
that garbage-collects TTL-expired mirror rows independently of the WF
mirror writes.
"""

from __future__ import annotations

from custos_workflow.steps.resume.canceller import (
    CancelSweepReport,
    ResumeSubscriptionCanceller,
)
from custos_workflow.steps.resume.handler import (
    CancelResumeSubscriptionCall,
    DeleteMirrorCall,
    PersistMirrorCall,
    RegisterResumeSubscriptionCall,
    ResumeCall,
    WaitForExternalEventCall,
    WaitForStepHandler,
    drive_resume_generator,
    drive_resume_registration_to_wait,
)
from custos_workflow.steps.resume.mirror import (
    InMemoryResumeSubscriptionMirrorRepository,
    ResumeSubscriptionMirror,
    ResumeSubscriptionMirrorRepository,
)
from custos_workflow.steps.resume.reconciler import (
    NoopResumeSubscriptionAuditPublisher,
    ReplayReconcileReport,
    ResumeSubscriptionAuditPublisher,
    ResumeSubscriptionReplayReconciler,
)
from custos_workflow.steps.resume.sweeper import (
    DEFAULT_RESUME_SUB_SWEEP_INTERVAL_SECONDS,
    ResumeSubscriptionTtlSweeper,
    TtlSweepReport,
)

__all__ = [
    "DEFAULT_RESUME_SUB_SWEEP_INTERVAL_SECONDS",
    "CancelResumeSubscriptionCall",
    "CancelSweepReport",
    "DeleteMirrorCall",
    "InMemoryResumeSubscriptionMirrorRepository",
    "NoopResumeSubscriptionAuditPublisher",
    "PersistMirrorCall",
    "RegisterResumeSubscriptionCall",
    "ReplayReconcileReport",
    "ResumeCall",
    "ResumeSubscriptionAuditPublisher",
    "ResumeSubscriptionCanceller",
    "ResumeSubscriptionMirror",
    "ResumeSubscriptionMirrorRepository",
    "ResumeSubscriptionReplayReconciler",
    "ResumeSubscriptionTtlSweeper",
    "TtlSweepReport",
    "WaitForExternalEventCall",
    "WaitForStepHandler",
    "drive_resume_generator",
    "drive_resume_registration_to_wait",
]
