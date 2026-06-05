"""The linear matching pipeline (Classify → Match → Dispatch).

The :func:`classify` step routes a :class:`~custos_trigger.events.NormalizedEvent`
to the start and/or resume arms; :class:`StartMatcher` and :class:`ResumeMatcher`
then select the subscriptions an event actually fires; the :class:`Dispatcher`
(``TS-IMPL-014``) drives those matches to the Workflow Service with retry,
dedup, and fan-out loop protection.
"""

from __future__ import annotations

from custos_trigger.pipeline.classify import Classification, classify
from custos_trigger.pipeline.dispatch import (
    AuditSink,
    Dispatcher,
    DispatchOutcome,
    DispatchStatus,
    NoopAuditSink,
)
from custos_trigger.pipeline.match_resume import (
    ResumeCandidate,
    ResumeKey,
    ResumeMatch,
    ResumeMatcher,
    resume_key_from_event,
)
from custos_trigger.pipeline.match_start import StartMatch, StartMatcher

__all__ = [
    "AuditSink",
    "Classification",
    "DispatchOutcome",
    "DispatchStatus",
    "Dispatcher",
    "NoopAuditSink",
    "ResumeCandidate",
    "ResumeKey",
    "ResumeMatch",
    "ResumeMatcher",
    "StartMatch",
    "StartMatcher",
    "classify",
    "resume_key_from_event",
]
