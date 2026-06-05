"""The linear matching pipeline (Classify → Match) — TS-IMPL-012.

The :func:`classify` step routes a :class:`~custos_trigger.events.NormalizedEvent`
to the start and/or resume arms; :class:`StartMatcher` and :class:`ResumeMatcher`
then select the subscriptions an event actually fires. Dedup and dispatch
(``TS-IMPL-009`` / ``TS-IMPL-014``) consume the matches this package produces.
"""

from __future__ import annotations

from custos_trigger.pipeline.classify import Classification, classify
from custos_trigger.pipeline.match_resume import (
    ResumeCandidate,
    ResumeKey,
    ResumeMatch,
    ResumeMatcher,
    resume_key_from_event,
)
from custos_trigger.pipeline.match_start import StartMatch, StartMatcher

__all__ = [
    "Classification",
    "ResumeCandidate",
    "ResumeKey",
    "ResumeMatch",
    "ResumeMatcher",
    "StartMatch",
    "StartMatcher",
    "classify",
    "resume_key_from_event",
]
