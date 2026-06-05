"""Resume Matcher — selects ``kind=resume`` subscriptions an event resumes.

A resume subscription is registered by the Workflow Service for an in-flight
step waiting on an external signal, keyed by the ``(runId, stepId, eventKey)``
triple (design § Data Models). The matcher resumes a waiting step only on an
**exact** triple match, then applies the optional CEL selector to narrow which
event satisfies the wait.

The event supplies its half of the triple via ``event.data.runId`` /
``event.data.stepId`` (the run context the receiver stamped in) and
``event.kind`` (the event name the step waits on, e.g. ``pr.merged``). An event
that carries no run/step context cannot resume anything and yields no matches.
As in the start arm, a non-boolean selector result is a no-match.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from custos_trigger.events import NormalizedEvent
from custos_trigger.models import ResumeRegistration
from custos_trigger.selector import SelectorEvaluator, SelectorTypeError

__all__ = [
    "ResumeCandidate",
    "ResumeKey",
    "ResumeMatch",
    "ResumeMatcher",
    "resume_key_from_event",
]

#: ``NormalizedEvent.data`` keys carrying the run context a resume event matches on.
_DATA_RUN_ID = "runId"
_DATA_STEP_ID = "stepId"


@dataclass(frozen=True, slots=True)
class ResumeKey:
    """The ``(runId, stepId, eventKey)`` triple an event resumes against."""

    run_id: str
    step_id: str
    event_key: str


@dataclass(frozen=True, slots=True)
class ResumeCandidate:
    """A registered resume subscription, paired with its store ``resume_id``.

    The ``resume_id`` keys the one-shot ``ResumeSubscription`` row the dispatcher
    deletes after a successful ``RaiseExternalEvent`` and the selector cache.
    """

    resume_id: str
    registration: ResumeRegistration


@dataclass(frozen=True, slots=True)
class ResumeMatch:
    """A resume subscription that an event satisfied."""

    resume_id: str
    registration: ResumeRegistration


def resume_key_from_event(event: NormalizedEvent) -> ResumeKey | None:
    """Extract the resume triple from ``event``, or ``None`` if it carries none.

    ``runId`` / ``stepId`` are read from ``event.data``; ``eventKey`` is the
    event ``kind``. A missing or non-string run/step id means the event has no
    run context and cannot resume a step.
    """
    run_id = event.data.get(_DATA_RUN_ID)
    step_id = event.data.get(_DATA_STEP_ID)
    if not isinstance(run_id, str) or not run_id:
        return None
    if not isinstance(step_id, str) or not step_id:
        return None
    return ResumeKey(run_id=run_id, step_id=step_id, event_key=event.kind)


class ResumeMatcher:
    """Matches resume subscriptions on the exact triple plus optional selector."""

    def __init__(self, evaluator: SelectorEvaluator) -> None:
        self._evaluator = evaluator

    def match(
        self, event: NormalizedEvent, candidates: Iterable[ResumeCandidate]
    ) -> list[ResumeMatch]:
        """Return the resume subscriptions whose triple (and selector) match ``event``."""
        key = resume_key_from_event(event)
        if key is None:
            return []
        matches: list[ResumeMatch] = []
        for candidate in candidates:
            reg = candidate.registration
            if reg.run_id != key.run_id:
                continue
            if reg.step_id != key.step_id:
                continue
            if reg.event_key != key.event_key:
                continue
            if self._selector_matches(candidate, event):
                matches.append(ResumeMatch(resume_id=candidate.resume_id, registration=reg))
        return matches

    def _selector_matches(self, candidate: ResumeCandidate, event: NormalizedEvent) -> bool:
        selector = candidate.registration.selector
        if not selector:
            # No selector = match on the event key alone (design § Internal RPC:
            # `selector=None` matches on event key alone).
            return True
        try:
            return self._evaluator.matches(selector, event, subscription_id=candidate.resume_id)
        except SelectorTypeError:
            return False
