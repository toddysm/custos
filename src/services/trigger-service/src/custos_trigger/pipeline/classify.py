"""Classifier — routes a normalized event to the start and/or resume arms.

Both arms can match the same event (design § Module responsibilities): a
``workflow.completed`` event can start a chained workflow *and* resume a parent
run waiting on its child. The classifier therefore routes every event to the
start arm and, by default, to the resume arm as well — the matchers downstream
decide which subscriptions actually fire.

The one exception is a **manual fire** (``source.type == manual``): the manual
receiver targets one specific start subscription by id
(``POST …/triggers/{id}:fire``), so a manual fire is never a step-resume signal
and is routed to the start arm only.
"""

from __future__ import annotations

from dataclasses import dataclass

from custos_trigger.events import NormalizedEvent
from custos_trigger.models import SourceType

__all__ = ["Classification", "classify"]


@dataclass(frozen=True, slots=True)
class Classification:
    """Which matcher arms an event is routed to. Both may be ``True``."""

    to_start: bool
    to_resume: bool


def classify(event: NormalizedEvent) -> Classification:
    """Route ``event`` to the start and/or resume matcher arms.

    Every event is a start candidate; every event except a manual fire is also
    a resume candidate (a manual fire directly targets a start subscription and
    cannot resume a step).
    """
    is_manual_fire = event.source.type is SourceType.MANUAL
    return Classification(to_start=True, to_resume=not is_manual_fire)
