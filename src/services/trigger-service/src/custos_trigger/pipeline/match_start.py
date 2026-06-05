"""Start Matcher — selects active ``kind=start`` subscriptions an event fires.

Given a normalized event and a set of candidate :class:`Subscription` rows
(enumerated by the caller — the locked SPL metadata store exposes no list
surface), the matcher keeps the active start subscriptions whose CEL selector
evaluates ``True`` against the event. A subscription with no selector is an
unconditional match (it fires on every event the caller routed to it).

A selector that evaluates to a non-boolean is treated as a no-match (design
§ Selector Language step 4 — ``trigger.selector_type_error`` is audited and the
event does not fire that subscription).
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from custos_trigger.events import NormalizedEvent
from custos_trigger.models import Subscription, SubscriptionKind, SubscriptionState
from custos_trigger.selector import SelectorEvaluator, SelectorTypeError

__all__ = ["StartMatch", "StartMatcher"]


@dataclass(frozen=True, slots=True)
class StartMatch:
    """A start subscription that fired for an event."""

    subscription: Subscription


class StartMatcher:
    """Evaluates start-subscription selectors against a normalized event."""

    def __init__(self, evaluator: SelectorEvaluator) -> None:
        self._evaluator = evaluator

    def match(self, event: NormalizedEvent, candidates: Iterable[Subscription]) -> list[StartMatch]:
        """Return the active start subscriptions whose selector matches ``event``."""
        matches: list[StartMatch] = []
        for sub in candidates:
            if sub.kind is not SubscriptionKind.START:
                continue
            if sub.state is not SubscriptionState.ACTIVE:
                continue
            if self._selector_matches(sub, event):
                matches.append(StartMatch(subscription=sub))
        return matches

    def _selector_matches(self, sub: Subscription, event: NormalizedEvent) -> bool:
        if not sub.selector:
            # No selector = unconditional: the subscription fires on every event
            # the caller routed to it.
            return True
        try:
            return self._evaluator.matches(sub.selector, event, subscription_id=sub.subscription_id)
        except SelectorTypeError:
            # A non-bool selector result is a no-match (audited downstream).
            return False
