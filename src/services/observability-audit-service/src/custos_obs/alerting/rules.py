"""Alert-rule DSL loader + matcher (OBS-IMPL-008, design TODO-001).

The alerting subsystem is driven by a small declarative DSL shipped as the
``custos-alert-rules`` ConfigMap (default content in
``deploy/alert-rules/default.yaml``). Each rule matches drained audit events on
``eventName``, ``severity``, and ``component``, plus arbitrary ``match:`` field
equality, then names the sinks to dispatch to. ``throttle:`` collapses repeated
firings of the same alert into one per window; ``dedupKey:`` chooses which event
fields define "the same alert" (defaults to the whole rule).

This module is two layers:

* **Load + validate** — :func:`load_alert_rules` parses the YAML/dict, validates
  it *strictly* (unknown keys, missing names, duplicate names, malformed
  durations all raise :class:`AlertRuleConfigError`), and returns an immutable
  :class:`AlertRuleSet`. Malformed rules fail loudly at startup rather than
  silently dropping alerts.
* **Match + suppress** — :class:`AlertEngine` wraps a rule set with throttle
  state. :meth:`AlertEngine.evaluate` returns the rules that fire for an event,
  honouring per-(rule, dedup-key) throttle windows.

Severity and component are read from the audit event's ``subject`` block (the
shape every Custos service emits via ``to_audit_event``); ``match:`` fields are
resolved against ``payload`` first, then ``subject``.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import timedelta
from heapq import heappop, heappush
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

import yaml

if TYPE_CHECKING:
    from datetime import datetime

    from custos_spl import AuditEvent

#: Recognised throttle-duration unit suffixes mapped to seconds.
_DURATION_UNITS: dict[str, int] = {"s": 1, "m": 60, "h": 3600, "d": 86400}

#: Rule keys the loader accepts. Anything else fails validation loudly.
_ALLOWED_RULE_KEYS: frozenset[str] = frozenset(
    {"name", "eventName", "severity", "component", "match", "throttle", "dedupKey", "sinks"}
)


class AlertRuleConfigError(ValueError):
    """A malformed alert-rule ConfigMap. Raised at load/startup time."""


def _parse_duration(raw: object, *, where: str) -> timedelta:
    """Parse a ``<int><unit>`` duration (e.g. ``5m``) into a :class:`timedelta`.

    Units are ``s``/``m``/``h``/``d``. The value must be a positive integer.
    """
    if not isinstance(raw, str) or not raw:
        raise AlertRuleConfigError(f"{where}: throttle must be a duration string like '5m'")
    unit = raw[-1]
    if unit not in _DURATION_UNITS:
        raise AlertRuleConfigError(f"{where}: throttle '{raw}' has an unknown unit (use s/m/h/d)")
    try:
        value = int(raw[:-1])
    except ValueError:
        raise AlertRuleConfigError(
            f"{where}: throttle '{raw}' is not an integer count of {unit}"
        ) from None
    if value <= 0:
        raise AlertRuleConfigError(f"{where}: throttle '{raw}' must be positive")
    return timedelta(seconds=value * _DURATION_UNITS[unit])


@dataclass(frozen=True, slots=True)
class AlertRule:
    """A single declarative alert rule.

    A rule matches an event when every declared criterion matches: the optional
    ``event_name``/``severity``/``component`` equalities and every ``match``
    field equality. At least one criterion is always present (enforced at load).
    """

    name: str
    sinks: tuple[str, ...]
    event_name: str | None = None
    severity: str | None = None
    component: str | None = None
    match: Mapping[str, str] = field(default_factory=dict)
    throttle: timedelta | None = None
    dedup_key: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        # Make the declared immutability observable: a frozen rule must not be
        # mutable through its ``match`` mapping.
        object.__setattr__(self, "match", MappingProxyType(dict(self.match)))

    def matches(self, event: MatchableEvent) -> bool:
        """Return whether ``event`` satisfies every criterion of this rule."""
        if self.event_name is not None and event.event_name != self.event_name:
            return False
        if self.severity is not None and event.severity != self.severity:
            return False
        if self.component is not None and event.component != self.component:
            return False
        for key, expected in self.match.items():
            actual = event.get_field(key)
            if actual is None or str(actual) != expected:
                return False
        return True

    def dedup_identity(self, event: MatchableEvent) -> tuple[str, ...]:
        """The throttle identity for ``event`` under this rule.

        Defaults to the rule name alone (the whole rule is throttled); with
        ``dedup_key`` set, the chosen field values extend the identity so
        distinct subjects throttle independently.
        """
        return (self.name, *(str(event.get_field(k)) for k in self.dedup_key))


@dataclass(frozen=True, slots=True)
class AlertRuleSet:
    """An immutable, validated collection of alert rules."""

    rules: tuple[AlertRule, ...]

    def matching(self, event: MatchableEvent) -> tuple[AlertRule, ...]:
        """Return, in declaration order, every rule that matches ``event``.

        This ignores throttle state; use :class:`AlertEngine` for suppression.
        """
        return tuple(rule for rule in self.rules if rule.matches(event))


@dataclass(frozen=True, slots=True)
class MatchableEvent:
    """A normalized, matchable view of an audit event.

    ``event_name`` comes from the event type; ``severity`` and ``component`` from
    the ``subject`` block; ``match`` fields resolve against ``payload`` first
    then ``subject``.
    """

    event_name: str
    severity: str | None
    component: str | None
    payload: Mapping[str, Any] = field(default_factory=dict)
    subject: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        # Protect the frozen view from external mutation of the source mappings
        # (an ``AuditEvent`` may carry mutable dicts).
        object.__setattr__(self, "payload", MappingProxyType(dict(self.payload)))
        object.__setattr__(self, "subject", MappingProxyType(dict(self.subject)))

    @classmethod
    def from_audit_event(cls, event: AuditEvent) -> MatchableEvent:
        """Build a matchable view from an SPL :class:`~custos_spl.AuditEvent`."""
        subject = event.subject
        severity = subject.get("severity")
        component = subject.get("component")
        return cls(
            event_name=event.event_type,
            severity=str(severity) if severity is not None else None,
            component=str(component) if component is not None else None,
            payload=event.payload,
            subject=subject,
        )

    def get_field(self, name: str) -> Any:
        """Resolve a ``match`` field: ``payload`` first, then ``subject``."""
        if name in self.payload:
            return self.payload[name]
        return self.subject.get(name)


@dataclass(frozen=True, slots=True)
class AlertMatch:
    """A rule that fired for an event, with its resolved dedup identity."""

    rule: AlertRule
    event: MatchableEvent
    dedup_identity: tuple[str, ...]

    @property
    def sinks(self) -> tuple[str, ...]:
        return self.rule.sinks


class AlertEngine:
    """Matches events against a rule set, suppressing throttled repeats.

    The engine is stateful: it remembers, per ``(rule, dedup-key)`` identity, the
    instant its throttle window expires, so a rule with a ``throttle`` emits at
    most once per window per distinct identity. Expired identities are evicted on
    each evaluation (via an expiry heap), so memory stays bounded by the set of
    *currently throttled* identities even with a high-cardinality ``dedupKey``.
    Callers pass an explicit ``now`` so the engine stays deterministic.
    """

    def __init__(self, ruleset: AlertRuleSet) -> None:
        self._ruleset = ruleset
        # identity -> instant the throttle window ends.
        self._throttled_until: dict[tuple[str, ...], datetime] = {}
        # min-heap of (expiry, identity) used to evict stale identities.
        self._expiry: list[tuple[datetime, tuple[str, ...]]] = []

    @property
    def ruleset(self) -> AlertRuleSet:
        return self._ruleset

    def _evict_expired(self, now: datetime) -> None:
        """Drop identities whose throttle window has elapsed by ``now``."""
        heap = self._expiry
        while heap and heap[0][0] <= now:
            _, identity = heappop(heap)
            current = self._throttled_until.get(identity)
            # Only evict if this heap entry is the identity's live window and it
            # has truly expired (a later firing may have re-armed it).
            if current is not None and current <= now:
                del self._throttled_until[identity]

    def evaluate(self, event: MatchableEvent, *, now: datetime) -> list[AlertMatch]:
        """Return the matches that should dispatch for ``event`` at ``now``.

        Rules without a ``throttle`` always fire; throttled rules fire only when
        their dedup identity is not inside an active window.
        """
        self._evict_expired(now)
        matches: list[AlertMatch] = []
        for rule in self._ruleset.matching(event):
            identity = rule.dedup_identity(event)
            if rule.throttle is not None:
                until = self._throttled_until.get(identity)
                if until is not None and now < until:
                    continue
                new_until = now + rule.throttle
                self._throttled_until[identity] = new_until
                heappush(self._expiry, (new_until, identity))
            matches.append(AlertMatch(rule=rule, event=event, dedup_identity=identity))
        return matches

    def evaluate_audit(self, event: AuditEvent, *, now: datetime) -> list[AlertMatch]:
        """Convenience wrapper: match a raw SPL audit event at ``now``."""
        return self.evaluate(MatchableEvent.from_audit_event(event), now=now)


def _require_str(value: object, *, where: str) -> str:
    if not isinstance(value, str) or not value:
        raise AlertRuleConfigError(f"{where}: must be a non-empty string")
    return value


def _parse_match(raw: object, *, where: str) -> dict[str, str]:
    if not isinstance(raw, Mapping):
        raise AlertRuleConfigError(f"{where}: match must be a mapping of field to value")
    result: dict[str, str] = {}
    for key, value in raw.items():
        if not isinstance(key, str) or not key:
            raise AlertRuleConfigError(f"{where}: match keys must be non-empty strings")
        if isinstance(value, (Mapping, list)):
            raise AlertRuleConfigError(
                f"{where}: match['{key}'] must be a scalar value, not a collection"
            )
        result[key] = str(value)
    return result


def _parse_dedup_key(raw: object, *, where: str) -> tuple[str, ...]:
    if not isinstance(raw, list):
        raise AlertRuleConfigError(f"{where}: dedupKey must be a list of field names")
    keys: list[str] = []
    for item in raw:
        keys.append(_require_str(item, where=f"{where}.dedupKey[]"))
    return tuple(keys)


def _parse_sinks(raw: object, *, where: str) -> tuple[str, ...]:
    if not isinstance(raw, list) or not raw:
        raise AlertRuleConfigError(f"{where}: sinks must be a non-empty list")
    return tuple(_require_str(item, where=f"{where}.sinks[]") for item in raw)


def _parse_rule(raw: object, *, index: int) -> AlertRule:
    where = f"rule[{index}]"
    if not isinstance(raw, Mapping):
        raise AlertRuleConfigError(f"{where}: must be a mapping")
    unknown = set(raw) - _ALLOWED_RULE_KEYS
    if unknown:
        raise AlertRuleConfigError(f"{where}: unknown key(s) {sorted(unknown)}")

    name = _require_str(raw.get("name"), where=f"{where}.name")
    where = f"rule '{name}'"

    event_name = raw.get("eventName")
    severity = raw.get("severity")
    component = raw.get("component")
    for label, value in (
        ("eventName", event_name),
        ("severity", severity),
        ("component", component),
    ):
        if value is not None and (not isinstance(value, str) or not value):
            raise AlertRuleConfigError(f"{where}: {label} must be a non-empty string")

    match = _parse_match(raw["match"], where=where) if "match" in raw else {}

    if event_name is None and severity is None and component is None and not match:
        raise AlertRuleConfigError(
            f"{where}: must declare at least one of eventName/severity/component/match"
        )

    throttle = _parse_duration(raw["throttle"], where=where) if "throttle" in raw else None
    dedup_key = _parse_dedup_key(raw["dedupKey"], where=where) if "dedupKey" in raw else ()
    sinks = _parse_sinks(raw.get("sinks"), where=where)

    return AlertRule(
        name=name,
        sinks=sinks,
        event_name=event_name,
        severity=severity,
        component=component,
        match=match,
        throttle=throttle,
        dedup_key=dedup_key,
    )


def load_alert_rules(raw: str | Mapping[str, Any]) -> AlertRuleSet:
    """Parse and strictly validate an alert-rule ConfigMap.

    ``raw`` is either the YAML document text or an already-parsed mapping. The
    top level must be a mapping with a ``rules`` list. Every structural problem
    — bad types, missing/duplicate names, unknown keys, malformed durations —
    raises :class:`AlertRuleConfigError` so misconfiguration is caught at
    startup, never silently dropping alerts.
    """
    document: object = yaml.safe_load(raw) if isinstance(raw, str) else raw
    if document is None:
        raise AlertRuleConfigError("alert rules document is empty")
    if not isinstance(document, Mapping):
        raise AlertRuleConfigError("alert rules document must be a mapping with a 'rules' list")
    rules_raw = document.get("rules")
    if not isinstance(rules_raw, list):
        raise AlertRuleConfigError("'rules' must be a list")

    rules: list[AlertRule] = []
    seen: set[str] = set()
    for index, rule_raw in enumerate(rules_raw):
        rule = _parse_rule(rule_raw, index=index)
        if rule.name in seen:
            raise AlertRuleConfigError(f"duplicate rule name '{rule.name}'")
        seen.add(rule.name)
        rules.append(rule)

    return AlertRuleSet(rules=tuple(rules))


__all__ = [
    "AlertEngine",
    "AlertMatch",
    "AlertRule",
    "AlertRuleConfigError",
    "AlertRuleSet",
    "MatchableEvent",
    "load_alert_rules",
]
