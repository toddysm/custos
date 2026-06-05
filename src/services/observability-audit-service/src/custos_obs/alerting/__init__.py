"""Alerting subsystem for the Observability and Audit Service.

Houses the alert-rule DSL: :mod:`rules` loads the ``custos-alert-rules``
ConfigMap (shape of ``deploy/alert-rules/default.yaml``) at startup and matches
drained audit events against the declared rules, applying throttle/dedup
suppression. The dispatcher (OBS-IMPL-009) consumes the resulting matches.
"""

from __future__ import annotations

from custos_obs.alerting.rules import (
    AlertEngine,
    AlertMatch,
    AlertRule,
    AlertRuleConfigError,
    AlertRuleSet,
    MatchableEvent,
    load_alert_rules,
)

__all__ = [
    "AlertEngine",
    "AlertMatch",
    "AlertRule",
    "AlertRuleConfigError",
    "AlertRuleSet",
    "MatchableEvent",
    "load_alert_rules",
]
