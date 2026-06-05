"""Alerting subsystem for the Observability and Audit Service.

Houses the alert-rule DSL: :mod:`rules` loads the ``custos-alert-rules``
ConfigMap (shape of ``deploy/alert-rules/default.yaml``) at startup and matches
drained audit events against the declared rules, applying throttle/dedup
suppression. :mod:`dispatcher` (OBS-IMPL-009) consumes the resulting matches
and delivers them to webhook + SMTP sinks with retry and dead-lettering.
"""

from __future__ import annotations

from custos_obs.alerting.dispatcher import (
    AlertDispatcher,
    AlertPayload,
    AlertSink,
    AlertSinkError,
    DeadLetterRecord,
    DeadLetterStore,
    EmailTransport,
    HttpxWebhookTransport,
    SmtpEmailTransport,
    SmtpSink,
    WebhookSink,
    WebhookTransport,
)
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
    "AlertDispatcher",
    "AlertEngine",
    "AlertMatch",
    "AlertPayload",
    "AlertRule",
    "AlertRuleConfigError",
    "AlertRuleSet",
    "AlertSink",
    "AlertSinkError",
    "DeadLetterRecord",
    "DeadLetterStore",
    "EmailTransport",
    "HttpxWebhookTransport",
    "MatchableEvent",
    "SmtpEmailTransport",
    "SmtpSink",
    "WebhookSink",
    "WebhookTransport",
    "load_alert_rules",
]
