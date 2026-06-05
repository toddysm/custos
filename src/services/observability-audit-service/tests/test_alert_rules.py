"""Tests for the alert-rule DSL loader + matcher (OBS-IMPL-008).

Cover three concerns: the shipped ``deploy/alert-rules/default.yaml`` parses and
matches; strict load-time validation rejects malformed rules loudly; and the
:class:`AlertEngine` suppresses throttled/deduped repeats within a window.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from custos_spl import AuditEvent
from custos_spl.ids import WorkspaceId

from custos_obs.alerting.rules import (
    AlertEngine,
    AlertRule,
    AlertRuleConfigError,
    AlertRuleSet,
    MatchableEvent,
    load_alert_rules,
)

T0 = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)

DEFAULT_RULES_PATH = Path(__file__).resolve().parents[4] / "deploy" / "alert-rules" / "default.yaml"


def _event(
    event_type: str,
    *,
    severity: str | None = None,
    component: str | None = None,
    payload: dict[str, Any] | None = None,
) -> MatchableEvent:
    subject: dict[str, Any] = {}
    if severity is not None:
        subject["severity"] = severity
    if component is not None:
        subject["component"] = component
    return MatchableEvent(
        event_name=event_type,
        severity=severity,
        component=component,
        payload=payload or {},
        subject=subject,
    )


# --------------------------------------------------------------------------- #
# Loading + the shipped default rules                                          #
# --------------------------------------------------------------------------- #


def test_default_rules_file_parses() -> None:
    ruleset = load_alert_rules(DEFAULT_RULES_PATH.read_text())
    names = [r.name for r in ruleset.rules]
    assert names == ["audit-drain-lagging", "exporter-config-rejected", "authz-deny-burst"]


def test_default_rules_match_their_events() -> None:
    ruleset = load_alert_rules(DEFAULT_RULES_PATH.read_text())

    lagging = _event("obs.outbox.lagging", severity="warning")
    rejected = _event("obs.exporter.config.rejected", severity="error")
    deny = _event("authz.decision", payload={"decision": "deny"})

    assert [r.name for r in ruleset.matching(lagging)] == ["audit-drain-lagging"]
    assert [r.name for r in ruleset.matching(rejected)] == ["exporter-config-rejected"]
    assert [r.name for r in ruleset.matching(deny)] == ["authz-deny-burst"]


def test_severity_mismatch_does_not_match() -> None:
    ruleset = load_alert_rules(DEFAULT_RULES_PATH.read_text())
    # Right event name, wrong severity.
    evt = _event("obs.outbox.lagging", severity="info")
    assert ruleset.matching(evt) == ()


def test_match_field_absent_does_not_match() -> None:
    ruleset = load_alert_rules(DEFAULT_RULES_PATH.read_text())
    evt = _event("authz.decision", payload={"decision": "allow"})
    assert ruleset.matching(evt) == ()
    evt_missing = _event("authz.decision", payload={})
    assert ruleset.matching(evt_missing) == ()


def test_load_from_mapping_is_equivalent_to_yaml() -> None:
    doc = {"rules": [{"name": "r", "eventName": "e", "sinks": ["webhook"]}]}
    ruleset = load_alert_rules(doc)
    assert ruleset.rules[0].name == "r"
    assert ruleset.rules[0].event_name == "e"
    assert ruleset.rules[0].sinks == ("webhook",)


def test_component_and_multiple_criteria_match() -> None:
    doc = {
        "rules": [
            {
                "name": "scoped",
                "eventName": "x.y",
                "component": "auth-service",
                "severity": "error",
                "match": {"k": "v"},
                "sinks": ["webhook"],
            }
        ]
    }
    ruleset = load_alert_rules(doc)
    good = MatchableEvent(
        event_name="x.y",
        severity="error",
        component="auth-service",
        payload={"k": "v"},
        subject={"component": "auth-service", "severity": "error"},
    )
    assert [r.name for r in ruleset.matching(good)] == ["scoped"]
    # One mismatch (component) and it drops.
    bad = MatchableEvent(
        event_name="x.y",
        severity="error",
        component="other",
        payload={"k": "v"},
        subject={},
    )
    assert ruleset.matching(bad) == ()


def test_match_resolves_subject_when_absent_from_payload() -> None:
    doc = {"rules": [{"name": "r", "match": {"region": "eu"}, "sinks": ["webhook"]}]}
    ruleset = load_alert_rules(doc)
    evt = MatchableEvent(
        event_name="e",
        severity=None,
        component=None,
        payload={},
        subject={"region": "eu"},
    )
    assert [r.name for r in ruleset.matching(evt)] == ["r"]


# --------------------------------------------------------------------------- #
# Strict validation                                                           #
# --------------------------------------------------------------------------- #


def test_empty_document_rejected() -> None:
    with pytest.raises(AlertRuleConfigError, match="empty"):
        load_alert_rules("")


def test_non_mapping_document_rejected() -> None:
    with pytest.raises(AlertRuleConfigError, match="must be a mapping"):
        load_alert_rules("- just\n- a\n- list\n")


def test_missing_rules_list_rejected() -> None:
    with pytest.raises(AlertRuleConfigError, match="'rules' must be a list"):
        load_alert_rules({"notrules": []})


def test_rule_not_mapping_rejected() -> None:
    with pytest.raises(AlertRuleConfigError, match=r"rule\[0\]: must be a mapping"):
        load_alert_rules({"rules": ["nope"]})


def test_unknown_rule_key_rejected() -> None:
    with pytest.raises(AlertRuleConfigError, match="unknown key"):
        load_alert_rules({"rules": [{"name": "r", "sinks": ["w"], "bogus": 1}]})


def test_missing_name_rejected() -> None:
    with pytest.raises(AlertRuleConfigError, match="name: must be a non-empty string"):
        load_alert_rules({"rules": [{"eventName": "e", "sinks": ["w"]}]})


def test_duplicate_name_rejected() -> None:
    doc = {
        "rules": [
            {"name": "dup", "eventName": "a", "sinks": ["w"]},
            {"name": "dup", "eventName": "b", "sinks": ["w"]},
        ]
    }
    with pytest.raises(AlertRuleConfigError, match="duplicate rule name 'dup'"):
        load_alert_rules(doc)


def test_no_criteria_rejected() -> None:
    with pytest.raises(AlertRuleConfigError, match="at least one of"):
        load_alert_rules({"rules": [{"name": "r", "sinks": ["w"]}]})


def test_non_string_criterion_rejected() -> None:
    with pytest.raises(AlertRuleConfigError, match="eventName must be a non-empty string"):
        load_alert_rules({"rules": [{"name": "r", "eventName": 5, "sinks": ["w"]}]})


def test_sinks_required_non_empty() -> None:
    with pytest.raises(AlertRuleConfigError, match="sinks must be a non-empty list"):
        load_alert_rules({"rules": [{"name": "r", "eventName": "e", "sinks": []}]})


def test_sink_must_be_string() -> None:
    with pytest.raises(AlertRuleConfigError, match=r"sinks\[\]"):
        load_alert_rules({"rules": [{"name": "r", "eventName": "e", "sinks": [1]}]})


def test_match_not_mapping_rejected() -> None:
    with pytest.raises(AlertRuleConfigError, match="match must be a mapping"):
        load_alert_rules({"rules": [{"name": "r", "match": ["a"], "sinks": ["w"]}]})


def test_match_key_must_be_string() -> None:
    with pytest.raises(AlertRuleConfigError, match="match keys must be non-empty strings"):
        load_alert_rules({"rules": [{"name": "r", "match": {1: "v"}, "sinks": ["w"]}]})


def test_match_value_must_be_scalar() -> None:
    with pytest.raises(AlertRuleConfigError, match="must be a scalar value"):
        load_alert_rules({"rules": [{"name": "r", "match": {"k": ["v"]}, "sinks": ["w"]}]})


def test_match_value_coerced_to_string() -> None:
    ruleset = load_alert_rules({"rules": [{"name": "r", "match": {"n": 7}, "sinks": ["w"]}]})
    assert ruleset.rules[0].match == {"n": "7"}
    evt = _event("e", payload={"n": 7})
    assert [r.name for r in ruleset.matching(evt)] == ["r"]


@pytest.mark.parametrize("bad", ["", "5x", "abc", "m", "0m", "-5m"])
def test_bad_throttle_rejected(bad: str) -> None:
    with pytest.raises(AlertRuleConfigError, match="throttle"):
        load_alert_rules(
            {"rules": [{"name": "r", "eventName": "e", "throttle": bad, "sinks": ["w"]}]}
        )


def test_throttle_non_string_rejected() -> None:
    with pytest.raises(AlertRuleConfigError, match="throttle must be a duration string"):
        load_alert_rules(
            {"rules": [{"name": "r", "eventName": "e", "throttle": 300, "sinks": ["w"]}]}
        )


@pytest.mark.parametrize(
    ("text", "seconds"),
    [("30s", 30), ("5m", 300), ("2h", 7200), ("1d", 86400)],
)
def test_throttle_units_parse(text: str, seconds: int) -> None:
    ruleset = load_alert_rules(
        {"rules": [{"name": "r", "eventName": "e", "throttle": text, "sinks": ["w"]}]}
    )
    assert ruleset.rules[0].throttle == timedelta(seconds=seconds)


def test_dedup_key_must_be_list() -> None:
    with pytest.raises(AlertRuleConfigError, match="dedupKey must be a list"):
        load_alert_rules(
            {"rules": [{"name": "r", "eventName": "e", "dedupKey": "k", "sinks": ["w"]}]}
        )


def test_dedup_key_items_must_be_strings() -> None:
    with pytest.raises(AlertRuleConfigError, match="dedupKey"):
        load_alert_rules(
            {"rules": [{"name": "r", "eventName": "e", "dedupKey": [1], "sinks": ["w"]}]}
        )


# --------------------------------------------------------------------------- #
# AlertEngine — throttle + dedup                                              #
# --------------------------------------------------------------------------- #


def _engine(**rule_kwargs: Any) -> AlertEngine:
    base: dict[str, Any] = {"name": "r", "eventName": "e", "sinks": ["webhook"]}
    base.update(rule_kwargs)
    return AlertEngine(load_alert_rules({"rules": [base]}))


def test_untrottled_rule_always_fires() -> None:
    engine = _engine()
    evt = _event("e")
    assert len(engine.evaluate(evt, now=T0)) == 1
    assert len(engine.evaluate(evt, now=T0)) == 1  # again, immediately


def test_throttle_suppresses_within_window() -> None:
    engine = _engine(throttle="5m")
    evt = _event("e")
    assert len(engine.evaluate(evt, now=T0)) == 1
    # Inside the 5-minute window -> suppressed.
    assert engine.evaluate(evt, now=T0 + timedelta(minutes=4)) == []
    # At/after the window boundary -> fires again.
    assert len(engine.evaluate(evt, now=T0 + timedelta(minutes=5))) == 1


def test_dedup_key_throttles_independently_per_subject() -> None:
    engine = _engine(throttle="5m", dedupKey=["principal"])
    a = _event("e", payload={"principal": "alice"})
    b = _event("e", payload={"principal": "bob"})

    assert len(engine.evaluate(a, now=T0)) == 1
    # Different principal is a different identity -> not throttled.
    assert len(engine.evaluate(b, now=T0)) == 1
    # Same principal within window -> suppressed.
    assert engine.evaluate(a, now=T0 + timedelta(minutes=1)) == []


def test_match_returns_alertmatch_with_sinks_and_identity() -> None:
    engine = _engine(throttle="5m", dedupKey=["principal"])
    evt = _event("e", payload={"principal": "alice"})
    matches = engine.evaluate(evt, now=T0)
    assert matches[0].sinks == ("webhook",)
    assert matches[0].dedup_identity == ("r", "alice")
    assert matches[0].event is evt


def test_no_match_returns_empty() -> None:
    engine = _engine()
    assert engine.evaluate(_event("other"), now=T0) == []


def test_evaluate_audit_builds_view_from_audit_event() -> None:
    ruleset = load_alert_rules(
        {
            "rules": [
                {
                    "name": "r",
                    "eventName": "obs.outbox.lagging",
                    "severity": "warning",
                    "sinks": ["w"],
                }
            ]
        }
    )
    engine = AlertEngine(ruleset)
    audit = AuditEvent(
        workspace_id=WorkspaceId("__platform__"),
        event_id="evt-1",
        event_type="obs.outbox.lagging",
        actor="system",
        subject={"component": "observability-audit-service", "severity": "warning"},
        payload={"pipeline_id": "audit-store"},
        occurred_at=T0,
    )
    matches = engine.evaluate_audit(audit, now=T0)
    assert [m.rule.name for m in matches] == ["r"]
    assert matches[0].event.component == "observability-audit-service"


def test_from_audit_event_handles_missing_subject_fields() -> None:
    audit = AuditEvent(
        workspace_id=WorkspaceId("ws"),
        event_id="evt-2",
        event_type="some.event",
        actor="system",
        subject={},
        payload={},
        occurred_at=T0,
    )
    view = MatchableEvent.from_audit_event(audit)
    assert view.severity is None
    assert view.component is None
    assert view.event_name == "some.event"


def test_ruleset_and_rule_are_frozen() -> None:
    rule = AlertRule(name="r", sinks=("w",), event_name="e")
    with pytest.raises(AttributeError):
        rule.name = "x"  # type: ignore[misc]
    ruleset = AlertRuleSet(rules=(rule,))
    with pytest.raises(AttributeError):
        ruleset.rules = ()  # type: ignore[misc]


def test_engine_exposes_ruleset() -> None:
    ruleset = load_alert_rules({"rules": [{"name": "r", "eventName": "e", "sinks": ["w"]}]})
    engine = AlertEngine(ruleset)
    assert engine.ruleset is ruleset


def test_throttle_state_is_evicted_after_window() -> None:
    engine = _engine(throttle="5m", dedupKey=["principal"])
    a = _event("e", payload={"principal": "alice"})
    b = _event("e", payload={"principal": "bob"})

    engine.evaluate(a, now=T0)
    engine.evaluate(b, now=T0)
    assert len(engine._throttled_until) == 2

    # After both windows elapse, a later evaluation evicts the stale identities.
    engine.evaluate(_event("other"), now=T0 + timedelta(minutes=10))
    assert engine._throttled_until == {}


def test_refire_after_eviction_rearms_window() -> None:
    engine = _engine(throttle="5m")
    evt = _event("e")
    assert len(engine.evaluate(evt, now=T0)) == 1
    # Past the window -> fires again and re-arms.
    assert len(engine.evaluate(evt, now=T0 + timedelta(minutes=6))) == 1
    # Inside the new window -> suppressed again.
    assert engine.evaluate(evt, now=T0 + timedelta(minutes=7)) == []


def test_rule_match_mapping_is_immutable() -> None:
    rule = AlertRule(name="r", sinks=("w",), match={"k": "v"})
    with pytest.raises(TypeError):
        rule.match["k"] = "other"  # type: ignore[index]


def test_matchable_event_mappings_are_immutable() -> None:
    payload = {"k": "v"}
    subject = {"s": "t"}
    evt = MatchableEvent(
        event_name="e",
        severity=None,
        component=None,
        payload=payload,
        subject=subject,
    )
    with pytest.raises(TypeError):
        evt.payload["k"] = "x"  # type: ignore[index]
    with pytest.raises(TypeError):
        evt.subject["s"] = "x"  # type: ignore[index]
    # Mutating the source dict must not bleed into the frozen view.
    payload["k"] = "mutated"
    assert evt.payload["k"] == "v"
