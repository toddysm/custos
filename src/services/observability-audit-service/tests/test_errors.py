"""Tests for the locked RFC 7807 error taxonomy (OBS-IMPL-003)."""

from __future__ import annotations

import pytest

from custos_obs.errors import (
    LOCKED_OBS_ERROR_KINDS,
    PROBLEM_CONTENT_TYPE,
    PROBLEM_TYPE_PREFIX,
    AlertSinkUnavailable,
    AuditDrainLagging,
    ExporterConfigInvalid,
    LogQueryUnavailable,
    MetricsQueryUnavailable,
    ObsError,
    ObsErrorKind,
)

#: Every concrete error class paired with its design-pinned kind + HTTP status.
_ERROR_CLASSES: list[tuple[type[ObsError], ObsErrorKind, int]] = [
    (LogQueryUnavailable, ObsErrorKind.LOG_QUERY_UNAVAILABLE, 503),
    (MetricsQueryUnavailable, ObsErrorKind.METRICS_QUERY_UNAVAILABLE, 503),
    (AuditDrainLagging, ObsErrorKind.AUDIT_DRAIN_LAGGING, 500),
    (AlertSinkUnavailable, ObsErrorKind.ALERT_SINK_UNAVAILABLE, 502),
    (ExporterConfigInvalid, ObsErrorKind.EXPORTER_CONFIG_INVALID, 422),
]


def test_locked_kind_set_matches_enum() -> None:
    assert {member.value for member in ObsErrorKind} == LOCKED_OBS_ERROR_KINDS


def test_locked_kind_set_is_exactly_the_design_failure_modes() -> None:
    assert {
        "LogQueryUnavailable",
        "MetricsQueryUnavailable",
        "AuditDrainLagging",
        "AlertSinkUnavailable",
        "ExporterConfigInvalid",
    } == LOCKED_OBS_ERROR_KINDS


def test_kind_string_values_are_pascal_case_names() -> None:
    assert ObsErrorKind.LOG_QUERY_UNAVAILABLE.value == "LogQueryUnavailable"
    assert ObsErrorKind.METRICS_QUERY_UNAVAILABLE.value == "MetricsQueryUnavailable"
    assert ObsErrorKind.AUDIT_DRAIN_LAGGING.value == "AuditDrainLagging"
    assert ObsErrorKind.ALERT_SINK_UNAVAILABLE.value == "AlertSinkUnavailable"
    assert ObsErrorKind.EXPORTER_CONFIG_INVALID.value == "ExporterConfigInvalid"


def test_every_error_class_pins_kind_title_and_status() -> None:
    for cls, kind, status in _ERROR_CLASSES:
        assert cls.kind is kind
        assert cls.status == status
        assert cls.title  # non-empty human-readable title


def test_problem_content_type_constant() -> None:
    assert PROBLEM_CONTENT_TYPE == "application/problem+json"


def test_each_error_is_an_obs_error_subclass() -> None:
    for cls, _kind, _status in _ERROR_CLASSES:
        assert issubclass(cls, ObsError)
        assert issubclass(cls, Exception)


def test_to_dict_is_rfc7807_shaped() -> None:
    err = LogQueryUnavailable("loki backend unreachable")
    body = err.to_dict()
    assert body == {
        "type": f"{PROBLEM_TYPE_PREFIX}LogQueryUnavailable",
        "title": "Log query backend unavailable",
        "status": 503,
        "detail": "loki backend unreachable",
    }


def test_type_uri_property() -> None:
    err = MetricsQueryUnavailable("down")
    assert err.type_uri == "urn:custos:obs:problem:MetricsQueryUnavailable"
    assert err.to_dict()["type"] == err.type_uri


def test_to_dict_includes_instance_when_set() -> None:
    err = ExporterConfigInvalid(
        "unknown exporter type 'frobnicate'",
        instance="/v1/exporters/custos-otel-exporters",
    )
    body = err.to_dict()
    assert body["instance"] == "/v1/exporters/custos-otel-exporters"


def test_to_dict_omits_instance_when_absent() -> None:
    assert "instance" not in AuditDrainLagging("behind").to_dict()


def test_to_dict_merges_extension_members() -> None:
    err = LogQueryUnavailable(
        "provider is noop",
        extensions={"externalUrl": "https://logs.example.com"},
    )
    body = err.to_dict()
    assert body["externalUrl"] == "https://logs.example.com"
    # Reserved members are still present and untouched.
    assert body["status"] == 503


def test_extensions_may_not_shadow_reserved_members() -> None:
    with pytest.raises(ValueError, match="reserved Problem Details members"):
        AlertSinkUnavailable("smtp down", extensions={"status": 200, "title": "x"})


def test_str_includes_kind_and_detail() -> None:
    err = AuditDrainLagging("12000 rows behind")
    assert str(err) == "AuditDrainLagging: 12000 rows behind"


def test_errors_can_be_raised_and_caught_as_obs_error() -> None:
    for cls, _kind, _status in _ERROR_CLASSES:
        with pytest.raises(ObsError):
            raise cls("boom")
