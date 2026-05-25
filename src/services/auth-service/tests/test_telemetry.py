"""Tests for :mod:`custos_auth._telemetry` (AS-IMPL-026 / #261).

Installs an in-memory OpenTelemetry SDK so we can read back every
span + metric the auth-service public surface emits. The SDK is only
a dev dependency — production deployments wire the Helm-managed OTel
Collector — so the imports below are inside the test module rather
than the source surface.
"""

from __future__ import annotations

from typing import Any

import pytest
from opentelemetry import metrics, trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

# ---------------------------------------------------------------------------
# Bootstrap an in-memory SDK before importing _telemetry
# ---------------------------------------------------------------------------
#
# ``custos_auth._telemetry`` captures the global tracer + meter at
# import time. To make those globals point at the in-memory SDK the
# global providers must already be installed when the module first
# imports ``opentelemetry.trace`` / ``opentelemetry.metrics``. Set
# them up in module scope so the assertions below see the spans /
# samples emitted by the auth-service public-API wrappers.

_SPAN_EXPORTER = InMemorySpanExporter()
_TRACE_PROVIDER = TracerProvider()
_TRACE_PROVIDER.add_span_processor(SimpleSpanProcessor(_SPAN_EXPORTER))
trace.set_tracer_provider(_TRACE_PROVIDER)

_METRIC_READER = InMemoryMetricReader()
_METER_PROVIDER = MeterProvider(metric_readers=[_METRIC_READER])
metrics.set_meter_provider(_METER_PROVIDER)


# Importing here forces module init *after* the providers are wired.
from custos_auth import _telemetry as telemetry  # noqa: E402

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _reset_exporter() -> None:
    _SPAN_EXPORTER.clear()


def _collect_metrics() -> list[dict[str, Any]]:
    """Return a flat list of ``{name, labels, value}`` data points.

    Aggregates the latest ``MetricsData`` snapshot exposed by the
    in-memory reader. Histogram points are flattened to ``count`` so
    tests can assert how many calls landed in each bucket.
    """
    data = _METRIC_READER.get_metrics_data()
    out: list[dict[str, Any]] = []
    if data is None:
        return out
    for resource_metric in data.resource_metrics:
        for scope_metric in resource_metric.scope_metrics:
            for metric in scope_metric.metrics:
                points = metric.data.data_points
                for point in points:
                    value = getattr(point, "count", None)
                    if value is None:
                        value = getattr(point, "value", None)
                    out.append(
                        {
                            "name": metric.name,
                            "labels": dict(point.attributes or {}),
                            "value": value,
                        },
                    )
    return out


class _Boom(Exception):
    """Programmer-error exception with no structured ``code``."""


class _AuthTaxonomy(Exception):
    """Structured auth-style error carrying a stable code."""

    code = "auth.test_failure"


# ---------------------------------------------------------------------------
# observe_operation
# ---------------------------------------------------------------------------


def test_observe_operation_records_success_sample_and_span() -> None:
    _reset_exporter()

    with telemetry.observe_operation(telemetry.OP_AUTHZ_VERIFY_AND_AUTHORIZE):
        pass

    spans = _SPAN_EXPORTER.get_finished_spans()
    assert any(s.name == f"custos_auth.{telemetry.OP_AUTHZ_VERIFY_AND_AUTHORIZE}" for s in spans)

    points = _collect_metrics()
    op_points = [
        p
        for p in points
        if p["name"] == "custos_auth_operation_duration_ms"
        and p["labels"].get("operation") == telemetry.OP_AUTHZ_VERIFY_AND_AUTHORIZE
    ]
    assert op_points, "operation duration sample was not recorded"
    assert any(p["labels"]["outcome"] == "success" for p in op_points)


def test_observe_operation_records_mapped_outcome_on_taxonomy_error() -> None:
    _reset_exporter()

    with (
        pytest.raises(_AuthTaxonomy),
        telemetry.observe_operation(
            telemetry.OP_SERVICE_TOKEN_ISSUE,
            outcomes={_AuthTaxonomy: "validation_failed"},
        ),
    ):
        raise _AuthTaxonomy

    points = _collect_metrics()
    op_points = [
        p
        for p in points
        if p["name"] == "custos_auth_operation_duration_ms"
        and p["labels"].get("operation") == telemetry.OP_SERVICE_TOKEN_ISSUE
    ]
    assert any(p["labels"]["outcome"] == "validation_failed" for p in op_points)

    err_points = [
        p
        for p in points
        if p["name"] == "custos_auth_errors_total"
        and p["labels"].get("kind") == "auth.test_failure"
    ]
    assert err_points, "structured error kind was not counted"


def test_observe_operation_unmapped_exception_falls_back_to_internal_error() -> None:
    _reset_exporter()

    with pytest.raises(_Boom), telemetry.observe_operation(telemetry.OP_PRINCIPAL_DISABLE):
        raise _Boom

    points = _collect_metrics()
    op_points = [
        p
        for p in points
        if p["name"] == "custos_auth_operation_duration_ms"
        and p["labels"].get("operation") == telemetry.OP_PRINCIPAL_DISABLE
    ]
    assert any(p["labels"]["outcome"] == "internal_error" for p in op_points)


def test_observe_operation_does_not_count_kind_when_code_absent() -> None:
    _reset_exporter()

    with pytest.raises(_Boom), telemetry.observe_operation(telemetry.OP_ROLE_LIST):
        raise _Boom

    points = _collect_metrics()
    # The _Boom class has no ``code`` attribute, so ERRORS_TOTAL must
    # not have been bumped for this call.
    boom_kind_points = [
        p
        for p in points
        if p["name"] == "custos_auth_errors_total"
        and p["labels"].get("kind", "").startswith("Boom")
    ]
    assert not boom_kind_points


def test_observe_operation_resolves_specific_before_base() -> None:
    """Declaration order: specific subclass wins over a broader base."""

    _reset_exporter()

    class _Base(Exception):
        code = "auth.base"

    class _Sub(_Base):
        code = "auth.sub"

    with (
        pytest.raises(_Sub),
        telemetry.observe_operation(
            telemetry.OP_ROLE_BINDING_GRANT,
            outcomes={_Sub: "sub_label", _Base: "base_label"},
        ),
    ):
        raise _Sub

    points = _collect_metrics()
    op_points = [
        p
        for p in points
        if p["name"] == "custos_auth_operation_duration_ms"
        and p["labels"].get("operation") == telemetry.OP_ROLE_BINDING_GRANT
    ]
    # The most-recent label observed for this operation must be the
    # subclass label, not the base.
    assert any(p["labels"]["outcome"] == "sub_label" for p in op_points)


# ---------------------------------------------------------------------------
# Process-control exceptions must propagate unrecorded
# ---------------------------------------------------------------------------


def test_keyboard_interrupt_is_not_recorded() -> None:
    _reset_exporter()

    # Snapshot ERRORS_TOTAL count *for any kind* before the call so
    # we can confirm KeyboardInterrupt does not bump the counter.
    before = _collect_metrics()
    before_errors = sum(p["value"] or 0 for p in before if p["name"] == "custos_auth_errors_total")

    with (
        pytest.raises(KeyboardInterrupt),
        telemetry.observe_operation(
            telemetry.OP_JWKS_GET,
        ),
    ):
        raise KeyboardInterrupt

    after = _collect_metrics()
    after_errors = sum(p["value"] or 0 for p in after if p["name"] == "custos_auth_errors_total")
    assert after_errors == before_errors


# ---------------------------------------------------------------------------
# record_error_kind
# ---------------------------------------------------------------------------


def test_record_error_kind_bumps_counter() -> None:
    _reset_exporter()

    before = _collect_metrics()
    before_n = sum(
        p["value"] or 0
        for p in before
        if p["name"] == "custos_auth_errors_total"
        and p["labels"].get("kind") == "callctx.signature_invalid"
    )

    telemetry.record_error_kind("callctx.signature_invalid")

    after = _collect_metrics()
    after_n = sum(
        p["value"] or 0
        for p in after
        if p["name"] == "custos_auth_errors_total"
        and p["labels"].get("kind") == "callctx.signature_invalid"
    )
    assert after_n == before_n + 1
