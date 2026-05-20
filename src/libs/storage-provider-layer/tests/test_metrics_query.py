"""Tests for MetricsQueryProvider Protocol and its data shapes."""

from __future__ import annotations

import inspect
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime

import pytest

from custos_spl.interfaces import (
    MetricRange,
    MetricSample,
    MetricSelector,
    MetricSeries,
    MetricsQueryProvider,
)


def _now() -> datetime:
    return datetime(2026, 1, 1, tzinfo=UTC)


# ----- Data shape -----


def test_metric_selector_is_frozen() -> None:
    s = MetricSelector(name="cpu_seconds_total")
    with pytest.raises(FrozenInstanceError):
        s.name = "other"  # type: ignore[misc]


def test_metric_selector_accepts_label_matchers() -> None:
    s = MetricSelector(
        name="cpu_seconds_total", label_matchers={"job": "api"}
    )
    assert s.label_matchers["job"] == "api"


def test_metric_range_is_frozen() -> None:
    r = MetricRange(start=_now(), end=_now(), step_seconds=60)
    with pytest.raises(FrozenInstanceError):
        r.step_seconds = 30  # type: ignore[misc]


def test_metric_sample_is_frozen() -> None:
    s = MetricSample(timestamp=_now(), value=1.0)
    with pytest.raises(FrozenInstanceError):
        s.value = 2.0  # type: ignore[misc]


def test_metric_series_defaults_to_empty_samples() -> None:
    s = MetricSeries(name="cpu_seconds_total")
    assert s.samples == ()
    assert s.labels == {}


def test_metric_series_carries_samples_and_labels() -> None:
    sample = MetricSample(
        timestamp=_now(), value=0.5, labels={"instance": "a"}
    )
    series = MetricSeries(
        name="cpu_seconds_total",
        labels={"job": "api"},
        samples=(sample,),
    )
    assert series.samples[0] is sample
    assert series.labels["job"] == "api"


# ----- Protocol shape -----


def test_protocol_declares_schema_revision_zero() -> None:
    """Query facades own no schema; SCHEMA_REVISION is 0."""
    assert MetricsQueryProvider.SCHEMA_REVISION == 0


REQUIRED_METHODS = [
    "query_run_metrics",
    "query_workspace_metrics",
    "query_instant_metric",
]


@pytest.mark.parametrize("method", REQUIRED_METHODS)
def test_protocol_exposes_method(method: str) -> None:
    assert hasattr(MetricsQueryProvider, method)


@pytest.mark.parametrize("method", REQUIRED_METHODS)
def test_protocol_methods_are_async(method: str) -> None:
    fn = getattr(MetricsQueryProvider, method)
    assert inspect.iscoroutinefunction(fn), f"{method} must be async"


@pytest.mark.parametrize("method", REQUIRED_METHODS)
def test_methods_take_workspace_id_first(method: str) -> None:
    sig = inspect.signature(getattr(MetricsQueryProvider, method))
    params = list(sig.parameters)
    # params[0] is self
    assert params[1] == "workspace_id", (
        f"{method} must take workspace_id as the first non-self argument"
    )


# ----- runtime_checkable conformance -----


class _MinimalMetricsQuery:
    """Just enough of the Protocol to satisfy isinstance() at runtime."""

    SCHEMA_REVISION = 0

    async def query_run_metrics(self, *a: object, **kw: object) -> None: ...

    async def query_workspace_metrics(self, *a: object, **kw: object) -> None: ...

    async def query_instant_metric(self, *a: object, **kw: object) -> None: ...


def test_runtime_checkable_recognizes_duck_typed_impl() -> None:
    assert isinstance(_MinimalMetricsQuery(), MetricsQueryProvider)


def test_runtime_checkable_rejects_partial_impl() -> None:
    class Partial:
        async def query_run_metrics(self, *a: object, **kw: object) -> None: ...

    assert not isinstance(Partial(), MetricsQueryProvider)
