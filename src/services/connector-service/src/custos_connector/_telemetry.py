"""OpenTelemetry instrumentation for the connector-service public surface.

Implements CONN-IMPL-029 (Phase K, #312). Exposes a tracer, a meter, the
five named Prometheus metrics from the issue acceptance criteria, and
context-manager helpers that wrap the load-bearing paths
(``BindForStep``, lease issue/refresh, pull-loop tick, manifest
discovery, identity resolution) with a span.

Design notes
------------

The module imports the OpenTelemetry API only. The API ships default
no-op providers, so consumers without an SDK installed can import
``custos_connector`` safely without configuring telemetry first.

Production deployments configure their own SDK on
:func:`custos_connector.create_app`: the lifespan hook installs a
:class:`opentelemetry.sdk.trace.TracerProvider` and a
:class:`opentelemetry.sdk.metrics.MeterProvider` whose
:class:`~opentelemetry.exporter.prometheus.PrometheusMetricReader`
registers every meter with the global ``prometheus_client`` registry
that backs the FastAPI ``/metrics`` endpoint.

Metric names (frozen by issue #312)
-----------------------------------

* ``custos_connector_bind_latency_seconds`` — histogram, no labels.
  Recorded by :func:`observe_bind` around
  :meth:`BindForStepService.bind_for_step`.
* ``custos_connector_active_leases`` — up-down counter (treated as a
  gauge by the Prometheus exporter), labelled by
  ``connectorInstanceId``. Bumped ``+1`` on
  :meth:`LeaseManager.issue` success and ``-1`` on
  :meth:`LeaseManager.release` / lease expiry / revoke.
* ``custos_connector_pull_tick_duration_seconds`` — histogram, no
  labels. Recorded by :func:`observe_pull_tick` around
  :meth:`CursorService.tick`.
* ``custos_connector_cursor_lag_seconds`` — observable gauge, labelled
  by ``connectorInstanceId``. Computed from the wall-clock delta to
  the last ``advancedAt`` for each registered instance via
  :class:`CursorLagRegistry`.
* ``custos_connector_manifest_fallback_total`` — counter, labelled by
  ``outcome`` (``used`` | ``ignored`` | ``rejected``). Bumped by the
  manifest discovery audit-emission sites.

Span names
----------

* ``custos_connector.bind_for_step``
* ``custos_connector.lease.issue``
* ``custos_connector.lease.refresh``
* ``custos_connector.pull_tick``
* ``custos_connector.manifest.discovery``
* ``custos_connector.identity.resolve``

Each context manager records into the appropriate histogram (when one
exists for that path) and marks the span as ERROR on any propagated
exception. ``KeyboardInterrupt`` / ``SystemExit`` /
``asyncio.CancelledError`` always propagate untouched so process
control is never recorded as an application error.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import AbstractContextManager
from datetime import UTC, datetime
from typing import Final

from opentelemetry import context, metrics, trace
from opentelemetry.metrics import (
    CallbackOptions,
    Counter,
    Histogram,
    Meter,
    Observation,
    UpDownCounter,
)
from opentelemetry.trace import Span, Status, StatusCode, Tracer

_LOGGER = logging.getLogger("custos_connector.telemetry")

_INSTRUMENTATION_NAME: Final[str] = "custos_connector"
_INSTRUMENTATION_VERSION: Final[str] = "0.1.0"

_tracer: Tracer = trace.get_tracer(_INSTRUMENTATION_NAME, _INSTRUMENTATION_VERSION)
_meter: Meter = metrics.get_meter(_INSTRUMENTATION_NAME, _INSTRUMENTATION_VERSION)


# ---------------------------------------------------------------------------
# Manifest-fallback outcome labels (frozen by issue #312)
# ---------------------------------------------------------------------------

#: Referrers API was empty and the deterministic fallback tag resolved.
FALLBACK_OUTCOME_USED: Final[str] = "used"
#: Referrers API resolved authoritatively; the fallback path was
#: deliberately not consulted.
FALLBACK_OUTCOME_IGNORED: Final[str] = "ignored"
#: Discovery rejected on the fallback path or during final resolution
#: (ambiguous, unknown digest algorithm, tag too long, etc.).
FALLBACK_OUTCOME_REJECTED: Final[str] = "rejected"


# ---------------------------------------------------------------------------
# Histograms
# ---------------------------------------------------------------------------

BIND_LATENCY_SECONDS: Final[Histogram] = _meter.create_histogram(
    name="custos_connector_bind_latency_seconds",
    unit="s",
    description=(
        "Wall-clock latency of BindForStepService.bind_for_step, the "
        "service-to-service entry point that resolves a workflow step's "
        "connector slots into ConnectorContexts. Recorded around the "
        "whole bind pipeline (workspace/type lookup, capability check, "
        "identity resolution, plugin bind hook, audit emission). The "
        "cache-hit fast path is intentionally NOT recorded so the "
        "histogram tracks resolver work only."
    ),
)


PULL_TICK_DURATION_SECONDS: Final[Histogram] = _meter.create_histogram(
    name="custos_connector_pull_tick_duration_seconds",
    unit="s",
    description=(
        "Wall-clock duration of CursorService.tick, the per-instance "
        "pull-loop iteration that acquires the cursor lease, invokes the "
        "plugin listen(mode=pull) hook, publish-acks the resulting batch, "
        "and commits the new cursor. Recorded for every tick that "
        "started a plugin call regardless of outcome (success, halt, or "
        "lease-busy bail-out)."
    ),
)


# ---------------------------------------------------------------------------
# Counters + up-down counters
# ---------------------------------------------------------------------------

ACTIVE_LEASES: Final[UpDownCounter] = _meter.create_up_down_counter(
    name="custos_connector_active_leases",
    description=(
        "Current count of un-released, un-expired, un-revoked leases the "
        "Lease Manager has issued, labelled by connectorInstanceId. "
        "Bumped +1 on LeaseManager.issue success; -1 on release / sweeper "
        "expiry / revoke. Treated as a gauge by the Prometheus exporter."
    ),
)


MANIFEST_FALLBACK_TOTAL: Final[Counter] = _meter.create_counter(
    name="custos_connector_manifest_fallback_total",
    description=(
        "Count of manifest-discovery decisions, labelled by outcome "
        "(used | ignored | rejected). One increment per discover_manifest "
        "call. Matches the connector.manifest.fallback-{used,ignored,"
        "rejected} audit-event taxonomy 1:1."
    ),
)


def record_manifest_fallback(outcome: str) -> None:
    """Bump :data:`MANIFEST_FALLBACK_TOTAL` with the given outcome label.

    Centralised so emission sites cannot accidentally drift on the
    label string (the prometheus_client registry will accept any
    string, so this guard prevents silent label-set fragmentation).
    """
    if outcome not in (FALLBACK_OUTCOME_USED, FALLBACK_OUTCOME_IGNORED, FALLBACK_OUTCOME_REJECTED):
        # Defensive: refuse unknown labels rather than fragmenting the
        # metric's label set. Counter still ticks under ``unknown`` so
        # the operator can see drift on the dashboard.
        _LOGGER.warning("manifest fallback outcome %r is not a known label", outcome)
        MANIFEST_FALLBACK_TOTAL.add(1, {"outcome": "unknown"})
        return
    MANIFEST_FALLBACK_TOTAL.add(1, {"outcome": outcome})


def record_lease_issued(connector_instance_id: str) -> None:
    """Increment :data:`ACTIVE_LEASES` by one for ``connector_instance_id``."""
    ACTIVE_LEASES.add(1, {"connectorInstanceId": connector_instance_id})


def record_lease_closed(connector_instance_id: str) -> None:
    """Decrement :data:`ACTIVE_LEASES` by one for ``connector_instance_id``.

    Called from the release / expiry / revoke success paths. The
    up-down counter is allowed to go negative if a deployment restarts
    mid-flight (the in-memory baseline does not survive process
    restarts); operators should rely on the lease store's authoritative
    count for forensic reconciliation.
    """
    ACTIVE_LEASES.add(-1, {"connectorInstanceId": connector_instance_id})


# ---------------------------------------------------------------------------
# Cursor-lag observable gauge
# ---------------------------------------------------------------------------


class CursorLagRegistry:
    """Per-process registry of ``(workspace, instance) → advancedAt``.

    Backs the :data:`CURSOR_LAG_SECONDS` observable gauge.
    :class:`~custos_connector.cursor.service.CursorService` writes the
    new wall-clock on every successful commit via :meth:`record_advanced`;
    on instance halt or operator-driven removal the caller invokes
    :meth:`forget`. The gauge callback walks the snapshot once per scrape
    and emits ``now - advancedAt`` per registered instance.

    Thread safety: a single :class:`threading.Lock` guards the dict so a
    concurrent collector thread (the OTel SDK runs metric callbacks on
    its own thread) sees a consistent snapshot. The lock is held only
    for the dict copy / mutation; the gauge math runs outside the lock.
    """

    def __init__(self, clock: Callable[[], datetime] | None = None) -> None:
        self._clock = clock if clock is not None else lambda: datetime.now(UTC)
        self._lock = threading.Lock()
        self._advanced_at: dict[tuple[str, str], datetime] = {}

    def record_advanced(
        self,
        *,
        workspace_id: str,
        instance_id: str,
        advanced_at: datetime,
    ) -> None:
        """Record the latest ``advancedAt`` for one instance.

        Overwrites the previous entry. Idempotent — callers may write
        the same value repeatedly without effect on the gauge.
        """
        with self._lock:
            self._advanced_at[(workspace_id, instance_id)] = advanced_at

    def forget(self, *, workspace_id: str, instance_id: str) -> None:
        """Drop one entry. No-op if the key is absent."""
        with self._lock:
            self._advanced_at.pop((workspace_id, instance_id), None)

    def snapshot(self) -> Mapping[tuple[str, str], datetime]:
        """Return a shallow copy of the current advanced-at map.

        Used by tests + the gauge callback. Holding the snapshot does
        not block subsequent writers because the lock is released on
        return.
        """
        with self._lock:
            return dict(self._advanced_at)


#: Process-wide registry. :class:`CursorService` constructs against the
#: default instance so the gauge callback below has data to read.
#: Tests may construct their own :class:`CursorLagRegistry` and inject
#: it into :class:`CursorService`; the gauge callback always reads
#: from the module-level default.
CURSOR_LAG_REGISTRY: Final[CursorLagRegistry] = CursorLagRegistry()


def _cursor_lag_observable_callback(
    _options: CallbackOptions,
) -> Iterator[Observation]:
    """Compute ``now - advancedAt`` per registered instance.

    Run on every scrape by the OTel SDK. The callback is robust to an
    empty registry: it yields no observations, which the exporter
    encodes as an absent series.
    """
    now = datetime.now(UTC)
    for (workspace_id, instance_id), advanced_at in CURSOR_LAG_REGISTRY.snapshot().items():
        lag_seconds = (now - advanced_at).total_seconds()
        # Clamp to zero so a clock skew between writer and reader does
        # not produce a negative-lag sample (Prometheus tolerates them,
        # but operators interpret negative lag as a bug).
        if lag_seconds < 0.0:
            lag_seconds = 0.0
        yield Observation(
            lag_seconds,
            {
                "workspaceId": workspace_id,
                "connectorInstanceId": instance_id,
            },
        )


CURSOR_LAG_SECONDS = _meter.create_observable_gauge(
    name="custos_connector_cursor_lag_seconds",
    description=(
        "Wall-clock seconds since the last successful cursor commit, "
        "labelled by workspaceId + connectorInstanceId. Computed from "
        "CURSOR_LAG_REGISTRY on every scrape; an instance disappears "
        "from the gauge as soon as it halts (cursor-expired or "
        "encoding-mismatch) or is unregistered."
    ),
    unit="s",
    callbacks=[_cursor_lag_observable_callback],
)


# ---------------------------------------------------------------------------
# Span-only helpers (no histogram recording)
# ---------------------------------------------------------------------------


class _SpanOnly:
    """Plain class-based context manager wrapping a span.

    Not a :func:`contextlib.contextmanager`: contextlib's ``__exit__``
    rebinds ``exc.__traceback__`` on the in-flight exception while
    re-raising through ``gen.throw()``, which triggers a ``TypeError``
    on ``@dataclass(frozen=True, slots=True)`` Exception subclasses
    such as :class:`ManifestDiscoveryError` (the bare ``super()``
    inside the dataclass-generated ``__setattr__`` resolves to the
    pre-slot class and fails the ``isinstance`` check). A plain class
    implementation lets Python's native re-raise machinery propagate
    the exception without touching ``__traceback__``.

    ``KeyboardInterrupt`` / ``SystemExit`` / ``asyncio.CancelledError``
    propagate untouched and never mark the span as ERROR; those events
    are not application failures.
    """

    __slots__ = ("_span", "_span_name", "_token")

    def __init__(self, span_name: str) -> None:
        self._span_name = span_name
        self._span: Span | None = None
        self._token: object | None = None

    def __enter__(self) -> Span:
        self._span = _tracer.start_span(self._span_name)
        self._token = context.attach(trace.set_span_in_context(self._span))
        return self._span

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        _tb: object,
    ) -> None:
        span = self._span
        token = self._token
        try:
            if (
                exc is not None
                and exc_type is not None
                and not issubclass(exc_type, KeyboardInterrupt | SystemExit)
                and exc_type.__name__ != "CancelledError"
                and span is not None
            ):
                span.set_status(Status(StatusCode.ERROR, exc_type.__name__))
                _safe_record_exception(span, exc)
        finally:
            if token is not None:
                context.detach(token)  # type: ignore[arg-type]
            if span is not None:
                span.end()
        # Returning ``None`` (treated as falsy) tells Python to
        # propagate the exception via the native re-raise path, which
        # does NOT touch ``exc.__traceback__``.
        return None


class _SpanAndHistogram(_SpanOnly):
    """Span + histogram-recording context manager.

    Records wall-clock seconds into ``histogram`` on both the success
    and the exception path so the histogram count matches the call
    count. Inherits from :class:`_SpanOnly` for the span lifecycle;
    histogram recording happens in ``__exit__`` before the span ends.
    """

    __slots__ = ("_histogram", "_labels", "_start")

    def __init__(
        self,
        span_name: str,
        histogram: Histogram,
        labels: Mapping[str, str] | None = None,
    ) -> None:
        super().__init__(span_name)
        self._histogram = histogram
        self._labels: dict[str, str] = dict(labels) if labels else {}
        self._start: float = 0.0

    def __enter__(self) -> Span:
        self._start = time.perf_counter()
        return super().__enter__()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: object,
    ) -> None:
        elapsed = time.perf_counter() - self._start
        if exc is None:
            self._histogram.record(elapsed, {**self._labels, "outcome": "success"})
        elif (
            exc_type is not None
            and not issubclass(exc_type, KeyboardInterrupt | SystemExit)
            and exc_type.__name__ != "CancelledError"
        ):
            self._histogram.record(elapsed, {**self._labels, "outcome": "error"})
        super().__exit__(exc_type, exc, tb)
        return None


def _safe_record_exception(span: Span, exc: BaseException) -> None:
    """Best-effort wrapper around :meth:`Span.record_exception`.

    Suppresses any error raised while serialising the exception
    (typically the ``@dataclass(frozen=True, slots=True)`` Exception
    subclass interaction described in :class:`_SpanOnly`). The span
    still carries the ERROR status set by the caller; only the
    structured event attachment is best-effort.
    """
    try:
        span.record_exception(exc)
    except Exception:
        _LOGGER.debug("span.record_exception swallowed", exc_info=True)


def observe_bind() -> AbstractContextManager[Span]:
    """Wrap a ``BindForStep`` call with a span + bind-latency sample."""
    return _SpanAndHistogram(
        "custos_connector.bind_for_step",
        BIND_LATENCY_SECONDS,
    )


def observe_lease_issue() -> AbstractContextManager[Span]:
    """Wrap a :meth:`LeaseManager.issue` call with a span only."""
    return _SpanOnly("custos_connector.lease.issue")


def observe_lease_refresh() -> AbstractContextManager[Span]:
    """Wrap a :meth:`LeaseManager.refresh` call with a span only."""
    return _SpanOnly("custos_connector.lease.refresh")


def observe_pull_tick() -> AbstractContextManager[Span]:
    """Wrap a :meth:`CursorService.tick` call with span + duration sample."""
    return _SpanAndHistogram(
        "custos_connector.pull_tick",
        PULL_TICK_DURATION_SECONDS,
    )


def observe_manifest_discovery() -> AbstractContextManager[Span]:
    """Wrap a :func:`discover_manifest` call with a span only."""
    return _SpanOnly("custos_connector.manifest.discovery")


def observe_identity_resolution() -> AbstractContextManager[Span]:
    """Wrap an :meth:`IdentityResolverRegistry.resolve` call with a span only."""
    return _SpanOnly("custos_connector.identity.resolve")


__all__ = [
    "ACTIVE_LEASES",
    "BIND_LATENCY_SECONDS",
    "CURSOR_LAG_REGISTRY",
    "CURSOR_LAG_SECONDS",
    "FALLBACK_OUTCOME_IGNORED",
    "FALLBACK_OUTCOME_REJECTED",
    "FALLBACK_OUTCOME_USED",
    "MANIFEST_FALLBACK_TOTAL",
    "PULL_TICK_DURATION_SECONDS",
    "CursorLagRegistry",
    "observe_bind",
    "observe_identity_resolution",
    "observe_lease_issue",
    "observe_lease_refresh",
    "observe_manifest_discovery",
    "observe_pull_tick",
    "record_lease_closed",
    "record_lease_issued",
    "record_manifest_fallback",
]
