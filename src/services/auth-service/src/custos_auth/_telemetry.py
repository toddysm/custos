"""OpenTelemetry instrumentation for the ``custos_auth`` public surface.

Implements AS-IMPL-026 (#261). Exposes a tracer, a meter, a duration
histogram covering every REST route and internal-RPC method, and one
per-``kind`` error counter — all keyed to the error taxonomy raised
by the auth-service handlers.

Design notes
------------

The module imports ``opentelemetry-api`` only. The API ships default
no-op providers, so consumers without an SDK installed can import
``custos_auth`` safely without configuring telemetry first.
Production deployments configure their own SDK (the auth-service
Helm subchart wires the OTel Collector sidecar per design §
Telemetry); the in-memory SDK is dev-only and exists exclusively to
drive the assertions in ``tests/test_telemetry.py``.

Auth-service has no internal pipeline like catalog-service's
publish-stages — every public operation is a single REST handler or
RPC method, so a single operation histogram suffices. The
instrumentation is intentionally narrow: spans + samples are emitted
at each public entry point (REST handler body or RPC method body)
rather than around individual helpers, so a histogram bucket count
matches the request count for that operation.

Metric / span names
-------------------

* ``custos_auth_operation_duration_ms`` — histogram, labels
  ``operation`` (one of the canonical operation strings exported
  below) and ``outcome`` (``success`` or a stable error-kind slug).
* ``custos_auth_errors_total`` — counter, label ``kind``. The
  ``kind`` string is the ``code:`` attribute on the structured
  errors raised by the handler (``auth.principal_disabled`` etc.)
  or the HTTP error envelope ``code`` for boundary failures.
* Spans: ``custos_auth.<operation>`` (e.g.
  ``custos_auth.authz.verify_and_authorize``) at the public entry
  point.

The list of canonical operation labels mirrors the public surface
table in ``design/components/auth-service/design.md`` § "Surfaces"
and the RPC table in § "Internal RPC". Adding a new public route or
RPC method requires adding a matching ``OP_*`` constant here so the
histogram labelset stays bounded and the test asserting that every
route is instrumented stays exhaustive.
"""

from __future__ import annotations

import time
from collections.abc import Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager
from typing import Final

from opentelemetry import metrics, trace
from opentelemetry.metrics import Counter, Histogram, Meter
from opentelemetry.trace import Span, Status, StatusCode, Tracer

_INSTRUMENTATION_NAME: Final[str] = "custos_auth"
_INSTRUMENTATION_VERSION: Final[str] = "0.1.0"

# Outcome label used on the duration histogram when the wrapped
# operation returns normally.
_SUCCESS: Final[str] = "success"

# Outcome label used for any exception not in the per-call outcomes
# map. Public APIs may raise built-in exceptions (validation guards,
# programmer-error ``RuntimeError`` etc.); this catch-all labels them
# ``internal_error`` so histogram totals match call counts even when
# something unexpected slips through.
_INTERNAL_ERROR: Final[str] = "internal_error"


_tracer: Tracer = trace.get_tracer(_INSTRUMENTATION_NAME, _INSTRUMENTATION_VERSION)
_meter: Meter = metrics.get_meter(_INSTRUMENTATION_NAME, _INSTRUMENTATION_VERSION)


OPERATION_DURATION_MS: Final[Histogram] = _meter.create_histogram(
    name="custos_auth_operation_duration_ms",
    unit="ms",
    description=(
        "Wall-clock time of auth-service public operations, labelled by "
        "operation (one of the REST route or RPC slugs, e.g. "
        "authz.verify_and_authorize, rpc.callctx_verify) and outcome "
        "(success or a stable error-kind slug)."
    ),
)


ERRORS_TOTAL: Final[Counter] = _meter.create_counter(
    name="custos_auth_errors_total",
    description=(
        "Count of auth-service errors raised through the public REST and "
        "RPC surfaces, labelled by the structured error 'kind' (the "
        "``code`` attribute on the handler error or the HTTP envelope "
        "code)."
    ),
)


# ---------------------------------------------------------------------------
# Canonical operation labels
# ---------------------------------------------------------------------------
#
# REST surface (``/v1/...``). One label per HTTP route declared under
# ``custos_auth/api/routes/``. The label values stay dotted so they
# group naturally in dashboards (``token.*``, ``role_binding.*`` …).

# auth.py
OP_AUTH_VERIFY: Final[str] = "auth.verify"

# authz.py
OP_AUTHZ_VERIFY_AND_AUTHORIZE: Final[str] = "authz.verify_and_authorize"

# jwks.py
OP_JWKS_GET: Final[str] = "jwks.get"

# oidc.py
OP_OIDC_CALLBACK: Final[str] = "oidc.callback"

# principals.py
OP_PRINCIPAL_GET_ME: Final[str] = "principal.get_me"
OP_PRINCIPAL_DISABLE: Final[str] = "principal.disable"

# role_bindings.py
OP_ROLE_BINDING_GRANT: Final[str] = "role_binding.grant"
OP_ROLE_BINDING_REVOKE: Final[str] = "role_binding.revoke"

# roles.py
OP_ROLE_LIST: Final[str] = "role.list"
OP_PERMISSION_LIST: Final[str] = "permission.list"

# service_accounts.py
OP_SERVICE_ACCOUNT_CREATE: Final[str] = "service_account.create"

# service_tokens.py
OP_SERVICE_TOKEN_ISSUE: Final[str] = "service_token.issue"
OP_SERVICE_TOKEN_LIST: Final[str] = "service_token.list"
OP_SERVICE_TOKEN_REVOKE: Final[str] = "service_token.revoke"
OP_SERVICE_TOKEN_REVOKE_ALL: Final[str] = "service_token.revoke_all"

# tenants.py
OP_TENANT_CREATE: Final[str] = "tenant.create"
OP_TENANT_LIST: Final[str] = "tenant.list"
OP_WORKSPACE_CREATE: Final[str] = "workspace.create"

# workspaces.py
OP_WORKSPACE_LIST: Final[str] = "workspace.list"
OP_WORKSPACE_GET: Final[str] = "workspace.get"

# RPC surface (``/rpc/...``). One label per dotted method name in the
# design's "Internal RPC" table.
OP_RPC_AUTHN_VERIFY_TOKEN: Final[str] = "rpc.authn_verify_token"
OP_RPC_AUTHZ_AUTHORIZE: Final[str] = "rpc.authz_authorize"
OP_RPC_AUTHZ_VERIFY_AND_AUTHORIZE: Final[str] = "rpc.authz_verify_and_authorize"
OP_RPC_CALLCTX_SIGN: Final[str] = "rpc.callctx_sign"
OP_RPC_CALLCTX_VERIFY: Final[str] = "rpc.callctx_verify"


def _outcome_for(
    exc: BaseException,
    mapping: Mapping[type[BaseException], str],
) -> str:
    """Resolve the duration-histogram ``outcome`` label for ``exc``.

    Walks the mapping in declaration order so more specific exception
    types match before broader base classes. Falls back to
    :data:`_INTERNAL_ERROR` when nothing matches.
    """
    for exc_type, label in mapping.items():
        if isinstance(exc, exc_type):
            return label
    return _INTERNAL_ERROR


def _error_kind(exc: BaseException) -> str | None:
    """Return the structured ``code`` slug for an exception if it carries one.

    Auth-service handler errors expose their HTTP/error-envelope code
    as a class attribute named ``code``. Anything else is treated as
    an unstructured failure and is not counted into
    :data:`ERRORS_TOTAL`.
    """
    code = getattr(exc, "code", None)
    if isinstance(code, str) and code:
        return code
    return None


@contextmanager
def instrument(
    span_name: str,
    histogram: Histogram,
    labels: Mapping[str, str],
    outcomes: Mapping[type[BaseException], str],
) -> Iterator[Span]:
    """Wrap a public-API call with a span + duration sample + error counter.

    Yields the active span so the caller can attach
    operation-specific attributes (principal id, workspace id, etc.)
    before the wrapped work runs. On normal completion the duration
    histogram receives a sample with the supplied ``labels`` plus
    ``outcome=success``; on a raised exception it receives a sample
    labelled by ``outcomes[type(exc)]`` (falling back to
    ``internal_error``) and the :data:`ERRORS_TOTAL` counter is bumped
    by one with the error's stable ``code`` slug when present. The
    exception is always re-raised so the wrapper is transparent.

    Args:
        span_name: Dotted span name (``custos_auth.authz.authorize``
          etc.) — becomes the OTel span's display name.
        histogram: Duration histogram to record into; currently
          always :data:`OPERATION_DURATION_MS`.
        labels: Constant labels for both the success and error sample
          paths (e.g. ``{"operation": "authz.authorize"}``).
        outcomes: Per-call-site mapping from exception type to the
          ``outcome`` label the histogram understands. Anything not
          present in the mapping falls back to ``internal_error``.
    """
    start = time.perf_counter()
    with _tracer.start_as_current_span(span_name) as span:
        try:
            yield span
        except Exception as exc:
            # Catch ``Exception`` (not ``BaseException``) so process-
            # control unwinds — ``KeyboardInterrupt``, ``SystemExit``,
            # ``GeneratorExit`` — propagate untouched and are never
            # recorded into histograms or the error counter. Those
            # events are not application errors; mislabelling them as
            # such would skew SLO dashboards.
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            outcome = _outcome_for(exc, outcomes)
            histogram.record(elapsed_ms, {**labels, "outcome": outcome})
            kind = _error_kind(exc)
            if kind is not None:
                ERRORS_TOTAL.add(1, {"kind": kind})
            span.set_status(Status(StatusCode.ERROR, type(exc).__name__))
            span.record_exception(exc)
            raise
        else:
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            histogram.record(elapsed_ms, {**labels, "outcome": _SUCCESS})


def observe_operation(
    operation: str,
    outcomes: Mapping[type[BaseException], str] | None = None,
) -> AbstractContextManager[Span]:
    """Context manager wrapping a public REST handler or RPC method.

    Produces span ``custos_auth.<operation>`` and records into
    :data:`OPERATION_DURATION_MS` labelled by ``operation`` + outcome.
    """
    return instrument(
        f"custos_auth.{operation}",
        OPERATION_DURATION_MS,
        {"operation": operation},
        outcomes or {},
    )


def record_error_kind(kind: str) -> None:
    """Bump :data:`ERRORS_TOTAL` for an out-of-band error path.

    Used by middleware / exception handlers which catch failures
    *outside* a surrounding :func:`observe_operation` block — for
    example, the call-context middleware short-circuits the request
    before any route handler runs, so its rejection paths feed the
    counter through this helper rather than through ``instrument``.
    """
    ERRORS_TOTAL.add(1, {"kind": kind})


__all__ = [
    "ERRORS_TOTAL",
    "OPERATION_DURATION_MS",
    "OP_AUTHZ_VERIFY_AND_AUTHORIZE",
    "OP_AUTH_VERIFY",
    "OP_JWKS_GET",
    "OP_OIDC_CALLBACK",
    "OP_PERMISSION_LIST",
    "OP_PRINCIPAL_DISABLE",
    "OP_PRINCIPAL_GET_ME",
    "OP_ROLE_BINDING_GRANT",
    "OP_ROLE_BINDING_REVOKE",
    "OP_ROLE_LIST",
    "OP_RPC_AUTHN_VERIFY_TOKEN",
    "OP_RPC_AUTHZ_AUTHORIZE",
    "OP_RPC_AUTHZ_VERIFY_AND_AUTHORIZE",
    "OP_RPC_CALLCTX_SIGN",
    "OP_RPC_CALLCTX_VERIFY",
    "OP_SERVICE_ACCOUNT_CREATE",
    "OP_SERVICE_TOKEN_ISSUE",
    "OP_SERVICE_TOKEN_LIST",
    "OP_SERVICE_TOKEN_REVOKE",
    "OP_SERVICE_TOKEN_REVOKE_ALL",
    "OP_TENANT_CREATE",
    "OP_TENANT_LIST",
    "OP_WORKSPACE_CREATE",
    "OP_WORKSPACE_GET",
    "OP_WORKSPACE_LIST",
    "instrument",
    "observe_operation",
    "record_error_kind",
]
