"""OpenTelemetry HTTP-server middleware for the Workflow API surface (WF-IMPL-070).

Implements the inbound side of the
:mod:`custos_workflow._telemetry` instrument set for every public
REST + Internal RPC route mounted by :func:`create_app`:

* One ``custos_workflow.http.request`` span per request, with the
  standard OTel HTTP-server semconv attributes (``http.method``,
  ``http.route``, ``http.status_code``) plus the workflow-service
  ``wf.*`` attributes documented in
  :mod:`custos_workflow._telemetry`.
* One ``custos_workflow_http_server_duration_ms`` sample per
  request, labelled by ``http.route`` (the FastAPI template path,
  **not** the live URL — keeps cardinality bounded by the closed
  route set), ``http.method``, and ``http.status_code``.

The middleware is intentionally tiny: it owns the span lifecycle
and the per-request label set, but leaves
``custos_workflow_api_errors_total`` to the exception handlers in
:mod:`custos_workflow.api.errors` (each handler already knows the
locked ``kind`` it's emitting) and
``custos_workflow_idempotency_outcomes_total`` to the
:class:`~custos_workflow.validator.StartRunValidator` (it owns the
``fresh`` / ``replay`` / ``conflict`` decision). Splitting the
recording call sites this way keeps each counter close to the
authoritative source — adding a new validator outcome or API kind
in the future does not require touching middleware code.

Per-request attribute resolution
--------------------------------

``wf.workspace.id`` and ``wf.run.id`` are read from
``request.path_params`` after FastAPI has matched the route (the
``ws`` and ``run_id`` template tokens). ``wf.workflow_version.id``
is read from ``request.state`` (the ``StartRun`` route stashes it
after validation succeeds — only that route knows the validated
id without re-parsing the body). ``wf.idempotency.outcome`` and
``wf.error.kind`` follow the same ``request.state`` side-channel
pattern.

This wiring is documented end-to-end in
:mod:`custos_workflow._telemetry` (the WF-IMPL-070 section near
the bottom of the module) so future maintainers can find the
contract from either side.
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from typing import Final

from fastapi import FastAPI, Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

from custos_workflow._telemetry import observe_http_request, record_http_server_duration

__all__ = ["OTelHttpServerMiddleware", "register_http_observability"]


#: Path-parameter names the middleware looks for on every matched
#: route. The mapping value is the span attribute name.
_PATH_PARAM_TO_ATTR: Final[dict[str, str]] = {
    "ws": "wf.workspace.id",
    "run_id": "wf.run.id",
    "step_id": "wf.step.id",
}

#: Request-state attribute names the middleware mirrors onto the
#: span after ``call_next`` returns. The mapping value is the span
#: attribute name. The route handler (or an exception handler)
#: sets ``request.state.<attr>`` when it knows the value.
_STATE_ATTR_TO_SPAN: Final[dict[str, str]] = {
    "wf_workflow_version_id": "wf.workflow_version.id",
    "wf_run_id": "wf.run.id",
    "wf_idempotency_outcome": "wf.idempotency.outcome",
    "wf_error_kind": "wf.error.kind",
}


def _resolve_route_template(request: Request) -> str:
    """Return the FastAPI template path for the matched route.

    Falls back to the live URL path when no route matched (404,
    422 before routing, etc.) so the histogram label is still
    bounded — Starlette's 404 path is well-defined and matches at
    most one bucket per unknown URL prefix, which is a deliberate
    trade-off (unknown URLs flood the meter via cardinality
    rather than silently dropping the sample).
    """
    route = request.scope.get("route")
    # Starlette ``Route`` exposes ``path``; ``Mount`` exposes
    # ``path`` too. Use ``getattr`` so a None route (no match)
    # falls back cleanly.
    template = getattr(route, "path", None)
    if isinstance(template, str) and template:
        return template
    return request.url.path


class OTelHttpServerMiddleware(BaseHTTPMiddleware):
    """ASGI middleware that wraps each request in a span + duration sample.

    Installed by :func:`register_http_observability` (which
    :func:`~custos_workflow.app.create_app` calls during app
    construction). Sits outside the router so it sees every
    inbound request, including ones that fail routing entirely
    (Starlette emits a 404 envelope, the middleware still records
    the span and histogram with ``http.status_code=404``).

    The middleware never swallows exceptions: the WF-IMPL-061
    handler chain is responsible for translating route exceptions
    into Problem+JSON responses, and the middleware's job is to
    keep the span + sample shape uniform regardless of which side
    produced the final response.
    """

    def __init__(self, app: ASGIApp) -> None:
        # BaseHTTPMiddleware's __init__ takes only ``app`` and an
        # optional ``dispatch`` override; we override ``dispatch``
        # via the method below instead. Keeping the constructor
        # trivial means ``app.add_middleware(OTelHttpServerMiddleware)``
        # in :func:`create_app` works without extra arguments.
        super().__init__(app)

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        """Run the request inside a span + duration sample.

        Span attributes:

        * ``http.method`` / ``http.route`` — set by
          :func:`observe_http_request` from the args.
        * ``http.status_code`` — set on the way out from the
          response.
        * ``wf.workspace.id`` / ``wf.run.id`` / ``wf.step.id`` —
          set from ``request.path_params`` after routing.
        * ``wf.workflow_version.id`` / ``wf.run.id`` (override) /
          ``wf.idempotency.outcome`` / ``wf.error.kind`` — set
          from ``request.state`` after ``call_next`` returns
          (route handlers populate these for the StartRun path).

        The histogram sample is recorded on every exit (success
        or exception) via :func:`record_http_server_duration` so
        the total stays consistent with the request count. On a
        raised exception we fall back to ``http.status_code=500``
        — Starlette's outer error middleware will produce a 500
        response anyway, so the label matches what the client
        actually sees on the wire.
        """
        # ``_resolve_route_template`` returns the URL path before
        # ``call_next`` runs because Starlette only populates
        # ``request.scope["route"]`` during route matching, which
        # happens inside ``call_next``. We start the span with a
        # provisional route value and re-resolve below — keeping
        # the histogram label aligned with the actual matched
        # route template (or the live URL for genuine 404s).
        route = request.url.path
        method = request.method
        start = time.perf_counter()
        status_code = 500
        try:
            with observe_http_request(method, route) as span:
                response = await call_next(request)
                status_code = response.status_code
                route = _resolve_route_template(request)
                span.set_attribute("http.route", route)

                # Path-param attributes — best-effort; reading
                # ``request.path_params`` after ``call_next``
                # returns sees the post-match values set by the
                # router.
                for param, attr in _PATH_PARAM_TO_ATTR.items():
                    value = request.path_params.get(param)
                    if isinstance(value, str) and value:
                        span.set_attribute(attr, value)

                # Side-channel attributes set by the route or by
                # an exception handler. ``request.state`` is a
                # plain namespace; ``getattr`` with a sentinel
                # keeps the contract opt-in (routes that don't
                # stash a value produce no attribute).
                for state_attr, span_attr in _STATE_ATTR_TO_SPAN.items():
                    value = getattr(request.state, state_attr, None)
                    if isinstance(value, str) and value:
                        span.set_attribute(span_attr, value)

                span.set_attribute("http.status_code", status_code)
                return response
        finally:
            duration_ms = (time.perf_counter() - start) * 1000.0
            record_http_server_duration(
                method=method,
                route=route,
                status_code=status_code,
                duration_ms=duration_ms,
            )


def register_http_observability(app: FastAPI) -> None:
    """Install :class:`OTelHttpServerMiddleware` on ``app``.

    Idempotent: calling twice on the same app instance is a no-op
    (the second call sees the marker attribute set by the first).
    Mirrors the
    :func:`~custos_workflow.api.errors.register_exception_handlers`
    contract so the WF-IMPL-069 ``create_app`` body can wire
    both helpers without worrying about double-registration in
    the test fixtures (which often call ``create_app`` more than
    once against a single app object).
    """
    if getattr(app, "_custos_workflow_http_observability_registered", False):
        return
    app.add_middleware(OTelHttpServerMiddleware)
    app._custos_workflow_http_observability_registered = True  # type: ignore[attr-defined]
