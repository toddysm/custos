"""FastAPI dependency factories for the Workflow Service API.

The factories pull pre-built collaborators off ``app.state`` (the
:class:`~custos_workflow.providers.RunComponents` bundle the FastAPI
lifespan installs in :mod:`custos_workflow.app`) and expose them as
``Depends``-compatible callables that the REST routers
(``WF-IMPL-065`` / ``-066``) and Internal RPC routers
(``WF-IMPL-067`` / ``-068``) consume uniformly.

Three accessor families ship here:

* :func:`get_run_controller` — returns the singleton
  :class:`~custos_workflow.runs.controller.RunController` the
  lifespan wired onto ``app.state.run_components`` (per the
  :class:`~custos_workflow.providers.RunComponents` contract). The
  controller is reused across requests; per-request state lives in
  the request-bound :class:`~custos_workflow.call_context.CallContext`.

* :func:`get_validator` — returns the singleton
  :class:`~custos_workflow.validator.StartRunValidator` the lifespan
  installs on ``app.state.start_run_validator``. The validator is
  stateless apart from its bound
  :class:`~custos_workflow.validator.IdempotencyLedger`, so a single
  process-local instance is correct.

* :func:`get_call_context` — returns the
  :class:`~custos_workflow.call_context.CallContext` the middleware
  attached to ``request.state``. The middleware short-circuits with
  a 401 envelope before this dependency is reached in production
  mode, so reaching the body means the context is guaranteed to be
  populated.

* :func:`workspace_path` — a ``Path(...)`` parameter dependency that
  pulls the ``{ws}`` URL segment with a length floor. Routers
  declare ``ws: str = Depends(workspace_path)`` so the wire surface
  stays uniform across routes/ and rpc.py.

Missing-state failures from every state-reading accessor
(:func:`get_run_components`, :func:`get_run_controller` transitively,
:func:`get_validator`, and :func:`get_call_context`) raise
:class:`~custos_workflow.runs.errors.WorkflowRuntimeUnavailableError`,
which the WF-IMPL-061 exception handler chain renders as a 503
:class:`~custos_workflow.api.errors.ProblemDetail` envelope with the
``workflow.workflow_runtime_unavailable`` kind. That is the locked
public-API kind that means *the workflow runtime is not currently
serving requests*; reading it on a lifespan-startup failure or a
missing middleware install is semantically right and lines up with
the kind→status table in :mod:`custos_workflow.api.errors`.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Final

from fastapi import Depends, Path, Request

from custos_workflow.runs.errors import WorkflowRuntimeUnavailableError

if TYPE_CHECKING:
    from custos_workflow.call_context import CallContext
    from custos_workflow.providers import RunComponents
    from custos_workflow.runs.controller import RunController
    from custos_workflow.validator import StartRunValidator


__all__ = [
    "WORKSPACE_ID_PATTERN",
    "get_call_context",
    "get_run_components",
    "get_run_controller",
    "get_validator",
    "workspace_path",
]


#: Canonical workspace identifier grammar. Byte-equal to
#: :data:`custos_workflow.document.models._NAME_PATTERN` — the
#: DNS-1123-like grammar the Catalog already validates against when
#: a workspace is published. Mirroring the regex here lets FastAPI
#: reject malformed ``{ws}`` segments at the routing layer (via
#: ``Path(pattern=...)``) instead of letting them reach the
#: validator + controller as bogus workspace identifiers. The
#: declaration is intentionally a module-level constant so the
#: regex can be reused by future routers and matched byte-equal in
#: tests if the document grammar ever drifts.
WORKSPACE_ID_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[a-z][a-z0-9-]{0,62}$")


# ---------------------------------------------------------------------------
# State accessors
# ---------------------------------------------------------------------------


def get_run_components(request: Request) -> RunComponents:
    """Return the :class:`RunComponents` bundle held on ``app.state``.

    The lifespan hook in :func:`custos_workflow.create_app` populates
    ``app.state.run_components`` before any request is dispatched.
    Missing state means the lifespan failed to wire the bundle and
    the service is not actually ready to serve work — raise the
    locked 503 kind so the SDK sees a stable envelope rather than
    the FastAPI default 500.

    Args:
        request: The incoming FastAPI request; the application
            instance is reached through ``request.app``.

    Returns:
        The pre-built collaborator bundle.

    Raises:
        WorkflowRuntimeUnavailableError: The bundle is missing.
            Mapped to a 503
            ``workflow.workflow_runtime_unavailable``
            :class:`~custos_workflow.api.errors.ProblemDetail` by
            the WF-IMPL-061 handler chain.
    """
    # Local import keeps the module import-time cycle-free: providers
    # imports validators / runtime adapters transitively, but the
    # dependency module is also imported from the FastAPI app
    # bootstrap path.
    from custos_workflow.providers import RunComponents

    components = getattr(request.app.state, "run_components", None)
    if components is None:
        raise WorkflowRuntimeUnavailableError(
            "workflow-service is not ready: run_components missing on app.state "
            "(the FastAPI lifespan has not yet populated the dependency bundle)."
        )
    if not isinstance(components, RunComponents):
        raise WorkflowRuntimeUnavailableError(
            "workflow-service is not ready: app.state.run_components is bound "
            f"to {type(components).__name__!r}, expected RunComponents."
        )
    return components


def get_run_controller(
    components: RunComponents = Depends(get_run_components),
) -> RunController:
    """Return the singleton :class:`RunController` bound by the lifespan.

    The controller is stateless across requests (per-request state
    flows through the :class:`CallContext`), so the lifespan-built
    singleton is reused for the lifetime of the process.

    Args:
        components: The :class:`RunComponents` bundle, resolved via
            :func:`get_run_components`. Tests can override the
            transitive :func:`get_run_components` dependency in
            FastAPI to inject a controller.

    Returns:
        The pre-built :class:`RunController`.
    """
    return components.run_controller


def get_validator(request: Request) -> StartRunValidator:
    """Return the singleton :class:`StartRunValidator` from ``app.state``.

    The validator is intentionally not held on
    :class:`RunComponents`: it is a thin façade around the catalog
    client plus the idempotency ledger, both of which the WF-IMPL-069
    / -070 lifespan tasks will wire onto ``app.state`` directly. By
    keeping the accessor independent we let tests inject a fake
    validator without rebuilding the entire ``RunComponents`` bundle.

    Args:
        request: The incoming FastAPI request; the application
            instance is reached through ``request.app``.

    Returns:
        The pre-built :class:`StartRunValidator`.

    Raises:
        WorkflowRuntimeUnavailableError: The validator is missing
            from ``app.state``. Mapped to a 503 envelope by the
            WF-IMPL-061 handler chain.
    """
    # Local import keeps the module import-time cycle-free.
    from custos_workflow.validator import StartRunValidator

    validator = getattr(request.app.state, "start_run_validator", None)
    if validator is None:
        raise WorkflowRuntimeUnavailableError(
            "workflow-service is not ready: start_run_validator missing on "
            "app.state (the FastAPI lifespan has not yet wired the validator)."
        )
    if not isinstance(validator, StartRunValidator):
        raise WorkflowRuntimeUnavailableError(
            "workflow-service is not ready: app.state.start_run_validator is "
            f"bound to {type(validator).__name__!r}, expected StartRunValidator."
        )
    return validator


def get_call_context(request: Request) -> CallContext:
    """Return the :class:`CallContext` :mod:`call_context` middleware attached.

    The middleware short-circuits requests in production mode
    (``WF_REQUIRE_CALL_CONTEXT=1``) with a 401 envelope before any
    route runs, so reaching this accessor means the context is
    guaranteed to exist. In dev mode the middleware still attaches
    a :class:`CallContext` with ``None`` fields so the downstream
    route does not have to special-case absence.

    Args:
        request: The incoming FastAPI request; the call context is
            held on ``request.state``.

    Returns:
        The per-request call context.

    Raises:
        WorkflowRuntimeUnavailableError: The middleware did not run
            (the application was built without
            :class:`~custos_workflow.call_context.CallContextMiddleware`).
            This is a misconfiguration; the 503 envelope keeps the
            SDK contract uniform.
    """
    # Local import keeps the module import-time cycle-free.
    from custos_workflow.call_context import CallContext

    ctx = getattr(request.state, "call_context", None)
    if ctx is None:
        raise WorkflowRuntimeUnavailableError(
            "workflow-service is not ready: call_context missing on "
            "request.state (CallContextMiddleware did not run)."
        )
    if not isinstance(ctx, CallContext):
        raise WorkflowRuntimeUnavailableError(
            "workflow-service is not ready: request.state.call_context is "
            f"bound to {type(ctx).__name__!r}, expected CallContext."
        )
    return ctx


# ---------------------------------------------------------------------------
# Path parameter dependency
# ---------------------------------------------------------------------------


def workspace_path(
    ws: str = Path(
        ...,
        description=(
            "Workspace id from the URL path. Echoed verbatim into the "
            "downstream RunController + StartRunValidator calls so the "
            "request workspace is the same identifier audit + idempotency "
            "ledger use. Validated against the canonical DNS-1123-like "
            "workspace grammar the Catalog publishes."
        ),
        min_length=1,
        max_length=63,
        pattern=WORKSPACE_ID_PATTERN.pattern,
    ),
) -> str:
    """Pull the ``{ws}`` URL segment as the workspace identifier.

    Routers declare ``ws: str = Depends(workspace_path)`` so the
    wire surface is uniform across the REST routes (``WF-IMPL-065``
    / ``-066``) and Internal RPC routes (``WF-IMPL-067`` / ``-068``).
    Centralising the parameter here lets later tasks tighten the
    constraint in one place without touching every route.

    FastAPI enforces :data:`WORKSPACE_ID_PATTERN`,
    ``min_length=1``, and ``max_length=63`` at the routing layer
    — a malformed ``{ws}`` segment fails the ``RequestValidationError``
    handler and surfaces as a 400
    :class:`~custos_workflow.api.errors.ProblemDetail` with the
    ``workflow.api.bad_request`` kind. The function-body re-check
    catches the same grammar drift for callers that invoke the
    dependency directly (unit tests, in-process composition), where
    FastAPI's ``Path`` defaults do not run; that path raises
    :class:`ValueError` so misuse is obvious in the test failure.

    Args:
        ws: The path segment value FastAPI injected.

    Returns:
        The workspace identifier string.

    Raises:
        ValueError: ``ws`` does not match
            :data:`WORKSPACE_ID_PATTERN`. Only reachable on direct
            calls; FastAPI rejects the same shape at the routing
            layer before this body runs.
    """
    if not WORKSPACE_ID_PATTERN.fullmatch(ws):
        raise ValueError(
            f"workspace id {ws!r} does not match the canonical workspace "
            f"grammar {WORKSPACE_ID_PATTERN.pattern!r}."
        )
    return ws
