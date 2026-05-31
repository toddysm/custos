"""Public + internal wire Pydantic models for the workflow-service API.

WF-IMPL-062 — these models pin every request / response shape
documented in ``design/components/workflow-service/design.md``
§ Public Interface. The Python attribute names are
``snake_case`` (per the project's PEP 8 convention); the wire
representation is ``camelCase`` (per ``design.md`` § Public
Interface and the existing API conventions across
``custos`` services). The translation is handled centrally by
:class:`_CamelModel` via Pydantic's
:func:`pydantic.alias_generators.to_camel` so individual model
definitions stay one-attribute-per-line and free of repeated
``Field(alias=...)`` boilerplate.

Every model in this module:

* Forbids unknown fields (``extra="forbid"``). This is a *public
  API* contract surface — a stray ``runID`` (capital-D) or a
  reserved-for-future-use field must surface as a validation
  error today rather than silently round-trip and bake in a
  contract bug.
* Allows population by either the snake_case Python name *or*
  the wire camelCase alias (``populate_by_name=True``) so the
  REST routes (WF-IMPL-065 onwards) and the internal Python
  test fixtures can both construct instances naturally.
* Re-uses the locked :class:`~custos_workflow.runs.model.RunStatus`
  enum so status strings stay in lockstep with the Run
  Controller's authoritative lifecycle taxonomy.

The actual Run Controller hand-off (``StartRunValidator`` ->
``RunController.start_run``) lands with WF-IMPL-063 / -065; this
module is intentionally I/O-free so the models can be imported
from the validator package without dragging in FastAPI or any
runtime client.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Final

from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel

from custos_workflow.runs.model import RunStatus

__all__ = [
    "MAX_LIST_LIMIT",
    "CancelRunRequest",
    "InternalCancelRunRequest",
    "InternalStartRunRequest",
    "PageRefResponse",
    "RaiseExternalEventRequest",
    "RunListQuery",
    "RunListResponse",
    "RunRefResponse",
    "RunResponse",
    "StartRunRequest",
    "StartRunResponse",
    "StepAttemptSummary",
    "StepResponse",
]


# ---------------------------------------------------------------------------
# Base
# ---------------------------------------------------------------------------


#: Pagination upper bound enforced on every list endpoint. Mirrors
#: the value the Run Controller's :meth:`list_runs` defers to the
#: provider for; pinning it here keeps the public REST contract
#: stable even if the provider default shifts under the hood.
MAX_LIST_LIMIT: Final[int] = 200


class _CamelModel(BaseModel):
    """Shared Pydantic base for every wire model in this module.

    Centralises:

    * the ``snake_case`` <-> ``camelCase`` alias generator,
    * ``populate_by_name=True`` so callers can use either spelling,
    * ``extra="forbid"`` so a typo on the wire is a 400 rather
      than a silent contract drift, and
    * ``str_strip_whitespace=True`` so leading/trailing whitespace
      in string fields can never break downstream identity
      comparisons (workspace ids, idempotency keys, …).
    """

    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        extra="forbid",
        str_strip_whitespace=True,
    )


# ---------------------------------------------------------------------------
# StartRun
# ---------------------------------------------------------------------------


class StartRunRequest(_CamelModel):
    """Request body for ``POST /v1/workspaces/{ws}/runs``.

    The ``idempotencyKey`` body field is optional and falls back
    to the ``Idempotency-Key`` header at the route layer
    (WF-IMPL-065). Both spellings are passed through unchanged
    to :class:`~custos_workflow.validator.StartRunValidator`
    (WF-IMPL-063) for ledger lookup; the validator is the single
    arbiter of which one wins.

    ``inputs`` is intentionally typed as ``dict[str, Any]`` rather
    than a schema-specific model — the per-workflow inputs schema
    is published by Catalog and validated at runtime by the
    Validator (WF-IMPL-063 ``validate_inputs_against_schema``),
    not at the wire boundary. This keeps the API a thin envelope
    and concentrates the schema contract in one place.
    """

    workflow_version_id: str = Field(
        min_length=1,
        description="The Catalog `WorkflowVersion.id` to instantiate.",
    )
    inputs: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Caller-supplied inputs; validated against the workflow's published JSON-Schema."
        ),
    )
    idempotency_key: str | None = Field(
        default=None,
        description=(
            "Optional client-supplied dedup key for the "
            "(workspaceId, idempotencyKey) ledger. Falls back to "
            "the Idempotency-Key header per RFC."
        ),
    )


class RunRefResponse(_CamelModel):
    """Lightweight handle for a run; the ``202 Accepted`` body of ``StartRun``.

    Also used as the per-item shape inside :class:`RunListResponse`
    so that list reads stay cheap (no step timeline, no inputs /
    outputs payloads). The ``startedAt`` field is optional because
    the Run Controller may return a freshly-queued run before the
    runtime has stamped a start time; once the run transitions to
    ``running`` the field is guaranteed to be present in any
    subsequent read.
    """

    run_id: str = Field(min_length=1, description="The opaque run identifier.")
    status: RunStatus = Field(description="Current run lifecycle status.")
    workspace_id: str = Field(min_length=1, description="Owning workspace.")
    workflow_version_id: str = Field(
        min_length=1,
        description="The Catalog `WorkflowVersion.id` this run instantiates.",
    )
    started_at: datetime | None = Field(
        default=None,
        description=(
            "Wall-clock instant the run was first persisted; None for "
            "fresh QUEUED rows in some adapters."
        ),
    )


#: Alias kept for spec parity — the design + implementation plan
#: reference ``StartRunResponse`` and ``RunRefResponse`` as the
#: same wire shape (the 202 body for StartRun is just the
#: caller-facing handle). Exporting both names lets future tasks
#: name-import the one that reads best at the call site without
#: a second-definition divergence risk.
StartRunResponse = RunRefResponse


# ---------------------------------------------------------------------------
# Step timeline (used by RunResponse)
# ---------------------------------------------------------------------------


class StepAttemptSummary(_CamelModel):
    """One row of a step's ``attempts[]`` history.

    Mirrors the persisted ``StepAttempt`` projection: each retry
    of an activity / sub-workflow step appends a new attempt. The
    ``error`` field is populated for terminal-failed attempts
    (and on every non-final attempt of a step that ultimately
    succeeded, so operators can trace flapping behaviour); it is
    a free-form string today and will tighten to a structured
    envelope once WF-IMPL-070's observability wiring lands.
    """

    attempt: int = Field(ge=1, description="1-based attempt index; monotonic per (runId, stepId).")
    status: str = Field(
        description="Attempt-level status (`started`, `succeeded`, `failed`, `cancelled`, …)."
    )
    started_at: datetime | None = Field(
        default=None, description="When the attempt began executing."
    )
    finished_at: datetime | None = Field(
        default=None, description="When the attempt reached a terminal state."
    )
    error: str | None = Field(
        default=None,
        description="Human-readable error string for failed / retried attempts.",
    )


class StepResponse(_CamelModel):
    """Per-step state for the ``Run.steps[]`` timeline.

    ``kind`` is a string (``activity`` / ``waitFor`` / ``for`` /
    ``approval`` / ``workflow`` / ``let``) rather than an enum so
    additions to the step-kind taxonomy in
    :mod:`custos_workflow.document.models` do not force a
    breaking change to this wire model.

    The ``outputs`` field is intentionally optional + only
    populated for steps in a terminal-success state; pending /
    failed steps return ``None`` so callers can branch on
    presence rather than parsing an empty dict.
    """

    step_id: str = Field(min_length=1, description="The compiled step's stable identifier.")
    kind: str = Field(min_length=1, description="Step kind (e.g. `activity`, `waitFor`).")
    status: str = Field(
        min_length=1,
        description="Aggregate step status (`pending`, `running`, `succeeded`, `failed`, …).",
    )
    attempts: list[StepAttemptSummary] = Field(
        default_factory=list,
        description="Per-attempt history; empty for steps that have not started yet.",
    )
    started_at: datetime | None = Field(default=None, description="First-attempt start time.")
    finished_at: datetime | None = Field(
        default=None, description="Last-attempt finish time for terminal steps."
    )
    outputs: dict[str, Any] | None = Field(
        default=None,
        description="Step outputs for terminal-success steps; None otherwise.",
    )


# ---------------------------------------------------------------------------
# Full Run read
# ---------------------------------------------------------------------------


class RunResponse(_CamelModel):
    """Response body for ``GET /v1/workspaces/{ws}/runs/{runId}``.

    Carries the full run record plus the step timeline so a single
    read serves the typical UI / SDK workflow-detail screen
    without an N+1 follow-up. The ``inputs`` field always echoes
    the originally-supplied request inputs (post-validation,
    pre-schema-coercion); ``outputs`` is populated only when the
    run is in a terminal-success state.

    Mirrors :class:`~custos_workflow.runs.model.RunRecord` plus
    the step timeline projection. ``reason`` is populated for
    explicitly-cancelled or failed runs (the caller-supplied
    cancellation reason or the failure summary) and is ``None``
    on the happy path.
    """

    run_id: str = Field(min_length=1)
    status: RunStatus
    workspace_id: str = Field(min_length=1)
    workflow_version_id: str = Field(min_length=1)
    reason: str | None = Field(default=None)
    started_at: datetime
    updated_at: datetime
    inputs: dict[str, Any] = Field(default_factory=dict)
    outputs: dict[str, Any] | None = Field(default=None)
    steps: list[StepResponse] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Cancel + RaiseExternalEvent
# ---------------------------------------------------------------------------


class CancelRunRequest(_CamelModel):
    """Request body for ``POST /v1/workspaces/{ws}/runs/{runId}:cancel``.

    ``reason`` is plumbed straight through to
    :meth:`~custos_workflow.runs.controller.RunController.cancel_run`
    and surfaces on every subsequent read of the run (see
    :attr:`RunResponse.reason`).
    """

    reason: str | None = Field(
        default=None,
        max_length=1024,
        description="Operator- or system-supplied cancellation explanation.",
    )


# ---------------------------------------------------------------------------
# Internal RPC bodies (WF-IMPL-067)
# ---------------------------------------------------------------------------


class InternalStartRunRequest(StartRunRequest):
    """Request body for ``POST /internal/runs:start``.

    The Internal RPC surface ships under a flat ``/internal/`` prefix
    (no ``/v1/workspaces/{ws}/`` segment) so the Helm chart / mesh
    can pin mTLS-only access to a single path stem (see
    ``design/components/workflow-service/design.md`` § Internal RPC).
    Because the workspace is no longer carried in the URL it has to
    travel in the body — internal callers like the Trigger Service
    typically issue ``StartRun`` on behalf of a workflow run whose
    workspace is determined by their own ledger, not by the inbound
    request, so promoting ``workspaceId`` to a top-level body field
    keeps the contract explicit at the wire.

    All other fields are inherited unchanged from
    :class:`StartRunRequest` so the public + internal surfaces stay
    in lockstep — anything the public POST accepts the internal
    RPC accepts too.
    """

    workspace_id: str = Field(
        min_length=1,
        description=(
            "Owning workspace for the run. Required on the Internal "
            "RPC surface (the path carries no `{ws}` segment)."
        ),
    )


class InternalCancelRunRequest(CancelRunRequest):
    """Request body for ``POST /internal/runs/{runId}:cancel``.

    Mirrors :class:`InternalStartRunRequest`: the Internal RPC
    surface has no ``{ws}`` path segment so the workspace travels
    in the body. ``reason`` is inherited unchanged from
    :class:`CancelRunRequest`.
    """

    workspace_id: str = Field(
        min_length=1,
        description=(
            "Owning workspace for the run. Required on the Internal "
            "RPC surface (the path carries no `{ws}` segment)."
        ),
    )


class RaiseExternalEventRequest(_CamelModel):
    """Internal-RPC body for the Trigger Service `RaiseExternalEvent` bridge.

    Mirrors the design.md § Internal RPC table:
    ``RaiseExternalEvent(runId, stepId, eventName, payload, idempotencyKey)``.
    ``runId`` and ``stepId`` are path-bound (not body-bound) so the
    body carries only the event payload + dedup key. Idempotency
    on this surface is keyed by
    ``(runId, stepId, eventName, idempotencyKey)`` per design.md
    — the Validator (WF-IMPL-063) owns the ledger lookup; this
    model is a plain envelope.
    """

    event_name: str = Field(
        min_length=1,
        description="Wire-stable event name the workflow's `waitFor:` step subscribed to.",
    )
    payload: dict[str, Any] = Field(
        default_factory=dict,
        description="Event payload delivered into the workflow's `raise_event` primitive.",
    )
    idempotency_key: str | None = Field(
        default=None,
        description=(
            "Optional client-supplied dedup key for the "
            "(runId, stepId, eventName, idempotencyKey) ledger."
        ),
    )


# ---------------------------------------------------------------------------
# List endpoint
# ---------------------------------------------------------------------------


class RunListQuery(_CamelModel):
    """Query parameter envelope for ``GET /v1/workspaces/{ws}/runs``.

    Lives as its own model (rather than a flat function signature
    on the route) so the validation rules — in particular the
    ``limit`` upper bound — stay co-located with the wire
    contract and round-trip through unit tests without a
    TestClient.

    All four fields are optional; ``cursor`` is the opaque token
    returned by a previous page's :attr:`RunListResponse.next_cursor`
    and is treated as a black box by callers.
    """

    status: RunStatus | None = Field(
        default=None, description="Restrict to runs currently in this lifecycle status."
    )
    workflow_version_id: str | None = Field(
        default=None,
        min_length=1,
        description="Restrict to runs of this `WorkflowVersion.id`.",
    )
    cursor: str | None = Field(
        default=None,
        min_length=1,
        description="Opaque pagination token from a previous page's `nextCursor`.",
    )
    limit: int | None = Field(
        default=None,
        ge=1,
        le=MAX_LIST_LIMIT,
        description=f"Max items per page; capped at {MAX_LIST_LIMIT}.",
    )


class RunListResponse(_CamelModel):
    """Paginated response body for ``GET /v1/workspaces/{ws}/runs``.

    The ``items`` list carries :class:`RunRefResponse` rows (not
    full :class:`RunResponse` records) so list reads stay cheap.
    ``nextCursor`` is ``None`` when this is the final page; an
    empty ``items`` list with a non-``None`` ``nextCursor`` is a
    legal "keep paging, nothing in this window" response.
    """

    items: list[RunRefResponse] = Field(default_factory=list)
    next_cursor: str | None = Field(
        default=None,
        min_length=1,
        description="Opaque token to pass as `cursor` on the next request; None on the final page.",
    )


# ---------------------------------------------------------------------------
# Internal helpers — exported for downstream tasks that need to
# project Run Controller dataclasses onto the wire models without
# duplicating the field list.
# ---------------------------------------------------------------------------


class PageRefResponse(_CamelModel):
    """Generic ``{items, nextCursor}`` envelope.

    Today :class:`RunListResponse` is the only consumer of this
    shape, but the design plan calls out follow-on list endpoints
    (steps, audit events) that will reuse the same wire envelope.
    Defining the shape once here avoids per-endpoint drift on the
    ``nextCursor`` spelling.
    """

    items: list[dict[str, Any]] = Field(default_factory=list)
    next_cursor: str | None = Field(default=None, min_length=1)
