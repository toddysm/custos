"""REST routes for the ``Run`` resource (WF-IMPL-065, #451).

Per ``design.md`` § Public Interface, the public Run surface is
four routes:

* ``POST   /v1/workspaces/{ws}/runs``                — start a run.
* ``GET    /v1/workspaces/{ws}/runs``                — list runs (paged + filters).
* ``GET    /v1/workspaces/{ws}/runs/{runId}``        — fetch a single run + timeline.
* ``POST   /v1/workspaces/{ws}/runs/{runId}:cancel`` — request cancellation.

Each route is a thin orchestration: lift the path workspace through
:func:`~custos_workflow.api.dependencies.workspace_path`, resolve
collaborators off ``app.state`` via the WF-IMPL-064 dependency
factories, then call the
:class:`~custos_workflow.validator.StartRunValidator` (where the
verb mutates) and the
:class:`~custos_workflow.runs.controller.RunController` in sequence
and translate the result into a wire model.

The error taxonomy is intentionally untouched here: every locked
error class (run + validator) already has an exception handler
registered by
:func:`~custos_workflow.api.errors.register_exception_handlers`
(WF-IMPL-061), so the routes can simply propagate.

Idempotency
-----------

``POST /v1/workspaces/{ws}/runs`` reads the optional
``Idempotency-Key`` HTTP header per RFC and falls it through to
the validator. When the request body carries a non-empty
``idempotencyKey`` field, the body value wins (mirrors the order
the issue acceptance criteria pin: "body-field idempotencyKey
overrides header"). An empty-string body field is treated as
"not supplied" so callers can opt out by sending ``""``.

The validator's :class:`~custos_workflow.validator.ValidatedStartRun`
result carries the normalised key + inputs + fingerprint; we pass
the normalised values straight into
:meth:`~custos_workflow.runs.controller.RunController.start_run`
so the controller observes the same fingerprint the validator
recorded in the ledger. The controller's own dedup gate is the
second line of defence — the API surface trusts the validator's
output but the controller still owns its contract for non-API
callers (the Internal RPC surface that lands in WF-IMPL-067).

List filtering
--------------

The current :meth:`RunController.list_runs` does not surface
``status`` or ``workflow_version_id`` filters: the underlying
:class:`~custos_spl.interfaces.metadata_store.RunFilter` plumbing
lands with a follow-on task (see the controller's ``list_runs``
docstring). To keep the wire surface honest today we apply the
filters in-memory over the fetched page. The trade-off is that a
page with many filtered-out rows may return fewer items than
``limit`` requested; the ``nextCursor`` is unchanged so paginated
clients still converge. Once the store-layer ``RunFilter`` ships
this code will swap to pushing the filter through unchanged.
"""

from __future__ import annotations

from typing import Annotated

from custos_spl.pagination import Cursor
from fastapi import APIRouter, Body, Depends, Header, Path, Query

from custos_workflow.api.dependencies import (
    get_call_context,
    get_run_controller,
    get_validator,
    workspace_path,
)
from custos_workflow.api.models import (
    MAX_LIST_LIMIT,
    CancelRunRequest,
    RunListResponse,
    RunRefResponse,
    RunResponse,
    StartRunRequest,
)
from custos_workflow.call_context import CallContext
from custos_workflow.runs.controller import RunController, RunRef
from custos_workflow.runs.ids import RunId
from custos_workflow.runs.model import RunRecord, RunStatus
from custos_workflow.validator import StartRunValidator

__all__ = ["router"]

router = APIRouter(tags=["runs"])


# ---------------------------------------------------------------------------
# Projection helpers
# ---------------------------------------------------------------------------


def _ref_response_from_ref(ref: RunRef) -> RunRefResponse:
    """Translate a controller :class:`RunRef` to its wire shape.

    The controller's :class:`RunRef` carries the four identity
    fields plus the live status; ``startedAt`` is None because the
    handle is minted before the runtime stamps a start time. The
    full :class:`RunResponse` reads fill ``startedAt`` from the
    persisted :class:`RunRecord`.
    """
    return RunRefResponse(
        run_id=str(ref.run_id),
        status=ref.status,
        workspace_id=ref.workspace_id,
        workflow_version_id=ref.workflow_version_id,
        started_at=None,
    )


def _run_response_from_record(record: RunRecord) -> RunResponse:
    """Translate a persisted :class:`RunRecord` into the full wire shape.

    The ``inputs`` / ``outputs`` / ``steps`` projections are
    deliberately empty: the persisted record does not carry the
    inputs payload (it lives in the audit trail) nor the per-step
    timeline (it lives in the step store, WF-IMPL-033+). The
    step-timeline plumbing lands with the observability wiring
    (WF-IMPL-070); until then the wire shape is contract-stable —
    the fields are present, just empty — so SDK clients can branch
    on them without a future-breaking change.
    """
    return RunResponse(
        run_id=str(record.run_id),
        status=record.status,
        workspace_id=record.workspace_id,
        workflow_version_id=record.workflow_version,
        reason=record.reason,
        started_at=record.started_at,
        updated_at=record.updated_at,
        inputs={},
        outputs=None,
        steps=[],
    )


def _resolve_idempotency_key(
    *,
    body_key: str | None,
    header_key: str | None,
) -> str | None:
    """Pick the effective idempotency key per the WF-IMPL-065 contract.

    Order: a non-empty body field wins over the header (the issue
    acceptance criteria pin "body-field idempotencyKey overrides
    header"); an empty string in either field is normalised to
    ``None`` so callers can opt out by sending ``""``. The
    validator + ledger both treat ``None`` as "no key supplied",
    so we keep the same convention here.
    """
    body = (body_key or "").strip() or None
    header = (header_key or "").strip() or None
    return body if body is not None else header


# ---------------------------------------------------------------------------
# POST /v1/workspaces/{ws}/runs — StartRun
# ---------------------------------------------------------------------------


@router.post(
    "/v1/workspaces/{ws}/runs",
    response_model=RunRefResponse,
    status_code=202,
    summary="Start a workflow run.",
)
async def start_run(
    body: Annotated[StartRunRequest, Body(...)],
    ws: Annotated[str, Depends(workspace_path)],
    validator: Annotated[StartRunValidator, Depends(get_validator)],
    controller: Annotated[RunController, Depends(get_run_controller)],
    ctx: Annotated[CallContext, Depends(get_call_context)],
    idempotency_key_header: Annotated[
        str | None,
        Header(
            alias="Idempotency-Key",
            description=(
                "Optional RFC-style idempotency key; superseded by a "
                "non-empty `idempotencyKey` body field if both are present."
            ),
        ),
    ] = None,
) -> RunRefResponse:
    """Validate then start a workflow run.

    Returns ``202 Accepted`` with the caller-facing
    :class:`RunRefResponse` handle. The controller's idempotency
    gate guarantees a replayed ``StartRun`` against an existing
    ledger entry returns the original ``runId`` without scheduling
    a second workflow instance.
    """
    effective_key = _resolve_idempotency_key(
        body_key=body.idempotency_key,
        header_key=idempotency_key_header,
    )
    validated = await validator.validate_start_run(
        workspace_id=ws,
        workflow_version_id=body.workflow_version_id,
        inputs=body.inputs,
        idempotency_key=effective_key,
        call_context=ctx,
    )
    ref = await controller.start_run(
        workspace_id=ws,
        workflow_version_id=validated.workflow_version_id,
        inputs=validated.inputs,
        idempotency_key=validated.idempotency_key,
    )
    return _ref_response_from_ref(ref)


# ---------------------------------------------------------------------------
# GET /v1/workspaces/{ws}/runs — ListRuns
# ---------------------------------------------------------------------------


@router.get(
    "/v1/workspaces/{ws}/runs",
    response_model=RunListResponse,
    status_code=200,
    summary="List workflow runs in a workspace.",
)
async def list_runs(
    ws: Annotated[str, Depends(workspace_path)],
    controller: Annotated[RunController, Depends(get_run_controller)],
    _ctx: Annotated[CallContext, Depends(get_call_context)],
    status: Annotated[
        RunStatus | None,
        Query(description="Restrict to runs currently in this lifecycle status."),
    ] = None,
    workflow_version_id: Annotated[
        str | None,
        Query(
            alias="workflowVersionId",
            min_length=1,
            description="Restrict to runs of this `WorkflowVersion.id`.",
        ),
    ] = None,
    cursor: Annotated[
        str | None,
        Query(
            min_length=1,
            description="Opaque pagination token from a previous page's `nextCursor`.",
        ),
    ] = None,
    limit: Annotated[
        int | None,
        Query(
            ge=1,
            le=MAX_LIST_LIMIT,
            description=f"Max items per page; capped at {MAX_LIST_LIMIT}.",
        ),
    ] = None,
) -> RunListResponse:
    """Return a paginated slice of the workspace's runs.

    The optional ``status`` and ``workflowVersionId`` filters are
    applied in-memory over the fetched page until the
    store-layer :class:`RunFilter` plumbing lands (see the module
    docstring). The ``nextCursor`` is forwarded unchanged so
    paginated callers converge regardless.
    """
    page = await controller.list_runs(
        workspace_id=ws,
        cursor=Cursor(token=cursor) if cursor else None,
        limit=limit,
    )

    def _matches(ref: RunRef) -> bool:
        if status is not None and ref.status != status:
            return False
        return not (
            workflow_version_id is not None and ref.workflow_version_id != workflow_version_id
        )

    items = [_ref_response_from_ref(ref) for ref in page.items if _matches(ref)]
    return RunListResponse(
        items=items,
        next_cursor=page.next_cursor.token if page.next_cursor is not None else None,
    )


# ---------------------------------------------------------------------------
# GET /v1/workspaces/{ws}/runs/{runId} — GetRun
# ---------------------------------------------------------------------------


@router.get(
    "/v1/workspaces/{ws}/runs/{run_id}",
    response_model=RunResponse,
    status_code=200,
    summary="Fetch a workflow run plus its step timeline.",
)
async def get_run(
    ws: Annotated[str, Depends(workspace_path)],
    controller: Annotated[RunController, Depends(get_run_controller)],
    _ctx: Annotated[CallContext, Depends(get_call_context)],
    run_id: Annotated[
        str,
        Path(
            min_length=1,
            description="The opaque run identifier returned by `StartRun`.",
        ),
    ],
) -> RunResponse:
    """Return the persisted :class:`RunRecord` projection.

    Step timeline + inputs / outputs ride on follow-on tasks
    (WF-IMPL-070); the wire shape is contract-stable today so SDK
    clients can branch on field presence without a breaking
    change.
    """
    record = await controller.get_run(workspace_id=ws, run_id=RunId(run_id))
    return _run_response_from_record(record)


# ---------------------------------------------------------------------------
# POST /v1/workspaces/{ws}/runs/{runId}:cancel — CancelRun
# ---------------------------------------------------------------------------


@router.post(
    "/v1/workspaces/{ws}/runs/{run_id}:cancel",
    response_model=RunRefResponse,
    status_code=202,
    summary="Request cancellation of a running workflow.",
)
async def cancel_run(
    ws: Annotated[str, Depends(workspace_path)],
    controller: Annotated[RunController, Depends(get_run_controller)],
    _ctx: Annotated[CallContext, Depends(get_call_context)],
    run_id: Annotated[
        str,
        Path(
            min_length=1,
            description="The opaque run identifier returned by `StartRun`.",
        ),
    ],
    body: Annotated[CancelRunRequest | None, Body()] = None,
) -> RunRefResponse:
    """Forward a cancellation request to the runtime.

    Returns ``202 Accepted``; the runtime processes the
    cancellation asynchronously. Idempotent: cancelling an
    already-cancelled / -cancelling run is a no-op on the
    controller (see
    :meth:`RunController.cancel_run`); the wire shape returned is
    the current :class:`RunRef`.
    """
    reason: str | None = body.reason if body is not None else None
    ref = await controller.cancel_run(
        workspace_id=ws,
        run_id=RunId(run_id),
        reason=reason,
    )
    return _ref_response_from_ref(ref)
