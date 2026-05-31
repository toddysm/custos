"""REST routes for the ``Step`` resource (WF-IMPL-066, #452).

Per ``design.md`` § Public Interface, the public Step surface is
two routes:

* ``GET /v1/workspaces/{ws}/runs/{runId}/steps/{stepId}``      — fetch one step.
* ``GET /v1/workspaces/{ws}/runs/{runId}/steps/{stepId}/logs`` — stream step logs.

The log-stream endpoint is intentionally a documented stub: the
implementation plan delegates log streaming to the Observability
Service (COMP-009) and defers the actual streaming logic until the
*Full Observability Client integration* sub-module lands (see the
deferred-sub-modules list in
``design/components/workflow-service/todos.md``). Until then the
endpoint returns ``501 Not Implemented`` with the locked
``workflow.api.not_implemented`` RFC 7807 envelope so SDK clients
can branch deterministically on ``code`` rather than depending on
status text.

The step-fetch endpoint projects the persisted run's compiled
:class:`~custos_workflow.graph.model.ExecutionGraph` into the
public :class:`~custos_workflow.api.models.StepResponse` wire
shape. Per-step lifecycle state + per-attempt history will land
with the step-state persistence work (the
``WF-IMPL-070``-aligned timeline projection); the fields are
present on the wire today \u2014 ``status`` defaults to
``"pending"`` and ``attempts`` is empty \u2014 so SDK clients can
branch on field presence without a future-breaking change once
the real state plumbing wires through.

When the persisted run carries no compiled graph (the controller
returned a :class:`~custos_workflow.runs.model.RunRecord` whose
``compiled_graph`` attribute is ``None`` because the workflow
compile step has not yet run) or when the requested ``stepId`` is
not a member of the compiled graph, the route renders the
``workflow.step_not_found`` (404) envelope. Unknown ``runId`` is
translated by the WF-IMPL-061 handler for
:class:`~custos_workflow.runs.errors.RunNotFoundError`.
"""

from __future__ import annotations

from typing import Annotated, Final

from fastapi import APIRouter, Depends, Path, Request
from starlette.responses import JSONResponse

from custos_workflow.api.dependencies import (
    get_call_context,
    get_run_controller,
    workspace_path,
)
from custos_workflow.api.errors import problem_response
from custos_workflow.api.models import StepResponse
from custos_workflow.call_context import CallContext
from custos_workflow.runs.controller import RunController
from custos_workflow.runs.ids import RunId
from custos_workflow.runs.model import RunRecord

__all__ = ["LOG_STREAM_NOT_IMPLEMENTED_DETAIL", "router"]

router = APIRouter(tags=["steps"])


#: The exact ``detail`` string returned by the log-stream stub.
#: Locked verbatim by the WF-IMPL-066 acceptance criteria so
#: dev-docs (WF-IMPL-072) and contributor docs can reproduce the
#: envelope without re-parsing the source. The wording calls out
#: the delegation to the Observability Service (COMP-009) and the
#: deferred sub-module so operators reading the response know
#: which work item ships the real handler.
LOG_STREAM_NOT_IMPLEMENTED_DETAIL: Final[str] = (
    "Step log streaming is delegated to the Observability Service "
    "(COMP-009); deferred until the Full Observability Client "
    "integration sub-module lands."
)


# ---------------------------------------------------------------------------
# Projection helpers
# ---------------------------------------------------------------------------


def _step_response_from_record(record: RunRecord, *, step_id: str) -> StepResponse | None:
    """Project the compiled :class:`ExecutionNode` for ``step_id`` to wire.

    Returns ``None`` when the record either has no compiled graph
    (the run has not reached the compile stage) or when no
    :class:`~custos_workflow.graph.model.ExecutionNode` in the
    graph carries the requested ``step_id``. Callers translate
    that ``None`` into the locked ``workflow.step_not_found`` (404)
    envelope.

    Per-step lifecycle state + per-attempt history are not
    persisted yet (the design promises them with the follow-on
    step-state persistence work); the projection emits the
    contract-stable empty / defaulted values so SDK clients can
    branch on field presence without a breaking change once the
    real state plumbing wires through.
    """
    if record.compiled_graph is None:
        return None
    node = next((n for n in record.compiled_graph.nodes if n.step_id == step_id), None)
    if node is None:
        return None
    return StepResponse(
        step_id=node.step_id,
        kind=str(node.kind),
        status="pending",
        attempts=[],
        started_at=None,
        finished_at=None,
        outputs=None,
    )


def _step_not_found_response(
    request: Request,
    *,
    workspace_id: str,
    run_id: str,
    step_id: str,
) -> JSONResponse:
    """Render the locked ``workflow.step_not_found`` (404) envelope.

    The ``workspaceId`` / ``runId`` / ``stepId`` extension fields
    mirror the equivalents on
    :class:`~custos_workflow.runs.errors.RunNotFoundError` so SDK
    branch logic can correlate a step-fetch 404 with the owning
    run / workspace without re-parsing the URL.
    """
    return problem_response(
        kind="workflow.step_not_found",
        detail=(
            f"No step {step_id!r} in run {run_id!r} for workspace "
            f"{workspace_id!r}; the run exists but its compiled "
            "graph does not carry that step id."
        ),
        instance=request.url.path,
        extras={
            "workspaceId": workspace_id,
            "runId": run_id,
            "stepId": step_id,
        },
    )


# ---------------------------------------------------------------------------
# GET /v1/workspaces/{ws}/runs/{run_id}/steps/{step_id} \u2014 GetStep
# ---------------------------------------------------------------------------


@router.get(
    "/v1/workspaces/{ws}/runs/{run_id}/steps/{step_id}",
    response_model=StepResponse,
    status_code=200,
    summary="Fetch a single step's compiled-graph projection.",
)
async def get_step(
    request: Request,
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
    step_id: Annotated[
        str,
        Path(
            min_length=1,
            description="The compiled step's stable identifier.",
        ),
    ],
) -> StepResponse | JSONResponse:
    """Return the :class:`StepResponse` projection for one step.

    Unknown ``runId`` raises :class:`RunNotFoundError` which the
    WF-IMPL-061 handler chain translates into the
    ``workflow.run_not_found`` (404) envelope. Unknown ``stepId``
    (or a record with no compiled graph yet) returns the locked
    ``workflow.step_not_found`` (404) envelope inline.
    """
    record = await controller.get_run(workspace_id=ws, run_id=RunId(run_id))
    projected = _step_response_from_record(record, step_id=step_id)
    if projected is None:
        return _step_not_found_response(
            request,
            workspace_id=ws,
            run_id=run_id,
            step_id=step_id,
        )
    return projected


# ---------------------------------------------------------------------------
# GET /v1/workspaces/{ws}/runs/{run_id}/steps/{step_id}/logs \u2014 StreamStepLogs
# ---------------------------------------------------------------------------


@router.get(
    "/v1/workspaces/{ws}/runs/{run_id}/steps/{step_id}/logs",
    status_code=501,
    summary="Stream a step's logs (deferred to Observability Service).",
)
async def stream_step_logs(
    request: Request,
    ws: Annotated[str, Depends(workspace_path)],
    _ctx: Annotated[CallContext, Depends(get_call_context)],
    run_id: Annotated[
        str,
        Path(
            min_length=1,
            description="The opaque run identifier returned by `StartRun`.",
        ),
    ],
    step_id: Annotated[
        str,
        Path(
            min_length=1,
            description="The compiled step's stable identifier.",
        ),
    ],
) -> JSONResponse:
    """Return the locked ``workflow.api.not_implemented`` (501) envelope.

    The implementation plan delegates real step-log streaming to
    the Observability Service (COMP-009); this route ships as a
    documented stub so the route table is contract-complete the
    moment WF-IMPL-066 merges. The wording of
    :data:`LOG_STREAM_NOT_IMPLEMENTED_DETAIL` is locked by the
    issue's acceptance criteria so dev docs (WF-IMPL-072) can
    reproduce it verbatim.
    """
    return problem_response(
        kind="workflow.api.not_implemented",
        detail=LOG_STREAM_NOT_IMPLEMENTED_DETAIL,
        instance=request.url.path,
        extras={
            "workspaceId": ws,
            "runId": run_id,
            "stepId": step_id,
        },
    )
