"""Internal RPC routes for the ``Run`` resource (WF-IMPL-067, #453).

Per ``design.md`` \u00a7 Internal RPC, internal callers (today the
Trigger Service; tomorrow any in-cluster sibling) reach the Run
Controller through a flat ``/internal/`` URL prefix rather than
the public ``/v1/workspaces/{ws}/`` shape. The prefix is the seam
the Helm chart / mesh uses to pin mTLS-only access; the actual
mTLS gate lands with the API Gateway integration (see
``design/components/workflow-service/todos.md``).

Two RPC verbs land in this task:

* ``POST /internal/runs:start`` \u2014 ``StartRun`` RPC. Body shape is
  :class:`~custos_workflow.api.models.InternalStartRunRequest`
  which extends the public
  :class:`~custos_workflow.api.models.StartRunRequest` with an
  explicit ``workspaceId`` body field (the path carries no
  ``{ws}`` segment).
* ``POST /internal/runs/{runId}:cancel`` \u2014 ``CancelRun`` RPC. Body
  shape is
  :class:`~custos_workflow.api.models.InternalCancelRunRequest`
  which extends :class:`~custos_workflow.api.models.CancelRunRequest`
  the same way.

Both routes share the WF-IMPL-064 dependency factories
(:class:`StartRunValidator`, :class:`RunController`, the
``CallContext``) with the public REST routes; the only difference
is how the workspace travels.

Idempotency
-----------

``POST /internal/runs:start`` reuses the WF-IMPL-065 idempotency
contract: the body's ``idempotencyKey`` falls back to the
``Idempotency-Key`` HTTP header per RFC, with the body field
winning when both are present (empty strings normalised to
``None``). The validator + ledger are the single source of truth
for the dedup decision; replaying a request with the same key
returns the original ``runId`` without scheduling a second
workflow instance, matching the public surface exactly.

Error envelopes
---------------

The error taxonomy is identical to the public surface: every
:class:`RunControllerError` / :class:`ValidatorError` is mapped
through the WF-IMPL-061 handler chain to the locked RFC 7807
envelope. ``CancelRun`` of an unknown run id returns the
``workflow.run_not_found`` (404) envelope; an
already-cancelled run returns the
``workflow.run_state_conflict`` (409) envelope. The Internal
RPC routes do NOT introduce any new ``code`` values.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Body, Depends, Header, Path

from custos_workflow.api.dependencies import (
    get_call_context,
    get_run_controller,
    get_validator,
)
from custos_workflow.api.models import (
    InternalCancelRunRequest,
    InternalStartRunRequest,
    RunRefResponse,
)
from custos_workflow.api.routes.runs import (
    ref_response_from_ref,
    resolve_idempotency_key,
)
from custos_workflow.call_context import CallContext
from custos_workflow.runs.controller import RunController
from custos_workflow.runs.ids import RunId
from custos_workflow.validator import StartRunValidator

__all__ = ["router"]

router = APIRouter(tags=["internal-rpc"])


# ---------------------------------------------------------------------------
# POST /internal/runs:start \u2014 StartRun
# ---------------------------------------------------------------------------


@router.post(
    "/internal/runs:start",
    response_model=RunRefResponse,
    status_code=202,
    summary="Internal RPC: start a workflow run.",
)
async def start_run(
    body: Annotated[InternalStartRunRequest, Body(...)],
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
    """Validate then start a workflow run on behalf of an internal caller.

    The body's ``workspaceId`` is the authoritative workspace
    identifier for the run (the path carries none). Validator +
    controller see it as if it had arrived through the public
    REST surface; auth / mTLS gating is the caller's
    responsibility upstream of this route.
    """
    effective_key = resolve_idempotency_key(
        body_key=body.idempotency_key,
        header_key=idempotency_key_header,
    )
    validated = await validator.validate_start_run(
        workspace_id=body.workspace_id,
        workflow_version_id=body.workflow_version_id,
        inputs=body.inputs,
        idempotency_key=effective_key,
        call_context=ctx,
    )
    ref = await controller.start_run(
        workspace_id=body.workspace_id,
        workflow_version_id=validated.workflow_version_id,
        inputs=validated.inputs,
        idempotency_key=validated.idempotency_key,
    )
    return ref_response_from_ref(ref)


# ---------------------------------------------------------------------------
# POST /internal/runs/{run_id}:cancel \u2014 CancelRun
# ---------------------------------------------------------------------------


@router.post(
    "/internal/runs/{run_id}:cancel",
    response_model=RunRefResponse,
    status_code=202,
    summary="Internal RPC: request cancellation of a running workflow.",
)
async def cancel_run(
    body: Annotated[InternalCancelRunRequest, Body(...)],
    controller: Annotated[RunController, Depends(get_run_controller)],
    _ctx: Annotated[CallContext, Depends(get_call_context)],
    run_id: Annotated[
        str,
        Path(
            min_length=1,
            description="The opaque run identifier returned by `StartRun`.",
        ),
    ],
) -> RunRefResponse:
    """Forward an Internal-RPC cancellation request to the runtime.

    The body's ``workspaceId`` is the authoritative workspace
    identifier (the path carries none); ``reason`` is plumbed
    through to
    :meth:`~custos_workflow.runs.controller.RunController.cancel_run`
    unchanged. Unknown run ids surface the
    ``workflow.run_not_found`` (404) envelope; an
    already-cancelled run surfaces the
    ``workflow.run_state_conflict`` (409) envelope \u2014 both
    locked by the WF-IMPL-061 handler chain.
    """
    ref = await controller.cancel_run(
        workspace_id=body.workspace_id,
        run_id=RunId(run_id),
        reason=body.reason,
    )
    return ref_response_from_ref(ref)
