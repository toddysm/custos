"""Internal RPC routes for the ``Run`` resource (WF-IMPL-067 + WF-IMPL-068).

Per ``design.md`` § Internal RPC, internal callers (today the
Trigger Service; tomorrow any in-cluster sibling) reach the Run
Controller through a flat ``/internal/`` URL prefix rather than
the public ``/v1/workspaces/{ws}/`` shape. The prefix is the seam
the Helm chart / mesh uses to pin mTLS-only access; the actual
mTLS gate lands with the API Gateway integration (see
``design/components/workflow-service/todos.md``).

Three RPC verbs land in this module:

* ``POST /internal/runs:start`` — ``StartRun`` RPC. Body shape is
  :class:`~custos_workflow.api.models.InternalStartRunRequest`
  which extends the public
  :class:`~custos_workflow.api.models.StartRunRequest` with an
  explicit ``workspaceId`` body field (the path carries no
  ``{ws}`` segment).
* ``POST /internal/runs/{runId}:cancel`` — ``CancelRun`` RPC. Body
  shape is
  :class:`~custos_workflow.api.models.InternalCancelRunRequest`
  which extends :class:`~custos_workflow.api.models.CancelRunRequest`
  the same way.
* ``POST /internal/runs/{runId}/steps/{stepId}:raiseEvent`` —
  ``RaiseExternalEvent`` RPC (WF-IMPL-068). Body shape is
  :class:`~custos_workflow.api.models.RaiseExternalEventRequest`
  which carries ``workspaceId`` + ``eventName`` + ``payload`` +
  ``idempotencyKey``. The route returns ``202 Accepted`` with an
  empty body — the workflow's ``wait_for_external_event`` step
  resumes asynchronously inside Dapr.

All three routes share the WF-IMPL-064 dependency factories
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

``POST /internal/runs/{runId}/steps/{stepId}:raiseEvent`` keys its
in-process dedup ledger on
``(workspaceId, runId, stepId, eventName, idempotencyKey)`` for a
``WF_IDEMPOTENCY_KEY_TTL`` window. A duplicate body inside the
window is a no-op (still 202) — no second event lands on the
workflow. Omitting ``idempotencyKey`` opts out of dedup entirely;
every such call dispatches.

Error envelopes
---------------

The error taxonomy is identical to the public surface: every
:class:`RunControllerError` / :class:`ValidatorError` is mapped
through the WF-IMPL-061 handler chain to the locked RFC 7807
envelope.

* ``CancelRun`` and ``RaiseExternalEvent`` of an unknown run id
  return the ``workflow.run_not_found`` (404) envelope.
* ``CancelRun`` of a run that is already in a terminal
  *non-cancel* status (``succeeded`` / ``failed``) returns the
  ``workflow.run_state_conflict`` (409) envelope; calling
  ``CancelRun`` on a run that is already ``cancelling`` /
  ``cancelled`` is an idempotent no-op that returns 202 with the
  current :class:`RunRef` (the controller short-circuits without
  re-issuing the terminate).
* ``RaiseExternalEvent`` against a run in ANY terminal status
  (``succeeded`` / ``failed`` / ``cancelled``) returns the
  ``workflow.run_state_conflict`` (409) envelope — the Dapr
  ``raise_event`` primitive cannot deliver to a finished
  instance.

The Internal RPC routes do NOT introduce any new ``code`` values.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Annotated

from custos_spl.errors import (
    BackendUnavailable,
    ImmutableViolation,
    LeaseBusy,
    LeaseExpired,
    MigrationRequired,
)
from custos_spl.interfaces.delivery import RESUME_CHANNEL_PREFIX
from fastapi import APIRouter, Body, Depends, Header, Path, Request, Response

from custos_workflow.api.dependencies import (
    get_call_context,
    get_run_components,
    get_run_controller,
    get_validator,
)
from custos_workflow.api.models import (
    InternalCancelRunRequest,
    InternalStartRunRequest,
    RaiseExternalEventRequest,
    RunRefResponse,
)
from custos_workflow.api.routes.runs import (
    ref_response_from_ref,
    resolve_idempotency_key,
)
from custos_workflow.call_context import CallContext
from custos_workflow.runs.controller import RunController
from custos_workflow.runs.errors import RunStateConflictError, WorkflowRuntimeUnavailableError
from custos_workflow.runs.ids import RunId
from custos_workflow.runs.trigger_delivery import TriggerDeliveryController
from custos_workflow.validator import StartRunValidator
from custos_workflow.validator.errors import WorkspaceUnauthorizedError

__all__ = ["router"]

router = APIRouter(tags=["internal-rpc"])


def _delivery_controller(request: Request) -> TriggerDeliveryController | None:
    """Resolve the opt-in controller without changing the legacy RPC dependency contract."""
    if getattr(request.app.state, "run_components", None) is None:
        return None
    return get_run_components(request).trigger_delivery_controller


def _delivery_scope(ctx: CallContext, workspace_id: str) -> None:
    """Authorize scope before looking up an original receipt or runtime instance."""
    if ctx.workspace is not None and ctx.workspace != workspace_id:
        raise WorkspaceUnauthorizedError(
            "delivery workspace does not match call context",
            workspace_id=workspace_id,
            principal=ctx.principal,
        )


@contextmanager
def _delivery_errors() -> Iterator[None]:
    """Map explicit storage/ownership failures through the existing Workflow error envelope."""
    try:
        yield
    except (ImmutableViolation, ValueError) as exc:
        raise RunStateConflictError(str(exc)) from exc
    except (BackendUnavailable, LeaseBusy, LeaseExpired, MigrationRequired) as exc:
        raise WorkflowRuntimeUnavailableError(
            "durable delivery is temporarily unavailable", cause=type(exc).__name__
        ) from exc


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
    delivery: Annotated[TriggerDeliveryController | None, Depends(_delivery_controller)],
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
    if body.delivery_protocol == 2:
        _delivery_scope(ctx, body.workspace_id)
        if delivery is None:
            raise WorkflowRuntimeUnavailableError("version-2 Trigger delivery is not enabled")
        assert effective_key is not None and body.retain_until is not None
        with _delivery_errors():
            prior = await delivery.lookup_start(
                body.workspace_id,
                body.workflow_version_id,
                body.inputs,
                effective_key,
                body.retain_until,
            )
            if prior is not None:
                return ref_response_from_ref(prior)
            validated = await validator.validate_start_run(
                workspace_id=body.workspace_id,
                workflow_version_id=body.workflow_version_id,
                inputs=body.inputs,
                idempotency_key=effective_key,
                call_context=ctx,
            )
            ref = await delivery.start(
                body.workspace_id,
                validated.workflow_version,
                validated.inputs,
                effective_key,
                body.retain_until,
            )
            return ref_response_from_ref(ref)
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
    ``workflow.run_not_found`` (404) envelope. A run that is
    already in a terminal non-cancel state (``succeeded`` /
    ``failed``) surfaces the ``workflow.run_state_conflict``
    (409) envelope. A run that is already ``cancelling`` /
    ``cancelled`` is an idempotent no-op handled by the
    controller — the route returns 202 with the run's current
    :class:`RunRef` without re-issuing the terminate.
    """
    ref = await controller.cancel_run(
        workspace_id=body.workspace_id,
        run_id=RunId(run_id),
        reason=body.reason,
    )
    return ref_response_from_ref(ref)


# ---------------------------------------------------------------------------
# POST /internal/runs/{run_id}/steps/{step_id}:raiseEvent — RaiseExternalEvent
# ---------------------------------------------------------------------------


@router.post(
    "/internal/runs/{run_id:path}/steps/{step_id}:raiseEvent",
    status_code=202,
    summary="Internal RPC: deliver an external event into a running workflow.",
)
async def raise_external_event(
    body: Annotated[RaiseExternalEventRequest, Body(...)],
    controller: Annotated[RunController, Depends(get_run_controller)],
    _ctx: Annotated[CallContext, Depends(get_call_context)],
    delivery: Annotated[TriggerDeliveryController | None, Depends(_delivery_controller)],
    run_id: Annotated[
        str,
        Path(
            min_length=1,
            description="Opaque root or slash-addressed child run identifier.",
        ),
    ],
    step_id: Annotated[
        str,
        Path(
            min_length=1,
            description=(
                "Stable identifier of the `waitFor:` step the event "
                "is paired with. Used for dedup; the runtime keys "
                "delivery by `eventName` alone."
            ),
        ),
    ],
) -> Response:
    """Forward a Trigger-Service ``RaiseExternalEvent`` into Dapr.

    Mirrors design.md § Internal RPC (inbound) row
    ``RaiseExternalEvent``. The body carries the authoritative
    ``workspaceId`` (the ``/internal/`` URL surface drops the
    ``{ws}`` path segment). The route returns ``202 Accepted``
    with an empty body: the workflow's
    ``wait_for_external_event`` step resumes asynchronously
    inside Dapr.

    Idempotency on this surface is keyed by
    ``(workspaceId, runId, stepId, eventName, idempotencyKey)``
    inside the controller's in-process event-dispatch ledger
    (WF-IMPL-068). A duplicate body within the
    ``WF_IDEMPOTENCY_KEY_TTL`` window is a no-op (still returns
    202) — no second event lands on the workflow. Omitting
    ``idempotencyKey`` opts out of dedup entirely; every such
    call dispatches.

    Error envelopes:

    * Unknown ``runId`` → ``workflow.run_not_found`` (404).
    * Terminal-state run (``succeeded`` / ``failed`` /
      ``cancelled``) → ``workflow.run_state_conflict`` (409).
    * Runtime failure → ``workflow.runtime_unavailable`` (503).
    """
    _delivery_scope(_ctx, body.workspace_id)
    if body.delivery_protocol == 1 and body.event_name.casefold().startswith(RESUME_CHANNEL_PREFIX):
        raise RunStateConflictError("reserved resume channels require versioned delivery")
    if body.delivery_protocol == 2:
        if delivery is None:
            raise WorkflowRuntimeUnavailableError("version-2 Trigger delivery is not enabled")
        assert body.idempotency_key is not None and body.retain_until is not None
        with _delivery_errors():
            await delivery.resume(
                body.workspace_id,
                run_id,
                step_id,
                body.event_name,
                body.payload,
                body.idempotency_key,
                body.retain_until,
            )
        return Response(status_code=202)
    await controller.raise_external_event(
        workspace_id=body.workspace_id,
        run_id=RunId(run_id),
        step_id=step_id,
        event_name=body.event_name,
        payload=body.payload,
        idempotency_key=body.idempotency_key,
    )
    return Response(status_code=202)
