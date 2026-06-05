"""Manual REST CRUD + ``:fire`` for trigger subscriptions (TS-IMPL-015).

This router is the operator-facing surface of the Trigger Service. It owns the
subscription lifecycle —

* ``POST   /v1/workspaces/{ws}/triggers``        create a start subscription
* ``GET    /v1/workspaces/{ws}/triggers/{id}``   read one back
* ``PATCH  /v1/workspaces/{ws}/triggers/{id}``   amend selector/mapping/state
* ``DELETE /v1/workspaces/{ws}/triggers/{id}``   soft-delete (state -> expired)
* ``POST   /v1/workspaces/{ws}/triggers/{id}:fire`` fire it now, get the runId

Every selector is validated through the CEL evaluator on create/patch so a
malformed expression is rejected at author time (422) rather than silently
never matching. A manual ``:fire`` normalizes into the same pipeline an
inbound event takes (classify -> match -> dispatch), so manual and automatic
starts share one dispatch path. RBAC is delegated to the call-context
middleware via :func:`require_permission`. Failures surface through the
RFC 7807 Problem+JSON envelope in :mod:`custos_trigger.api.errors`.

The workspace in the path is authoritative for persistence; the call context's
workspace governs RBAC. A subscription id is server-minted (uuid4 hex) so
clients never collide on the immutable base row.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Path, Request, Response, status
from fastapi.responses import JSONResponse

from custos_trigger.api.errors import (
    API_SUBSCRIPTION_NOT_FIREABLE,
    problem_response,
)
from custos_trigger.dependencies import (
    get_dispatcher,
    get_selector_evaluator,
    get_subscription_store,
)
from custos_trigger.errors import TriggerError, TriggerErrorKind
from custos_trigger.middleware import CallContext, require_permission
from custos_trigger.models import (
    ManualFireRequest,
    ManualFireResult,
    Subscription,
    SubscriptionCreate,
    SubscriptionKind,
    SubscriptionPatch,
    SubscriptionState,
)
from custos_trigger.normalize import normalize_manual_fire
from custos_trigger.pipeline.dispatch import Dispatcher
from custos_trigger.pipeline.match_start import StartMatcher
from custos_trigger.selector import SelectorEvaluator
from custos_trigger.stores import SubscriptionStore

__all__ = ["router"]

router = APIRouter(prefix="/v1/workspaces/{workspace_id}/triggers", tags=["subscriptions"])

# Permission scopes delegated to the call-context middleware (RBAC).
PERM_READ = "trigger:subscriptions:read"
PERM_WRITE = "trigger:subscriptions:write"
PERM_DELETE = "trigger:subscriptions:delete"
PERM_FIRE = "trigger:subscriptions:fire"

_WorkspacePath = Annotated[str, Path(min_length=1, description="Owning workspace id.")]
_SubscriptionPath = Annotated[str, Path(min_length=1, description="Subscription id.")]

StoreDep = Annotated[SubscriptionStore, Depends(get_subscription_store)]
EvaluatorDep = Annotated[SelectorEvaluator, Depends(get_selector_evaluator)]
DispatcherDep = Annotated[Dispatcher, Depends(get_dispatcher)]


def _now() -> datetime:
    return datetime.now(UTC)


def _validate_selector(
    evaluator: SelectorEvaluator, selector: str | None, *, subscription_id: str
) -> None:
    """Compile ``selector`` to reject a malformed expression at author time.

    A :class:`~custos_trigger.selector.SelectorInvalidError` is a
    :class:`TriggerError` (kind ``trigger.selector_invalid``) and propagates to
    the Problem+JSON handler as a 422.
    """
    if selector:
        evaluator.compile(selector, subscription_id=subscription_id)


def _not_found(subscription_id: str) -> TriggerError:
    return TriggerError(
        TriggerErrorKind.SUBSCRIPTION_NOT_FOUND,
        "subscription not found",
        details={"subscriptionId": subscription_id},
    )


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=Subscription,
    response_model_by_alias=True,
)
async def create_subscription(
    workspace_id: _WorkspacePath,
    body: SubscriptionCreate,
    store: StoreDep,
    evaluator: EvaluatorDep,
    _ctx: Annotated[CallContext, Depends(require_permission(PERM_WRITE))],
) -> Subscription:
    """Create a manual/start subscription and return the persisted row."""
    subscription_id = uuid.uuid4().hex
    _validate_selector(evaluator, body.selector, subscription_id=subscription_id)
    now = _now()
    subscription = Subscription(
        workspace_id=workspace_id,
        subscription_id=subscription_id,
        kind=SubscriptionKind.START,
        source_type=body.source_type,
        workflow_id=body.workflow_id,
        target_workflow_version_id=body.target_workflow_version_id,
        selector=body.selector,
        input_mapping=body.input_mapping,
        state=SubscriptionState.ACTIVE,
        created_at=now,
        updated_at=now,
    )
    return await store.create(subscription)


@router.get(
    "/{subscription_id}",
    response_model=Subscription,
    response_model_by_alias=True,
)
async def get_subscription(
    workspace_id: _WorkspacePath,
    subscription_id: _SubscriptionPath,
    store: StoreDep,
    _ctx: Annotated[CallContext, Depends(require_permission(PERM_READ))],
) -> Subscription:
    """Read one subscription back by id (404 when unknown)."""
    subscription = await store.get(workspace_id, subscription_id)
    if subscription is None:
        raise _not_found(subscription_id)
    return subscription


@router.patch(
    "/{subscription_id}",
    response_model=Subscription,
    response_model_by_alias=True,
)
async def patch_subscription(
    workspace_id: _WorkspacePath,
    subscription_id: _SubscriptionPath,
    body: SubscriptionPatch,
    store: StoreDep,
    evaluator: EvaluatorDep,
    _ctx: Annotated[CallContext, Depends(require_permission(PERM_WRITE))],
) -> Subscription:
    """Amend a subscription's selector / input mapping / target / state."""
    existing = await store.get(workspace_id, subscription_id)
    if existing is None:
        raise _not_found(subscription_id)

    if body.selector is not None:
        _validate_selector(evaluator, body.selector, subscription_id=subscription_id)

    # Fields carried in the selector revision blob (selector / mapping /
    # target) versus the base-row state column are persisted via distinct SPL
    # writes, so split the patch accordingly.
    blob_updates: dict[str, object] = {}
    if body.selector is not None:
        blob_updates["selector"] = body.selector
    if body.input_mapping is not None:
        blob_updates["input_mapping"] = body.input_mapping
    if body.target_workflow_version_id is not None:
        blob_updates["target_workflow_version_id"] = body.target_workflow_version_id

    next_state = body.state if body.state is not None else existing.state
    merged = existing.model_copy(update={**blob_updates, "state": next_state, "updated_at": _now()})

    if body.state is not None and body.state is not existing.state:
        await store.set_state(workspace_id, subscription_id, body.state)
    if blob_updates:
        await store.reauthor_selector(merged)

    updated = await store.get(workspace_id, subscription_id)
    if updated is None:  # pragma: no cover - row cannot vanish mid-request
        raise _not_found(subscription_id)
    return updated


@router.delete("/{subscription_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_subscription(
    workspace_id: _WorkspacePath,
    subscription_id: _SubscriptionPath,
    store: StoreDep,
    _ctx: Annotated[CallContext, Depends(require_permission(PERM_DELETE))],
) -> Response:
    """Soft-delete a subscription by transitioning it to ``expired``.

    The locked SPL surface has no row-delete; ``expired`` is the terminal
    lifecycle state and stops the subscription from matching any future event.
    """
    existing = await store.get(workspace_id, subscription_id)
    if existing is None:
        raise _not_found(subscription_id)
    await store.set_state(workspace_id, subscription_id, SubscriptionState.EXPIRED)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post(
    "/{subscription_id}:fire",
    response_model=ManualFireResult,
    response_model_by_alias=True,
)
async def fire_subscription(
    request: Request,
    workspace_id: _WorkspacePath,
    subscription_id: _SubscriptionPath,
    body: ManualFireRequest,
    store: StoreDep,
    evaluator: EvaluatorDep,
    dispatcher: DispatcherDep,
    _ctx: Annotated[CallContext, Depends(require_permission(PERM_FIRE))],
) -> ManualFireResult | JSONResponse:
    """Fire a subscription now: normalize -> match -> dispatch -> ``{runId}``."""
    subscription = await store.get(workspace_id, subscription_id)
    if subscription is None:
        raise _not_found(subscription_id)

    extras = {"subscriptionId": subscription_id}
    event = normalize_manual_fire(
        occurred_at=_now().isoformat(),
        subscription_id=subscription_id,
        inputs=body.inputs,
    )
    matches = StartMatcher(evaluator).match(event, [subscription])
    if not matches:
        return problem_response(
            request,
            kind=API_SUBSCRIPTION_NOT_FIREABLE,
            detail="subscription is not active or its selector did not match the fire inputs",
            extras=extras,
        )

    outcome = await dispatcher.dispatch_start(event, matches[0])
    if outcome.is_dispatched and outcome.run_ref is not None:
        return ManualFireResult(run_id=outcome.run_ref.run_id)
    if outcome.is_duplicate:
        return problem_response(
            request,
            kind=TriggerErrorKind.DEDUP_DUPLICATE.value,
            detail="a run for this fire was already dispatched",
            extras=extras,
        )
    detail = (
        str(outcome.error)
        if outcome.error is not None
        else "dispatch to the Workflow Service failed"
    )
    return problem_response(
        request,
        kind=TriggerErrorKind.DISPATCH_FAILED.value,
        detail=detail,
        extras=extras,
    )
