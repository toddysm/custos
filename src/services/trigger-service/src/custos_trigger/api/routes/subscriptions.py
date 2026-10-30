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
    get_providers,
    get_receiver_configuration_enabled,
    get_selector_evaluator,
    get_subscription_store,
)
from custos_trigger.errors import TriggerError, TriggerErrorKind
from custos_trigger.input_mapping import (
    EVENT_MAPPED_SOURCES,
    resolve_input_mapping,
    validate_input_mapping,
)
from custos_trigger.middleware import CallContext, require_permission
from custos_trigger.middleware import ensure_workspace as _ensure_workspace
from custos_trigger.models import (
    ManualFireRequest,
    ManualFireResult,
    SourceType,
    Subscription,
    SubscriptionCreate,
    SubscriptionKind,
    SubscriptionPatch,
    SubscriptionState,
)
from custos_trigger.normalize import normalize_manual_fire
from custos_trigger.pipeline.dispatch import Dispatcher
from custos_trigger.pipeline.match_start import StartMatch, StartMatcher
from custos_trigger.providers import Providers
from custos_trigger.selector import SelectorEvaluator
from custos_trigger.source_config import (
    require_receiver_configuration,
    validate_connector_reference,
    validate_source_config,
)
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
ProvidersDep = Annotated[Providers, Depends(get_providers)]
ReceiverEnabledDep = Annotated[bool, Depends(get_receiver_configuration_enabled)]


def _now() -> datetime:
    return datetime.now(UTC)


def _validate_selector(
    evaluator: SelectorEvaluator,
    selector: str | None,
    *,
    subscription_id: str,
    source_type: SourceType,
) -> None:
    """Compile against the subscription's source schema at author time.

    A :class:`~custos_trigger.selector.SelectorInvalidError` is a
    :class:`TriggerError` (kind ``trigger.selector_invalid``) and propagates to
    the Problem+JSON handler as a 422.
    """
    if selector:
        evaluator.compile(selector, subscription_id=subscription_id, source_type=source_type)


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
    ctx: Annotated[CallContext, Depends(require_permission(PERM_WRITE))],
    providers: ProvidersDep,
    receiver_enabled: ReceiverEnabledDep,
) -> Subscription:
    """Create a manual/start subscription and return the persisted row."""
    _ensure_workspace(ctx, workspace_id)
    if body.source_type.value not in ("manual", "internal"):
        require_receiver_configuration(receiver_enabled)
    subscription_id = uuid.uuid4().hex
    _validate_selector(
        evaluator, body.selector, subscription_id=subscription_id, source_type=body.source_type
    )
    if body.source_type in EVENT_MAPPED_SOURCES:
        validate_input_mapping(
            body.input_mapping,
            evaluator=evaluator,
            subscription_id=subscription_id,
            source_type=body.source_type,
        )
    if body.source_config is not None and body.source_config.connector_instance_id is not None:
        await validate_connector_reference(
            providers.connector_references, workspace_id, body.source_config.connector_instance_id
        )
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
        source_config=body.source_config,
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
    ctx: Annotated[CallContext, Depends(require_permission(PERM_READ))],
) -> Subscription:
    """Read one subscription back by id (404 when unknown)."""
    _ensure_workspace(ctx, workspace_id)
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
    ctx: Annotated[CallContext, Depends(require_permission(PERM_WRITE))],
    providers: ProvidersDep,
    receiver_enabled: ReceiverEnabledDep,
) -> Subscription:
    """Amend a subscription's selector / input mapping / target / state."""
    _ensure_workspace(ctx, workspace_id)
    existing = await store.get(workspace_id, subscription_id)
    if existing is None:
        raise _not_found(subscription_id)

    # ``model_fields_set`` distinguishes a field sent as ``null`` (an explicit
    # clear) from a field omitted entirely. ``selector`` and
    # ``target_workflow_version_id`` are optional on the entity and so may be
    # cleared; ``input_mapping`` is non-optional, so a value is applied only
    # when present.
    fields_set = body.model_fields_set

    # Configuration and state are committed together after validating the merged request.
    blob_updates: dict[str, object] = {}
    if "selector" in fields_set:
        blob_updates["selector"] = body.selector
    if "target_workflow_version_id" in fields_set:
        blob_updates["target_workflow_version_id"] = body.target_workflow_version_id
    if "input_mapping" in fields_set and body.input_mapping is not None:
        blob_updates["input_mapping"] = body.input_mapping
    if "source_config" in fields_set:
        blob_updates["source_config"] = body.source_config
    if body.source_type is not None:
        blob_updates["source_type"] = body.source_type

    next_state = body.state if body.state is not None else existing.state
    merged = existing.model_copy(update={**blob_updates, "state": next_state, "updated_at": _now()})
    validate_source_config(
        merged.source_type.value,
        merged.source_config,
        required=merged.state is SubscriptionState.ACTIVE,
    )
    if "selector" in fields_set or merged.source_type != existing.source_type:
        _validate_selector(
            evaluator,
            merged.selector,
            subscription_id=subscription_id,
            source_type=merged.source_type,
        )
    source_type_changed = merged.source_type != existing.source_type
    if (
        merged.source_type in EVENT_MAPPED_SOURCES
        and (
            "input_mapping" in fields_set
            or source_type_changed
            or body.state is SubscriptionState.ACTIVE
        )
    ) or (source_type_changed and existing.source_type in EVENT_MAPPED_SOURCES):
        validate_input_mapping(
            merged.input_mapping,
            evaluator=evaluator,
            subscription_id=subscription_id,
            source_type=merged.source_type,
        )
    if not blob_updates and (body.state is None or body.state is existing.state):
        return existing
    source_changed = (
        existing.source_config != merged.source_config or existing.source_type != merged.source_type
    )
    if merged.source_type.value not in ("manual", "internal") and (
        source_changed or merged.state is SubscriptionState.ACTIVE
    ):
        require_receiver_configuration(receiver_enabled)
    if (
        merged.source_config is not None
        and merged.source_config.connector_instance_id is not None
        and (
            existing.source_config != merged.source_config
            or existing.source_type != merged.source_type
            or (merged.state is SubscriptionState.ACTIVE and existing.state is not merged.state)
        )
    ):
        await validate_connector_reference(
            providers.connector_references, workspace_id, merged.source_config.connector_instance_id
        )
    return await store.update(
        merged,
        expected_revision=existing.store_revision,
        preserve_state=body.state is None,
        metadata_changed=bool(blob_updates),
        reset_schedule=(
            existing.source_config != merged.source_config
            or existing.source_type != merged.source_type
        ),
    )


@router.delete("/{subscription_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_subscription(
    workspace_id: _WorkspacePath,
    subscription_id: _SubscriptionPath,
    store: StoreDep,
    ctx: Annotated[CallContext, Depends(require_permission(PERM_DELETE))],
) -> Response:
    """Soft-delete a subscription by transitioning it to ``expired``.

    The locked SPL surface has no row-delete; ``expired`` is the terminal
    lifecycle state and stops the subscription from matching any future event.
    """
    _ensure_workspace(ctx, workspace_id)
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
    ctx: Annotated[CallContext, Depends(require_permission(PERM_FIRE))],
) -> ManualFireResult | JSONResponse:
    """Fire a subscription now: normalize -> match -> dispatch -> ``{runId}``."""
    _ensure_workspace(ctx, workspace_id)
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

    match = matches[0]
    if subscription.source_type in EVENT_MAPPED_SOURCES:
        match = StartMatch(
            subscription.model_copy(
                update={
                    "input_mapping": resolve_input_mapping(
                        subscription.input_mapping,
                        event,
                        evaluator=evaluator,
                        subscription_id=subscription_id,
                        source_type=subscription.source_type,
                    )
                }
            )
        )
    outcome = await dispatcher.dispatch_start(event, match)
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
