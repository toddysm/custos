"""Subscription wire/domain models + mapping onto the SPL persistence rows.

This module is the typed surface the REST + RPC layers (TS-IMPL-015..018)
construct and validate against, and the translation seam onto the
contract-locked storage-provider-layer (SPL) dataclasses
(:mod:`custos_spl.interfaces.metadata_store`).

The SPL ``Subscription`` row is intentionally minimal (identity + workflow +
state + timestamps). The richer Trigger Service metadata — ``kind``,
``sourceType``, ``targetWorkflowVersionId``, ``inputMapping``, and the CEL
``selector`` — is carried in the free-form ``SubscriptionSelector.selector``
JSON blob, preserving the locked SPL v1 schema. Resume tokens map onto
``ResumeSubscription`` with their extension fields in ``payload``; schedules
and dedup rows map onto ``Schedule`` / ``DedupKey`` builders. See design
``§ Data Models`` and the selector-cel-parity change record
``changes/2026-06-04-006-selector-cel-parity.md``.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any, Final

from custos_spl.ids import RunId, StepId, SubscriptionId, WorkflowId, WorkspaceId
from custos_spl.interfaces.metadata_store import (
    DedupKey as SplDedupKey,
)
from custos_spl.interfaces.metadata_store import (
    ResumeSubscription as SplResumeSubscription,
)
from custos_spl.interfaces.metadata_store import (
    Schedule as SplSchedule,
)
from custos_spl.interfaces.metadata_store import (
    Subscription as SplSubscription,
)
from custos_spl.interfaces.metadata_store import (
    SubscriptionSelector as SplSubscriptionSelector,
)
from pydantic import Field

from custos_trigger._wire import WireModel


class SubscriptionKind(StrEnum):
    """Whether a subscription starts a new run or resumes a waiting step."""

    START = "start"
    RESUME = "resume"


class SourceType(StrEnum):
    """The origin class of the events a subscription is driven by."""

    MANUAL = "manual"
    SCHEDULED = "scheduled"
    WEBHOOK = "webhook"
    VENDOR_PUSH = "vendor-push"
    PULL = "pull"
    INTERNAL = "internal"


class SubscriptionState(StrEnum):
    """Lifecycle state of a subscription."""

    ACTIVE = "active"
    PAUSED = "paused"
    EXPIRED = "expired"


class SelectorMatchType(StrEnum):
    """The locked SPL ``SubscriptionSelector`` match-type enum.

    ``cel`` is the canonical persisted form; the legacy ``eq|prefix|regex|
    jsonpath`` sugar is accepted at the API and lowered to ``cel`` before
    storage (design ``§ Selector Language``).
    """

    CEL = "cel"
    EQ = "eq"
    PREFIX = "prefix"
    REGEX = "regex"
    JSONPATH = "jsonpath"


#: Keys the Trigger Service stamps into the free-form
#: ``SubscriptionSelector.selector`` blob to carry the metadata the minimal SPL
#: ``Subscription`` row has no column for.
_BLOB_KIND: Final[str] = "kind"
_BLOB_SOURCE_TYPE: Final[str] = "sourceType"
_BLOB_TARGET_VERSION: Final[str] = "targetWorkflowVersionId"
_BLOB_INPUT_MAPPING: Final[str] = "inputMapping"
_BLOB_MATCH_TYPE: Final[str] = "matchType"
_BLOB_VALUE: Final[str] = "value"
_BLOB_FIELD_PATH: Final[str] = "fieldPath"


class SubscriptionCreate(WireModel):
    """Request body for ``POST /v1/workspaces/{ws}/triggers``.

    Creates a *start* subscription (manual, scheduled, webhook, vendor-push,
    pull, internal). ``selector`` carries a CEL boolean expression (or the
    legacy sugar lowered to CEL before storage); ``inputMapping`` carries the
    ``${{ … }}`` placeholder map handed to the started run.
    """

    source_type: SourceType
    workflow_id: str = Field(..., min_length=1)
    target_workflow_version_id: str | None = None
    selector: str | None = None
    input_mapping: dict[str, Any] = Field(default_factory=dict)


class SubscriptionPatch(WireModel):
    """Request body for ``PATCH /v1/workspaces/{ws}/triggers/{id}``.

    Every field is optional — only the supplied fields are updated. ``state``
    drives the active/paused/expired transitions; ``selector`` and
    ``inputMapping`` re-author the match and mapping.
    """

    state: SubscriptionState | None = None
    selector: str | None = None
    input_mapping: dict[str, Any] | None = None
    target_workflow_version_id: str | None = None


class ManualFireRequest(WireModel):
    """Request body for ``POST /v1/workspaces/{ws}/triggers/{id}:fire``.

    ``inputs`` is the optional payload handed to the started run; it becomes
    the normalized event ``data`` (and feeds the deterministic dedup key).
    """

    inputs: dict[str, Any] = Field(default_factory=dict)


class ManualFireResult(WireModel):
    """Response body for a successful manual ``:fire`` \u2014 the started run id."""

    run_id: str = Field(..., min_length=1)


class Subscription(WireModel):
    """The full subscription domain/response model.

    Round-trips through :func:`to_spl_subscription` +
    :func:`to_spl_subscription_selector` for persistence and
    :func:`subscription_from_spl` for reads.
    """

    workspace_id: str = Field(..., min_length=1)
    subscription_id: str = Field(..., min_length=1)
    kind: SubscriptionKind = SubscriptionKind.START
    source_type: SourceType
    workflow_id: str = Field(..., min_length=1)
    target_workflow_version_id: str | None = None
    selector: str | None = None
    input_mapping: dict[str, Any] = Field(default_factory=dict)
    state: SubscriptionState = SubscriptionState.ACTIVE
    created_at: datetime
    updated_at: datetime


class ResumeRegistration(WireModel):
    """A step-resume wait registered via ``RegisterResumeSubscription``.

    The ``(run_id, step_id, event_key)`` triple is the idempotency key; an
    optional CEL ``selector`` narrows which event satisfies the wait
    (``None`` = match on event key alone). Round-trips through
    :func:`to_spl_resume_subscription` / :func:`resume_registration_from_spl`.
    """

    run_id: str = Field(..., min_length=1)
    step_id: str = Field(..., min_length=1)
    event_key: str = Field(..., min_length=1)
    selector: str | None = None


#: Keys the Trigger Service stamps into the free-form
#: ``ResumeSubscription.payload`` blob.
_PAYLOAD_EVENT_KEY: Final[str] = "eventKey"
_PAYLOAD_SELECTOR: Final[str] = "selector"


def build_selector_blob(sub: Subscription) -> dict[str, Any]:
    """Encode a :class:`Subscription`'s metadata into the SPL selector blob.

    Produces the single ``SubscriptionSelector.selector`` mapping that carries
    the rich fields plus the locked ``matchType``/``value``/``fieldPath``
    triple (``matchType="cel"``, ``fieldPath=""``).
    """
    return {
        _BLOB_KIND: sub.kind.value,
        _BLOB_SOURCE_TYPE: sub.source_type.value,
        _BLOB_TARGET_VERSION: sub.target_workflow_version_id,
        _BLOB_INPUT_MAPPING: dict(sub.input_mapping),
        _BLOB_MATCH_TYPE: SelectorMatchType.CEL.value,
        _BLOB_VALUE: sub.selector or "",
        _BLOB_FIELD_PATH: "",
    }


def to_spl_subscription(sub: Subscription) -> SplSubscription:
    """Map a :class:`Subscription` onto the minimal SPL ``Subscription`` row."""
    return SplSubscription(
        workspace_id=WorkspaceId(sub.workspace_id),
        subscription_id=SubscriptionId(sub.subscription_id),
        workflow_id=WorkflowId(sub.workflow_id),
        state=sub.state.value,
        created_at=sub.created_at,
        updated_at=sub.updated_at,
    )


def to_spl_subscription_selector(
    sub: Subscription, *, added_at: datetime
) -> SplSubscriptionSelector:
    """Map a :class:`Subscription`'s metadata onto an SPL selector row."""
    return SplSubscriptionSelector(
        workspace_id=WorkspaceId(sub.workspace_id),
        subscription_id=SubscriptionId(sub.subscription_id),
        selector=build_selector_blob(sub),
        added_at=added_at,
    )


def subscription_from_spl(
    row: SplSubscription, selector: SplSubscriptionSelector | None
) -> Subscription:
    """Rebuild a :class:`Subscription` from its SPL row + latest selector blob.

    Missing blob keys fall back to the model defaults so a row written by an
    older code path still reads cleanly.
    """
    blob: dict[str, Any] = dict(selector.selector) if selector is not None else {}
    return Subscription(
        workspace_id=str(row.workspace_id),
        subscription_id=str(row.subscription_id),
        kind=SubscriptionKind(blob.get(_BLOB_KIND, SubscriptionKind.START.value)),
        source_type=SourceType(blob.get(_BLOB_SOURCE_TYPE, SourceType.MANUAL.value)),
        workflow_id=str(row.workflow_id),
        target_workflow_version_id=blob.get(_BLOB_TARGET_VERSION),
        selector=(blob.get(_BLOB_VALUE) or None),
        input_mapping=dict(blob.get(_BLOB_INPUT_MAPPING) or {}),
        state=SubscriptionState(row.state),
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def to_spl_resume_subscription(
    reg: ResumeRegistration,
    *,
    workspace_id: str,
    resume_id: str,
    expires_at: datetime,
) -> SplResumeSubscription:
    """Map a :class:`ResumeRegistration` onto the SPL ``ResumeSubscription`` row.

    The ``eventKey`` and optional CEL ``selector`` are carried in ``payload``,
    the free-form extension point the minimal row provides.
    """
    return SplResumeSubscription(
        workspace_id=WorkspaceId(workspace_id),
        resume_id=resume_id,
        run_id=RunId(reg.run_id),
        step_id=StepId(reg.step_id),
        expires_at=expires_at,
        payload={
            _PAYLOAD_EVENT_KEY: reg.event_key,
            _PAYLOAD_SELECTOR: reg.selector,
        },
    )


def resume_registration_from_spl(row: SplResumeSubscription) -> ResumeRegistration:
    """Rebuild a :class:`ResumeRegistration` from its SPL row."""
    payload: dict[str, Any] = dict(row.payload)
    return ResumeRegistration(
        run_id=str(row.run_id),
        step_id=str(row.step_id),
        event_key=str(payload.get(_PAYLOAD_EVENT_KEY, "")),
        selector=payload.get(_PAYLOAD_SELECTOR),
    )


def to_spl_dedup_key(*, workspace_id: str, key: str, expires_at: datetime) -> SplDedupKey:
    """Build an SPL ``DedupKey`` row from its primitive parts."""
    return SplDedupKey(
        workspace_id=WorkspaceId(workspace_id),
        key=key,
        expires_at=expires_at,
    )


def to_spl_schedule(
    *,
    workspace_id: str,
    schedule_id: str,
    workflow_id: str,
    cron: str,
    next_fire_at: datetime,
    enabled: bool = True,
) -> SplSchedule:
    """Build an SPL ``Schedule`` row from its primitive parts."""
    return SplSchedule(
        workspace_id=WorkspaceId(workspace_id),
        schedule_id=schedule_id,
        workflow_id=WorkflowId(workflow_id),
        cron=cron,
        next_fire_at=next_fire_at,
        enabled=enabled,
    )


__all__ = [
    "ManualFireRequest",
    "ManualFireResult",
    "ResumeRegistration",
    "SelectorMatchType",
    "SourceType",
    "Subscription",
    "SubscriptionCreate",
    "SubscriptionKind",
    "SubscriptionPatch",
    "SubscriptionState",
    "build_selector_blob",
    "resume_registration_from_spl",
    "subscription_from_spl",
    "to_spl_dedup_key",
    "to_spl_resume_subscription",
    "to_spl_schedule",
    "to_spl_subscription",
    "to_spl_subscription_selector",
]
