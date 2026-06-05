"""Internal resume-subscription RPCs (TS-IMPL-016, REQ-081).

Dapr Service-Invocation method routes the Workflow Service calls across each
``waitFor:`` step's lifecycle:

* ``POST /RegisterResumeSubscription`` — register (or idempotently
  re-register) a one-shot resume wait keyed on the ``(runId, stepId,
  eventKey)`` triple; a replay returns the existing ``subscriptionId`` rather
  than minting a duplicate. On a divergent ``selector`` the *original wins*
  and a ``resume.subscription.divergent`` audit event is emitted.
* ``POST /CancelResumeSubscription`` — cancel an open wait; a clean no-op for
  an unknown / already-expired key.

These are *internal* service-to-service calls authenticated at the Dapr mesh
layer (mTLS + app-id allow-list), not through the public call-context
envelope: the Workflow Service ``DaprTriggerServiceClient`` propagates no
``x-custos-callctx`` header on the Dapr invoke. The call-context middleware
therefore bypasses both method paths (see
:mod:`custos_trigger.middleware.callctx`).

A resume registration carries no workspace of its own — the
``(runId, stepId, eventKey)`` triple is globally unique because ``runId`` is a
Workflow Service-owned global id — so every resume row is partitioned under
the reserved :data:`RESUME_WORKSPACE` sentinel. The dispatch back to the
Workflow Service (TS-IMPL-017) carries the tenant workspace from the inbound
event instead.

The Dapr sidecar forwards ``…/v1.0/invoke/<trigger-app-id>/method/<Method>``
to the app at ``POST /<Method>``, so the route paths are the bare method
names (no ``/v1/workspaces/...`` prefix).
"""

from __future__ import annotations

import hashlib
import re
from datetime import UTC, datetime, timedelta
from typing import Annotated, Final

from fastapi import APIRouter, Depends, Response, status

from custos_trigger.dependencies import (
    get_audit_sink,
    get_resume_default_ttl_seconds,
    get_resume_subscription_store,
    get_selector_evaluator,
)
from custos_trigger.models import (
    CancelResumeRequest,
    RegisterResumeRequest,
    RegisterResumeResponse,
    ResumeRegistration,
)
from custos_trigger.pipeline.dispatch import AuditSink
from custos_trigger.selector import SelectorEvaluator
from custos_trigger.stores import ResumeSubscriptionStore

__all__ = [
    "AUDIT_RESUME_DIVERGENT",
    "CANCEL_RESUME_PATH",
    "REGISTER_RESUME_PATH",
    "RESUME_WORKSPACE",
    "compute_resume_id",
    "router",
]

#: Dapr method paths the Workflow Service invokes. The call-context middleware
#: bypass set (``custos_trigger.middleware.callctx._BYPASS_PATHS``) must list
#: these verbatim — internal Dapr invokes carry no call-context header.
REGISTER_RESUME_PATH: Final[str] = "/RegisterResumeSubscription"
CANCEL_RESUME_PATH: Final[str] = "/CancelResumeSubscription"

#: Reserved partition for resume rows. A resume registration has no workspace;
#: the globally-unique ``(runId, stepId, eventKey)`` triple keys it instead.
RESUME_WORKSPACE: Final[str] = "_resume"

#: Audit event emitted when a replay registers a divergent selector for an
#: already-live wait (design ``§ Resume Subscription Replay Protocol`` —
#: *original wins*).
AUDIT_RESUME_DIVERGENT: Final[str] = "resume.subscription.divergent"

router = APIRouter(tags=["resume-rpc"])

ResumeStoreDep = Annotated[ResumeSubscriptionStore, Depends(get_resume_subscription_store)]
EvaluatorDep = Annotated[SelectorEvaluator, Depends(get_selector_evaluator)]
AuditSinkDep = Annotated[AuditSink, Depends(get_audit_sink)]
DefaultTtlDep = Annotated[int, Depends(get_resume_default_ttl_seconds)]


def _now() -> datetime:
    return datetime.now(UTC)


def compute_resume_id(run_id: str, step_id: str, event_key: str) -> str:
    """Derive the deterministic store id for a resume idempotency triple.

    The same ``(run_id, step_id, event_key)`` always maps to the same id, so a
    re-registration finds the existing row and returns its ``subscriptionId``.
    Components are NUL-joined before hashing so no concatenation collision can
    forge a different triple's id.
    """
    digest = hashlib.sha256("\x00".join((run_id, step_id, event_key)).encode("utf-8"))
    return f"res_{digest.hexdigest()}"


#: ISO-8601 duration grammar limited to the week/day/hour/minute/second
#: components the Workflow Service emits (e.g. ``PT24H``, ``P7D``). Year/month
#: components are intentionally unsupported (variable length); such a value
#: falls back to the configured default TTL.
_ISO8601_DURATION_RE: Final[re.Pattern[str]] = re.compile(
    r"^P"
    r"(?:(?P<weeks>\d+)W)?"
    r"(?:(?P<days>\d+)D)?"
    r"(?:T"
    r"(?:(?P<hours>\d+)H)?"
    r"(?:(?P<minutes>\d+)M)?"
    r"(?:(?P<seconds>\d+)(?:\.\d+)?S)?"
    r")?$"
)


def _parse_iso8601_duration_seconds(value: str) -> int | None:
    """Parse an ISO-8601 duration to whole seconds, or ``None`` if malformed.

    Fractional seconds are truncated. Returns ``None`` for an empty match
    (e.g. bare ``P`` / ``PT``) so the caller falls back to the default TTL.
    """
    match = _ISO8601_DURATION_RE.fullmatch(value.strip())
    if match is None:
        return None
    parts = match.groupdict()
    if not any(parts.values()):
        return None
    weeks = int(parts["weeks"] or 0)
    days = int(parts["days"] or 0)
    hours = int(parts["hours"] or 0)
    minutes = int(parts["minutes"] or 0)
    seconds = int(parts["seconds"] or 0)
    return ((((weeks * 7 + days) * 24 + hours) * 60 + minutes) * 60) + seconds


def _resolve_ttl_seconds(ttl: str | None, default_seconds: int) -> int:
    """Resolve the request TTL to whole seconds, falling back to the default.

    An absent, blank, unparseable, or non-positive ``ttl`` yields the
    configured ``TRIGGER_RESUME_DEFAULT_TTL_SECONDS`` default.
    """
    if ttl:
        parsed = _parse_iso8601_duration_seconds(ttl)
        if parsed is not None and parsed > 0:
            return parsed
    return default_seconds


@router.post(
    REGISTER_RESUME_PATH,
    status_code=status.HTTP_200_OK,
    summary="Register (or idempotently re-register) a resume subscription.",
)
async def register_resume_subscription(
    body: RegisterResumeRequest,
    store: ResumeStoreDep,
    evaluator: EvaluatorDep,
    audit: AuditSinkDep,
    default_ttl_seconds: DefaultTtlDep,
) -> RegisterResumeResponse:
    """Register a one-shot resume wait, idempotent on the request triple.

    Re-registering a still-live ``(runId, stepId, eventKey)`` returns the
    existing ``subscriptionId`` without a write; a divergent ``selector`` keeps
    the original registration and emits a ``resume.subscription.divergent``
    audit. A registration whose prior row has lapsed past its TTL is treated as
    a fresh registration (the stale row is dropped first so the immutable store
    accepts the re-put). The CEL ``selector`` is compiled at register time so a
    malformed expression is rejected (422) rather than silently never matching.
    """
    resume_id = compute_resume_id(body.run_id, body.step_id, body.event_key)
    existing = await store.get(RESUME_WORKSPACE, resume_id)
    now = _now()

    if existing is not None and existing.expires_at > now:
        # Idempotent replay of a still-live wait. Original wins on divergence.
        if existing.registration.selector != body.selector:
            await audit.emit(
                AUDIT_RESUME_DIVERGENT,
                workspace_id=RESUME_WORKSPACE,
                attributes={
                    "resumeId": resume_id,
                    "runId": body.run_id,
                    "stepId": body.step_id,
                    "eventKey": body.event_key,
                    "originalSelector": existing.registration.selector,
                    "replaySelector": body.selector,
                },
            )
        return RegisterResumeResponse(subscription_id=resume_id)

    if existing is not None:
        # Prior row lapsed past its TTL — drop it so the re-put is accepted.
        await store.cancel(RESUME_WORKSPACE, resume_id)

    if body.selector:
        # Raises SelectorInvalidError (TriggerError, kind selector_invalid) ->
        # rendered as a 422 Problem+JSON by the registered exception handler.
        evaluator.compile(body.selector, subscription_id=resume_id)

    ttl_seconds = _resolve_ttl_seconds(body.ttl, default_ttl_seconds)
    expires_at = now + timedelta(seconds=ttl_seconds)
    await store.register(
        ResumeRegistration(
            run_id=body.run_id,
            step_id=body.step_id,
            event_key=body.event_key,
            selector=body.selector,
        ),
        workspace_id=RESUME_WORKSPACE,
        resume_id=resume_id,
        expires_at=expires_at,
    )
    return RegisterResumeResponse(subscription_id=resume_id)


@router.post(
    CANCEL_RESUME_PATH,
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Cancel a resume subscription (idempotent no-op on unknown keys).",
)
async def cancel_resume_subscription(
    body: CancelResumeRequest,
    store: ResumeStoreDep,
) -> Response:
    """Cancel an open resume wait; a clean no-op for an unknown/expired key.

    The store delete is idempotent (deleting an absent id is a no-op), so a
    cancel always succeeds with ``204 No Content`` — the Workflow Service
    treats any 2xx (and 404/409) as a clean no-op.
    """
    resume_id = compute_resume_id(body.run_id, body.step_id, body.event_key)
    await store.cancel(RESUME_WORKSPACE, resume_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
