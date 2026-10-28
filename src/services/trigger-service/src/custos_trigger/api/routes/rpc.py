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

These are *internal* service-to-service calls intended for the Dapr mesh,
not through the public call-context
envelope: the Workflow Service ``DaprTriggerServiceClient`` propagates no
``x-custos-callctx`` header on the Dapr invoke. The call-context middleware
therefore bypasses both method paths (see
:mod:`custos_trigger.middleware.callctx`).

Scoped calls additionally require an explicitly configured Workflow-principal
verifier before tenant access. Mesh/caller headers alone are not authentication;
the default factory fails closed until production authentication is wired.

Legacy internal registrations use the reserved :data:`RESUME_WORKSPACE`
partition. External registrations carry the owning run's workspace and an
immutable connector/delivery association; cancellation must use that workspace.

The Dapr sidecar forwards ``…/v1.0/invoke/<trigger-app-id>/method/<Method>``
to the app at ``POST /<Method>``, so the route paths are the bare method
names (no ``/v1/workspaces/...`` prefix).
"""

from __future__ import annotations

import hashlib
import re
from datetime import UTC, datetime, timedelta
from typing import Annotated, Final

from custos_spl.interfaces.trigger_source import parse_resume_duration
from fastapi import APIRouter, Depends, Request, Response, status

from custos_trigger.dependencies import (
    WorkflowCallerVerifier,
    get_audit_sink,
    get_providers,
    get_receiver_configuration_enabled,
    get_resume_default_ttl_seconds,
    get_resume_subscription_store,
    get_selector_evaluator,
    get_workflow_caller_verifier,
    require_workflow_caller,
)
from custos_trigger.errors import TriggerError, TriggerErrorKind
from custos_trigger.models import (
    CancelResumeRequest,
    RegisterResumeRequest,
    RegisterResumeResponse,
    ResumeRegistration,
)
from custos_trigger.pipeline.dispatch import AuditSink
from custos_trigger.providers import Providers
from custos_trigger.selector import SelectorEvaluator
from custos_trigger.source_config import (
    SourceConfigurationError,
    require_receiver_configuration,
    validate_connector_reference,
)
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

#: Reserved partition for legacy internal registrations without a source.
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
ProvidersDep = Annotated[Providers, Depends(get_providers)]
ReceiverEnabledDep = Annotated[bool, Depends(get_receiver_configuration_enabled)]
WorkflowVerifierDep = Annotated[
    WorkflowCallerVerifier | None, Depends(get_workflow_caller_verifier)
]


async def _reject_source_rebinding(
    original: ResumeRegistration,
    body: RegisterResumeRequest,
    audit: AuditSink,
    *,
    workspace_id: str,
    resume_id: str,
) -> None:
    """Audit and reject a different source while leaving the persisted association untouched."""
    if original.source == body.source:
        return
    await audit.emit(
        AUDIT_RESUME_DIVERGENT,
        workspace_id=workspace_id,
        attributes={
            "resumeId": resume_id,
            "runId": body.run_id,
            "stepId": body.step_id,
            "eventKey": body.event_key,
            "reason": "source_rebinding",
            "originalSource": original.source.to_source().to_wire() if original.source else None,
            "replaySource": body.source.to_source().to_wire() if body.source else None,
        },
    )
    raise TriggerError(
        TriggerErrorKind.RESUME_DIVERGENT,
        "the source association of an existing resume key cannot be changed",
        details={"resumeId": resume_id},
    )


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
    r"(?:(?P<seconds>\d+(?:\.\d+)?)S)?"
    r")?$"
)


def _parse_iso8601_duration(value: str) -> timedelta | None:
    """Parse a positive duration with the same microsecond rounding as Workflow.

    Returns ``None`` for an empty match
    (e.g. bare ``P`` / ``PT``) so the caller falls back to the default TTL.
    """
    match = _ISO8601_DURATION_RE.fullmatch(value.strip())
    if match is None:
        return None
    parts = match.groupdict()
    if not any(parts.values()):
        return None
    try:
        weeks = int(parts["weeks"] or 0)
        days = int(parts["days"] or 0)
        hours = int(parts["hours"] or 0)
        minutes = int(parts["minutes"] or 0)
        seconds = float(parts["seconds"] or 0)
    except ValueError as exc:
        raise SourceConfigurationError("resume TTL exceeds the supported numeric range") from exc
    if weeks == days == hours == minutes == seconds == 0:
        return None
    try:
        # Retain the legacy combined week/day syntax while sharing strict
        # precision/range validation with published Workflow waits.
        normalized = f"P{weeks * 7 + days}DT{hours}H{minutes}M{parts['seconds'] or '0'}S"
        return parse_resume_duration(normalized)
    except ValueError as exc:
        raise SourceConfigurationError(f"invalid resume TTL: {exc}") from exc


def _resolve_ttl(ttl: str | None, default_seconds: int) -> timedelta:
    """Resolve the request TTL with Workflow-compatible subsecond precision.

    An absent, blank, unparseable, or non-positive ``ttl`` yields the
    configured ``TRIGGER_RESUME_DEFAULT_TTL_SECONDS`` default.
    """
    if ttl:
        parsed = _parse_iso8601_duration(ttl)
        if parsed is not None:
            return parsed
    return timedelta(seconds=default_seconds)


@router.post(
    REGISTER_RESUME_PATH,
    status_code=status.HTTP_200_OK,
    summary="Register (or idempotently re-register) a resume subscription.",
)
async def register_resume_subscription(
    request: Request,
    body: RegisterResumeRequest,
    store: ResumeStoreDep,
    evaluator: EvaluatorDep,
    audit: AuditSinkDep,
    default_ttl_seconds: DefaultTtlDep,
    providers: ProvidersDep,
    receiver_enabled: ReceiverEnabledDep,
    workflow_verifier: WorkflowVerifierDep,
) -> RegisterResumeResponse:
    """Register a one-shot resume wait, idempotent on the request triple.

    Re-registering a still-live ``(runId, stepId, eventKey)`` returns the
    existing ``subscriptionId`` without a write; a divergent ``selector`` keeps
    the original registration and emits a ``resume.subscription.divergent``
    audit. A registration whose prior row has lapsed past its TTL is treated as
    a fresh registration by an atomic provider operation. Concurrent requests
    return the same live winner, including during expiry refresh. The CEL
    ``selector`` is compiled up front on every
    path (including an idempotent replay) so a malformed expression is rejected
    (422) rather than silently never matching.
    """
    if body.source is not None:
        await require_workflow_caller(request, workflow_verifier)
        require_receiver_configuration(receiver_enabled)
    if body.selector:
        # Compiled before the idempotency lookup so a malformed selector is
        # rejected on every path, replay included. Raises SelectorInvalidError
        # (TriggerError, kind selector_invalid) -> 422 Problem+JSON.
        evaluator.compile(body.selector, subscription_id="resume-register")

    resume_id = compute_resume_id(body.run_id, body.step_id, body.event_key)
    workspace_id = body.workspace_id if body.workspace_id is not None else RESUME_WORKSPACE
    existing = await store.get(workspace_id, resume_id)
    now = _now()

    if existing is not None:
        await _reject_source_rebinding(
            existing.registration, body, audit, workspace_id=workspace_id, resume_id=resume_id
        )
    if existing is None or existing.expires_at <= now:
        if body.source is not None:
            await validate_connector_reference(
                providers.connector_references, workspace_id, body.source.connector_instance_id
            )
        ttl = _resolve_ttl(body.ttl, default_ttl_seconds)
        try:
            expires_at = now + ttl
        except OverflowError as exc:
            raise SourceConfigurationError(
                "resume TTL exceeds the supported timestamp range"
            ) from exc
        existing = await store.register_or_get(
            ResumeRegistration(
                run_id=body.run_id,
                step_id=body.step_id,
                event_key=body.event_key,
                selector=body.selector,
                workspace_id=body.workspace_id,
                source=body.source,
            ),
            workspace_id=workspace_id,
            resume_id=resume_id,
            expires_at=expires_at,
            as_of=now,
        )
        await _reject_source_rebinding(
            existing.registration, body, audit, workspace_id=workspace_id, resume_id=resume_id
        )
    if existing.registration.selector != body.selector:
        await audit.emit(
            AUDIT_RESUME_DIVERGENT,
            workspace_id=workspace_id,
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


@router.post(
    CANCEL_RESUME_PATH,
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Cancel a resume subscription (idempotent no-op on unknown keys).",
)
async def cancel_resume_subscription(
    request: Request,
    body: CancelResumeRequest,
    store: ResumeStoreDep,
    workflow_verifier: WorkflowVerifierDep,
) -> Response:
    """Cancel an open resume wait; a clean no-op for an unknown/expired key.

    The store delete is idempotent (deleting an absent id is a no-op), so a
    cancel always succeeds with ``204 No Content`` — the Workflow Service
    treats any 2xx (and 404/409) as a clean no-op.
    """
    if body.workspace_id is not None:
        await require_workflow_caller(request, workflow_verifier)
    resume_id = compute_resume_id(body.run_id, body.step_id, body.event_key)
    await store.cancel(
        body.workspace_id if body.workspace_id is not None else RESUME_WORKSPACE, resume_id
    )
    return Response(status_code=status.HTTP_204_NO_CONTENT)
