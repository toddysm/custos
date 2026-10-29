"""FastAPI dependencies for providers, receiver readiness and scoped caller verification.

The lifespan in :mod:`custos_trigger.app` stashes the :class:`Providers`
bundle on ``app.state.providers``; these helpers surface it (and the
metadata store within) to request handlers introduced by the REST/RPC
phases (TS-IMPL-015..018), mirroring the auth-service convention.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Annotated, cast

from fastapi import Depends, Request

from custos_trigger.errors import TriggerError, TriggerErrorKind
from custos_trigger.pipeline.dispatch import AuditSink, Dispatcher
from custos_trigger.providers import Providers
from custos_trigger.selector import SelectorEvaluator
from custos_trigger.stores import ResumeSubscriptionStore, SubscriptionStore
from custos_trigger.stores.base import TriggerMetadataStore

#: Must authenticate the Workflow service on every transport, including direct
#: app-port requests. Return only after verification; raise on any failure.
WorkflowCallerVerifier = Callable[[Request], Awaitable[None]]

__all__ = [
    "WorkflowCallerVerifier",
    "get_audit_sink",
    "get_dispatcher",
    "get_metadata_store",
    "get_providers",
    "get_receiver_configuration_enabled",
    "get_resume_default_ttl_seconds",
    "get_resume_subscription_store",
    "get_selector_evaluator",
    "get_subscription_store",
    "get_workflow_caller_verifier",
    "require_workflow_caller",
]


def get_receiver_configuration_enabled(request: Request) -> bool:
    """Return the explicitly initialized compatibility rollout gate."""
    enabled = getattr(request.app.state, "receiver_configuration_enabled", None)
    if not isinstance(enabled, bool):
        raise RuntimeError("receiver configuration gate is not initialized; did the lifespan run?")
    return enabled


def get_workflow_caller_verifier(request: Request) -> WorkflowCallerVerifier | None:
    """Return the trusted authenticator, never an implicit allow-all implementation."""
    return cast(
        WorkflowCallerVerifier | None,
        getattr(request.app.state, "workflow_caller_verifier", None),
    )


async def require_workflow_caller(
    request: Request, verifier: WorkflowCallerVerifier | None
) -> None:
    """Fail closed before scoped state access until authenticated Workflow access is wired."""
    if getattr(request.state, "workflow_caller_verified", False) is True:
        return
    if verifier is None:
        raise TriggerError(
            TriggerErrorKind.SOURCE_UNAVAILABLE,
            "authenticated Workflow service access is not configured for scoped resume RPCs",
        )
    await verifier(request)


def get_providers(request: Request) -> Providers:
    """Return the :class:`Providers` bundle attached during startup."""
    providers = getattr(request.app.state, "providers", None)
    if providers is None:
        raise RuntimeError("Providers bundle is not attached to app.state; did the lifespan run?")
    return cast(Providers, providers)


def get_metadata_store(
    providers: Annotated[Providers, Depends(get_providers)],
) -> TriggerMetadataStore:
    """Return the Trigger Service metadata store from the providers bundle."""
    return providers.metadata_store


def get_subscription_store(request: Request) -> SubscriptionStore:
    """Return the :class:`SubscriptionStore` attached during startup."""
    store = getattr(request.app.state, "subscription_store", None)
    if store is None:
        raise RuntimeError("SubscriptionStore is not attached to app.state; did the lifespan run?")
    return cast(SubscriptionStore, store)


def get_dispatcher(request: Request) -> Dispatcher:
    """Return the matching/dispatch :class:`Dispatcher` attached during startup."""
    dispatcher = getattr(request.app.state, "dispatcher", None)
    if dispatcher is None:
        raise RuntimeError("Dispatcher is not attached to app.state; did the lifespan run?")
    return cast(Dispatcher, dispatcher)


def get_selector_evaluator(request: Request) -> SelectorEvaluator:
    """Return the shared :class:`SelectorEvaluator` attached during startup."""
    evaluator = getattr(request.app.state, "selector_evaluator", None)
    if evaluator is None:
        raise RuntimeError("SelectorEvaluator is not attached to app.state; did the lifespan run?")
    return cast(SelectorEvaluator, evaluator)


def get_resume_subscription_store(request: Request) -> ResumeSubscriptionStore:
    """Return the :class:`ResumeSubscriptionStore` attached during startup."""
    store = getattr(request.app.state, "resume_subscription_store", None)
    if store is None:
        raise RuntimeError(
            "ResumeSubscriptionStore is not attached to app.state; did the lifespan run?"
        )
    return cast(ResumeSubscriptionStore, store)


def get_audit_sink(request: Request) -> AuditSink:
    """Return the :class:`AuditSink` attached during startup."""
    audit = getattr(request.app.state, "audit_sink", None)
    if audit is None:
        raise RuntimeError("AuditSink is not attached to app.state; did the lifespan run?")
    return cast(AuditSink, audit)


def get_resume_default_ttl_seconds(request: Request) -> int:
    """Return the default resume TTL (seconds) parsed during startup."""
    ttl = getattr(request.app.state, "resume_default_ttl_seconds", None)
    if ttl is None:
        raise RuntimeError(
            "resume_default_ttl_seconds is not attached to app.state; did the lifespan run?"
        )
    return cast(int, ttl)
