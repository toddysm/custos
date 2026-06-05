"""FastAPI dependency helpers for the SPL provider bundle (TS-IMPL-008).

The lifespan in :mod:`custos_trigger.app` stashes the :class:`Providers`
bundle on ``app.state.providers``; these helpers surface it (and the
metadata store within) to request handlers introduced by the REST/RPC
phases (TS-IMPL-015..018), mirroring the auth-service convention.
"""

from __future__ import annotations

from typing import Annotated, cast

from fastapi import Depends, Request

from custos_trigger.pipeline.dispatch import AuditSink, Dispatcher
from custos_trigger.providers import Providers
from custos_trigger.selector import SelectorEvaluator
from custos_trigger.stores import ResumeSubscriptionStore, SubscriptionStore
from custos_trigger.stores.base import TriggerMetadataStore

__all__ = [
    "get_audit_sink",
    "get_dispatcher",
    "get_metadata_store",
    "get_providers",
    "get_resume_default_ttl_seconds",
    "get_resume_subscription_store",
    "get_selector_evaluator",
    "get_subscription_store",
]


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
