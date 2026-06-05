"""Versioned REST + RPC routers for the Trigger Service public surface."""

from __future__ import annotations

from custos_trigger.api.routes.rpc import router as resume_rpc_router
from custos_trigger.api.routes.subscriptions import router as subscriptions_router

__all__ = ["resume_rpc_router", "subscriptions_router"]
