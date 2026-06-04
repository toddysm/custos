"""Inbound RPC adapter for the Activity Runtime Manager (ARM-IMPL-018).

Exposes the Dapr Service-Invocation surface (``ScheduleActivity`` /
``CancelActivity``) as a FastAPI router plus the wire models that translate the
Workflow Service envelope into the Scheduler's inputs.
"""

from __future__ import annotations

from .models import CancelActivityWire, ConnectorContextWire, ScheduleActivityWire
from .router import get_scheduler, router

__all__ = [
    "CancelActivityWire",
    "ConnectorContextWire",
    "ScheduleActivityWire",
    "get_scheduler",
    "router",
]
