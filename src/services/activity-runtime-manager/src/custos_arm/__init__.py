"""Custos Activity Runtime Manager (COMP-006).

The Activity Runtime Manager (ARM) schedules, isolates, and runs workflow
activities as OCI containers, brokering their typed I/O, injecting
short-lived secrets, and mapping process outcomes back to the locked
``ActivityResultEnvelope`` contract the Workflow Service consumes.

ARM-IMPL-001 scaffolds the service package and exposes the FastAPI
application factory plus the ``/healthz`` / ``/readyz`` probes. Later
ARM-IMPL-* tasks extend the lifespan and the API/RPC surface without
touching the public re-export below.
"""

from __future__ import annotations

from custos_arm.app import create_app

__all__ = ["create_app"]
