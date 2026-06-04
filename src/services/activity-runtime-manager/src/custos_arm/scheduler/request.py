"""The Scheduler-facing ``ScheduleActivity`` request.

The RPC Adapter (ARM-IMPL-018) builds this from the verified call context and
the Workflow Service payload; the Scheduler consumes it as the single input to
one attempt. It carries the idempotency triple (:attr:`step`), the activity to
run, the structured ``inputs`` payload, the credential-free connector handles
exposed to ``ctx.json``, and the pre-resolved connector ``contexts`` whose
secrets are mounted into the sandbox.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from custos_arm.contract import ConnectorRef, StepRef
from custos_arm.limit import ResourceOverride
from custos_arm.secrets import ConnectorContext


@dataclass(frozen=True, slots=True)
class ScheduleRequest:
    """One ``ScheduleActivity`` request resolved to its Scheduler inputs.

    :param workspace_id: The tenant the attempt runs under.
    :param step: The ``(runId, stepId, attempt)`` idempotency triple.
    :param activity_ref: The fully-qualified activity reference to resolve.
    :param inputs: The structured ``inputs.json`` payload.
    :param connectors: Credential-free connector handles exposed via ``ctx.json``.
    :param connector_contexts: Pre-resolved connector bindings whose secrets the
        Secret Injector materializes into the sandbox input tree.
    :param step_deadline: Optional orchestrator deadline; clamps the attempt
        deadline below the manifest timeout.
    :param override: Optional per-step resource/isolation tuning.
    :param cluster_ceiling: Optional operator-policy absolute resource cap.
    """

    workspace_id: str
    step: StepRef
    activity_ref: str
    inputs: Mapping[str, Any] = field(default_factory=dict)
    connectors: Mapping[str, ConnectorRef] = field(default_factory=dict)
    connector_contexts: tuple[ConnectorContext, ...] = ()
    step_deadline: datetime | None = None
    override: ResourceOverride | None = None
    cluster_ceiling: ResourceOverride | None = None


__all__ = ["ScheduleRequest"]
