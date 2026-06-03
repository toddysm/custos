"""Resource Limiter domain models (ARM-IMPL-008).

These are internal domain objects — not wire contracts — so they are plain
frozen dataclasses (no pydantic aliases) per the ARM persistence-model
convention.

* :class:`ResourceOverride` is the optional per-step tuning the Workflow
  Service may attach to a step: a tighter request/limit for any resource and
  an isolation-tier *upgrade*. Every field is optional; an absent field means
  "inherit the layer above".
* :class:`EffectiveResources` is the limiter's output: the fully-resolved
  request/limit envelope (always populated — platform defaults fill any gap),
  the selected isolation :class:`~custos_arm.manifest.IsolationTier`, and the
  concrete cluster ``RuntimeClass`` the Scheduler stamps onto the Pod.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from custos_arm.manifest import IsolationTier

__all__ = [
    "EffectiveResources",
    "ResourceOverride",
]


@dataclass(frozen=True, slots=True)
class ResourceOverride:
    """Optional per-step resource tuning supplied by the Workflow Service.

    Each quantity is a Kubernetes quantity string (``"500m"``, ``"512Mi"``);
    ``min_tier`` may only *raise* the isolation floor. Any ``None`` field
    inherits the resolved value from the layer above (manifest → platform
    default).
    """

    cpu_request: str | None = None
    cpu_limit: str | None = None
    memory_request: str | None = None
    memory_limit: str | None = None
    ephemeral_storage_limit: str | None = None
    min_tier: IsolationTier | None = None


@dataclass(frozen=True, slots=True)
class EffectiveResources:
    """The fully-resolved resource envelope and isolation decision.

    Every request/limit is populated: a value the manifest and step both leave
    silent falls back to the platform default. ``runtime_class`` is the empty
    string when the selected tier maps to the cluster-default runtime
    (``process`` under runc).
    """

    cpu_request: str
    cpu_limit: str
    memory_request: str
    memory_limit: str
    ephemeral_storage_limit: str
    timeout: timedelta
    tier: IsolationTier
    runtime_class: str
