"""Resource Limiter (ARM-IMPL-008).

Computes the effective resource envelope and selects the isolation tier /
``RuntimeClass`` for an activity attempt from the layered
manifest → step-override → platform-default → cluster-ceiling hierarchy.
"""

from __future__ import annotations

from custos_arm.limit.errors import (
    LimitError,
    ResourceLimitError,
    RuntimeUnavailableError,
)
from custos_arm.limit.limiter import ResourceLimiter
from custos_arm.limit.models import EffectiveResources, ResourceOverride
from custos_arm.limit.quantity import Quantity

__all__ = [
    "EffectiveResources",
    "LimitError",
    "Quantity",
    "ResourceLimitError",
    "ResourceLimiter",
    "ResourceOverride",
    "RuntimeUnavailableError",
]
