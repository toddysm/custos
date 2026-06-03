"""Resource Limiter — effective envelope + isolation-tier selection (ARM-IMPL-008).

The limiter sits between the resolved :class:`ActivityTypeVersion` and the
Scheduler. It answers two questions for one attempt:

1. **What resource envelope does the Pod run with?** Resolved through the
   layered hierarchy (design § ``spec.resources``)::

       cluster ceiling (operator policy)   ← absolute cap, may not be exceeded
               ↓
       platform default (Custos config)    ← applied when the manifest is silent
               ↓
       manifest spec.resources             ← the activity author's recommendation
               ↓
       step override (workflow per-step)   ← may only tighten, never loosen

   Each request/limit is always populated in the output: a value the manifest
   and step both leave silent falls back to the platform default. A step
   override that *loosens* (exceeds) the manifest/default value, or any value
   that exceeds the cluster ceiling, is a permanent
   :class:`~custos_arm.limit.errors.ResourceLimitError`.

2. **What isolation tier (and concrete ``RuntimeClass``) does it run at?** The
   selected tier is ``max(manifest.minTier, step.minTier)`` — a step may
   *upgrade* the floor but never downgrade below it. The limiter then resolves
   the tier to a cluster ``RuntimeClass`` via :class:`Settings`; an unconfigured
   tier raises :class:`~custos_arm.limit.errors.RuntimeUnavailableError` before
   any sandbox is created (design § No silent downgrade).

The limiter is pure (config + manifest + step override → decision) and performs
no I/O.
"""

from __future__ import annotations

from datetime import timedelta

from custos_arm.config import Settings, parse_iso8601_duration
from custos_arm.limit.errors import ResourceLimitError, RuntimeUnavailableError
from custos_arm.limit.models import EffectiveResources, ResourceOverride
from custos_arm.limit.quantity import Quantity
from custos_arm.manifest import IsolationTier, Resources

__all__ = ["ResourceLimiter"]

#: Tier strength ordering used to pick ``max(manifest, step)`` and to detect a
#: step attempting to downgrade below the manifest floor.
_TIER_RANK: dict[IsolationTier, int] = {
    IsolationTier.PROCESS: 0,
    IsolationTier.VM: 1,
    IsolationTier.MICROVM: 2,
}


class ResourceLimiter:
    """Computes the effective resource envelope and isolation tier for an attempt."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    # -- resource envelope ---------------------------------------------------

    def _resolve_value(
        self,
        *,
        field: str,
        default: str,
        manifest_value: str | None,
        step_value: str | None,
        ceiling: str | None,
    ) -> str:
        """Resolve one request/limit through default → manifest → step, capped by the ceiling.

        The manifest value (or the platform default when the manifest is
        silent) is the *baseline*. A step override may only tighten it; a
        loosening override is a violation. The final value may never exceed the
        cluster ceiling.
        """
        baseline = manifest_value if manifest_value is not None else default
        baseline_q = self._parse(field, baseline)

        resolved = baseline
        resolved_q = baseline_q
        if step_value is not None:
            step_q = self._parse(f"{field} (step override)", step_value)
            if step_q > baseline_q:
                raise ResourceLimitError(
                    f"step override for {field} ({step_value!r}) exceeds the "
                    f"baseline {baseline!r}; a step may only tighten resources"
                )
            resolved = step_value
            resolved_q = step_q

        if ceiling is not None:
            ceiling_q = self._parse(f"{field} (cluster ceiling)", ceiling)
            if resolved_q > ceiling_q:
                raise ResourceLimitError(
                    f"resolved {field} ({resolved!r}) exceeds the cluster ceiling {ceiling!r}"
                )
        # Return the parser's normalized source so a whitespace-padded input
        # never leaks an invalid Kubernetes quantity into the envelope.
        return str(resolved_q)

    @staticmethod
    def _parse(field: str, raw: str) -> Quantity:
        try:
            return Quantity.parse(raw)
        except ValueError as exc:
            raise ResourceLimitError(f"invalid {field} quantity: {exc}") from exc

    def _effective_envelope(
        self,
        *,
        resources: Resources,
        override: ResourceOverride,
        ceiling: ResourceOverride | None,
    ) -> dict[str, str]:
        s = self._settings
        cap = ceiling or ResourceOverride()
        cpu = resources.cpu
        memory = resources.memory
        storage = resources.ephemeral_storage
        envelope = {
            "cpu_request": self._resolve_value(
                field="cpu.request",
                default=s.default_cpu_request,
                manifest_value=cpu.request if cpu else None,
                step_value=override.cpu_request,
                ceiling=cap.cpu_request,
            ),
            "cpu_limit": self._resolve_value(
                field="cpu.limit",
                default=s.default_cpu_limit,
                manifest_value=cpu.limit if cpu else None,
                step_value=override.cpu_limit,
                ceiling=cap.cpu_limit,
            ),
            "memory_request": self._resolve_value(
                field="memory.request",
                default=s.default_memory_request,
                manifest_value=memory.request if memory else None,
                step_value=override.memory_request,
                ceiling=cap.memory_request,
            ),
            "memory_limit": self._resolve_value(
                field="memory.limit",
                default=s.default_memory_limit,
                manifest_value=memory.limit if memory else None,
                step_value=override.memory_limit,
                ceiling=cap.memory_limit,
            ),
            "ephemeral_storage_limit": self._resolve_value(
                field="ephemeralStorage.limit",
                default=s.default_ephemeral_storage_limit,
                manifest_value=storage.limit if storage else None,
                step_value=override.ephemeral_storage_limit,
                ceiling=cap.ephemeral_storage_limit,
            ),
        }
        self._check_request_not_above_limit("cpu", envelope["cpu_request"], envelope["cpu_limit"])
        self._check_request_not_above_limit(
            "memory", envelope["memory_request"], envelope["memory_limit"]
        )
        return envelope

    def _check_request_not_above_limit(self, field: str, request: str, limit: str) -> None:
        """Reject an envelope where ``request`` exceeds ``limit`` (Kubernetes would too)."""
        if self._parse(f"{field}.request", request) > self._parse(f"{field}.limit", limit):
            raise ResourceLimitError(
                f"resolved {field}.request ({request!r}) exceeds {field}.limit ({limit!r})"
            )

    # -- timeout -------------------------------------------------------------

    def _effective_timeout(self, manifest_timeout: str) -> timedelta:
        """Parse the manifest timeout and clamp it to the ``ARM_MAX_TIMEOUT`` ceiling."""
        parsed = parse_iso8601_duration("spec.resources.timeout", manifest_timeout)
        return min(parsed, self._settings.max_timeout)

    # -- isolation tier ------------------------------------------------------

    def _select_tier(
        self, *, manifest_floor: IsolationTier, step_tier: IsolationTier | None
    ) -> IsolationTier:
        """Select ``max(manifest.minTier, step.minTier)``.

        A step may upgrade the floor but never downgrade below it.
        """
        if step_tier is None:
            return manifest_floor
        if _TIER_RANK[step_tier] >= _TIER_RANK[manifest_floor]:
            return step_tier
        # The step asked for a weaker tier than the manifest floor. ARM never
        # downgrades isolation, so the floor wins (a downgrade is never selected).
        return manifest_floor

    def _runtime_class(self, tier: IsolationTier) -> str:
        runtime_class = self._settings.runtime_class_for_tier(tier.value)
        # ``process`` maps to the cluster-default runtime (runc) when empty; for
        # the harder tiers an empty class means the tier is unavailable.
        if runtime_class == "" and tier is not IsolationTier.PROCESS:
            raise RuntimeUnavailableError(tier.value)
        return runtime_class

    # -- entry point ---------------------------------------------------------

    def limit(
        self,
        *,
        resources: Resources,
        isolation_floor: IsolationTier,
        override: ResourceOverride | None = None,
        cluster_ceiling: ResourceOverride | None = None,
    ) -> EffectiveResources:
        """Resolve the effective resource envelope and isolation tier for one attempt.

        Args:
            resources: The manifest's ``spec.resources`` (timeout required;
                cpu/memory/storage optional).
            isolation_floor: The manifest's isolation floor (``process`` when
                the manifest is silent — see
                :attr:`ActivityTypeVersion.isolation_floor`).
            override: Optional per-step tuning; may only tighten resources and
                upgrade the isolation tier.
            cluster_ceiling: Optional absolute resource cap (operator policy);
                any resolved value exceeding it is a violation.

        Returns:
            The fully-resolved :class:`EffectiveResources`.

        Raises:
            ResourceLimitError: A step override loosened a resource, a value
                exceeded the cluster ceiling, or a quantity was malformed.
            RuntimeUnavailableError: The selected tier has no ``RuntimeClass``
                configured.
        """
        step = override or ResourceOverride()
        envelope = self._effective_envelope(
            resources=resources, override=step, ceiling=cluster_ceiling
        )
        tier = self._select_tier(manifest_floor=isolation_floor, step_tier=step.min_tier)
        return EffectiveResources(
            cpu_request=envelope["cpu_request"],
            cpu_limit=envelope["cpu_limit"],
            memory_request=envelope["memory_request"],
            memory_limit=envelope["memory_limit"],
            ephemeral_storage_limit=envelope["ephemeral_storage_limit"],
            timeout=self._effective_timeout(resources.timeout),
            tier=tier,
            runtime_class=self._runtime_class(tier),
        )
