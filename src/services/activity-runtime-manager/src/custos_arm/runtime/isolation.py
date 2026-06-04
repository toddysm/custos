"""Sandbox isolation model (ARM-IMPL-014).

This module pins the concrete M1 realization of REQ-039 (activities run
isolated from the host and the orchestrator):

* the **hardened baseline `SecurityContext`** every activity container runs
  with, regardless of tier (design § Guaranteed process-tier hardening);
* the **tier → `RuntimeClass`** resolution with the **no-silent-downgrade**
  rule — an isolation tier with no configured `RuntimeClass` fails the
  attempt with ``system.runtime_unavailable`` (permanent) before any sandbox
  is created (design § No silent downgrade), rather than running at a weaker
  tier.

The Resource Limiter (ARM-IMPL-008) already resolves the tier and stamps the
``RuntimeClass`` onto :class:`~custos_arm.limit.EffectiveResources`; this
module is the reusable isolation API the OCI Job builder (ARM-IMPL-015)
calls to assemble the Pod's security posture, and it reuses the limiter's
documented :class:`~custos_arm.limit.RuntimeUnavailableError`
(``system.runtime_unavailable``) so the error taxonomy stays single-sourced.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Final

from custos_arm.config import Settings
from custos_arm.limit import RuntimeUnavailableError
from custos_arm.manifest import IsolationTier

__all__ = [
    "HARDENED_SECURITY_CONTEXT",
    "SECCOMP_RUNTIME_DEFAULT",
    "Capabilities",
    "IsolationProfile",
    "SeccompProfile",
    "SecurityContext",
    "build_isolation_profile",
    "is_tier_available",
    "resolve_runtime_class",
]

#: The seccomp profile type mandated for every sandbox container.
SECCOMP_RUNTIME_DEFAULT: Final[str] = "RuntimeDefault"


@dataclass(frozen=True, slots=True)
class Capabilities:
    """Linux capability set for the container.

    The baseline drops every capability and adds none; activities never need
    ambient privileges.
    """

    drop: tuple[str, ...] = ("ALL",)
    add: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class SeccompProfile:
    """The container seccomp profile (always ``RuntimeDefault`` in M1)."""

    type: str = SECCOMP_RUNTIME_DEFAULT


@dataclass(frozen=True, slots=True)
class SecurityContext:
    """The hardened container ``SecurityContext`` applied to every sandbox.

    Mirrors the Kubernetes container ``securityContext`` so the Job builder
    can stamp it onto the activity container verbatim. Pod-level hardening
    (no host network / PID / IPC, automount disabled, tmpfs-only mounts) is
    applied by the Job builder on the ``PodSpec``.
    """

    run_as_non_root: bool
    allow_privilege_escalation: bool
    privileged: bool
    read_only_root_filesystem: bool
    capabilities: Capabilities
    seccomp_profile: SeccompProfile


#: The single hardened baseline every activity container runs with. The
#: ``process`` tier *is* this baseline on runc; ``vm`` / ``microvm`` add a
#: kernel boundary on top via the selected ``RuntimeClass``.
HARDENED_SECURITY_CONTEXT: Final[SecurityContext] = SecurityContext(
    run_as_non_root=True,
    allow_privilege_escalation=False,
    privileged=False,
    read_only_root_filesystem=True,
    capabilities=Capabilities(drop=("ALL",)),
    seccomp_profile=SeccompProfile(type=SECCOMP_RUNTIME_DEFAULT),
)


@dataclass(frozen=True, slots=True)
class IsolationProfile:
    """The resolved isolation posture for one sandbox.

    ``runtime_class`` is the empty string when the tier maps to the
    cluster-default runtime (the common case for ``process``, which runs on
    runc unless ``ARM_RUNTIME_CLASS_PROCESS`` overrides it); otherwise it is
    the operator-configured class for the tier. A hard tier (``vm`` /
    ``microvm``) is never empty once it has resolved successfully.
    """

    tier: IsolationTier
    runtime_class: str
    security_context: SecurityContext = field(default=HARDENED_SECURITY_CONTEXT)


def is_tier_available(settings: Settings, tier: IsolationTier) -> bool:
    """Whether ``tier`` can run on this cluster.

    ``process`` is always available — it needs no dedicated ``RuntimeClass``
    and runs on the cluster-default runtime (runc) unless
    ``ARM_RUNTIME_CLASS_PROCESS`` pins it to a specific class. ``vm`` and
    ``microvm`` are available only when their ``RuntimeClass`` is configured.
    """
    if tier is IsolationTier.PROCESS:
        return True
    return settings.runtime_class_for_tier(tier.value) != ""


def resolve_runtime_class(settings: Settings, tier: IsolationTier) -> str:
    """Resolve ``tier`` to its cluster ``RuntimeClass``, enforcing no downgrade.

    Returns the empty string when the tier maps to the cluster-default
    runtime — typically ``process`` (runc) unless ``ARM_RUNTIME_CLASS_PROCESS``
    pins it to an explicit class — or the configured class otherwise.

    Raises:
        RuntimeUnavailableError: ``tier`` is ``vm`` / ``microvm`` and has no
            ``RuntimeClass`` configured — the attempt fails with
            ``system.runtime_unavailable`` rather than silently downgrading.
    """
    runtime_class = settings.runtime_class_for_tier(tier.value)
    if runtime_class == "" and tier is not IsolationTier.PROCESS:
        raise RuntimeUnavailableError(tier.value)
    return runtime_class


def build_isolation_profile(settings: Settings, tier: IsolationTier) -> IsolationProfile:
    """Assemble the :class:`IsolationProfile` for ``tier``.

    Resolves the ``RuntimeClass`` (raising for an unsatisfiable tier) and
    attaches the hardened baseline :data:`HARDENED_SECURITY_CONTEXT`.
    """
    return IsolationProfile(
        tier=tier,
        runtime_class=resolve_runtime_class(settings, tier),
    )
