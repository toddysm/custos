"""Tests for the sandbox isolation model (ARM-IMPL-014)."""

from __future__ import annotations

import dataclasses

import pytest

from custos_arm.config import Settings, load_settings
from custos_arm.limit import RuntimeUnavailableError
from custos_arm.manifest import IsolationTier
from custos_arm.runtime import (
    HARDENED_SECURITY_CONTEXT,
    SECCOMP_RUNTIME_DEFAULT,
    IsolationProfile,
    SecurityContext,
    build_isolation_profile,
    is_tier_available,
    resolve_runtime_class,
)


def _settings(**overrides: str) -> Settings:
    base = {
        "ARM_ARTIFACT_STORE": "artifacts",
        "ARM_METADATA_STORE": "metadata",
        "ARM_CATALOG_ENDPOINT": "http://catalog.svc:8080",
        "ARM_CONNECTOR_ENDPOINT": "http://connector.svc:8080",
        "ARM_SANDBOX_NAMESPACE": "custos-activities",
        "ARM_SIDECAR_IMAGE": "ghcr.io/custos/connector-sidecar:0.1.0",
        "ENVIRONMENT": "development",
    }
    base.update(overrides)
    return load_settings(base)


# ---------------------------------------------------------------------------
# Hardened SecurityContext baseline
# ---------------------------------------------------------------------------


def test_hardened_security_context_matches_design_baseline() -> None:
    ctx = HARDENED_SECURITY_CONTEXT
    assert ctx.run_as_non_root is True
    assert ctx.allow_privilege_escalation is False
    assert ctx.privileged is False
    assert ctx.read_only_root_filesystem is True
    assert ctx.capabilities.drop == ("ALL",)
    assert ctx.capabilities.add == ()
    assert ctx.seccomp_profile.type == SECCOMP_RUNTIME_DEFAULT
    assert SECCOMP_RUNTIME_DEFAULT == "RuntimeDefault"


def test_security_context_is_frozen() -> None:
    with pytest.raises(dataclasses.FrozenInstanceError):
        HARDENED_SECURITY_CONTEXT.privileged = True  # type: ignore[misc]


def test_security_context_is_constructible() -> None:
    ctx = SecurityContext(
        run_as_non_root=True,
        allow_privilege_escalation=False,
        privileged=False,
        read_only_root_filesystem=True,
        capabilities=HARDENED_SECURITY_CONTEXT.capabilities,
        seccomp_profile=HARDENED_SECURITY_CONTEXT.seccomp_profile,
    )
    assert ctx == HARDENED_SECURITY_CONTEXT


# ---------------------------------------------------------------------------
# Tier availability
# ---------------------------------------------------------------------------


def test_process_tier_always_available() -> None:
    assert is_tier_available(_settings(), IsolationTier.PROCESS) is True


def test_hard_tiers_unavailable_when_unconfigured() -> None:
    settings = _settings()
    assert is_tier_available(settings, IsolationTier.VM) is False
    assert is_tier_available(settings, IsolationTier.MICROVM) is False


def test_hard_tiers_available_when_configured() -> None:
    settings = _settings(
        ARM_RUNTIME_CLASS_VM="kata-clh",
        ARM_RUNTIME_CLASS_MICROVM="kata-fc",
    )
    assert is_tier_available(settings, IsolationTier.VM) is True
    assert is_tier_available(settings, IsolationTier.MICROVM) is True


# ---------------------------------------------------------------------------
# RuntimeClass resolution / no-silent-downgrade
# ---------------------------------------------------------------------------


def test_process_resolves_to_cluster_default_runc() -> None:
    assert resolve_runtime_class(_settings(), IsolationTier.PROCESS) == ""


def test_process_honours_configured_runtime_class() -> None:
    settings = _settings(ARM_RUNTIME_CLASS_PROCESS="gvisor")
    assert resolve_runtime_class(settings, IsolationTier.PROCESS) == "gvisor"


def test_configured_hard_tier_resolves_to_its_class() -> None:
    settings = _settings(ARM_RUNTIME_CLASS_VM="kata-clh")
    assert resolve_runtime_class(settings, IsolationTier.VM) == "kata-clh"


def test_unconfigured_vm_tier_raises_runtime_unavailable() -> None:
    with pytest.raises(RuntimeUnavailableError) as excinfo:
        resolve_runtime_class(_settings(), IsolationTier.VM)
    assert excinfo.value.tier == "vm"
    assert excinfo.value.code == "system.runtime_unavailable"


def test_unconfigured_microvm_tier_raises_runtime_unavailable() -> None:
    with pytest.raises(RuntimeUnavailableError) as excinfo:
        resolve_runtime_class(_settings(), IsolationTier.MICROVM)
    assert excinfo.value.tier == "microvm"


# ---------------------------------------------------------------------------
# Isolation profile
# ---------------------------------------------------------------------------


def test_build_isolation_profile_process() -> None:
    profile = build_isolation_profile(_settings(), IsolationTier.PROCESS)
    assert isinstance(profile, IsolationProfile)
    assert profile.tier is IsolationTier.PROCESS
    assert profile.runtime_class == ""
    assert profile.security_context is HARDENED_SECURITY_CONTEXT


def test_build_isolation_profile_configured_microvm() -> None:
    settings = _settings(ARM_RUNTIME_CLASS_MICROVM="kata-fc")
    profile = build_isolation_profile(settings, IsolationTier.MICROVM)
    assert profile.runtime_class == "kata-fc"
    assert profile.security_context == HARDENED_SECURITY_CONTEXT


def test_build_isolation_profile_rejects_unsatisfiable_tier() -> None:
    with pytest.raises(RuntimeUnavailableError):
        build_isolation_profile(_settings(), IsolationTier.VM)
