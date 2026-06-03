"""Tests for the Resource Limiter (ARM-IMPL-008)."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest

from custos_arm.config import Settings, load_settings
from custos_arm.limit import (
    EffectiveResources,
    Quantity,
    ResourceLimiter,
    ResourceLimitError,
    ResourceOverride,
    RuntimeUnavailableError,
)
from custos_arm.manifest import IsolationTier, Resources, parse_manifest


def _manifest_resources(
    *,
    cpu: dict[str, str] | None = None,
    memory: dict[str, str] | None = None,
    ephemeral: dict[str, str] | None = None,
    timeout: str = "PT5M",
    min_tier: str | None = None,
) -> tuple[Resources, IsolationTier]:
    """Build a ``Resources`` + isolation floor by round-tripping a real manifest."""
    runtime: dict[str, Any] = {
        "kind": "oci-container",
        "image": "ghcr.io/acme/scan:1.0.0",
        "digest": "sha256:" + "cd" * 32,
    }
    if min_tier is not None:
        runtime["isolation"] = {"minTier": min_tier}
    resources: dict[str, Any] = {"timeout": timeout}
    if cpu is not None:
        resources["cpu"] = cpu
    if memory is not None:
        resources["memory"] = memory
    if ephemeral is not None:
        resources["ephemeralStorage"] = ephemeral
    manifest = parse_manifest(
        {
            "apiVersion": "custos.dev/v1",
            "kind": "ActivityManifest",
            "metadata": {
                "type": "scan-image",
                "version": "1.0.0",
                "namespace": "security",
                "description": "Scans an OCI image.",
                "owner": "team-sec",
            },
            "spec": {
                "contractVersion": "1",
                "runtime": runtime,
                "inputs": {"schema": {"type": "object"}},
                "outputs": {"schema": {"type": "object"}},
                "resources": resources,
            },
        }
    )
    floor = (
        manifest.spec.runtime.isolation.min_tier
        if manifest.spec.runtime.isolation and manifest.spec.runtime.isolation.min_tier
        else IsolationTier.PROCESS
    )
    return manifest.spec.resources, floor


def _settings_with(**overrides: str) -> Settings:
    """Load ``Settings`` with extra env overrides on top of the test baseline."""
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
# Quantity
# ---------------------------------------------------------------------------


def test_quantity_milli_equals_decimal() -> None:
    assert Quantity.parse("250m") == Quantity.parse("0.25")


def test_quantity_binary_ordering() -> None:
    assert Quantity.parse("1Gi") > Quantity.parse("1000Mi")
    assert Quantity.parse("256Mi") < Quantity.parse("1Gi")


def test_quantity_decimal_si_suffix() -> None:
    assert Quantity.parse("1G") == Quantity.parse("1000M")


def test_quantity_comparisons_total_order() -> None:
    small = Quantity.parse("500m")
    big = Quantity.parse("2")
    assert small <= small
    assert big >= small
    assert (small < big) and (big > small)
    assert small != big


def test_quantity_str_roundtrips_source() -> None:
    assert str(Quantity.parse(" 512Mi ")) == "512Mi"
    assert repr(Quantity.parse("1Gi")) == "Quantity('1Gi')"


def test_quantity_is_hashable() -> None:
    assert len({Quantity.parse("250m"), Quantity.parse("0.25")}) == 1


def test_quantity_is_immutable() -> None:
    q = Quantity.parse("1Gi")
    with pytest.raises(AttributeError):
        q._value = q._value  # type: ignore[misc]


def test_quantity_eq_with_non_quantity_is_not_implemented() -> None:
    assert Quantity.parse("1") != "1"


@pytest.mark.parametrize("bad", ["", "abc", "1.2.3", "-1", "10Xi", "Mi", "1 Gi"])
def test_quantity_rejects_malformed(bad: str) -> None:
    with pytest.raises(ValueError):
        Quantity.parse(bad)


# ---------------------------------------------------------------------------
# Effective envelope
# ---------------------------------------------------------------------------


def test_platform_defaults_applied_when_manifest_silent() -> None:
    resources, floor = _manifest_resources()
    limiter = ResourceLimiter(_settings_with())

    eff = limiter.limit(resources=resources, isolation_floor=floor)

    assert isinstance(eff, EffectiveResources)
    assert eff.cpu_request == "250m"
    assert eff.cpu_limit == "1"
    assert eff.memory_request == "256Mi"
    assert eff.memory_limit == "1Gi"
    assert eff.ephemeral_storage_limit == "2Gi"


def test_manifest_values_override_defaults() -> None:
    resources, floor = _manifest_resources(
        cpu={"request": "500m", "limit": "2"},
        memory={"request": "512Mi", "limit": "2Gi"},
        ephemeral={"limit": "4Gi"},
    )
    limiter = ResourceLimiter(_settings_with())

    eff = limiter.limit(resources=resources, isolation_floor=floor)

    assert eff.cpu_request == "500m"
    assert eff.cpu_limit == "2"
    assert eff.memory_request == "512Mi"
    assert eff.memory_limit == "2Gi"
    assert eff.ephemeral_storage_limit == "4Gi"


def test_step_override_may_tighten() -> None:
    resources, floor = _manifest_resources(memory={"limit": "2Gi"})
    limiter = ResourceLimiter(_settings_with())

    eff = limiter.limit(
        resources=resources,
        isolation_floor=floor,
        override=ResourceOverride(memory_limit="1Gi"),
    )

    assert eff.memory_limit == "1Gi"


def test_step_override_may_not_loosen() -> None:
    resources, floor = _manifest_resources(memory={"limit": "1Gi"})
    limiter = ResourceLimiter(_settings_with())

    with pytest.raises(ResourceLimitError):
        limiter.limit(
            resources=resources,
            isolation_floor=floor,
            override=ResourceOverride(memory_limit="2Gi"),
        )


def test_step_override_tightens_below_default_baseline() -> None:
    # Manifest silent → baseline is the platform default (1Gi); a 512Mi
    # override tightens it.
    resources, floor = _manifest_resources()
    limiter = ResourceLimiter(_settings_with())

    eff = limiter.limit(
        resources=resources,
        isolation_floor=floor,
        override=ResourceOverride(memory_limit="512Mi"),
    )

    assert eff.memory_limit == "512Mi"


def test_value_exceeding_cluster_ceiling_is_rejected() -> None:
    resources, floor = _manifest_resources(memory={"limit": "2Gi"})
    limiter = ResourceLimiter(_settings_with())

    with pytest.raises(ResourceLimitError):
        limiter.limit(
            resources=resources,
            isolation_floor=floor,
            cluster_ceiling=ResourceOverride(memory_limit="1Gi"),
        )


def test_value_within_cluster_ceiling_is_allowed() -> None:
    resources, floor = _manifest_resources(memory={"limit": "1Gi"})
    limiter = ResourceLimiter(_settings_with())

    eff = limiter.limit(
        resources=resources,
        isolation_floor=floor,
        cluster_ceiling=ResourceOverride(memory_limit="4Gi"),
    )

    assert eff.memory_limit == "1Gi"


def test_malformed_manifest_quantity_is_rejected() -> None:
    resources, floor = _manifest_resources(cpu={"request": "not-a-quantity"})
    limiter = ResourceLimiter(_settings_with())

    with pytest.raises(ResourceLimitError):
        limiter.limit(resources=resources, isolation_floor=floor)


def test_resource_limit_error_uses_reserved_namespace() -> None:
    resources, floor = _manifest_resources(memory={"limit": "1Gi"})
    limiter = ResourceLimiter(_settings_with())

    with pytest.raises(ResourceLimitError) as excinfo:
        limiter.limit(
            resources=resources,
            isolation_floor=floor,
            override=ResourceOverride(memory_limit="2Gi"),
        )

    assert excinfo.value.code == "system.resource_limit_violation"


def test_whitespace_padded_quantity_is_normalized() -> None:
    resources, floor = _manifest_resources(memory={"limit": " 512Mi "})
    limiter = ResourceLimiter(_settings_with())

    eff = limiter.limit(resources=resources, isolation_floor=floor)

    assert eff.memory_limit == "512Mi"


def test_request_above_limit_is_rejected() -> None:
    # Manifest leaves cpu.request at the 250m default but the step tightens
    # cpu.limit below it → request > limit, which Kubernetes would reject.
    resources, floor = _manifest_resources()
    limiter = ResourceLimiter(_settings_with())

    with pytest.raises(ResourceLimitError):
        limiter.limit(
            resources=resources,
            isolation_floor=floor,
            override=ResourceOverride(cpu_limit="100m"),
        )


# ---------------------------------------------------------------------------
# Timeout clamping
# ---------------------------------------------------------------------------


def test_timeout_passthrough_within_ceiling() -> None:
    resources, floor = _manifest_resources(timeout="PT5M")
    limiter = ResourceLimiter(_settings_with())

    eff = limiter.limit(resources=resources, isolation_floor=floor)

    assert eff.timeout == timedelta(minutes=5)


def test_timeout_clamped_to_max() -> None:
    resources, floor = _manifest_resources(timeout="PT5M")
    limiter = ResourceLimiter(_settings_with(ARM_MAX_TIMEOUT="PT2M"))

    eff = limiter.limit(resources=resources, isolation_floor=floor)

    assert eff.timeout == timedelta(minutes=2)


# ---------------------------------------------------------------------------
# Isolation tier selection
# ---------------------------------------------------------------------------


def test_tier_defaults_to_manifest_floor() -> None:
    resources, floor = _manifest_resources(min_tier="vm")
    limiter = ResourceLimiter(_settings_with(ARM_RUNTIME_CLASS_VM="kata-vm"))

    eff = limiter.limit(resources=resources, isolation_floor=floor)

    assert eff.tier is IsolationTier.VM
    assert eff.runtime_class == "kata-vm"


def test_step_may_upgrade_tier() -> None:
    resources, floor = _manifest_resources(min_tier="process")
    limiter = ResourceLimiter(_settings_with(ARM_RUNTIME_CLASS_MICROVM="kata-fc"))

    eff = limiter.limit(
        resources=resources,
        isolation_floor=floor,
        override=ResourceOverride(min_tier=IsolationTier.MICROVM),
    )

    assert eff.tier is IsolationTier.MICROVM
    assert eff.runtime_class == "kata-fc"


def test_step_may_not_downgrade_tier() -> None:
    resources, floor = _manifest_resources(min_tier="vm")
    limiter = ResourceLimiter(_settings_with(ARM_RUNTIME_CLASS_VM="kata-vm"))

    eff = limiter.limit(
        resources=resources,
        isolation_floor=floor,
        override=ResourceOverride(min_tier=IsolationTier.PROCESS),
    )

    # Downgrade is never selected — the manifest floor wins.
    assert eff.tier is IsolationTier.VM


def test_process_tier_maps_to_empty_runtime_class() -> None:
    resources, floor = _manifest_resources()
    limiter = ResourceLimiter(_settings_with())

    eff = limiter.limit(resources=resources, isolation_floor=floor)

    assert eff.tier is IsolationTier.PROCESS
    assert eff.runtime_class == ""


def test_unconfigured_tier_raises_runtime_unavailable() -> None:
    resources, floor = _manifest_resources(min_tier="microvm")
    limiter = ResourceLimiter(_settings_with())  # no ARM_RUNTIME_CLASS_MICROVM

    with pytest.raises(RuntimeUnavailableError) as excinfo:
        limiter.limit(resources=resources, isolation_floor=floor)

    assert excinfo.value.tier == "microvm"
    assert excinfo.value.code == "system.runtime_unavailable"
