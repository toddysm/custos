"""Tests for the typed ``Settings`` loader (ARM-IMPL-002)."""

from __future__ import annotations

from datetime import timedelta

import pytest

from custos_arm.config import (
    DEFAULT_ALLOW_UNPINNED_IMAGES,
    DEFAULT_ARTIFACT_MAX_BYTES,
    DEFAULT_CPU_LIMIT,
    DEFAULT_CPU_REQUEST,
    DEFAULT_IO_BRIDGE_IMAGE,
    DEFAULT_OUTPUT_MAX_BYTES,
    DEFAULT_TIER,
    Settings,
    SettingsError,
    load_settings,
    parse_iso8601_duration,
)

_BASE_ENV: dict[str, str] = {
    "ARM_ARTIFACT_STORE": "artifacts",
    "ARM_METADATA_STORE": "metadata",
    "ARM_CATALOG_ENDPOINT": "http://catalog.svc:8080",
    "ARM_CONNECTOR_ENDPOINT": "http://connector.svc:8080",
    "ARM_SANDBOX_NAMESPACE": "custos-activities",
    "ARM_SIDECAR_IMAGE": "ghcr.io/custos/connector-sidecar:0.1.0",
}


def _env(**overrides: str) -> dict[str, str]:
    env = dict(_BASE_ENV)
    env.update(overrides)
    return env


def test_load_settings_applies_documented_defaults() -> None:
    settings = load_settings(_env())
    assert settings.default_tier == DEFAULT_TIER
    assert settings.default_cpu_request == DEFAULT_CPU_REQUEST
    assert settings.default_cpu_limit == DEFAULT_CPU_LIMIT
    assert settings.output_max_bytes == DEFAULT_OUTPUT_MAX_BYTES
    assert settings.artifact_max_bytes == DEFAULT_ARTIFACT_MAX_BYTES
    assert settings.max_timeout == timedelta(hours=1)
    assert settings.idempotency_ttl == timedelta(hours=24)
    assert settings.host == "0.0.0.0"
    assert settings.port == 8080
    assert settings.environment == "development"
    assert settings.io_bridge_image == DEFAULT_IO_BRIDGE_IMAGE
    # The default io-bridge image must be digest-pinned for reproducibility.
    assert "@sha256:" in settings.io_bridge_image
    assert settings.allow_unpinned_images == DEFAULT_ALLOW_UNPINNED_IMAGES
    # Production must reject unpinned images by default.
    assert settings.allow_unpinned_images is False


def test_load_settings_reads_required_and_optional_values() -> None:
    settings = load_settings(
        _env(
            ARM_DEFAULT_TIER="vm",
            ARM_RUNTIME_CLASS_VM="kata",
            ARM_OUTPUT_MAX_BYTES="2048",
            PORT="9090",
        )
    )
    assert settings.artifact_store == "artifacts"
    assert settings.sidecar_image == "ghcr.io/custos/connector-sidecar:0.1.0"
    assert settings.default_tier == "vm"
    assert settings.runtime_class_for_tier("vm") == "kata"
    assert settings.output_max_bytes == 2048
    assert settings.port == 9090


def test_io_bridge_image_override_is_honored() -> None:
    settings = load_settings(_env(ARM_IO_BRIDGE_IMAGE="registry.internal/io-bridge@sha256:beef"))
    assert settings.io_bridge_image == "registry.internal/io-bridge@sha256:beef"


@pytest.mark.parametrize("raw", ["true", "1", "yes", "on", "TRUE", "On"])
def test_allow_unpinned_images_truthy_values(raw: str) -> None:
    settings = load_settings(_env(ARM_ALLOW_UNPINNED_IMAGES=raw))
    assert settings.allow_unpinned_images is True


@pytest.mark.parametrize("raw", ["false", "0", "no", "off", "FALSE", "Off"])
def test_allow_unpinned_images_falsy_values(raw: str) -> None:
    settings = load_settings(_env(ARM_ALLOW_UNPINNED_IMAGES=raw))
    assert settings.allow_unpinned_images is False


def test_allow_unpinned_images_rejects_non_boolean() -> None:
    with pytest.raises(SettingsError, match="ARM_ALLOW_UNPINNED_IMAGES must be a boolean"):
        load_settings(_env(ARM_ALLOW_UNPINNED_IMAGES="maybe"))


@pytest.mark.parametrize(
    "missing",
    [
        "ARM_ARTIFACT_STORE",
        "ARM_METADATA_STORE",
        "ARM_CATALOG_ENDPOINT",
        "ARM_CONNECTOR_ENDPOINT",
        "ARM_SANDBOX_NAMESPACE",
        "ARM_SIDECAR_IMAGE",
    ],
)
def test_missing_required_variable_fails_fast(missing: str) -> None:
    env = _env()
    del env[missing]
    with pytest.raises(SettingsError) as exc:
        load_settings(env)
    assert missing in str(exc.value)
    assert "required" in str(exc.value)


def test_blank_required_variable_is_rejected() -> None:
    with pytest.raises(SettingsError, match="ARM_ARTIFACT_STORE is required"):
        load_settings(_env(ARM_ARTIFACT_STORE="   "))


def test_invalid_default_tier_is_rejected() -> None:
    with pytest.raises(SettingsError, match="ARM_DEFAULT_TIER must be one of"):
        load_settings(_env(ARM_DEFAULT_TIER="nitro"))


def test_non_integer_byte_cap_is_rejected() -> None:
    with pytest.raises(SettingsError, match="ARM_OUTPUT_MAX_BYTES must be an integer"):
        load_settings(_env(ARM_OUTPUT_MAX_BYTES="lots"))


def test_byte_cap_below_minimum_is_rejected() -> None:
    with pytest.raises(SettingsError, match="ARM_ARTIFACT_MAX_BYTES must be >= 1"):
        load_settings(_env(ARM_ARTIFACT_MAX_BYTES="0"))


def test_authz_endpoint_empty_enables_dev_shim() -> None:
    settings = load_settings(_env())
    assert settings.authz_endpoint == ""
    assert settings.use_callctx_dev_shim is True


def test_authz_endpoint_set_disables_dev_shim() -> None:
    settings = load_settings(_env(ARM_AUTHZ_ENDPOINT="http://auth.svc:8080"))
    assert settings.use_callctx_dev_shim is False


def test_is_production_is_case_insensitive() -> None:
    settings = load_settings(_env(ENVIRONMENT="Production"))
    assert settings.is_production is True
    assert load_settings(_env()).is_production is False


def test_runtime_class_for_tier_defaults_to_empty() -> None:
    settings = load_settings(_env())
    assert settings.runtime_class_for_tier("process") == ""
    assert settings.runtime_class_for_tier("vm") == ""
    assert settings.runtime_class_for_tier("microvm") == ""


def test_runtime_class_for_unknown_tier_raises() -> None:
    settings = load_settings(_env())
    with pytest.raises(ValueError, match="unknown isolation tier"):
        settings.runtime_class_for_tier("nitro")


def test_settings_is_frozen() -> None:
    settings = load_settings(_env())
    with pytest.raises(AttributeError):
        settings.port = 1  # type: ignore[misc]


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("PT1H", timedelta(hours=1)),
        ("PT24H", timedelta(hours=24)),
        ("PT30M", timedelta(minutes=30)),
        ("P1D", timedelta(days=1)),
        ("P1W", timedelta(weeks=1)),
        ("PT1H30M", timedelta(hours=1, minutes=30)),
        ("PT0.5S", timedelta(seconds=0.5)),
    ],
)
def test_parse_iso8601_duration_accepts_valid(raw: str, expected: timedelta) -> None:
    assert parse_iso8601_duration("ARM_MAX_TIMEOUT", raw) == expected


@pytest.mark.parametrize(
    "raw",
    ["", "1H", "PT", "P", "PT0S", "P1M", "P1Y", "garbage", "PT-1H"],
)
def test_parse_iso8601_duration_rejects_invalid(raw: str) -> None:
    with pytest.raises(SettingsError):
        parse_iso8601_duration("ARM_MAX_TIMEOUT", raw)


def test_iso8601_duration_settings_paths_are_validated() -> None:
    with pytest.raises(SettingsError, match="ARM_MAX_TIMEOUT"):
        load_settings(_env(ARM_MAX_TIMEOUT="nope"))
    with pytest.raises(SettingsError, match="ARM_IDEMPOTENCY_TTL"):
        load_settings(_env(ARM_IDEMPOTENCY_TTL="P1Y"))


def test_max_timeout_raw_is_retained() -> None:
    settings = load_settings(_env(ARM_MAX_TIMEOUT="PT2H"))
    assert settings.max_timeout_raw == "PT2H"
    assert settings.max_timeout == timedelta(hours=2)


def test_settings_can_be_constructed_directly() -> None:
    settings = Settings(
        artifact_store="a",
        metadata_store="m",
        catalog_endpoint="c",
        connector_endpoint="x",
        authz_endpoint="",
        sandbox_namespace="ns",
        sidecar_image="img",
        io_bridge_image="bridge",
        default_tier="process",
        runtime_class_process="",
        runtime_class_vm="",
        runtime_class_microvm="",
        default_cpu_request="250m",
        default_cpu_limit="1",
        default_memory_request="256Mi",
        default_memory_limit="1Gi",
        default_ephemeral_storage_limit="2Gi",
        max_timeout=timedelta(hours=1),
        output_max_bytes=1,
        artifact_max_bytes=1,
        idempotency_ttl=timedelta(hours=24),
        environment="development",
        host="0.0.0.0",
        port=8080,
    )
    assert settings.use_callctx_dev_shim is True
