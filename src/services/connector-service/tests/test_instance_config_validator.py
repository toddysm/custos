"""Unit tests for :func:`validate_instance_config` (CONN-IMPL-012, #295).

The validator is exercised against minimal synthesised manifest dicts
— there is no need to round-trip through the manifest validator
because CONN-IMPL-005 already covers that path. Each kind/auth/
capability rule is covered by both a happy-path and a negative-path
test.
"""

from __future__ import annotations

import pytest

from custos_connector.instances.validator import (
    InstanceConfigCode,
    InstanceConfigValidationError,
    validate_instance_config,
)


def _manifest(
    *,
    kind: str = "oci-registry",
    target_config: dict[str, object] | None = None,
    auth_type: str = "azure-key-vault",
    authentication: dict[str, object] | None = None,
    capabilities: tuple[str, ...] = ("oci.registry.read",),
) -> dict[str, object]:
    """Build a minimal normalized-manifest dict for the validator."""
    return {
        "metadata": {},
        "spec": {
            "target": {
                "kind": kind,
                "config": dict(target_config) if target_config is not None else {},
            },
            "credentials": {
                "authenticationType": auth_type,
                "authentication": dict(authentication) if authentication is not None else {},
            },
            "capabilities": list(capabilities),
        },
    }


# ---------------------------------------------------------------------------
# Happy paths
# ---------------------------------------------------------------------------


def test_validates_when_manifest_supplies_all_required_fields() -> None:
    """If the manifest already carries every required key, an empty
    operator override is valid."""
    manifest = _manifest(
        target_config={"repositoryNamespace": "acme/widgets"},
        authentication={"vaultUri": "https://kv.example.com", "secretName": "pat"},
    )

    validate_instance_config(
        manifest=manifest,
        target_config={},
        credentials_authentication={},
        used_capabilities=None,
    )


def test_validates_when_operator_supplies_required_overrides() -> None:
    """If the manifest's default is empty, the operator can fill it."""
    manifest = _manifest()
    validate_instance_config(
        manifest=manifest,
        target_config={"repositoryNamespace": "acme/widgets"},
        credentials_authentication={
            "vaultUri": "https://kv.example.com",
            "secretName": "pat",
        },
        used_capabilities=None,
    )


@pytest.mark.parametrize(
    ("kind", "config"),
    [
        ("oci-registry", {"repositoryNamespace": "acme/widgets"}),
        ("azure-blob-storage", {"storageAccount": "acmeprod", "container": "data"}),
        ("amazon-s3-bucket", {"bucket": "acme-prod", "region": "us-east-1"}),
    ],
)
def test_validates_each_known_target_kind(kind: str, config: dict[str, object]) -> None:
    manifest = _manifest(
        kind=kind,
        target_config=config,
        authentication={"vaultUri": "https://kv.example.com", "secretName": "pat"},
    )
    validate_instance_config(
        manifest=manifest,
        target_config={},
        credentials_authentication={},
        used_capabilities=None,
    )


@pytest.mark.parametrize(
    ("auth_type", "authentication"),
    [
        ("azure-key-vault", {"vaultUri": "https://kv.example.com", "secretName": "pat"}),
        ("amazon-kms", {"keyId": "alias/prod"}),
        ("azure-managed-identity", {}),
        ("oidc", {"issuerUri": "https://oidc.example.com", "audience": "test"}),
    ],
)
def test_validates_each_known_auth_type(auth_type: str, authentication: dict[str, object]) -> None:
    manifest = _manifest(
        target_config={"repositoryNamespace": "acme/widgets"},
        auth_type=auth_type,
        authentication=authentication,
    )
    validate_instance_config(
        manifest=manifest,
        target_config={},
        credentials_authentication={},
        used_capabilities=None,
    )


def test_vendor_auth_type_skips_field_check() -> None:
    """``x-<vendor>`` auth types bypass the per-auth required table."""
    manifest = _manifest(
        target_config={"repositoryNamespace": "acme/widgets"},
        auth_type="x-acme-vault",
        authentication={"any": "value"},
    )
    validate_instance_config(
        manifest=manifest,
        target_config={},
        credentials_authentication={},
        used_capabilities=None,
    )


def test_used_capabilities_subset_is_valid() -> None:
    manifest = _manifest(
        target_config={"repositoryNamespace": "acme/widgets"},
        authentication={"vaultUri": "https://kv.example.com", "secretName": "pat"},
        capabilities=("oci.registry.read", "oci.referrers.list"),
    )
    validate_instance_config(
        manifest=manifest,
        target_config={},
        credentials_authentication={},
        used_capabilities=("oci.registry.read",),
    )


def test_used_capabilities_empty_tuple_is_valid() -> None:
    """An operator may pin zero capabilities — that's a stricter pin
    than ``None`` (which leaves the catalog superset in place)."""
    manifest = _manifest(
        target_config={"repositoryNamespace": "acme/widgets"},
        authentication={"vaultUri": "https://kv.example.com", "secretName": "pat"},
        capabilities=("oci.registry.read",),
    )
    validate_instance_config(
        manifest=manifest,
        target_config={},
        credentials_authentication={},
        used_capabilities=(),
    )


def test_operator_override_supersedes_manifest_default() -> None:
    """When the manifest has a default but operator overrides with an
    empty string, the value is treated as missing."""
    manifest = _manifest(
        target_config={"repositoryNamespace": "acme/widgets"},
        authentication={"vaultUri": "https://kv.example.com", "secretName": "pat"},
    )
    with pytest.raises(InstanceConfigValidationError) as excinfo:
        validate_instance_config(
            manifest=manifest,
            target_config={"repositoryNamespace": "   "},
            credentials_authentication={},
            used_capabilities=None,
        )
    codes = [i.code for i in excinfo.value.issues]
    assert InstanceConfigCode.MISSING_TARGET_CONFIG_FIELD in codes


# ---------------------------------------------------------------------------
# Negative paths
# ---------------------------------------------------------------------------


def test_missing_target_config_field_is_collected() -> None:
    manifest = _manifest(
        target_config={},
        authentication={"vaultUri": "https://kv.example.com", "secretName": "pat"},
    )
    with pytest.raises(InstanceConfigValidationError) as excinfo:
        validate_instance_config(
            manifest=manifest,
            target_config={},
            credentials_authentication={},
            used_capabilities=None,
        )
    issues = excinfo.value.issues
    assert len(issues) == 1
    assert issues[0].code == InstanceConfigCode.MISSING_TARGET_CONFIG_FIELD
    assert issues[0].path == "target_config/repositoryNamespace"


def test_missing_authentication_field_is_collected() -> None:
    manifest = _manifest(
        target_config={"repositoryNamespace": "acme/widgets"},
        authentication={},
    )
    with pytest.raises(InstanceConfigValidationError) as excinfo:
        validate_instance_config(
            manifest=manifest,
            target_config={},
            credentials_authentication={},
            used_capabilities=None,
        )
    codes = {i.code for i in excinfo.value.issues}
    paths = {i.path for i in excinfo.value.issues}
    assert codes == {InstanceConfigCode.MISSING_AUTHENTICATION_FIELD}
    assert paths == {
        "credentials_authentication/vaultUri",
        "credentials_authentication/secretName",
    }


def test_unknown_target_kind_is_collected() -> None:
    manifest = _manifest(kind="ftp-server")
    with pytest.raises(InstanceConfigValidationError) as excinfo:
        validate_instance_config(
            manifest=manifest,
            target_config={},
            credentials_authentication={},
            used_capabilities=None,
        )
    codes = [i.code for i in excinfo.value.issues]
    assert InstanceConfigCode.UNKNOWN_TARGET_KIND in codes


def test_unknown_capability_is_collected() -> None:
    manifest = _manifest(
        target_config={"repositoryNamespace": "acme/widgets"},
        authentication={"vaultUri": "https://kv.example.com", "secretName": "pat"},
        capabilities=("oci.registry.read",),
    )
    with pytest.raises(InstanceConfigValidationError) as excinfo:
        validate_instance_config(
            manifest=manifest,
            target_config={},
            credentials_authentication={},
            used_capabilities=("oci.image.push", "oci.registry.read"),
        )
    issues = excinfo.value.issues
    assert len(issues) == 1
    assert issues[0].code == InstanceConfigCode.UNKNOWN_CAPABILITY_ON_INSTANCE
    assert issues[0].path == "used_capabilities/0"


def test_collect_all_errors_aggregates_across_rule_families() -> None:
    """A single call surfacing one of each rejection demonstrates the
    collect-all-errors UX promise."""
    manifest = _manifest(
        kind="amazon-s3-bucket",
        target_config={"bucket": "acme"},  # missing region
        auth_type="oidc",
        authentication={"audience": "test"},  # missing issuerUri
        capabilities=("s3.read",),
    )
    with pytest.raises(InstanceConfigValidationError) as excinfo:
        validate_instance_config(
            manifest=manifest,
            target_config={},
            credentials_authentication={},
            used_capabilities=("s3.write",),
        )
    codes = [i.code for i in excinfo.value.issues]
    assert codes.count(InstanceConfigCode.MISSING_TARGET_CONFIG_FIELD) == 1
    assert codes.count(InstanceConfigCode.MISSING_AUTHENTICATION_FIELD) == 1
    assert codes.count(InstanceConfigCode.UNKNOWN_CAPABILITY_ON_INSTANCE) == 1
