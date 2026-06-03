"""Tests for the Activity Manifest v1 model and parser."""

from __future__ import annotations

import json
from copy import deepcopy
from typing import Any

import pytest

from custos_arm.manifest import (
    RESERVED_NAMESPACE_PREFIXES,
    ActivityManifest,
    Determinism,
    Idempotency,
    IsolationTier,
    ManifestError,
    is_capability_token,
    parse_manifest,
    parse_semver,
    reserved_namespace_prefix,
    to_canonical_json,
)

# The design's authoring example (§ Activity Manifest v1) in its actual JSON form.
_MANIFEST: dict[str, Any] = {
    "apiVersion": "custos.dev/v1",
    "kind": "ActivityManifest",
    "metadata": {
        "type": "scan-image",
        "version": "1.2.0",
        "namespace": "custos.builtin",
        "description": "Scan an OCI image for vulnerabilities using Trivy.",
        "labels": {"category": "security", "engine": "trivy"},
        "owner": "custos-maintainers",
    },
    "spec": {
        "contractVersion": "1",
        "runtime": {
            "kind": "oci-container",
            "image": "ghcr.io/custos/scan-image:1.2.0",
            "digest": "sha256:abc",
            "isolation": {"minTier": "microvm", "preferred": "microvm-firecracker"},
        },
        "inputs": {
            "schema": {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "required": ["image"],
                "properties": {
                    "image": {"$ref": "custos://types/ImageRef"},
                    "severity": {
                        "type": "string",
                        "enum": ["low", "medium", "high", "critical"],
                        "default": "high",
                    },
                },
            }
        },
        "outputs": {
            "schema": {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "required": ["findings", "reportRef"],
                "properties": {
                    "findings": {"type": "integer"},
                    "reportRef": {"$ref": "custos://types/ArtifactRef"},
                },
            },
            "artifacts": [
                {
                    "name": "report",
                    "mediaType": "application/vnd.cyclonedx+json",
                    "required": True,
                }
            ],
        },
        "connectors": [
            {
                "name": "registry",
                "type": "oci-registry",
                "required": True,
                "capabilities": ["oci.pull"],
            }
        ],
        "resources": {
            "cpu": {"request": "500m", "limit": "2"},
            "memory": {"request": "512Mi", "limit": "2Gi"},
            "ephemeralStorage": {"limit": "5Gi"},
            "timeout": "PT15M",
        },
        "errors": [
            {"code": "registry.unauthorized", "class": "permanent"},
            {"code": "scan.engine_failed", "class": "retryable"},
        ],
        "determinism": "side-effecting",
        "idempotency": "by-input-hash",
    },
}


def _manifest() -> dict[str, Any]:
    return deepcopy(_MANIFEST)


def test_authoring_example_parses() -> None:
    manifest = parse_manifest(_manifest())
    assert isinstance(manifest, ActivityManifest)
    assert manifest.metadata.type == "scan-image"
    assert manifest.spec.runtime.kind == "oci-container"
    assert manifest.spec.runtime.digest == "sha256:abc"
    assert manifest.spec.runtime.isolation is not None
    assert manifest.spec.runtime.isolation.min_tier is IsolationTier.MICROVM
    assert manifest.spec.outputs.artifacts[0].media_type == "application/vnd.cyclonedx+json"
    assert manifest.spec.determinism is Determinism.SIDE_EFFECTING
    assert manifest.spec.idempotency is Idempotency.BY_INPUT_HASH


def test_parses_from_json_text() -> None:
    manifest = parse_manifest(json.dumps(_manifest()))
    assert manifest.metadata.version == "1.2.0"


def test_invalid_json_raises_manifest_error() -> None:
    with pytest.raises(ManifestError, match="not valid JSON"):
        parse_manifest("{not json")


def test_non_object_payload_raises_manifest_error() -> None:
    with pytest.raises(ManifestError, match="must be a JSON object"):
        parse_manifest("[1, 2, 3]")


def test_defaults_applied_when_optional_fields_absent() -> None:
    payload = _manifest()
    del payload["spec"]["determinism"]
    del payload["spec"]["idempotency"]
    payload["spec"]["connectors"] = []
    manifest = parse_manifest(payload)
    assert manifest.spec.determinism is Determinism.SIDE_EFFECTING
    assert manifest.spec.idempotency is Idempotency.NONE
    assert manifest.spec.connectors == []


def test_timeout_is_required() -> None:
    payload = _manifest()
    del payload["spec"]["resources"]["timeout"]
    with pytest.raises(ManifestError, match="timeout"):
        parse_manifest(payload)


def test_digest_is_required() -> None:
    payload = _manifest()
    del payload["spec"]["runtime"]["digest"]
    with pytest.raises(ManifestError, match="digest"):
        parse_manifest(payload)


@pytest.mark.parametrize("kind", ["http", "wasm"])
def test_non_oci_runtime_kinds_rejected(kind: str) -> None:
    payload = _manifest()
    payload["spec"]["runtime"]["kind"] = kind
    with pytest.raises(ManifestError, match="kind"):
        parse_manifest(payload)


def test_unknown_field_rejected() -> None:
    payload = _manifest()
    payload["spec"]["surprise"] = "nope"
    with pytest.raises(ManifestError):
        parse_manifest(payload)


def test_contract_version_must_be_one() -> None:
    payload = _manifest()
    payload["spec"]["contractVersion"] = "2"
    with pytest.raises(ManifestError, match="contractVersion"):
        parse_manifest(payload)


def test_invalid_semver_rejected() -> None:
    payload = _manifest()
    payload["metadata"]["version"] = "1.2"
    with pytest.raises(ManifestError, match="semver"):
        parse_manifest(payload)


@pytest.mark.parametrize("bad", ["pull", "event.pushed", "OCI.Pull", ""])
def test_bare_or_event_capabilities_rejected(bad: str) -> None:
    payload = _manifest()
    payload["spec"]["connectors"][0]["capabilities"] = [bad]
    with pytest.raises(ManifestError, match="capability"):
        parse_manifest(payload)


def test_dot_namespaced_capabilities_accepted() -> None:
    payload = _manifest()
    payload["spec"]["connectors"][0]["capabilities"] = ["oci.pull", "oci.list-tags"]
    manifest = parse_manifest(payload)
    assert manifest.spec.connectors[0].capabilities == ["oci.pull", "oci.list-tags"]


def test_duplicate_artifact_names_rejected() -> None:
    payload = _manifest()
    payload["spec"]["outputs"]["artifacts"].append(
        {"name": "report", "mediaType": "application/json", "required": False}
    )
    with pytest.raises(ManifestError, match="unique"):
        parse_manifest(payload)


def test_canonical_json_is_deterministic_and_round_trips() -> None:
    manifest = parse_manifest(_manifest())
    canonical = to_canonical_json(manifest)
    # Stable: re-serializing the re-parsed manifest yields identical bytes.
    assert to_canonical_json(parse_manifest(canonical)) == canonical
    # Canonical: sorted keys + compact separators (re-dump matches byte-for-byte).
    assert canonical == json.dumps(json.loads(canonical), sort_keys=True, separators=(",", ":"))
    assert '"apiVersion":"custos.dev/v1"' in canonical


def test_parse_semver_components() -> None:
    version = parse_semver("3.4.5")
    assert (version.major, version.minor, version.patch) == (3, 4, 5)


@pytest.mark.parametrize(
    "bad", ["1", "1.2", "1.2.3.4", "v1.2.3", "1.2.x", "01.2.3", "1.02.3", "1.2.03"]
)
def test_parse_semver_rejects_non_triples(bad: str) -> None:
    with pytest.raises(ValueError, match="semver"):
        parse_semver(bad)


def test_reserved_namespace_prefix_identifies_platform() -> None:
    assert reserved_namespace_prefix("custos.builtin") == "custos"
    assert reserved_namespace_prefix("system.foo") == "system"
    assert reserved_namespace_prefix("acme") is None
    assert reserved_namespace_prefix("8a1b/scan") is None


def test_reserved_prefixes_constant() -> None:
    assert frozenset({"custos", "system", "platform", "builtin"}) == RESERVED_NAMESPACE_PREFIXES


@pytest.mark.parametrize(
    ("token", "expected"),
    [
        ("oci.pull", True),
        ("s3.read", True),
        ("oci.list-tags", True),
        ("pull", False),
        ("event.created", False),
        ("OCI.pull", False),
        ("", False),
        ("oci..pull", False),
    ],
)
def test_is_capability_token(token: str, expected: bool) -> None:
    assert is_capability_token(token) is expected
