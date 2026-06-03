"""Activity Manifest v1 — models, parser, and canonical JSON form.

The activity manifest is the contract document for an activity. This package
provides the typed model tree (:class:`ActivityManifest`), semver / namespace /
capability helpers, and the parser that turns JSON into a validated manifest.
"""

from __future__ import annotations

from custos_arm.manifest._base import (
    RESERVED_NAMESPACE_PREFIXES,
    ManifestModel,
    SemVer,
    is_capability_token,
    parse_semver,
    reserved_namespace_prefix,
)
from custos_arm.manifest.models import (
    ActivityManifest,
    ArtifactSpec,
    ConnectorSpec,
    Determinism,
    EphemeralStorage,
    ErrorSpec,
    Idempotency,
    InputsSpec,
    Isolation,
    IsolationTier,
    Metadata,
    OutputsSpec,
    ResourceQuota,
    Resources,
    Runtime,
    Spec,
)
from custos_arm.manifest.parser import ManifestError, parse_manifest, to_canonical_json

__all__ = [
    "RESERVED_NAMESPACE_PREFIXES",
    "ActivityManifest",
    "ArtifactSpec",
    "ConnectorSpec",
    "Determinism",
    "EphemeralStorage",
    "ErrorSpec",
    "Idempotency",
    "InputsSpec",
    "Isolation",
    "IsolationTier",
    "ManifestError",
    "ManifestModel",
    "Metadata",
    "OutputsSpec",
    "ResourceQuota",
    "Resources",
    "Runtime",
    "SemVer",
    "Spec",
    "is_capability_token",
    "parse_manifest",
    "parse_semver",
    "reserved_namespace_prefix",
    "to_canonical_json",
]
