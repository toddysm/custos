"""ConnectorManifest pipeline (Phase C).

Public surface for the publish-time validator (CONN-IMPL-005), the
canonical normalizer + sha256 digest computation (CONN-IMPL-006), and
the OCI manifest-discovery client (CONN-IMPL-007).

The pipeline reads top-to-bottom:

    raw bytes  ->  json.loads
              ->  validate_manifest()                       # CONN-IMPL-005
              ->  normalize_manifest() + compute_digest()   # CONN-IMPL-006
              ->  CatalogStoreProvider.put_connector_type_version(digest=...)

The discovery client is independent of the publish path; it answers
*"given a connector image digest, where is its manifest artifact in the
registry?"* and is consumed by the Plugin Loader (CONN-IMPL-008).
"""

from __future__ import annotations

from custos_connector.manifest.capabilities import (
    FORBIDDEN_IN_CAPABILITIES,
    REGISTRY_VIEW,
    TIER1_RESERVED_PREFIXES,
    TIER1_TOKENS,
    TIER2_VENDOR_RE,
    CapabilityTier,
    classify_capability_token,
    extract_capability_name,
    is_deprecated_entry,
)
from custos_connector.manifest.discovery import (
    AUDIT_EVENT_FALLBACK_IGNORED,
    AUDIT_EVENT_FALLBACK_REJECTED,
    AUDIT_EVENT_FALLBACK_USED,
    CONNECTOR_MANIFEST_MEDIA_TYPE,
    MAX_OCI_TAG_LENGTH,
    ManifestDescriptor,
    discover_manifest,
    fallback_tag_for_digest,
    resolve_fallback_tag,
    resolve_referrers,
)
from custos_connector.manifest.errors import (
    DiscoveryErrorCode,
    ManifestDiscoveryError,
    ManifestValidationError,
    ManifestValidationIssue,
    ValidationErrorCode,
)
from custos_connector.manifest.normalizer import (
    canonical_bytes,
    canonical_json,
    compute_digest,
    normalize_manifest,
)
from custos_connector.manifest.validator import (
    CONNECTOR_MANIFEST_V1_SCHEMA,
    validate_manifest,
)

__all__ = [
    "AUDIT_EVENT_FALLBACK_IGNORED",
    "AUDIT_EVENT_FALLBACK_REJECTED",
    "AUDIT_EVENT_FALLBACK_USED",
    "CONNECTOR_MANIFEST_MEDIA_TYPE",
    "CONNECTOR_MANIFEST_V1_SCHEMA",
    "FORBIDDEN_IN_CAPABILITIES",
    "MAX_OCI_TAG_LENGTH",
    "REGISTRY_VIEW",
    "TIER1_RESERVED_PREFIXES",
    "TIER1_TOKENS",
    "TIER2_VENDOR_RE",
    "CapabilityTier",
    "DiscoveryErrorCode",
    "ManifestDescriptor",
    "ManifestDiscoveryError",
    "ManifestValidationError",
    "ManifestValidationIssue",
    "ValidationErrorCode",
    "canonical_bytes",
    "canonical_json",
    "classify_capability_token",
    "compute_digest",
    "discover_manifest",
    "extract_capability_name",
    "fallback_tag_for_digest",
    "is_deprecated_entry",
    "normalize_manifest",
    "resolve_fallback_tag",
    "resolve_referrers",
    "validate_manifest",
]
