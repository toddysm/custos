"""Structured error taxonomy for connector-manifest pipeline (Phase C).

Every rejection surfaced by :mod:`custos_connector.manifest.validator`,
:mod:`custos_connector.manifest.normalizer`, and
:mod:`custos_connector.manifest.discovery` carries a stable string code
from the enums in this module. Codes are part of the wire contract
between Connector Service, Catalog Service, and the operator audit log
(catalog cross-component wiring is CONN-IMPL-034 / 008); changing a code
is a breaking change.

The validator emits :class:`ManifestValidationError` (codes in
:class:`ValidationErrorCode`). The discovery pipeline emits
:class:`ManifestDiscoveryError` for fallback-tag rejections and
"exactly one valid manifest" enforcement (codes in
:class:`DiscoveryErrorCode`).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum


class ValidationErrorCode(StrEnum):
    """Stable rejection codes emitted by :func:`validate_manifest`.

    Order is irrelevant to wire compatibility; the string value is.
    """

    #: ``jsonschema`` rejected the payload against the v1 schema and the
    #: validator could not assign a more specific code. The
    #: :attr:`ManifestValidationError.detail` carries the underlying
    #: ``jsonschema`` message and :attr:`ManifestValidationError.path`
    #: carries the JSON-Pointer-like location.
    SCHEMA_VIOLATION = "schema-violation"

    #: ``metadata.contractVersion`` is not the constant ``"1"``.
    UNSUPPORTED_CONTRACT_VERSION = "unsupported-contract-version"

    #: ``metadata.version`` is not a SemVer 2.0 string.
    INVALID_SEMVER = "invalid-semver"

    #: A connector-type-specific ``target.config`` is missing a required
    #: field for the selected ``target.kind`` (e.g. ``repositoryNamespace``
    #: for ``oci-registry``).
    MISSING_TARGET_CONFIG_FIELD = "missing-target-config-field"

    #: ``credentials.authenticationType`` is neither a known enum value
    #: nor a valid ``x-<vendor>`` extension token.
    UNKNOWN_AUTHENTICATION_TYPE = "unknown-authentication-type"

    #: A token in ``spec.capabilities`` is in the reserved ``event.*``
    #: namespace; event-delivery verbs belong in ``spec.events`` not in
    #: capabilities.
    EVENT_TOKEN_IN_CAPABILITIES = "event-token-in-capabilities"

    #: A capability or event token violates the dot-delimited lowercase
    #: token grammar. Retained for ``spec.events.produced`` tokens; new
    #: capability rejections use the more specific
    #: :attr:`UNKNOWN_CORE_CAPABILITY` /
    #: :attr:`INVALID_CAPABILITY_SYNTAX` codes from CONN-IMPL-009.
    INVALID_TOKEN_SYNTAX = "invalid-token-syntax"

    #: A capability token uses a reserved Tier 1 prefix (``oci.``,
    #: ``s3.``, ``blob.``, ``http.``, ``sql.``, ``notification.``) but
    #: is not in the curated Tier 1 registry
    #: (``design/architecture/capabilities.md``).
    UNKNOWN_CORE_CAPABILITY = "unknown-core-capability"

    #: A capability token is neither a curated Tier 1 token nor a valid
    #: Tier 2 vendor extension (``x-<vendor>.<verb>``).
    INVALID_CAPABILITY_SYNTAX = "invalid-capability-syntax"

    #: ``spec.events.delivery`` carries a value outside ``{push, pull}``.
    INVALID_EVENT_DELIVERY = "invalid-event-delivery"

    #: ``spec.events.produced`` is missing or empty when the ``events``
    #: block is present.
    EMPTY_EVENT_PRODUCED = "empty-event-produced"


class DiscoveryErrorCode(StrEnum):
    """Stable rejection codes emitted by :func:`discover_manifest`.

    Mirrors the audit-event suffix names so operators can grep both the
    audit log and the error payload with the same string.
    """

    #: Digest carries an algorithm other than ``sha256`` (v1 only
    #: implements ``sha256``).
    UNSUPPORTED_DIGEST_ALGORITHM = "unsupported-digest-algorithm"

    #: Digest does not match the ``<alg>:<hex>`` shape, or the hex part
    #: contains non-hex characters / a wrong length.
    INVALID_DIGEST_FORMAT = "invalid-digest-format"

    #: The computed fallback tag would exceed the OCI distribution-spec
    #: 128-character limit.
    FALLBACK_TAG_TOO_LONG = "fallback-tag-too-long"

    #: Neither the Referrers API nor the fallback tag yielded a
    #: manifest descriptor.
    NO_MANIFEST_FOUND = "no-manifest-found"

    #: Both discovery paths returned descriptors but they disagree, or
    #: more than one descriptor was returned for a single digest.
    AMBIGUOUS_MANIFEST = "ambiguous-manifest"


@dataclass(frozen=True, slots=True)
class ManifestValidationError(Exception):
    """Raised by :func:`validate_manifest` on the first hard rejection.

    Attributes:
        code: A :class:`ValidationErrorCode` value (compared as a plain
            string).
        detail: Human-readable explanation of the rejection. Safe to
            surface in an HTTP response body but not in the audit log
            without further redaction (carries author-supplied payload
            fragments).
        path: JSON-Pointer-style ``"/"``-joined path to the offending
            element (empty string for root-level errors).
        issues: Optional supplementary validation issues accompanying the
            primary error. Empty by default.
    """

    code: ValidationErrorCode
    detail: str
    path: str = ""
    # Optional supplementary validation issues accompanying the primary
    # error. Empty by default to preserve the existing "first hard error
    # wins" caller contract.
    issues: tuple[ManifestValidationIssue, ...] = field(default_factory=tuple)

    def __str__(self) -> str:  # pragma: no cover - trivial
        prefix = f"[{self.code}]"
        if self.path:
            return f"{prefix} {self.path}: {self.detail}"
        return f"{prefix} {self.detail}"


@dataclass(frozen=True, slots=True)
class ManifestValidationIssue:
    """One non-fatal validation issue surfaced alongside the primary error."""

    code: ValidationErrorCode
    detail: str
    path: str = ""


@dataclass(frozen=True, slots=True)
class ManifestDiscoveryError(Exception):
    """Raised by :func:`discover_manifest` and fallback-tag helpers.

    Attributes:
        code: A :class:`DiscoveryErrorCode` value.
        detail: Human-readable explanation. Safe for audit-log inclusion
            (no author-supplied payload material; only digest/tag strings).
        digest: The digest input that triggered the rejection (empty
            when not applicable; e.g. when the registry call itself
            errored out).
    """

    code: DiscoveryErrorCode
    detail: str
    digest: str = ""

    def __str__(self) -> str:  # pragma: no cover - trivial
        prefix = f"[{self.code}]"
        if self.digest:
            return f"{prefix} {self.digest}: {self.detail}"
        return f"{prefix} {self.detail}"


__all__ = [
    "DiscoveryErrorCode",
    "ManifestDiscoveryError",
    "ManifestValidationError",
    "ManifestValidationIssue",
    "ValidationErrorCode",
]
