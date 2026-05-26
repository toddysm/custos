"""Tier 1 capability registry + capability token grammar (CONN-IMPL-009).

Two-tier capability namespace per design § Capabilities and Events →
Namespace governance:

* **Tier 1** — Reserved core prefixes (``oci.*``, ``s3.*``, ``blob.*``,
  ``http.*``, ``sql.*``, ``event.*``, ``notification.*``). The platform
  curates the exact tokens in
  ``design/architecture/capabilities.md``. This module loads the
  curated list from a packaged JSON sidecar at
  ``custos_connector.manifest._data.capability_registry.v1.json``.
* **Tier 2** — Vendor extension tokens matching
  ``^x-[a-z][a-z0-9-]*\\.[a-z][a-z0-9.-]*$``. Syntax-only check; no
  platform-side semantics.

The classifier returns a stable :class:`CapabilityTier` enum so the
validator and the loader can branch on the result without re-parsing
the token. Rejections raise :class:`ManifestValidationError` with
either :attr:`ValidationErrorCode.UNKNOWN_CORE_CAPABILITY` (token's
first segment is a reserved prefix but the token is not in the
curated registry) or
:attr:`ValidationErrorCode.INVALID_CAPABILITY_SYNTAX` (token is
neither Tier 1 nor a valid Tier 2 vendor token).

The ``event.*`` prefix is reserved AND forbidden in capabilities
(event-stream verbs live in ``spec.events``); that case has its own
:attr:`ValidationErrorCode.EVENT_TOKEN_IN_CAPABILITIES` code so
operator audit can distinguish it from the more generic
``unknown-core-capability`` rejection.

The drift between this JSON sidecar and the human-curated markdown
registry is enforced by ``tests/test_capability_registry_drift.py``.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from enum import StrEnum
from importlib import resources
from types import MappingProxyType
from typing import Any, Final, TypedDict

from custos_connector.manifest.errors import (
    ManifestValidationError,
    ValidationErrorCode,
)

#: Packaged resource path for the Tier 1 registry sidecar.
_REGISTRY_RESOURCE: Final[str] = "capability_registry.v1.json"


class _RegistryData(TypedDict):
    """Internal shape of the loaded JSON sidecar."""

    version: int
    reservedPrefixes: list[str]
    forbiddenInCapabilities: list[str]
    tokens: list[str]


def _load_registry() -> _RegistryData:
    """Load the Tier 1 capability registry from the packaged sidecar."""
    pkg = resources.files("custos_connector.manifest._data")
    raw = (pkg / _REGISTRY_RESOURCE).read_text(encoding="utf-8")
    parsed: Any = json.loads(raw)
    if not isinstance(parsed, dict):  # pragma: no cover - defensive
        raise RuntimeError(f"packaged registry {_REGISTRY_RESOURCE} is not a JSON object")
    # Strip any "$comment" keys before returning.
    return {
        "version": parsed["version"],
        "reservedPrefixes": list(parsed["reservedPrefixes"]),
        "forbiddenInCapabilities": list(parsed["forbiddenInCapabilities"]),
        "tokens": list(parsed["tokens"]),
    }


_REGISTRY: Final[_RegistryData] = _load_registry()

#: Reserved core prefixes — first dot-segments whose namespace is
#: reserved by the curated registry. When a token's first segment is in
#: this set but the full token is not in :data:`TIER1_TOKENS`, the
#: classifier raises ``UNKNOWN_CORE_CAPABILITY`` (instead of falling
#: through to ``INVALID_CAPABILITY_SYNTAX``) so operators get a precise,
#: actionable error pointing at the curated registry.
#:
#: Not every Tier 1 token's first segment appears here. Individually
#: curated tokens such as ``slack.post`` / ``teams.post`` /
#: ``email.send`` are accepted via :data:`TIER1_TOKENS` membership; their
#: prefixes are NOT namespace-reserved (an unknown ``slack.*`` token
#: therefore falls through to ``INVALID_CAPABILITY_SYNTAX`` rather than
#: ``UNKNOWN_CORE_CAPABILITY``).
#:
#: ``event`` is reserved as well, but is special-cased into
#: :data:`FORBIDDEN_IN_CAPABILITIES` because ``event.*`` MUST NOT appear
#: in the ``capabilities`` array (event-stream verbs live in
#: ``spec.events``).
TIER1_RESERVED_PREFIXES: Final[frozenset[str]] = frozenset(_REGISTRY["reservedPrefixes"])

#: Prefixes that are reserved but MUST NOT appear in ``capabilities``.
FORBIDDEN_IN_CAPABILITIES: Final[frozenset[str]] = frozenset(_REGISTRY["forbiddenInCapabilities"])

#: Curated Tier 1 capability tokens. A token is accepted as Tier 1 iff
#: it is exactly in this set; a token whose first dot-segment is a
#: reserved prefix but which is not in this set is rejected with
#: :attr:`ValidationErrorCode.UNKNOWN_CORE_CAPABILITY`.
TIER1_TOKENS: Final[frozenset[str]] = frozenset(_REGISTRY["tokens"])

#: Tier 2 vendor extension pattern. Anchored to start/end so a bare
#: ``re.match`` cannot accept a token with a stray suffix.
TIER2_VENDOR_RE: Final[re.Pattern[str]] = re.compile(r"^x-[a-z][a-z0-9-]*\.[a-z][a-z0-9.-]*$")


class CapabilityTier(StrEnum):
    """Classification result for a capability token."""

    #: Token is in the curated Tier 1 registry.
    TIER1 = "tier1"

    #: Token matches the Tier 2 ``x-<vendor>.<verb>`` syntax. No
    #: platform-side semantic check.
    TIER2_VENDOR = "tier2-vendor"


def extract_capability_name(entry: object) -> str:
    """Return the canonical token name from a capability entry.

    A capability entry is either a bare string (live, non-deprecated) or
    an object of shape ``{"name": ..., "deprecated": bool, "since": str,
    "removeIn": str}`` (the deprecation envelope from design §
    Deprecation flow).

    The validator post-check runs *after* the JSON Schema pass which has
    already enforced the object/string union and the ``name``
    requirement on the object form; this function therefore raises
    :class:`TypeError` only as a defence-in-depth guard against a
    direct caller skipping the schema layer.
    """
    if isinstance(entry, str):
        return entry
    if isinstance(entry, Mapping):
        name = entry.get("name")
        if isinstance(name, str):
            return name
    raise TypeError(  # pragma: no cover - schema-blocked
        f"capability entry has unexpected shape: {entry!r}"
    )


def is_deprecated_entry(entry: object) -> bool:
    """Return ``True`` if the entry is the object form and ``deprecated`` is True."""
    if isinstance(entry, Mapping):
        return bool(entry.get("deprecated", False))
    return False


def classify_capability_token(token: str) -> CapabilityTier:
    """Classify ``token`` into a capability Tier.

    Returns the matching :class:`CapabilityTier` if the token is
    accepted under either tier's rules. Raises
    :class:`ManifestValidationError` for reserved or invalid tokens —
    the validator wraps this into the per-index error path.
    """
    # First, drop the reserved-in-capabilities case (``event.*``) — the
    # validator already raises a distinct EVENT_TOKEN_IN_CAPABILITIES
    # error for this before we get here, but we keep the guard so a
    # direct caller cannot accidentally classify it as Tier 1.
    first_segment = token.split(".", 1)[0]
    if first_segment in FORBIDDEN_IN_CAPABILITIES:
        raise ManifestValidationError(
            code=ValidationErrorCode.EVENT_TOKEN_IN_CAPABILITIES,
            detail=(
                f"capability token {token!r} uses the reserved {first_segment!r} "
                f"namespace; event-delivery verbs belong in spec.events"
            ),
        )
    if token in TIER1_TOKENS:
        return CapabilityTier.TIER1
    if first_segment in TIER1_RESERVED_PREFIXES:
        # First segment looks Tier 1 but the full token is not in the
        # curated registry — operator-actionable code.
        raise ManifestValidationError(
            code=ValidationErrorCode.UNKNOWN_CORE_CAPABILITY,
            detail=(
                f"capability token {token!r} uses the reserved {first_segment!r} "
                f"prefix but is not in the curated Tier 1 registry "
                f"(design/architecture/capabilities.md)"
            ),
        )
    if TIER2_VENDOR_RE.fullmatch(token):
        return CapabilityTier.TIER2_VENDOR
    raise ManifestValidationError(
        code=ValidationErrorCode.INVALID_CAPABILITY_SYNTAX,
        detail=(
            f"capability token {token!r} is neither a curated Tier 1 token "
            f"nor a valid Tier 2 vendor token "
            f"(must match ^x-[a-z][a-z0-9-]*\\.[a-z][a-z0-9.-]*$)"
        ),
    )


#: A read-only view of the loaded registry suitable for log / diagnostic
#: emission. The runtime never mutates the registry.
REGISTRY_VIEW: Final[Mapping[str, Any]] = MappingProxyType(
    {
        "version": _REGISTRY["version"],
        "reservedPrefixes": tuple(sorted(TIER1_RESERVED_PREFIXES)),
        "forbiddenInCapabilities": tuple(sorted(FORBIDDEN_IN_CAPABILITIES)),
        "tokens": tuple(sorted(TIER1_TOKENS)),
    }
)


__all__ = [
    "FORBIDDEN_IN_CAPABILITIES",
    "REGISTRY_VIEW",
    "TIER1_RESERVED_PREFIXES",
    "TIER1_TOKENS",
    "TIER2_VENDOR_RE",
    "CapabilityTier",
    "classify_capability_token",
    "extract_capability_name",
    "is_deprecated_entry",
]
