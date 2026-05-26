"""Unit tests for the Tier 1 capability registry + token classifier.

Covers :mod:`custos_connector.manifest.capabilities` end-to-end:

* Every Tier 1 token in the curated registry classifies as ``TIER1``.
* Tokens whose first segment is a reserved core prefix but which are
  not in the registry emit :attr:`UNKNOWN_CORE_CAPABILITY`.
* Tokens that match the Tier 2 ``x-<vendor>.<verb>`` pattern classify
  as ``TIER2_VENDOR``.
* Tokens that match neither emit :attr:`INVALID_CAPABILITY_SYNTAX`.
* ``event.*`` tokens emit :attr:`EVENT_TOKEN_IN_CAPABILITIES`.
* :func:`extract_capability_name` accepts both string and object form.
* :func:`is_deprecated_entry` correctly identifies the object form's
  ``deprecated:true`` flag.
"""

from __future__ import annotations

import pytest

from custos_connector.manifest.capabilities import (
    FORBIDDEN_IN_CAPABILITIES,
    REGISTRY_VIEW,
    TIER1_RESERVED_PREFIXES,
    TIER1_TOKENS,
    CapabilityTier,
    classify_capability_token,
    extract_capability_name,
    is_deprecated_entry,
)
from custos_connector.manifest.errors import (
    ManifestValidationError,
    ValidationErrorCode,
)

# ---------------------------------------------------------------------------
# Registry shape sanity
# ---------------------------------------------------------------------------


def test_registry_view_is_immutable() -> None:
    """The registry view is a read-only mapping (no .clear, no __setitem__)."""
    with pytest.raises(TypeError):
        REGISTRY_VIEW["version"] = 99  # type: ignore[index]


def test_registry_contains_every_reserved_prefix_section() -> None:
    """Every reserved (non-forbidden) prefix has at least one curated token."""
    available_prefixes = {tok.split(".", 1)[0] for tok in TIER1_TOKENS}
    for prefix in TIER1_RESERVED_PREFIXES - FORBIDDEN_IN_CAPABILITIES:
        # ``slack`` / ``teams`` / ``email`` tokens are under the
        # ``notification`` umbrella prefix per design; they expand the
        # available_prefixes beyond the reserved set, which is fine.
        assert prefix in available_prefixes, (
            f"reserved prefix {prefix!r} has no curated Tier 1 tokens"
        )


def test_event_is_forbidden_in_capabilities() -> None:
    assert "event" in FORBIDDEN_IN_CAPABILITIES
    assert "event" in TIER1_RESERVED_PREFIXES


# ---------------------------------------------------------------------------
# classify_capability_token — Tier 1 happy path
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("token", sorted(TIER1_TOKENS))
def test_every_registry_token_classifies_as_tier1(token: str) -> None:
    assert classify_capability_token(token) is CapabilityTier.TIER1


# ---------------------------------------------------------------------------
# classify_capability_token — Tier 2 happy path
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "token",
    [
        "x-acme.foo",
        "x-acme.foo.bar",
        "x-acme-corp.do-thing",
        "x-a.b",
        "x-acme.v1.read",
    ],
)
def test_valid_vendor_tokens_classify_as_tier2(token: str) -> None:
    assert classify_capability_token(token) is CapabilityTier.TIER2_VENDOR


# ---------------------------------------------------------------------------
# classify_capability_token — rejections
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "token",
    [
        "oci.bogus",
        "oci.list",  # close to oci.list-tags but not in registry
        "s3.execute",
        "blob.copy",
        "http.patch",
        "sql.truncate",
        "notification.pageall",
    ],
)
def test_unknown_tier1_verb_is_rejected(token: str) -> None:
    """Reserved-prefix tokens not in the registry → unknown-core-capability."""
    with pytest.raises(ManifestValidationError) as exc:
        classify_capability_token(token)
    assert exc.value.code is ValidationErrorCode.UNKNOWN_CORE_CAPABILITY
    assert token in exc.value.detail


@pytest.mark.parametrize(
    "token",
    [
        "event.created",
        "event.updated",
        "event",
    ],
)
def test_event_namespace_is_rejected(token: str) -> None:
    """event.* and bare ``event`` token → event-token-in-capabilities."""
    with pytest.raises(ManifestValidationError) as exc:
        classify_capability_token(token)
    assert exc.value.code is ValidationErrorCode.EVENT_TOKEN_IN_CAPABILITIES


@pytest.mark.parametrize(
    "token",
    [
        "noprefix",  # no dot
        "Foo.Bar",  # uppercase
        "x-Acme.foo",  # uppercase vendor
        "x-.foo",  # empty vendor segment
        "x-acme.",  # empty verb segment
        "1abc.foo",  # leading digit on prefix
        "x-acme",  # vendor without verb segment
        "weird.but.not-tier1",
    ],
)
def test_non_tier1_non_tier2_token_is_rejected(token: str) -> None:
    """Tokens outside both tiers → invalid-capability-syntax."""
    with pytest.raises(ManifestValidationError) as exc:
        classify_capability_token(token)
    assert exc.value.code is ValidationErrorCode.INVALID_CAPABILITY_SYNTAX


# ---------------------------------------------------------------------------
# extract_capability_name
# ---------------------------------------------------------------------------


def test_extract_name_from_string_entry() -> None:
    assert extract_capability_name("oci.pull") == "oci.pull"


def test_extract_name_from_object_entry() -> None:
    entry = {"name": "oci.legacy-copy", "deprecated": True, "since": "2.4.0"}
    assert extract_capability_name(entry) == "oci.legacy-copy"


def test_extract_name_rejects_unknown_shape() -> None:
    with pytest.raises(TypeError):
        extract_capability_name(42)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# is_deprecated_entry
# ---------------------------------------------------------------------------


def test_string_entry_is_not_deprecated() -> None:
    assert is_deprecated_entry("oci.pull") is False


def test_object_entry_without_deprecated_flag_is_not_deprecated() -> None:
    assert is_deprecated_entry({"name": "oci.pull"}) is False


def test_object_entry_with_deprecated_true_is_deprecated() -> None:
    assert (
        is_deprecated_entry({"name": "oci.legacy-copy", "deprecated": True, "since": "2.4.0"})
        is True
    )
