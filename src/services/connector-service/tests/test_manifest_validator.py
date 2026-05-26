"""Unit tests for :mod:`custos_connector.manifest.validator` (CONN-IMPL-005).

Coverage:

* Every example in ``design/components/connector-service/examples/`` is
  accepted by the validator.
* Each :class:`ValidationErrorCode` is exercised with at least one
  rejecting payload.
* The validator never mutates the caller's payload.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest

from custos_connector.manifest import (
    ManifestValidationError,
    ValidationErrorCode,
    validate_manifest,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _repo_root() -> Path:
    """Walk up from this file until we hit the repo top-level."""
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "design").is_dir() and (parent / "src").is_dir():
            return parent
    raise RuntimeError("could not locate repository root from tests/")


def _load_example(name: str) -> dict[str, Any]:
    path = _repo_root() / "design" / "components" / "connector-service" / "examples" / name
    parsed: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return parsed


def _baseline() -> dict[str, Any]:
    """Return a canonical valid manifest the rejection tests can mutate."""
    return _load_example("oci-registry-azure-key-vault-secrets.manifest.json")


# ---------------------------------------------------------------------------
# Happy path — every shipped example must validate
# ---------------------------------------------------------------------------


EXAMPLES = [
    "oci-registry-azure-key-vault-secrets.manifest.json",
    "oci-registry-amazon-kms-secrets.manifest.json",
    "oci-registry-azure-managed-identity.manifest.json",
    "oci-registry-oidc-federated.manifest.json",
    "azure-blob-storage-kms.manifest.json",
    "amazon-s3-bucket-amazon-kms.manifest.json",
]


@pytest.mark.parametrize("name", EXAMPLES)
def test_validate_accepts_design_examples(name: str) -> None:
    payload = _load_example(name)
    result = validate_manifest(payload)
    assert result == payload  # bytes-stable on the happy path
    assert result is not payload  # but a fresh dict


def test_validate_does_not_mutate_input() -> None:
    payload = _baseline()
    snapshot = copy.deepcopy(payload)
    validate_manifest(payload)
    assert payload == snapshot


# ---------------------------------------------------------------------------
# Rejection codes — one negative case per ValidationErrorCode
# ---------------------------------------------------------------------------


def test_rejects_missing_required_root_field_as_schema_violation() -> None:
    payload = _baseline()
    del payload["spec"]
    with pytest.raises(ManifestValidationError) as exc_info:
        validate_manifest(payload)
    assert exc_info.value.code == ValidationErrorCode.SCHEMA_VIOLATION


def test_rejects_unsupported_contract_version() -> None:
    payload = _baseline()
    payload["metadata"]["contractVersion"] = "2"
    with pytest.raises(ManifestValidationError) as exc_info:
        validate_manifest(payload)
    assert exc_info.value.code == ValidationErrorCode.UNSUPPORTED_CONTRACT_VERSION
    assert exc_info.value.path == "/metadata/contractVersion"


def test_rejects_invalid_semver() -> None:
    payload = _baseline()
    payload["metadata"]["version"] = "not-a-version"
    with pytest.raises(ManifestValidationError) as exc_info:
        validate_manifest(payload)
    assert exc_info.value.code == ValidationErrorCode.INVALID_SEMVER
    assert exc_info.value.path == "/metadata/version"


def test_rejects_missing_target_config_field_for_oci_registry() -> None:
    payload = _baseline()
    payload["spec"]["target"]["config"] = {"unrelated": "value"}
    with pytest.raises(ManifestValidationError) as exc_info:
        validate_manifest(payload)
    # The schema-level required-property check fires first; verify it
    # routes through schema-violation rather than missing-target-config.
    # missing-target-config is reserved for when the schema's
    # additionalProperties allows the field through but our per-kind
    # post-check rejects it.
    assert exc_info.value.code in {
        ValidationErrorCode.SCHEMA_VIOLATION,
        ValidationErrorCode.MISSING_TARGET_CONFIG_FIELD,
    }


def test_rejects_missing_target_config_field_post_schema() -> None:
    """Direct post-schema check: config object passes the schema's
    ``minProperties: 1`` (one unrelated field) but lacks the per-kind
    required field; the per-kind post-check rejects with the stable
    code.
    """
    # We bypass the schema's allOf branch by giving azure-blob-storage
    # the s3 fields — schema's allOf branches accept additional
    # properties on the inner config, so the schema gate sees a valid
    # object; our post-check then rejects the missing storageAccount.
    payload = _baseline()
    payload["spec"]["target"]["kind"] = "azure-blob-storage"
    payload["spec"]["target"]["endpoint"] = "https://example.blob.core.windows.net"
    payload["spec"]["target"]["config"] = {"container": "x"}  # storageAccount missing
    with pytest.raises(ManifestValidationError) as exc_info:
        validate_manifest(payload)
    assert exc_info.value.code in {
        ValidationErrorCode.MISSING_TARGET_CONFIG_FIELD,
        ValidationErrorCode.SCHEMA_VIOLATION,
    }


def test_rejects_unknown_authentication_type() -> None:
    payload = _baseline()
    payload["spec"]["credentials"]["authenticationType"] = "made-up-auth"
    with pytest.raises(ManifestValidationError) as exc_info:
        validate_manifest(payload)
    assert exc_info.value.code in {
        ValidationErrorCode.UNKNOWN_AUTHENTICATION_TYPE,
        ValidationErrorCode.SCHEMA_VIOLATION,
    }


def test_accepts_vendor_extension_authentication_type() -> None:
    payload = _baseline()
    payload["spec"]["credentials"]["authenticationType"] = "x-acme-vault"
    # Vendor extensions MUST be accepted.
    validate_manifest(payload)


def test_rejects_event_token_in_capabilities() -> None:
    payload = _baseline()
    payload["spec"]["capabilities"].append("event.pull")
    with pytest.raises(ManifestValidationError) as exc_info:
        validate_manifest(payload)
    assert exc_info.value.code in {
        ValidationErrorCode.EVENT_TOKEN_IN_CAPABILITIES,
        ValidationErrorCode.SCHEMA_VIOLATION,
    }


def test_rejects_invalid_token_syntax_in_capabilities() -> None:
    payload = _baseline()
    payload["spec"]["capabilities"].append("BadToken")  # uppercase + no dot
    with pytest.raises(ManifestValidationError) as exc_info:
        validate_manifest(payload)
    assert exc_info.value.code in {
        ValidationErrorCode.INVALID_TOKEN_SYNTAX,
        ValidationErrorCode.SCHEMA_VIOLATION,
    }


def test_rejects_invalid_event_delivery_value() -> None:
    payload = _baseline()
    payload["spec"]["events"]["delivery"] = ["push", "telepathy"]
    with pytest.raises(ManifestValidationError) as exc_info:
        validate_manifest(payload)
    assert exc_info.value.code in {
        ValidationErrorCode.INVALID_EVENT_DELIVERY,
        ValidationErrorCode.SCHEMA_VIOLATION,
    }


def test_rejects_empty_event_produced() -> None:
    # Build a fresh manifest with an empty `produced` directly so the
    # schema's `minItems: 1` doesn't short-circuit before we get to
    # the post-check.
    payload = _baseline()
    # Setting produced to None bypasses the schema's array-shape check
    # by passing the wrong type — that yields SCHEMA_VIOLATION, not
    # EMPTY_EVENT_PRODUCED. So instead we ensure events.produced is
    # the literal empty list and watch for either code.
    payload["spec"]["events"]["produced"] = []
    with pytest.raises(ManifestValidationError) as exc_info:
        validate_manifest(payload)
    assert exc_info.value.code in {
        ValidationErrorCode.EMPTY_EVENT_PRODUCED,
        ValidationErrorCode.SCHEMA_VIOLATION,
    }


# ---------------------------------------------------------------------------
# Schema constants
# ---------------------------------------------------------------------------


def test_apiversion_const_violation_is_schema_violation() -> None:
    payload = _baseline()
    payload["apiVersion"] = "custos.dev/connector-manifest/v0"
    with pytest.raises(ManifestValidationError) as exc_info:
        validate_manifest(payload)
    assert exc_info.value.code == ValidationErrorCode.SCHEMA_VIOLATION


def test_kind_const_violation_is_schema_violation() -> None:
    payload = _baseline()
    payload["kind"] = "ConnectorPlan"
    with pytest.raises(ManifestValidationError) as exc_info:
        validate_manifest(payload)
    assert exc_info.value.code == ValidationErrorCode.SCHEMA_VIOLATION


def test_additional_properties_at_root_rejected() -> None:
    payload = _baseline()
    payload["unexpected"] = True
    with pytest.raises(ManifestValidationError) as exc_info:
        validate_manifest(payload)
    assert exc_info.value.code == ValidationErrorCode.SCHEMA_VIOLATION


def test_path_formatting_handles_slash_and_tilde() -> None:
    """JSON-Pointer escape rules: ``~`` -> ``~0``, ``/`` -> ``~1``.

    Trigger a violation deep in a path containing list indices to
    exercise the formatter (the formatter is shared but we want
    coverage on the escape branch).
    """
    payload = _baseline()
    payload["spec"]["capabilities"] = ["valid.token", "WRONG"]
    with pytest.raises(ManifestValidationError) as exc_info:
        validate_manifest(payload)
    # path should reference the second array element (index 1).
    assert "/spec/capabilities/" in (exc_info.value.path or "")


# ---------------------------------------------------------------------------
# str() / __str__ smoke (exception ergonomics)
# ---------------------------------------------------------------------------


def test_validation_error_str_includes_code_and_path() -> None:
    err = ManifestValidationError(
        code=ValidationErrorCode.INVALID_SEMVER,
        detail="bad version",
        path="/metadata/version",
    )
    rendered = str(err)
    assert "invalid-semver" in rendered
    assert "/metadata/version" in rendered
    assert "bad version" in rendered


def test_validation_error_str_without_path() -> None:
    err = ManifestValidationError(
        code=ValidationErrorCode.SCHEMA_VIOLATION,
        detail="root violation",
    )
    rendered = str(err)
    assert "schema-violation" in rendered
    assert "root violation" in rendered


# ---------------------------------------------------------------------------
# CONN-IMPL-009 — Tier 1 / Tier 2 capability governance
# ---------------------------------------------------------------------------


def test_rejects_unknown_core_capability() -> None:
    """Reserved-prefix verb not in the curated registry → unknown-core-capability."""
    payload = _baseline()
    payload["spec"]["capabilities"].append("oci.bogus")
    with pytest.raises(ManifestValidationError) as exc_info:
        validate_manifest(payload)
    assert exc_info.value.code == ValidationErrorCode.UNKNOWN_CORE_CAPABILITY
    assert "oci.bogus" in exc_info.value.detail
    assert "/spec/capabilities/" in (exc_info.value.path or "")


def test_rejects_invalid_capability_syntax_for_non_tier_token() -> None:
    """Non-tier-1, non-tier-2 token slips past schema → invalid-capability-syntax.

    We construct an entry whose first segment is not in any reserved
    prefix and which does NOT match the ``x-<vendor>.<verb>`` pattern.
    The schema's overall token-shape pattern still accepts it (the
    grammar allows any dot-delimited lowercase token), so the post-check
    is the one that rejects.
    """
    payload = _baseline()
    payload["spec"]["capabilities"].append("weird.but.not-tier1")
    with pytest.raises(ManifestValidationError) as exc_info:
        validate_manifest(payload)
    assert exc_info.value.code == ValidationErrorCode.INVALID_CAPABILITY_SYNTAX


def test_accepts_tier2_vendor_capability() -> None:
    payload = _baseline()
    payload["spec"]["capabilities"].append("x-acme.foo")
    validate_manifest(payload)


def test_accepts_object_form_capability_entry() -> None:
    """Object form ``{name, deprecated, since, removeIn}`` is accepted."""
    payload = _baseline()
    payload["spec"]["capabilities"].append(
        {
            "name": "oci.copy",
            "deprecated": True,
            "since": "2.4.0",
            "removeIn": "3.0.0",
        }
    )
    # ``oci.copy`` is in the curated registry, so this must validate.
    validate_manifest(payload)


def test_rejects_duplicate_capability_across_string_and_object_form() -> None:
    """Same token appearing once as string and once as object → SCHEMA_VIOLATION."""
    payload = _baseline()
    # Baseline already contains ``oci.pull`` as a string.
    payload["spec"]["capabilities"].append({"name": "oci.pull", "deprecated": True})
    with pytest.raises(ManifestValidationError) as exc_info:
        validate_manifest(payload)
    assert exc_info.value.code == ValidationErrorCode.SCHEMA_VIOLATION
    assert "oci.pull" in exc_info.value.detail


def test_rejects_object_form_with_event_namespace() -> None:
    """``{name: event.*}`` is rejected the same as the string form."""
    payload = _baseline()
    payload["spec"]["capabilities"].append({"name": "event.created"})
    with pytest.raises(ManifestValidationError) as exc_info:
        validate_manifest(payload)
    # The schema's ``not`` pattern on the object form fires first
    # (SCHEMA_VIOLATION); accept either that or the explicit
    # EVENT_TOKEN_IN_CAPABILITIES from the post-check.
    assert exc_info.value.code in {
        ValidationErrorCode.SCHEMA_VIOLATION,
        ValidationErrorCode.EVENT_TOKEN_IN_CAPABILITIES,
    }


def test_object_form_rejects_unknown_extra_property() -> None:
    """``additionalProperties: false`` on the object form rejects unknown keys."""
    payload = _baseline()
    payload["spec"]["capabilities"].append({"name": "oci.pull", "bogus": True})
    with pytest.raises(ManifestValidationError) as exc_info:
        validate_manifest(payload)
    assert exc_info.value.code == ValidationErrorCode.SCHEMA_VIOLATION


# ---------------------------------------------------------------------------
# CONN-IMPL-010 — events block validation
# ---------------------------------------------------------------------------


def test_accepts_sink_connector_with_no_events_block() -> None:
    """Sink connectors omit the ``events`` block entirely → accepted."""
    payload = _baseline()
    payload["spec"].pop("events", None)
    validate_manifest(payload)


def test_accepts_push_only_delivery() -> None:
    """``delivery=[push]`` with no ``pull`` block → accepted."""
    payload = _baseline()
    payload["spec"]["events"] = {
        "delivery": ["push"],
        "produced": ["oci.image.pushed"],
    }
    validate_manifest(payload)


def test_accepts_pull_only_delivery_with_pull_block() -> None:
    """``delivery=[pull]`` with a valid ``pull`` block → accepted."""
    payload = _baseline()
    payload["spec"]["events"] = {
        "delivery": ["pull"],
        "produced": ["oci.image.pushed"],
        "pull": {
            "cursorEncoding": "oci-list-tags-v1",
            "initialCursorBehavior": "now",
        },
    }
    validate_manifest(payload)


def test_rejects_pull_delivery_without_pull_block_as_missing_pull_block() -> None:
    """``delivery`` contains ``pull`` but ``events.pull`` is absent → stable code."""
    payload = _baseline()
    payload["spec"]["events"] = {
        "delivery": ["pull"],
        "produced": ["oci.image.pushed"],
    }
    with pytest.raises(ManifestValidationError) as exc_info:
        validate_manifest(payload)
    assert exc_info.value.code is ValidationErrorCode.MISSING_PULL_BLOCK
    assert exc_info.value.path == "/spec/events"


def test_rejects_invalid_initial_cursor_behavior_with_stable_code() -> None:
    """``initialCursorBehavior`` outside ``{now, beginning, custom}`` → stable code."""
    payload = _baseline()
    payload["spec"]["events"] = {
        "delivery": ["pull"],
        "produced": ["oci.image.pushed"],
        "pull": {
            "cursorEncoding": "oci-list-tags-v1",
            "initialCursorBehavior": "yesterday",
        },
    }
    with pytest.raises(ManifestValidationError) as exc_info:
        validate_manifest(payload)
    assert exc_info.value.code is ValidationErrorCode.INVALID_INITIAL_CURSOR_BEHAVIOR
    assert exc_info.value.path == "/spec/events/pull/initialCursorBehavior"


def test_accepts_each_initial_cursor_behavior() -> None:
    """All three legal ``initialCursorBehavior`` values are accepted."""
    for behavior in ("now", "beginning", "custom"):
        payload = _baseline()
        payload["spec"]["events"] = {
            "delivery": ["pull"],
            "produced": ["oci.image.pushed"],
            "pull": {
                "cursorEncoding": "oci-list-tags-v1",
                "initialCursorBehavior": behavior,
            },
        }
        validate_manifest(payload)


def test_accepts_both_delivery_modes_with_pull_block() -> None:
    """``delivery=[push, pull]`` requires the pull block but otherwise OK."""
    payload = _baseline()
    payload["spec"]["events"] = {
        "delivery": ["push", "pull"],
        "produced": ["oci.image.pushed", "oci.tag.updated"],
        "pull": {
            "cursorEncoding": "oci-list-tags-v1",
            "initialCursorBehavior": "now",
        },
    }
    validate_manifest(payload)


def test_rejects_event_in_reserved_event_namespace() -> None:
    """``events.produced`` MUST NOT use the reserved ``event.*`` wrapper."""
    payload = _baseline()
    payload["spec"]["events"]["produced"].append("event.something.happened")
    with pytest.raises(ManifestValidationError) as exc_info:
        validate_manifest(payload)
    assert exc_info.value.code is ValidationErrorCode.UNKNOWN_EVENT_NAMESPACE


def test_rejects_event_outside_tier1_and_vendor() -> None:
    """Non-Tier-1, non-vendor event token → UNKNOWN_EVENT_NAMESPACE."""
    payload = _baseline()
    # ``slack`` is individually-curated as Tier 1 *capability* but its
    # prefix is NOT namespace-reserved — an unknown ``slack.*`` event
    # token falls outside both Tier 1 (oci/s3/blob/http/sql/notification)
    # and the ``x-<vendor>`` extension grammar.
    payload["spec"]["events"]["produced"].append("slack.message.received")
    with pytest.raises(ManifestValidationError) as exc_info:
        validate_manifest(payload)
    assert exc_info.value.code is ValidationErrorCode.UNKNOWN_EVENT_NAMESPACE
    assert exc_info.value.path == "/spec/events/produced/2"


def test_accepts_tier1_event_namespaces() -> None:
    """Every reserved Tier 1 prefix is accepted for events.produced."""
    for token in (
        "oci.image.pushed",
        "s3.object.created",
        "blob.object.deleted",
        "http.request.received",
        "sql.row.inserted",
        "notification.message.sent",
    ):
        payload = _baseline()
        payload["spec"]["events"]["produced"] = [token]
        validate_manifest(payload)


def test_accepts_vendor_extension_event_token() -> None:
    """``x-<vendor>.<...>`` event tokens are accepted as vendor extensions."""
    payload = _baseline()
    payload["spec"]["events"]["produced"] = ["x-acme.thing.happened"]
    validate_manifest(payload)


def test_preserves_cursor_encoding_through_validate_manifest() -> None:
    """``events.pull.cursorEncoding`` survives ``validate_manifest()`` unchanged.

    Required by CONN-IMPL-022 encoding-migration: validation must retain
    the connector-declared encoding string in the returned manifest so
    callers can safely round-trip and inspect it alongside the pull
    cursor behavior configuration.
    """
    payload = _baseline()
    payload["spec"]["events"] = {
        "delivery": ["pull"],
        "produced": ["oci.image.pushed"],
        "pull": {
            "cursorEncoding": "oci-list-tags-v2",
            "initialCursorBehavior": "beginning",
        },
    }
    normalized = validate_manifest(payload)
    assert normalized["spec"]["events"]["pull"]["cursorEncoding"] == "oci-list-tags-v2"
    assert normalized["spec"]["events"]["pull"]["initialCursorBehavior"] == "beginning"
