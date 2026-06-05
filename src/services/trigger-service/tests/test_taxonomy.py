"""Tests for the canonical platform event taxonomy (TS-IMPL-006)."""

from __future__ import annotations

import pytest

from custos_trigger.taxonomy import (
    CANONICAL_EVENT_KINDS,
    KIND_PATTERN,
    PLATFORM_DOMAINS,
    InvalidKindError,
    is_canonical_kind,
    is_platform_domain,
    kind_domain,
    validate_kind,
)

#: The closed platform registry locked by TS-IMPL-006, mirrored verbatim from
#: design § Event Taxonomy. The grid pins every domain and its kind list so any
#: addition, removal, or rename must update this table deliberately.
_EXPECTED_DOMAINS: dict[str, frozenset[str]] = {
    "manual": frozenset({"manual.fire"}),
    "cron": frozenset({"cron.tick"}),
    "webhook": frozenset({"webhook.received"}),
    "workflow": frozenset(
        {
            "workflow.started",
            "workflow.completed",
            "workflow.failed",
            "workflow.cancelled",
        }
    ),
    "run": frozenset(
        {
            "run.started",
            "run.completed",
            "run.failed",
            "run.cancelled",
        }
    ),
    "step": frozenset(
        {
            "step.started",
            "step.succeeded",
            "step.failed",
            "step.retry_scheduled",
            "step.waiting",
            "step.resumed",
            "step.timed_out",
        }
    ),
    "activity": frozenset(
        {
            "activity.scheduled",
            "activity.started",
            "activity.succeeded",
            "activity.failed",
            "activity.timed_out",
            "activity.cancelled",
        }
    ),
    "registry": frozenset(
        {
            "registry.push",
            "registry.tag",
            "registry.delete",
        }
    ),
    "pr": frozenset(
        {
            "pr.opened",
            "pr.merged",
            "pr.closed",
            "pr.review_requested",
            "pr.synchronized",
        }
    ),
    "scan": frozenset(
        {
            "scan.started",
            "scan.completed",
            "scan.failed",
            "scan.vulnerable",
        }
    ),
}


# ---------------------------------------------------------------------------
# Registry shape (enum-grid)
# ---------------------------------------------------------------------------


def test_platform_domains_grid_is_exhaustive_and_pinned() -> None:
    assert dict(PLATFORM_DOMAINS) == _EXPECTED_DOMAINS


def test_canonical_kinds_is_the_flat_union() -> None:
    expected = frozenset(kind for kinds in _EXPECTED_DOMAINS.values() for kind in kinds)
    assert expected == CANONICAL_EVENT_KINDS


def test_no_kind_appears_in_two_domains() -> None:
    counts: dict[str, int] = {}
    for kinds in PLATFORM_DOMAINS.values():
        for kind in kinds:
            counts[kind] = counts.get(kind, 0) + 1
    assert all(c == 1 for c in counts.values())


def test_every_canonical_kind_matches_shape_and_its_domain() -> None:
    for domain, kinds in PLATFORM_DOMAINS.items():
        for kind in kinds:
            assert validate_kind(kind) == kind
            assert kind_domain(kind) == domain
            assert kind.startswith(f"{domain}.")


def test_platform_domains_mapping_is_read_only() -> None:
    with pytest.raises(TypeError):
        PLATFORM_DOMAINS["evil"] = frozenset()  # type: ignore[index]


# ---------------------------------------------------------------------------
# is_canonical_kind / is_platform_domain / kind_domain
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("kind", sorted(CANONICAL_EVENT_KINDS))
def test_is_canonical_kind_true_for_every_platform_kind(kind: str) -> None:
    assert is_canonical_kind(kind) is True


@pytest.mark.parametrize(
    "kind",
    ["ghcr.push", "github.pr_opened", "acr.image_pushed", "workflow.unknown", "nope"],
)
def test_is_canonical_kind_false_for_non_platform(kind: str) -> None:
    assert is_canonical_kind(kind) is False


@pytest.mark.parametrize("domain", sorted(PLATFORM_DOMAINS))
def test_is_platform_domain_true_for_registry(domain: str) -> None:
    assert is_platform_domain(domain) is True


@pytest.mark.parametrize("domain", ["ghcr", "github", "acr", "vendor", "Manual"])
def test_is_platform_domain_false_for_vendor(domain: str) -> None:
    assert is_platform_domain(domain) is False


def test_kind_domain_extracts_first_segment() -> None:
    assert kind_domain("registry.push") == "registry"
    assert kind_domain("ghcr.image.push") == "ghcr"
    assert kind_domain("nodot") == "nodot"


# ---------------------------------------------------------------------------
# validate_kind — vendor shape accepted
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "kind",
    [
        "ghcr.push",
        "github.pr_opened",
        "acr.image_pushed",
        "vendor.some_event",
        "ghcr.image.pushed",  # nested vendor namespace
    ],
)
def test_validate_kind_accepts_vendor_shape(kind: str) -> None:
    assert validate_kind(kind) == kind


# ---------------------------------------------------------------------------
# validate_kind — malformed shape rejected
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "kind",
    [
        "",  # empty
        "nodot",  # no domain separator
        "Workflow.completed",  # uppercase domain
        "workflow.Completed",  # uppercase event
        "1workflow.completed",  # domain starts with digit
        "workflow.",  # trailing dot / empty event
        ".completed",  # empty domain
        "work flow.completed",  # space
        "workflow..completed",  # empty middle segment
        "work-flow.completed",  # hyphen in domain
        "workflow.completed!",  # punctuation
    ],
)
def test_validate_kind_rejects_malformed(kind: str) -> None:
    with pytest.raises(InvalidKindError) as ei:
        validate_kind(kind)
    assert ei.value.kind == kind
    assert "does not match" in ei.value.reason


# ---------------------------------------------------------------------------
# validate_kind — platform-collision guard
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "kind",
    [
        "workflow.unknown",  # platform domain, non-canonical event
        "registry.deleted",  # near-miss of registry.delete
        "manual.fired",  # near-miss of manual.fire
        "step.done",  # not a real step kind
        "pr.reopened",  # not enumerated
    ],
)
def test_validate_kind_rejects_platform_domain_collision(kind: str) -> None:
    with pytest.raises(InvalidKindError) as ei:
        validate_kind(kind)
    assert ei.value.kind == kind
    assert "platform-owned" in ei.value.reason


def test_invalid_kind_error_is_a_value_error() -> None:
    assert issubclass(InvalidKindError, ValueError)
    with pytest.raises(ValueError):
        validate_kind("workflow.unknown")


def test_validate_kind_message_includes_kind() -> None:
    err = InvalidKindError("bad.kind", "because")
    assert "bad.kind" in str(err)
    assert err.reason == "because"


def test_kind_pattern_is_the_documented_regex() -> None:
    assert KIND_PATTERN == r"^[a-z][a-z0-9]*(\.[a-z][a-z0-9_]*)+$"
