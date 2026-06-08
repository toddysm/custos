"""Tests for the migration runner: schema-revision negotiation.

Pure-Python contract tests — no live Postgres. SPL-013 / SPL-017 will
exercise `apply_pending()` against real backends.
"""

from __future__ import annotations

from collections.abc import Mapping
from collections.abc import Set as AbstractSet

import pytest

from custos_spl import (
    ArtifactStoreProvider,
    AuthStoreProvider,
    CatalogStoreProvider,
    DefinitionStoreProvider,
    LeaseStoreProvider,
    MetadataStoreProvider,
    MigrationCapable,
    MigrationRequired,
    check_revisions,
    required_revisions,
)

# ----- required_revisions -----


def test_required_revisions_pinned_to_protocol_class_vars() -> None:
    """`required_revisions` must read each Protocol's SCHEMA_REVISION."""
    req = required_revisions()
    assert req["MetadataStoreProvider"] == MetadataStoreProvider.SCHEMA_REVISION
    assert req["DefinitionStoreProvider"] == DefinitionStoreProvider.SCHEMA_REVISION
    assert req["CatalogStoreProvider"] == CatalogStoreProvider.SCHEMA_REVISION
    assert req["AuthStoreProvider"] == AuthStoreProvider.SCHEMA_REVISION
    assert req["ArtifactStoreProvider"] == ArtifactStoreProvider.SCHEMA_REVISION
    assert req["LeaseStoreProvider"] == LeaseStoreProvider.SCHEMA_REVISION


def test_required_revisions_excludes_query_facades() -> None:
    """LogQueryProvider/MetricsQueryProvider pin SCHEMA_REVISION to 0
    and own no schema — they must not appear."""
    req = required_revisions()
    assert "LogQueryProvider" not in req
    assert "MetricsQueryProvider" not in req


def test_required_revisions_returns_int_values() -> None:
    for name, rev in required_revisions().items():
        assert isinstance(rev, int), f"{name} required revision is not int"
        assert rev >= 1, f"{name} stateful interface should be at rev >= 1"


# ----- MigrationCapable Protocol -----


class _FakeAdapter:
    """A minimal adapter for negotiation tests."""

    def __init__(
        self,
        declared: Mapping[str, AbstractSet[int]],
        *,
        applied_summaries: list[str] | None = None,
    ) -> None:
        self._declared = dict(declared)
        self._applied_summaries = applied_summaries or []
        self.apply_called = 0

    @property
    def declared_revisions(self) -> Mapping[str, AbstractSet[int]]:
        return self._declared

    async def apply_pending(self) -> list[str]:
        self.apply_called += 1
        return list(self._applied_summaries)


def test_fake_adapter_satisfies_migration_capable() -> None:
    adapter = _FakeAdapter({"MetadataStoreProvider": {1, 2, 3, 4}})
    assert isinstance(adapter, MigrationCapable)


def test_non_capable_object_does_not_satisfy_protocol() -> None:
    class _MissingAttrs:
        pass

    assert not isinstance(_MissingAttrs(), MigrationCapable)


# ----- check_revisions: success -----


def test_check_revisions_passes_when_all_revisions_present() -> None:
    """An adapter that declares every required revision satisfies the check."""
    req = required_revisions()
    declared = {iface: set(range(1, rev + 1)) for iface, rev in req.items()}
    adapter = _FakeAdapter(declared)
    # Must not raise.
    check_revisions([adapter])


def test_check_revisions_unions_across_multiple_adapters() -> None:
    """Two adapters can collectively cover what one cannot."""
    req = required_revisions()
    # Adapter A covers Metadata + Definition; B covers Catalog + Auth + Artifact + Lease.
    a = _FakeAdapter(
        {
            "MetadataStoreProvider": set(range(1, req["MetadataStoreProvider"] + 1)),
            "DefinitionStoreProvider": set(range(1, req["DefinitionStoreProvider"] + 1)),
        }
    )
    b = _FakeAdapter(
        {
            "CatalogStoreProvider": set(range(1, req["CatalogStoreProvider"] + 1)),
            "AuthStoreProvider": set(range(1, req["AuthStoreProvider"] + 1)),
            "ArtifactStoreProvider": set(range(1, req["ArtifactStoreProvider"] + 1)),
            "LeaseStoreProvider": set(range(1, req["LeaseStoreProvider"] + 1)),
        }
    )
    check_revisions([a, b])


def test_check_revisions_skips_non_capable_objects() -> None:
    """Stateless objects (e.g. query-facade adapters) pass through silently."""
    req = required_revisions()
    capable = _FakeAdapter({iface: set(range(1, rev + 1)) for iface, rev in req.items()})

    class _StatelessFacade:
        # No declared_revisions, no apply_pending — must be skipped.
        pass

    check_revisions([capable, _StatelessFacade()])


# ----- check_revisions: failure -----


def test_check_revisions_raises_when_required_revision_missing() -> None:
    """Adapter at lower revision must surface a gap for the platform's required level."""
    req = required_revisions()
    # Cover everything EXCEPT the topmost MetadataStoreProvider revision.
    declared = {iface: set(range(1, rev + 1)) for iface, rev in req.items()}
    declared["MetadataStoreProvider"].discard(req["MetadataStoreProvider"])
    adapter = _FakeAdapter(declared)

    with pytest.raises(MigrationRequired) as exc:
        check_revisions([adapter])
    assert ("MetadataStoreProvider", req["MetadataStoreProvider"]) in exc.value.gaps


def test_check_revisions_with_no_adapters_is_noop() -> None:
    """With nothing deployed there is nothing to migrate, so no gaps.

    The check is scoped to interfaces a deployed MigrationCapable adapter
    owns; an empty adapter set owns nothing and must not raise.
    """
    check_revisions([])


def test_check_revisions_skips_unowned_interfaces() -> None:
    """A required interface with no deployed adapter is not gated.

    Mirrors the ArtifactStoreProvider case: it is required platform-wide
    but its only adapter (object storage) is not MigrationCapable, so a
    Postgres-only migrate run that satisfies every interface it *does*
    own must pass.
    """
    req = required_revisions()
    owned = {
        iface: set(range(1, rev + 1))
        for iface, rev in req.items()
        if iface != "ArtifactStoreProvider"
    }
    # No adapter owns ArtifactStoreProvider, yet the check must pass.
    check_revisions([_FakeAdapter(owned)])


def test_check_revisions_raises_for_owned_but_behind_interface() -> None:
    """A deployed adapter that owns an interface but is behind still gates."""
    req = required_revisions()
    # Owns Auth but declares no revisions (fresh, unmigrated store).
    adapter = _FakeAdapter({"AuthStoreProvider": set()})
    with pytest.raises(MigrationRequired) as exc:
        check_revisions([adapter])
    assert ("AuthStoreProvider", req["AuthStoreProvider"]) in exc.value.gaps


def test_check_revisions_reports_sorted_gaps() -> None:
    """Stable ordering keeps operator logs diff-friendly across runs."""
    # Two owned-but-unmigrated interfaces produce multiple gaps.
    adapter = _FakeAdapter({"AuthStoreProvider": set(), "MetadataStoreProvider": set()})
    with pytest.raises(MigrationRequired) as exc:
        check_revisions([adapter])
    assert exc.value.gaps == sorted(exc.value.gaps)
