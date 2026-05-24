"""Tests for :mod:`custos_catalog.managers.activity_registry` (CS-IMPL-015)."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, ClassVar

import pytest
from custos_spl.errors import ConflictDigest
from custos_spl.interfaces.catalog_store import ActivityTypeVersion
from custos_spl.pagination import Cursor, Page

from custos_catalog.managers.activity_registry import (
    ActivityManifestError,
    ActivityNamespaceError,
    ActivityRegistryConflict,
    ActivityTypeNotFound,
    ActivityTypeRef,
    ActivityTypeRegistry,
)
from tests._fakes import FakeMetadataStore

WS = "ws-1"
ADMIN = "alice"
USER = "bob"


# ---------------------------------------------------------------------------
# Hand-rolled fake store
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FakeCatalogStore:
    """In-memory ``CatalogStoreProvider`` for the activity-type surface.

    Captures every ``put`` for assertion and lets tests inject digest
    conflicts via ``conflict_for``. ``parent_deprecated[(ns, type)]``
    drives the denormalized flag exposed on returned rows.
    """

    SCHEMA_REVISION: ClassVar[int] = 1

    activity_versions: dict[tuple[str, str, str], ActivityTypeVersion] = field(default_factory=dict)
    parent_deprecated: dict[tuple[str, str], bool] = field(default_factory=dict)
    conflict_for: dict[tuple[str, str, str], str] = field(default_factory=dict)
    put_calls: list[tuple[str, str, str, str]] = field(default_factory=list)
    list_calls: list[tuple[str, str]] = field(default_factory=list)
    deprecate_calls: list[tuple[str, str, bool]] = field(default_factory=list)

    # ---- activity surface ----

    async def put_activity_type_version(
        self,
        namespace: str,
        type: str,
        version: str,
        digest: str,
        normalized_manifest: Mapping[str, Any],
    ) -> ActivityTypeVersion:
        key = (namespace, type, version)
        self.put_calls.append((namespace, type, version, digest))
        if key in self.conflict_for and self.conflict_for[key] != digest:
            raise ConflictDigest(
                f"{namespace}/{type}@{version} stored with digest "
                f"{self.conflict_for[key]} != supplied {digest}",
            )
        existing = self.activity_versions.get(key)
        if existing is not None and existing.digest != digest:
            raise ConflictDigest(
                f"{namespace}/{type}@{version} stored with digest "
                f"{existing.digest} != supplied {digest}",
            )
        row = ActivityTypeVersion(
            namespace=namespace,
            type=type,
            version=version,
            digest=digest,
            normalized_manifest=dict(normalized_manifest),
            parent_deprecated=self.parent_deprecated.get((namespace, type), False),
            published_at=datetime.now(tz=UTC),
        )
        self.activity_versions[key] = row
        return row

    async def get_activity_type_version(
        self,
        namespace: str,
        type: str,
        version: str,
    ) -> ActivityTypeVersion | None:
        row = self.activity_versions.get((namespace, type, version))
        if row is None:
            # Conflict path may also need a "stored" digest probe — surface from
            # conflict_for if present so we can still attach it to the error.
            stored_digest = self.conflict_for.get((namespace, type, version))
            if stored_digest is not None:
                return ActivityTypeVersion(
                    namespace=namespace,
                    type=type,
                    version=version,
                    digest=stored_digest,
                    normalized_manifest={},
                    parent_deprecated=self.parent_deprecated.get((namespace, type), False),
                    published_at=datetime.now(tz=UTC),
                )
            return None
        return self._with_parent_deprecation(row)

    async def list_activity_type_versions(
        self,
        namespace: str,
        type: str,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[ActivityTypeVersion]:
        self.list_calls.append((namespace, type))
        rows = [
            self._with_parent_deprecation(row)
            for (ns, tp, _), row in self.activity_versions.items()
            if ns == namespace and tp == type
        ]
        start = int(cursor.token) if cursor is not None else 0
        end = start + (limit or len(rows))
        slice_ = rows[start:end]
        next_cursor = Cursor(token=str(end)) if end < len(rows) else None
        return Page(items=slice_, next_cursor=next_cursor)

    async def set_activity_type_deprecated(
        self,
        namespace: str,
        type: str,
        deprecated: bool,
    ) -> None:
        self.parent_deprecated[(namespace, type)] = deprecated
        self.deprecate_calls.append((namespace, type, deprecated))

    async def resolve(
        self,
        namespace: str,
        type: str,
        semver_range: str,
    ) -> ActivityTypeVersion | None:
        # Find the highest exact version under (namespace, type) whose major
        # matches semver_range. Good enough for unit tests; the real adapter
        # implements proper range parsing.
        if self.parent_deprecated.get((namespace, type), False):
            return None
        rows = [
            row
            for (ns, tp, _), row in self.activity_versions.items()
            if ns == namespace and tp == type
        ]
        if not rows:
            return None
        matching = [row for row in rows if row.version.split(".")[0] == semver_range]
        if not matching:
            return None
        # Lexicographic on (int, int, int) tuples
        matching.sort(key=lambda r: tuple(int(p) for p in r.version.split(".")))
        return self._with_parent_deprecation(matching[-1])

    # ---- connector surface (unused here — present to satisfy the Protocol shape) ----

    async def put_connector_type_version(
        self, type: str, version: str, digest: str, normalized_manifest: Mapping[str, Any]
    ) -> Any:
        raise NotImplementedError

    async def get_connector_type_version(self, type: str, version: str) -> Any:
        return None

    async def list_connector_type_versions(
        self, type: str, cursor: Cursor | None = None, limit: int | None = None
    ) -> Page[Any]:
        return Page()

    async def set_connector_type_deprecated(self, type: str, deprecated: bool) -> None: ...

    # ---- helpers ----

    def _with_parent_deprecation(self, row: ActivityTypeVersion) -> ActivityTypeVersion:
        flag = self.parent_deprecated.get((row.namespace, row.type), False)
        if flag == row.parent_deprecated:
            return row
        return ActivityTypeVersion(
            namespace=row.namespace,
            type=row.type,
            version=row.version,
            digest=row.digest,
            normalized_manifest=row.normalized_manifest,
            parent_deprecated=flag,
            published_at=row.published_at,
        )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _manifest(
    *,
    namespace: str = "ws-1",
    type_: str = "scan-image",
    version: str = "1.0.0",
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Smallest envelope-valid activity manifest."""
    manifest: dict[str, Any] = {
        "apiVersion": "custos.dev/v1",
        "kind": "ActivityManifest",
        "metadata": {
            "namespace": namespace,
            "type": type_,
            "version": version,
        },
        "spec": {
            "contractVersion": "1",
            "runtime": {"kind": "oci-container", "image": "ghcr.io/x:v1", "digest": "sha256:abc"},
        },
    }
    if extra:
        manifest["spec"].update(extra)
    return manifest


def _make_registry(
    store: FakeCatalogStore,
    *,
    platform_admins: frozenset[str] | None = None,
    vendor_grants: Mapping[str, frozenset[str]] | None = None,
) -> ActivityTypeRegistry:
    return ActivityTypeRegistry(
        catalog_store=store,
        metadata_store=FakeMetadataStore(),  # type: ignore[arg-type]
        platform_admins=platform_admins,
        vendor_grants=vendor_grants,
    )


# ---------------------------------------------------------------------------
# Register — happy paths
# ---------------------------------------------------------------------------


async def test_register_workspace_namespace_returns_ref() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    ref = await registry.register(
        workspace_id=WS,
        principal_id=USER,
        manifest=_manifest(namespace=WS),
    )
    assert isinstance(ref, ActivityTypeRef)
    assert ref.namespace == WS
    assert ref.type == "scan-image"
    assert ref.version == "1.0.0"
    assert ref.digest.startswith("sha256:")
    assert (WS, "scan-image", "1.0.0") in store.activity_versions


async def test_register_persists_normalized_manifest_with_sorted_keys() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    await registry.register(
        workspace_id=WS,
        principal_id=USER,
        manifest=_manifest(
            namespace=WS,
            extra={
                "z_last": "z",
                "a_first": "a",
            },
        ),
    )
    row = store.activity_versions[(WS, "scan-image", "1.0.0")]
    spec_keys = list(row.normalized_manifest["spec"].keys())
    assert spec_keys == sorted(spec_keys)


async def test_register_identical_manifest_is_idempotent() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    ref1 = await registry.register(
        workspace_id=WS,
        principal_id=USER,
        manifest=_manifest(namespace=WS),
    )
    ref2 = await registry.register(
        workspace_id=WS,
        principal_id=USER,
        manifest=_manifest(namespace=WS),
    )
    assert ref1 == ref2
    assert len(store.put_calls) == 2


async def test_register_records_referrer_ref_in_audit_only() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    ref = await registry.register(
        workspace_id=WS,
        principal_id=USER,
        manifest=_manifest(namespace=WS),
        referrer_ref="oci://ghcr.io/example/manifest@sha256:abc",
    )
    # Referrer is captured in audit emission but not persisted on the row.
    row = store.activity_versions[(ref.namespace, ref.type, ref.version)]
    assert "referrer_ref" not in row.normalized_manifest


# ---------------------------------------------------------------------------
# Namespace tier rules
# ---------------------------------------------------------------------------


async def test_register_workspace_namespace_mismatch_rejected_as_vendor() -> None:
    """A workspace cannot publish into another workspace's namespace
    without an explicit vendor grant — it falls into the vendor tier."""
    store = FakeCatalogStore()
    registry = _make_registry(store)
    with pytest.raises(ActivityNamespaceError) as exc:
        await registry.register(
            workspace_id=WS,
            principal_id=USER,
            manifest=_manifest(namespace="ws-2"),
        )
    assert exc.value.tier == "vendor"
    assert exc.value.namespace == "ws-2"


async def test_register_reserved_namespace_requires_platform_admin() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    with pytest.raises(ActivityNamespaceError) as exc:
        await registry.register(
            workspace_id=WS,
            principal_id=USER,
            manifest=_manifest(namespace="custos.builtin"),
        )
    assert exc.value.tier == "platform"


async def test_register_reserved_namespace_succeeds_for_platform_admin() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store, platform_admins=frozenset({ADMIN}))
    ref = await registry.register(
        workspace_id=WS,
        principal_id=ADMIN,
        manifest=_manifest(namespace="custos.builtin"),
    )
    assert ref.namespace == "custos.builtin"


@pytest.mark.parametrize(
    "ns",
    ["custos.scanners", "system.notifier", "platform.core", "builtin.echo"],
)
async def test_register_other_reserved_prefixes_also_platform_only(ns: str) -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    with pytest.raises(ActivityNamespaceError) as exc:
        await registry.register(
            workspace_id=WS,
            principal_id=USER,
            manifest=_manifest(namespace=ns),
        )
    assert exc.value.tier == "platform"


async def test_register_vendor_namespace_requires_grant() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    with pytest.raises(ActivityNamespaceError) as exc:
        await registry.register(
            workspace_id=WS,
            principal_id=USER,
            manifest=_manifest(namespace="snyk"),
        )
    assert exc.value.tier == "vendor"


async def test_register_vendor_namespace_succeeds_with_grant() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(
        store,
        vendor_grants={WS: frozenset({"snyk"})},
    )
    ref = await registry.register(
        workspace_id=WS,
        principal_id=USER,
        manifest=_manifest(namespace="snyk"),
    )
    assert ref.namespace == "snyk"


async def test_register_vendor_grant_is_workspace_scoped() -> None:
    """A grant for ws-1 must not let ws-2 publish into the vendor namespace."""
    store = FakeCatalogStore()
    registry = _make_registry(
        store,
        vendor_grants={WS: frozenset({"snyk"})},
    )
    with pytest.raises(ActivityNamespaceError):
        await registry.register(
            workspace_id="ws-2",
            principal_id=USER,
            manifest=_manifest(namespace="snyk"),
        )


# ---------------------------------------------------------------------------
# Manifest envelope validation
# ---------------------------------------------------------------------------


async def test_register_rejects_wrong_api_version() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    bad = _manifest(namespace=WS)
    bad["apiVersion"] = "v0"
    with pytest.raises(ActivityManifestError) as exc:
        await registry.register(workspace_id=WS, principal_id=USER, manifest=bad)
    paths = {issue.path for issue in exc.value.issues}
    assert "/apiVersion" in paths


async def test_register_rejects_wrong_kind() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    bad = _manifest(namespace=WS)
    bad["kind"] = "Workflow"
    with pytest.raises(ActivityManifestError) as exc:
        await registry.register(workspace_id=WS, principal_id=USER, manifest=bad)
    paths = {issue.path for issue in exc.value.issues}
    assert "/kind" in paths


async def test_register_collects_multiple_envelope_issues() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    bad: dict[str, Any] = {"apiVersion": "v0", "kind": "Bad", "metadata": {}}
    with pytest.raises(ActivityManifestError) as exc:
        await registry.register(workspace_id=WS, principal_id=USER, manifest=bad)
    paths = {issue.path for issue in exc.value.issues}
    assert "/apiVersion" in paths
    assert "/kind" in paths
    assert "/metadata/namespace" in paths
    assert "/metadata/type" in paths
    assert "/metadata/version" in paths


async def test_register_rejects_missing_metadata() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    bad = {"apiVersion": "custos.dev/v1", "kind": "ActivityManifest"}
    with pytest.raises(ActivityManifestError) as exc:
        await registry.register(workspace_id=WS, principal_id=USER, manifest=bad)
    paths = {issue.path for issue in exc.value.issues}
    assert "/metadata" in paths


async def test_register_rejects_partial_semver() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    with pytest.raises(ActivityManifestError) as exc:
        await registry.register(
            workspace_id=WS,
            principal_id=USER,
            manifest=_manifest(namespace=WS, version="1.0"),
        )
    paths = {issue.path for issue in exc.value.issues}
    assert "/metadata/version" in paths


async def test_register_rejects_bad_namespace_token() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    with pytest.raises(ActivityManifestError) as exc:
        await registry.register(
            workspace_id=WS,
            principal_id=USER,
            manifest=_manifest(namespace="Bad Namespace"),
        )
    paths = {issue.path for issue in exc.value.issues}
    assert "/metadata/namespace" in paths


async def test_register_rejects_bad_type_token() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    with pytest.raises(ActivityManifestError) as exc:
        await registry.register(
            workspace_id=WS,
            principal_id=USER,
            manifest=_manifest(namespace=WS, type_="Bad Type"),
        )
    paths = {issue.path for issue in exc.value.issues}
    assert "/metadata/type" in paths


async def test_register_rejects_non_string_metadata_namespace() -> None:
    """A present-but-non-string ``metadata.namespace`` must be flagged so it
    never reaches the store or namespace-tier classifier as a non-str."""
    store = FakeCatalogStore()
    registry = _make_registry(store)
    bad = _manifest(namespace=WS)
    bad["metadata"]["namespace"] = 42
    with pytest.raises(ActivityManifestError) as exc:
        await registry.register(workspace_id=WS, principal_id=USER, manifest=bad)
    issues = {(issue.path, issue.code) for issue in exc.value.issues}
    assert ("/metadata/namespace", "type") in issues


async def test_register_rejects_non_string_metadata_type() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    bad = _manifest(namespace=WS)
    bad["metadata"]["type"] = True
    with pytest.raises(ActivityManifestError) as exc:
        await registry.register(workspace_id=WS, principal_id=USER, manifest=bad)
    issues = {(issue.path, issue.code) for issue in exc.value.issues}
    assert ("/metadata/type", "type") in issues


async def test_register_rejects_non_string_metadata_version() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    bad = _manifest(namespace=WS)
    bad["metadata"]["version"] = 1
    with pytest.raises(ActivityManifestError) as exc:
        await registry.register(workspace_id=WS, principal_id=USER, manifest=bad)
    issues = {(issue.path, issue.code) for issue in exc.value.issues}
    assert ("/metadata/version", "type") in issues


# ---------------------------------------------------------------------------
# Digest conflict
# ---------------------------------------------------------------------------


async def test_register_digest_conflict_surfaces_both_digests() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    # Pre-register a row with one digest, then have the next put with a
    # different digest raise ConflictDigest from the fake.
    await registry.register(
        workspace_id=WS,
        principal_id=USER,
        manifest=_manifest(namespace=WS),
    )
    stored_digest = store.activity_versions[(WS, "scan-image", "1.0.0")].digest
    with pytest.raises(ActivityRegistryConflict) as exc:
        await registry.register(
            workspace_id=WS,
            principal_id=USER,
            manifest=_manifest(namespace=WS, extra={"determinism": "pure"}),
        )
    assert exc.value.namespace == WS
    assert exc.value.type == "scan-image"
    assert exc.value.version == "1.0.0"
    assert exc.value.supplied_digest != stored_digest
    assert exc.value.stored_digest == stored_digest


# ---------------------------------------------------------------------------
# get / list / deprecate
# ---------------------------------------------------------------------------


async def test_get_returns_row() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    await registry.register(workspace_id=WS, principal_id=USER, manifest=_manifest(namespace=WS))
    row = await registry.get(namespace=WS, type="scan-image", version="1.0.0")
    assert row.namespace == WS
    assert row.version == "1.0.0"


async def test_get_missing_raises_not_found() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    with pytest.raises(ActivityTypeNotFound):
        await registry.get(namespace=WS, type="ghost", version="1.0.0")


async def test_list_returns_versions_for_pair() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    await registry.register(workspace_id=WS, principal_id=USER, manifest=_manifest(namespace=WS))
    await registry.register(
        workspace_id=WS,
        principal_id=USER,
        manifest=_manifest(namespace=WS, version="1.1.0"),
    )
    page = await registry.list(namespace=WS, type="scan-image")
    versions = sorted(row.version for row in page.items)
    assert versions == ["1.0.0", "1.1.0"]


async def test_list_paginates() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    for patch in range(3):
        await registry.register(
            workspace_id=WS,
            principal_id=USER,
            manifest=_manifest(namespace=WS, version=f"1.0.{patch}"),
        )
    page1 = await registry.list(namespace=WS, type="scan-image", limit=2)
    assert len(page1.items) == 2
    assert page1.next_cursor is not None
    page2 = await registry.list(namespace=WS, type="scan-image", limit=2, cursor=page1.next_cursor)
    assert len(page2.items) == 1
    assert page2.next_cursor is None


async def test_deprecate_flips_parent_flag() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    await registry.register(workspace_id=WS, principal_id=USER, manifest=_manifest(namespace=WS))
    await registry.deprecate(
        workspace_id=WS,
        principal_id=USER,
        namespace=WS,
        type="scan-image",
        reason="superseded",
    )
    assert store.deprecate_calls == [(WS, "scan-image", True)]
    row = await registry.get(namespace=WS, type="scan-image", version="1.0.0")
    assert row.parent_deprecated is True


async def test_deprecate_unknown_type_raises_not_found() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    with pytest.raises(ActivityTypeNotFound):
        await registry.deprecate(
            workspace_id=WS,
            principal_id=USER,
            namespace=WS,
            type="ghost",
        )


async def test_deprecate_enforces_namespace_tier() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store, platform_admins=frozenset({ADMIN}))
    await registry.register(
        workspace_id=WS,
        principal_id=ADMIN,
        manifest=_manifest(namespace="custos.builtin"),
    )
    with pytest.raises(ActivityNamespaceError) as exc:
        await registry.deprecate(
            workspace_id=WS,
            principal_id=USER,
            namespace="custos.builtin",
            type="scan-image",
        )
    assert exc.value.tier == "platform"


# ---------------------------------------------------------------------------
# Resolver Protocol pass-throughs (CS-IMPL-008 wiring)
# ---------------------------------------------------------------------------


async def test_resolve_pass_through_to_store() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store, platform_admins=frozenset({ADMIN}))
    await registry.register(
        workspace_id=WS,
        principal_id=ADMIN,
        manifest=_manifest(namespace="custos.builtin", version="2.4.1"),
    )
    await registry.register(
        workspace_id=WS,
        principal_id=ADMIN,
        manifest=_manifest(namespace="custos.builtin", version="2.5.0"),
    )
    row = await registry.resolve("custos.builtin", "scan-image", "2")
    assert row is not None
    assert row.version == "2.5.0"


async def test_resolve_returns_none_when_parent_deprecated() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store, platform_admins=frozenset({ADMIN}))
    await registry.register(
        workspace_id=WS,
        principal_id=ADMIN,
        manifest=_manifest(namespace="custos.builtin", version="2.4.1"),
    )
    await registry.deprecate(
        workspace_id=WS,
        principal_id=ADMIN,
        namespace="custos.builtin",
        type="scan-image",
    )
    row = await registry.resolve("custos.builtin", "scan-image", "2")
    assert row is None


async def test_get_activity_type_version_pass_through() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    await registry.register(workspace_id=WS, principal_id=USER, manifest=_manifest(namespace=WS))
    row = await registry.get_activity_type_version(WS, "scan-image", "1.0.0")
    assert row is not None
    assert row.digest.startswith("sha256:")
