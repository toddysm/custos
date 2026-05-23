"""Tests for :mod:`custos_catalog.managers.connector_registry` (CS-IMPL-016)."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, ClassVar

import pytest
from custos_spl.errors import ConflictDigest
from custos_spl.interfaces.catalog_store import ActivityTypeVersion, ConnectorTypeVersion
from custos_spl.pagination import Cursor, Page

from custos_catalog.managers.connector_registry import (
    ConnectorManifestError,
    ConnectorRegistryConflict,
    ConnectorTypeNotFound,
    ConnectorTypeRef,
    ConnectorTypeRegistry,
)

PRINCIPAL = "connector-svc"


# ---------------------------------------------------------------------------
# Hand-rolled fake store
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FakeCatalogStore:
    """In-memory ``CatalogStoreProvider`` for the connector-type surface."""

    SCHEMA_REVISION: ClassVar[int] = 1

    connector_versions: dict[tuple[str, str], ConnectorTypeVersion] = field(default_factory=dict)
    parent_deprecated: dict[str, bool] = field(default_factory=dict)
    put_calls: list[tuple[str, str, str]] = field(default_factory=list)
    list_calls: list[str] = field(default_factory=list)
    deprecate_calls: list[tuple[str, bool]] = field(default_factory=list)

    # ---- connector surface ----

    async def put_connector_type_version(
        self,
        type: str,
        version: str,
        digest: str,
        normalized_manifest: Mapping[str, Any],
    ) -> ConnectorTypeVersion:
        key = (type, version)
        self.put_calls.append((type, version, digest))
        existing = self.connector_versions.get(key)
        if existing is not None and existing.digest != digest:
            raise ConflictDigest(
                f"{type}@{version} stored with digest {existing.digest} != supplied {digest}",
            )
        row = ConnectorTypeVersion(
            type=type,
            version=version,
            digest=digest,
            normalized_manifest=dict(normalized_manifest),
            parent_deprecated=self.parent_deprecated.get(type, False),
            published_at=datetime.now(tz=UTC),
        )
        self.connector_versions[key] = row
        return row

    async def get_connector_type_version(
        self,
        type: str,
        version: str,
    ) -> ConnectorTypeVersion | None:
        row = self.connector_versions.get((type, version))
        if row is None:
            return None
        return self._with_parent_deprecation(row)

    async def list_connector_type_versions(
        self,
        type: str,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[ConnectorTypeVersion]:
        self.list_calls.append(type)
        rows = [
            self._with_parent_deprecation(row)
            for (tp, _), row in self.connector_versions.items()
            if tp == type
        ]
        start = int(cursor.token) if cursor is not None else 0
        end = start + (limit or len(rows))
        slice_ = rows[start:end]
        next_cursor = Cursor(token=str(end)) if end < len(rows) else None
        return Page(items=slice_, next_cursor=next_cursor)

    async def set_connector_type_deprecated(self, type: str, deprecated: bool) -> None:
        self.parent_deprecated[type] = deprecated
        self.deprecate_calls.append((type, deprecated))

    # ---- activity surface (unused — satisfies Protocol shape) ----

    async def put_activity_type_version(
        self,
        namespace: str,
        type: str,
        version: str,
        digest: str,
        normalized_manifest: Mapping[str, Any],
    ) -> ActivityTypeVersion:
        raise NotImplementedError

    async def get_activity_type_version(
        self, namespace: str, type: str, version: str
    ) -> ActivityTypeVersion | None:
        return None

    async def list_activity_type_versions(
        self,
        namespace: str,
        type: str,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[ActivityTypeVersion]:
        return Page()

    async def set_activity_type_deprecated(
        self, namespace: str, type: str, deprecated: bool
    ) -> None: ...

    async def resolve(
        self, namespace: str, type: str, semver_range: str
    ) -> ActivityTypeVersion | None:
        return None

    # ---- helpers ----

    def _with_parent_deprecation(self, row: ConnectorTypeVersion) -> ConnectorTypeVersion:
        flag = self.parent_deprecated.get(row.type, False)
        if flag == row.parent_deprecated:
            return row
        return ConnectorTypeVersion(
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
    type_: str = "oci-registry",
    version: str = "2.3.1",
    capabilities: list[str] | None = None,
    events: Mapping[str, Any] | None = None,
    include_events: bool = True,
) -> dict[str, Any]:
    """Smallest envelope-valid connector manifest."""
    spec: dict[str, Any] = {
        "description": "OCI registry connector",
        "capabilities": capabilities or ["oci.pull", "oci.push"],
        "target": {
            "kind": "oci-registry",
            "endpoint": "https://ghcr.io",
            "verifyTls": True,
            "config": {"repositoryNamespace": "my-org"},
        },
        "credentials": {
            "authenticationType": "oidc",
            "authentication": {
                "provider": "oidc",
                "issuer": "https://token.actions.githubusercontent.com",
                "audience": "https://ghcr.io",
                "subjectTemplate": "repo:my-org/my-repo:ref:{ref}",
            },
        },
    }
    if include_events:
        spec["events"] = events or {
            "delivery": ["push", "pull"],
            "produced": ["oci.image.pushed", "oci.tag.updated"],
        }
    return {
        "apiVersion": "custos.dev/connector-manifest/v1",
        "kind": "ConnectorManifest",
        "metadata": {
            "type": type_,
            "version": version,
            "contractVersion": "1",
        },
        "spec": spec,
    }


def _make_registry(store: FakeCatalogStore) -> ConnectorTypeRegistry:
    return ConnectorTypeRegistry(catalog_store=store)


# ---------------------------------------------------------------------------
# Register — happy paths
# ---------------------------------------------------------------------------


async def test_register_returns_ref() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    ref = await registry.register(principal_id=PRINCIPAL, manifest=_manifest())
    assert isinstance(ref, ConnectorTypeRef)
    assert ref.type == "oci-registry"
    assert ref.version == "2.3.1"
    assert ref.digest.startswith("sha256:")
    assert ("oci-registry", "2.3.1") in store.connector_versions


async def test_register_persists_normalized_manifest_with_sorted_keys() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    manifest = _manifest()
    # Insert keys in inverse-alphabetical order
    manifest["spec"] = {"zzz": "z", "aaa": "a", **manifest["spec"]}
    await registry.register(principal_id=PRINCIPAL, manifest=manifest)
    row = store.connector_versions[("oci-registry", "2.3.1")]
    spec_keys = list(row.normalized_manifest["spec"].keys())
    assert spec_keys == sorted(spec_keys)


async def test_register_idempotent_on_identical_manifest() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    ref1 = await registry.register(principal_id=PRINCIPAL, manifest=_manifest())
    ref2 = await registry.register(principal_id=PRINCIPAL, manifest=_manifest())
    assert ref1 == ref2
    assert len(store.put_calls) == 2


async def test_register_sink_connector_without_events_block() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    ref = await registry.register(
        principal_id=PRINCIPAL,
        manifest=_manifest(
            type_="slack-notifier",
            version="1.0.0",
            capabilities=["slack.post"],
            include_events=False,
        ),
    )
    row = store.connector_versions[(ref.type, ref.version)]
    assert "events" not in row.normalized_manifest["spec"]


# ---------------------------------------------------------------------------
# Envelope validation
# ---------------------------------------------------------------------------


async def test_register_rejects_wrong_api_version() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    bad = _manifest()
    bad["apiVersion"] = "custos.dev/v1"  # the activity envelope, not connector
    with pytest.raises(ConnectorManifestError) as exc:
        await registry.register(principal_id=PRINCIPAL, manifest=bad)
    paths = {issue.path for issue in exc.value.issues}
    assert "/apiVersion" in paths


async def test_register_rejects_wrong_kind() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    bad = _manifest()
    bad["kind"] = "ActivityManifest"
    with pytest.raises(ConnectorManifestError) as exc:
        await registry.register(principal_id=PRINCIPAL, manifest=bad)
    paths = {issue.path for issue in exc.value.issues}
    assert "/kind" in paths


async def test_register_collects_multiple_envelope_issues() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    bad: dict[str, Any] = {"apiVersion": "v0", "kind": "Bad", "metadata": {}}
    with pytest.raises(ConnectorManifestError) as exc:
        await registry.register(principal_id=PRINCIPAL, manifest=bad)
    paths = {issue.path for issue in exc.value.issues}
    assert "/apiVersion" in paths
    assert "/kind" in paths
    assert "/metadata/type" in paths
    assert "/metadata/version" in paths


async def test_register_rejects_partial_semver() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    with pytest.raises(ConnectorManifestError) as exc:
        await registry.register(principal_id=PRINCIPAL, manifest=_manifest(version="2.3"))
    paths = {issue.path for issue in exc.value.issues}
    assert "/metadata/version" in paths


async def test_register_rejects_bad_type_token() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    with pytest.raises(ConnectorManifestError) as exc:
        await registry.register(principal_id=PRINCIPAL, manifest=_manifest(type_="Bad Type"))
    paths = {issue.path for issue in exc.value.issues}
    assert "/metadata/type" in paths


async def test_register_rejects_non_string_metadata_type() -> None:
    """A present-but-non-string ``metadata.type`` must be flagged so it never
    reaches the store as a non-str value (or sneaks into error messages)."""
    store = FakeCatalogStore()
    registry = _make_registry(store)
    bad = _manifest()
    bad["metadata"]["type"] = 123
    with pytest.raises(ConnectorManifestError) as exc:
        await registry.register(principal_id=PRINCIPAL, manifest=bad)
    issues = {(issue.path, issue.code) for issue in exc.value.issues}
    assert ("/metadata/type", "type") in issues


async def test_register_rejects_non_string_metadata_version() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    bad = _manifest()
    bad["metadata"]["version"] = 2
    with pytest.raises(ConnectorManifestError) as exc:
        await registry.register(principal_id=PRINCIPAL, manifest=bad)
    issues = {(issue.path, issue.code) for issue in exc.value.issues}
    assert ("/metadata/version", "type") in issues


async def test_register_rejects_event_token_in_capabilities() -> None:
    """Per design § Capabilities and Events, event.* tokens must live in
    events.* not in capabilities."""
    store = FakeCatalogStore()
    registry = _make_registry(store)
    with pytest.raises(ConnectorManifestError) as exc:
        await registry.register(
            principal_id=PRINCIPAL,
            manifest=_manifest(capabilities=["oci.pull", "event.image.pushed"]),
        )
    codes = {issue.code for issue in exc.value.issues}
    assert "forbidden_event_token" in codes


async def test_register_rejects_malformed_capability_token() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    with pytest.raises(ConnectorManifestError) as exc:
        await registry.register(
            principal_id=PRINCIPAL,
            manifest=_manifest(capabilities=["BAD CAP"]),
        )
    paths = {issue.path for issue in exc.value.issues}
    assert "/spec/capabilities/0" in paths


async def test_register_rejects_empty_events_delivery() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    with pytest.raises(ConnectorManifestError) as exc:
        await registry.register(
            principal_id=PRINCIPAL,
            manifest=_manifest(events={"delivery": [], "produced": ["x.y"]}),
        )
    paths = {issue.path for issue in exc.value.issues}
    assert "/spec/events/delivery" in paths


async def test_register_rejects_invalid_delivery_mode() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    with pytest.raises(ConnectorManifestError) as exc:
        await registry.register(
            principal_id=PRINCIPAL,
            manifest=_manifest(events={"delivery": ["push", "stream"], "produced": ["x.y"]}),
        )
    paths = {issue.path for issue in exc.value.issues}
    assert "/spec/events/delivery/1" in paths


async def test_register_rejects_empty_events_produced() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    with pytest.raises(ConnectorManifestError) as exc:
        await registry.register(
            principal_id=PRINCIPAL,
            manifest=_manifest(events={"delivery": ["push"], "produced": []}),
        )
    paths = {issue.path for issue in exc.value.issues}
    assert "/spec/events/produced" in paths


async def test_register_rejects_non_object_spec() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    bad = _manifest()
    bad["spec"] = "oops"
    with pytest.raises(ConnectorManifestError) as exc:
        await registry.register(principal_id=PRINCIPAL, manifest=bad)
    paths = {issue.path for issue in exc.value.issues}
    assert "/spec" in paths


# ---------------------------------------------------------------------------
# Digest conflict
# ---------------------------------------------------------------------------


async def test_register_digest_conflict_surfaces_both_digests() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    await registry.register(principal_id=PRINCIPAL, manifest=_manifest())
    stored_digest = store.connector_versions[("oci-registry", "2.3.1")].digest
    # Republish with a tweak so the digest differs.
    tweaked = _manifest()
    tweaked["spec"]["description"] = "Different description"
    with pytest.raises(ConnectorRegistryConflict) as exc:
        await registry.register(principal_id=PRINCIPAL, manifest=tweaked)
    assert exc.value.type == "oci-registry"
    assert exc.value.version == "2.3.1"
    assert exc.value.supplied_digest != stored_digest
    assert exc.value.stored_digest == stored_digest


# ---------------------------------------------------------------------------
# get / list / deprecate
# ---------------------------------------------------------------------------


async def test_get_returns_row() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    await registry.register(principal_id=PRINCIPAL, manifest=_manifest())
    row = await registry.get(type="oci-registry", version="2.3.1")
    assert row.type == "oci-registry"
    assert row.version == "2.3.1"


async def test_get_missing_raises_not_found() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    with pytest.raises(ConnectorTypeNotFound):
        await registry.get(type="ghost", version="1.0.0")


async def test_list_returns_all_versions_for_type() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    await registry.register(principal_id=PRINCIPAL, manifest=_manifest(version="2.3.1"))
    await registry.register(principal_id=PRINCIPAL, manifest=_manifest(version="2.4.0"))
    page = await registry.list(type="oci-registry")
    versions = sorted(row.version for row in page.items)
    assert versions == ["2.3.1", "2.4.0"]


async def test_list_paginates() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    for patch in range(3):
        await registry.register(principal_id=PRINCIPAL, manifest=_manifest(version=f"1.0.{patch}"))
    page1 = await registry.list(type="oci-registry", limit=2)
    assert len(page1.items) == 2
    assert page1.next_cursor is not None
    page2 = await registry.list(type="oci-registry", limit=2, cursor=page1.next_cursor)
    assert len(page2.items) == 1
    assert page2.next_cursor is None


async def test_deprecate_flips_parent_flag() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    await registry.register(principal_id=PRINCIPAL, manifest=_manifest())
    await registry.deprecate(
        principal_id=PRINCIPAL,
        type="oci-registry",
        reason="superseded",
    )
    assert store.deprecate_calls == [("oci-registry", True)]
    row = await registry.get(type="oci-registry", version="2.3.1")
    assert row.parent_deprecated is True


async def test_deprecate_unknown_type_raises_not_found() -> None:
    store = FakeCatalogStore()
    registry = _make_registry(store)
    with pytest.raises(ConnectorTypeNotFound):
        await registry.deprecate(principal_id=PRINCIPAL, type="ghost")
