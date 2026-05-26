"""Shared fixtures for the API test suite.

Builds an end-to-end :func:`custos_catalog.create_app` instance backed
by in-memory fakes that satisfy the SPL ``DefinitionStoreProvider`` +
``CatalogStoreProvider`` Protocols. The fakes mirror the surface used
by the catalog managers (publish + read + deprecate + manifest puts +
template puts) so tests exercise the routes end-to-end without
mocking the manager layer.

Authorisation: tests pass an ``x-custos-callctx`` header built by
:func:`callctx_header`; the dev shim is active because
``CAT_AUTHZ_ENDPOINT`` is intentionally unset.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, ClassVar

import pytest
from custos_spl.errors import ConflictDigest, ImmutableViolation
from custos_spl.ids import WorkflowId, WorkflowTemplateId, WorkspaceId
from custos_spl.interfaces.catalog_store import (
    ActivityTypeVersion,
    ConnectorTypeVersion,
)
from custos_spl.interfaces.definition_store import (
    DefinitionListFilter,
    WorkflowTemplateVersion,
    WorkflowVersion,
)
from custos_spl.interfaces.metadata_store import AuditEvent
from custos_spl.pagination import Cursor, Page
from fastapi.testclient import TestClient

from custos_catalog import create_app
from custos_catalog.providers import Providers
from custos_catalog.settings import load_settings

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------

_ENV: dict[str, str] = {
    "CAT_DEFINITION_STORE": "postgresql://u:p@h:5432/def",
    "CAT_CATALOG_STORE": "postgresql://u:p@h:5432/cat",
    "CAT_METADATA_STORE": "postgresql://u:p@h:5432/meta",
    "CAT_CONNECTOR_ENDPOINT": "http://connector-service:8080",
    # CAT_AUTHZ_ENDPOINT intentionally unset — exercises the dev shim.
}


# ---------------------------------------------------------------------------
# Fake DefinitionStoreProvider
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FakeDefinitionStore:
    """Composite fake covering both workflow + template surfaces."""

    SCHEMA_REVISION: ClassVar[int] = 1
    applied: set[int] = field(default_factory=lambda: {1})

    workflows: dict[tuple[str, str], list[WorkflowVersion]] = field(default_factory=dict)
    templates: dict[tuple[str, str], list[WorkflowTemplateVersion]] = field(
        default_factory=dict,
    )
    workflow_deprecated: dict[tuple[str, str], bool] = field(default_factory=dict)
    template_deprecated: dict[tuple[str, str], bool] = field(default_factory=dict)

    # ---- Migration surface ----

    @property
    def declared_revisions(self) -> Mapping[str, frozenset[int]]:
        from types import MappingProxyType

        return MappingProxyType({"DefinitionStoreProvider": frozenset(self.applied)})

    async def apply_pending(self) -> list[str]:  # pragma: no cover
        return []

    async def refresh_declared(self) -> None:
        return None

    # ---- Workflow surface ----

    async def put_workflow_version(
        self,
        workspace_id: WorkspaceId,
        workflow_id: WorkflowId,
        version: str,
        normalized_doc: Mapping[str, Any],
        derived_from_template_version_id: str | None = None,
    ) -> WorkflowVersion:
        key = (str(workspace_id), str(workflow_id))
        rows = self.workflows.setdefault(key, [])
        for r in rows:
            if r.version == version:
                raise ImmutableViolation(
                    f"{workflow_id!r} version {version} already exists",
                )
        new_row = WorkflowVersion(
            workspace_id=workspace_id,
            workflow_id=workflow_id,
            version=version,
            normalized_doc=dict(normalized_doc),
            derived_from_template_version_id=derived_from_template_version_id,
            parent_deprecated=self.workflow_deprecated.get(key, False),
            published_at=datetime.now(tz=UTC),
        )
        rows.append(new_row)
        return new_row

    async def get_workflow_version(
        self,
        workspace_id: WorkspaceId,
        workflow_id: WorkflowId,
        version: str,
    ) -> WorkflowVersion | None:
        key = (str(workspace_id), str(workflow_id))
        for r in self.workflows.get(key, []):
            if r.version == version:
                return self._with_workflow_dep(r, key)
        return None

    async def list_workflow_versions(
        self,
        workspace_id: WorkspaceId,
        workflow_id: WorkflowId,
        filter: DefinitionListFilter | None = None,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[WorkflowVersion]:
        key = (str(workspace_id), str(workflow_id))
        items = list(self.workflows.get(key, []))
        items.sort(key=lambda r: r.published_at, reverse=True)
        return Page(
            items=tuple(self._with_workflow_dep(r, key) for r in items),
            next_cursor=None,
        )

    async def get_latest_workflow_version(
        self,
        workspace_id: WorkspaceId,
        workflow_id: WorkflowId,
    ) -> WorkflowVersion | None:
        key = (str(workspace_id), str(workflow_id))
        rows = self.workflows.get(key, [])
        if not rows:
            return None
        latest = max(rows, key=lambda r: r.published_at)
        return self._with_workflow_dep(latest, key)

    async def set_workflow_deprecated(
        self,
        workspace_id: WorkspaceId,
        workflow_id: WorkflowId,
        deprecated: bool,
    ) -> None:
        self.workflow_deprecated[(str(workspace_id), str(workflow_id))] = deprecated

    def _with_workflow_dep(self, row: WorkflowVersion, key: tuple[str, str]) -> WorkflowVersion:
        return WorkflowVersion(
            workspace_id=row.workspace_id,
            workflow_id=row.workflow_id,
            version=row.version,
            normalized_doc=row.normalized_doc,
            derived_from_template_version_id=row.derived_from_template_version_id,
            parent_deprecated=self.workflow_deprecated.get(key, False),
            published_at=row.published_at,
        )

    # ---- Template surface ----

    async def put_workflow_template_version(
        self,
        workspace_id: WorkspaceId,
        template_id: WorkflowTemplateId,
        version: str,
        normalized_doc: Mapping[str, Any],
        derived_from_workflow_version_id: str | None = None,
    ) -> WorkflowTemplateVersion:
        key = (str(workspace_id), str(template_id))
        rows = self.templates.setdefault(key, [])
        for r in rows:
            if r.version == version:
                raise ImmutableViolation(
                    f"{template_id!r} version {version} already exists",
                )
        new_row = WorkflowTemplateVersion(
            workspace_id=workspace_id,
            template_id=template_id,
            version=version,
            normalized_doc=dict(normalized_doc),
            derived_from_workflow_version_id=derived_from_workflow_version_id,
            parent_deprecated=self.template_deprecated.get(key, False),
            published_at=datetime.now(tz=UTC),
        )
        rows.append(new_row)
        return new_row

    async def get_workflow_template_version(
        self,
        workspace_id: WorkspaceId,
        template_id: WorkflowTemplateId,
        version: str,
    ) -> WorkflowTemplateVersion | None:
        key = (str(workspace_id), str(template_id))
        for r in self.templates.get(key, []):
            if r.version == version:
                return self._with_template_dep(r, key)
        return None

    async def list_workflow_template_versions(
        self,
        workspace_id: WorkspaceId,
        template_id: WorkflowTemplateId,
        filter: DefinitionListFilter | None = None,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[WorkflowTemplateVersion]:
        key = (str(workspace_id), str(template_id))
        items = list(self.templates.get(key, []))
        items.sort(key=lambda r: r.published_at, reverse=True)
        return Page(
            items=tuple(self._with_template_dep(r, key) for r in items),
            next_cursor=None,
        )

    async def set_workflow_template_deprecated(
        self,
        workspace_id: WorkspaceId,
        template_id: WorkflowTemplateId,
        deprecated: bool,
    ) -> None:
        self.template_deprecated[(str(workspace_id), str(template_id))] = deprecated

    def _with_template_dep(
        self, row: WorkflowTemplateVersion, key: tuple[str, str]
    ) -> WorkflowTemplateVersion:
        return WorkflowTemplateVersion(
            workspace_id=row.workspace_id,
            template_id=row.template_id,
            version=row.version,
            normalized_doc=row.normalized_doc,
            derived_from_workflow_version_id=row.derived_from_workflow_version_id,
            parent_deprecated=self.template_deprecated.get(key, False),
            published_at=row.published_at,
        )


# ---------------------------------------------------------------------------
# Fake CatalogStoreProvider
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FakeCatalogStore:
    """Composite fake covering both activity-type + connector-type surfaces."""

    SCHEMA_REVISION: ClassVar[int] = 1
    applied: set[int] = field(default_factory=lambda: {1})

    activity_versions: dict[tuple[str, str, str], ActivityTypeVersion] = field(
        default_factory=dict,
    )
    activity_deprecated: dict[tuple[str, str], bool] = field(default_factory=dict)
    connector_versions: dict[tuple[str, str], ConnectorTypeVersion] = field(
        default_factory=dict,
    )
    connector_deprecated: dict[str, bool] = field(default_factory=dict)

    # ---- Migration ----

    @property
    def declared_revisions(self) -> Mapping[str, frozenset[int]]:
        from types import MappingProxyType

        return MappingProxyType({"CatalogStoreProvider": frozenset(self.applied)})

    async def apply_pending(self) -> list[str]:  # pragma: no cover
        return []

    async def refresh_declared(self) -> None:
        return None

    # ---- Activity surface ----

    async def put_activity_type_version(
        self,
        namespace: str,
        type: str,
        version: str,
        digest: str,
        normalized_manifest: Mapping[str, Any],
    ) -> ActivityTypeVersion:
        key = (namespace, type, version)
        existing = self.activity_versions.get(key)
        if existing is not None and existing.digest != digest:
            raise ConflictDigest(
                f"{namespace}/{type}@{version} digest mismatch",
            )
        row = ActivityTypeVersion(
            namespace=namespace,
            type=type,
            version=version,
            digest=digest,
            normalized_manifest=dict(normalized_manifest),
            parent_deprecated=self.activity_deprecated.get((namespace, type), False),
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
            return None
        return self._with_activity_dep(row)

    async def list_activity_type_versions(
        self,
        namespace: str,
        type: str,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[ActivityTypeVersion]:
        rows = [
            self._with_activity_dep(row)
            for (ns, tp, _), row in self.activity_versions.items()
            if ns == namespace and tp == type
        ]
        return Page(items=tuple(rows), next_cursor=None)

    async def set_activity_type_deprecated(
        self,
        namespace: str,
        type: str,
        deprecated: bool,
    ) -> None:
        self.activity_deprecated[(namespace, type)] = deprecated

    async def resolve(
        self,
        namespace: str,
        type: str,
        semver_range: str,
    ) -> ActivityTypeVersion | None:
        from packaging.specifiers import SpecifierSet
        from packaging.version import Version

        if self.activity_deprecated.get((namespace, type), False):
            return None
        spec = SpecifierSet(semver_range)
        matching = [
            row
            for (ns, tp, _), row in self.activity_versions.items()
            if ns == namespace and tp == type and Version(row.version) in spec
        ]
        if not matching:
            return None
        matching.sort(key=lambda r: Version(r.version))
        return self._with_activity_dep(matching[-1])

    def _with_activity_dep(self, row: ActivityTypeVersion) -> ActivityTypeVersion:
        return ActivityTypeVersion(
            namespace=row.namespace,
            type=row.type,
            version=row.version,
            digest=row.digest,
            normalized_manifest=row.normalized_manifest,
            parent_deprecated=self.activity_deprecated.get((row.namespace, row.type), False),
            published_at=row.published_at,
        )

    # ---- Connector surface ----

    async def put_connector_type_version(
        self,
        type: str,
        version: str,
        digest: str,
        image_ref: str,
        normalized_manifest: Mapping[str, Any],
    ) -> ConnectorTypeVersion:
        key = (type, version)
        existing = self.connector_versions.get(key)
        if existing is not None and existing.digest != digest:
            raise ConflictDigest(f"{type}@{version} digest mismatch")
        row = ConnectorTypeVersion(
            type=type,
            version=version,
            digest=digest,
            image_ref=image_ref,
            normalized_manifest=dict(normalized_manifest),
            parent_deprecated=self.connector_deprecated.get(type, False),
            published_at=datetime.now(tz=UTC),
        )
        self.connector_versions[key] = row
        return row

    async def get_connector_type_version(
        self, type: str, version: str
    ) -> ConnectorTypeVersion | None:
        row = self.connector_versions.get((type, version))
        if row is None:
            return None
        return self._with_connector_dep(row)

    async def list_connector_type_versions(
        self,
        type: str,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[ConnectorTypeVersion]:
        rows = [
            self._with_connector_dep(row)
            for (tp, _), row in self.connector_versions.items()
            if tp == type
        ]
        return Page(items=tuple(rows), next_cursor=None)

    async def set_connector_type_deprecated(self, type: str, deprecated: bool) -> None:
        self.connector_deprecated[type] = deprecated

    def _with_connector_dep(self, row: ConnectorTypeVersion) -> ConnectorTypeVersion:
        return ConnectorTypeVersion(
            type=row.type,
            version=row.version,
            digest=row.digest,
            image_ref=row.image_ref,
            normalized_manifest=row.normalized_manifest,
            parent_deprecated=self.connector_deprecated.get(row.type, False),
            published_at=row.published_at,
        )


# ---------------------------------------------------------------------------
# Fake MetadataStoreProvider (audit-only slice for CS-IMPL-019)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FakeMetadataStore:
    """In-memory ``MetadataStoreProvider`` slice for the catalog tests.

    Implements only the audit-emission surface plus the migration
    surface the schema-revision startup gate requires. Catalog-service
    never calls the run / step / cursor / idempotency / dedupe / lease
    methods so they are not stubbed.

    Attributes:
        audit: Every event appended via :meth:`append_audit`, in
            chronological order.
        raise_on_append: If set, every :meth:`append_audit` call
            raises this exception. Used to exercise the audit
            best-effort path in the catalog managers.
    """

    SCHEMA_REVISION: ClassVar[int] = 4
    applied: set[int] = field(default_factory=lambda: {1, 2, 3, 4})
    audit: list[AuditEvent] = field(default_factory=list)
    raise_on_append: BaseException | None = None

    # ---- Migration surface ----

    @property
    def declared_revisions(self) -> Mapping[str, frozenset[int]]:
        from types import MappingProxyType

        return MappingProxyType({"MetadataStoreProvider": frozenset(self.applied)})

    async def apply_pending(self) -> list[str]:  # pragma: no cover
        return []

    async def refresh_declared(self) -> None:
        return None

    # ---- Audit surface ----

    async def append_audit(
        self,
        workspace_id: WorkspaceId,
        event: AuditEvent,
        tx: Any | None = None,
    ) -> None:
        if self.raise_on_append is not None:
            raise self.raise_on_append
        self.audit.append(event)


# ---------------------------------------------------------------------------
# CallContext header helper
# ---------------------------------------------------------------------------


def callctx_header(
    *,
    workspace_id: str = "ws-1",
    principal_id: str = "alice",
    permissions: Iterable[str] = (),
) -> dict[str, str]:
    """Build the dev-shim call-context header for tests."""
    payload = {
        "workspace_id": workspace_id,
        "principal_id": principal_id,
        "permissions": sorted(set(permissions)),
    }
    return {"x-custos-callctx": json.dumps(payload)}


# Convenience permission bundles ------------------------------------------------

ALL_PERMISSIONS: tuple[str, ...] = (
    "catalog:workflows:read",
    "catalog:workflows:write",
    "catalog:templates:read",
    "catalog:templates:write",
    "catalog:activity-types:read",
    "catalog:activity-types:write",
    "catalog:connector-types:read",
    "catalog:connector-types:write",
    "catalog:rpc:read",
)


def admin_header(ws: str = "ws-1") -> dict[str, str]:
    """Header with every catalog permission set."""
    return callctx_header(
        workspace_id=ws,
        principal_id="alice",
        permissions=ALL_PERMISSIONS,
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def stores() -> tuple[FakeDefinitionStore, FakeCatalogStore, FakeMetadataStore]:
    """Fresh empty in-memory stores per test."""
    return FakeDefinitionStore(), FakeCatalogStore(), FakeMetadataStore()


@pytest.fixture
def providers(
    stores: tuple[FakeDefinitionStore, FakeCatalogStore, FakeMetadataStore],
) -> Providers:
    """Build a :class:`Providers` bundle from the in-memory stores."""
    definition_store, catalog_store, metadata_store = stores
    return Providers(
        definition_store=definition_store,  # type: ignore[arg-type, unused-ignore]
        catalog_store=catalog_store,  # type: ignore[arg-type, unused-ignore]
        metadata_store=metadata_store,  # type: ignore[arg-type, unused-ignore]
    )


@pytest.fixture
def client(providers: Providers) -> Iterable[TestClient]:
    """A :class:`TestClient` mounted on a fully-wired application."""
    settings = load_settings(_ENV)
    app = create_app(settings=settings, providers=providers)
    with TestClient(app) as test_client:
        yield test_client


# ---------------------------------------------------------------------------
# Sample document factories
# ---------------------------------------------------------------------------


def minimal_workflow(name: str = "orders", ws: str = "ws-1") -> dict[str, Any]:
    """Smallest workflow document that passes every publish gate.

    Uses a single ``custos.builtin/echo@1`` step which the publish
    pipeline's resolver matches against the catalog store's
    ``custos.builtin`` rows. The in-memory catalog has no
    ``custos.builtin`` activities loaded by default — tests that want
    publishes to succeed must seed the catalog (see
    :func:`seed_builtin_echo`).
    """
    return {
        "apiVersion": "custos.dev/v1",
        "kind": "Workflow",
        "metadata": {"name": name, "workspace": ws},
        "spec": {
            "inputs": {"image": {"type": "string"}},
            "steps": [
                {
                    "id": "say-hi",
                    "activity": "custos.builtin/echo@1",
                    "with": {"message": "hello"},
                },
            ],
        },
    }


def minimal_template(name: str = "orders-tmpl", ws: str = "ws-1") -> dict[str, Any]:
    """Smallest template document that passes the publish gates."""
    return {
        "apiVersion": "custos.dev/v1",
        "kind": "WorkflowTemplate",
        "metadata": {"name": name, "workspace": ws},
        "spec": {
            "placeholders": [
                {
                    "name": "topic",
                    "type": "string",
                    "required": False,
                    "default": "default-topic",
                },
            ],
            "workflow": {
                "inputs": {"image": {"type": "string"}},
                "steps": [
                    {
                        "id": "say-hi",
                        "activity": "custos.builtin/echo@1",
                        "with": {"message": "hello"},
                    },
                ],
            },
        },
    }


def minimal_activity_manifest(
    namespace: str = "ws-1",
    type: str = "fetch-orders",
    version: str = "1.0.0",
) -> dict[str, Any]:
    """Minimal activity manifest accepted by :meth:`ActivityTypeRegistry.register`."""
    return {
        "apiVersion": "custos.dev/v1",
        "kind": "ActivityManifest",
        "metadata": {"namespace": namespace, "type": type, "version": version},
        "spec": {
            "contractVersion": "1",
            "runtime": {
                "kind": "oci-container",
                "image": "ghcr.io/x:v1",
                "digest": "sha256:abc",
            },
        },
    }


def minimal_connector_manifest(
    type: str = "oci-registry",
    version: str = "1.0.0",
) -> dict[str, Any]:
    """Minimal connector manifest accepted by :meth:`ConnectorTypeRegistry.register`."""
    return {
        "apiVersion": "custos.dev/connector-manifest/v1",
        "kind": "ConnectorManifest",
        "metadata": {"type": type, "version": version},
        "spec": {
            "capabilities": ["oci.pull"],
            "target": {"kind": "oci-registry", "endpoint": "https://ghcr.io"},
            "credentials": {"authenticationType": "oidc"},
        },
    }


def seed_builtin_echo(catalog_store: FakeCatalogStore) -> None:
    """Pre-populate ``custos.builtin/echo@1.0.0`` so workflow publishes resolve."""
    catalog_store.activity_versions[("custos.builtin", "echo", "1.0.0")] = ActivityTypeVersion(
        namespace="custos.builtin",
        type="echo",
        version="1.0.0",
        digest="sha256:builtin-echo",
        normalized_manifest={
            "apiVersion": "custos.dev/activity-manifest/v1",
            "kind": "ActivityManifest",
            "metadata": {"namespace": "custos.builtin", "type": "echo", "version": "1.0.0"},
            "spec": {"inputs": {}, "outputs": {}},
        },
        parent_deprecated=False,
        published_at=datetime.now(tz=UTC),
    )
