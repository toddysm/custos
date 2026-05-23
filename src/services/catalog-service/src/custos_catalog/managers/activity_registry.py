"""Activity Type Registry (CS-IMPL-015).

Implements the register / list / get / deprecate surface for
``ActivityTypeVersion`` rows, plus the publish-time
:class:`custos_catalog.resolve.ActivityTypeRegistry` Protocol used by
the resolver (CS-IMPL-008). One manager, two faces:

* **Authoring surface** (this module's public methods) — wraps
  :class:`custos_spl.interfaces.catalog_store.CatalogStoreProvider`
  with namespace-tier RBAC, manifest envelope validation, digest
  computation, and a stable error code mapping.
* **Resolver Protocol** (``resolve`` / ``get_activity_type_version``)
  — methods with these exact signatures forward to the underlying
  store so a :class:`ActivityTypeRegistry` instance can be passed
  straight into :class:`custos_catalog.managers.definition.DefinitionManager`
  as the ``activity_registry`` collaborator.

Namespace tier rules (design § Operation: Register Activity Type
Version + ARM § Namespace model):

============  ==============================================  ==============================
Tier          Match                                           Authorisation
============  ==============================================  ==============================
platform      Equal to ``custos.builtin`` *or* starts with    ``principal_id`` in
              ``custos.`` / ``system.`` / ``platform.`` /     ``platform_admins``
              ``builtin.``
workspace     Literally equal to ``workspace_id``             Always allowed
vendor        Anything else matching the namespace regex      ``namespace`` in
                                                              ``vendor_grants[workspace_id]``
============  ==============================================  ==============================

Reserved-prefix namespaces are the strongest tier: only platform
admins may publish into them and the rule does not depend on the
workspace.

Digest semantics mirror the SPL contract: re-registering the same
``(namespace, type, version)`` with an identical normalized manifest
is idempotent; a mismatch surfaces as
:class:`ActivityRegistryConflict` carrying both digests.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final

from custos_spl.errors import ConflictDigest
from custos_spl.interfaces.catalog_store import ActivityTypeVersion, CatalogStoreProvider
from custos_spl.pagination import Cursor, Page

from custos_catalog.audit import emit_event
from custos_catalog.normalize import canonical_hash, canonical_json

_LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Reference shapes
# ---------------------------------------------------------------------------


#: Token grammar shared by namespace and type — mirrors the JSON Schema
#: rule used in workflow / template documents (DNS-friendly slug).
_TOKEN_RE: Final[re.Pattern[str]] = re.compile(r"^[a-z][a-z0-9._-]*$")

#: Strict ``MAJOR.MINOR.PATCH`` triple (the only legal version form at
#: publish time per ARM § Versioning).
_EXACT_VERSION_RE: Final[re.Pattern[str]] = re.compile(
    r"^(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)$",
)

#: Prefixes (and the bare token ``custos.builtin``) reserved for the
#: platform tier. Any namespace matching one of these is platform-only.
_RESERVED_PREFIXES: Final[tuple[str, ...]] = (
    "custos.",
    "system.",
    "platform.",
    "builtin.",
)


@dataclass(frozen=True, slots=True)
class ActivityTypeRef:
    """A handle to a registered activity-type version.

    The triple ``(namespace, type, version)`` is the SPL primary key.
    ``digest`` is the SHA-256 of the canonical-JSON form of the
    normalized manifest and is returned so callers can pin the
    reference in their own audit trail.

    Attributes:
        namespace: The activity-type namespace token.
        type: The activity-type slug.
        version: Exact ``MAJOR.MINOR.PATCH`` triple.
        digest: ``sha256:<hex>`` of the normalized manifest.
    """

    namespace: str
    type: str
    version: str
    digest: str


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class ActivityRegistryError(Exception):
    """Base class for activity-registry failures.

    Carries a stable :attr:`code` so the API surface (CS-IMPL-017) can
    map errors to remediation hints without parsing the message.
    """

    code: str = "catalog.activity_registry_failed"


@dataclass(frozen=True, slots=True)
class ActivityManifestIssue:
    """One issue from manifest envelope validation.

    Mirrors :class:`custos_catalog.managers.definition.PublishValidationIssue`
    so the FastAPI layer can render a uniform error envelope across the
    workflow- and registry-publish paths.

    Attributes:
        path: JSON Pointer-style path of the offending field
            (``"/metadata/version"``, ``""`` for envelope-level issues).
        code: Stable machine-readable code (e.g. ``"required"``,
            ``"value"``, ``"format"``).
        message: Human-readable message suitable for API response.
    """

    path: str
    code: str
    message: str


class ActivityManifestError(ActivityRegistryError):
    """Raised when the manifest envelope or required metadata fails validation."""

    code: str = "catalog.activity_manifest_invalid"

    def __init__(self, issues: list[ActivityManifestIssue]) -> None:
        self.issues = issues
        rendered = "; ".join(f"{issue.path or '<root>'} -> {issue.message}" for issue in issues)
        super().__init__(f"activity manifest invalid: {len(issues)} issue(s): {rendered}")


class ActivityNamespaceError(ActivityRegistryError):
    """Raised when namespace-tier rules forbid the registration.

    Carries the offending namespace plus the resolved tier so the API
    surface can surface a "request elevation" hint for the platform
    tier or a "request vendor grant" hint for the vendor tier.
    """

    code: str = "catalog.activity_namespace_forbidden"

    def __init__(
        self,
        *,
        namespace: str,
        tier: str,
        workspace_id: str,
        principal_id: str,
        reason: str,
    ) -> None:
        self.namespace = namespace
        self.tier = tier
        self.workspace_id = workspace_id
        self.principal_id = principal_id
        self.reason = reason
        super().__init__(
            f"namespace {namespace!r} (tier {tier!r}) not publishable by "
            f"principal {principal_id!r} in workspace {workspace_id!r}: {reason}",
        )


class ActivityRegistryConflict(ActivityRegistryError):
    """Raised when the SPL surface reports a digest conflict.

    The store accepts identical-digest re-puts as idempotent; a
    different digest on the same ``(namespace, type, version)`` triple
    raises this. Both digests are carried so clients can diff their
    local manifest against the stored one.
    """

    code: str = "catalog.activity_digest_conflict"

    def __init__(
        self,
        *,
        namespace: str,
        type: str,
        version: str,
        supplied_digest: str,
        stored_digest: str | None = None,
    ) -> None:
        self.namespace = namespace
        self.type = type
        self.version = version
        self.supplied_digest = supplied_digest
        self.stored_digest = stored_digest
        msg = (
            f"activity {namespace}/{type}@{version} already registered "
            f"with a different digest (supplied {supplied_digest}"
        )
        if stored_digest is not None:
            msg += f", stored {stored_digest}"
        msg += ")"
        super().__init__(msg)


class ActivityTypeNotFound(ActivityRegistryError):
    """Raised when ``get`` / ``deprecate`` cannot locate the activity type."""

    code: str = "catalog.activity_type_not_found"

    def __init__(self, *, namespace: str, type: str, version: str | None = None) -> None:
        self.namespace = namespace
        self.type = type
        self.version = version
        if version is not None:
            msg = f"activity {namespace}/{type}@{version} not found"
        else:
            msg = f"activity {namespace}/{type} not found"
        super().__init__(msg)


# ---------------------------------------------------------------------------
# Manager
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _ManifestProjection:
    """Internal value object — the bits we lift out of the raw manifest."""

    namespace: str
    type: str
    version: str
    normalized: dict[str, Any]
    digest: str
    referrer_ref: str | None = None


class ActivityTypeRegistry:
    """Authoring + read surface for activity-type versions.

    The manager wraps :class:`CatalogStoreProvider` with namespace-tier
    RBAC and manifest envelope checks. The structural subset
    (``resolve`` / ``get_activity_type_version``) used by the
    publish-time resolver is forwarded transparently, so a single
    instance can be passed as the ``activity_registry`` collaborator
    of :class:`custos_catalog.managers.definition.DefinitionManager`.

    Args:
        catalog_store: SPL :class:`CatalogStoreProvider` instance.
        platform_admins: Iterable of principal IDs allowed to publish
            into reserved-prefix namespaces.
        vendor_grants: Mapping ``workspace_id -> set[namespace]``
            listing vendor namespaces a workspace may publish into.
            Missing workspace keys behave like an empty set.
    """

    def __init__(
        self,
        *,
        catalog_store: CatalogStoreProvider,
        platform_admins: frozenset[str] | set[str] | None = None,
        vendor_grants: Mapping[str, frozenset[str] | set[str]] | None = None,
    ) -> None:
        self._store = catalog_store
        self._platform_admins: frozenset[str] = frozenset(platform_admins or ())
        self._vendor_grants: dict[str, frozenset[str]] = {
            workspace: frozenset(grants) for workspace, grants in (vendor_grants or {}).items()
        }

    # ------------------------------------------------------------------
    # Register
    # ------------------------------------------------------------------

    async def register(
        self,
        *,
        workspace_id: str,
        principal_id: str,
        manifest: Mapping[str, Any],
        referrer_ref: str | None = None,
    ) -> ActivityTypeRef:
        """Register a new activity-type version.

        Pipeline:

        1. Validate the manifest envelope (apiVersion, kind, metadata
           triple, version format) — collect-all, raises
           :class:`ActivityManifestError` on any failure.
        2. Resolve the namespace tier and enforce RBAC — raises
           :class:`ActivityNamespaceError`.
        3. Canonicalize the manifest (sorted keys), compute the
           SHA-256 digest, and call
           :meth:`CatalogStoreProvider.put_activity_type_version`.
        4. Translate :class:`custos_spl.errors.ConflictDigest` into
           :class:`ActivityRegistryConflict` carrying both digests.

        ``referrer_ref`` is recorded in the audit event but not
        persisted — Catalog stores the normalized projection only; the
        OCI Referrer pointer lives in the audit trail / OCI registry.

        Returns:
            The :class:`ActivityTypeRef` for the registered version.
        """
        projection = self._project_manifest(manifest, referrer_ref=referrer_ref)
        self._authorise_namespace(
            namespace=projection.namespace,
            workspace_id=workspace_id,
            principal_id=principal_id,
        )

        try:
            stored = await self._store.put_activity_type_version(
                namespace=projection.namespace,
                type=projection.type,
                version=projection.version,
                digest=projection.digest,
                normalized_manifest=projection.normalized,
            )
        except ConflictDigest as exc:
            stored_digest = await self._lookup_stored_digest(
                namespace=projection.namespace,
                type=projection.type,
                version=projection.version,
            )
            raise ActivityRegistryConflict(
                namespace=projection.namespace,
                type=projection.type,
                version=projection.version,
                supplied_digest=projection.digest,
                stored_digest=stored_digest,
            ) from exc

        ref = ActivityTypeRef(
            namespace=stored.namespace,
            type=stored.type,
            version=stored.version,
            digest=stored.digest,
        )
        emit_event(
            "activity_type.registered",
            {
                "workspace_id": workspace_id,
                "principal_id": principal_id,
                "namespace": ref.namespace,
                "type": ref.type,
                "version": ref.version,
                "digest": ref.digest,
                "referrer_ref": referrer_ref,
            },
        )
        return ref

    # ------------------------------------------------------------------
    # Read surface
    # ------------------------------------------------------------------

    async def get(
        self,
        *,
        namespace: str,
        type: str,
        version: str,
    ) -> ActivityTypeVersion:
        """Fetch one ``(namespace, type, version)`` row.

        Raises :class:`ActivityTypeNotFound` if the row is absent.
        Parent-deprecation state is exposed via the returned row's
        ``parent_deprecated`` flag — see SPL doc for the model.
        """
        row = await self._store.get_activity_type_version(namespace, type, version)
        if row is None:
            raise ActivityTypeNotFound(namespace=namespace, type=type, version=version)
        return row

    async def list(
        self,
        *,
        namespace: str,
        type: str,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[ActivityTypeVersion]:
        """List versions for a given ``(namespace, type)``.

        The SPL list surface is keyed on the full ``(namespace, type)``
        pair; workspace-wide enumeration across all types requires an
        index-side aggregate that lands in a follow-up
        (cross-component design § Operation: List Activity Types for
        Workflow Authoring UX). The API gateway layer (CS-IMPL-017)
        handles that fan-out by enumerating tier-relevant
        ``(namespace, type)`` keys.
        """
        return await self._store.list_activity_type_versions(
            namespace,
            type,
            cursor=cursor,
            limit=limit,
        )

    # ------------------------------------------------------------------
    # Deprecate
    # ------------------------------------------------------------------

    async def deprecate(
        self,
        *,
        workspace_id: str,
        principal_id: str,
        namespace: str,
        type: str,
        reason: str | None = None,
    ) -> None:
        """Flag the parent ``ActivityType`` row as deprecated.

        Deprecation is forward-only and acts on the parent row only.
        Version rows themselves are never mutated — the design's
        REQ-025 immutability rule means existing workflow versions
        that reference the deprecated activity continue to run; only
        *new* workflow publishes that resolve to the deprecated type
        are blocked by the resolver.

        Authorisation reuses the same namespace-tier rules as
        :meth:`register` — only a principal authorised to publish into
        the namespace may deprecate it.
        """
        self._authorise_namespace(
            namespace=namespace,
            workspace_id=workspace_id,
            principal_id=principal_id,
        )
        # Existence probe — SPL's list surface is the only available
        # cheap check (the store does not expose a get-parent method).
        probe = await self._store.list_activity_type_versions(namespace, type, limit=1)
        if not probe.items:
            raise ActivityTypeNotFound(namespace=namespace, type=type)
        await self._store.set_activity_type_deprecated(namespace, type, True)
        emit_event(
            "activity_type.deprecated",
            {
                "workspace_id": workspace_id,
                "principal_id": principal_id,
                "namespace": namespace,
                "type": type,
                "reason": reason,
            },
        )

    # ------------------------------------------------------------------
    # Resolver Protocol pass-throughs
    # ------------------------------------------------------------------

    async def resolve(
        self,
        namespace: str,
        type: str,
        semver_range: str,
    ) -> ActivityTypeVersion | None:
        """Forward to :meth:`CatalogStoreProvider.resolve`.

        Returning the SPL row directly lets the publish-time resolver
        (CS-IMPL-008) treat the registry and a raw store
        interchangeably.
        """
        return await self._store.resolve(namespace, type, semver_range)

    async def get_activity_type_version(
        self,
        namespace: str,
        type: str,
        version: str,
    ) -> ActivityTypeVersion | None:
        """Forward to :meth:`CatalogStoreProvider.get_activity_type_version`."""
        return await self._store.get_activity_type_version(namespace, type, version)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _project_manifest(
        self,
        manifest: Mapping[str, Any],
        *,
        referrer_ref: str | None,
    ) -> _ManifestProjection:
        issues: list[ActivityManifestIssue] = list(_validate_envelope(manifest))
        if issues:
            raise ActivityManifestError(issues)

        metadata = manifest["metadata"]
        namespace = metadata["namespace"]
        type_ = metadata["type"]
        version = metadata["version"]
        raw: dict[str, Any] = {
            "apiVersion": manifest["apiVersion"],
            "kind": manifest["kind"],
            "metadata": dict(metadata),
            "spec": dict(manifest.get("spec", {})),
        }
        # Persist the canonical (sorted-keys) form so the digest is a true
        # content address of what we store. ``json.loads(canonical_json(...))``
        # is the cheapest way to round-trip through ``_canonicalize`` without
        # importing the private helper.
        canonical_text = canonical_json(raw)
        normalized: dict[str, Any] = json.loads(canonical_text)
        digest = "sha256:" + canonical_hash(raw)
        _LOGGER.debug(
            "activity manifest projected: namespace=%s type=%s version=%s digest=%s json=%s",
            namespace,
            type_,
            version,
            digest,
            canonical_text,
        )
        return _ManifestProjection(
            namespace=namespace,
            type=type_,
            version=version,
            normalized=normalized,
            digest=digest,
            referrer_ref=referrer_ref,
        )

    def _authorise_namespace(
        self,
        *,
        namespace: str,
        workspace_id: str,
        principal_id: str,
    ) -> None:
        tier = _classify_namespace(namespace, workspace_id=workspace_id)
        if tier == "platform":
            if principal_id not in self._platform_admins:
                raise ActivityNamespaceError(
                    namespace=namespace,
                    tier=tier,
                    workspace_id=workspace_id,
                    principal_id=principal_id,
                    reason="reserved namespace requires platform admin",
                )
            return
        if tier == "workspace":
            return
        # vendor tier
        grants = self._vendor_grants.get(workspace_id, frozenset())
        if namespace not in grants:
            raise ActivityNamespaceError(
                namespace=namespace,
                tier=tier,
                workspace_id=workspace_id,
                principal_id=principal_id,
                reason="vendor namespace requires explicit grant",
            )

    async def _lookup_stored_digest(
        self,
        *,
        namespace: str,
        type: str,
        version: str,
    ) -> str | None:
        try:
            row = await self._store.get_activity_type_version(namespace, type, version)
        except Exception:
            _LOGGER.warning(
                "could not refetch stored digest for %s/%s@%s after ConflictDigest",
                namespace,
                type,
                version,
            )
            return None
        return row.digest if row is not None else None


# ---------------------------------------------------------------------------
# Module-private helpers
# ---------------------------------------------------------------------------


def _classify_namespace(namespace: str, *, workspace_id: str) -> str:
    """Return the tier name (``"platform" | "workspace" | "vendor"``).

    The platform tier covers the bare token ``custos.builtin`` and any
    namespace beginning with one of the :data:`_RESERVED_PREFIXES`. The
    workspace tier requires the namespace to equal the workspace ID
    literally. Everything else (any dotted vendor token,
    ``snyk``, ``aquasec``, ``acme.scanners`` ...) is the vendor tier.
    """
    if namespace == "custos.builtin" or namespace.startswith(_RESERVED_PREFIXES):
        return "platform"
    if namespace == workspace_id:
        return "workspace"
    return "vendor"


def _validate_envelope(manifest: Mapping[str, Any]) -> list[ActivityManifestIssue]:
    """Collect-all envelope validator.

    Surface-level only — full schema validation of ``spec`` is owned
    by ARM (and JSON Schema gates that live alongside the manifest).
    Catalog's job is to confirm the projection has enough metadata to
    persist the index row.
    """
    issues: list[ActivityManifestIssue] = []
    if not isinstance(manifest, Mapping):
        issues.append(
            ActivityManifestIssue(path="", code="type", message="manifest must be a JSON object"),
        )
        return issues

    api_version = manifest.get("apiVersion")
    if api_version != "custos.dev/v1":
        issues.append(
            ActivityManifestIssue(
                path="/apiVersion",
                code="value",
                message=f"expected 'custos.dev/v1', got {api_version!r}",
            ),
        )
    kind = manifest.get("kind")
    if kind != "ActivityManifest":
        issues.append(
            ActivityManifestIssue(
                path="/kind",
                code="value",
                message=f"expected 'ActivityManifest', got {kind!r}",
            ),
        )

    metadata = manifest.get("metadata")
    if not isinstance(metadata, Mapping):
        issues.append(
            ActivityManifestIssue(
                path="/metadata",
                code="required",
                message="metadata block is required",
            ),
        )
        return issues

    for field_name in ("namespace", "type", "version"):
        if field_name not in metadata:
            issues.append(
                ActivityManifestIssue(
                    path=f"/metadata/{field_name}",
                    code="required",
                    message=f"metadata.{field_name} is required",
                ),
            )

    namespace = metadata.get("namespace")
    if isinstance(namespace, str) and not _TOKEN_RE.match(namespace):
        issues.append(
            ActivityManifestIssue(
                path="/metadata/namespace",
                code="format",
                message=f"namespace {namespace!r} does not match token grammar",
            ),
        )

    type_ = metadata.get("type")
    if isinstance(type_, str) and not _TOKEN_RE.match(type_):
        issues.append(
            ActivityManifestIssue(
                path="/metadata/type",
                code="format",
                message=f"type {type_!r} does not match token grammar",
            ),
        )

    version = metadata.get("version")
    if isinstance(version, str) and not _EXACT_VERSION_RE.match(version):
        issues.append(
            ActivityManifestIssue(
                path="/metadata/version",
                code="format",
                message=(
                    f"version {version!r} is not MAJOR.MINOR.PATCH "
                    "(short forms forbidden at publish)"
                ),
            ),
        )

    return issues


__all__ = [
    "ActivityManifestError",
    "ActivityManifestIssue",
    "ActivityNamespaceError",
    "ActivityRegistryConflict",
    "ActivityRegistryError",
    "ActivityTypeNotFound",
    "ActivityTypeRef",
    "ActivityTypeRegistry",
]
