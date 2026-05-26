"""Connector Type Registry (CS-IMPL-016).

Implements the register / list / get / deprecate surface for
``ConnectorTypeVersion`` rows. Unlike the Activity Type Registry, the
Connector Type Registry is **platform-wide** — there is no workspace
dimension on either the storage rows or the public surface. The
writer at runtime is the Connector Service at plugin-load time
(CS-IMPL-023 / COMP-005); Catalog ships the inbound endpoints in M1
so they are wired and testable before Connector Service lands.

Per design § Operation: Register Connector Type Version, Catalog
persists the **normalized projection** — type, version, declared
capabilities, ``events.delivery`` / ``events.produced``, config
schema digest — while Connector Service remains the source of truth
for the manifest itself.

Digest semantics mirror the SPL contract: re-registering the same
``(type, version)`` with an identical normalized manifest is
idempotent; a digest mismatch surfaces as
:class:`ConnectorRegistryConflict` carrying both digests.

This module deliberately does not duplicate the namespace-tier RBAC
that lives in :mod:`custos_catalog.managers.activity_registry` — the
endpoint sits at ``/v1/catalog/connector-types`` and authorisation
is enforced upstream by the gateway based on the principal's
``role:connector-service`` or platform-admin grant.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final

from custos_spl.errors import ConflictDigest
from custos_spl.interfaces.catalog_store import CatalogStoreProvider, ConnectorTypeVersion
from custos_spl.interfaces.metadata_store import MetadataStoreProvider
from custos_spl.pagination import Cursor, Page

from custos_catalog import _telemetry as telemetry
from custos_catalog.audit import (
    audit_connector_deprecated,
    audit_connector_registered,
)
from custos_catalog.normalize import canonical_hash, canonical_json

_LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Reference shapes
# ---------------------------------------------------------------------------


_TOKEN_RE: Final[re.Pattern[str]] = re.compile(r"^[a-z][a-z0-9._-]*$")
_EXACT_VERSION_RE: Final[re.Pattern[str]] = re.compile(
    r"^(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)$",
)
_CAPABILITY_TOKEN_RE: Final[re.Pattern[str]] = re.compile(r"^[a-z][a-z0-9]*(?:\.[a-z][a-z0-9]*)+$")
_SYNTHETIC_IMAGE_REF_PREFIX: Final[str] = "catalog://connector/"


@dataclass(frozen=True, slots=True)
class ConnectorTypeRef:
    """A handle to a registered connector-type version.

    ``digest`` is the SHA-256 of the canonical-JSON form of the
    normalized manifest. Together with ``(type, version)`` it
    uniquely identifies the row.
    """

    type: str
    version: str
    digest: str


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class ConnectorRegistryError(Exception):
    """Base class for connector-registry failures."""

    code: str = "catalog.connector_registry_failed"


@dataclass(frozen=True, slots=True)
class ConnectorManifestIssue:
    """One issue from manifest envelope validation."""

    path: str
    code: str
    message: str


class ConnectorManifestError(ConnectorRegistryError):
    """Raised when the connector manifest envelope fails validation."""

    code: str = "catalog.connector_manifest_invalid"

    def __init__(self, issues: list[ConnectorManifestIssue]) -> None:
        self.issues = issues
        rendered = "; ".join(f"{issue.path or '<root>'} -> {issue.message}" for issue in issues)
        super().__init__(f"connector manifest invalid: {len(issues)} issue(s): {rendered}")


class ConnectorRegistryConflict(ConnectorRegistryError):
    """Raised when the SPL surface reports a digest conflict."""

    code: str = "catalog.connector_digest_conflict"

    def __init__(
        self,
        *,
        type: str,
        version: str,
        supplied_digest: str,
        stored_digest: str | None = None,
    ) -> None:
        self.type = type
        self.version = version
        self.supplied_digest = supplied_digest
        self.stored_digest = stored_digest
        msg = (
            f"connector {type}@{version} already registered with a different digest "
            f"(supplied {supplied_digest}"
        )
        if stored_digest is not None:
            msg += f", stored {stored_digest}"
        msg += ")"
        super().__init__(msg)


class ConnectorTypeNotFound(ConnectorRegistryError):
    """Raised when ``get`` / ``deprecate`` cannot locate the connector type."""

    code: str = "catalog.connector_type_not_found"

    def __init__(self, *, type: str, version: str | None = None) -> None:
        self.type = type
        self.version = version
        if version is not None:
            msg = f"connector {type}@{version} not found"
        else:
            msg = f"connector type {type} not found"
        super().__init__(msg)


# ---------------------------------------------------------------------------
# Manager
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _ManifestProjection:
    """Internal projection — the bits we lift out of the raw manifest."""

    type: str
    version: str
    normalized: dict[str, Any]
    digest: str


class ConnectorTypeRegistry:
    """Authoring + read surface for connector-type versions.

    Args:
        catalog_store: SPL :class:`CatalogStoreProvider` instance.
        metadata_store: SPL :class:`MetadataStoreProvider` instance,
            used to append connector lifecycle events to the audit
            outbox (CS-IMPL-019).
    """

    def __init__(
        self,
        *,
        catalog_store: CatalogStoreProvider,
        metadata_store: MetadataStoreProvider,
    ) -> None:
        self._store = catalog_store
        self._metadata_store = metadata_store

    # ------------------------------------------------------------------
    # Register
    # ------------------------------------------------------------------

    async def register(
        self,
        *,
        workspace_id: str,
        principal_id: str,
        image_ref: str,
        manifest: Mapping[str, Any],
    ) -> ConnectorTypeRef:
        """Register a new connector-type version.

        Validates the manifest envelope, canonicalises it, computes a
        SHA-256 digest, and persists via
        :meth:`CatalogStoreProvider.put_connector_type_version`. A
        digest conflict on the same ``(type, version)`` raises
        :class:`ConnectorRegistryConflict` with both digests so the
        client can diff its local manifest against the stored one.

        Args:
            workspace_id: Workspace id of the call-context that
                triggered the registration. Connector types are
                globally addressable in the catalog row model but the
                audit event still carries a workspace partition key
                per the SPL contract.
            principal_id: Caller identity, recorded in the audit
                event. Authorisation is enforced upstream by the
                gateway.
            image_ref: Authoritative OCI image reference for runtime
                invocation (e.g. ``ghcr.io/org/connector@sha256:...``).
            manifest: Raw connector manifest mapping. Must carry the
                ``custos.dev/connector-manifest/v1`` envelope.
        """
        with telemetry.observe_operation(
            telemetry.OP_CONNECTOR_REGISTER,
            outcomes={
                ConnectorManifestError: "manifest_invalid",
                ConnectorRegistryConflict: "digest_conflict",
            },
        ):
            self._validate_image_ref(image_ref)
            projection = self._project_manifest(manifest)
            try:
                stored = await self._store.put_connector_type_version(
                    type=projection.type,
                    version=projection.version,
                    digest=projection.digest,
                    image_ref=image_ref,
                    normalized_manifest=projection.normalized,
                )
            except ConflictDigest as exc:
                stored_digest = await self._lookup_stored_digest(
                    type=projection.type,
                    version=projection.version,
                )
                raise ConnectorRegistryConflict(
                    type=projection.type,
                    version=projection.version,
                    supplied_digest=projection.digest,
                    stored_digest=stored_digest,
                ) from exc

            ref = ConnectorTypeRef(
                type=stored.type,
                version=stored.version,
                digest=stored.digest,
            )
        await audit_connector_registered(
            self._metadata_store,
            workspace_id=workspace_id,
            actor=principal_id,
            type_name=ref.type,
            version=ref.version,
            digest=ref.digest,
        )
        return ref

    # ------------------------------------------------------------------
    # Read surface
    # ------------------------------------------------------------------

    async def get(
        self,
        *,
        type: str,
        version: str,
    ) -> ConnectorTypeVersion:
        """Fetch one ``(type, version)`` row.

        Raises :class:`ConnectorTypeNotFound` if the row is absent.
        Parent-deprecation state is exposed via the returned row's
        ``parent_deprecated`` flag (SPL convention).
        """
        row = await self._store.get_connector_type_version(type, version)
        if row is None:
            raise ConnectorTypeNotFound(type=type, version=version)
        return row

    async def list(
        self,
        *,
        type: str,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[ConnectorTypeVersion]:
        """List versions for a given ``type``.

        The SPL list surface is keyed on the connector type; broader
        enumeration across types is the responsibility of the API
        gateway (CS-IMPL-017), which fans out over the catalog of
        known types.
        """
        return await self._store.list_connector_type_versions(
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
        type: str,
        reason: str | None = None,
    ) -> None:
        """Flag the parent ``ConnectorType`` row as deprecated.

        Deprecation is forward-only and toggles the parent row only —
        the version row itself never changes, so existing connector
        instances and bound workflow versions continue to operate.
        """
        with telemetry.observe_operation(
            telemetry.OP_CONNECTOR_DEPRECATE,
            outcomes={ConnectorTypeNotFound: "not_found"},
        ):
            probe = await self._store.list_connector_type_versions(type, limit=1)
            if not probe.items:
                raise ConnectorTypeNotFound(type=type)
            await self._store.set_connector_type_deprecated(type, True)
        await audit_connector_deprecated(
            self._metadata_store,
            workspace_id=workspace_id,
            actor=principal_id,
            type_name=type,
            reason=reason,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _project_manifest(self, manifest: Mapping[str, Any]) -> _ManifestProjection:
        issues: list[ConnectorManifestIssue] = list(_validate_envelope(manifest))
        if issues:
            raise ConnectorManifestError(issues)

        metadata = manifest["metadata"]
        type_ = metadata["type"]
        version = metadata["version"]
        raw: dict[str, Any] = {
            "apiVersion": manifest["apiVersion"],
            "kind": manifest["kind"],
            "metadata": dict(metadata),
            "spec": dict(manifest.get("spec", {})),
        }
        canonical_text = canonical_json(raw)
        normalized: dict[str, Any] = json.loads(canonical_text)
        digest = "sha256:" + canonical_hash(raw)
        _LOGGER.debug(
            "connector manifest projected: type=%s version=%s digest=%s bytes=%d",
            type_,
            version,
            digest,
            len(canonical_text),
        )
        return _ManifestProjection(
            type=type_,
            version=version,
            normalized=normalized,
            digest=digest,
        )

    async def _lookup_stored_digest(
        self,
        *,
        type: str,
        version: str,
    ) -> str | None:
        try:
            row = await self._store.get_connector_type_version(type, version)
        except Exception:
            _LOGGER.warning(
                "could not refetch stored digest for %s@%s after ConflictDigest",
                type,
                version,
            )
            return None
        return row.digest if row is not None else None

    def _validate_image_ref(self, image_ref: str) -> None:
        normalized = image_ref.strip()
        if not normalized:
            raise ConnectorManifestError(
                [
                    ConnectorManifestIssue(
                        path="/imageRef",
                        code="required",
                        message="imageRef is required",
                    )
                ]
            )
        if normalized.startswith(_SYNTHETIC_IMAGE_REF_PREFIX):
            raise ConnectorManifestError(
                [
                    ConnectorManifestIssue(
                        path="/imageRef",
                        code="value",
                        message="imageRef must be an OCI image reference, not a catalog URI",
                    )
                ]
            )
        if not _looks_like_oci_image_ref(normalized):
            raise ConnectorManifestError(
                [
                    ConnectorManifestIssue(
                        path="/imageRef",
                        code="format",
                        message=(
                            "imageRef must look like an OCI reference "
                            "(<registry>/<repo>:<tag> or <registry>/<repo>@sha256:<digest>)"
                        ),
                    )
                ]
            )


# ---------------------------------------------------------------------------
# Module-private helpers
# ---------------------------------------------------------------------------


def _validate_envelope(manifest: Mapping[str, Any]) -> list[ConnectorManifestIssue]:
    """Surface-level envelope validator.

    Catalog's job is to confirm the manifest carries enough metadata
    to persist the projection row. Per-target / per-credentials
    schema validation lives in Connector Service; we accept any spec
    shape so long as the envelope and required projection fields are
    present.
    """
    issues: list[ConnectorManifestIssue] = []
    if not isinstance(manifest, Mapping):
        issues.append(
            ConnectorManifestIssue(path="", code="type", message="manifest must be a JSON object"),
        )
        return issues

    api_version = manifest.get("apiVersion")
    if api_version != "custos.dev/connector-manifest/v1":
        issues.append(
            ConnectorManifestIssue(
                path="/apiVersion",
                code="value",
                message=f"expected 'custos.dev/connector-manifest/v1', got {api_version!r}",
            ),
        )

    kind = manifest.get("kind")
    if kind != "ConnectorManifest":
        issues.append(
            ConnectorManifestIssue(
                path="/kind",
                code="value",
                message=f"expected 'ConnectorManifest', got {kind!r}",
            ),
        )

    metadata = manifest.get("metadata")
    if not isinstance(metadata, Mapping):
        issues.append(
            ConnectorManifestIssue(
                path="/metadata",
                code="required",
                message="metadata block is required",
            ),
        )
        return issues

    for field_name in ("type", "version"):
        if field_name not in metadata:
            issues.append(
                ConnectorManifestIssue(
                    path=f"/metadata/{field_name}",
                    code="required",
                    message=f"metadata.{field_name} is required",
                ),
            )

    if "type" in metadata:
        type_ = metadata["type"]
        if not isinstance(type_, str):
            issues.append(
                ConnectorManifestIssue(
                    path="/metadata/type",
                    code="type",
                    message=(f"metadata.type must be a string, got {type(type_).__name__}"),
                ),
            )
        elif not _TOKEN_RE.match(type_):
            issues.append(
                ConnectorManifestIssue(
                    path="/metadata/type",
                    code="format",
                    message=f"type {type_!r} does not match token grammar",
                ),
            )

    if "version" in metadata:
        version = metadata["version"]
        if not isinstance(version, str):
            issues.append(
                ConnectorManifestIssue(
                    path="/metadata/version",
                    code="type",
                    message=(f"metadata.version must be a string, got {type(version).__name__}"),
                ),
            )
        elif not _EXACT_VERSION_RE.match(version):
            issues.append(
                ConnectorManifestIssue(
                    path="/metadata/version",
                    code="format",
                    message=(
                        f"version {version!r} is not MAJOR.MINOR.PATCH "
                        "(short forms forbidden at publish)"
                    ),
                ),
            )

    # ---- spec-level lightweight checks ----------------------------------
    if "spec" not in manifest:
        # Missing spec is allowed at the envelope level — Connector Service
        # validates spec contents at register time.
        return issues

    spec = manifest.get("spec")
    if not isinstance(spec, Mapping):
        issues.append(
            ConnectorManifestIssue(
                path="/spec",
                code="type",
                message="spec must be an object when present",
            ),
        )
        return issues

    capabilities = spec.get("capabilities")
    if capabilities is not None:
        if not isinstance(capabilities, list):
            issues.append(
                ConnectorManifestIssue(
                    path="/spec/capabilities",
                    code="type",
                    message="capabilities must be a list",
                ),
            )
        else:
            for idx, cap in enumerate(capabilities):
                if not isinstance(cap, str) or not _CAPABILITY_TOKEN_RE.match(cap):
                    issues.append(
                        ConnectorManifestIssue(
                            path=f"/spec/capabilities/{idx}",
                            code="format",
                            message=(
                                f"capability {cap!r} must be a dotted lowercase "
                                "token (e.g. 'oci.pull')"
                            ),
                        ),
                    )
                elif cap.startswith("event."):
                    # Design § Capabilities and Events: event.* tokens are
                    # forbidden inside capabilities — events live in events.*.
                    issues.append(
                        ConnectorManifestIssue(
                            path=f"/spec/capabilities/{idx}",
                            code="forbidden_event_token",
                            message=(
                                f"capability {cap!r} must not start with 'event.' — "
                                "use the spec.events block instead"
                            ),
                        ),
                    )

    events = spec.get("events")
    if events is not None:
        if not isinstance(events, Mapping):
            issues.append(
                ConnectorManifestIssue(
                    path="/spec/events",
                    code="type",
                    message="events must be an object when present",
                ),
            )
        else:
            delivery = events.get("delivery")
            if not isinstance(delivery, list) or not delivery:
                issues.append(
                    ConnectorManifestIssue(
                        path="/spec/events/delivery",
                        code="required",
                        message="events.delivery must be a non-empty list when events is present",
                    ),
                )
            else:
                for idx, mode in enumerate(delivery):
                    if mode not in ("push", "pull"):
                        issues.append(
                            ConnectorManifestIssue(
                                path=f"/spec/events/delivery/{idx}",
                                code="value",
                                message=f"delivery {mode!r} must be 'push' or 'pull'",
                            ),
                        )
            produced = events.get("produced")
            if not isinstance(produced, list) or not produced:
                issues.append(
                    ConnectorManifestIssue(
                        path="/spec/events/produced",
                        code="required",
                        message="events.produced must be a non-empty list when events is present",
                    ),
                )

    return issues


def _looks_like_oci_image_ref(image_ref: str) -> bool:
    """Best-effort check for OCI image references used by runtime invokers."""
    if "://" in image_ref:
        return False
    if "/" not in image_ref:
        return False
    tail = image_ref.rsplit("/", 1)[1]
    return ":" in tail or "@" in tail


__all__ = [
    "ConnectorManifestError",
    "ConnectorManifestIssue",
    "ConnectorRegistryConflict",
    "ConnectorRegistryError",
    "ConnectorTypeNotFound",
    "ConnectorTypeRef",
    "ConnectorTypeRegistry",
]
