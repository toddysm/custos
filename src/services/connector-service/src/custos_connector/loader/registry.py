"""Plugin Loader implementation.

End-to-end registration flow for a single connector manifest:

.. code-block:: text

    image_ref ``<repo>@sha256:<hex>``
       │
       ▼
    discover_manifest()                     # find the artifact descriptor
       │
       ▼
    GET /v2/<repo>/manifests/<descriptor>   # fetch the artifact wrapper
       │
       ▼
    GET /v2/<repo>/blobs/<layer.digest>     # fetch the payload bytes
       │
       ▼
    json.loads ─► validate_manifest         # schema + post-checks
       │
       ▼
    normalize_manifest + compute_digest     # canonical bytes + sha256
       │
       ▼
    derive_identity_category()              # authenticationType → category
       │
       ▼
    catalog_store.put_connector_type_version(type, version, digest, image_ref, normalized)

Idempotency: SPL's ``put_connector_type_version`` succeeds on identical
``(type, version, digest)`` re-puts and raises
:class:`custos_spl.ConflictDigest` when the same key is re-put with a
different digest. The loader translates the conflict into a
:class:`LoaderError` so the public surface can return a single uniform
error envelope.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Final

import httpx
from custos_spl import ConflictDigest
from custos_spl.interfaces.catalog_store import CatalogStoreProvider, ConnectorTypeVersion

from custos_connector.audit import (
    EVENT_DEPRECATION_TOGGLED as AUDIT_EVENT_DEPRECATION_TOGGLED,
)
from custos_connector.audit import (
    EVENT_REGISTRATION_ACCEPTED as AUDIT_EVENT_REGISTRATION_ACCEPTED,
)
from custos_connector.audit import (
    EVENT_REGISTRATION_REJECTED as AUDIT_EVENT_REGISTRATION_REJECTED,
)
from custos_connector.audit import (
    audit_deprecation_toggled,
    audit_registration_accepted,
    audit_registration_rejected,
    emit_event,
)
from custos_connector.loader.errors import LoaderError, LoaderErrorCode
from custos_connector.loader.identity import IdentityCategory, derive_identity_category
from custos_connector.manifest import (
    CONNECTOR_MANIFEST_MEDIA_TYPE,
    ManifestDiscoveryError,
    ManifestValidationError,
    compute_digest,
    discover_manifest,
    validate_manifest,
)
from custos_connector.manifest.capabilities import extract_capability_name

if TYPE_CHECKING:
    from custos_spl import MetadataStoreProvider

#: Audit event names. Centralised so tests + dashboards refer to them as
#: constants rather than open-coded strings. Re-exported from
#: :mod:`custos_connector.audit` so the canonical strings live in one
#: place; the local aliases retain back-compat for existing callers /
#: tests that imported them from this module before CONN-IMPL-029.
__all_audit_events__ = (
    "AUDIT_EVENT_REGISTRATION_ACCEPTED",
    "AUDIT_EVENT_REGISTRATION_REJECTED",
    "AUDIT_EVENT_DEPRECATION_TOGGLED",
)

#: ``Accept`` value sent on the artifact-manifest GET. The OCI artifact
#: wrapper always serialises as an image manifest; we list the legacy
#: Docker manifest type as well so registries that pin matching strictly
#: on Accept don't 406.
_ARTIFACT_MANIFEST_ACCEPT: Final[str] = ", ".join(
    (
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.docker.distribution.manifest.v2+json",
    )
)

#: ``<repository>@sha256:<hex>`` parser. Repository follows the OCI
#: distribution-spec name grammar (lowercase alphanumerics, separators
#: ``_`` / ``-`` / ``.`` / ``/``); we keep the check permissive here and
#: let the registry reject malformed names at HEAD/GET time rather than
#: maintain a second copy of the full grammar.
_IMAGE_REF_RE: Final[re.Pattern[str]] = re.compile(
    r"^(?P<repository>[a-z0-9][a-z0-9._\-/]*[a-z0-9])@(?P<digest>sha256:[0-9a-fA-F]{64})$"
)

#: SemVer 2.0 pattern with named groups for ordering (mirrors the
#: schema's ``metadata.version`` pattern).
_SEMVER_RE: Final[re.Pattern[str]] = re.compile(
    r"^(?P<major>0|[1-9]\d*)\.(?P<minor>0|[1-9]\d*)\.(?P<patch>0|[1-9]\d*)"
    r"(?:-(?P<pre>[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?"
    r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$"
)


#: SemVer 2.0 §11.4.1 says a numeric identifier MUST NOT include leading
#: zeros (except for ``"0"`` itself). Any all-digit identifier that has
#: a leading zero is therefore treated as alphanumeric.
def _is_numeric_identifier(ident: str) -> bool:
    return ident.isdigit() and (len(ident) == 1 or not ident.startswith("0"))


#: Per-identifier sort key. Numeric identifiers carry ``(0, int_value)``
#: so they compare numerically and rank *below* alphanumeric identifiers
#: which carry ``(1, str_value)``. This mirrors SemVer 2.0 §11.4.1-3:
#: "Identifiers consisting of only digits are compared numerically. […]
#: Numeric identifiers always have lower precedence than alphanumeric
#: identifiers."
_PreIdent = tuple[int, int] | tuple[int, str]


def _pre_identifier_key(ident: str) -> _PreIdent:
    if _is_numeric_identifier(ident):
        return (0, int(ident))
    return (1, ident)


_SemverKey = tuple[int, int, int, int, tuple[_PreIdent, ...]]


def _semver_key(
    version: str,
) -> _SemverKey:
    """Return a sortable tuple for SemVer 2.0 ordering.

    The tuple is ``(major, minor, patch, release_rank, pre_identifiers)``.

    ``release_rank`` is ``1`` for a final release (no ``-pre``) and ``0``
    for a pre-release, so a final release sorts *after* any pre-release
    of the same ``(major, minor, patch)`` triple (SemVer 2.0 §11.3).

    ``pre_identifiers`` is the dot-separated pre-release split into
    per-identifier keys per §11.4:

    * §11.4.1 — Identifiers consisting of only digits compare numerically.
    * §11.4.2 — Identifiers with letters or hyphens compare lexically.
    * §11.4.3 — Numeric identifiers always have lower precedence than
      alphanumeric identifiers.
    * §11.4.4 — A larger set of pre-release fields has higher precedence
      than a smaller set, if all preceding identifiers are equal. Python
      tuple comparison handles this naturally (shorter prefix < longer
      when the prefix matches).

    Final releases produce an empty ``()`` for ``pre_identifiers``; the
    ``release_rank`` discriminator ensures finals still order correctly
    against pre-releases despite the empty tuple sorting low.

    Example::

        >>> _semver_key("1.0.0-alpha.2") < _semver_key("1.0.0-alpha.10")
        True   # numeric comparison — NOT lexicographic (where "10" < "2")
        >>> _semver_key("1.0.0-alpha.1") < _semver_key("1.0.0-alpha.beta")
        True   # numeric < alphanumeric
        >>> _semver_key("1.0.0-alpha") < _semver_key("1.0.0-alpha.1")
        True   # shorter prefix < longer
        >>> _semver_key("1.0.0-rc.1") < _semver_key("1.0.0")
        True   # pre-release < final
    """
    match = _SEMVER_RE.fullmatch(version)
    if match is None:  # pragma: no cover - schema already rejects non-semver
        raise ValueError(f"version {version!r} is not a SemVer 2.0 string")
    major = int(match.group("major"))
    minor = int(match.group("minor"))
    patch = int(match.group("patch"))
    pre = match.group("pre") or ""
    release_rank = 0 if pre else 1
    pre_identifiers: tuple[_PreIdent, ...] = (
        tuple(_pre_identifier_key(ident) for ident in pre.split(".")) if pre else ()
    )
    return (major, minor, patch, release_rank, pre_identifiers)


@dataclass(frozen=True, slots=True)
class LoadedConnectorType:
    """Result of a successful :meth:`Loader.register` call.

    Attributes:
        row: The :class:`ConnectorTypeVersion` returned by SPL. Carries
            the persisted ``type`` / ``version`` / ``digest`` /
            ``normalized_manifest`` and the denormalised
            ``parent_deprecated`` flag.
        identity_category: Derived from
            ``normalized_manifest.spec.credentials.authenticationType``;
            not persisted as a separate column because it is a pure
            function of the manifest (see design § Identity and
            Credential Model).
    """

    row: ConnectorTypeVersion
    identity_category: IdentityCategory


class Loader:
    """Register, look up, and deprecate :class:`ConnectorTypeVersion` rows.

    A loader is a thin object that composes the manifest pipeline +
    discovery client + :class:`CatalogStoreProvider`. It owns no state
    of its own beyond the injected dependencies and is safe to construct
    per-request or per-app; the underlying ``httpx.AsyncClient`` and
    :class:`CatalogStoreProvider` are the resource-holding parts.
    """

    def __init__(
        self,
        *,
        catalog_store: CatalogStoreProvider,
        registry_client: httpx.AsyncClient,
        vendor_identity_categories: Mapping[str, IdentityCategory] | None = None,
        metadata_store: MetadataStoreProvider | None = None,
    ) -> None:
        """
        Args:
            catalog_store: The SPL CatalogStoreProvider Connector Service
                will write the new ``ConnectorTypeVersion`` row through.
            registry_client: An ``httpx.AsyncClient`` already configured
                with the OCI registry base URL, authentication, and
                timeout. The caller owns the client lifecycle.
            vendor_identity_categories: Optional out-of-band identity
                categories for ``x-<vendor>`` ``authenticationType``
                tokens. Built-in token mappings are NOT overridable.
            metadata_store: Optional
                :class:`~custos_spl.MetadataStoreProvider`. When
                provided, the loader writes
                ``connector.registration.{accepted,rejected}`` and
                ``connector.deprecation.toggled`` audit rows through the
                SPL outbox under :data:`PLATFORM_WORKSPACE_ID`. When
                ``None`` (the unit-test path), the loader falls back
                to the legacy log-only :func:`emit_event` shim so
                existing tests continue to assert on the structured
                log line.
        """
        self._catalog = catalog_store
        self._client = registry_client
        self._metadata_store = metadata_store
        # Defensive copy + read-only view so the loader's view of the map
        # cannot be mutated by the caller after construction. The copy
        # captures the caller's snapshot; the ``MappingProxyType`` wrapper
        # ensures the loader cannot accidentally mutate it either.
        self._vendor_overrides: Mapping[str, IdentityCategory] | None = (
            MappingProxyType(dict(vendor_identity_categories))
            if vendor_identity_categories
            else None
        )

    # ------------------------------------------------------------------
    # Public surface
    # ------------------------------------------------------------------

    async def register(self, image_ref: str) -> LoadedConnectorType:
        """Discover, validate, and persist a connector manifest.

        Args:
            image_ref: ``<repository>@sha256:<hex>`` reference of the
                connector OCI image. Tag-based references are not
                accepted in v1 — they require a separate tag-resolution
                step (HEAD the tag → ``Docker-Content-Digest``) which is
                tracked under CONN-IMPL-026's REST adapter so it can
                surface a stable 4xx code on resolution failure.

        Returns:
            A :class:`LoadedConnectorType` carrying the persisted row
            and the derived :class:`IdentityCategory`.

        Raises:
            LoaderError: With one of the :class:`LoaderErrorCode`
                values. Every failure path emits
                ``connector.registration.rejected`` with the same code
                in its payload before raising.
        """
        try:
            return await self._register_impl(image_ref)
        except LoaderError as exc:
            # CONN-IMPL-029 (Phase K) — emit the typed
            # ``connector.registration.rejected`` audit row through the
            # SPL outbox in addition to the log-only line that
            # :meth:`_reject` already wrote. The legacy log line stays
            # so unit tests that don't wire a metadata store keep
            # passing; the SPL row lands whenever a metadata store is
            # available.
            if self._metadata_store is not None:
                await audit_registration_rejected(
                    self._metadata_store,
                    image_ref=image_ref,
                    code=str(exc.code),
                    detail=exc.detail,
                )
            raise

    async def _register_impl(self, image_ref: str) -> LoadedConnectorType:
        """Run the registration pipeline; see :meth:`register` for the
        public contract."""
        try:
            repository, subject_digest = self._parse_image_ref(image_ref)
        except LoaderError as exc:
            raise self._reject(
                code=exc.code,
                detail=exc.detail,
                image_ref=image_ref,
            ) from exc

        # 1) Discover the OCI artifact descriptor that points at the
        # connector-manifest wrapper.
        try:
            descriptor = await discover_manifest(
                self._client,
                repository=repository,
                subject_digest=subject_digest,
                metadata_store=self._metadata_store,
            )
        except ManifestDiscoveryError as exc:
            raise self._reject(
                code=LoaderErrorCode.DISCOVERY_FAILED,
                detail=f"discovery rejected with code={exc.code}: {exc.detail}",
                image_ref=image_ref,
            ) from exc

        # 2) Fetch the artifact wrapper itself and extract the
        # connector-manifest payload layer descriptor.
        layer_digest, layer_size = await self._fetch_payload_layer(
            repository=repository,
            descriptor_digest=descriptor.digest,
            image_ref=image_ref,
        )

        # 3) Fetch the payload bytes and verify against the layer digest.
        payload_bytes = await self._fetch_payload_blob(
            repository=repository,
            layer_digest=layer_digest,
            layer_size=layer_size,
            image_ref=image_ref,
        )

        # 4) Parse + validate + normalize.
        try:
            payload = json.loads(payload_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise self._reject(
                code=LoaderErrorCode.PAYLOAD_NOT_JSON,
                detail=f"connector-manifest payload was not valid UTF-8 JSON: {exc}",
                image_ref=image_ref,
            ) from exc

        try:
            validated = validate_manifest(payload)
        except ManifestValidationError as exc:
            raise self._reject(
                code=LoaderErrorCode.MANIFEST_INVALID,
                detail=f"manifest rejected with code={exc.code} at {exc.path!r}: {exc.detail}",
                image_ref=image_ref,
            ) from exc

        normalized, payload_digest = compute_digest(validated)

        # 5) Derive identity category.
        spec = normalized.get("spec", {})
        credentials = spec.get("credentials", {}) if isinstance(spec, dict) else {}
        authentication_type = (
            credentials.get("authenticationType") if isinstance(credentials, dict) else None
        )
        if not isinstance(authentication_type, str):  # pragma: no cover - schema-blocked
            # Should have been blocked by the validator, but keep a
            # defensive code path so the loader's contract holds even
            # against a future schema regression.
            raise self._reject(
                code=LoaderErrorCode.MANIFEST_INVALID,
                detail="normalized manifest missing spec.credentials.authenticationType",
                image_ref=image_ref,
            )
        try:
            identity_category = derive_identity_category(
                authentication_type,
                vendor_overrides=self._vendor_overrides,
            )
        except LoaderError as exc:
            # Re-emit the rejection with the registration context, then
            # re-raise the same code so callers see a single envelope.
            raise self._reject(
                code=exc.code,
                detail=exc.detail,
                image_ref=image_ref,
            ) from exc

        # 6) Persist.
        metadata = normalized.get("metadata", {})
        connector_type = metadata["type"]
        connector_version = metadata["version"]

        # 6a) Capability-regression check (CONN-IMPL-009, design §
        # Compatibility policy). Runs before put_connector_type_version
        # so a regression is rejected with its own audit code rather
        # than being silently persisted and discovered at bind time.
        spec_caps = spec.get("capabilities", []) if isinstance(spec, dict) else []
        await self._check_capability_regression(
            connector_type=connector_type,
            new_version=connector_version,
            new_capabilities=list(spec_caps) if isinstance(spec_caps, list) else [],
            image_ref=image_ref,
        )

        try:
            row = await self._catalog.put_connector_type_version(
                type=connector_type,
                version=connector_version,
                digest=payload_digest,
                image_ref=image_ref,
                normalized_manifest=normalized,
            )
        except ConflictDigest as exc:
            raise self._reject(
                code=LoaderErrorCode.CONFLICT_DIGEST,
                detail=(
                    f"({connector_type!r}, {connector_version!r}) already registered "
                    f"with a different digest; new digest={payload_digest}"
                ),
                image_ref=image_ref,
            ) from exc

        emit_event(
            AUDIT_EVENT_REGISTRATION_ACCEPTED,
            {
                "image_ref": image_ref,
                "type": connector_type,
                "version": connector_version,
                "digest": payload_digest,
                "identity_category": str(identity_category),
                "authentication_type": authentication_type,
            },
        )
        if self._metadata_store is not None:
            await audit_registration_accepted(
                self._metadata_store,
                type_name=connector_type,
                version=connector_version,
                image_ref=image_ref,
                manifest_digest=payload_digest,
            )

        return LoadedConnectorType(row=row, identity_category=identity_category)

    async def get(
        self,
        connector_type: str,
        version: str,
    ) -> ConnectorTypeVersion | None:
        """Look up one connector-type version.

        Returns ``None`` if absent. Equivalent to the SPL pass-through;
        kept on the loader so call sites in the public surface have a
        single dependency to inject.
        """
        return await self._catalog.get_connector_type_version(connector_type, version)

    async def list_versions(
        self,
        connector_type: str,
        *,
        limit: int | None = None,
    ) -> list[ConnectorTypeVersion]:
        """Return every persisted version for ``connector_type``.

        Walks SPL's pagination cursor to completion. ``limit`` caps the
        total number of rows returned (the cursor walk stops as soon as
        the cap is hit) — a hard cap is enforced when the caller passes
        one because SPL accepts ``None`` as "implementation default" and
        we want the loader's contract to be explicit.

        Args:
            connector_type: The parent connector type to list.
            limit: Optional cap on the number of rows returned. Must be
                a non-negative integer when provided. ``limit=0``
                returns the empty list without contacting SPL;
                ``limit=None`` (the default) walks the whole cursor
                chain.

        Raises:
            ValueError: When ``limit`` is negative. We refuse rather
                than silently mapping negative limits to ``out[:limit]``
                (which would drop trailing rows the caller never asked
                to drop).
        """
        if limit is not None and limit < 0:
            raise ValueError(f"limit must be non-negative, got {limit!r}")
        if limit == 0:
            return []
        out: list[ConnectorTypeVersion] = []
        cursor = None
        while True:
            page = await self._catalog.list_connector_type_versions(
                connector_type,
                cursor=cursor,
                limit=limit,
            )
            out.extend(page.items)
            if limit is not None and len(out) >= limit:
                return out[:limit]
            if page.next_cursor is None:
                return out
            cursor = page.next_cursor

    async def deprecate(
        self,
        connector_type: str,
        *,
        deprecated: bool = True,
    ) -> None:
        """Toggle the parent ``ConnectorType`` deprecation flag.

        SPL's ``set_connector_type_deprecated`` is idempotent — calling
        with the current value is a no-op at the storage layer; the
        loader still emits ``connector.deprecation.toggled`` so audit
        logs record the operator-initiated transition (or attempted
        transition) regardless.
        """
        await self._catalog.set_connector_type_deprecated(
            connector_type,
            deprecated=deprecated,
        )
        emit_event(
            AUDIT_EVENT_DEPRECATION_TOGGLED,
            {"type": connector_type, "deprecated": deprecated},
        )
        if self._metadata_store is not None:
            # The loader exposes no per-version surface here (the SPL
            # call deprecates the whole connector-type, not one
            # version). Emit with version="*" so the typed subject
            # still pins (type, version) per the helper contract.
            await audit_deprecation_toggled(
                self._metadata_store,
                type_name=connector_type,
                version="*",
                deprecated=deprecated,
            )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_image_ref(image_ref: str) -> tuple[str, str]:
        """Split ``<repo>@sha256:<hex>`` into ``(repo, digest)``.

        Raises:
            LoaderError: With code
                :attr:`LoaderErrorCode.INVALID_IMAGE_REF` when the
                reference does not match the v1 grammar.
        """
        match = _IMAGE_REF_RE.fullmatch(image_ref) if isinstance(image_ref, str) else None
        if match is None:
            raise LoaderError(
                code=LoaderErrorCode.INVALID_IMAGE_REF,
                detail=(
                    f"image_ref {image_ref!r} must match '<repository>@sha256:<64-hex>' "
                    "(tag-based references are not supported in v1)"
                ),
            )
        return match.group("repository"), match.group("digest").lower()

    async def _fetch_payload_layer(
        self,
        *,
        repository: str,
        descriptor_digest: str,
        image_ref: str,
    ) -> tuple[str, int]:
        """GET the OCI artifact wrapper and return ``(layer_digest, size)``.

        The wrapper must:

        * deserialize as a JSON object,
        * carry ``artifactType=CONNECTOR_MANIFEST_MEDIA_TYPE``,
        * carry exactly one ``layers`` entry,
        * whose ``mediaType`` equals ``CONNECTOR_MANIFEST_MEDIA_TYPE``,
        * with ``digest`` shaped ``sha256:<64-hex>`` and a positive
          ``size``.

        Any deviation emits ``connector.registration.rejected`` with
        :attr:`LoaderErrorCode.INVALID_ARTIFACT_MANIFEST` and raises.
        """
        path = f"/v2/{repository}/manifests/{descriptor_digest}"
        try:
            response = await self._client.get(path, headers={"Accept": _ARTIFACT_MANIFEST_ACCEPT})
        except httpx.HTTPError as exc:
            raise self._reject(
                code=LoaderErrorCode.PAYLOAD_FETCH_FAILED,
                detail=f"GET {path} failed: {exc}",
                image_ref=image_ref,
            ) from exc
        if response.status_code != 200:
            raise self._reject(
                code=LoaderErrorCode.PAYLOAD_FETCH_FAILED,
                detail=f"GET {path} returned status={response.status_code}",
                image_ref=image_ref,
            )

        try:
            wrapper = response.json()
        except (ValueError, json.JSONDecodeError) as exc:
            raise self._reject(
                code=LoaderErrorCode.INVALID_ARTIFACT_MANIFEST,
                detail=f"GET {path} body was not JSON: {exc}",
                image_ref=image_ref,
            ) from exc
        if not isinstance(wrapper, dict):
            raise self._reject(
                code=LoaderErrorCode.INVALID_ARTIFACT_MANIFEST,
                detail=f"GET {path} returned a non-object body of type {type(wrapper).__name__}",
                image_ref=image_ref,
            )

        artifact_type = wrapper.get("artifactType")
        if artifact_type != CONNECTOR_MANIFEST_MEDIA_TYPE:
            raise self._reject(
                code=LoaderErrorCode.INVALID_ARTIFACT_MANIFEST,
                detail=(
                    f"wrapper artifactType={artifact_type!r}, expected "
                    f"{CONNECTOR_MANIFEST_MEDIA_TYPE!r}"
                ),
                image_ref=image_ref,
            )

        layers = wrapper.get("layers")
        if not isinstance(layers, list) or len(layers) != 1:
            raise self._reject(
                code=LoaderErrorCode.INVALID_ARTIFACT_MANIFEST,
                detail=(
                    "wrapper must carry exactly one layer; got "
                    f"{len(layers) if isinstance(layers, list) else 'none'}"
                ),
                image_ref=image_ref,
            )
        layer = layers[0]
        if not isinstance(layer, dict):
            raise self._reject(
                code=LoaderErrorCode.INVALID_ARTIFACT_MANIFEST,
                detail=f"wrapper layer[0] is not an object (type={type(layer).__name__})",
                image_ref=image_ref,
            )
        if layer.get("mediaType") != CONNECTOR_MANIFEST_MEDIA_TYPE:
            raise self._reject(
                code=LoaderErrorCode.INVALID_ARTIFACT_MANIFEST,
                detail=(
                    f"wrapper layer[0] mediaType={layer.get('mediaType')!r}, expected "
                    f"{CONNECTOR_MANIFEST_MEDIA_TYPE!r}"
                ),
                image_ref=image_ref,
            )
        layer_digest = layer.get("digest")
        layer_size = layer.get("size")
        if (
            not isinstance(layer_digest, str)
            or re.fullmatch(r"sha256:[0-9a-fA-F]{64}", layer_digest) is None
        ):
            raise self._reject(
                code=LoaderErrorCode.INVALID_ARTIFACT_MANIFEST,
                detail=f"wrapper layer[0] digest={layer_digest!r} is not a sha256:<64-hex>",
                image_ref=image_ref,
            )
        layer_digest = layer_digest.lower()
        if not isinstance(layer_size, int) or layer_size <= 0:
            raise self._reject(
                code=LoaderErrorCode.INVALID_ARTIFACT_MANIFEST,
                detail=f"wrapper layer[0] size={layer_size!r} must be a positive int",
                image_ref=image_ref,
            )
        return layer_digest, layer_size

    async def _fetch_payload_blob(
        self,
        *,
        repository: str,
        layer_digest: str,
        layer_size: int,
        image_ref: str,
    ) -> bytes:
        """GET the connector-manifest payload blob and verify its digest.

        Compares the layer descriptor's ``digest`` against
        ``sha256(body)`` so a registry that returns the wrong blob
        (or a body whose ``Content-Length`` lies) is caught locally
        before validation runs.
        """
        path = f"/v2/{repository}/blobs/{layer_digest}"
        try:
            response = await self._client.get(path)
        except httpx.HTTPError as exc:
            raise self._reject(
                code=LoaderErrorCode.PAYLOAD_FETCH_FAILED,
                detail=f"GET {path} failed: {exc}",
                image_ref=image_ref,
            ) from exc
        if response.status_code != 200:
            raise self._reject(
                code=LoaderErrorCode.PAYLOAD_FETCH_FAILED,
                detail=f"GET {path} returned status={response.status_code}",
                image_ref=image_ref,
            )

        body = response.content
        actual_digest = "sha256:" + hashlib.sha256(body).hexdigest()
        if actual_digest != layer_digest:
            raise self._reject(
                code=LoaderErrorCode.PAYLOAD_DIGEST_MISMATCH,
                detail=(
                    f"payload sha256 {actual_digest} does not match layer digest {layer_digest}"
                ),
                image_ref=image_ref,
            )
        if len(body) != layer_size:
            # Defensive: the digest check above already covers byte-level
            # tampering, but a length mismatch with a matching digest is
            # impossible only if sha256 is collision-free — emit a
            # distinct rejection so operator dashboards can flag the
            # offending registry implementation.
            raise self._reject(  # pragma: no cover - digest match implies len match
                code=LoaderErrorCode.PAYLOAD_DIGEST_MISMATCH,
                detail=(f"payload length {len(body)} does not match layer size {layer_size}"),
                image_ref=image_ref,
            )
        return body

    async def _check_capability_regression(
        self,
        *,
        connector_type: str,
        new_version: str,
        new_capabilities: list[object],
        image_ref: str,
    ) -> None:
        """Reject patch/minor bumps that drop a prior capability.

        Per design § Compatibility policy: within the same major, the
        new version's capability set MUST be a strict superset of the
        immediate predecessor's. Capability removal is only permitted
        at a major bump.

        Implementation:

        1. Parse the new version's major. (Validator already enforced
           the SemVer pattern; ``_semver_key`` would raise on a stray
           non-SemVer string but that path is unreachable in production.)
        2. Walk every persisted version for ``connector_type``,
           ignoring those in a different major.
        3. Pick the predecessor — the highest version strictly less
           than ``new_version`` in the same major.
        4. If a predecessor exists, diff the capability name sets;
           any name present in the predecessor but missing from the
           new version is a regression.

        ``new_capabilities`` is the list of capability entries (string
        or object form) from the validated, normalized manifest.
        """
        new_key = _semver_key(new_version)
        new_major = new_key[0]
        new_names = {extract_capability_name(c) for c in new_capabilities}

        predecessor: ConnectorTypeVersion | None = None
        predecessor_key: _SemverKey | None = None
        cursor = None
        while True:
            page = await self._catalog.list_connector_type_versions(
                connector_type,
                cursor=cursor,
            )
            for row in page.items:
                try:
                    row_key = _semver_key(row.version)
                except ValueError:  # pragma: no cover - schema-blocked
                    continue
                if row_key[0] != new_major:
                    continue
                if row_key >= new_key:
                    continue
                if predecessor_key is None or row_key > predecessor_key:
                    predecessor = row
                    predecessor_key = row_key
            if page.next_cursor is None:
                break
            cursor = page.next_cursor

        if predecessor is None:
            return

        prior_spec = predecessor.normalized_manifest.get("spec", {})
        prior_caps = prior_spec.get("capabilities", []) if isinstance(prior_spec, dict) else []
        if not isinstance(prior_caps, list):  # pragma: no cover - schema-blocked
            return
        prior_names = {extract_capability_name(c) for c in prior_caps}
        dropped = prior_names - new_names
        if dropped:
            raise self._reject(
                code=LoaderErrorCode.CAPABILITY_REGRESSION,
                detail=(
                    f"capabilities {sorted(dropped)!r} present in parent version "
                    f"{predecessor.version!r} are missing from {new_version!r}; "
                    f"patch/minor bumps within the same major must add "
                    f"capabilities, not drop them (major bump required to remove)"
                ),
                image_ref=image_ref,
            )

    @staticmethod
    def _reject(
        *,
        code: LoaderErrorCode,
        detail: str,
        image_ref: str,
    ) -> LoaderError:
        """Emit ``connector.registration.rejected`` and build the wrapper.

        Returns the exception so the caller can use
        ``raise self._reject(...) from exc`` to preserve the cause.
        """
        emit_event(
            AUDIT_EVENT_REGISTRATION_REJECTED,
            {
                "image_ref": image_ref,
                "code": str(code),
                "detail": detail,
            },
        )
        return LoaderError(code=code, detail=detail)


__all__ = [
    "AUDIT_EVENT_DEPRECATION_TOGGLED",
    "AUDIT_EVENT_REGISTRATION_ACCEPTED",
    "AUDIT_EVENT_REGISTRATION_REJECTED",
    "LoadedConnectorType",
    "Loader",
]
