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
    catalog_store.put_connector_type_version(type, version, digest, normalized)

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
from typing import Final

import httpx
from custos_spl import ConflictDigest
from custos_spl.interfaces.catalog_store import CatalogStoreProvider, ConnectorTypeVersion

from custos_connector.audit import emit_event
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

#: Audit event names. Centralised so tests + dashboards refer to them as
#: constants rather than open-coded strings.
AUDIT_EVENT_REGISTRATION_ACCEPTED: Final[str] = "connector.registration.accepted"
AUDIT_EVENT_REGISTRATION_REJECTED: Final[str] = "connector.registration.rejected"
AUDIT_EVENT_DEPRECATION_TOGGLED: Final[str] = "connector.deprecation.toggled"

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
        """
        self._catalog = catalog_store
        self._client = registry_client
        # Defensive copy + frozen view so the loader's view of the map
        # cannot be mutated by the caller after construction.
        self._vendor_overrides: Mapping[str, IdentityCategory] | None = (
            dict(vendor_identity_categories) if vendor_identity_categories else None
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
        repository, subject_digest = self._parse_image_ref(image_ref)

        # 1) Discover the OCI artifact descriptor that points at the
        # connector-manifest wrapper.
        try:
            descriptor = await discover_manifest(
                self._client,
                repository=repository,
                subject_digest=subject_digest,
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

        try:
            row = await self._catalog.put_connector_type_version(
                type=connector_type,
                version=connector_version,
                digest=payload_digest,
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
        """
        out: list[ConnectorTypeVersion] = []
        cursor = None
        while True:
            page = await self._catalog.list_connector_type_versions(
                connector_type,
                cursor=cursor,
                limit=limit if limit is not None and limit > 0 else None,
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
            or not layer_digest.startswith("sha256:")
            or len(layer_digest) != len("sha256:") + 64
        ):
            raise self._reject(
                code=LoaderErrorCode.INVALID_ARTIFACT_MANIFEST,
                detail=f"wrapper layer[0] digest={layer_digest!r} is not a sha256:<64-hex>",
                image_ref=image_ref,
            )
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
