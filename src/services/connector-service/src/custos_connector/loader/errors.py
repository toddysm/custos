"""Loader error taxonomy.

The Plugin Loader composes three distinct error sources:

* :class:`custos_connector.manifest.ManifestDiscoveryError` — registry
  reachability / Referrers + fallback selection failures.
* :class:`custos_connector.manifest.ManifestValidationError` — payload
  rejected by the v1 schema and post-checks.
* :class:`custos_spl.ConflictDigest` — re-registering an existing
  ``(type, version)`` with a different normalized digest.

Each of those raises with its own native code. The Loader wraps them
into a unified :class:`LoaderError` carrying a :class:`LoaderErrorCode`
so the public REST surface can format a single consistent error body
regardless of which sub-pipeline rejected. The original exception is
preserved on ``__cause__`` (set by ``raise ... from`` at the call site)
for diagnostics.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class LoaderErrorCode(StrEnum):
    """Stable rejection codes emitted by :class:`Loader`.

    Strings are part of the wire contract for the
    ``connector.registration.rejected`` audit envelope and any 4xx
    response bodies emitted by CONN-IMPL-026; do not rename without a
    contract bump.
    """

    #: ``image_ref`` was not parseable as ``<repository>@<digest>``.
    INVALID_IMAGE_REF = "invalid-image-ref"

    #: :func:`custos_connector.manifest.discover_manifest` raised. The
    #: original :class:`ManifestDiscoveryError` is on ``__cause__``; its
    #: ``code`` is mirrored into :attr:`LoaderError.detail` so audit
    #: consumers see the precise discovery sub-code without a second
    #: lookup.
    DISCOVERY_FAILED = "discovery-failed"

    #: The OCI artifact manifest at the descriptor digest does not have
    #: the wire-format shape we require: missing ``layers``, wrong
    #: ``artifactType``, wrong layer media type, or wrong layer count.
    INVALID_ARTIFACT_MANIFEST = "invalid-artifact-manifest"

    #: HTTP fetch of the artifact manifest itself or of the embedded
    #: connector-manifest payload blob returned a non-2xx response.
    PAYLOAD_FETCH_FAILED = "payload-fetch-failed"

    #: The connector-manifest payload bytes pulled from the layer blob
    #: do not match the layer descriptor's ``digest`` (sha256 mismatch).
    PAYLOAD_DIGEST_MISMATCH = "payload-digest-mismatch"

    #: The connector-manifest payload was not valid UTF-8 JSON.
    PAYLOAD_NOT_JSON = "payload-not-json"

    #: :func:`custos_connector.manifest.validate_manifest` raised. The
    #: original :class:`ManifestValidationError` is on ``__cause__``;
    #: its ``code`` is mirrored into :attr:`LoaderError.detail`.
    MANIFEST_INVALID = "manifest-invalid"

    #: ``credentials.authenticationType`` is not in the built-in
    #: identity-category map (see
    #: :data:`custos_connector.loader.BUILTIN_IDENTITY_CATEGORIES`) and
    #: is not an ``x-<vendor>`` token. The validator rejects this case
    #: first; the loader emits this code only as a defence-in-depth
    #: guard when an out-of-band vendor mapping is required and missing.
    UNKNOWN_AUTHENTICATION_TYPE = "unknown-authentication-type"

    #: ``credentials.authenticationType`` is an ``x-<vendor>`` token
    #: but no out-of-band identity-category was registered for it at
    #: :class:`Loader` construction time. Resolvable by passing
    #: ``vendor_identity_categories={...}`` to the loader.
    UNKNOWN_VENDOR_AUTH_TYPE = "unknown-vendor-auth-type"

    #: Re-registering the same ``(type, version)`` with a different
    #: normalized digest. Wraps :class:`custos_spl.ConflictDigest`.
    CONFLICT_DIGEST = "conflict-digest"


@dataclass(frozen=True, slots=True)
class LoaderError(Exception):
    """Raised by :class:`Loader` on any registration / deprecation failure.

    Attributes:
        code: A :class:`LoaderErrorCode` value (compared as a plain
            string).
        detail: Human-readable explanation of the rejection. When the
            loader wraps a sub-pipeline error (discovery / validation /
            SPL conflict), ``detail`` MUST carry the sub-code so audit
            consumers do not need to inspect ``__cause__``.
    """

    code: LoaderErrorCode
    detail: str

    def __post_init__(self) -> None:
        # dataclass frozen Exception subclasses don't set ``Exception.args``
        # automatically the way a hand-written ``Exception.__init__``
        # would, so ``str(exc)`` falls back to the default
        # ``LoaderError(...)`` repr without this nudge.
        #
        # We cannot use ``super().__init__(...)`` here because
        # ``@dataclass(slots=True)`` returns a brand-new class object
        # while ``__post_init__`` was defined on the original — the
        # zero-arg ``super()`` cell still points at the original class
        # so ``isinstance(self, original_cls)`` is False at runtime.
        # Setting ``args`` directly on the frozen instance is the
        # documented escape hatch.
        object.__setattr__(self, "args", (f"{self.code}: {self.detail}",))


__all__ = [
    "LoaderError",
    "LoaderErrorCode",
]
