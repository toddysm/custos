"""Identity-category derivation from ``credentials.authenticationType``.

Per design § Identity and Credential Model, the manifest does not
declare which identity category (KMS-backed / workload identity /
federated) a connector instance falls into — Connector Service derives
it from the concrete ``authenticationType``. Built-in mappings are
exhaustively listed here so unknown values are caught at registration
time; vendor ``x-<vendor>`` types declare their category out of band
via a registration-time mapping passed to :class:`Loader`.
"""

from __future__ import annotations

from collections.abc import Mapping
from enum import StrEnum
from types import MappingProxyType
from typing import Final

from custos_connector.loader.errors import LoaderError, LoaderErrorCode


class IdentityCategory(StrEnum):
    """The three identity categories supported in v1.

    Values are the stable wire form (lowercase) used by audit envelopes
    and the future Identity Resolvers (CONN-IMPL-015).
    """

    #: KMS-backed credentials — the connector reads credential material
    #: from a managed KMS (Azure Key Vault, AWS Secrets Manager, etc.)
    #: using its own workload identity.
    KMS = "kms"

    #: Workload identity — the connector uses its assigned
    #: managed/workload identity directly against the upstream system.
    WORKLOAD = "workload"

    #: Federated identity — token-exchange (OIDC today, extensible later).
    FEDERATED = "federated"


#: Mapping from concrete ``authenticationType`` to identity category.
#: Wrapped in :class:`types.MappingProxyType` so callers see a typed
#: read-only view and a stray mutation can't silently corrupt the
#: derivation rule. Adding an entry here is the v1 way to teach the
#: platform a new built-in auth type; vendor ``x-*`` types use the
#: per-registration override map on :class:`Loader` instead.
BUILTIN_IDENTITY_CATEGORIES: Final[Mapping[str, IdentityCategory]] = MappingProxyType(
    {
        "azure-key-vault": IdentityCategory.KMS,
        "amazon-kms": IdentityCategory.KMS,
        "azure-managed-identity": IdentityCategory.WORKLOAD,
        "oidc": IdentityCategory.FEDERATED,
    }
)


def derive_identity_category(
    authentication_type: str,
    *,
    vendor_overrides: Mapping[str, IdentityCategory] | None = None,
) -> IdentityCategory:
    """Resolve ``authentication_type`` to its :class:`IdentityCategory`.

    Resolution order:

    1. Exact match in :data:`BUILTIN_IDENTITY_CATEGORIES`.
    2. Exact match in ``vendor_overrides`` (only consulted for
       ``x-<vendor>`` tokens — built-ins cannot be overridden because
       that would let an operator silently change platform semantics).
    3. Otherwise: rejection.

    The schema validator (:func:`custos_connector.manifest.validate_manifest`)
    already constrains the manifest to a known built-in or an
    ``x-<vendor>`` token, so the only path that reaches the
    rejection branch in normal operation is an ``x-<vendor>`` token
    without a registered override.

    Args:
        authentication_type: Value of ``spec.credentials.authenticationType``.
        vendor_overrides: Optional out-of-band map for ``x-<vendor>``
            tokens. Built-in tokens in this map are ignored.

    Returns:
        The resolved :class:`IdentityCategory`.

    Raises:
        LoaderError: With code
            :attr:`LoaderErrorCode.UNKNOWN_AUTHENTICATION_TYPE` for a
            non-built-in non-``x-*`` value, or
            :attr:`LoaderErrorCode.UNKNOWN_VENDOR_AUTH_TYPE` for an
            ``x-<vendor>`` token without a registered override.
    """
    if authentication_type in BUILTIN_IDENTITY_CATEGORIES:
        return BUILTIN_IDENTITY_CATEGORIES[authentication_type]

    if authentication_type.startswith("x-"):
        if vendor_overrides is not None and authentication_type in vendor_overrides:
            return vendor_overrides[authentication_type]
        raise LoaderError(
            code=LoaderErrorCode.UNKNOWN_VENDOR_AUTH_TYPE,
            detail=(
                f"vendor authenticationType {authentication_type!r} has no registered "
                f"identity category; pass vendor_identity_categories={{...}} to Loader"
            ),
        )

    raise LoaderError(
        code=LoaderErrorCode.UNKNOWN_AUTHENTICATION_TYPE,
        detail=(
            f"authenticationType {authentication_type!r} is not a built-in token "
            f"({sorted(BUILTIN_IDENTITY_CATEGORIES)}) and does not match the "
            "x-<vendor> extension grammar"
        ),
    )


__all__ = [
    "BUILTIN_IDENTITY_CATEGORIES",
    "IdentityCategory",
    "derive_identity_category",
]
