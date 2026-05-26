"""Error taxonomy for the identity-resolver path.

These mirror the :class:`~custos_connector.runtime.PluginRuntimeError`
shape (stable string ``code`` + free-form ``detail`` + frozen ``data``
mapping) so the audit envelope can carry the same fields without
per-resolver special-casing. The codes are part of the audit-wire
contract: do not rename without bumping the
``connector.identity.failed`` schema version.
"""

from __future__ import annotations

from collections.abc import Mapping
from enum import StrEnum
from types import MappingProxyType
from typing import Any


class IdentityResolverErrorCode(StrEnum):
    """Stable rejection codes emitted by an :class:`IdentityResolver`.

    These are persisted in the ``connector.identity.failed`` audit
    payload, so the values are part of the wire contract.
    """

    #: The :class:`IdentityResolverRegistry` did not have a resolver
    #: registered for the supplied ``authenticationType``.
    UNKNOWN_AUTHENTICATION_TYPE = "unknown-authentication-type"

    #: A resolver was registered for the supplied ``authenticationType``
    #: but its ``category`` does not match the one the Loader derived at
    #: registration time — a misconfiguration of the vendor override map.
    CATEGORY_MISMATCH = "category-mismatch"

    #: A required field is missing from ``credentials.authentication``.
    MISSING_CREDENTIAL_FIELD = "missing-credential-field"

    #: A field in ``credentials.authentication`` is structurally
    #: malformed (wrong type, unparseable URL, empty string, etc).
    INVALID_CREDENTIAL_FIELD = "invalid-credential-field"

    #: Network reachability failure against the upstream identity
    #: provider (DNS, TLS, timeout).
    UPSTREAM_UNAVAILABLE = "upstream-unavailable"

    #: The upstream identity provider returned 401/403 (or a vendor
    #: equivalent) — the workload's own identity is not authorized to
    #: read the requested material.
    UPSTREAM_UNAUTHORIZED = "upstream-unauthorized"

    #: The upstream identity provider returned a non-success, non-auth
    #: error (4xx other than 401/403, or 5xx).
    UPSTREAM_REJECTED = "upstream-rejected"

    #: The upstream identity provider returned a success status but the
    #: payload could not be decoded into the expected shape.
    INVALID_UPSTREAM_RESPONSE = "invalid-upstream-response"


def _freeze_mapping(value: Mapping[str, Any] | None) -> Mapping[str, Any]:
    if value is None:
        return MappingProxyType({})
    return MappingProxyType(dict(value))


class IdentityResolverError(Exception):
    """Resolver-side error raised by :meth:`IdentityResolver.resolve`.

    Mirrors :class:`~custos_connector.runtime.PluginRuntimeError`:

    * ``code`` is the stable audit-wire token.
    * ``detail`` is a free-form human-readable string used by both the
      audit payload and any operator-visible log line.
    * ``data`` is a frozen mapping carrying structured diagnostics —
      typically ``status_code``, ``reason``, or ``field`` — that we want
      preserved alongside the human detail.

    Resolvers wrap *all* upstream failures into this type before
    re-raising; the registry catches it and converts it into a
    ``connector.identity.failed`` audit event. No upstream-vendor
    exception type ever escapes the resolver boundary.
    """

    code: IdentityResolverErrorCode = IdentityResolverErrorCode.UPSTREAM_REJECTED

    def __init__(
        self,
        detail: str,
        *,
        code: IdentityResolverErrorCode | None = None,
        data: Mapping[str, Any] | None = None,
    ) -> None:
        if code is not None:
            self.code = code
        self.detail = detail
        self.data = _freeze_mapping(data)
        super().__init__(detail)


__all__ = [
    "IdentityResolverError",
    "IdentityResolverErrorCode",
]
