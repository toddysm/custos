"""Error taxonomy for the ``BindForStep`` RPC (CONN-IMPL-016, Phase G).

Mirrors the pattern in
:mod:`custos_connector.identity.errors`: a single exception type with a
stable :class:`BindErrorCode` enum carrying the audit-wire token and an
HTTP status code so the router layer can fan a single in-process
exception out to the right ``{"error": {"code", "detail"}}`` envelope.

The taxonomy is deliberately small — every operational failure of
``BindForStep`` maps to exactly one code. Identity-resolver failures
are folded into :attr:`BindErrorCode.IDENTITY_FAILED` (their internal
sub-codes flow through to the audit payload). Plugin-bind hook failures
are folded into :attr:`BindErrorCode.UPSTREAM_BIND_FAILED`.
"""

from __future__ import annotations

from collections.abc import Mapping
from enum import StrEnum
from types import MappingProxyType
from typing import Any


class BindErrorCode(StrEnum):
    """Stable rejection codes emitted by ``BindForStepService.bind_for_step``.

    These tokens land in ``connector.binding.rejected`` audit payloads
    and in the HTTP ``{"error": {"code"}}`` envelope; the auth gate and
    request-shape errors raised by the FastAPI middleware layer use
    their own codes and never reach this enum.
    """

    #: Request shape was invalid (empty slots, blank step coordinates,
    #: a slot with no required capabilities). Maps to HTTP 422.
    INVALID_REQUEST = "invalid-request"
    #: The slot's referenced ``connector_instance_id`` does not exist
    #: in the caller's workspace, or the catalog ``ConnectorTypeVersion``
    #: backing the instance has been deleted. Maps to HTTP 404.
    INSTANCE_NOT_FOUND = "instance-not-found"
    #: The slot's connector instance is disabled. Maps to HTTP 503 so
    #: Workflow Service's retry/backoff path treats it as "operator
    #: action required" rather than a permanent rejection.
    INSTANCE_DISABLED = "instance-disabled"
    #: The slot's connector instance is in a non-healthy state. Maps to
    #: HTTP 503 (same retry semantics as ``instance-disabled``).
    INSTANCE_UNHEALTHY = "instance-unhealthy"
    #: One or more required capabilities are not in the instance's
    #: ``used_capabilities``. Maps to HTTP 412 (precondition failed):
    #: the request is well-formed but the bind precondition does not
    #: hold; retrying without changing the instance configuration will
    #: produce the same answer.
    CAPABILITY_SHORTFALL = "capability-shortfall"
    #: The identity resolver returned an :class:`IdentityResolverError`.
    #: Maps to HTTP 502 (bad gateway): the upstream KMS / IdP rejected
    #: the resolve. The resolver's internal code + detail flow through
    #: to the audit payload but not the HTTP response.
    IDENTITY_FAILED = "identity-failed"
    #: The plugin's ``bind`` hook failed or returned a malformed
    #: response. Maps to HTTP 502.
    UPSTREAM_BIND_FAILED = "upstream-bind-failed"


# Map each code to its HTTP status so the router layer is a single
# table lookup. Kept here so the contract is local to the error type.
_STATUS_BY_CODE: dict[BindErrorCode, int] = {
    BindErrorCode.INVALID_REQUEST: 422,
    BindErrorCode.INSTANCE_NOT_FOUND: 404,
    BindErrorCode.INSTANCE_DISABLED: 503,
    BindErrorCode.INSTANCE_UNHEALTHY: 503,
    BindErrorCode.CAPABILITY_SHORTFALL: 412,
    BindErrorCode.IDENTITY_FAILED: 502,
    BindErrorCode.UPSTREAM_BIND_FAILED: 502,
}


def http_status_for(code: BindErrorCode) -> int:
    """Return the HTTP status code the router emits for ``code``."""
    return _STATUS_BY_CODE[code]


def _freeze_mapping(value: Mapping[str, Any] | None) -> Mapping[str, Any]:
    if value is None:
        return MappingProxyType({})
    return MappingProxyType(dict(value))


class BindError(Exception):
    """Domain-level rejection raised by ``BindForStepService``.

    Carries the stable :class:`BindErrorCode`, a human-readable detail,
    the slot/instance context (when known), and a frozen mapping of
    structured diagnostics. The router converts the exception into the
    ``{"error": {"code", "detail"}}`` envelope; the service layer
    converts it into a ``connector.binding.rejected`` audit event.
    """

    def __init__(
        self,
        code: BindErrorCode,
        detail: str,
        *,
        slot: str | None = None,
        instance_id: str | None = None,
        data: Mapping[str, Any] | None = None,
    ) -> None:
        self.code = code
        self.detail = detail
        self.slot = slot
        self.instance_id = instance_id
        self.data = _freeze_mapping(data)
        super().__init__(detail)


__all__ = [
    "BindError",
    "BindErrorCode",
    "http_status_for",
]
