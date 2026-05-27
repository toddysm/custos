"""Lease Gateway \u2014 HTTP client for the Connector Service internal RPC.

Delegates lease bookkeeping (capacity tracking + audit emission) to
Connector Service over ``/internal/v1/leases:{issue,refresh,release}``.
The sidecar never touches Postgres directly; the gateway is the single
choke-point.

Wire contract (locked by the matching CS router in CONN-IMPL-019):

* ``POST /internal/v1/leases:issue`` body:
  ``{runId, stepId, attempt, slot, capability, connectorInstanceId,
  tokenType, requestedTtlSec?, typeMaxTtlSec?, instanceTtlSec?,
  stepDeadline?}`` \u2192 200 ``{lease: {...full lease envelope...}}``
  or 4xx ``{error: {code, detail}}``.
* ``POST /internal/v1/leases:refresh`` body:
  ``{leaseId, requestedTtlSec?, ...}`` \u2192 200 ``{lease: {...}}``.
* ``POST /internal/v1/leases:release`` body: ``{leaseId}`` \u2192 204.

The ``workspaceId`` is not carried in the body \u2014 the call-context
header carries it. The gateway attaches an ``X-Call-Context`` header
on every request, picked from the supplied :class:`LeaseGatewaySettings`
(in production this comes from the same secret bundle ARM seeds the
sidecar with at startup).

Domain failures returned by CS (``CAPACITY_EXCEEDED``, ``NOT_FOUND``,
``ALREADY_RELEASED``, ``INVALID_REQUEST``) are decoded back into a
:class:`GatewayLeaseError` carrying the same code so the router layer
can map them to the right :class:`SidecarErrorCode` 1:1.

Transport-level failures (connect timeout, 5xx, malformed body) raise
:class:`GatewayTransportError` which the router maps to
:class:`SidecarErrorCode.CONNECTOR_UNAVAILABLE` (HTTP 503) per the
design's failure-mode table for "instance disabled / Connector
Service unreachable".
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import httpx

CALLCTX_HEADER = "X-Call-Context"


@dataclass(frozen=True, slots=True)
class LeaseGatewaySettings:
    """Static configuration for the gateway.

    Attributes:
        connector_service_url: Base URL for Connector Service, e.g.
            ``http://connector-service:8080``. The gateway appends the
            ``/internal/v1/leases:*`` path itself.
        call_context: Pre-serialized call-context blob sent in
            ``X-Call-Context``. Carries the workspace id, principal id
            (``svc:connector-sidecar``), and the
            ``connector:lease-mint`` permission. ARM provisions this
            string at sidecar start; the sidecar treats it as opaque.
        timeout_sec: Per-request total timeout. Defaults to 5s so a
            stalled CS does not pin the activity for the full UDS
            request budget.
    """

    connector_service_url: str
    call_context: str
    timeout_sec: float = 5.0


@dataclass(frozen=True, slots=True)
class LeaseRecord:
    """Decoded lease envelope returned by Connector Service.

    Datetime fields arrive as RFC 3339 strings and are parsed into
    timezone-aware :class:`datetime`. ``None`` fields are preserved.
    """

    workspace_id: str
    lease_id: str
    run_id: str
    step_id: str
    attempt: int
    slot: str
    capability: str
    connector_instance_id: str
    token_type: str
    issued_at: datetime
    expires_at: datetime
    released_at: datetime | None
    revoked_at: datetime | None
    revoke_reason: str | None

    @classmethod
    def from_wire(cls, wire: dict[str, Any]) -> LeaseRecord:
        """Decode the ``{lease: ...}`` envelope's inner record."""

        def _dt(value: str | None) -> datetime | None:
            return datetime.fromisoformat(value) if value else None

        issued = _dt(wire["issuedAt"])
        expires = _dt(wire["expiresAt"])
        if issued is None or expires is None:
            raise GatewayTransportError("lease envelope missing issuedAt/expiresAt")
        return cls(
            workspace_id=str(wire["workspaceId"]),
            lease_id=str(wire["leaseId"]),
            run_id=str(wire["runId"]),
            step_id=str(wire["stepId"]),
            attempt=int(wire["attempt"]),
            slot=str(wire["slot"]),
            capability=str(wire["capability"]),
            connector_instance_id=str(wire["connectorInstanceId"]),
            token_type=str(wire["tokenType"]),
            issued_at=issued,
            expires_at=expires,
            released_at=_dt(wire.get("releasedAt")),
            revoked_at=_dt(wire.get("revokedAt")),
            revoke_reason=wire.get("revokeReason"),
        )


class GatewayLeaseError(Exception):
    """Domain rejection returned by Connector Service.

    Carries the :class:`~custos_connector.lease.errors.LeaseErrorCode`
    string verbatim (``CAPACITY_EXCEEDED`` / ``NOT_FOUND`` /
    ``ALREADY_RELEASED`` / ``INVALID_REQUEST``) so the router can map
    1:1 to a :class:`SidecarErrorCode` without re-introducing a
    side-table of strings.
    """

    def __init__(
        self,
        code: str,
        detail: str,
        *,
        http_status: int,
        retry_after_sec: int | None = None,
    ) -> None:
        super().__init__(f"{code}: {detail}")
        self.code = code
        self.detail = detail
        self.http_status = http_status
        self.retry_after_sec = retry_after_sec


class GatewayTransportError(Exception):
    """Connector Service is unreachable or returned an unparseable response.

    Covers connect/read timeouts, 5xx, and bodies that do not match the
    ``{lease: ...}`` / ``{error: ...}`` envelope contract. The router
    maps this to a 503 ``connector-unavailable`` problem document.
    """


class LeaseGateway:
    """Thin async HTTP client for the CS internal lease RPC.

    Holds an :class:`httpx.AsyncClient` for connection pooling. Tests
    inject a pre-built client (with a :class:`httpx.MockTransport`); in
    production :meth:`from_settings` builds the default client from
    :class:`LeaseGatewaySettings`.
    """

    def __init__(
        self,
        *,
        client: httpx.AsyncClient,
        call_context: str,
    ) -> None:
        self._client = client
        self._headers = {
            CALLCTX_HEADER: call_context,
            "content-type": "application/json",
        }

    @classmethod
    def from_settings(cls, settings: LeaseGatewaySettings) -> LeaseGateway:
        """Build a gateway with the production httpx client."""
        client = httpx.AsyncClient(
            base_url=settings.connector_service_url,
            timeout=settings.timeout_sec,
        )
        return cls(client=client, call_context=settings.call_context)

    async def aclose(self) -> None:
        """Release the underlying connection pool."""
        await self._client.aclose()

    # ------------------------------------------------------------------
    # Lease ops
    # ------------------------------------------------------------------

    async def issue(self, body: dict[str, Any]) -> LeaseRecord:
        """Mint a lease. See :class:`LeaseGateway` for the wire body."""
        return await self._post_lease("/internal/v1/leases:issue", body)

    async def refresh(self, body: dict[str, Any]) -> LeaseRecord:
        """Extend a lease's expiry without changing its id."""
        return await self._post_lease("/internal/v1/leases:refresh", body)

    async def release(self, lease_id: str) -> None:
        """Release a lease. Best-effort; raises only on transport failures."""
        try:
            response = await self._client.post(
                "/internal/v1/leases:release",
                json={"leaseId": lease_id},
                headers=self._headers,
            )
        except httpx.HTTPError as exc:
            raise GatewayTransportError(
                f"connector-service unreachable on release: {exc!s}"
            ) from exc
        if response.status_code == 204:
            return
        # Any 4xx on release is treated as success (idempotent best-effort);
        # 5xx surfaces as transport error so operators see it.
        if 500 <= response.status_code < 600:
            raise GatewayTransportError(
                f"connector-service returned {response.status_code} on release"
            )

    async def revoke_many(
        self,
        lease_ids: list[str],
        reason: str,
    ) -> list[dict[str, str]]:
        """Revoke a batch of leases via CS internal RPC (CONN-IMPL-020).

        Wire body: ``{leaseIds: [...], reason: "..."}``.

        Returns the per-lease ack list as decoded JSON
        ``[{leaseId, status}, ...]`` where ``status`` is one of
        ``revoked`` / ``already-revoked`` / ``already-expired`` /
        ``not-found``. The endpoint always responds 200; non-200 or a
        malformed body surfaces as :class:`GatewayTransportError`.

        Order is preserved 1:1 with the input list. Duplicate ids in
        the input produce one ack each (second one sees
        ``already-revoked``).
        """
        try:
            response = await self._client.post(
                "/internal/v1/leases:revoke",
                json={"leaseIds": list(lease_ids), "reason": reason},
                headers=self._headers,
            )
        except httpx.HTTPError as exc:
            raise GatewayTransportError(
                f"connector-service unreachable on revoke: {exc!s}"
            ) from exc
        if response.status_code != 200:
            raise GatewayTransportError(
                f"connector-service returned {response.status_code} on revoke"
            )
        try:
            payload = response.json()
            raw_results = payload["results"]
        except (ValueError, KeyError, TypeError) as exc:
            raise GatewayTransportError(
                f"connector-service returned malformed 200 body on revoke: {exc!s}"
            ) from exc
        if not isinstance(raw_results, list):
            raise GatewayTransportError(
                "connector-service revoke 'results' must be a list; got "
                f"{type(raw_results).__name__}"
            )
        decoded: list[dict[str, str]] = []
        for entry in raw_results:
            if not isinstance(entry, dict):
                raise GatewayTransportError(
                    "connector-service revoke 'results' entry is not an object"
                )
            try:
                decoded.append(
                    {
                        "leaseId": str(entry["leaseId"]),
                        "status": str(entry["status"]),
                    }
                )
            except KeyError as exc:
                raise GatewayTransportError(
                    f"connector-service revoke 'results' entry missing key: {exc!s}"
                ) from exc
        return decoded

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _post_lease(self, path: str, body: dict[str, Any]) -> LeaseRecord:
        try:
            response = await self._client.post(
                path,
                json=body,
                headers=self._headers,
            )
        except httpx.HTTPError as exc:
            raise GatewayTransportError(
                f"connector-service unreachable on {path}: {exc!s}"
            ) from exc
        if response.status_code == 200:
            try:
                payload = response.json()
                lease = payload["lease"]
            except (ValueError, KeyError, TypeError) as exc:
                raise GatewayTransportError(
                    f"connector-service returned malformed 200 body on {path}: {exc!s}"
                ) from exc
            return LeaseRecord.from_wire(lease)
        # 4xx domain rejection envelope: { "error": { "code", "detail" } }
        if 400 <= response.status_code < 500:
            try:
                payload = response.json()
                err = payload["error"]
                code = str(err["code"])
                detail = str(err.get("detail", ""))
            except (ValueError, KeyError, TypeError) as exc:
                raise GatewayTransportError(
                    f"connector-service returned malformed "
                    f"{response.status_code} body on {path}: {exc!s}"
                ) from exc
            retry_after = _parse_retry_after(response.headers.get("retry-after"))
            raise GatewayLeaseError(
                code,
                detail,
                http_status=response.status_code,
                retry_after_sec=retry_after,
            )
        # 5xx: transport-level failure.
        raise GatewayTransportError(f"connector-service returned {response.status_code} on {path}")


def _parse_retry_after(raw: str | None) -> int | None:
    """Parse a ``Retry-After`` header value as integer seconds.

    The CS router sends a delta-seconds value (per RFC 7231); the
    HTTP-date form is not used. Non-integer values are silently
    dropped so a misconfigured CS does not crash the sidecar.
    """
    if raw is None:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


__all__ = [
    "CALLCTX_HEADER",
    "GatewayLeaseError",
    "GatewayTransportError",
    "LeaseGateway",
    "LeaseGatewaySettings",
    "LeaseRecord",
]
