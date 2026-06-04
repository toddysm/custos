"""Connector Service ``RefreshLease`` adapter (design § Internal RPC outbound).

For a long-running step ARM proactively extends the connector lease so the
upstream credential outlives the step's working window, calling the Connector
Service's ``POST /internal/v1/leases:refresh`` endpoint over Dapr
Service-Invocation. The lease id is stable across refreshes; only the expiry
moves.

.. note::

   The connector sidecar also refreshes leases on the activity-facing UDS
   (see ``connector-service`` § Secret and Token Flow to Activities). This
   adapter is ARM's *proactive* refresh path for the lease ids ARM holds on
   long-running steps, per the ARM design's outbound-RPC contract; it shares
   the same wire shape as the sidecar's refresh call.

The concrete :class:`DaprConnectorLeaseClient` speaks through an injected,
lifespan-owned :class:`httpx.AsyncClient` (mirroring the Activity Resolver
precedent — the client is *not* owned here). The base URL is the
``ARM_CONNECTOR_ENDPOINT`` value, which in production points at the local Dapr
sidecar's invoke path for the ``connector`` app; the Dapr sidecar propagates
the call-context (workspace identity) the Connector Service reads from the
request headers.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, runtime_checkable

import httpx

from custos_arm.contract import ErrorClass

#: Default per-request timeout (seconds) against the Connector Service. Matches
#: the ARM Catalog-resolution and Workflow Service outbound-RPC envelopes.
DEFAULT_LEASE_TIMEOUT_SECONDS: float = 10.0

#: Path (appended to ``ARM_CONNECTOR_ENDPOINT``) of the internal lease-refresh
#: RPC. The ``:refresh`` action suffix matches the Connector Service router.
REFRESH_LEASE_PATH: str = "/internal/v1/leases:refresh"


class ConnectorLeaseError(Exception):
    """Base class for every ``RefreshLease`` adapter failure."""


class ConnectorUnavailableError(ConnectorLeaseError):
    """The Connector Service was unreachable or returned an unexpected status.

    Transient: a network error, a timeout, a malformed body, or any ``5xx``
    response. The refresh may be retried — this is distinct from
    :class:`LeaseRefreshRejectedError`, which is permanent.
    """

    def __init__(self, lease_id: str, message: str) -> None:
        self.lease_id = lease_id
        super().__init__(message)


class LeaseRefreshRejectedError(ConnectorLeaseError):
    """The Connector Service refused to refresh the lease.

    Permanent: the lease is unknown, already released, or revoked (HTTP
    ``404`` / ``409`` / ``410``), or the request was rejected (``4xx``).
    Retrying with the same lease id can never succeed; the activity will see a
    ``410 Gone`` from the sidecar and is expected to exit promptly.
    """

    #: Canonical error code for the synthesized envelope (reserved namespace).
    code: str = "system.lease_refresh_rejected"

    #: Orchestrator-facing failure class — never retried.
    error_class: ErrorClass = ErrorClass.PERMANENT

    def __init__(self, lease_id: str, message: str) -> None:
        self.lease_id = lease_id
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class Lease:
    """The subset of the Connector Service lease envelope ARM tracks.

    :param lease_id: Stable id, unchanged by a refresh.
    :param expires_at: New expiry after the refresh (timezone-aware UTC).
    :param slot: Connector slot the lease was issued for.
    :param connector_instance_id: Bound instance behind the lease.
    """

    lease_id: str
    expires_at: datetime
    slot: str
    connector_instance_id: str


@runtime_checkable
class ConnectorLeaseClient(Protocol):
    """Extends an existing connector lease for a long-running step."""

    async def refresh_lease(
        self,
        *,
        lease_id: str,
        requested_ttl_sec: int | None = None,
        step_deadline: datetime | None = None,
    ) -> Lease:
        """Refresh ``lease_id`` and return the updated :class:`Lease`.

        :raises LeaseRefreshRejectedError: the lease is gone or the request was
            rejected (permanent).
        :raises ConnectorUnavailableError: the Connector Service was
            unreachable or returned an unexpected status (transient).
        """
        ...


class DaprConnectorLeaseClient:
    """``RefreshLease`` over Dapr Service-Invocation to the Connector Service.

    :param http_client: A lifespan-owned async client; not closed here.
    :param connector_endpoint: The Connector base URL
        (``ARM_CONNECTOR_ENDPOINT``).
    :param timeout: Per-request timeout in seconds.
    """

    def __init__(
        self,
        http_client: httpx.AsyncClient,
        *,
        connector_endpoint: str,
        timeout: float = DEFAULT_LEASE_TIMEOUT_SECONDS,
    ) -> None:
        self._http = http_client
        self._base = connector_endpoint.rstrip("/")
        self._timeout = timeout

    async def refresh_lease(
        self,
        *,
        lease_id: str,
        requested_ttl_sec: int | None = None,
        step_deadline: datetime | None = None,
    ) -> Lease:
        if not lease_id:
            raise ValueError("lease_id must be a non-empty string")

        body: dict[str, Any] = {"leaseId": lease_id}
        if requested_ttl_sec is not None:
            body["requestedTtlSec"] = requested_ttl_sec
        if step_deadline is not None:
            body["stepDeadline"] = step_deadline.isoformat()

        url = f"{self._base}{REFRESH_LEASE_PATH}"
        try:
            response = await self._http.post(url, json=body, timeout=self._timeout)
        except httpx.HTTPError as exc:
            raise ConnectorUnavailableError(
                lease_id, f"lease refresh for {lease_id} failed: {exc}"
            ) from exc

        if response.status_code == httpx.codes.TOO_MANY_REQUESTS:
            raise ConnectorUnavailableError(
                lease_id,
                f"connector is throttling lease refresh for {lease_id} (429); retry with backoff",
            )
        if 400 <= response.status_code < 500:
            raise LeaseRefreshRejectedError(
                lease_id,
                f"connector refused to refresh lease {lease_id}: status {response.status_code}",
            )
        if response.status_code != httpx.codes.OK:
            raise ConnectorUnavailableError(
                lease_id,
                f"connector returned unexpected status {response.status_code} "
                f"refreshing lease {lease_id}",
            )

        try:
            payload = response.json()
        except ValueError as exc:
            raise ConnectorUnavailableError(
                lease_id, f"lease refresh response for {lease_id} is not valid JSON: {exc}"
            ) from exc
        return self._parse(lease_id=lease_id, payload=payload)

    def _parse(self, *, lease_id: str, payload: object) -> Lease:
        if not isinstance(payload, dict):
            raise ConnectorUnavailableError(
                lease_id, f"lease refresh response for {lease_id} is not a JSON object"
            )
        lease = payload.get("lease")
        if not isinstance(lease, dict):
            raise ConnectorUnavailableError(
                lease_id, f"lease refresh response for {lease_id} is missing the 'lease' envelope"
            )
        try:
            returned_id = lease["leaseId"]
            expires_at_raw = lease["expiresAt"]
            slot = lease["slot"]
            connector_instance_id = lease["connectorInstanceId"]
        except KeyError as exc:
            raise ConnectorUnavailableError(
                lease_id, f"lease refresh response for {lease_id} is missing required field {exc}"
            ) from exc

        if not isinstance(expires_at_raw, str):
            raise ConnectorUnavailableError(
                lease_id, f"lease refresh response for {lease_id} has a non-string 'expiresAt'"
            )
        try:
            # Normalise an RFC3339 trailing 'Z' (UTC) which datetime.fromisoformat
            # does not accept on all supported runtimes.
            expires_at = datetime.fromisoformat(expires_at_raw.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ConnectorUnavailableError(
                lease_id,
                f"lease refresh response for {lease_id} has an unparseable 'expiresAt': {exc}",
            ) from exc
        if expires_at.tzinfo is None:
            raise ConnectorUnavailableError(
                lease_id,
                f"lease refresh response for {lease_id} has a naive (non-UTC) 'expiresAt'",
            )

        returned_id_str = str(returned_id)
        if returned_id_str != lease_id:
            raise ConnectorUnavailableError(
                lease_id,
                f"lease refresh response returned a different lease id {returned_id_str!r} "
                f"than requested {lease_id!r}",
            )

        return Lease(
            lease_id=returned_id_str,
            expires_at=expires_at,
            slot=str(slot),
            connector_instance_id=str(connector_instance_id),
        )


__all__ = [
    "DEFAULT_LEASE_TIMEOUT_SECONDS",
    "REFRESH_LEASE_PATH",
    "ConnectorLeaseClient",
    "ConnectorLeaseError",
    "ConnectorUnavailableError",
    "DaprConnectorLeaseClient",
    "Lease",
    "LeaseRefreshRejectedError",
]
