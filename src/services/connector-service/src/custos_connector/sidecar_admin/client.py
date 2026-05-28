"""HTTPS client for the sidecar control-channel revoke endpoint (CONN-IMPL-028).

Issues ``POST {endpoint}/sidecar-admin/v1/revoke`` with body
``{"leaseIds": [...], "reason": "..."}`` and parses the per-lease
ack response into :class:`SidecarRevokeAck` rows.

The client is best-effort: callers signal the sidecar to short-
circuit in-flight activity reads, while the SPL lease store
remains the authoritative terminal-revoke record. Transport
errors and the documented ``503 sidecar shutting down`` are
swallowed and logged at warning level — the operator route does
not surface them.

mTLS is required by the design (the sidecar's control listener
authenticates each caller's workload cert), but the asyncpg /
operator unit-test surface uses plain HTTP against an ASGI mock.
The constructor therefore accepts a pre-built
:class:`httpx.AsyncClient` so tests can plumb a custom transport
(``httpx.MockTransport``) without spinning up real TLS material.
Production wiring in :func:`custos_connector.providers.load_providers`
will build the client with mTLS once CONN-IMPL-029 lands the cert
plumbing alongside ARM.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Final

import httpx

#: Path the sidecar control-channel exposes (CONN-IMPL-020).
SIDECAR_REVOKE_PATH: Final[str] = "/sidecar-admin/v1/revoke"

#: Default timeout for a single ``revoke`` round-trip. The sidecar's
#: handler itself calls back to Connector Service's internal RPC, so
#: a generous-but-bounded value keeps a slow sidecar from holding
#: the operator request open indefinitely.
DEFAULT_REVOKE_TIMEOUT_SEC: Final[float] = 5.0

#: HTTP status the sidecar returns when it is shutting down. Per
#: design § Sidecar revoke control-channel API, the operator treats
#: this as a successful terminal-revoke because the activity is
#: exiting anyway.
_SIDECAR_SHUTTING_DOWN: Final[int] = 503

_logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class SidecarRevokeAck:
    """One row of the sidecar's per-lease ack list.

    ``status`` matches the four wire values the sidecar control
    router emits: ``revoked``, ``already-revoked``,
    ``already-expired``, ``not-found``. Callers that only need to
    know "did the sidecar enforce locally?" can check whether
    ``status`` is one of the terminal values.
    """

    lease_id: str
    status: str


class SidecarAdminClient:
    """Issues ``/sidecar-admin/v1/revoke`` against a sidecar control listener.

    The client owns an :class:`httpx.AsyncClient` so a single TCP /
    TLS connection pool is shared across operator calls. Construct
    once at lifespan start and dispose with :meth:`aclose` on
    shutdown.

    The constructor accepts a pre-built :class:`httpx.AsyncClient`
    so the test surface can inject :class:`httpx.MockTransport`. The
    production factory in :func:`custos_connector.providers` will
    build the client with the mTLS cert + key pair declared by the
    deployment's :class:`Settings`.
    """

    def __init__(
        self,
        *,
        http_client: httpx.AsyncClient,
        timeout_sec: float = DEFAULT_REVOKE_TIMEOUT_SEC,
    ) -> None:
        self._http_client = http_client
        self._timeout_sec = timeout_sec

    async def aclose(self) -> None:
        """Close the underlying HTTP client. Idempotent."""
        await self._http_client.aclose()

    async def revoke(
        self,
        *,
        endpoint: str,
        lease_ids: list[str],
        reason: str,
    ) -> list[SidecarRevokeAck]:
        """Signal a sidecar to mark ``lease_ids`` revoked.

        Returns the sidecar's per-lease ack list on success. On any
        transport-level failure, on a non-2xx HTTP status, or on a
        malformed response body, returns an empty list — the
        operator route logs the failure and continues; the SPL
        lease store has already recorded the terminal revoke so the
        operator's response remains correct.

        The 503 ``sidecar shutting down`` response is special-cased
        per the design: each requested lease is reported as
        ``revoked`` so the operator audit count matches the number
        of leases that were actually terminated. The sidecar would
        emit the same terminal state on its way out regardless.
        """
        if not lease_ids:
            return []
        url = endpoint.rstrip("/") + SIDECAR_REVOKE_PATH
        body = {"leaseIds": lease_ids, "reason": reason}
        try:
            response = await self._http_client.post(
                url,
                json=body,
                timeout=self._timeout_sec,
            )
        except httpx.HTTPError as exc:
            _logger.warning(
                "sidecar-admin revoke transport failure",
                extra={"endpoint": endpoint, "lease_count": len(lease_ids), "error": str(exc)},
            )
            return []
        if response.status_code == _SIDECAR_SHUTTING_DOWN:
            _logger.info(
                "sidecar-admin revoke saw shutting-down 503; treating as terminal-revoke",
                extra={"endpoint": endpoint, "lease_count": len(lease_ids)},
            )
            return [SidecarRevokeAck(lease_id=lid, status="revoked") for lid in lease_ids]
        if response.status_code != 200:
            _logger.warning(
                "sidecar-admin revoke returned non-200 status",
                extra={"endpoint": endpoint, "status_code": response.status_code},
            )
            return []
        try:
            payload = response.json()
            raw_results = payload["results"]
        except (KeyError, TypeError, ValueError) as exc:
            _logger.warning(
                "sidecar-admin revoke response was malformed",
                extra={"endpoint": endpoint, "error": str(exc)},
            )
            return []
        acks: list[SidecarRevokeAck] = []
        for row in raw_results:
            try:
                lease_id = row["leaseId"]
                status = row["status"]
            except (KeyError, TypeError):
                _logger.warning(
                    "sidecar-admin revoke ack row missing leaseId/status",
                    extra={"endpoint": endpoint, "row": row},
                )
                continue
            acks.append(SidecarRevokeAck(lease_id=str(lease_id), status=str(status)))
        return acks


__all__ = [
    "DEFAULT_REVOKE_TIMEOUT_SEC",
    "SIDECAR_REVOKE_PATH",
    "SidecarAdminClient",
    "SidecarRevokeAck",
]
