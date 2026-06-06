"""Idempotency Coordinator for the Custos API Gateway write path (AGW-IMPL-009).

Every *write* endpoint (`POST`/`PUT`/`PATCH`/`DELETE`) is deduplicated so a
client (or a gateway-internal retry) can safely resend a request without
double-applying it. The client SHOULD supply ``Idempotency-Key: <opaque>``
(IETF draft "The Idempotency-Key HTTP Header Field"); when absent the gateway
generates one so its own retries are safe.

The dedup key is ``(workspaceId, principalId, route, idempotencyKey)`` and the
request fingerprint is ``SHA-256(method || route || workspaceId ||
sorted-headers-subset || body)``. Reservation is an *atomic* reserve-or-read on
the SPL :class:`~custos_spl.MetadataStoreProvider`, which returns one of four
outcomes (see ``design/components/api-gateway/design.md`` § Idempotency
Coordinator); :meth:`IdempotencyCoordinator.reserve` maps them onto a small
gateway-level decision:

* ``IdemReserved`` → :class:`ProceedReservation` (perform the work, then
  :meth:`IdempotencyCoordinator.complete` records the response snapshot);
* ``ExistingCompleted`` → :class:`ReplayReservation` (replay the stored
  snapshot, never re-running the work);
* ``ExistingInFlight`` → ``409 idempotency-in-flight`` + ``Retry-After``;
* ``KeyReuse`` → ``409 idempotency-key-reuse``.

Reads (`GET`/`HEAD`/`OPTIONS`) skip the coordinator entirely
(:func:`is_idempotent_method`). The coordinator owns no orchestration — the
request router (AGW-IMPL-016) calls :func:`compute_request_hash` +
:meth:`reserve` before forwarding and :meth:`complete` afterwards.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final, Protocol

from custos_spl import (
    ExistingCompleted,
    ExistingInFlight,
    IdemReserved,
    KeyReuse,
    PrincipalId,
    WorkspaceId,
)

from custos_gateway.errors import GatewayError, GatewayErrorCode
from custos_gateway.middleware.correlation import new_correlation_id

if TYPE_CHECKING:
    from collections.abc import Mapping

    from custos_spl import IdempotencyRecord, ReserveIdempotencyResult

__all__ = [
    "DEFAULT_RETRY_AFTER_SECONDS",
    "HASHED_REQUEST_HEADERS",
    "IDEMPOTENCY_KEY_HEADER",
    "RETRY_AFTER_HEADER",
    "WRITE_METHODS",
    "IdempotencyCoordinator",
    "IdempotencyKey",
    "IdempotencyStore",
    "ProceedReservation",
    "ReplayReservation",
    "ReservationOutcome",
    "compute_request_hash",
    "is_idempotent_method",
    "resolve_idempotency_key",
]

#: Request header carrying the client-supplied idempotency key (IETF draft
#: "The Idempotency-Key HTTP Header Field").
IDEMPOTENCY_KEY_HEADER: Final[str] = "idempotency-key"

#: ``Retry-After`` response header set on a ``409 idempotency-in-flight``.
RETRY_AFTER_HEADER: Final[str] = "retry-after"

#: Methods that mutate state and are therefore deduplicated. Reads skip the
#: coordinator (RFC 9110 § 9.2.2 safe methods are inherently idempotent).
WRITE_METHODS: Final[frozenset[str]] = frozenset({"POST", "PUT", "PATCH", "DELETE"})

#: The (lowercased) request-header subset folded into the request fingerprint.
#: The body's media type changes how the downstream interprets identical bytes,
#: so a ``content-type`` change is a *different* request even under the same key.
HASHED_REQUEST_HEADERS: Final[frozenset[str]] = frozenset({"content-type"})

#: ``Retry-After`` seconds advertised when an identical request is in flight.
DEFAULT_RETRY_AFTER_SECONDS: Final[int] = 1

#: Field separator folded into the request fingerprint. A NUL byte can never
#: appear in a header value or route (Starlette rejects control chars at the
#: parser), so it unambiguously delimits the hashed components.
_HASH_SEPARATOR: Final[bytes] = b"\x00"


def is_idempotent_method(method: str) -> bool:
    """Return whether ``method`` is a write method the coordinator dedups."""
    return method.upper() in WRITE_METHODS


def resolve_idempotency_key(header_value: str | None) -> str:
    """Return the effective idempotency key for a request.

    A non-blank ``Idempotency-Key`` header is honoured *verbatim*; an absent or
    whitespace-only header yields a freshly generated key so a retry the gateway
    itself performs is still deduplicated.
    """
    if header_value is not None and header_value.strip():
        return header_value
    return new_correlation_id()


def compute_request_hash(
    *,
    method: str,
    route: str,
    workspace_id: str,
    headers: Mapping[str, str],
    body: bytes,
) -> str:
    """Return the ``SHA-256`` request fingerprint for idempotency comparison.

    The digest covers the method, the route template, the workspace, the
    :data:`HASHED_REQUEST_HEADERS` subset (sorted for determinism), and the raw
    body. Header names are matched case-insensitively so a plain ``dict`` of
    HTTP headers (e.g. ``{"Content-Type": ...}``) hashes identically to a
    lowercased mapping. Two requests under the same key with differing
    fingerprints are a ``KeyReuse`` violation; identical fingerprints are safe
    replays.
    """
    folded = {name.lower(): value for name, value in headers.items()}
    hasher = hashlib.sha256()
    hasher.update(method.upper().encode("utf-8"))
    hasher.update(_HASH_SEPARATOR)
    hasher.update(route.encode("utf-8"))
    hasher.update(_HASH_SEPARATOR)
    hasher.update(workspace_id.encode("utf-8"))
    hasher.update(_HASH_SEPARATOR)
    for name in sorted(HASHED_REQUEST_HEADERS):
        value = folded.get(name)
        if value is not None:
            hasher.update(name.encode("utf-8"))
            hasher.update(b":")
            hasher.update(value.encode("utf-8"))
        hasher.update(_HASH_SEPARATOR)
    hasher.update(body)
    return hasher.hexdigest()


@dataclass(frozen=True, slots=True)
class IdempotencyKey:
    """The composite dedup key for a write request."""

    workspace_id: str
    principal_id: str
    route: str
    idempotency_key: str


@dataclass(frozen=True, slots=True)
class ProceedReservation:
    """A fresh reservation: perform the work, then :meth:`complete` it."""

    key: IdempotencyKey
    request_hash: str


@dataclass(frozen=True, slots=True)
class ReplayReservation:
    """An identical request already completed: replay this snapshot."""

    response_snapshot: Mapping[str, Any]


#: The router-facing decision returned by a successful reservation.
ReservationOutcome = ProceedReservation | ReplayReservation


class IdempotencyStore(Protocol):
    """The narrow SPL metadata-store surface the coordinator depends on.

    The full :class:`custos_spl.MetadataStoreProvider` structurally satisfies
    this protocol; depending on only the two idempotency methods keeps the
    coordinator decoupled from the rest of the store and trivially testable.
    """

    async def reserve_idempotency_record(
        self,
        workspace_id: WorkspaceId,
        principal_id: PrincipalId,
        route: str,
        idempotency_key: str,
        request_hash: str,
        ttl_seconds: int,
    ) -> ReserveIdempotencyResult: ...

    async def complete_idempotency_record(
        self,
        workspace_id: WorkspaceId,
        principal_id: PrincipalId,
        route: str,
        idempotency_key: str,
        response_snapshot: Mapping[str, Any],
    ) -> IdempotencyRecord: ...


@dataclass(frozen=True, slots=True)
class IdempotencyCoordinator:
    """Reserves and completes idempotency records on the SPL metadata store.

    The ``store`` and ``ttl_seconds`` are owned by the app lifespan; the
    coordinator is a thin, stateless translator between the SPL reserve outcomes
    and the gateway's locked error taxonomy.
    """

    store: IdempotencyStore
    ttl_seconds: int
    retry_after_seconds: int = DEFAULT_RETRY_AFTER_SECONDS

    async def reserve(self, key: IdempotencyKey, request_hash: str) -> ReservationOutcome:
        """Atomically reserve-or-read ``key`` against ``request_hash``.

        Returns a :class:`ProceedReservation` when the row is newly reserved or a
        :class:`ReplayReservation` when an identical request already completed.

        Raises:
            GatewayError: ``idempotency-in-flight`` (409 + ``Retry-After``) when
                an identical request is still in progress; ``idempotency-key-reuse``
                (409) when the key is reused with a different request hash.
        """
        result = await self.store.reserve_idempotency_record(
            WorkspaceId(key.workspace_id),
            PrincipalId(key.principal_id),
            key.route,
            key.idempotency_key,
            request_hash,
            self.ttl_seconds,
        )
        if isinstance(result, IdemReserved):
            return ProceedReservation(key=key, request_hash=request_hash)
        if isinstance(result, ExistingCompleted):
            return ReplayReservation(response_snapshot=result.response_snapshot)
        if isinstance(result, ExistingInFlight):
            raise GatewayError(
                GatewayErrorCode.IDEMPOTENCY_IN_FLIGHT,
                detail="An identical request is already in progress; retry shortly.",
                headers={RETRY_AFTER_HEADER: str(self.retry_after_seconds)},
            )
        if isinstance(result, KeyReuse):
            raise GatewayError(
                GatewayErrorCode.IDEMPOTENCY_KEY_REUSE,
                detail="The idempotency key was reused with a different request.",
            )
        msg = f"unexpected reserve outcome: {type(result).__name__}"  # pragma: no cover
        raise TypeError(msg)  # pragma: no cover

    async def complete(self, key: IdempotencyKey, response_snapshot: Mapping[str, Any]) -> None:
        """Record ``response_snapshot`` and flip the reservation to completed."""
        await self.store.complete_idempotency_record(
            WorkspaceId(key.workspace_id),
            PrincipalId(key.principal_id),
            key.route,
            key.idempotency_key,
            response_snapshot,
        )
