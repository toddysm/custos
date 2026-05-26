"""Stub credential minter (CONN-IMPL-019).

Real KMS/identity-provider integration ships in a follow-up ticket;
for #019 the sidecar needs a contract-shaped minter so the UDS surface
is fully testable end-to-end. The stub minter:

* Returns a deterministic synthetic token derived from the lease id +
  connector instance id (so tests can assert the token actually flows
  through without leaking any real credential).
* Lets tests inject failure-mode behaviours
  (``UpstreamFailure`` / ``InstanceUnavailable``) so the router's
  502 / 503 paths are exercised against the production-shape API.

The :class:`CredentialMinter` interface is the seam CONN-IMPL-020+
will swap a real implementation against; the protocol carries only
the fields the design's response envelope needs (``token`` +
``expires_at``).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from custos_sidecar.context_registry import SlotContext


@dataclass(frozen=True, slots=True)
class MintedCredential:
    """A credential bound to a lease.

    Returned by every :meth:`CredentialMinter.mint` call. The minter
    chooses the token format (Bearer string, AWS SigV4 envelope, etc.)
    so the sidecar is agnostic.

    Attributes:
        token: The opaque credential string the activity will pass to
            the upstream service. Treat as a secret.
        expires_at: Wall-clock expiry of ``token``. The sidecar
            forwards this to the activity so it can schedule its
            refresh well before the upstream rejects the credential.
    """

    token: str
    expires_at: datetime


class UpstreamMintFailure(Exception):
    """The upstream KMS / IdP rejected the mint call.

    The router maps this to :class:`SidecarErrorCode.UPSTREAM_FAILED`
    (HTTP 502) per the design failure-mode table.
    """


class InstanceUnavailable(Exception):
    """The connector instance is disabled or unhealthy upstream.

    The router maps this to
    :class:`SidecarErrorCode.CONNECTOR_UNAVAILABLE` (HTTP 503).
    """


class CredentialMinter(Protocol):
    """Mints upstream credentials for a lease.

    The sidecar calls :meth:`mint` after the Lease Manager has
    successfully recorded the lease, so the lease id and the slot
    metadata are both available. Implementations should keep ``mint``
    fast enough for the activity's UDS request budget; long-running
    KMS calls must time out and raise :class:`UpstreamMintFailure`
    rather than blocking the request indefinitely.
    """

    async def mint(
        self,
        *,
        lease_id: str,
        slot_ctx: SlotContext,
        ttl_hint: datetime,
    ) -> MintedCredential:
        """Produce a credential for ``lease_id`` against ``slot_ctx``.

        ``ttl_hint`` is the lease's ``expires_at`` from the Lease
        Manager; the minter may shorten the credential lifetime but
        should not extend past it. Returning a credential that
        expires *after* the lease would let the activity hold a
        usable token after Connector Service believes the lease has
        ended; the sidecar relies on the minter honouring this bound.
        """
        ...


@dataclass(frozen=True, slots=True)
class _StubBehaviour:
    """Behaviour switch the stub minter honours for tests.

    Default behaviour returns a synthetic token. Tests flip the flags
    to drive the 502 / 503 failure paths through the real router code
    without needing a live KMS.
    """

    raise_upstream: bool = False
    raise_unavailable: bool = False


class StubCredentialMinter:
    """A deterministic in-process :class:`CredentialMinter`.

    Production sidecars will swap this for a real implementation in a
    later ticket. The stub:

    * Emits ``stub-token::<connector_instance_id>::<lease_id>`` so
      tests can assert the lease wiring without leaking any real
      credential.
    * Uses the lease's ``expires_at`` (passed as ``ttl_hint``) as the
      credential expiry, matching the production contract that the
      minter must not extend past the lease.
    * Honours an injected behaviour flag so the router's 502 / 503
      failure paths are reachable end-to-end.
    """

    def __init__(
        self,
        *,
        behaviour: _StubBehaviour | None = None,
    ) -> None:
        self._behaviour = behaviour or _StubBehaviour()

    async def mint(
        self,
        *,
        lease_id: str,
        slot_ctx: SlotContext,
        ttl_hint: datetime,
    ) -> MintedCredential:
        if self._behaviour.raise_unavailable:
            raise InstanceUnavailable(
                f"connector instance {slot_ctx.connector_instance_id} is unavailable (stub)"
            )
        if self._behaviour.raise_upstream:
            raise UpstreamMintFailure(
                f"upstream mint rejected for {slot_ctx.connector_instance_id} (stub)"
            )
        return MintedCredential(
            token=f"stub-token::{slot_ctx.connector_instance_id}::{lease_id}",
            expires_at=ttl_hint,
        )


# Convenience factories used by tests so they do not have to import the
# private ``_StubBehaviour`` dataclass.
def stub_minter_returning_upstream_failure() -> CredentialMinter:
    """Return a stub minter that always raises :class:`UpstreamMintFailure`."""
    return StubCredentialMinter(behaviour=_StubBehaviour(raise_upstream=True))


def stub_minter_returning_unavailable() -> CredentialMinter:
    """Return a stub minter that always raises :class:`InstanceUnavailable`."""
    return StubCredentialMinter(behaviour=_StubBehaviour(raise_unavailable=True))


# Callable factory shape exported so the router can accept a single
# function and unit tests can supply lambdas without instantiating
# protocol-checking gymnastics.
MinterFactory = Callable[[], Awaitable[CredentialMinter]]


__all__ = [
    "CredentialMinter",
    "InstanceUnavailable",
    "MintedCredential",
    "MinterFactory",
    "StubCredentialMinter",
    "UpstreamMintFailure",
    "stub_minter_returning_unavailable",
    "stub_minter_returning_upstream_failure",
]
