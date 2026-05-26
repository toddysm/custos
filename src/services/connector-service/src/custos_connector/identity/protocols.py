"""The :class:`IdentityResolver` Protocol — the resolver contract.

Each per-auth-type resolver implements this Protocol structurally.
Resolvers carry two :class:`ClassVar` declarations so the
:class:`IdentityResolverRegistry` can index them and verify their
declared category against the one the Loader derived at registration
time:

* ``authentication_type`` — the manifest token (e.g. ``"oidc"``).
* ``category`` — the :class:`IdentityCategory` (kms / workload /
  federated).

The ``resolve`` method is async because every built-in resolver here
talks to an upstream identity provider; it returns a fully populated
:class:`ResolvedIdentity`, or raises :class:`IdentityResolverError`.

The :class:`IdentityResolverContext` payload (passed *into*
``resolve``) carries everything the resolver needs that is *not* part
of the per-instance credentials payload: the workspace and instance
identifiers (so the resolver can rate-limit or scope upstream calls),
the lease-TTL hint (so the resolver can ask the upstream for matching
expiry semantics), and the clock seam (so unit tests can pin time).
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, ClassVar, Protocol, runtime_checkable

from custos_connector.identity.models import ResolvedIdentity
from custos_connector.loader.identity import IdentityCategory


@dataclass(frozen=True, slots=True)
class IdentityResolverContext:
    """Per-call context the registry passes into each resolver.

    Attributes:
        workspace_id: Workspace owning the connector instance whose
            credentials are being resolved. Used by resolvers that
            scope upstream calls by tenant (e.g. an Azure tenant ID
            override) and by the registry's cache key.
        instance_id: The ConnectorInstance UUID. Used by resolvers that
            mint per-instance subjects (e.g. OIDC ``sub`` claims) and
            by the registry's cache key.
        lease_ttl_seconds: The lease TTL the caller is about to request
            from the Lease Manager (CONN-IMPL-017). Resolvers use this
            to ask the upstream for a matching expiry — e.g. the OIDC
            token exchange asks for an access-token TTL no longer than
            this value.
        now: Clock seam. The registry passes ``datetime.now(UTC)`` by
            default; unit tests pin this to a fixed value so the cache
            and the audit ``occurred_at`` are deterministic.
    """

    workspace_id: str
    instance_id: str
    lease_ttl_seconds: int
    now: Callable[[], datetime]


@runtime_checkable
class IdentityResolver(Protocol):
    """Structural contract every per-auth-type resolver implements.

    Implementations declare their ``authentication_type`` /
    ``category`` as class-level :class:`ClassVar` so the registry can
    introspect them without instantiating. Implementations should be
    stateless w.r.t. the per-call payload — the registry creates one
    resolver instance per process and reuses it across calls — but
    *are* allowed to hold long-lived state such as a JWKS cache or an
    HTTP-client handle.
    """

    authentication_type: ClassVar[str]
    category: ClassVar[IdentityCategory]

    async def resolve(
        self,
        *,
        credentials_authentication: Mapping[str, Any],
        context: IdentityResolverContext,
    ) -> ResolvedIdentity:
        """Mint a :class:`ResolvedIdentity` for the supplied credentials.

        Args:
            credentials_authentication: The manifest-validated
                ``credentials.authentication`` payload from the
                ConnectorInstance. Resolvers consume this directly — the
                registry does not pre-validate per-resolver fields.
            context: The :class:`IdentityResolverContext` for the call.

        Raises:
            IdentityResolverError: For any failure — missing fields,
                upstream errors, malformed responses. The registry
                catches this exception and emits the
                ``connector.identity.failed`` audit event.
        """
        ...


__all__ = [
    "IdentityResolver",
    "IdentityResolverContext",
]
