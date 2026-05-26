"""The :class:`ResolvedIdentity` value object returned by every resolver.

The fields here are deliberately small. Connector Service does *not*
introspect ``material`` — the mapping is opaque to the platform and
flows straight into :meth:`PluginInvoker.bind` at the CONN-IMPL-016
call site (Phase G). The remaining fields exist so the
:class:`IdentityResolverRegistry` cache can be keyed and aged, and so
the rate-limited ``connector.identity.resolved`` audit payload can
carry redacted diagnostics (``descriptor``, ``expires_at``) without
ever leaking the secret material itself.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import Any

from custos_connector.loader.identity import IdentityCategory


@dataclass(frozen=True, slots=True)
class ResolvedIdentity:
    """Opaque-to-platform credential material plus housekeeping metadata.

    Attributes:
        authentication_type: Echoes the manifest's
            ``credentials.authenticationType``. Built-in tokens
            (e.g. ``"azure-key-vault"``) or vendor ``x-<vendor>``
            tokens.
        category: The :class:`IdentityCategory` the Loader derived
            for ``authentication_type`` at registration time. The
            registry verifies that the registered resolver agrees with
            this category before invoking it.
        material: The opaque credential payload that flows into the
            plugin via :meth:`PluginInvoker.bind`. Connector Service
            never logs the values in this mapping; only the keys are
            surfaced (in the audit payload) for diagnostics.
        descriptor: A stable, *non-secret* identifier for the credential
            source. For KMS-backed resolvers this is typically the
            ``<vault-or-store-uri>/<secret-name>`` pair; for federated
            resolvers it is the ``issuer + audience`` pair. The
            registry uses this in the cache key so two instances that
            point at the same secret share a cache entry.
        issued_at: When this material was minted by the resolver (UTC).
            Used by the cache and the audit payload.
        expires_at: When this material should be discarded. ``None``
            means "honour the per-instance lease TTL exclusively" — the
            cache will still age it out via ``lease_ttl_seconds``.
        plugin_envelope_keys: The keys present in ``material``. Carried
            separately so the registry can surface them in the
            ``connector.identity.resolved`` audit payload without
            iterating over the (sensitive) mapping itself.
    """

    authentication_type: str
    category: IdentityCategory
    material: Mapping[str, Any]
    descriptor: str
    issued_at: datetime
    expires_at: datetime | None
    plugin_envelope_keys: tuple[str, ...]

    @classmethod
    def build(
        cls,
        *,
        authentication_type: str,
        category: IdentityCategory,
        material: Mapping[str, Any],
        descriptor: str,
        issued_at: datetime,
        expires_at: datetime | None,
    ) -> ResolvedIdentity:
        """Convenience constructor that snapshots ``material``.

        Freezes the supplied mapping into a :class:`MappingProxyType` so
        downstream code (cache, plugin invocation) cannot mutate the
        resolver's output, and derives ``plugin_envelope_keys`` from
        the frozen view so the audit payload sees the exact keys that
        will flow into the plugin.
        """
        frozen = MappingProxyType(dict(material))
        keys = tuple(sorted(frozen.keys()))
        return cls(
            authentication_type=authentication_type,
            category=category,
            material=frozen,
            descriptor=descriptor,
            issued_at=issued_at,
            expires_at=expires_at,
            plugin_envelope_keys=keys,
        )


__all__ = ["ResolvedIdentity"]
