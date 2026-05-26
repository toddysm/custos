"""Compose the built-in resolvers, cache results, and emit audit events.

Phase F (CONN-IMPL-015) ships the registry as the single seam every
future caller (CONN-IMPL-016 bind, the secret-bridge sidecar) goes
through to obtain a :class:`ResolvedIdentity`. The registry is
deliberately small but covers four responsibilities:

1. **Lookup.** Built-in resolvers are registered at construction.
   Vendor (``x-<vendor>``) resolvers are added via
   :meth:`register_vendor_resolver`, mirroring the
   ``vendor_identity_categories`` override hook the Loader uses for
   the matching category derivation.

2. **Category check.** Each resolver declares its
   :class:`IdentityCategory` as a :class:`ClassVar`. The registry
   refuses to invoke a resolver whose ``category`` disagrees with the
   one the Loader derived for ``authentication_type``: that pairing
   would only ever happen via a misconfigured vendor override map and
   we'd rather fail loudly at first call than mint material under the
   wrong identity model.

3. **TTL cache.** The cache key is ``(workspace_id, instance_id,
   authentication_type, descriptor_key)``, where ``descriptor_key`` is
   a stable string derived from ``credentials_authentication``. Entries
   are aged out using the resolved ``expires_at`` (or the supplied
   ``lease_ttl_seconds`` when ``expires_at`` is ``None``).
   hammer the upstream identity provider; that pattern is exactly what
   the design's "lease TTL" guidance is about.

4. **Audit emission.** :func:`audit_identity_resolved` is rate-limited
   per ``(workspace_id, instance_id)`` (default: once per minute) so a
   well-cached bind loop does not flood the audit outbox. The
   ``connector.identity.failed`` emission is *not* rate-limited
   because each failure is operationally significant.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Final

from custos_connector.identity.errors import (
    IdentityResolverError,
    IdentityResolverErrorCode,
)
from custos_connector.identity.models import ResolvedIdentity
from custos_connector.identity.protocols import (
    IdentityResolver,
    IdentityResolverContext,
)
from custos_connector.identity.transport import AsyncHttpClient
from custos_connector.loader.identity import (
    BUILTIN_IDENTITY_CATEGORIES,
    IdentityCategory,
    derive_identity_category,
)

if TYPE_CHECKING:
    from custos_spl import MetadataStoreProvider

_LOGGER = logging.getLogger("custos_connector.identity.registry")

#: Default rate limit for the ``connector.identity.resolved`` event.
#: Keyed by ``(workspace_id, instance_id)``: once a ``resolved`` event
#: has been emitted for a key, further resolutions inside this window
#: are skipped at the emission layer. The cache still does its job —
#: callers still see the cached :class:`ResolvedIdentity` — but the
#: audit outbox is not flooded.
DEFAULT_RESOLVED_EVENT_RATE_LIMIT_SECONDS: Final[int] = 60


@dataclass(frozen=True, slots=True)
class _CacheEntry:
    """Internal cache entry: a resolved identity plus its aging bound."""

    value: ResolvedIdentity
    expires_at: datetime


class IdentityResolverRegistry:
    """Compose the per-auth-type resolvers and cache their results.

    The registry is the only public surface call sites use to obtain a
    :class:`ResolvedIdentity`; per-resolver instances are an
    implementation detail.

    Args:
        resolvers: The set of built-in resolvers to register. The
            registry rejects duplicates by ``authentication_type``.
            Each resolver's declared ``category`` is checked against
            :data:`BUILTIN_IDENTITY_CATEGORIES` at construction time so
            a programming error in the resolver class is caught at
            startup, not at first resolution.
        metadata_store: Optional :class:`MetadataStoreProvider` for the
            audit emissions. When ``None``, the registry runs without
            an audit pipeline (used by unit tests that focus on
            resolution and caching).
        clock: Time source the registry uses for cache aging *and* for
            the :class:`IdentityResolverContext`. Defaults to
            ``datetime.now(UTC)``.
        resolved_event_rate_limit_seconds: How often the
            ``connector.identity.resolved`` audit event fires per
            ``(workspace_id, instance_id)``.
        http_transport: Optional :class:`AsyncHttpClient` whose
            lifecycle the registry owns. When the registry is built
            via :func:`load_identity_registry` we pass the shared
            production transport here so the FastAPI lifespan can
            close it on shutdown via :meth:`aclose`. Unit tests that
            inject a stub transport per-resolver leave this ``None``.
    """

    def __init__(
        self,
        *,
        resolvers: Iterable[IdentityResolver] = (),
        metadata_store: MetadataStoreProvider | None = None,
        clock: Callable[[], datetime] | None = None,
        resolved_event_rate_limit_seconds: int = (DEFAULT_RESOLVED_EVENT_RATE_LIMIT_SECONDS),
        http_transport: AsyncHttpClient | None = None,
    ) -> None:
        if resolved_event_rate_limit_seconds < 0:
            raise ValueError(
                "resolved_event_rate_limit_seconds must be >= 0 "
                f"(got {resolved_event_rate_limit_seconds})"
            )
        self._builtin: dict[str, IdentityResolver] = {}
        self._vendor: dict[str, IdentityResolver] = {}
        self._vendor_categories: dict[str, IdentityCategory] = {}
        self._metadata_store = metadata_store
        self._clock = clock if clock is not None else (lambda: datetime.now(UTC))
        self._resolved_rate_limit = timedelta(seconds=resolved_event_rate_limit_seconds)
        self._last_resolved_emission: dict[tuple[str, str], datetime] = {}
        self._cache: dict[tuple[str, str, str, str], _CacheEntry] = {}
        self._lock = asyncio.Lock()
        self._http_transport = http_transport

        for resolver in resolvers:
            self._register_builtin(resolver)

    async def aclose(self) -> None:
        """Release any HTTP transport owned by the registry.

        Safe to call multiple times. When no transport was supplied
        (the unit-test path), this is a no-op so the FastAPI lifespan
        hook can call :meth:`aclose` unconditionally.
        """
        transport = self._http_transport
        if transport is None:
            return
        self._http_transport = None
        await transport.aclose()

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def _register_builtin(self, resolver: IdentityResolver) -> None:
        auth_type = resolver.authentication_type
        if auth_type not in BUILTIN_IDENTITY_CATEGORIES:
            raise ValueError(
                f"resolver for {auth_type!r} is not a built-in authentication "
                "type; register vendor resolvers via register_vendor_resolver()"
            )
        expected = BUILTIN_IDENTITY_CATEGORIES[auth_type]
        if resolver.category is not expected:
            raise ValueError(
                f"resolver for {auth_type!r} declares category "
                f"{resolver.category.value!r} but the Loader derives "
                f"{expected.value!r} for that authentication type"
            )
        if auth_type in self._builtin:
            raise ValueError(f"resolver for {auth_type!r} is already registered")
        self._builtin[auth_type] = resolver

    def register_vendor_resolver(
        self,
        resolver: IdentityResolver,
        *,
        category: IdentityCategory,
    ) -> None:
        """Register a vendor (``x-<vendor>``) resolver.

        Mirrors :class:`~custos_connector.loader.Loader`'s
        ``vendor_identity_categories`` override hook so an operator can
        teach the registry about a vendor token without code changes to
        the platform. The vendor's declared :class:`IdentityCategory`
        is recorded here and threaded into
        :func:`derive_identity_category` at resolution time, which
        means the Loader-side override map and the registry-side
        category must agree.

        Args:
            resolver: The vendor resolver. Its
                ``authentication_type`` must start with ``"x-"`` and
                its declared ``category`` must equal ``category``.
            category: The :class:`IdentityCategory` to record for this
                vendor token.

        Raises:
            ValueError: If the token is not ``x-*``, the resolver's
                declared category disagrees with ``category``, or a
                resolver is already registered for the same token.
        """
        auth_type = resolver.authentication_type
        if not auth_type.startswith("x-"):
            raise ValueError(
                f"vendor resolver token {auth_type!r} must start with 'x-'; "
                "built-in resolvers are registered via the constructor"
            )
        if resolver.category is not category:
            raise ValueError(
                f"vendor resolver for {auth_type!r} declares "
                f"category {resolver.category.value!r} but the "
                f"registration override is {category.value!r}"
            )
        if auth_type in self._vendor:
            raise ValueError(f"vendor resolver for {auth_type!r} is already registered")
        self._vendor[auth_type] = resolver
        self._vendor_categories[auth_type] = category

    @property
    def vendor_categories(self) -> Mapping[str, IdentityCategory]:
        """Read-only snapshot of the registered vendor category overrides.

        Suitable for threading into
        :class:`~custos_connector.loader.Loader` so the Loader's
        category derivation agrees with the resolver-side registration
        (a single source of truth for the ``x-<vendor>`` table).
        """
        return MappingProxyType(dict(self._vendor_categories))

    def supports(self, authentication_type: str) -> bool:
        """Return ``True`` when a resolver is registered for the token."""
        return authentication_type in self._builtin or authentication_type in self._vendor

    # ------------------------------------------------------------------
    # Resolution
    # ------------------------------------------------------------------

    async def resolve(
        self,
        *,
        workspace_id: str,
        actor: str,
        instance_id: str,
        authentication_type: str,
        credentials_authentication: Mapping[str, Any],
        lease_ttl_seconds: int,
    ) -> ResolvedIdentity:
        """Resolve an identity, hitting the cache first.

        On a hit, returns the cached :class:`ResolvedIdentity` without
        invoking the resolver and without emitting an audit event. On a
        miss, runs the resolver, populates the cache, and emits a
        (rate-limited) ``connector.identity.resolved`` event. On a
        resolver failure, emits a ``connector.identity.failed`` event
        and re-raises the original :class:`IdentityResolverError`.

        Args:
            workspace_id: Workspace owning the instance. Carried into
                the cache key, the resolver context, and the audit
                subject.
            actor: Caller identity for the audit payload (typically
                ``"connector-service"`` for system-driven binds).
            instance_id: ConnectorInstance UUID. Carried everywhere.
            authentication_type: The manifest token whose resolver
                this call should invoke.
            credentials_authentication: The per-instance
                ``credentials.authentication`` payload.
            lease_ttl_seconds: The lease TTL the caller will request
                from the Lease Manager; passed into the resolver
                context.

        Raises:
            IdentityResolverError: On any resolver-side failure. The
                exception is re-raised after the failure audit event is
                emitted.
        """
        resolver = self._lookup(authentication_type)
        category = self._resolve_category(authentication_type)
        descriptor_key = _descriptor_key(authentication_type, credentials_authentication)
        cache_key = (workspace_id, instance_id, authentication_type, descriptor_key)

        # Cache check (lock-free read is fine; staleness only causes a
        # second resolve, never a wrong return).
        now = self._clock()
        cached = self._cache.get(cache_key)
        if cached is not None and cached.expires_at > now:
            return cached.value

        async with self._lock:
            # Re-check inside the lock so concurrent callers for the
            # same key collapse onto a single upstream call.
            now = self._clock()
            cached = self._cache.get(cache_key)
            if cached is not None and cached.expires_at > now:
                return cached.value

            context = IdentityResolverContext(
                workspace_id=workspace_id,
                instance_id=instance_id,
                lease_ttl_seconds=lease_ttl_seconds,
                now=self._clock,
            )
            try:
                resolved = await resolver.resolve(
                    credentials_authentication=credentials_authentication,
                    context=context,
                )
            except IdentityResolverError as exc:
                await self._emit_failure(
                    workspace_id=workspace_id,
                    actor=actor,
                    instance_id=instance_id,
                    authentication_type=authentication_type,
                    category=category,
                    error=exc,
                )
                raise

            if resolved.category is not category:
                # A resolver returning a category that disagrees with
                # the Loader-derived category is a programming error;
                # surface it the same way as a mismatched registration.
                category_mismatch = IdentityResolverError(
                    detail=(
                        f"resolver for {authentication_type!r} returned "
                        f"category {resolved.category.value!r} but the "
                        f"registry expects {category.value!r}"
                    ),
                    code=IdentityResolverErrorCode.CATEGORY_MISMATCH,
                )
                await self._emit_failure(
                    workspace_id=workspace_id,
                    actor=actor,
                    instance_id=instance_id,
                    authentication_type=authentication_type,
                    category=category,
                    error=category_mismatch,
                )
                raise category_mismatch

            cache_expiry = self._cache_expiry(
                resolved=resolved,
                lease_ttl_seconds=lease_ttl_seconds,
                now=now,
            )
            self._cache[cache_key] = _CacheEntry(value=resolved, expires_at=cache_expiry)

            await self._emit_resolved_rate_limited(
                workspace_id=workspace_id,
                actor=actor,
                instance_id=instance_id,
                resolved=resolved,
                now=now,
            )
            return resolved

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _lookup(self, authentication_type: str) -> IdentityResolver:
        if authentication_type in self._builtin:
            return self._builtin[authentication_type]
        if authentication_type in self._vendor:
            return self._vendor[authentication_type]
        raise IdentityResolverError(
            detail=(f"no resolver is registered for authentication_type {authentication_type!r}"),
            code=IdentityResolverErrorCode.UNKNOWN_AUTHENTICATION_TYPE,
        )

    def _resolve_category(self, authentication_type: str) -> IdentityCategory:
        # ``derive_identity_category`` raises a Loader error for
        # unknown tokens, but we already filtered to known tokens via
        # ``_lookup``; here we just thread the vendor overrides so the
        # derived category for ``x-*`` matches what was registered.
        return derive_identity_category(
            authentication_type,
            vendor_overrides=self._vendor_categories,
        )

    def _cache_expiry(
        self,
        *,
        resolved: ResolvedIdentity,
        lease_ttl_seconds: int,
        now: datetime,
    ) -> datetime:
        # When the resolver reported an upstream expiry, honour it. We
        # also clamp to the lease TTL so a long-lived upstream secret
        # does not outlive the lease that authorised its use.
        ttl_bound = now + timedelta(seconds=max(0, lease_ttl_seconds))
        if resolved.expires_at is None:
            return ttl_bound
        return min(resolved.expires_at, ttl_bound)

    async def _emit_resolved_rate_limited(
        self,
        *,
        workspace_id: str,
        actor: str,
        instance_id: str,
        resolved: ResolvedIdentity,
        now: datetime,
    ) -> None:
        if self._metadata_store is None:
            return
        key = (workspace_id, instance_id)
        last = self._last_resolved_emission.get(key)
        if last is not None and now - last < self._resolved_rate_limit:
            return
        from custos_connector.audit import audit_identity_resolved

        self._last_resolved_emission[key] = now
        await audit_identity_resolved(
            self._metadata_store,
            workspace_id=workspace_id,
            actor=actor,
            instance_id=instance_id,
            authentication_type=resolved.authentication_type,
            category=resolved.category.value,
            descriptor=resolved.descriptor,
            material_keys=resolved.plugin_envelope_keys,
            expires_at=resolved.expires_at,
            issued_at=resolved.issued_at,
        )

    async def _emit_failure(
        self,
        *,
        workspace_id: str,
        actor: str,
        instance_id: str,
        authentication_type: str,
        category: IdentityCategory,
        error: IdentityResolverError,
    ) -> None:
        if self._metadata_store is None:
            _LOGGER.warning(
                "identity resolution failed instance=%s auth_type=%s code=%s detail=%s",
                instance_id,
                authentication_type,
                error.code.value,
                error.detail,
            )
            return
        from custos_connector.audit import audit_identity_failed

        await audit_identity_failed(
            self._metadata_store,
            workspace_id=workspace_id,
            actor=actor,
            instance_id=instance_id,
            authentication_type=authentication_type,
            category=category.value,
            error_code=error.code.value,
            error_detail=error.detail,
            error_data=dict(error.data),
        )


def _descriptor_key(
    authentication_type: str,
    credentials_authentication: Mapping[str, Any],
) -> str:
    """Build a stable cache key from a (token, credentials) pair.

    We hash the credentials mapping's *keys + scalar values* so two
    instances pointing at the same vault / secret name / issuer share a
    cache entry, without persisting the secret material itself. JSON
    with ``sort_keys=True`` would also work but pulls in a JSON encode
    on every cache check; ``repr`` of a sorted tuple is enough because
    the registry only needs a stable string for the key.
    """
    items = sorted((str(k), _scalar_repr(v)) for k, v in credentials_authentication.items())
    return f"{authentication_type}|" + "|".join(f"{k}={v}" for k, v in items)


def _scalar_repr(value: Any) -> str:
    if value is None or isinstance(value, str | int | float | bool):
        return repr(value)
    # Nested mappings / sequences: degrade to ``repr`` for stability —
    # the only goal here is a deterministic string, not a normalised
    # canonical form.
    return repr(value)


__all__ = [
    "DEFAULT_RESOLVED_EVENT_RATE_LIMIT_SECONDS",
    "IdentityResolverRegistry",
]
