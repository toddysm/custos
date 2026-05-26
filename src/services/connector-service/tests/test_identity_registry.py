"""Unit tests for :class:`IdentityResolverRegistry`.

The registry composes resolvers, caches results, threads the loader's
vendor-override pattern, and emits rate-limited audit events. We use a
stub resolver that records every call so we can assert lookup, caching,
and audit behaviour without standing up the real HTTP-backed resolvers.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, ClassVar

import pytest

from custos_connector.identity import (
    IdentityResolverError,
    IdentityResolverRegistry,
    ResolvedIdentity,
)
from custos_connector.identity.errors import IdentityResolverErrorCode
from custos_connector.identity.protocols import IdentityResolverContext
from custos_connector.loader.identity import IdentityCategory
from tests._fakes import FakeMetadataAdapter


class _StubResolver:
    """Records each call and returns a configurable :class:`ResolvedIdentity`."""

    authentication_type: ClassVar[str] = "azure-key-vault"
    category: ClassVar[IdentityCategory] = IdentityCategory.KMS

    def __init__(
        self,
        *,
        material: Mapping[str, Any] | None = None,
        expires_at: datetime | None = None,
        fail_with: IdentityResolverError | None = None,
        category_override: IdentityCategory | None = None,
    ) -> None:
        self.material = dict(material) if material is not None else {"secret": "s"}
        self.expires_at = expires_at
        self.fail_with = fail_with
        self.category_override = category_override
        self.calls: list[tuple[Mapping[str, Any], IdentityResolverContext]] = []

    async def resolve(
        self,
        *,
        credentials_authentication: Mapping[str, Any],
        context: IdentityResolverContext,
    ) -> ResolvedIdentity:
        self.calls.append((dict(credentials_authentication), context))
        if self.fail_with is not None:
            raise self.fail_with
        return ResolvedIdentity.build(
            authentication_type=self.authentication_type,
            category=self.category_override or self.category,
            material=self.material,
            descriptor=(
                f"azure-key-vault:{credentials_authentication.get('vaultUri', '?')}"
                f"/secrets/{credentials_authentication.get('secretName', '?')}"
            ),
            issued_at=context.now(),
            expires_at=self.expires_at,
        )


class _FixedClock:
    def __init__(self, *, start: datetime) -> None:
        self.now_value = start

    def __call__(self) -> datetime:
        return self.now_value

    def advance(self, seconds: float) -> None:
        self.now_value = self.now_value + timedelta(seconds=seconds)


class TestRegistration:
    def test_registers_each_builtin_once(self) -> None:
        resolver = _StubResolver()
        registry = IdentityResolverRegistry(resolvers=[resolver])
        assert registry.supports("azure-key-vault")
        assert not registry.supports("oidc")

    def test_duplicate_builtin_registration_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="already registered"):
            IdentityResolverRegistry(resolvers=[_StubResolver(), _StubResolver()])

    def test_unknown_builtin_token_is_rejected(self) -> None:
        class _Bogus(_StubResolver):
            authentication_type: ClassVar[str] = "not-a-builtin"
            category: ClassVar[IdentityCategory] = IdentityCategory.KMS

        with pytest.raises(ValueError, match="not a built-in"):
            IdentityResolverRegistry(resolvers=[_Bogus()])

    def test_builtin_category_must_match_loader_table(self) -> None:
        class _Misaligned(_StubResolver):
            category: ClassVar[IdentityCategory] = IdentityCategory.FEDERATED

        with pytest.raises(ValueError, match="declares category"):
            IdentityResolverRegistry(resolvers=[_Misaligned()])

    def test_vendor_resolver_round_trip(self) -> None:
        class _Vendor(_StubResolver):
            authentication_type: ClassVar[str] = "x-acme"
            category: ClassVar[IdentityCategory] = IdentityCategory.WORKLOAD

        registry = IdentityResolverRegistry()
        registry.register_vendor_resolver(_Vendor(), category=IdentityCategory.WORKLOAD)
        assert registry.supports("x-acme")
        assert registry.vendor_categories == {"x-acme": IdentityCategory.WORKLOAD}

    def test_vendor_token_must_start_with_x_dash(self) -> None:
        class _NotVendor(_StubResolver):
            authentication_type: ClassVar[str] = "oidc"
            category: ClassVar[IdentityCategory] = IdentityCategory.FEDERATED

        with pytest.raises(ValueError, match="must start with 'x-'"):
            IdentityResolverRegistry().register_vendor_resolver(
                _NotVendor(), category=IdentityCategory.FEDERATED
            )

    def test_vendor_category_mismatch_is_rejected(self) -> None:
        class _Vendor(_StubResolver):
            authentication_type: ClassVar[str] = "x-acme"
            category: ClassVar[IdentityCategory] = IdentityCategory.WORKLOAD

        with pytest.raises(ValueError, match="declares"):
            IdentityResolverRegistry().register_vendor_resolver(
                _Vendor(), category=IdentityCategory.KMS
            )


class TestResolveLookup:
    @pytest.mark.asyncio
    async def test_unknown_authentication_type_raises(self) -> None:
        registry = IdentityResolverRegistry()
        with pytest.raises(IdentityResolverError) as info:
            await registry.resolve(
                workspace_id="ws-1",
                actor="connector-service",
                instance_id="inst-1",
                authentication_type="oidc",
                credentials_authentication={},
                lease_ttl_seconds=600,
            )
        assert info.value.code is (IdentityResolverErrorCode.UNKNOWN_AUTHENTICATION_TYPE)

    @pytest.mark.asyncio
    async def test_unknown_authentication_type_emits_failure_audit(self) -> None:
        # ``connector.identity.failed`` is contractually always-on; the
        # registry must emit it even when ``_lookup`` raises before any
        # resolver is invoked. The category is reported as ``"unknown"``
        # since we never got far enough to derive it.
        store = FakeMetadataAdapter()
        registry = IdentityResolverRegistry(
            metadata_store=store,  # type: ignore[arg-type]
        )
        with pytest.raises(IdentityResolverError):
            await registry.resolve(
                workspace_id="ws-1",
                actor="connector-service",
                instance_id="inst-1",
                authentication_type="oidc",
                credentials_authentication={},
                lease_ttl_seconds=600,
            )
        assert len(store.append_audit_calls) == 1
        _, event = store.append_audit_calls[0]
        assert event.event_type == "connector.identity.failed"
        assert event.subject["authentication_type"] == "oidc"
        assert event.subject["category"] == "unknown"
        assert event.payload["error_code"] == "unknown-authentication-type"

    @pytest.mark.asyncio
    async def test_resolver_called_with_context_fields(self) -> None:
        resolver = _StubResolver()
        clock = _FixedClock(start=datetime(2026, 5, 1, tzinfo=UTC))
        registry = IdentityResolverRegistry(resolvers=[resolver], clock=clock)
        resolved = await registry.resolve(
            workspace_id="ws-1",
            actor="connector-service",
            instance_id="inst-A",
            authentication_type="azure-key-vault",
            credentials_authentication={
                "vaultUri": "https://v.example",
                "secretName": "foo",
            },
            lease_ttl_seconds=600,
        )
        assert resolved.authentication_type == "azure-key-vault"
        assert resolved.category is IdentityCategory.KMS
        assert resolver.calls[0][1].workspace_id == "ws-1"
        assert resolver.calls[0][1].instance_id == "inst-A"
        assert resolver.calls[0][1].lease_ttl_seconds == 600


class TestCache:
    @pytest.mark.asyncio
    async def test_repeated_call_hits_cache(self) -> None:
        resolver = _StubResolver()
        clock = _FixedClock(start=datetime(2026, 5, 1, tzinfo=UTC))
        registry = IdentityResolverRegistry(resolvers=[resolver], clock=clock)

        payload = {"vaultUri": "https://v", "secretName": "foo"}
        await registry.resolve(
            workspace_id="ws-1",
            actor="connector-service",
            instance_id="inst-A",
            authentication_type="azure-key-vault",
            credentials_authentication=payload,
            lease_ttl_seconds=600,
        )
        await registry.resolve(
            workspace_id="ws-1",
            actor="connector-service",
            instance_id="inst-A",
            authentication_type="azure-key-vault",
            credentials_authentication=payload,
            lease_ttl_seconds=600,
        )
        assert len(resolver.calls) == 1

    @pytest.mark.asyncio
    async def test_cache_evicted_when_ttl_passes(self) -> None:
        resolver = _StubResolver()
        clock = _FixedClock(start=datetime(2026, 5, 1, tzinfo=UTC))
        registry = IdentityResolverRegistry(resolvers=[resolver], clock=clock)

        payload = {"vaultUri": "https://v", "secretName": "foo"}
        await registry.resolve(
            workspace_id="ws-1",
            actor="connector-service",
            instance_id="inst-A",
            authentication_type="azure-key-vault",
            credentials_authentication=payload,
            lease_ttl_seconds=60,
        )
        clock.advance(61)
        await registry.resolve(
            workspace_id="ws-1",
            actor="connector-service",
            instance_id="inst-A",
            authentication_type="azure-key-vault",
            credentials_authentication=payload,
            lease_ttl_seconds=60,
        )
        assert len(resolver.calls) == 2

    @pytest.mark.asyncio
    async def test_different_instances_do_not_share_cache(self) -> None:
        resolver = _StubResolver()
        clock = _FixedClock(start=datetime(2026, 5, 1, tzinfo=UTC))
        registry = IdentityResolverRegistry(resolvers=[resolver], clock=clock)

        payload = {"vaultUri": "https://v", "secretName": "foo"}
        for instance_id in ("inst-A", "inst-B"):
            await registry.resolve(
                workspace_id="ws-1",
                actor="connector-service",
                instance_id=instance_id,
                authentication_type="azure-key-vault",
                credentials_authentication=payload,
                lease_ttl_seconds=600,
            )
        assert len(resolver.calls) == 2

    @pytest.mark.asyncio
    async def test_cache_expiry_clamped_to_lease_ttl(self) -> None:
        # Resolver claims expiry far in the future; lease TTL is tight.
        resolver = _StubResolver(
            expires_at=datetime(2026, 12, 31, tzinfo=UTC),
        )
        clock = _FixedClock(start=datetime(2026, 5, 1, tzinfo=UTC))
        registry = IdentityResolverRegistry(resolvers=[resolver], clock=clock)

        payload = {"vaultUri": "https://v", "secretName": "foo"}
        await registry.resolve(
            workspace_id="ws-1",
            actor="connector-service",
            instance_id="inst-A",
            authentication_type="azure-key-vault",
            credentials_authentication=payload,
            lease_ttl_seconds=30,
        )
        clock.advance(31)
        await registry.resolve(
            workspace_id="ws-1",
            actor="connector-service",
            instance_id="inst-A",
            authentication_type="azure-key-vault",
            credentials_authentication=payload,
            lease_ttl_seconds=30,
        )
        # Lease TTL won the clamp: second call should re-resolve.
        assert len(resolver.calls) == 2


class _GatedResolver:
    """Resolver that blocks each call on an :class:`asyncio.Event`.

    Used to deterministically prove that concurrent resolves for
    *different* cache keys run in parallel (no global serialization)
    while concurrent resolves for the *same* cache key collapse onto a
    single upstream call.
    """

    authentication_type: ClassVar[str] = "azure-key-vault"
    category: ClassVar[IdentityCategory] = IdentityCategory.KMS

    def __init__(self) -> None:
        self.gate = asyncio.Event()
        self.calls_started = 0
        self.calls_completed = 0
        # ``concurrent_peak`` records the maximum number of in-flight
        # upstream calls observed at any point; > 1 proves unrelated
        # keys do not serialise behind a slow resolver.
        self._in_flight = 0
        self.concurrent_peak = 0

    async def resolve(
        self,
        *,
        credentials_authentication: Mapping[str, Any],
        context: IdentityResolverContext,
    ) -> ResolvedIdentity:
        self.calls_started += 1
        self._in_flight += 1
        self.concurrent_peak = max(self.concurrent_peak, self._in_flight)
        try:
            await self.gate.wait()
            return ResolvedIdentity.build(
                authentication_type=self.authentication_type,
                category=self.category,
                material={"secret": "s"},
                descriptor=(
                    f"azure-key-vault:{credentials_authentication.get('vaultUri', '?')}"
                    f"/secrets/{credentials_authentication.get('secretName', '?')}"
                ),
                issued_at=context.now(),
                expires_at=None,
            )
        finally:
            self._in_flight -= 1
            self.calls_completed += 1


class TestConcurrency:
    @pytest.mark.asyncio
    async def test_unrelated_cache_misses_run_in_parallel(self) -> None:
        # Two concurrent resolves for *different* cache keys must not
        # serialise. With a single global lock the second call would
        # only start after the first completes, so ``concurrent_peak``
        # would be 1; with per-key locks both are in flight together
        # and ``concurrent_peak`` is 2.
        resolver = _GatedResolver()
        registry = IdentityResolverRegistry(resolvers=[resolver])

        async def _resolve(secret: str) -> ResolvedIdentity:
            return await registry.resolve(
                workspace_id="ws-1",
                actor="connector-service",
                instance_id="inst-A",
                authentication_type="azure-key-vault",
                credentials_authentication={
                    "vaultUri": "https://v.example",
                    "secretName": secret,
                },
                lease_ttl_seconds=600,
            )

        task_a = asyncio.create_task(_resolve("foo"))
        task_b = asyncio.create_task(_resolve("bar"))

        # Yield control until both tasks are parked inside the
        # resolver. Without per-key locks, only the first ever enters
        # ``resolver.resolve``.
        for _ in range(50):
            if resolver.calls_started >= 2:
                break
            await asyncio.sleep(0)
        assert resolver.calls_started == 2, "second cache miss serialised behind the first"

        resolver.gate.set()
        results = await asyncio.gather(task_a, task_b)
        assert {r.descriptor for r in results} == {
            "azure-key-vault:https://v.example/secrets/foo",
            "azure-key-vault:https://v.example/secrets/bar",
        }
        assert resolver.concurrent_peak == 2

    @pytest.mark.asyncio
    async def test_same_key_concurrent_callers_collapse_onto_one_resolve(self) -> None:
        # Two concurrent resolves for the *same* cache key must run
        # the upstream resolver exactly once; the second caller blocks
        # on the per-key lock, re-checks the cache on entry, and
        # returns the cached value.
        resolver = _GatedResolver()
        registry = IdentityResolverRegistry(resolvers=[resolver])

        async def _resolve() -> ResolvedIdentity:
            return await registry.resolve(
                workspace_id="ws-1",
                actor="connector-service",
                instance_id="inst-A",
                authentication_type="azure-key-vault",
                credentials_authentication={
                    "vaultUri": "https://v.example",
                    "secretName": "shared",
                },
                lease_ttl_seconds=600,
            )

        task_a = asyncio.create_task(_resolve())
        task_b = asyncio.create_task(_resolve())

        # Park both tasks. ``calls_started`` must stay at 1 because
        # task_b is blocked on the per-key lock, not the gate.
        for _ in range(50):
            if resolver.calls_started >= 1:
                break
            await asyncio.sleep(0)
        # Give task_b a chance to also reach (and block on) the
        # per-key lock without entering the resolver.
        for _ in range(10):
            await asyncio.sleep(0)
        assert resolver.calls_started == 1

        resolver.gate.set()
        results = await asyncio.gather(task_a, task_b)
        assert results[0] is results[1]
        assert resolver.calls_completed == 1

    @pytest.mark.asyncio
    async def test_per_key_lock_dict_is_cleaned_up(self) -> None:
        # The per-key lock map must not grow unbounded. After all
        # waiters release, the entry is evicted.
        resolver = _StubResolver()
        registry = IdentityResolverRegistry(resolvers=[resolver])
        await registry.resolve(
            workspace_id="ws-1",
            actor="connector-service",
            instance_id="inst-A",
            authentication_type="azure-key-vault",
            credentials_authentication={
                "vaultUri": "https://v.example",
                "secretName": "foo",
            },
            lease_ttl_seconds=600,
        )
        # Private attribute access is justified here: the eviction
        # invariant is part of the per-key locking contract.
        assert registry._key_locks == {}


class TestAuditEmission:
    @pytest.mark.asyncio
    async def test_resolved_emits_when_metadata_store_provided(self) -> None:
        resolver = _StubResolver()
        clock = _FixedClock(start=datetime(2026, 5, 1, tzinfo=UTC))
        store = FakeMetadataAdapter()
        registry = IdentityResolverRegistry(
            resolvers=[resolver],
            metadata_store=store,  # type: ignore[arg-type]
            clock=clock,
            resolved_event_rate_limit_seconds=0,
        )
        await registry.resolve(
            workspace_id="ws-1",
            actor="connector-service",
            instance_id="inst-A",
            authentication_type="azure-key-vault",
            credentials_authentication={
                "vaultUri": "https://v",
                "secretName": "foo",
            },
            lease_ttl_seconds=600,
        )
        assert len(store.append_audit_calls) == 1
        _, event = store.append_audit_calls[0]
        assert event.event_type == "connector.identity.resolved"
        assert event.subject["instance_id"] == "inst-A"
        assert event.subject["authentication_type"] == "azure-key-vault"
        assert event.subject["category"] == "kms"
        assert event.payload["material_keys"] == ["secret"]

    @pytest.mark.asyncio
    async def test_resolved_emission_is_rate_limited(self) -> None:
        resolver = _StubResolver()
        clock = _FixedClock(start=datetime(2026, 5, 1, tzinfo=UTC))
        store = FakeMetadataAdapter()
        registry = IdentityResolverRegistry(
            resolvers=[resolver],
            metadata_store=store,  # type: ignore[arg-type]
            clock=clock,
            resolved_event_rate_limit_seconds=60,
        )
        # Two consecutive resolutions with cache misses (different
        # secrets) inside the 60-second window — only the first emits.
        await registry.resolve(
            workspace_id="ws-1",
            actor="connector-service",
            instance_id="inst-A",
            authentication_type="azure-key-vault",
            credentials_authentication={
                "vaultUri": "https://v",
                "secretName": "foo",
            },
            lease_ttl_seconds=600,
        )
        await registry.resolve(
            workspace_id="ws-1",
            actor="connector-service",
            instance_id="inst-A",
            authentication_type="azure-key-vault",
            credentials_authentication={
                "vaultUri": "https://v",
                "secretName": "bar",
            },
            lease_ttl_seconds=600,
        )
        assert len(store.append_audit_calls) == 1

        clock.advance(61)
        await registry.resolve(
            workspace_id="ws-1",
            actor="connector-service",
            instance_id="inst-A",
            authentication_type="azure-key-vault",
            credentials_authentication={
                "vaultUri": "https://v",
                "secretName": "baz",
            },
            lease_ttl_seconds=600,
        )
        assert len(store.append_audit_calls) == 2

    @pytest.mark.asyncio
    async def test_failure_emits_audit_and_reraises(self) -> None:
        resolver = _StubResolver(
            fail_with=IdentityResolverError(
                "missing field vaultUri",
                code=IdentityResolverErrorCode.MISSING_CREDENTIAL_FIELD,
                data={"field": "vaultUri"},
            )
        )
        store = FakeMetadataAdapter()
        registry = IdentityResolverRegistry(
            resolvers=[resolver],
            metadata_store=store,  # type: ignore[arg-type]
        )
        with pytest.raises(IdentityResolverError):
            await registry.resolve(
                workspace_id="ws-1",
                actor="connector-service",
                instance_id="inst-A",
                authentication_type="azure-key-vault",
                credentials_authentication={"secretName": "foo"},
                lease_ttl_seconds=600,
            )
        assert len(store.append_audit_calls) == 1
        _, event = store.append_audit_calls[0]
        assert event.event_type == "connector.identity.failed"
        assert event.payload["error_code"] == "missing-credential-field"
        assert event.payload["error_data"]["field"] == "vaultUri"

    @pytest.mark.asyncio
    async def test_category_mismatch_returned_by_resolver_is_audited(self) -> None:
        resolver = _StubResolver(category_override=IdentityCategory.FEDERATED)
        store = FakeMetadataAdapter()
        registry = IdentityResolverRegistry(
            resolvers=[resolver],
            metadata_store=store,  # type: ignore[arg-type]
        )
        with pytest.raises(IdentityResolverError) as info:
            await registry.resolve(
                workspace_id="ws-1",
                actor="connector-service",
                instance_id="inst-A",
                authentication_type="azure-key-vault",
                credentials_authentication={
                    "vaultUri": "https://v",
                    "secretName": "foo",
                },
                lease_ttl_seconds=600,
            )
        assert info.value.code is IdentityResolverErrorCode.CATEGORY_MISMATCH
        _, event = store.append_audit_calls[0]
        assert event.event_type == "connector.identity.failed"
        assert event.payload["error_code"] == "category-mismatch"
