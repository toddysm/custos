"""Tests for ``custos_auth.oidc.provisioning`` (AS-IMPL-023)."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from custos_spl import User
from custos_spl.ids import PrincipalId, TenantId

from custos_auth.oidc.config import GroupBinding, OidcIssuerConfig
from custos_auth.oidc.provisioning import (
    DEFAULT_PROVISION_TENANT_ID,
    PROVISIONED_USER_ID_PREFIX,
    OidcProvisioner,
    ProvisionResult,
)
from custos_auth.oidc.verifier import VerifiedOidcIdentity
from custos_auth.oidc_identity import OidcIdentityAlreadyBound
from tests._fakes import FakeAuthAdapter, FakeMetadataAdapter


def _entry(**overrides: object) -> OidcIssuerConfig:
    defaults: dict[str, object] = dict(
        id="primary",
        preset=None,
        issuer_url="https://issuer.example.com",
        jwks_uri="https://issuer.example.com/keys",
        audiences=("api://custos",),
        algorithms=("RS256",),
        subject_claim="sub",
        provisioning_policy="zero-binding",
        group_claim=None,
        group_bindings=(),
        token_endpoint=None,
        client_id=None,
        client_secret_env=None,
    )
    defaults.update(overrides)
    return OidcIssuerConfig(**defaults)  # type: ignore[arg-type]


def _identity(
    entry: OidcIssuerConfig,
    *,
    subject: str = "user-42",
    **claims: object,
) -> VerifiedOidcIdentity:
    return VerifiedOidcIdentity(
        issuer_config=entry,
        subject=subject,
        claims={"sub": subject, **claims},
    )


def _build_provisioner() -> tuple[OidcProvisioner, FakeAuthAdapter, FakeMetadataAdapter]:
    auth = FakeAuthAdapter()
    meta = FakeMetadataAdapter()
    provisioner = OidcProvisioner(auth, meta)  # type: ignore[arg-type]
    return provisioner, auth, meta


async def test_provision_creates_new_user_on_unknown_identity() -> None:
    provisioner, auth, meta = _build_provisioner()
    entry = _entry()
    identity = _identity(entry, name="Alice", email="alice@example.com")

    result = await provisioner.provision(identity)

    assert isinstance(result, ProvisionResult)
    assert result.newly_provisioned is True
    assert result.user.display_name == "Alice"
    assert result.user.email == "alice@example.com"
    assert str(result.user.principal_id).startswith(PROVISIONED_USER_ID_PREFIX)
    assert str(result.user.tenant_id) == DEFAULT_PROVISION_TENANT_ID
    # OIDC identity row was persisted.
    bound = await auth.get_oidc_identity(entry.issuer_url, identity.subject)
    assert bound == result.user.principal_id
    # Audit emitted oidc.identity-linked once.
    emitted_events = [event.event_type for _, event in meta.append_audit_calls]
    assert "oidc.identity-linked" in emitted_events


async def test_provision_returns_existing_user_when_already_bound() -> None:
    provisioner, _, _ = _build_provisioner()
    entry = _entry()
    identity = _identity(entry, name="Alice")

    first = await provisioner.provision(identity)
    second = await provisioner.provision(identity)

    assert second.newly_provisioned is False
    assert second.user.principal_id == first.user.principal_id


async def test_provision_skips_email_when_unverified() -> None:
    provisioner, _, _ = _build_provisioner()
    entry = _entry()
    identity = _identity(entry, email="alice@example.com", email_verified=False)
    result = await provisioner.provision(identity)
    assert result.user.email is None


async def test_provision_keeps_email_when_verified_flag_absent() -> None:
    # GitHub Actions workload tokens omit email_verified — we keep the
    # email when it is present and not explicitly False.
    provisioner, _, _ = _build_provisioner()
    entry = _entry()
    identity = _identity(entry, email="alice@example.com")
    result = await provisioner.provision(identity)
    assert result.user.email == "alice@example.com"


async def test_provision_falls_back_to_synthetic_display_name() -> None:
    provisioner, _, _ = _build_provisioner()
    entry = _entry(preset="github")
    identity = _identity(entry, subject="repo:acme/sandbox:ref:main")
    result = await provisioner.provision(identity)
    assert result.user.display_name == "github user repo:acme/sandbox:ref:main"


async def test_provision_surfaces_matched_group_bindings() -> None:
    provisioner, _, _ = _build_provisioner()
    entry = _entry(
        group_claim="groups",
        group_bindings=(
            GroupBinding(claim_value="admins-guid", role="platform-admin", workspace_id="ws-1"),
            GroupBinding(claim_value="ops-guid", role="viewer", workspace_id="ws-2"),
            GroupBinding(claim_value="missing-guid", role="x", workspace_id="ws-x"),
        ),
    )
    identity = VerifiedOidcIdentity(
        issuer_config=entry,
        subject="user-42",
        claims={"sub": "user-42", "groups": ["admins-guid", "ops-guid"]},
    )
    result = await provisioner.provision(identity)
    matched_values = {b.claim_value for b in result.matched_group_bindings}
    assert matched_values == {"admins-guid", "ops-guid"}


async def test_provision_returns_empty_bindings_when_group_claim_absent() -> None:
    provisioner, _, _ = _build_provisioner()
    entry = _entry(
        group_claim="groups",
        group_bindings=(GroupBinding(claim_value="g", role="r", workspace_id="ws"),),
    )
    identity = VerifiedOidcIdentity(issuer_config=entry, subject="u", claims={"sub": "u"})
    result = await provisioner.provision(identity)
    assert result.matched_group_bindings == ()


async def test_provision_returns_empty_bindings_when_group_claim_not_list() -> None:
    provisioner, _, _ = _build_provisioner()
    entry = _entry(
        group_claim="groups",
        group_bindings=(GroupBinding(claim_value="g", role="r", workspace_id="ws"),),
    )
    identity = VerifiedOidcIdentity(
        issuer_config=entry,
        subject="u",
        claims={"sub": "u", "groups": "not-a-list"},
    )
    result = await provisioner.provision(identity)
    assert result.matched_group_bindings == ()


async def test_provision_propagates_race_condition() -> None:
    # Pre-bind the identity to a *different* user id to simulate a peer
    # writing the link between get_oidc_identity() and put_oidc_identity().
    provisioner, auth, _ = _build_provisioner()
    entry = _entry()
    identity = _identity(entry, subject="user-42")

    peer_user_id = PrincipalId("oidc-usr-peer")
    # Stash the peer's user row first so the auth store is internally consistent.
    auth.principals[str(peer_user_id)] = User(
        kind="user",
        principal_id=peer_user_id,
        tenant_id=TenantId("platform"),
        display_name="Peer",
        email=None,
        disabled_at=None,
        disabled_reason=None,
        created_at=datetime.now(UTC),
    )

    # Patch get_oidc_identity to return None on the first call (cache
    # miss) but ImmutableViolation surfaces from the peer's pre-existing
    # binding written *after* the check.
    original_get = auth.get_oidc_identity
    call_count = {"n": 0}

    async def racing_get(issuer: str, subject: str) -> PrincipalId | None:
        call_count["n"] += 1
        if call_count["n"] == 1:
            return None
        return await original_get(issuer, subject)

    auth.get_oidc_identity = racing_get  # type: ignore[method-assign]
    # Pre-write the peer's binding so put_oidc_identity raises.
    auth.oidc_identities[(entry.issuer_url, "user-42")] = str(peer_user_id)

    with pytest.raises(OidcIdentityAlreadyBound):
        await provisioner.provision(identity)
