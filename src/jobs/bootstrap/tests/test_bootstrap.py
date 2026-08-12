"""Unit tests for the ``custos-bootstrap`` platform seeder.

A minimal in-memory ``AuthStoreProvider`` fake captures the seeded state so the
end-to-end seed, idempotency, and admin-skip paths are exercised without a live
Postgres.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from typing import cast

import pytest
from custos_auth.roles import BUILTIN_ROLES, ROLE_PLATFORM_ADMIN
from custos_auth.tokens import hash_token, mint_token
from custos_spl import AuthStoreProvider
from custos_spl.errors import ImmutableViolation
from custos_spl.ids import PrincipalId, RoleId, ServiceTokenId, TenantId, WorkspaceId
from custos_spl.interfaces.auth_store import (
    GlobalScope,
    Permission,
    Principal,
    Role,
    RoleBinding,
    RoleBindingScope,
    ServiceToken,
    Tenant,
    Workspace,
)

from custos_bootstrap import (
    DEFAULT_TENANT_ID,
    DEFAULT_WORKSPACE_ID,
    main,
    resolve_dsn,
    resolve_permission_paths,
    seed_platform,
)
from custos_bootstrap.__main__ import (
    ADMIN_TOKEN_ENV,
    ADMIN_TOKEN_MODE_ENV,
    BootstrapAdminMode,
    BootstrapAdminTokenConfig,
    resolve_bootstrap_admin_token_config,
)

_ISSUER = "https://login.example.com"
_SUBJECT = "auth0|admin"


class FakeAuthStore:
    """In-memory stand-in for the methods the bootstrap seeder touches."""

    def __init__(self) -> None:
        self.permissions: dict[str, Permission] = {}
        self.roles: dict[RoleId, Role] = {}
        self.tenants: dict[TenantId, Tenant] = {}
        self.workspaces: dict[WorkspaceId, Workspace] = {}
        self.principals: dict[PrincipalId, Principal] = {}
        self.oidc: dict[tuple[str, str], PrincipalId] = {}
        self.bindings: list[RoleBinding] = []
        self.tokens: dict[ServiceTokenId, ServiceToken] = {}

    async def upsert_permission(self, permission: Permission) -> None:
        self.permissions[permission.name] = permission

    async def put_role(self, role: Role) -> None:
        self.roles[role.role_id] = role

    async def get_tenant(self, tenant_id: TenantId) -> Tenant | None:
        return self.tenants.get(tenant_id)

    async def put_tenant(self, tenant: Tenant) -> None:
        self.tenants[tenant.tenant_id] = tenant

    async def get_workspace(self, workspace_id: WorkspaceId) -> Workspace | None:
        return self.workspaces.get(workspace_id)

    async def put_workspace(self, workspace: Workspace) -> None:
        self.workspaces[workspace.workspace_id] = workspace

    async def get_principal(self, principal_id: PrincipalId) -> Principal | None:
        return self.principals.get(principal_id)

    async def put_principal(self, principal: Principal) -> None:
        self.principals[principal.principal_id] = principal

    async def put_oidc_identity(self, issuer: str, subject: str, user_id: PrincipalId) -> None:
        if (issuer, subject) in self.oidc:
            raise ImmutableViolation(f"oidc identity already bound: {issuer}|{subject}")
        self.oidc[(issuer, subject)] = user_id

    async def get_oidc_identity(self, issuer: str, subject: str) -> PrincipalId | None:
        return self.oidc.get((issuer, subject))

    async def list_role_bindings_for_principal(
        self, principal_id: PrincipalId, scopes: tuple[RoleBindingScope, ...]
    ) -> tuple[RoleBinding, ...]:
        kinds = {type(scope) for scope in scopes}
        return tuple(
            b for b in self.bindings if b.principal_id == principal_id and type(b.scope) in kinds
        )

    async def put_role_binding(self, binding: RoleBinding) -> None:
        self.bindings.append(binding)

    async def put_service_token(self, token: ServiceToken) -> None:
        self.tokens[token.token_id] = token

    async def list_service_tokens_for_service_account(
        self, service_account_id: PrincipalId
    ) -> tuple[ServiceToken, ...]:
        return tuple(
            token
            for token in self.tokens.values()
            if token.service_account_id == service_account_id
        )

    async def revoke_service_token(
        self, token_id: ServiceTokenId, actor: PrincipalId, reason: str
    ) -> None:
        token = self.tokens[token_id]
        self.tokens[token_id] = replace(
            token,
            revoked_at=_NOW,
            revoked_by=actor,
            revoked_reason=reason,
        )


def _store() -> tuple[FakeAuthStore, AuthStoreProvider]:
    fake = FakeAuthStore()
    return fake, cast(AuthStoreProvider, fake)


_NOW = datetime(2025, 1, 1, tzinfo=UTC)


def test_resolve_dsn_prefers_explicit() -> None:
    env = {
        "CUSTOS_PG_DSN": "postgresql://u:p@explicit:5432/custos",
        "DATABASE_URL": "postgresql://u:p@fallback:5432/custos",
        "uri": "postgresql://u:p@cnpg:5432/custos",
    }
    assert resolve_dsn(env) == "postgresql://u:p@explicit:5432/custos"


def test_resolve_dsn_database_url_then_uri_fallback() -> None:
    assert (
        resolve_dsn({"DATABASE_URL": "postgresql://u:p@fallback:5432/custos"})
        == "postgresql://u:p@fallback:5432/custos"
    )
    assert (
        resolve_dsn({"uri": "postgresql://u:p@cnpg:5432/custos"})
        == "postgresql://u:p@cnpg:5432/custos"
    )


def test_resolve_dsn_none_when_absent() -> None:
    assert resolve_dsn({}) is None


def test_resolve_permission_paths_splits_colon_list() -> None:
    env = {
        "CUSTOS_AUTH_PERMISSIONS_PATHS": (
            "/opt/custos/permissions/auth-service.yaml:/opt/custos/permissions/catalog-service.yaml"
        )
    }
    assert resolve_permission_paths(env) == [
        "/opt/custos/permissions/auth-service.yaml",
        "/opt/custos/permissions/catalog-service.yaml",
    ]


def test_resolve_permission_paths_empty_when_unset() -> None:
    assert resolve_permission_paths({}) == []
    assert resolve_permission_paths({"CUSTOS_AUTH_PERMISSIONS_PATHS": ""}) == []


def test_resolve_permission_paths_trims_and_drops_blanks() -> None:
    env = {"CUSTOS_AUTH_PERMISSIONS_PATHS": "  /a.yaml : : /b.yaml  "}
    assert resolve_permission_paths(env) == ["/a.yaml", "/b.yaml"]


def test_resolve_bootstrap_admin_token_config_defaults_disabled() -> None:
    config = resolve_bootstrap_admin_token_config({})
    assert config.mode is BootstrapAdminMode.DISABLED
    assert config.token is None


def test_resolve_bootstrap_admin_token_config_requires_canonical_token() -> None:
    with pytest.raises(ValueError, match="canonical Custos service token"):
        resolve_bootstrap_admin_token_config(
            {ADMIN_TOKEN_MODE_ENV: "init", ADMIN_TOKEN_ENV: "not-a-token"}
        )


def test_resolve_bootstrap_admin_token_config_rejects_secret_when_disabled() -> None:
    plaintext, _ = mint_token()
    with pytest.raises(ValueError, match="must be absent"):
        resolve_bootstrap_admin_token_config({ADMIN_TOKEN_ENV: plaintext})


async def test_seed_platform_seeds_everything() -> None:
    fake, store = _store()

    await seed_platform(store, admin_oidc_issuer=_ISSUER, admin_oidc_subject=_SUBJECT, now=_NOW)

    assert fake.permissions  # canonical permission registry upserted
    assert len(fake.roles) == len(BUILTIN_ROLES)
    assert DEFAULT_TENANT_ID in fake.tenants
    assert DEFAULT_WORKSPACE_ID in fake.workspaces
    assert fake.workspaces[DEFAULT_WORKSPACE_ID].tenant_id == DEFAULT_TENANT_ID

    # Admin: a user is provisioned, its OIDC identity is linked, and it holds a
    # single global platform-admin grant.
    admin_id = fake.oidc[(_ISSUER, _SUBJECT)]
    assert admin_id in fake.principals
    admin_bindings = [b for b in fake.bindings if b.role_id == ROLE_PLATFORM_ADMIN]
    assert len(admin_bindings) == 1
    assert isinstance(admin_bindings[0].scope, GlobalScope)
    assert admin_bindings[0].principal_id == admin_id


async def test_seed_platform_is_idempotent() -> None:
    fake, store = _store()

    await seed_platform(store, admin_oidc_issuer=_ISSUER, admin_oidc_subject=_SUBJECT, now=_NOW)
    tenant_created_at = fake.tenants[DEFAULT_TENANT_ID].created_at

    later = datetime(2025, 6, 1, tzinfo=UTC)
    await seed_platform(store, admin_oidc_issuer=_ISSUER, admin_oidc_subject=_SUBJECT, now=later)

    # No duplicated rows; the default tenant keeps its original created_at.
    assert len(fake.roles) == len(BUILTIN_ROLES)
    assert len(fake.tenants) == 1
    assert len(fake.workspaces) == 1
    assert len(fake.principals) == 1
    assert len(fake.oidc) == 1
    assert len([b for b in fake.bindings if b.role_id == ROLE_PLATFORM_ADMIN]) == 1
    assert fake.tenants[DEFAULT_TENANT_ID].created_at == tenant_created_at


async def test_seed_platform_initializes_bootstrap_admin_token() -> None:
    fake, store = _store()
    plaintext, expected_hash = mint_token()
    config = BootstrapAdminTokenConfig(mode=BootstrapAdminMode.INIT, token=plaintext)

    await seed_platform(
        store,
        admin_oidc_issuer=None,
        admin_oidc_subject=None,
        now=_NOW,
        bootstrap_admin_token=config,
    )

    principal = fake.principals[config.principal_id]
    assert principal.kind == "serviceAccount"
    assert len(fake.tokens) == 1
    token = next(iter(fake.tokens.values()))
    assert token.hash == expected_hash
    assert plaintext not in repr(token)
    assert any(
        binding.principal_id == config.principal_id
        and binding.role_id == ROLE_PLATFORM_ADMIN
        and isinstance(binding.scope, GlobalScope)
        for binding in fake.bindings
    )


async def test_seed_platform_rejects_replayed_init() -> None:
    fake, store = _store()
    first, _ = mint_token()
    config = BootstrapAdminTokenConfig(mode=BootstrapAdminMode.INIT, token=first)
    await seed_platform(
        store,
        admin_oidc_issuer=None,
        admin_oidc_subject=None,
        now=_NOW,
        bootstrap_admin_token=config,
    )

    second, _ = mint_token()
    with pytest.raises(ValueError, match="already exists"):
        await seed_platform(
            store,
            admin_oidc_issuer=None,
            admin_oidc_subject=None,
            now=_NOW,
            bootstrap_admin_token=replace(config, token=second),
        )
    assert len(fake.tokens) == 1


async def test_seed_platform_recovers_bootstrap_admin_token() -> None:
    fake, store = _store()
    first, _ = mint_token()
    init = BootstrapAdminTokenConfig(mode=BootstrapAdminMode.INIT, token=first)
    await seed_platform(
        store,
        admin_oidc_issuer=None,
        admin_oidc_subject=None,
        now=_NOW,
        bootstrap_admin_token=init,
    )

    replacement, replacement_hash = mint_token()
    await seed_platform(
        store,
        admin_oidc_issuer=None,
        admin_oidc_subject=None,
        now=_NOW,
        bootstrap_admin_token=replace(
            init,
            mode=BootstrapAdminMode.RECOVER,
            token=replacement,
        ),
    )

    assert len(fake.tokens) == 2
    assert sum(token.revoked_at is None for token in fake.tokens.values()) == 1
    assert any(
        token.hash == replacement_hash and token.revoked_at is None
        for token in fake.tokens.values()
    )
    assert all(
        token.hash != hash_token(first) or token.revoked_at for token in fake.tokens.values()
    )


async def test_seed_platform_recovery_requires_existing_service_account() -> None:
    fake, store = _store()
    plaintext, _ = mint_token()
    config = BootstrapAdminTokenConfig(mode=BootstrapAdminMode.RECOVER, token=plaintext)

    with pytest.raises(ValueError, match="is not a service account"):
        await seed_platform(
            store,
            admin_oidc_issuer=None,
            admin_oidc_subject=None,
            now=_NOW,
            bootstrap_admin_token=config,
        )
    assert fake.tokens == {}


async def test_seed_platform_disabled_does_not_create_bootstrap_credential() -> None:
    fake, store = _store()
    await seed_platform(
        store,
        admin_oidc_issuer=None,
        admin_oidc_subject=None,
        now=_NOW,
        bootstrap_admin_token=BootstrapAdminTokenConfig(
            mode=BootstrapAdminMode.DISABLED,
            token=None,
        ),
    )
    assert fake.principals == {}
    assert fake.tokens == {}


@pytest.mark.parametrize(
    ("issuer", "subject"),
    [(None, None), (_ISSUER, None), (None, _SUBJECT)],
)
async def test_seed_platform_without_admin_skips_principal_and_binding(
    issuer: str | None, subject: str | None
) -> None:
    fake, store = _store()

    await seed_platform(store, admin_oidc_issuer=issuer, admin_oidc_subject=subject, now=_NOW)

    assert fake.permissions
    assert len(fake.roles) == len(BUILTIN_ROLES)
    assert DEFAULT_TENANT_ID in fake.tenants
    assert DEFAULT_WORKSPACE_ID in fake.workspaces
    assert fake.principals == {}
    assert fake.oidc == {}
    assert fake.bindings == []


async def test_seed_platform_forwards_permission_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The resolved per-service paths reach the registry seeder verbatim."""
    _, store = _store()
    captured: dict[str, object] = {}

    import custos_bootstrap.__main__ as bootstrap_main

    async def _fake_seed(auth_store: object, *, paths: object, roles: object) -> dict[str, object]:
        captured["paths"] = paths
        return {}

    monkeypatch.setattr(bootstrap_main, "seed_permissions_and_validate_roles", _fake_seed)

    paths = ["/opt/custos/permissions/auth-service.yaml"]
    await seed_platform(
        store,
        admin_oidc_issuer=None,
        admin_oidc_subject=None,
        now=_NOW,
        permission_paths=paths,
    )

    assert captured["paths"] == paths


async def test_seed_platform_defaults_to_bundled_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Omitting ``permission_paths`` keeps the empty bundled-fallback contract."""
    _, store = _store()
    captured: dict[str, object] = {}

    import custos_bootstrap.__main__ as bootstrap_main

    async def _fake_seed(auth_store: object, *, paths: object, roles: object) -> dict[str, object]:
        captured["paths"] = paths
        return {}

    monkeypatch.setattr(bootstrap_main, "seed_permissions_and_validate_roles", _fake_seed)

    await seed_platform(
        store,
        admin_oidc_issuer=None,
        admin_oidc_subject=None,
        now=_NOW,
    )

    assert list(cast("tuple[str, ...]", captured["paths"])) == []


def test_main_missing_dsn_returns_nonzero(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("CUSTOS_PG_DSN", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("uri", raising=False)

    assert main([]) == 1
    assert "no Postgres DSN available" in capsys.readouterr().err


def test_main_success_seeds_via_adapter(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CUSTOS_PG_DSN", "postgresql://u:p@db:5432/custos")
    monkeypatch.delenv("CUSTOS_BOOTSTRAP_ADMIN_OIDC_ISSUER", raising=False)
    monkeypatch.delenv("CUSTOS_BOOTSTRAP_ADMIN_OIDC_SUBJECT", raising=False)
    fake = FakeAuthStore()

    import custos_pg.adapters.auth as auth_adapter

    monkeypatch.setattr(auth_adapter, "make_adapter", lambda: fake)

    assert main(["--admin-oidc-issuer", _ISSUER, "--admin-oidc-subject", _SUBJECT]) == 0
    admin_id = fake.oidc[(_ISSUER, _SUBJECT)]
    assert admin_id in fake.principals
    assert any(b.role_id == ROLE_PLATFORM_ADMIN for b in fake.bindings)
