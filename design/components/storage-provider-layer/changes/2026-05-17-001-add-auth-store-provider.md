# Change: add-auth-store-provider

Date: 2026-05-17
Type: component-design
Component: storage-provider-layer
Sequence: 001
GitHub Issue: #66
Status: open

## Summary

Adds a fifth provider interface to the Storage Provider Layer: `AuthStoreProvider`. It persists the identity, tenancy, and RBAC entities owned by Auth Service (COMP-002): `Tenant`, `Workspace`, `Principal` (User / ServiceAccount discriminator), `OidcIdentity`, `ServiceToken`, `Role`, `Permission`, `RoleBinding`. The interface is exempt from the workspace-scoping middleware because its rows define workspaces rather than living inside one.

## Context

The COMP-002 Auth Service design session (2026-05-17) locked the decision that auth state must reach storage through the SPL rather than via a separate persistence layer or direct database access. The user explicitly chose "Add a dedicated interface — update the SPL" over reusing `MetadataStoreProvider` or owning persistence inside Auth Service. This change implements that decision.

## Impact

- **SPL contract**: grows from four to five interfaces. Adapters that today implement only the workspace-scoped four are unaffected; an `AuthStoreProvider` implementation is required for the platform to start (the migration runner gates on declared revisions for active interfaces).
- **Workspace-scoping middleware**: explicitly exempts `AuthStoreProvider`. The middleware contract clarifies that the exemption applies only to this interface and that Auth Service is the sole caller.
- **Migration runner**: gains a new schema revision `AuthStoreProvider:1` covering tenants, workspaces, principals, OIDC identities, service tokens, roles, permissions, and role bindings.
- **Configuration**: new required variable `CUSTOS_AUTH_STORE` (default `postgres`).
- **Audit outbox**: `AuthStoreProvider` participates in the same outbox transaction model as `MetadataStoreProvider`; auth mutations and their audit events commit together.
- **Auth Service**: now references this interface as its persistence boundary; no auth-service-specific schema lives outside SPL.

## Before / After

**Before**: SPL defined four interfaces (`DefinitionStoreProvider`, `CatalogStoreProvider`, `MetadataStoreProvider`, `ArtifactStoreProvider`). Auth Service persistence was unspecified.

**After**: SPL defines five interfaces. The new `AuthStoreProvider` owns the auth entity set with explicit methods for tenant/workspace lifecycle, principal CRUD, OIDC identity binding, service-token mint/verify/revoke, permission upsert (called at startup from each component's `permissions.yaml`), role lookup, and role-binding mutation. The interface takes no leading `workspaceId` argument; scoping is encoded on individual rows (`RoleBinding.scope`) where applicable.

## Files Changed

- `design/components/storage-provider-layer/design.md` — added `AuthStoreProvider` to interface inventory, internal structure diagram, entity-to-interface map, public interface section, tenancy clarification, workspace-scoping middleware exemption, migration runner table, configuration table, change history.
- `design/components/auth-service/design.md` — added in the parallel auth-service session; references this new interface as its sole persistence boundary.

## Open Follow-ups

- Conformance test suite skeleton (existing SPL TODO) must cover `AuthStoreProvider`.
- Adapter implementation cost for any non-Postgres backend that wants to host auth state.
