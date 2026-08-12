# Auth Service Change 003: First-Admin Service-Token Bootstrap

Date: 2026-08-11
Status: Approved
Tracking issue: #980

## Problem

A fresh installation seeds the default tenant, workspace, roles, and optional OIDC administrator, but it does not establish the first bearer credential. The evaluation guides require a pre-provisioned `custos_...` platform-admin token, while the APIs that create service accounts and mint tokens already require an authenticated administrator.

## Decision

The Kubernetes/operator administrative boundary is the root of trust for the first credential. Custos does not expose an unauthenticated HTTP bootstrap endpoint.

`custosctl bootstrap-admin init` generates a normal Custos service token on the operator's machine and writes it directly to a short-lived Kubernetes Secret. The bootstrap Job reads the Secret, stores only the canonical token hash, and creates a dedicated `custos-bootstrap-admin` service account in `workspace-default` with a global `platform.admin` binding.

The dedicated service account is distinct from the optional OIDC bootstrap user. This preserves the existing invariant that service tokens belong only to service accounts and avoids an AuthStore schema migration.

## Secret Contract

| Setting | Default | Meaning |
|---|---|---|
| `bootstrap.adminToken.secretName` | empty | Existing Kubernetes Secret containing the plaintext token. |
| `bootstrap.adminToken.secretKey` | `token` | Secret data key. |
| `bootstrap.adminToken.mode` | `disabled` | `disabled`, `init`, or `recover`. |
| `bootstrap.adminToken.principalId` | `custos-bootstrap-admin` | Dedicated service-account principal. |
| `bootstrap.adminToken.workspaceId` | `workspace-default` | Owning workspace. |
| `bootstrap.adminToken.ttlSeconds` | `7776000` | Token lifetime recorded in AuthStore. |

Plaintext is never accepted as a Helm value and never appears in rendered manifests, Helm release metadata, logs, audit payloads, or CI summaries.

## State Transitions

### Initial bootstrap

1. Validate the token with the canonical `looks_like_custos_token` helper.
2. Ensure the configured workspace exists.
3. If the bootstrap service account already exists, reject `init`; normal install and upgrade runs with mode `disabled` remain idempotent.
4. Create the service account and global platform-admin binding.
5. Insert a normal `ServiceToken` row containing only `hash_token(token)`.
6. Emit a non-secret completion signal. `custosctl` verifies the token through the gateway and deletes the temporary Secret by default.

### Recovery

Recovery requires explicit `recover` mode and Kubernetes-admin access. The Job requires the dedicated service account to exist, revokes every live token owned by it, and inserts the replacement token hash. Normal installation and upgrade never enter recovery implicitly.

## Idempotency and Failure Behavior

- `disabled` mode never reads a token Secret or mutates bootstrap credentials.
- `init` fails if the bootstrap service account already exists, preventing silent replacement during retries or upgrades.
- Missing Secret data, malformed tokens, invalid TTLs, partial configuration, and invalid mode transitions fail closed before credential mutation.
- The Job does not delete the Secret; `custosctl` deletes it only after gateway verification succeeds. Operators using Helm directly remove it explicitly.

## Deployment Modes

- Local/evaluation: `custosctl` generates, installs, verifies, and removes the temporary Secret.
- Connected/HA: operators may pre-create the Secret or synchronize it through External Secrets; Helm receives only the name/key reference.
- Air-gapped: the token is generated offline and placed in the same Kubernetes Secret before installation.

## Audit

The implementation emits `bootstrap.admin-token.created`, `bootstrap.admin-token.recovered`, and `bootstrap.admin-token.rejected` without including plaintext or hashes. Until bootstrap has a MetadataStore transaction, the Job emits structured events for collection by the platform audit pipeline; the durable auth rows remain the authorization source of truth.

## Security Invariants

- No static or default credential.
- Plaintext is never persisted in PostgreSQL.
- Bootstrap is unavailable after initialization except through explicit recovery.
- Development call-context shims are not a supported bootstrap mechanism.
- Token retrieval is impossible; loss requires recovery.

## Rejected Alternative

A one-time unauthenticated HTTP exchange endpoint was rejected because it adds a remotely reachable pre-authentication surface and duplicates lifecycle logic already protected by the Kubernetes/operator boundary.