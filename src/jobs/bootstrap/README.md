# Custos bootstrap Job (`custos-bootstrap`)

The idempotent **post-install / post-upgrade** Helm hook image that seeds a
freshly migrated Custos database into a *usable* platform. It runs after the
migration hook ([`src/jobs/migrate`](../migrate/README.md)) and reuses the
canonical auth domain so permissions and roles have a single source of truth.

On every run it:

1. upserts every component's declared permission (the bundled auth-service
   permission registry),
2. seeds the canonical built-in roles,
3. creates the default tenant (`tenant-default`) and workspace
   (`workspace-default`),
4. provisions the initial admin, links their OIDC identity, and grants them the
   platform-admin role.

Each step is keyed on the entity's identity, so re-running (e.g. on
`helm upgrade`, where the hook also runs as `post-upgrade`) converges without
duplicating rows.

## Configuration

| Variable | Purpose |
| --- | --- |
| `CUSTOS_PG_DSN` | libpq DSN for the Custos Postgres cluster. Falls back to `DATABASE_URL` then `uri` (the CloudNativePG application secret exposes the connection string under `uri`). |
| `CUSTOS_BOOTSTRAP_ADMIN_OIDC_ISSUER` | OIDC issuer URL of the initial admin to pre-bind. **Optional** — must be set together with the subject; when either is absent the seeder still seeds permissions/roles/tenant/workspace and exits `0`. |
| `CUSTOS_BOOTSTRAP_ADMIN_OIDC_SUBJECT` | OIDC `sub` claim of the initial admin to pre-bind. **Optional** (see above). |

When both OIDC values are set the seeder provisions an internal user (a stable
id derived from the issuer + subject), links the `(issuer, subject)` identity to
it so the admin can authenticate immediately, and grants it platform-admin.

## Usage

```bash
# Seed the platform (the image default ENTRYPOINT).
custos-bootstrap

# Override the admin OIDC identity (otherwise read from the env vars).
custos-bootstrap \
  --admin-oidc-issuer https://login.example.com \
  --admin-oidc-subject <sub-claim>
```

## Development

```bash
pip install -e src/libs/storage-provider-layer[dev]
pip install -e src/libs/custos-postgres[dev]
pip install -e src/services/auth-service
pip install -e src/jobs/bootstrap[dev]

ruff check . && ruff format --check . && mypy src tests && pytest -q
```
