# Change: impl-phase-k-integration-tests

Date: 2026-05-27
Type: component-design
Component: auth-service
Sequence: 005
GitHub Issue: #263
Status: closed

## Summary

Phase K (M2) of the auth-service implementation plan (AS-IMPL-028)
landed. A new `tests/integration/` suite exercises the full FastAPI
surface against a live Postgres — locally via
`testcontainers[postgres]>=4.0`, in CI via a `postgres:15-alpine`
service container under the new `auth-service-integration` GitHub
Actions job. Five round-trip tests pin the contracts highlighted by
the issue (mint → verify, grant → authorize → revoke, sign-callctx →
verify-callctx, JWKS rotation overlap, token-revoked cache eviction).
Default `pytest -q` continues to exclude the integration suite via the
existing `addopts = ["-m", "not integration"]` switch shipped in
sequence 004. Refs #267 (auth-service implementation tracker).

The integration suite immediately surfaced a latent bug in
`PgAuthAdapter.put_role_binding`: the adapter wrote a Python `dict`
into a `JSONB` column without registering a JSONB codec, so the call
blew up with `asyncpg.DataError: expected str, got dict` against a
real Postgres. The in-memory fakes used by the unit suite never saw
the column type, which is exactly the contract gap AS-IMPL-028 was
designed to catch. Fixed by adding an optional `init` hook to
`custos_pg.pool.LazyPool` and registering a dict ↔ JSONB codec from
`custos_auth.providers.load_providers`.

## Before

* No `tests/integration/` directory on auth-service. The unit suite
  (`tests/`) covered every route + dependency at 90 %+ but used
  `FakeAuthAdapter` / `FakeMetadataAdapter` exclusively, so the
  pgsql-only behaviours — FK enforcement on `role_binding.bound_by`
  and `service_token.revoked_by`, real-loop lifespan ordering for
  `LocalBindingChangedBus` / `LocalTokenRevokedBus` cache eviction,
  the JWKS overlap window after `KeyRing.rotate`, the `BYTEA`/`JSONB`
  asyncpg encoder paths — never executed in CI.
* `custos_pg.pool.LazyPool` accepted only `dsn` + size knobs. There
  was no surface for the consumer to register asyncpg type codecs,
  so every adapter that wrote a Python `dict` into a `JSONB` column
  had to handle the encoding itself.
* `PgAuthAdapter.put_role_binding` passed `self._scope_to_json(...)`
  (a `dict`) directly as the `$4` parameter for a `JSONB` column.
  This pattern is invisible to unit tests (the in-memory fakes never
  type-check the value) and to the `custos-postgres` repo's own
  conformance suite (which doesn't exercise role-binding writes).
* `.github/workflows/python-services.yml` had a `catalog-service-integration`
  job but no equivalent for auth-service, so Phase K M2's "green
  pre-merge gate" requirement was not met.

## After

### `tests/integration/__init__.py` + `tests/integration/conftest.py` (new)

* `_postgres_dsn` (session) — reads `CUSTOS_AUTH_PG_DSN` in CI;
  falls back to a `PostgresContainer("postgres:15-alpine")` locally
  via `testcontainers[postgres]>=4.0`. Skips cleanly when neither
  path is available so a contributor without Docker still sees a
  green `pytest -q` run (which excludes `-m integration`).
* `_reset_and_migrate(dsn)` — drops `auth`, `custos_state`, and
  `custos_meta` schemas, then calls `PgAuthAdapter.apply_pending()`
  and `PgMetadataAdapter.apply_pending()` for a clean per-test slate.
  Also seeds a `user-1` user principal so foreign-key constraints on
  `role_binding.bound_by` / `service_token.revoked_by` are satisfied
  when the dev-shim callctx middleware accepts the test's
  `principal_id="user-1"` at face value.
* `pg_dsn` — per-test fixture that runs the reset in a fresh
  `asyncio.run(...)` so the FastAPI lifespan that follows owns the
  pool used by the routes (no cross-loop asyncpg hazards).
* `client` — builds `settings = load_settings(_integration_env(pg_dsn))`,
  `providers = load_providers(settings)`, and wraps a real
  `create_app(...)` in `TestClient(...)`. The lifespan opens the
  `LazyPool`s inside `TestClient`'s loop.
* `_integration_env` — points both `CUSTOS_AUTH_STORE_DSN` /
  `CUSTOS_AUTH_METADATA_STORE_DSN` at the same DB, sets the
  token-sweeper interval and call-context key rotation period to `0`
  (no background loops to race with assertions), and disables OIDC
  so the suite focuses on the service-token / call-context surface.
* Header helpers `callctx_header` / `platform_admin_header` /
  `workspace_admin_header` mirror the unit suite's
  `tests/conftest.callctx_header` so test bodies read the same.

### `tests/integration/test_round_trip.py` (new)

| # | Test | Surface verified |
|---|---|---|
| 1 | `test_mint_then_verify_round_trip` | `POST /v1/service-accounts/{sa}/tokens` → `POST /v1/auth/verify` (REST) and `POST /rpc/authn.verifyToken` (RPC); unknown-token closed-set 401 / `principal=None` |
| 2 | `test_grant_authorize_revoke_round_trip` | `POST /v1/workspaces/{ws}/role-bindings` → `POST /rpc/authz.authorize` (allowed=True, cache hit on retry) → `DELETE /v1/workspaces/{ws}/role-bindings/{id}` → `POST /rpc/authz.authorize` (allowed=False — `LocalBindingChangedBus` evicted the warmed cache entry) |
| 3 | `test_callctx_sign_then_verify_round_trip` | `POST /rpc/callctx.sign` → `POST /rpc/callctx.verify`; every signed claim (principal_id, workspace_id, caller_component, kid, jti, iat, exp) round-trips |
| 4 | `test_jwks_rotation_overlap` | Pre-rotation JWKS = `{kid_old}`; rotate via `app.state.call_context_key_ring.rotate(SigningKey.generate())` + `resolver.set_key(...)`; post-rotation JWKS = `[kid_new, kid_old]`; tokens minted under either kid still verify; new mints use new kid |
| 5 | `test_token_revoke_evicts_authn_cache` | `POST /v1/auth/verify` (warms cache) → second verify (cache hit) → `DELETE /v1/tokens/{token_id}` → `POST /v1/auth/verify` returns 401 (`LocalTokenRevokedBus` evicted the cache entry) and RPC returns `principal=None` |

Acceptance criterion from the issue ("integration tests excluded from
default `pytest -q` via `addopts`") is met by the existing
`addopts = ["-m", "not integration"]` line in
`pyproject.toml [tool.pytest.ini_options]`; the suite ran 5/5 green
against `postgres:15-alpine` locally in 2.86s and the unit suite
remains 572 passed / 5 deselected at 97.94 % coverage.

### `custos_pg.pool.LazyPool` — new `init` hook

* `LazyPool.__init__` now accepts an optional
  `init: Callable[[Connection], Awaitable[None]] | None = None`.
  When supplied it is forwarded verbatim to `asyncpg.create_pool(init=...)`,
  which invokes it once per pooled connection. Backward-compatible —
  existing callers (`custos-postgres` conformance fixtures, catalog
  / definition adapters) keep the prior keyword-only positional
  surface untouched.
* The new `ConnectionInit` type alias is exported from `custos_pg.pool`
  so downstream services have a typed handle for codec-registration
  callbacks.

### `custos_auth.providers.load_providers` — JSONB codec wired

* `load_providers` now constructs both `LazyPool`s with an `init`
  that runs `await conn.set_type_codec("jsonb", encoder=json.dumps,
  decoder=json.loads, schema="pg_catalog")` on every checked-out
  connection. This unblocks `PgAuthAdapter.put_role_binding` (the
  immediate bug surfaced by the integration suite) and aligns the
  auth-service runtime with the catalog/definition adapters which
  use explicit `::jsonb` casts in their SQL. The codec is registered
  symmetrically so `_json_to_scope` keeps receiving a `dict` rather
  than a raw JSON string.

### `.github/workflows/python-services.yml` — new `auth-service-integration` job

Mirrors the existing `catalog-service-integration` template:

* `needs: auth-service`, `if: always() && (needs.auth-service.result == 'success' || needs.auth-service.result == 'skipped')`.
* `services.postgres`: `postgres:15-alpine` with the standard
  `pg_isready` healthcheck and a published `5432:5432` port mapping.
* Installs `src/libs/storage-provider-layer[dev]` and
  `src/libs/custos-postgres[dev]` from path so the integration job
  exercises the same SPL + adapter wiring as the unit suite.
* Single pytest invocation: `pytest tests/integration -v -m integration --tb=short --no-cov`.
* `CUSTOS_AUTH_PG_DSN=postgresql://testuser:testpass@localhost:5432/custos_auth_test`.

### `src/services/auth-service/pyproject.toml`

* `testcontainers[postgres]>=4.0` added to
  `[project.optional-dependencies].dev`. Comment notes it is a
  local-dev fallback only; CI uses the service container.

## Impact

* **Pre-merge confidence**: every route now has at least one assertion
  fired against a live `postgres:15-alpine`. Bugs that fakes hide
  (JSONB encoding, FK constraints, cross-loop pool ownership) are
  caught before merge.
* **Bug fix**: `PgAuthAdapter.put_role_binding` is functional against
  Postgres — Phase J's audit pipeline and Phase K M2's
  binding-changed cache invalidation now work end-to-end without the
  `expected str, got dict` blowup. No existing call sites had to
  change.
* **Public surface**: `custos_pg.pool.LazyPool` gains a backward-
  compatible `init` keyword; the new `ConnectionInit` alias is
  exported so other services can register their own codecs.
* **CI cost**: ~30 s added per PR (sequential to the `auth-service`
  unit lane, mirrors catalog-service's ratio).

## Verified

* `pytest tests/integration -v -m integration --tb=short --no-cov`:
  5 passed against `testcontainers[postgres]>=4.0`
  (`postgres:15-alpine`) in 2.86 s.
* `pytest -q --cov=custos_auth --cov-fail-under=90`:
  572 passed / 5 deselected at 97.94 % total coverage.
* `ruff check . && ruff format --check .` clean.
* `mypy src tests` and `mypy --python-version 3.11 src tests` clean
  on 89 source files.
* `custos-postgres` unit suite (`tests/test_unit.py`):
  34 passed against the auth-service venv.

## Related Requirements

* `REQ-051` — auth-service must verify service tokens against a
  durable store.
* `REQ-052` — auth-service must invalidate cached authorize decisions
  when a binding changes.

## Related Change Records

* `2026-05-26-004-impl-phase-k-coverage-gate.md` — Phase K M1
  (#262). Established the `-m "not integration"` `addopts` switch
  this change relies on for default `pytest -q` runs.
* `2026-05-25-003-impl-phase-j-observability.md` — Phase J. The
  audit pipeline and `LocalBindingChangedBus` / `LocalTokenRevokedBus`
  exercised by tests 2 and 5 land here.
