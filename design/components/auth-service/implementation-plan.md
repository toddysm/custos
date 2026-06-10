# `auth-service` Implementation Plan — AS-IMPL-032

> Source of truth: `design/components/auth-service/` and `design/architecture/`.
> Owned by the `implement-component` skill. This plan covers a single residual
> task filed as a follow-up to #856; the substantive permission-registry
> aggregation work shipped on `main` via #867 / #868 (Option 2).

## Summary

Issue #856 asked the platform to load the **same** permission-registry surface
at install-time seeding and at auth-service runtime, instead of the bootstrap
seeder always falling back to the bundled platform-M1 aggregate. Option 2 —
have each component ship its own `permissions.yaml` and have auth-service
aggregate them via the colon-separated `CUSTOS_AUTH_PERMISSIONS_PATHS` — was
chosen and landed in #867 / #868 (commit `45022bc`). The only residual gap was
the **bootstrap Job**: its seeder still called
`seed_permissions_and_validate_roles(..., paths=[])`, so the post-install seed
diverged from the running auth-service whenever the per-service files differed
from the bundled aggregate.

AS-IMPL-032 closes that gap: the bootstrap seeder now reads
`CUSTOS_AUTH_PERMISSIONS_PATHS` (reusing `custos_auth.settings.ENV_PERMISSIONS_PATHS`),
the bootstrap image bakes in the six component `permissions.yaml` files, and the
umbrella chart wires the env var by default. An empty value preserves the
bundled-aggregate fallback for dev/test. The permission-drift guard
(`tests/test_permissions_drift.py`) keeps the bundled aggregate and the union of
per-service files in lockstep, so the seeded data is unchanged today — this is a
correctness/consistency fix that prevents future drift, not a data change.

## Conventions

- Task prefix: `AS-IMPL-`.
- One task = one PR = one GitHub issue.
- Quality gate from `src/jobs/bootstrap`: `ruff format . && ruff check . && mypy src tests && pytest -q`.
- Helm render assertions from `tests/helm`: `pytest -q`.

## Decisions

- **Single PR, single task.** The change is small and cohesive (seeder + image +
  chart + tests); it ships as one PR (#870) closing one issue (#869), per the
  user's instruction to "file just a single task".
- **Chart default = the six baked-in paths.** `bootstrap.permissionsPaths`
  defaults to the colon-joined
  `/opt/custos/permissions/{auth,catalog,workflow,trigger,connector,observability-audit}-service.yaml`,
  mirroring the auth-service pod's image layout. Clearing it (`--set
  bootstrap.permissionsPaths=`) omits the env var so the seeder falls back to the
  bundled aggregate.
- **Reuse, don't redefine.** The seeder imports `ENV_PERMISSIONS_PATHS` and the
  same colon-split semantics from `custos_auth.settings` rather than hard-coding
  the env var name, keeping the bootstrap Job and auth-service parsing identical.

## Task

### AS-IMPL-032 (#869) — wire the bootstrap seeder through `CUSTOS_AUTH_PERMISSIONS_PATHS`

**Goal.** Make the bootstrap seed load the same aggregated permission registry as
the running auth-service.

**Changes.**

- `src/jobs/bootstrap/src/custos_bootstrap/__main__.py` — add
  `resolve_permission_paths(env)` (reads `CUSTOS_AUTH_PERMISSIONS_PATHS`, splits
  on `:`, trims, drops empties); thread `permission_paths` through
  `seed_platform(...)` into `seed_permissions_and_validate_roles(..., paths=...)`
  (was `paths=[]`); `_run` passes `resolve_permission_paths(os.environ)`.
- `src/jobs/bootstrap/src/custos_bootstrap/__init__.py` — export
  `resolve_permission_paths`.
- `src/jobs/bootstrap/Dockerfile` — bake the six component `permissions.yaml`
  files into `/opt/custos/permissions/<svc>.yaml`.
- `deploy/helm/custos/templates/bootstrap-job.yaml`, `values.yaml` — set
  `CUSTOS_AUTH_PERMISSIONS_PATHS` from `bootstrap.permissionsPaths` (default =
  six baked-in paths; empty omits the env var).
- `src/jobs/bootstrap/tests/test_bootstrap.py` — unit tests for path parsing and
  seeder forwarding (including the bundled-fallback default).
- `tests/helm/test_bootstrap_render.py` — render assertions across all four
  profiles (default wires the env var; cleared value omits it).

**Acceptance.**

- Bootstrap quality gate green; helm render tests green.
- Default render sets `CUSTOS_AUTH_PERMISSIONS_PATHS` to the six paths on all
  profiles; cleared value omits it.
- Permission-drift guard unaffected (seeded data unchanged today).

**Status.** Done — PR #870, closes #869.
