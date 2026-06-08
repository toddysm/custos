# Upgrade with a breaking schema change — operator runbook

This runbook covers a Custos `helm upgrade` that carries a **breaking storage
schema change** — one where the new release requires a schema revision that the
currently running adapters do not declare. For routine, forward-compatible
upgrades you do **not** need this procedure; follow the standard upgrade in
[reference-deployment.md](../../design/architecture/reference-deployment.md#upgrade-flow)
instead.

> **When does an upgrade count as "breaking"?** A release is breaking when the
> new schema is **not** readable by the previous code (columns dropped/renamed,
> NOT-NULL added without default, types narrowed). Custos migrations are
> **forward-only**: the migrate Job never rolls a schema back. That makes the
> migration the point of no return for a clean rollback, which is what this
> runbook plans around.

## How Custos applies migrations

- The chart ships a `custos-migrate` Job registered as a Helm
  `pre-install,pre-upgrade` hook (weight `0`), so it runs **before** any new
  component Pod starts.
- The Job calls the Storage Provider Layer (SPL) migration runner, which:
  - **applies** pending forward migrations (`apply_pending()`), then
  - **strict-checks** revisions (`check_revisions()`): every component refuses
    to start at runtime if a required schema revision is missing.
- If the migrate Job fails, Helm aborts the upgrade and the existing release
  keeps running against the **unchanged** database.

Because the migration runs first and is forward-only, the order of risk is:

1. migrate Job runs → 2. schema is now new → 3. old Pods are still serving the
**old** code against the **new** schema for the few seconds of the rolling
update.

For a forward-compatible change, step 3 is harmless. For a **breaking** change,
old code may error against the new schema during that window — hence the
maintenance window and read-only fallback below.

## Before you start

- [ ] Read the release notes / `CHANGELOG` and confirm whether the target
      version is flagged as a breaking schema change.
- [ ] Confirm you have a **fresh, verified database backup** (CNPG base backup +
      WAL, or your managed-Postgres snapshot). This is your only rollback path
      once the migration runs.
- [ ] Note the current chart version and image digests so you can reinstall the
      exact prior release if you must roll back:
      `helm get metadata custos -n custos-system`.
- [ ] Schedule a maintenance window (see sizing below) and announce it to API
      consumers.
- [ ] Verify the cluster is healthy: all Custos Pods `Ready`, no pending
      `ExternalSecret`s, Postgres primary healthy.

## Maintenance window

Size the window as: `migration duration + rolling-update duration + smoke-test
buffer`. Estimate the migration duration from a **rehearsal against a copy of
production data**, never from an empty dev database. A safe default for a first
run is **30 minutes**; large audit/outbox tables can push the migration alone
into the tens of minutes.

During the window the platform is in **reduced-availability / read-only
fallback** mode (next section). Plan the window for a low-traffic period.

## Read-only fallback during the migration window

Custos has no built-in read-only mode for M1, so the operator establishes it at
the edge. Pick the option that matches your ingress:

**Option A — pause writes at the Gateway (recommended).** Scale the write-path
components to zero so no new mutations land while the schema changes, leaving
read traffic served from the (still-old) replicas until the rolling update:

```bash
# Quiesce the write path just before running the upgrade.
kubectl -n custos-system scale deploy \
  custos-workflow-service custos-trigger-service custos-connector-service \
  custos-activity-runtime-manager --replicas=0
```

**Option B — drop write traffic at the Gateway API.** If you front Custos with
your own gateway/ingress, return `503` for mutating verbs (`POST/PUT/PATCH/
DELETE`) on the Custos routes for the duration of the window, while continuing to
serve `GET`.

In both cases, **reads remain available** through the API Gateway and Catalog
read paths. The audit outbox drainer continues to flush its backlog; watch the
**Custos — Audit Drainer Lag** dashboard (`custos_obs_audit_outbox_lag_rows`)
and let it settle toward zero before you cut writes back on.

## Upgrade procedure

1. **Snapshot/backup** the database and verify the backup is restorable.
2. **Engage the read-only fallback** (Option A or B above).
3. **Run the upgrade.** The migrate Job runs first as a pre-upgrade hook:

   ```bash
   helm upgrade custos deploy/helm/custos \
     -n custos-system \
     -f deploy/helm/custos/values-<profile>.yaml \
     --wait --timeout 30m
   ```

4. **Watch the migrate Job.** If it fails, the upgrade aborts with the database
   unchanged — go to [Rollback](#rollback-procedure):

   ```bash
   kubectl -n custos-system logs job/custos-migrate -f
   ```

5. **Let the rolling update finish.** Anti-affinity + PodDisruptionBudgets (HA)
   prevent simultaneous loss of all replicas. `kubectl rollout status` needs a
   named resource, so wait on each Custos Deployment in turn:

   ```bash
   for d in $(kubectl -n custos-system get deploy \
       -l app.kubernetes.io/instance=custos -o name); do
     kubectl -n custos-system rollout status "$d" --timeout=10m
   done
   ```

6. **Re-enable writes.** Scale the write-path components back to their configured
   replica counts (or remove the Gateway block from Option B):

   ```bash
   kubectl -n custos-system scale deploy \
     custos-workflow-service custos-trigger-service custos-connector-service \
     custos-activity-runtime-manager --replicas=<desired>
   ```

7. **Smoke test.** Run the bundled synthetic checks and confirm the dashboards
   are green:

   ```bash
   helm test custos -n custos-system
   ```

## Rollback procedure

Rollback depends on **whether the migration committed**, because migrations are
forward-only.

### Case 1 — migrate Job failed (schema unchanged)

The safe, common case. The pre-upgrade hook aborted before any new code rolled
out and the database is untouched.

1. Inspect the Job logs and fix the root cause (version mismatch, exhausted
   connections, insufficient privileges).
2. Re-run the **same** `helm upgrade`. The Job is idempotent and re-applies
   cleanly.
3. If you must abandon the upgrade entirely, the running release is already the
   old one — just lift the read-only fallback (step 6 above).

### Case 2 — migration committed but the new release is unhealthy

The schema is now new and forward-only, so you **cannot** simply
`helm rollback`: the old code would hit the new schema and the SPL strict
revision check would refuse to start. Recover by **restoring the database** to
the pre-upgrade backup and reinstalling the prior chart:

1. Keep the read-only fallback engaged (no new writes).
2. Restore Postgres from the backup taken in step 1 of the upgrade (CNPG
   point-in-time recovery to just before the migrate Job ran, or your managed
   snapshot restore).
3. Reinstall the **previous** chart version against the restored database. Run
   this from a checkout (or `helm pull` extraction) of the **previous** chart so
   `deploy/helm/custos` *is* the old chart — a local chart directory has no
   `--version` selector, so the version is whatever is on disk:

   ```bash
   # From the previous release's checkout / extracted chart package:
   helm upgrade custos deploy/helm/custos \
     -n custos-system \
     -f deploy/helm/custos/values-<profile>.yaml \
     --wait --timeout 30m
   ```

   If you publish the chart to an OCI/HTTP registry instead, pull a specific
   prior version directly with `helm upgrade custos <repo>/custos --version
   <previous-chart-version> ...`.

4. Lift the read-only fallback and smoke test.

> **Data written after the backup is lost on a Case 2 restore.** This is why the
> read-only fallback stays engaged across the whole window: with writes paused,
> the backup taken at step 1 stays current and a restore loses nothing.

## After the upgrade

- [ ] Confirm the **Custos — Components Overview** and **Custos — Audit Drainer
      Lag** dashboards are nominal (drainer lag trending to zero, no error-rate
      spikes).
- [ ] Confirm the bootstrap Job re-ran and reconciled permissions.
- [ ] Lift the maintenance window and notify consumers.
- [ ] Retain the pre-upgrade backup until the new release has soaked for at
      least one retention/backup cycle.

## Related

- [reference-deployment.md § Upgrade Flow](../../design/architecture/reference-deployment.md#upgrade-flow)
- [reference-deployment.md § Failure Modes](../../design/architecture/reference-deployment.md#failure-modes)
- Storage Provider Layer migration contract:
  [design/components/storage-provider-layer/design.md](../../design/components/storage-provider-layer/design.md)
- Grafana dashboard bundle: `deploy/helm/custos/dashboards/` (drainer-lag,
  audit-events, components).
