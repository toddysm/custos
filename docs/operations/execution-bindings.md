# Execution binding files and retained authorization

This is the deployment-input procedure for background execution identities.
It does not certify receiver effects, account grants or database migrations.
Use tested, file-aware binaries in Gateway, Trigger, Workflow and Activity
Runtime Manager before changing sources. Existing account authorization still
applies; these files contain references, never credentials.

## Limits and startup

Set `CUSTOS_EXECUTION_IDENTITY_ENABLED=true` and exactly one source:

| Source | Required configuration | UTF-8 byte limit |
| --- | --- | ---: |
| Inline | `CUSTOS_EXECUTION_BINDINGS` | 65,536 |
| File | Absolute `CUSTOS_EXECUTION_BINDINGS_FILE` and lowercase `CUSTOS_EXECUTION_BINDINGS_SHA256` | 1,000,000 |

Both use the same closed `bindings`/`defaults` JSON, 1,024-binding maximum and
120-character reference maximum. Multibyte text counts as bytes. The file
loader supports projected symlinks, rejects special files without blocking,
reads one bounded snapshot, verifies its digest and decodes strict UTF-8.
Invalid/missing/ambiguous sources abort enabled startup; there is no fallback.
Disabled mode ignores unused source settings.

The registry is immutable for a process's lifetime. Defaults choose new work
only; retained work resolves its original workspace, principal and binding.
Removing or rebinding that original reference fails closed. Rotate actual
credentials through the existing Secret/account mechanism, not this file.

## Prepare a fresh file deployment

Prerequisites: Helm 3.16.2, `kubectl` targeting the intended cluster, Python
3.11 or 3.12 with the repository's `custos-callctx` and Helm-test dependencies,
and operator permissions to create ConfigMaps, Dapr Configurations, Roles and
RoleBindings. Use an operator-owned controller Namespace protected from other
cleanup controllers. Namespace deletion is never part of ordinary uninstall
or bundle cleanup.
For a Helm-owned Namespace, both the owning release name and owning release
Namespace must match the intended deployment; partial Helm ownership metadata
or a same-named release in another Namespace is rejected.

Keep local registries, bundle artifacts, inventories and complete deployment
values under operator-controlled access. Never put credentials in any
reference field, print artifacts, or commit them. The helper does not read
Secret contents, but Helm release manifests/values can contain sensitive
operator configuration.
The bundle SHA-256 fingerprints registry bytes and reference-only policy
metadata. In particular, `databaseSecret` and `passwordSecret` identify
Kubernetes Secret resources, not passwords to store or verify.

Run from the repository root, supplying the same ordered profile and complete
overrides that will be passed to Helm:

```sh
python scripts/receiver_bindings.py prepare \
  --registry /operator/registry.json --bundle /operator/bundle.json \
  --namespace controllers --release custos \
  --values deploy/helm/custos/values-connected-eval.yaml \
  --values /operator/deployment.yaml
python scripts/receiver_bindings.py provision \
  --bundle /operator/bundle.json --namespace controllers --release custos \
  --values deploy/helm/custos/values-connected-eval.yaml \
  --values /operator/deployment.yaml
```

Preparation is an automated cluster/release preflight, not just an offline
JSON generator. It emits only a compact descriptor to standard output. Save
that descriptor under `global.receiverRuntime.executionBindingsBundle` in a
small values file, with `executionBindings: {bindings: [], defaults: null}`,
`retainLegacyExecutionResources: false` and `namespace.create: false`.
The null deletion marker clears defaults inherited from earlier values files;
an empty mapping would merge with and retain those inline defaults.
Do not pass the registry, bundle artifact or repeated Secret lists to Helm
through other values, `--set-file`, hooks or a subchart.

Provisioning server-validates all missing objects before creating them.
Existing resources must exactly match their expected digest/content and must
not have another lifecycle owner. Conflicts abort; nothing is overwritten.
A partial API failure can leave created objects: retain them and rerun the
same verified bundle, never replace them with a different snapshot.

Preflight the *intended* Helm operation: add `--atomic` when planning an atomic
file upgrade, or `--rollback-revision N` when planning a native rollback.
The helper rejects atomic first migration and native inline bridge targets;
rollback targets must match the verified retained file descriptor. These
arguments validate a plan, not execute Helm. Do not bypass a failed preflight.
Atomic preflight inspects the latest deployed/superseded history target, not
only the latest (possibly failed) manifest. An atomic retry remains prohibited
while that target is the inline bridge. Verify the retained rollback bundle
with `--rollback-revision N` before an atomic file-to-file upgrade.

Deploy with the compact descriptor and the complete deployment/security
values. Each consumer mounts `/custos/execution/bindings.json` read-only,
without `subPath`, and pins its digest and Dapr Configuration. Other services
and activity/hook sandboxes do not receive the registry.

## Migrate an existing inline release

1. Pause admission and drain. Install file-aware binaries using the same
   bounded inline registry, service identities and authorization policy.
2. Upgrade a **bridge release**, still inline, setting
   `global.receiverRuntime.retainLegacyExecutionResources: true`. If Helm
   owns the controller Namespace, retain `namespace.create: true` here.
   The stored release must mark all twelve fixed-name execution authorization
   resources, and that Namespace if chart-owned, with
   `helm.sh/resource-policy: keep`.
3. Package and preserve the exact bridge chart archive and complete ordered
   values. Record the verified stored/live bridge, original unique
   identifiers, content digests, ownership and artifact digests:

   ```sh
   helm package deploy/helm/custos --destination /operator/bridge
   python scripts/receiver_bindings.py record-bridge \
     --bundle /operator/bridge-inventory.json \
     --bridge-chart /operator/bridge/custos-0.1.0.tgz \
     --namespace controllers --release custos \
     --values /operator/complete-bridge-values.yaml \
     --acknowledge-paused-drained
   ```

   Use the archive name actually returned by `helm package`; do not assume its
   version. The complete values must reproduce the entire stored bridge as a
   recovery upgrade: all chart resources, non-consumer services and Helm hooks,
   not just the four managed Deployments and retained policies. Recording reads
   manifests/hooks at one deployed revision and rejects concurrent history
   changes. The inventory pins a `renderingSha256` as well as artifact hashes.
   Inventories without this full rendering pin cannot authorize a new cutover
   or recovery; record a verified bridge before cutover rather than accepting
   an unverified fallback.
4. Run preparation and provisioning with `--bridge-inventory
   /operator/bridge-inventory.json` in addition to the fresh-deployment
   arguments. Missing/changed/unretained resources or conflicting ownership
   abort first migration.
5. Upgrade to the compact file descriptor with retention false and Namespace
   creation false. **Do not use `--atomic` for this first cutover.** Keep all
   legacy authorization objects and any handed-off Namespace, including their
   Helm ownership metadata. Old application containers and sidecars still use
   them after restart.
6. Verify all consumers' readiness, snapshot digests and authorized/denied
   Secret access before resuming admission.

## Recovery

Do not use native `helm rollback` from file mode to the retained inline bridge
on Helm 3.16.2: it cannot reintroduce the kept live objects omitted from the
current manifest. File-to-file rollback is supported only with retained
compatible snapshots, identities and authorization.

For small, same-identity inline recovery, pause admission/drain first and use:

```sh
python scripts/receiver_bindings.py recover \
  --bundle /operator/bridge-inventory.json \
  --bridge-chart /operator/bridge/custos-0.1.0.tgz \
  --values /operator/complete-bridge-values.yaml \
  --acknowledge-paused-drained
```

The helper first verifies saved artifacts and the complete recovery rendering
against the recorded digest, including non-consumer services and hooks. A
changed image, metadata, resource or hook aborts before live reconciliation.
It then verifies original object identities/policies and
ownership, checks all four live Deployments' Helm release name, release
namespace, managed-by label and lifecycle metadata before any patch, and
checks that current configured identity references fit the bridge,
then reconciles each live Deployment's managed source fields with a
resource-version-tested patch. It removes file/digest environments, mounts,
volumes and annotations, restores the bridge source/Dapr Configuration, and
preserves unrelated fields. It validates that managed field set before a
normal `helm upgrade --reset-values --wait --timeout 20m`.

This reconciliation is necessary even after a first cutover times out under
`--wait`: resetting values alone can leave live-only file settings alongside
the restored inline source. A conflict or partial failure is an error, not
recovery success. Keep admission paused, retain all resources, inspect the
affected consumer and rerun from fresh live state after fixing the cause.
Never use force, delete/recreate, ownership bypass or a destructive fallback.

The helper cannot prove that all retained durable work has drained. Before
inline recovery, the operator must establish that *every accepted identity*,
not only currently configured defaults, remains resolvable by the bridge.
If the inline limit or identity compatibility cannot be met, stay on
file-aware binaries and a verified retained file bundle instead.

## Retention and explicit cleanup

Versioned RoleBindings coexist during rollout/recovery, giving the named
ServiceAccount the union of explicitly named retained Secrets. Audit that
overlap. Keep each bundle and legacy policy while any Pod, supported retained
Helm revision or retained work needs it, including across uninstall. Old
ReplicaSets and other retained workload controllers count as references too.
Standalone RoleBindings that reference a bundle Role also prevent cleanup;
only the verified RoleBindings being deleted with that bundle are excluded.

After pausing/draining, proving no retained work needs the identities and
retiring all referencing Pods/controllers/Helm history, explicitly clean up:

```sh
python scripts/receiver_bindings.py cleanup \
  --bundle /operator/bundle.json \
  --values /operator/complete-deployment-values.yaml \
  --acknowledge-paused-drained --confirm-no-retained-work
python scripts/receiver_bindings.py cleanup-bridge \
  --bundle /operator/bridge-inventory.json \
  --acknowledge-paused-drained --confirm-no-retained-work
```

Cleanup inspects every page of Helm releases and every retained revision.
It reads both `helm get manifest` and `helm get hooks` for each revision:
a completed hook Job can be absent from the live controller inventory while
its stored hook still needs the bundle during rollback.
It distinguishes resource kinds and actual configuration/map/Role references
from unrelated same-named objects, such as receiver NetworkPolicies.
Cleanup refuses live/retained references, missing/changed policies and changed
ownership. Every namespaced bundle or bridge resource must belong to the
controller Namespace being scanned; mixed-namespace inventories are rejected
before any live API read or deletion. Individual deletes require the verified
unique identifier and resource version. Partial/concurrent failure stops explicitly; preserve the
remaining inventory for diagnosis. **Cleanup never deletes the Namespace.**
Helm uninstall completion does not prove that terminating Pods have retired.
Wait for actual Pod/controller retirement before cleanup; do not shorten
production drain or shutdown budgets to bypass this retention check.
Bridge recording, verification and cleanup reject owner references and
terminating objects: Helm's keep annotation cannot prevent owner-driven
garbage collection.

Helper-authored validation failures include a fixed actionable reason
(for example, `bundle content or deployment/security inputs changed`).
Raw parser, filesystem and subprocess details remain suppressed; inspect
the reported operation and tool exit status without exposing credentials.

## Validation scope

[Loader tests](../../src/libs/custos-callctx/tests/test_execution_file.py) and
the four service suites cover actual startup/failure boundaries.
[Managed tests](../../tests/helm/test_execution_bundle.py) cover profiles,
budgets, policy equivalence and operator guards.
[Hosted lifecycle tests](../../tests/helm/test_execution_bundle_cluster.py)
use a disposable kind cluster, Helm's actual default Secret storage and actual
Dapr/Kubernetes permissions with maximum-length nonrepetitive 1,000/1,024
fixtures. The hosted installer downloads pinned kind/kubectl binaries from
their official release endpoints and verifies SHA-256 checksums before execution.
They use a configuration-probe image and no-op migration-hook
fixtures. PostgreSQL state-store and Redis broker use disposable real backends;
database/broker Secret authority and Dapr/Kubernetes authorization remain real.
Runtime Manager retains its HTTPS probes and Dapr application protocol using
a disposable certificate, a TLS 1.2 minimum and a certificate-verifying diagnostic client.
Unexpected Helm failures report container exits and fixed diagnostic categories,
never raw application/sidecar logs or Secret values.
These fixtures are explicitly not deployed application or migration certification.
Full receiver-to-Workflow effects and saturation remain #1018; final user
documentation remains #1019.
