# trigger-service

Helm subchart for the Custos Trigger Service (COMP-004). Rendered by the
`custos` umbrella chart (`deploy/helm/custos`) as a conditional dependency
(`trigger-service.enabled`).

Design: [`design/components/trigger-service/design.md`](../../../../design/components/trigger-service/design.md).

## What it renders

- **Deployment** — single container, Dapr sidecar annotations
  (`dapr.io/app-id: trigger-service`), `/healthz` + `/readyz` probes, env via
  `envFrom` (ConfigMap always; Secret when `externalSecret.enabled`).
- **Service** — `ClusterIP` on `name=http` port `8080`.
- **ConfigMap** — the non-secret `TRIGGER_*` env vars from design.md
  § Configuration plus the Dapr Pub/Sub component + topic refs and the
  sibling-service endpoints.
- **ServiceAccount**, **ServiceMonitor** (opt-in), **ExternalSecret** (opt-in).

## Configuration contract

Non-secret env (ConfigMap, from `config:` / `workflow:` / `connector:` in
`values.yaml`). The numeric / TTL knobs come from design.md § Configuration;
the Pub/Sub component + topic refs from § Dapr Pub/Sub subscriptions; the
sibling-service endpoints from § Dependencies:

| Env var | Default | Source |
|---|---|---|
| `TRIGGER_WEBHOOK_BASE_URL` | `http://trigger-service:8080` (placeholder, see note) | `config.webhookBaseUrl` |
| `TRIGGER_DEDUP_TTL_SECONDS` | `86400` | `config.dedupTtlSeconds` |
| `TRIGGER_POLLER_DEFAULT_INTERVAL_SECONDS` | `60` | `config.pollerDefaultIntervalSeconds` |
| `TRIGGER_RESUME_DEFAULT_TTL_SECONDS` | `604800` | `config.resumeDefaultTtlSeconds` |
| `TRIGGER_DISPATCH_MAX_RETRIES` | `5` | `config.dispatchMaxRetries` |
| `TRIGGER_FANOUT_MAX_DEPTH` | `16` | `config.fanoutMaxDepth` |
| `TRIGGER_SCHEDULER_LEADER_LEASE_SECONDS` | `30` | `config.schedulerLeaderLeaseSeconds` |
| `TRIGGER_PUBSUB_COMPONENT` | `custos-pubsub` | `config.pubsubComponent` |
| `TRIGGER_NORMALIZED_TOPIC` | `custos.triggers.normalized` | `config.normalizedTopic` |
| `TRIGGER_WORKFLOW_EVENTS_TOPIC` | `custos.workflow.events` | `config.workflowEventsTopic` |
| `TRIGGER_WF_ENDPOINT` | `http://workflow-service:8080` | `workflow.endpoint` |
| `TRIGGER_WORKFLOW_APP_ID` | `workflow-service` | `workflow.appId` (authenticated identity and Dapr dispatch target) |
| `TRIGGER_WORKFLOW_NAMESPACE` | release namespace | `workflow.namespace` (empty selects `.Release.Namespace`) |
| `TRIGGER_CONNECTOR_ENDPOINT` | `http://connector-service:8080` | `connector.endpoint` |

Secret env (ExternalSecret → Secret → `envFrom`), disabled by default:

| Env var | Secret-store key | Source |
|---|---|---|
| `TRIGGER_METADATA_STORE` | `custos/storage-provider-layer/metadata-store#dsn` | `externalSecret.data[0]` |

Scheduled, webhook and Connector-owned polling collaborators are implemented.
Their owned background drivers are default-off; use the explicit umbrella
activation below after upgrading every writer and applying migrations.

> **`TRIGGER_WEBHOOK_BASE_URL`** is marked Required with no default in
> design.md § Configuration — once the Generic Webhook receiver ships,
> operators must supply the externally-reachable ingress URL. The
> in-cluster Service address shipped here is a chart-provided placeholder so
> the profiles render without an operator-supplied value; it is not a
> design-sanctioned default.

## Dapr Pub/Sub

The Internal Event Receiver subscribes to `custos.workflow.events` and
receivers publish normalized events onto `custos.triggers.normalized`, both on
the shared platform pub/sub component (`TRIGGER_PUBSUB_COMPONENT`). Operators
provisioning Dapr Pub/Sub for the platform must include both topics
(design.md § Dapr Pub/Sub subscriptions).

## Production receiver activation

The [umbrella values](../../custos/values.yaml) expose two separate gates:

- `global.receiverRuntime.enabled=true` installs production identity, shared
  storage bindings, authenticated transport, private runtime and durable callbacks.
- `global.receiverRuntime.admissionEnabled=true` additionally starts scheduled
  and polling workers and accepts new verified webhooks. Leave it false while
  preparing or pausing a deployment.

Both default to false in connected-eval, connected-ha, airgapped-eval and
airgapped-ha. Off-mode renders retain the existing profiles and manual/internal
routes. On-mode deliberately replaces managed legacy `env` keys (storage,
endpoints, identity and activation), while preserving unrelated overrides.
It uses the registered app IDs, including Auth's `custos-auth`, and app port 8080.

Before activation, provision:

Use explicit tested controller image tags (prefer the released commit tag).
Set `migrate.image.tag` and, when bootstrap is enabled, `bootstrap.image.tag`
to the corresponding tested release too: they do not inherit `global.imageTag`.
Empty, `dev` and `latest` tags are rejected in this mode. Sidecar and helper images require
immutable digests. Native activity sidecars require Kubernetes 1.29 or newer;
the chart's legacy minimum remains unchanged when receiver mode is off.

1. The shared PostgreSQL Secret (`uri` key), also selected by `postgres.secretName`.
   Apply all migration revisions, including metadata 13/14/15 and lease 2.
   Startup checks fail closed; services do not auto-migrate.
2. One Dapr API-token Secret and **distinct** app-token Secrets for every service,
   each with a `token` key. Values contain Secret names only. Dapr must run with
   mutual Transport Layer Security (TLS); the supported baseline is 1.18.9.
3. A stable Auth signing-key Secret and explicit workspace execution accounts.
   Each execution binding contains only `workspaceId`, `principalId`, `bindingId`,
   `secretRef` and `secretKey`; `defaults` maps workspace IDs to binding IDs.
   Secrets live in the controller namespace. Account permissions, disablement and
   fresh Auth exchanges remain authoritative; no creator token is retained.
4. The enabled Connector Kubernetes hook runtime, with its own separate namespace,
   digest-pinned helper, registry URL and explicit upstream egress rules.
5. A different activity sandbox namespace, digest-pinned Sidecar and I/O bridge
   images, and a shared writable artifact PersistentVolumeClaim. High-availability
   replicas require storage supporting concurrent access; no per-replica artifact
   fallback is selected.
6. A runtime TLS Secret with `tls.crt`, `tls.key`, and `ca.crt`. Its certificate
   must cover `<release>-activity-runtime-manager.<namespace>.svc` (using the
   rendered, possibly truncated Service name). The public CA bundle is limited
   to 8 KiB and validated before runtime readiness and each new attempt. Only
   PEM certificate blocks and whitespace are accepted; combined key/certificate
   files are rejected. Only the public CA, not the
   TLS private key, is delivered with the Sidecar's private bootstrap files.
7. An explicit telemetry collector origin (`telemetryEndpoint`) accepting
   OpenTelemetry Protocol (OTLP) over HTTP. Configure `controllerEgress` for the
   collector, registry and other required external services, and explicit
   Kubernetes API endpoint CIDRs/ports. Hook/activity upstream egress remains
   separately allow-listed.
   `global.networkPolicies.monitoringNamespace` defaults to `custos-system`,
   matching the prerequisite installer. Override it explicitly for a separate
   monitoring namespace (including deployments previously relying on `monitoring`).
   Co-located Prometheus gets DNS, Kubernetes discovery and metrics egress through
   the managed deny-all policy; external Prometheus retains its own egress policy
   and must discover the actual controller namespace.
8. Configure Observability query providers in `observability-audit-service.env`.
   The default Loki and Prometheus providers require `CUSTOS_LOKI_URL` and
   `CUSTOS_PROMETHEUS_URL`; the supplied installer exposes `http://custos-loki:3100`
   and `http://custos-prometheus-server` in its namespace. Include their reachable
   destinations in the applicable network policies. An explicitly selected
   `noop` provider instead requires `CUSTOS_LOGS_EXTERNAL_URL` or
   `CUSTOS_METRICS_EXTERNAL_URL` for the external-view link; it does not serve
   query data. Missing conditional endpoints fail rendering for every profile.

Production mode creates namespace-scoped caller allowlists, protected broker
topics, and reference-scoped execution Secret readers. Remote pods cannot reach
local Dapr HTTP/gRPC application APIs (3500/50001); invocation uses the mutually
authenticated 50002 channel, including Auth public-key discovery. Protected
operations also require the distinct destination app token. Application authorization is not replaced by network
policy. Network policies restrict peers/ports, not HTTP paths. Validate policies
with the target cluster's network provider before use.
PostgreSQL/Redis ingress and peer traffic have explicit backend policies.
`global.networkPolicies.postgresOperatorNamespace` selects the CloudNativePG
operator namespace (default `cnpg-system`). Migration/bootstrap database and DNS
allowances, plus the injector gate's API egress, are pre-install/pre-upgrade
hooks at weight -15: they exist before the injector gate and migrations even
when upgrading a namespace already denied by policy. These three hook policies
persist between upgrades; remove them explicitly only when decommissioning the
release after its Jobs finish. Out-of-band infrastructure (including monitoring,
identity providers and backup destinations) must retain its own appropriate
network allowances; additive policies must not re-open private controller ports.
Authenticated local HTTP clients do not inherit ambient HTTP proxy routing.
External credential providers needing a proxy must use a separately configured
transport; never proxy the local Dapr API credential. Secret-reference uniqueness
is checked during rendering; provision independent token values as well.

Render the chosen profile with an operator-owned overlay before upgrading:

```bash
helm dependency build deploy/helm/custos
helm template custos deploy/helm/custos -n custos-system \
  -f deploy/helm/custos/values-connected-ha.yaml -f receiver-runtime-values.yaml
helm upgrade custos deploy/helm/custos -n custos-system \
  -f deploy/helm/custos/values-connected-ha.yaml -f receiver-runtime-values.yaml
```

The overlay supplies the references above under `global.receiverRuntime`;
configure `connector-service.hookRuntime` independently. Mirror pinned images
and provision the same prerequisites for either air-gapped profile. Missing
references, reused app tokens, unpinned helpers and inconsistent migration
bindings reject rendering. Referenced missing/invalid Secrets, certificates,
storage or authorization prevent startup/readiness rather than reporting success.

### Pause, drain and rollback

Set **only** `admissionEnabled=false` for an operational pause. Workers drain,
new webhook admission closes, and accepted broker callbacks, execution identities,
shared storage and runtime infrastructure remain configured. Existing manual and
scoped internal routes remain available. Audit and telemetry remain active for
accepted deliveries even with background workers paused. The pause rolls only
Trigger/Connector worker Pods, not Auth, Workflow or the activity runtime.

The normal worker drain budget is 330 seconds; termination grace must exceed it
by more than 65 seconds (420 by default). This reserves 15 seconds for HTTP,
30 for hook cancellation cleanup, 15 for resource closure, and five for Dapr.
Managed Trigger/Connector/Workflow processes and the runtime entry point
bound the initial HTTP shutdown wait to 15 seconds, so it
does not consume the worker drain budget. Every concurrently dispatched poll
hook is cancelled and joined before its driver exits. Owned cancellation is joined before HTTP clients
and storage pools close. A forced Kubernetes kill is not proof of sandbox
termination; unknown-effect fences must remain intact.

The four execution/receiver sidecars use Dapr's `block-shutdown-duration` for
the application HTTP wait, worker drain, hook cleanup and resource-close reserve
(390 seconds by default), followed by a five-second Dapr
drain. This keeps local Dapr APIs available while the application finishes its
owned work. Dapr application-health early exit is disabled for this window
because uvicorn closes its listener before lifespan cleanup. Pods may therefore
remain Terminating for the block window even when work completes sooner.

Do not roll back to binaries predating protocol 3 or remove metadata/lease rows,
signing references, execution accounts or accepted-request identities to force a
retry. Keep `receiverRuntime.enabled=true` while any accepted work needs the new
contracts. Disabling the entire mode is a separate, quiescent migration operation,
not the pause switch. Sandbox namespace/isolation/controller permissions are
retained on removal so unresolved jobs cannot lose their cleanup boundary; remove
those retained resources only after verifying termination and retained fences.
TLS and Dapr/app-token Secret changes require the corresponding Pod rollout;
never disable certificate checks. Rotation of provider material and workspace
execution credentials behind stable references remains a fresh-read operation.

### Operational signals

Trigger exports `custos_trigger_receiver_passes_total`,
`custos_trigger_receiver_pass_seconds`, `custos_trigger_schedule_lag_seconds`,
`custos_trigger_replay_age_seconds`, `custos_trigger_replay_events_total` and
`custos_trigger_retention_expired_total`, plus bounded pipeline outcome counters
and spans. New metric labels contain only operation/outcome categories, not
workspace/event/run identifiers. Connector adds
`custos_connector_webhook_verification_failures_total` and retains its existing
poll duration/cursor-lag instrumentation. Durable audit records retain safe
scoped identifiers and committed lifecycle events, never provider diagnostics
or credential values. Retention does not remove live replay claims, receipt
identities or schedule replay proofs.
Configure the collector's backend exporters separately; SDK export to a collector
does not itself establish metric/trace persistence.

Before changing `admissionEnabled` to true, verify the runtime's HTTPS endpoint
from the sandbox network using the configured CA, and check its certificate
names/expiry and the readiness of every required service. Valid PEM syntax alone
does not prove certificate-chain or network reachability in the target cluster.

Rendering/unit/native component checks are not deployed receiver-to-workflow
certification. That remains [#1018](https://github.com/toddysm/custos/issues/1018).
