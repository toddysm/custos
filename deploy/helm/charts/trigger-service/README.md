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
`values.yaml`), per design.md § Configuration:

| Env var | Default | Source |
|---|---|---|
| `TRIGGER_WEBHOOK_BASE_URL` | `http://trigger-service:8080` | `config.webhookBaseUrl` |
| `TRIGGER_DEDUP_TTL_SECONDS` | `86400` | `config.dedupTtlSeconds` |
| `TRIGGER_POLLER_DEFAULT_INTERVAL_SECONDS` | `60` | `config.pollerDefaultIntervalSeconds` |
| `TRIGGER_RESUME_DEFAULT_TTL_SECONDS` | `604800` | `config.resumeDefaultTtlSeconds` |
| `TRIGGER_DISPATCH_MAX_RETRIES` | `5` | `config.dispatchMaxRetries` |
| `TRIGGER_SCHEDULER_LEADER_LEASE_SECONDS` | `30` | `config.schedulerLeaderLeaseSeconds` |
| `TRIGGER_PUBSUB_COMPONENT` | `custos-pubsub` | `config.pubsubComponent` |
| `TRIGGER_NORMALIZED_TOPIC` | `custos.triggers.normalized` | `config.normalizedTopic` |
| `TRIGGER_WORKFLOW_EVENTS_TOPIC` | `custos.workflow.events` | `config.workflowEventsTopic` |
| `TRIGGER_WF_ENDPOINT` | `http://workflow-service:8080` | `workflow.endpoint` |
| `TRIGGER_CONNECTOR_ENDPOINT` | `http://connector-service:8080` | `connector.endpoint` |

Secret env (ExternalSecret → Secret → `envFrom`), disabled by default:

| Env var | Secret-store key | Source |
|---|---|---|
| `TRIGGER_METADATA_STORE` | `custos/storage-provider-layer/metadata-store#dsn` | `externalSecret.data[0]` |

The Scheduler / Generic Webhook / Pull receivers are deferred to M2 (see the
[implementation plan](../../../../design/components/trigger-service/implementation-plan.md)),
so the webhook base URL and poller / leader-lease knobs ship with their
documented defaults but are not exercised until those receivers land.

## Dapr Pub/Sub

The Internal Event Receiver subscribes to `custos.workflow.events` and
receivers publish normalized events onto `custos.triggers.normalized`, both on
the shared platform pub/sub component (`TRIGGER_PUBSUB_COMPONENT`). Operators
provisioning Dapr Pub/Sub for the platform must include both topics
(design.md § Dapr Pub/Sub subscriptions).
