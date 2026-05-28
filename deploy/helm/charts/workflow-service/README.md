# workflow-service

Custos workflow-service component chart. See
[`design/components/workflow-service/design.md`](../../../../design/components/workflow-service/design.md)
for the design.

## What this chart deploys

A Deployment + Service + ServiceAccount + ConfigMap for the workflow-service
process, optionally with an ExternalSecret stub for forward-compatible
projection of secret-store-backed env vars (none required in v1). The
Deployment binds:

| Container env source | Carries |
|---|---|
| ConfigMap `<release>-workflow-service` | `WF_DAPR_WORKFLOW_COMPONENT`, `WF_PUBLISH_TOPIC`, `WF_ARM_ENDPOINT`, `WF_TS_ENDPOINT`, `WF_CONNECTOR_ENDPOINT`, `WF_CATALOG_ENDPOINT`, `WF_RUN_HISTORY_RETENTION`, `WF_RESUME_SUB_DEFAULT_TTL`, `WF_REGISTER_SUB_MAX_RETRIES`, `WF_EXPR_TIMEOUT_MS`, `WF_IDEMPOTENCY_KEY_TTL` |
| Secret `<release>-workflow-service` (materialized by ExternalSecret when `externalSecret.enabled=true`) | None in v1 — stub for forward compatibility |

`HOST` / `PORT` come from the container image defaults (`0.0.0.0:8080`); the
Service port mirrors that.

The Pod itself will not start a working FastAPI surface until WF-IMPL-015
([#349](https://github.com/toddysm/custos/issues/349)) wires `create_app()`.
This chart, however, is the deploy-time contract: a `helm template` against
each umbrella profile must produce the documented manifests today so that
later tasks can land an image and immediately deploy a working pod.

## Values overview

See [`values.yaml`](values.yaml) for the full list. The keys most operators
will touch:

- `config.daprWorkflowComponent` — name of the Dapr Workflow component to
  bind. Required by the service at startup.
- `config.publishTopic` — Dapr Pub/Sub topic for lifecycle events.
- `arm.endpoint`, `trigger.endpoint`, `connector.endpoint`, `catalog.endpoint`
  — sibling service endpoints. Defaults match the in-cluster Service names
  emitted by the corresponding subcharts.
- `config.runHistoryRetention`, `config.resumeSubDefaultTtl`,
  `config.registerSubMaxRetries`, `config.exprTimeoutMs`,
  `config.idempotencyKeyTtl` — runtime behaviour tunables documented in
  `design.md § Configuration`.
- `externalSecret.enabled` / `externalSecret.data[]` — forward-compatible
  stub. v1 ships no secret env vars; leave disabled unless a downstream
  profile injects bespoke secrets.

## Failure modes covered

`livenessProbe` hits `/healthz` and `readinessProbe` hits `/readyz`. The
process implementing those endpoints is WF-IMPL-015's responsibility;
until that lands, deploying the chart produces a pod that crash-loops on
the not-yet-implemented FastAPI factory. This is intentional: deploys do
not silently succeed before the runtime is real.
