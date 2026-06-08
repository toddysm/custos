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

`livenessProbe` hits `/healthz` and `readinessProbe` hits `/readyz`. A
`startupProbe` (also `/healthz`) gates both until the process is serving: the
FastAPI lifespan blocks on the Dapr Workflow worker reporting ready before
uvicorn accepts connections, so on a cold cluster the liveness/readiness probes
would otherwise hit a connection-refused window and crash-loop the pod before
the worker converges (issue #816). Tune the cold-start budget via
`startupProbe.periodSeconds` × `startupProbe.failureThreshold`.

The Dapr Workflow engine is built on the actor model (REQ-046) and only
initialises once a state store advertises `actorStateStore: "true"`. That flag
is set on the umbrella's `custos-statestore` Component
(`dapr.components.stateStore.actorStateStore`), not in this subchart — without
it the workflow-service sidecar never hosts the workflow engine and the worker
never reaches ready.
