# First Use — Authenticate & Run a Workflow

Last Updated: 2026-06-08

This page walks through the first end-to-end use of a freshly deployed
evaluation platform: authenticate, confirm a workspace, publish a workflow, run
it, and inspect the result. Everything is done through the **HTTP API** — M1 has
no Web UI.

All requests go through the API gateway. This guide uses a `GATEWAY` variable
for the externally reachable base URL you found in [Verify](verify.md); for
local testing it may be a port-forwarded address.

```bash
export GATEWAY=https://custos.local      # adjust to your gateway endpoint
export WS=ws-default                      # the bootstrap-seeded workspace
```

> Examples use `curl`. Add `-k` if your gateway uses the eval self-signed
> certificate and you have not trusted its CA.

## 1. Get an access token (M1)

M1 uses service-token authentication; the interactive OIDC device-code flow is
disabled. On a clean installation, follow the
[first-admin bootstrap ceremony](../../operations/bootstrap-admin.md) to create
the dedicated platform administrator. The token begins with `custos_`, is shown
only when explicitly requested, and cannot be retrieved later. Export it:

```bash
export TOKEN="$CUSTOS_TOKEN"  # created by custosctl bootstrap-admin init
```

Verify the token is accepted and see the principal it resolves to:

```bash
curl -sS -X POST "$GATEWAY/v1/auth/verify" \
  -H 'content-type: application/json' \
  -d "{\"token\": \"$TOKEN\"}"
```

A `200` response returns a principal envelope (`principal_id`, tenant, granted
permissions). Every subsequent admin call carries the token as a bearer:
`-H "authorization: Bearer $TOKEN"`.

## 2. Confirm (or create) a workspace

The post-install bootstrap seeds `tenant-default` and a default workspace.
Confirm it is visible to your token:

```bash
curl -sS "$GATEWAY/v1/workspaces/$WS" \
  -H "authorization: Bearer $TOKEN"
```

To create a separate workspace for evaluation (returns `409 Conflict` if a
workspace with that id already exists):

```bash
curl -sS -X POST "$GATEWAY/v1/tenants/tenant-default/workspaces" \
  -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -d '{"workspace_id": "ws-eval", "display_name": "Evaluation Workspace"}'
```

## 3. Publish a workflow

Publish a workflow definition into the workspace catalog. The request body wraps
the definition document as a string under `definition`. The document must be a
valid Custos `Workflow` (`apiVersion`, `kind`, `metadata`, and `spec.steps`).
The sample below uses a single connectorless `let` step, so it publishes without
any activity-type or connector-type needing to be registered first:

```bash
curl -sS -X POST "$GATEWAY/v1/workspaces/$WS/workflows" \
  -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -d @- <<'JSON'
{
  "definition": "apiVersion: custos.dev/v1\nkind: Workflow\nmetadata:\n  name: hello\nspec:\n  steps:\n    - id: greet\n      let:\n        message: hello from custos\n"
}
JSON
```

A `201 Created` response returns the workspace, workflow name, and the assigned
monotonic integer version:

```json
{ "workspaceId": "ws-default", "workflowName": "hello", "version": 1 }
```

> **Publish-time validation.** The catalog validates the document in two passes
> (syntax/schema, then ref + CEL resolution). Any activity-type or connector-type
> an `activity` step references must already be published in the workspace, or
> publish fails with a `catalog.publish.*` error. To run real activities, adapt
> the sample to the activity-types available in your evaluation. See the
> [Catalog API](../../developers/catalog-api.md) and the
> [workflow authoring guide](../../developers/workflow-api.md) for the full
> document schema.

The **workflow-version-id** the run API needs is the triple
`<workspaceId>/<workflowName>@<version>`, which you can construct directly from
the publish response — no extra lookup required:

```bash
export WFV="$WS/hello@1"        # <workspaceId>/<workflowName>@<version>
```

## 4. Start a run

Start a run from the workflow-version-id. `inputs` are validated against the
workflow's published schema; `idempotencyKey` makes the start safe to retry:

```bash
curl -sS -X POST "$GATEWAY/v1/workspaces/$WS/runs" \
  -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -d "{
    \"workflowVersionId\": \"$WFV\",
    \"inputs\": {},
    \"idempotencyKey\": \"eval-first-run-1\"
  }"
```

The response is a run handle:

```json
{
  "runId": "run-...",
  "status": "running",
  "workspaceId": "ws-default",
  "workflowVersionId": "ws-default/hello@1",
  "startedAt": "2026-06-08T12:00:00Z"
}
```

```bash
export RUN=run-...        # copy runId from the response
```

## 5. Poll run status

Read the run until it reaches a terminal state (`succeeded`, `failed`, or
`cancelled`):

```bash
curl -sS "$GATEWAY/v1/workspaces/$WS/runs/$RUN" \
  -H "authorization: Bearer $TOKEN"
```

The full run record includes `status`, `startedAt`/`updatedAt`, `inputs`,
`outputs`, and the step timeline. `RunStatus` cycles through `queued` →
`running` → a terminal value.

## 6. Inspect run logs and telemetry

Fetch the run's logs and metrics through the Observability API:

```bash
# Structured run logs.
curl -sS "$GATEWAY/v1/workspaces/$WS/runs/$RUN/logs" \
  -H "authorization: Bearer $TOKEN"

# Run metrics. metric, from, and to are required query parameters; from/to are
# timezone-aware ISO-8601 timestamps bounding the window.
curl -sS -G "$GATEWAY/v1/workspaces/$WS/runs/$RUN/metrics" \
  -H "authorization: Bearer $TOKEN" \
  --data-urlencode 'metric=workflow_step_duration_seconds' \
  --data-urlencode 'from=2026-06-08T00:00:00Z' \
  --data-urlencode 'to=2026-06-08T23:59:59Z'
```

See the [Observability API](../../developers/observability-api.md) for log
tailing, the audit trail, and per-step log endpoints.

## What you just did

1. Authenticated with an operator-bootstrapped service token (M1 — no device-code flow).
2. Confirmed the bootstrap-seeded workspace.
3. Published a workflow into the catalog.
4. Started a run and polled it to completion.
5. Inspected the run's logs and metrics.

## Troubleshooting

If a call returns `401`/`403`, re-check the token and that your principal has the
required permission (e.g. `catalog:workflows:write` to publish). If publish
returns a `catalog.publish.*` error, fix the referenced activity/connector types.
See [Troubleshooting](troubleshooting.md).

## Related documentation

| Document | Description |
|---|---|
| [Copy-image walkthrough](copy-image-walkthrough.md) | Onboard the OOTB connectors + copy-image activity and run a Docker Hub -> GHCR copy end to end |
| [Auth API](../../developers/auth-api.md) | Tokens, service accounts, workspaces, permissions |
| [Catalog API](../../developers/catalog-api.md) | Publishing and resolving workflow definitions |
| [Workflow API](../../developers/workflow-api.md) | Starting, reading, and cancelling runs |
| [Observability API](../../developers/observability-api.md) | Run logs, metrics, and audit trail |
| [Verify](verify.md) | Finding the gateway endpoint |
