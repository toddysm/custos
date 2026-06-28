# Walkthrough — Copy an Image from Docker Hub to GHCR

Last Updated: 2026-06-27

This runbook is the single end-to-end path for the out-of-the-box (OOTB)
`copy-image` activity: starting from a deployed evaluation platform, you
**onboard** the OOTB connector-types and the activity-type into the catalog,
create a Docker Hub **source** and a GHCR **destination** connector instance,
then publish and run a workflow that copies an image between them and inspect
the result. Everything is done through the **HTTP API** — M1 has no Web UI.

## Prerequisites

- A deployed evaluation platform — see [prerequisites](prerequisites.md),
  [install (connected)](install-connected.md), and [verify](verify.md) for the
  gateway endpoint.
- A **platform-admin** service token (`cst_...`). Onboarding registers types in
  the reserved `custos.builtin` namespace and the platform-scoped connector
  catalog, which require platform admin. See
  [First Use](first-workflow.md#1-get-an-access-token-m1) for obtaining a token.
- A Docker Hub account/token and a GitHub `write:packages` PAT for the two
  connector instances (used in [Create connector instances](#3-create-connector-instances)).
- The OOTB images published to your registry (next section).

Shared variables (adjust the values; add `-k` to `curl` if your gateway uses the
eval self-signed certificate):

```bash {"promptEnv":"false"}
export GATEWAY=https://custos.local
export WS=ws-default
export TOKEN=cst_REPLACE_ME
```

## 1. Publish (or reuse) the OOTB images

The OOTB images are published by the per-extension publish workflows, each
triggered by a version tag:

| Extension | Workflow | Tag |
|---|---|---|
| `copy-image` activity | `publish-activity-copy-image.yml` | `activity-copy-image-vX.Y.Z` |
| `dockerhub` connector | `publish-connector-dockerhub.yml` | `connector-dockerhub-vX.Y.Z` |
| `ghcr` connector | `publish-connector-ghcr.yml` | `connector-ghcr-vX.Y.Z` |

Each pushes `ghcr.io/<owner>/custos/<name>` and prints the pushed digest in the
job summary. For a public evaluation, reuse the already-published images. For a
local/dev cluster you can instead build and load the images yourself, e.g.:

```bash {"cwd":"../../.."}
# Example: build the copy-image activity locally and load it into kind.
docker build -f extensions/activities/copy-image/Containerfile \
  -t ghcr.io/toddysm/custos/copy-image:v0.1.0 extensions/activities/copy-image
kind load docker-image ghcr.io/toddysm/custos/copy-image:v0.1.0 --name custos-eval
```

## 2. Onboard the OOTB types into the catalog

Register the two connector-types and the `copy-image` activity-type with the
idempotent onboarding script. It resolves each published image's digest and
registers against it:

```bash {"cwd":"../../.."}
GATEWAY="$GATEWAY" TOKEN="$TOKEN" scripts/seed-ootb.sh
# add INSECURE=1 if your gateway uses the eval self-signed certificate.
```

Verify the registrations resolved:

```bash
# Platform-scoped connector-types.
curl -sS "$GATEWAY/v1/catalog/connector-types" \
  -H "authorization: Bearer $TOKEN"

# The built-in activity-type (resolvable from any workspace).
curl -sS "$GATEWAY/v1/workspaces/$WS/activity-types/custos.builtin/copy-image@0" \
  -H "authorization: Bearer $TOKEN"
```

## 3. Create connector instances

Create a Docker Hub **source** instance and a GHCR **destination** instance. The
full steps (creating the credential Secret + the instance) are in the connector
usage guides — follow **step 1 (credentials)** and **step 3 (instance)** of
each:

- [Docker Hub connector](../connectors/dockerhub.md) → `dockerhub-prod` (source)
- [GHCR connector](../connectors/ghcr.md) → `ghcr-prod` (destination)

The instance bodies look like this (the `type` is the connector manifest's
`metadata.type`):

```bash
# Source: Docker Hub (oci.pull + oci.list-referrers). References the dockerhub-pat Secret.
curl -sS -X POST "$GATEWAY/v1/workspaces/$WS/connectors" \
  -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -d @- <<'JSON'
{
  "type": "custos-dockerhub",
  "version": "0.1.0",
  "name": "dockerhub-prod",
  "targetConfig": { "repositoryNamespace": "library" },
  "credentialsAuthentication": {
    "secretName": "dockerhub-pat",
    "usernameKey": "username",
    "tokenKey": "token",
    "namespace": "custos-connectors"
  },
  "usedCapabilities": ["oci.pull", "oci.list-referrers"],
  "leaseTtlSeconds": 3600
}
JSON
```

```bash
# Destination: GHCR (oci.push). References the ghcr-pat Secret from the guide.
curl -sS -X POST "$GATEWAY/v1/workspaces/$WS/connectors" \
  -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -d @- <<'JSON'
{
  "type": "custos-ghcr",
  "version": "0.1.0",
  "name": "ghcr-prod",
  "targetConfig": { "repositoryNamespace": "<your-gh-namespace>" },
  "credentialsAuthentication": {
    "secretName": "ghcr-pat",
    "usernameKey": "username",
    "tokenKey": "token",
    "namespace": "custos-connectors"
  },
  "usedCapabilities": ["oci.push"],
  "leaseTtlSeconds": 3600
}
JSON
```

## 4. Publish the copy workflow

Write a one-step workflow that binds the two instances to the `copy-image`
activity's `source`/`dest` slots:

```bash
cat > /tmp/copy-hello-world.yaml <<'YAML'
apiVersion: custos.dev/v1
kind: Workflow
metadata:
  name: copy-hello-world
spec:
  steps:
    - id: copy
      activity: custos.builtin/copy-image@0
      connectors:
        source: dockerhub-prod
        dest: ghcr-prod
      with:
        source:
          ref: docker.io/library/hello-world:latest
        destination:
          repository: <your-gh-namespace>/hello-world
          tag: mirrored
YAML
```

Publish it (the request wraps the document as a string under `definition`; `jq`
does the escaping):

```bash
curl -sS -X POST "$GATEWAY/v1/workspaces/$WS/workflows" \
  -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -d "$(jq -Rs '{definition: .}' /tmp/copy-hello-world.yaml)"
```

Publishing succeeds only if `custos.builtin/copy-image@0` resolves and both
named instances advertise the required capabilities (`oci.pull` +
`oci.list-referrers` on `source`, `oci.push` on `dest`) — otherwise the validator returns a `catalog.publish.*`
error. The workflow-version-id is `<workspaceId>/<name>@<version>`:

```bash
export WFV="$WS/copy-hello-world@1"
```

## 5. Run the copy

```bash
curl -sS -X POST "$GATEWAY/v1/workspaces/$WS/runs" \
  -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -d "{\"workflowVersionId\": \"$WFV\", \"inputs\": {}, \"idempotencyKey\": \"copy-hello-world-1\"}"
```

```bash
export RUN=run-...   # copy runId from the response
```

## 6. Inspect the result

Poll the run to a terminal state and read the copy step's outputs
(`destinationRef`, `digest`, `manifestsCopied`, and `reportRef`, which points
to the `copy-report` artifact) from the step timeline:

```bash
curl -sS "$GATEWAY/v1/workspaces/$WS/runs/$RUN" \
  -H "authorization: Bearer $TOKEN"
```

```bash
# Structured run logs (per-step).
curl -sS "$GATEWAY/v1/workspaces/$WS/runs/$RUN/logs" \
  -H "authorization: Bearer $TOKEN"
```

On success the `copy` step reports the destination it wrote, e.g.:

```json
{
  "destinationRef": "ghcr.io/<your-gh-namespace>/hello-world:mirrored",
  "digest": "sha256:...",
  "manifestsCopied": 1,
  "reportRef": { "kind": "ArtifactRef", "name": "copy-report" }
}
```

Confirm the image landed by pulling it from GHCR, and review the audit trail via
the [Observability API](../../developers/observability-api.md).

## Troubleshooting

- `catalog.publish.*` on publish → the activity-type or a connector instance
  isn't registered/bound; re-run [onboarding](#2-onboard-the-ootb-types-into-the-catalog)
  and confirm the instances exist.
- `source.unauthorized` / `dest.unauthorized` in the run → the connector
  credential Secret is missing or lacks pull/push rights; re-check step 1 of the
  connector guides.
- General gateway/auth issues → [Troubleshooting](troubleshooting.md).

## Related documentation

| Document | Description |
|---|---|
| [copy-image activity](../../../extensions/activities/copy-image/README.md) | Inputs/outputs, error codes, resources |
| [OOTB catalog index](../../../extensions/README.md) | All bundled extensions |
| [Docker Hub connector](../connectors/dockerhub.md) / [GHCR connector](../connectors/ghcr.md) | Credentials + instance creation |
| [Catalog API](../../developers/catalog-api.md) | Activity-/connector-type registration |
| [Workflow API](../../developers/workflow-api.md) | Starting and reading runs |
