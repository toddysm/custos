# Docker Hub connector (`custos-dockerhub`)

Last Updated: 2026-06-27

This guide walks the full lifecycle of using the out-of-the-box **Docker Hub**
connector to let your workflows pull from and push to
[Docker Hub](https://hub.docker.com). Read
[Using the OOTB registry connectors](README.md) first for the shared concepts
(credential model, auth, lease TTL).

```bash
export GATEWAY=https://custos.local   # your gateway endpoint
export WS=ws-default                  # your workspace id
export TOKEN=cst_...                  # a Custos service token (see First Use)
```

> Examples use `curl`. Add `-k` if your gateway uses the eval self-signed
> certificate and you have not trusted its CA.

## 1. Create the credential Secret

Create a **Docker Hub Personal Access Token** (Docker Hub -> *Account Settings ->
Security -> Personal access tokens*) with at least `Read & Write` scope if you
intend to push, then store it as a Kubernetes Secret in the
connector-credentials namespace:

```bash
kubectl create secret generic dockerhub-pat \
  --namespace custos-connectors \
  --from-literal=username='<docker-hub-username>' \
  --from-literal=token='<docker-hub-access-token>'
```

The Secret must expose the username under the `username` key and the PAT under
the `token` key (these key names are referenced by the instance in step 3). The
connector-service is granted namespace-scoped read access to this namespace by
the umbrella chart (`connectorCredentials.enabled`), so no Helm upgrade is
needed.

> The plugin never receives this token directly — it is resolved and leased by
> the connector sidecar at runtime via `x-dapr-secret`.

## 2. Register the connector type (control-plane)

A connector **type** must be registered with the platform before you can create
instances of it. This is a **control-plane action** performed by a service
identity holding `connector:register` (the
`POST /internal/v1/connectors:register` RPC — see
[Connector plugin author guide §8](../../developers/connector-plugin-author.md#8-registering-the-connector-type)).
The OOTB connectors are typically pre-registered at deployment time.

Confirm the type is available in your workspace:

```bash
curl -sS -G "$GATEWAY/v1/workspaces/$WS/connector-types" \
  -H "authorization: Bearer $TOKEN" \
  --data-urlencode 'type=custos-dockerhub'
```

A `200` lists the registered versions:

```json
{
  "items": [
    {
      "type": "custos-dockerhub",
      "version": "0.1.0",
      "digest": "sha256:...",
      "imageRef": "ghcr.io/.../custos-dockerhub@sha256:...",
      "deprecated": false
    }
  ],
  "nextCursor": null
}
```

If the list is empty, ask your platform operator to register the type.

## 3. Create a connector instance

A connector **instance** binds the type to a specific Docker Hub namespace and
the credential Secret from step 1. Requires `admin:connector`:

```bash
curl -sS -X POST "$GATEWAY/v1/workspaces/$WS/connectors" \
  -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -d @- <<'JSON'
{
  "type": "custos-dockerhub",
  "version": "0.1.0",
  "name": "dockerhub-prod",
  "targetConfig": {
    "repositoryNamespace": "<docker-namespace>"
  },
  "credentialsAuthentication": {
    "secretName": "dockerhub-pat",
    "usernameKey": "username",
    "tokenKey": "token",
    "namespace": "custos-connectors"
  },
  "usedCapabilities": ["oci.pull", "oci.push"],
  "leaseTtlSeconds": 3600
}
JSON
```

- `repositoryNamespace` is the Docker Hub namespace your repositories live under
   (an org or user; for official images it is `library`).
- `credentialsAuthentication` **references** the Secret — it carries no token.
- `leaseTtlSeconds` is optional (see [lease TTL](README.md#lease-ttl)).

A `201` returns the instance; capture its id:

```json
{
  "workspaceId": "ws-default",
  "instanceId": "conn-...",
  "type": "custos-dockerhub",
  "version": "0.1.0",
  "name": "dockerhub-prod",
  "enabled": true,
  "status": "active",
  "healthStatus": "unknown",
  "targetConfig": { "repositoryNamespace": "<docker-namespace>" },
  "leaseTtlSeconds": 3600,
  "usedCapabilities": ["oci.pull", "oci.push"]
}
```

```bash
export CONN=conn-...   # copy instanceId from the response
```

## 4. Reference the connector from a workflow

Workflow steps reference a connector instance **by name**. The Workflow Service
binds it automatically at step-execution time (you never call the bind RPC
yourself). A step that consumes a single connector:

```yaml
spec:
  steps:
    - id: pull-image
      activity: <activity>@1
      connector: dockerhub-prod        # the instance name from step 3
```

A step that consumes two connectors (e.g. a copy from a source to a
destination) names each one under a slot:

```yaml
    - id: copy
      activity: <activity>@1
      connectors:
        source: dockerhub-prod
        destination: ghcr-prod
```

`connector` and `connectors` are mutually exclusive. The activity declares the
capabilities it needs per slot; the binder rejects the step before execution if
the named instance does not advertise them.

### Worked example: copy Docker Hub -> GHCR

With a `dockerhub` connector in the `source` slot and a `ghcr` connector in
the `dest` slot, the out-of-the-box
[`copy-image`](../../../extensions/activities/copy-image/README.md) activity
copies an image between the two registries. A workflow step:

```yaml
spec:
  steps:
    - id: copy-hello-world
      activity: custos.builtin/copy-image@0
      connectors:
        source: dockerhub-prod        # this Docker Hub instance (oci.pull)
        dest: ghcr-prod               # a GHCR instance (oci.push)
      with:
        source:
          ref: docker.io/library/hello-world:latest
        destination:
          repository: octo-org/hello-world
          tag: mirrored
        # copyReferrers: true         # also copy signatures / SBOM / attestations
        # allPlatforms: true          # copy every arch in the manifest list
```

On success the step's outputs include the destination reference and digest:

```json
{
  "destinationRef": "ghcr.io/octo-org/hello-world:mirrored",
  "digest": "sha256:...",
  "manifestsCopied": 1
}
```

The activity requires `oci.pull` on the `source` slot and `oci.push` on the
`dest` slot; the binder rejects the step before execution if either named
instance does not advertise the capability. See the
[copy-image README](../../../extensions/activities/copy-image/README.md) for
the full input/output and error-code reference.

## 5. Verify & operate

Read the cached health snapshot (`connector:read`):

```bash
curl -sS "$GATEWAY/v1/workspaces/$WS/connectors/$CONN/health" \
  -H "authorization: Bearer $TOKEN"
```

```json
{
  "workspaceId": "ws-default",
  "instanceId": "conn-...",
  "healthy": true,
  "detail": "registry reachable; Bearer challenge advertises the Docker Hub token endpoint",
  "checkedAt": "2026-06-27T00:00:00Z",
  "source": "cache"
}
```

Force a live probe (bypasses the cache; `admin:connector`):

```bash
curl -sS -X POST "$GATEWAY/v1/workspaces/$WS/connectors/$CONN:force-health-check" \
  -H "authorization: Bearer $TOKEN"
```

Disable or re-enable the instance (`admin:connector`):

```bash
curl -sS -X POST "$GATEWAY/v1/workspaces/$WS/connectors/$CONN:disable" \
  -H "authorization: Bearer $TOKEN"
curl -sS -X POST "$GATEWAY/v1/workspaces/$WS/connectors/$CONN:enable" \
  -H "authorization: Bearer $TOKEN"
```

### What `health` does (and does not) check

`health` is a live **unauthenticated** reachability probe: it confirms
`registry-1.docker.io` is reachable and speaks the OCI protocol. It does **not**
validate your PAT — credential problems surface when a workflow step actually
uses the connector.

### Troubleshooting

| Symptom | Likely cause |
|---|---|
| `healthy: false`, detail mentions *unreachable* | Egress/DNS/TLS to `registry-1.docker.io` blocked from the cluster. |
| Step fails with an auth error at runtime | PAT missing/expired, wrong `username`, or insufficient scope (need write to push). Re-check the Secret. |
| Step fails resolving the credential | Secret name/namespace/keys in `credentialsAuthentication` don't match the Secret, or `connectorCredentials.enabled` RBAC is not installed. |
| `429` from the registry | Docker Hub pull-rate limits — use an authenticated/paid plan or a pull-through cache for high volume. |
