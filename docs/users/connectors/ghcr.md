# GHCR connector (`custos-ghcr`)

Last Updated: 2026-06-27

This guide walks the full lifecycle of using the out-of-the-box **GHCR (GitHub
Container Registry)** connector to let your workflows push to and pull from
[`ghcr.io`](https://ghcr.io). Read
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

Create a **GitHub Personal Access Token (classic)** (GitHub -> *Settings ->
Developer settings -> Personal access tokens -> Tokens (classic)*) with:

- `write:packages` — required to push,
- `read:packages` — required to pull **private** packages (public packages pull
  anonymously),
- `delete:packages` — only if you intend to delete.

Store it as a Kubernetes Secret in the connector-credentials namespace:

```bash
kubectl create secret generic ghcr-pat \
  --namespace custos-connectors \
  --from-literal=username='<github-username>' \
  --from-literal=token='<github-pat>'
```

The Secret must expose the username under the `username` key and the PAT under
the `token` key (referenced by the instance in step 3). The connector-service is
granted namespace-scoped read access by the umbrella chart
(`connectorCredentials.enabled`), so no Helm upgrade is needed.

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
  --data-urlencode 'type=custos-ghcr'
```

A `200` lists the registered versions:

```json
{
  "items": [
    {
      "type": "custos-ghcr",
      "version": "0.1.0",
      "digest": "sha256:...",
      "imageRef": "ghcr.io/.../custos-ghcr@sha256:...",
      "deprecated": false
    }
  ],
  "nextCursor": null
}
```

If the list is empty, ask your platform operator to register the type.

## 3. Create a connector instance

A connector **instance** binds the type to a specific GitHub owner (org or user)
and the credential Secret from step 1. Requires `admin:connector`:

```bash
curl -sS -X POST "$GATEWAY/v1/workspaces/$WS/connectors" \
  -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -d @- <<'JSON'
{
  "type": "custos-ghcr",
  "version": "0.1.0",
  "name": "ghcr-prod",
  "targetConfig": {
    "repositoryNamespace": "<github-org-or-user>"
  },
  "credentialsAuthentication": {
    "secretName": "ghcr-pat",
    "usernameKey": "username",
    "tokenKey": "token",
    "namespace": "custos-connectors"
  },
  "usedCapabilities": ["oci.pull", "oci.push"],
  "leaseTtlSeconds": 3600
}
JSON
```

- `repositoryNamespace` is the GitHub org or user that owns the packages
  (the `<owner>` in `ghcr.io/<owner>/<image>`).
- `credentialsAuthentication` **references** the Secret — it carries no token.
- `leaseTtlSeconds` is optional (see [lease TTL](README.md#lease-ttl)).

A `201` returns the instance; capture its id:

```json
{
  "workspaceId": "ws-default",
  "instanceId": "conn-...",
  "type": "custos-ghcr",
  "version": "0.1.0",
  "name": "ghcr-prod",
  "enabled": true,
  "status": "active",
  "healthStatus": "unknown",
  "targetConfig": { "repositoryNamespace": "<github-org-or-user>" },
  "leaseTtlSeconds": 3600,
  "usedCapabilities": ["oci.pull", "oci.push"]
}
```

```bash
export CONN=conn-...   # copy instanceId from the response
```

> **Package visibility.** A package's first push is **private** by default and
> owned by the namespace above. The PAT's account must have write access to that
> namespace; org packages may additionally require the package to be linked to a
> repository or granted role access.

## 4. Reference the connector from a workflow

Workflow steps reference a connector instance **by name**. The Workflow Service
binds it automatically at step-execution time (you never call the bind RPC
yourself). A step that consumes a single connector:

```yaml
spec:
  steps:
    - id: push-image
      activity: <activity>@1
      connector: ghcr-prod            # the instance name from step 3
```

A step that copies from a source to a destination names each one under a slot:

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

> A worked **Docker Hub -> GHCR copy** example will be added here once the
> copy-image activity ships (tracked in #889).

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
  "detail": "registry reachable; Bearer challenge advertises the GHCR token endpoint",
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

`health` is a live **unauthenticated** reachability probe: it confirms `ghcr.io`
is reachable and speaks the OCI protocol. It does **not** validate your PAT —
credential problems surface when a workflow step actually uses the connector.

### Troubleshooting

| Symptom | Likely cause |
|---|---|
| `healthy: false`, detail mentions *unreachable* | Egress/DNS/TLS to `ghcr.io` blocked from the cluster. |
| Step fails with an auth error at runtime | PAT missing/expired or missing `write:packages` / `read:packages`. Re-check the Secret. |
| `denied` / `403` on push | The PAT's account lacks write access to `<owner>`, or the org package isn't linked/role-granted. |
| Step fails resolving the credential | Secret name/namespace/keys in `credentialsAuthentication` don't match the Secret, or `connectorCredentials.enabled` RBAC is not installed. |
