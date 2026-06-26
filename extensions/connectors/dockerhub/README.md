# Custos Docker Hub connector (`custos-dockerhub`)

Out-of-the-box (OOTB) connector that lets Custos workflows pull from and
push to [Docker Hub](https://hub.docker.com) using a durable Docker Hub
**Personal Access Token (PAT)** resolved through the `x-dapr-secret`
credential mechanism.

The plugin is intentionally thin: it shapes the data-plane
`ConnectorContext` (`bind`) and reports liveness (`health`). It does
**not** see the resolved PAT and does **not** mint registry tokens. The
actual byte movement and the per-repository token exchange are performed
by the consuming OCI activity.

| | |
|---|---|
| Connector type | `custos-dockerhub` |
| Version | `0.1.0` |
| Contract version | `1` |
| Target kind | `oci-registry` |
| Endpoint | `https://registry-1.docker.io` |
| Capabilities | `oci.pull`, `oci.push`, `oci.list-tags`, `oci.list-referrers` |
| Credential mechanism | `x-dapr-secret` |

## Two-layer token model

Docker Hub uses the standard OCI registry auth flow, which involves two
distinct credentials:

1. **Layer 1 — the Personal Access Token (PAT).** Durable, user-scoped,
   stored as a Kubernetes Secret and resolved by the connector-service
   `x-dapr-secret` resolver. The PAT is leased to a workflow step through
   the connector sidecar; the plugin only ever receives a *reference*
   (`secretName`, `usernameKey`, `tokenKey`, `namespace`) — never the
   secret value.
2. **Layer 2 — a short-lived registry bearer.** Minted per repository by
   `https://auth.docker.io/token` in exchange for the PAT (presented as
   HTTP Basic credentials). Because the repository scope is unknown at
   `bind` time, the plugin cannot pre-mint this token — so `bind`
   advertises `tokenTypeHint: "basic"` and hands the consuming activity
   everything it needs (`tokenEndpoint`, `service`) to run the exchange
   itself.

The reusable consumer-side pattern (credential helper + proactive
re-minting at ~80% of lease TTL) is described in
[`design/architecture/registry-credential-refresh.md`](../../../design/architecture/registry-credential-refresh.md).

## Hooks

### `bind`
Deterministic, no network I/O. Pins the target to a Docker Hub registry
host over HTTPS (`registry-1.docker.io` / `registry.docker.io`) and fails
fast otherwise, derives
`https://registry-1.docker.io/v2/<repositoryNamespace>`, guards the
requested capability against the advertised set, and returns:

```json
{
  "endpoint": "https://registry-1.docker.io/v2/<namespace>",
  "tokenTypeHint": "basic",
  "handle": { "slot": "...", "capability": "...", "instanceId": "..." },
  "extras": {
    "registryKind": "oci-registry",
    "registryProvider": "dockerhub",
    "tokenEndpoint": "https://auth.docker.io/token",
    "service": "registry.docker.io",
    "verifyTls": true
  }
}
```

`registryKind` names the protocol so consumers can pick an OCI client;
`registryProvider` carries the vendor for any Docker-Hub-specific handling.
The sidecar maps `handle` onto the leased PAT at the data plane.

### `health`
A live, **unauthenticated** reachability probe: `GET /v2/` against
`registry-1.docker.io`, asserting a `401` carrying
`WWW-Authenticate: Bearer realm="https://auth.docker.io/token",service="registry.docker.io"`.
That confirms the registry is reachable, speaks the OCI distribution
protocol, and advertises the expected token endpoint — all without
credentials. The endpoint is pinned to Docker Hub hosts before any request
is issued (an unexpected endpoint yields an unhealthy verdict with no
network call). Credential *validity* surfaces when the activity uses the
lease, not here.

## Configuration

### Target
| Field | Description | Default |
|---|---|---|
| `repositoryNamespace` | Docker Hub namespace (org or user) repositories live under. | `library` |

### Credentials (`x-dapr-secret`)
The referenced Kubernetes Secret must expose a Docker Hub username and a
PAT:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: dockerhub-pat
  namespace: custos-connectors
type: Opaque
stringData:
  username: <docker-hub-username>
  token: <docker-hub-personal-access-token>
```

| Field | Description | Default |
|---|---|---|
| `secretName` | Name of the Kubernetes Secret. | `dockerhub-pat` |
| `usernameKey` | Secret key holding the username. | `username` |
| `tokenKey` | Secret key holding the PAT. | `token` |
| `namespace` | Namespace the Secret lives in. | `custos-connectors` |

> Create the Secret in the connector-credentials namespace at runtime — no
> Helm upgrade is required. The namespace-scoped RBAC granting the
> connector-service read access is installed by the umbrella chart
> (`connectorCredentials.enabled`).

### Lease TTL
The PAT is durable, so a longer lease reduces re-minting churn during long
copies while staying revocable. Set the lease TTL on the connector
**instance** (recommended `3600` / 1 h):

```yaml
lease:
  ttl: 3600
```

> A connector-*type* lease ceiling (`credentials.maxLeaseTtl` in the
> manifest) is **not** expressible in `connector-manifest.v1` — the
> `credentials` block is closed to `authenticationType` + `authentication`.
> Adding a manifest-level ceiling would require a schema extension
> (tracked as a connector-service follow-up).

## Rate limits

Docker Hub enforces pull-rate limits per account/IP. The Layer-2 token
exchange (run by the activity, not the plugin) may receive `429` responses;
operators should provision a paid Docker Hub plan or a pull-through cache
for high-volume workloads. The plugin's unauthenticated `health` probe is
not subject to the same pull caps.

## Development

```bash
pip install -e ".[dev]"
ruff check . && ruff format --check .
mypy -p dockerhub_plugin && mypy tests
pytest -q
docker build -t custos-plugin-dockerhub:dev .
```
