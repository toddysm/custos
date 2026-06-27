# Custos GHCR connector (`custos-ghcr`)

Out-of-the-box (OOTB) connector that lets Custos workflows push to and pull
from [GHCR (GitHub Container Registry)](https://ghcr.io) using a durable
**GitHub Personal Access Token** (with `write:packages`) resolved through
the `x-dapr-secret` credential mechanism.

The plugin is intentionally thin: it shapes the data-plane
`ConnectorContext` (`bind`) and reports liveness (`health`). It does
**not** see the resolved PAT and does **not** mint registry tokens. The
actual byte movement and the per-repository token exchange are performed
by the consuming OCI activity.

| | |
|---|---|
| Connector type | `custos-ghcr` |
| Version | `0.1.0` |
| Contract version | `1` |
| Target kind | `oci-registry` |
| Endpoint | `https://ghcr.io` |
| Capabilities | `oci.pull`, `oci.push`, `oci.list-tags`, `oci.list-referrers` |
| Credential mechanism | `x-dapr-secret` |

## Two-layer token model

GHCR uses the standard OCI registry auth flow, which involves two distinct
credentials:

1. **Layer 1 — the GitHub Personal Access Token (PAT).** Durable,
   user/org-scoped (needs `write:packages` to push, `read:packages` to
   pull private packages), stored as a Kubernetes Secret and resolved by
   the connector-service `x-dapr-secret` resolver. The PAT is leased to a
   workflow step through the connector sidecar; the plugin only ever
   receives a *reference* (`secretName`, `usernameKey`, `tokenKey`,
   `namespace`) — never the secret value.
2. **Layer 2 — a short-lived registry bearer.** Minted per repository by
   `https://ghcr.io/token` in exchange for the PAT (presented as HTTP
   Basic credentials). Because the repository scope is unknown at `bind`
   time, the plugin cannot pre-mint this token — so `bind` advertises
   `tokenTypeHint: "basic"` and hands the consuming activity everything it
   needs (`tokenEndpoint`, `service`) to run the exchange itself.

The reusable consumer-side pattern (credential helper + proactive
re-minting at ~80% of lease TTL) is described in
[`design/architecture/registry-credential-refresh.md`](../../../design/architecture/registry-credential-refresh.md).

## Hooks

### `bind`
Deterministic, no network I/O. Pins the target to `ghcr.io` over HTTPS and
fails fast otherwise, derives `https://ghcr.io/v2/<repositoryNamespace>`,
guards the requested capability against the advertised set, and returns:

```json
{
  "endpoint": "https://ghcr.io/v2/<namespace>",
  "tokenTypeHint": "basic",
  "handle": { "slot": "...", "capability": "...", "instanceId": "..." },
  "extras": {
    "registryKind": "oci-registry",
    "registryProvider": "ghcr",
    "tokenEndpoint": "https://ghcr.io/token",
    "service": "ghcr.io",
    "verifyTls": true
  }
}
```

`registryKind` names the protocol so consumers can pick an OCI client;
`registryProvider` carries the vendor for any GHCR-specific handling. The
sidecar maps `handle` onto the leased PAT at the data plane.

### `health`
A live, **unauthenticated** reachability probe: `GET /v2/` against
`ghcr.io`, asserting a `401` carrying
`WWW-Authenticate: Bearer realm="https://ghcr.io/token",service="ghcr.io"`.
That confirms the registry is reachable, speaks the OCI distribution
protocol, and advertises the expected token endpoint — all without
credentials. The endpoint is pinned to `ghcr.io` before any request is
issued (an unexpected endpoint yields an unhealthy verdict with no network
call). Credential *validity* surfaces when the activity uses the lease, not
here.

## Configuration

### Target
| Field | Description | Default |
|---|---|---|
| `repositoryNamespace` | GHCR namespace — the owning GitHub org or user the packages live under. | `octo-org` (example; override per instance) |

### Credentials (`x-dapr-secret`)
The referenced Kubernetes Secret must expose a GitHub username and a PAT
with `write:packages` (and `read:packages` for private pulls):

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: ghcr-pat
  namespace: custos-connectors
type: Opaque
stringData:
  username: <github-username>
  token: <github-personal-access-token>
```

| Field | Description | Default |
|---|---|---|
| `secretName` | Name of the Kubernetes Secret. | `ghcr-pat` |
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

## Package visibility & permissions

A package's first push is **private** by default and is owned by the
GitHub org/user in `repositoryNamespace`. Ensure the PAT's account has
write access to that namespace (org packages may additionally require the
package to be linked to a repository or granted Actions/role access).
Pulling a private package needs `read:packages`; public packages pull
anonymously.

## Rate limits

GHCR enforces abuse/rate limits per account/IP. The Layer-2 token exchange
(run by the activity, not the plugin) and the data-plane pulls/pushes may
receive `429`/`403` responses under heavy load; operators should stagger
high-volume workloads. The plugin's unauthenticated `health` probe is not
subject to the same caps.

## Development

```bash
pip install -e ".[dev]"
ruff check . && ruff format --check .
mypy -p ghcr_plugin && mypy tests
pytest -q
docker build -t custos-plugin-ghcr:dev .
```
