# Using the out-of-the-box registry connectors

Last Updated: 2026-06-27

Custos ships two **out-of-the-box (OOTB) registry connectors** that let your
workflows read from and write to container registries:

| Connector | Type | Registry | Guide |
|---|---|---|---|
| Docker Hub | `custos-dockerhub` | `registry-1.docker.io` | [dockerhub.md](dockerhub.md) |
| GHCR (GitHub Container Registry) | `custos-ghcr` | `ghcr.io` | [ghcr.md](ghcr.md) |

Both expose the same OCI capabilities — `oci.pull`, `oci.push`,
`oci.list-tags`, `oci.list-referrers` — and use the same credential model, so
the two guides share the same five-step shape. This page covers the concepts
common to both; follow the per-connector guide for the registry-specific
details (PAT creation, namespaces, caveats).

> These are **operator** guides. M1 has no Web UI. Most actions are HTTP API
> calls through the gateway (examples use `curl` with a service token — see
> [First Use](../evaluation/first-workflow.md) for how to obtain one); creating
> the credential Secret (step 1) uses `kubectl`, and registering a connector
> type (step 2) is a control-plane action.

## The five-step lifecycle

```ini
1. Create the credential Secret   (kubectl — store the registry PAT)
2. Register the connector type    (control-plane — usually pre-seeded)
3. Create a connector instance    (API — bind the type to a namespace + secret)
4. Reference it from a workflow    (workflow YAML — connector: <instance>)
5. Verify & operate               (API — health, enable/disable)
```

Step 1 is a `kubectl` action and step 2 is a control-plane action (see below).
Steps 3 and 5 are operator API calls against the gateway. Step 4 happens in
your workflow definition; the actual credential binding is performed
automatically by the Workflow Service at step-execution time.

## Credential model (`x-dapr-secret`)

Both connectors resolve their registry credential through the platform's
unified **`x-dapr-secret`** mechanism. You never hand a token to the API:

1. You store the registry **Personal Access Token (PAT)** in a Kubernetes
   Secret in the connector-credentials namespace (`custos-connectors` by
   default).
2. The connector **instance** references that Secret (by name + keys), not the
   token value.
3. At runtime the connector sidecar leases the resolved credential to the
   workflow step; the plugin itself never sees the raw token.

The namespace-scoped RBAC that lets the connector-service read those Secrets is
installed by the umbrella chart when `connectorCredentials.enabled` is set — so
you can create Secrets at runtime with `kubectl` without a Helm upgrade.

### Two-layer tokens

Both registries use the standard OCI auth flow, which involves **two**
credentials:

- **Layer 1 — the durable PAT** you store in the Secret (Docker Hub PAT /
   GitHub PAT). This is what `x-dapr-secret` resolves and leases.
- **Layer 2 — a short-lived, per-repository registry bearer** that the
   registry's token endpoint mints in exchange for the PAT.

The connector delivers the PAT to the activity as HTTP **Basic** credentials
(`tokenTypeHint: "basic"`) plus the registry's token endpoint; the consuming
activity performs the Layer-2 exchange. You don't configure any of this — it is
shaped automatically by `bind`.

## Authentication & permissions

All operator calls go through the API gateway with a bearer service token:

```bash
export GATEWAY=https://custos.local   # your gateway endpoint
export WS=ws-default                  # your workspace id
export TOKEN=custos_...               # a Custos service token
```

The token's principal must carry the right permissions:

| Action | Permission |
|---|---|
| List / get / read health of connectors and types | `connector:read` |
| Create / update / enable / disable / force-health-check | `admin:connector` |
| Register a connector **type** | `connector:register` (control-plane only) |

## Lease TTL

The PAT is durable, so a longer lease reduces token-exchange churn during long
operations while staying revocable. Set it per __instance__ with
`leaseTtlSeconds` on create (recommended `3600` / 1 hour). The platform default
is 10 minutes (`CONN_SIDECAR_DEFAULT_TTL`); a lease never outlives the step.

## Related references

- [Connections API](../../developers/connections-api.md) — connector manifest
   schema, the workflow step binding model, and the sidecar/lease contract.
- [Connector plugin author guide](../../developers/connector-plugin-author.md) —
   §8 documents the `connectors:register` registration RPC.
- Per-connector READMEs in `extensions/connectors/<name>/` — the connector's own
   manifest, hooks, and config reference.
