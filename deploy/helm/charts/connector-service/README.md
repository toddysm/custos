# connector-service

Custos connector-service component chart. See
[`design/components/connector-service/design.md`](../../../../design/components/connector-service/design.md)
for the design.

## What this chart deploys

A Deployment + Service + ServiceAccount + ConfigMap for the connector-service
process, optionally with an ExternalSecret projecting the two Postgres DSNs
the service reads at startup. The Deployment binds:

| Container env source | Carries |
|---|---|
| ConfigMap `<release>-connector-service` | `CONN_OCI_REFERRERS_TIMEOUT_MS`, `CONN_PUBLISH_MAX_BODY_MB`, `CONN_SIDECAR_DEFAULT_TTL`, `CONN_LEASE_MAX_CONCURRENT`, `CONN_PULL_LOOP_MIN_INTERVAL_SEC`, `CONN_CATALOG_ENDPOINT`, `CONN_AUTHZ_ENDPOINT`, `CONN_SIDECAR_MTLS_ISSUER` |
| Secret `<release>-connector-service` (materialized by ExternalSecret when `externalSecret.enabled=true`) | `CONN_CATALOG_STORE`, `CONN_METADATA_STORE` (Postgres DSNs) |

`HOST` / `PORT` come from the container image defaults (`0.0.0.0:8080`); the
Service port mirrors that.

The secret-bridge sidecar (CONN-IMPL-019..021 / #302..#304) is **not**
deployed by this chart — per the design's M1 implementation note it is
co-deployed with activity pods by the Activity Runtime Manager (COMP-006).
Until ARM lands, the sidecar image is exercised only in standalone
integration tests.

## Values overview

See [`values.yaml`](values.yaml) for the full list. The keys most operators
will touch:

- `config.publishMaxBodyMb`, `config.sidecarDefaultTtlSeconds`, `config.leaseMaxConcurrent`, `config.pullLoopMinIntervalSec`, `config.ociReferrersTimeoutMs`
- `catalog.endpoint`, `auth.endpoint`
- `sidecar.mtlsIssuer`
- `externalSecret.enabled` — flip on for HA / connected profiles (the
  umbrella `values-*-ha.yaml` already does this).
- `externalSecret.data[]` — override the upstream secret-store key paths
  when the operator-chosen layout differs from
  `custos/storage-provider-layer/<tier>`.

### `auth.endpoint` / `CONN_AUTHZ_ENDPOINT` semantics

`auth.endpoint` projects directly into `CONN_AUTHZ_ENDPOINT`. The default
(`http://auth-service:8080`) is the right value for any standard in-cluster
topology and must be non-empty in production. Setting it to the empty
string (e.g. `--set auth.endpoint=""` for local development) will switch
the service to a dev-shim call-context middleware (landing with
CONN-IMPL-004 / #287). The shim is expected to refuse to start when the
container's `ENVIRONMENT` env var is `production` (case-insensitive),
matching the catalog-service convention.
