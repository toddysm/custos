# catalog-service

Custos catalog-service component chart. See
[`design/components/catalog-service/design.md`](../../../../design/components/catalog-service/design.md)
for the design.

## What this chart deploys

A Deployment + Service + ServiceAccount + ConfigMap for the catalog-service
process, optionally with an ExternalSecret projecting the two Postgres DSNs
the service reads at startup. The Deployment binds:

| Container env source | Carries |
|---|---|
| ConfigMap `<release>-catalog-service` | `CAT_PUBLISH_MAX_BODY_MB`, `CAT_CEL_PARSE_TIMEOUT_MS`, `CAT_CONNECTOR_ENDPOINT`, `CAT_AUTHZ_ENDPOINT`, optional `CAT_DEFAULT_NAMESPACE_TIER_VENDOR` |
| Secret `<release>-catalog-service` (materialized by ExternalSecret when `externalSecret.enabled=true`) | `CAT_DEFINITION_STORE`, `CAT_CATALOG_STORE` (Postgres DSNs) |

`HOST` / `PORT` come from the container image defaults (`0.0.0.0:8080`); the
Service port mirrors that.

## Values overview

See [`values.yaml`](values.yaml) for the full list. The keys most operators
will touch:

- `config.publishMaxBodyMb`, `config.celParseTimeoutMs`
- `connector.endpoint`, `auth.endpoint`
- `externalSecret.enabled` — flip on for HA / connected profiles (the
  umbrella `values-*-ha.yaml` already does this).
- `externalSecret.data[]` — override the upstream secret-store key paths
  when the operator-chosen layout differs from
  `custos/storage-provider-layer/<tier>`.
