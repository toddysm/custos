# custos-connector-service

Custos Connector Service (COMP-005).

Connector Service is the access broker between workflows / activities and
external systems. It owns the connector type registry, the connector
instance lifecycle, capability matching at publish, context issuance to
running activities (via the secret-bridge sidecar), and the trigger
listen / pull streams that feed the Trigger Service.

## Status

CONN-IMPL-001 ([#284](https://github.com/toddysm/custos/issues/284), Phase A)
— scaffold. The package skeleton, the `create_app()` factory exposing
`/healthz` + `/readyz` so the IMPL-002 Helm chart can pass its liveness /
readiness gates, the `python -m custos_connector` entry point, and the CI
gate (`.github/workflows/python-services.yml`) are real. Everything else —
providers, middleware, REST surface, sidecar — is incremental work tracked
under [#318](https://github.com/toddysm/custos/issues/318) (CONN-IMPL-000).

Tracking issue: [#318](https://github.com/toddysm/custos/issues/318) (CONN-IMPL-000).

Design reference:
[`design/components/connector-service/design.md`](../../../design/components/connector-service/design.md).

## Configuration (planned, materialized incrementally)

Per the design (§ Internal Structure, § Identity and Credential Model,
§ Operator Admin Surface, § Pull Cursor Model), the runtime will read the
following `CONN_*` environment variables. The CI scaffold does not consume
any of them yet — they land alongside the providers (Phase B), the sidecar
(Phase H), and the listen / pull manager (Phase I).

| Variable | Required | Default | Description |
|---|---|---|---|
| `CONN_CATALOG_STORE` | Yes | — | `CatalogStoreProvider` binding (Postgres DSN in v1). Used for the `ConnectorTypeVersion` registry. |
| `CONN_METADATA_STORE` | Yes | — | `MetadataStoreProvider` binding (Postgres DSN in v1). Used for `ConnectorInstance`, leases, and pull cursors. |
| `CONN_AUTHZ_ENDPOINT` | Yes (prod) | — | AuthN/AuthZ Service endpoint. Empty switches the call-context middleware to a dev-shim and refuses to start when `ENVIRONMENT=production`. |
| `CONN_CATALOG_ENDPOINT` | Yes | — | Catalog Service endpoint for `ResolveConnectorTypeRef` lookups. |
| `CONN_OCI_REFERRERS_TIMEOUT_MS` | No | `5000` | OCI Referrers API per-request timeout. |
| `CONN_PUBLISH_MAX_BODY_MB` | No | `4` | Maximum ConnectorManifest size at registry write. |
| `CONN_SIDECAR_DEFAULT_TTL` | No | `600` (10 min) | Platform-wide default lease TTL when none is specified. |
| `CONN_SIDECAR_MTLS_ISSUER` | Yes (prod) | — | Issuer name for the sidecar control-channel mTLS certs. |
| `CONN_LEASE_MAX_CONCURRENT` | No | `16` | Per-step concurrent-lease cap. |
| `CONN_PULL_LOOP_MIN_INTERVAL_SEC` | No | `10` | Minimum interval between pull-loop ticks. |
| `ENVIRONMENT` | No | `development` | Operational env tag. The dev-shim refuses to start when this is `production` (case-insensitive). |

Process bind:

| Variable | Default | Purpose |
|---|---|---|
| `HOST` | `0.0.0.0` | Address the uvicorn process binds to. |
| `PORT` | `8080` | Port the uvicorn process listens on. |

## Local development

```bash
cd src/services/connector-service
pip install -e ".[dev]"
ruff check .
ruff format --check .
mypy src tests
pytest -q
```

`python -m custos_connector` starts a FastAPI app exposing only
`/healthz` and `/readyz` (both returning `{"status": "ok"}` with HTTP 200).
Providers, middleware, and the REST surface are wired across CONN-IMPL-003 /
CONN-IMPL-004 / CONN-IMPL-026.

## Layout

```
src/services/connector-service/
├── README.md
├── pyproject.toml
├── src/
│   └── custos_connector/
│       ├── __init__.py        # create_app() factory (healthz/readyz probes only)
│       ├── __main__.py        # uvicorn entry point
│       └── py.typed
└── tests/
    ├── __init__.py
    └── test_smoke.py
```
