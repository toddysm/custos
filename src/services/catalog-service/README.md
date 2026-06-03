# catalog-service

Custos Catalog Service (COMP-007). Owns workflow + template definitions,
the activity-type / connector-type read-side index, and the publish-time
validation gate (schema, CEL syntactic + name-binding, reference resolution,
digest pinning).

Design: [`design/components/catalog-service/design.md`](../../../design/components/catalog-service/design.md).

**Implemented** — the CS-IMPL-000 milestone ([#226](https://github.com/toddysm/custos/issues/226))
is complete; all 24 child tasks (CS-IMPL-001 … CS-IMPL-024) are merged and the
tracking issue is closed. The service exposes its full REST + Internal RPC
surface: workflow / template publish, normalization, schema + publish-time CEL
syntactic / name-binding validation, reference resolution, versioning +
immutability enforcement, the template engine (`:materialize`), the activity-type
and connector-type read-side registries, `GetWorkflowVersion` /
`ResolveConnectorTypeRef` Internal RPC, SPL provider wiring, the call-context
middleware (real Auth Service integration, CS-IMPL-024), the live Connector
Service client (CS-IMPL-023 / CONN-IMPL-034), observability + audit emission,
and the `/healthz` + `/readyz` probes. Backed by a unit + integration suite at
the ≥90 % coverage gate (CS-IMPL-020/021) and developer docs at
[`docs/developers/catalog-api.md`](../../../docs/developers/catalog-api.md)
(CS-IMPL-022).

## Configuration

Per [`design/components/catalog-service/design.md`](../../../design/components/catalog-service/design.md) § Configuration:

| Variable | Required | Default | Description |
|---|---|---|---|
| `CAT_DEFINITION_STORE` | Yes | — | `DefinitionStoreProvider` binding (Postgres DSN in v1). |
| `CAT_CATALOG_STORE` | Yes | — | `CatalogStoreProvider` binding (Postgres DSN in v1). |
| `CAT_CONNECTOR_ENDPOINT` | Yes | — | Connector Service endpoint for the `ValidateConnector` Internal RPC (publish-time connector-instance existence check, CONN-IMPL-034 / CS-IMPL-023). |
| `CAT_CONNECTOR_TIMEOUT_SECONDS` | No | `2.0` | Per-call timeout for the Connector Service client. |
| `CAT_CONNECTOR_NEGATIVE_CACHE_TTL_SECONDS` | No | `5.0` | TTL for the in-process negative-result cache used on 404 responses from Connector Service. |
| `CAT_USE_STUB_CONNECTOR_CLIENT` | No | `false` | Feature flag. When `true`, the catalog wires the offline `StubConnectorClient` (always-`True`) instead of the live HTTP client. Use only in airgapped or offline tests; production must leave this unset. |
| `CAT_AUTHZ_ENDPOINT` | Yes (prod) | — | AuthN/AuthZ Service endpoint. Leaving this empty switches the call-context middleware to a dev-shim that trusts `x-custos-callctx` verbatim, logs a WARNING per request, and refuses to start when `ENVIRONMENT=production`. See [`design/components/catalog-service/design.md`](../../../design/components/catalog-service/design.md) § Configuration. |
| `CAT_PUBLISH_MAX_BODY_MB` | No | `4` | Maximum workflow/template document size at publish. |
| `CAT_CEL_PARSE_TIMEOUT_MS` | No | `500` | Per-expression parse timeout at publish (separate from runtime evaluation timeout in WF). |
| `CAT_DEFAULT_NAMESPACE_TIER_VENDOR` | No | — | Optional default vendor namespace for short-form publishes (unused in M1). |
| `ENVIRONMENT` | No | `development` | Operational env tag. The dev-shim refuses to start when this is `production` (case-insensitive). |

Process bind:

| Variable | Default | Purpose |
|---|---|---|
| `HOST` | `0.0.0.0` | Address the uvicorn process binds to. |
| `PORT` | `8080` | Port the uvicorn process listens on. |

## Local development

```bash
cd src/services/catalog-service
pip install -e ".[dev]"
ruff check .
ruff format --check .
mypy src tests
pytest -q
```

`python -m custos_catalog` starts the FastAPI app with the full REST +
Internal RPC surface plus the `/healthz` and `/readyz` probes.

## Layout

```
src/services/catalog-service/
├── README.md
├── pyproject.toml
├── openapi.json
├── src/
│   └── custos_catalog/
│       ├── __init__.py        # create_app() factory (full app wiring)
│       ├── __main__.py        # uvicorn entry point
│       ├── api/               # FastAPI REST surface + Internal RPC routers
│       ├── managers/          # definition, versioning, template, registries
│       ├── clients/           # Connector Service client
│       ├── middleware/        # call-context middleware (+ dev shim)
│       ├── schema/            # workflow + template JSON Schema validators
│       ├── cel_validate.py    # publish-time CEL syntactic + name-binding gate
│       ├── normalize.py       # canonical document normalizer + hashing
│       ├── resolve.py         # reference resolver
│       ├── versioning.py      # versioning + immutability enforcement
│       ├── template_engine.py # :materialize
│       ├── providers.py       # SPL provider wiring + schema-revision gate
│       ├── _telemetry.py      # OpenTelemetry hooks
│       ├── audit.py           # audit event emission
│       └── py.typed
└── tests/
```
