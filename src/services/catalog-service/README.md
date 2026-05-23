# catalog-service

Custos Catalog Service (COMP-007). Owns workflow + template definitions,
the activity-type / connector-type read-side index, and the publish-time
validation gate (schema, CEL syntactic + name-binding, reference resolution,
digest pinning).

Design: [`design/components/catalog-service/design.md`](../../../design/components/catalog-service/design.md).

## Status

**Scaffold only** — CS-IMPL-001 ([#202](https://github.com/toddysm/custos/issues/202)).
The package skeleton, the `create_app()` factory placeholder, the `python -m custos_catalog`
entry point, and the CI gate (`.github/workflows/python-services.yml`) are real.
Everything else is incremental work tracked under [#226](https://github.com/toddysm/custos/issues/226)
(CS-IMPL-000).

## Configuration

Per [`design/components/catalog-service/design.md`](../../../design/components/catalog-service/design.md) § Configuration:

| Variable | Required | Default | Description |
|---|---|---|---|
| `CAT_DEFINITION_STORE` | Yes | — | `DefinitionStoreProvider` binding (Postgres DSN in v1). |
| `CAT_CATALOG_STORE` | Yes | — | `CatalogStoreProvider` binding (Postgres DSN in v1). |
| `CAT_CONNECTOR_ENDPOINT` | Yes | — | Connector Service endpoint for `ExistsConnectorInstance`. |
| `CAT_AUTHZ_ENDPOINT` | Yes | — | AuthN/AuthZ Service endpoint. |
| `CAT_PUBLISH_MAX_BODY_MB` | No | `4` | Maximum workflow/template document size at publish. |
| `CAT_CEL_PARSE_TIMEOUT_MS` | No | `500` | Per-expression parse timeout at publish (separate from runtime evaluation timeout in WF). |
| `CAT_DEFAULT_NAMESPACE_TIER_VENDOR` | No | — | Optional default vendor namespace for short-form publishes (unused in M1). |

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

`python -m custos_catalog` will currently raise `NotImplementedError` from
`create_app()` — that is the documented scaffold behaviour. The factory is
wired in CS-IMPL-017 ([#218](https://github.com/toddysm/custos/issues/218)).

## Layout

```
src/services/catalog-service/
├── README.md
├── pyproject.toml
├── src/
│   └── custos_catalog/
│       ├── __init__.py        # create_app() factory (scaffold stub)
│       ├── __main__.py        # uvicorn entry point
│       └── py.typed
└── tests/
    └── test_smoke.py
```
