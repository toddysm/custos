# Change: rest-list-endpoints-return-refs-only

Date: 2026-05-23
Type: component-design
Component: catalog-service
Sequence: 007
GitHub Issue: #218
Status: open

## Summary

The Catalog Service implementation drift was on the list endpoints
for activity types and connector types. Per design § Public Interface
the response for both `GET /v1/workspaces/{ws}/activity-types` and
`GET /v1/catalog/connector-types` is `[ActivityTypeRef]` /
`[ConnectorTypeRef]` — refs only. The initial Phase G implementation
emitted full `ActivityTypeVersionBody` / `ConnectorTypeVersionBody`
items including the entire `normalizedManifest`, which both diverged
from the documented contract and significantly inflated payload size
(authoring UIs commonly list-across many `(namespace, type)` pairs).

This change brings the implementation back to the spec: the list
endpoints now return refs only; the full normalized manifest stays
on the get-by-ref endpoints (`GET .../activity-types/{namespace}/{type}@{version}`
and `GET /v1/catalog/connector-types/{type}@{version}`).

## Before

```json
GET /v1/workspaces/ws-1/activity-types?namespace=ws-1&type=fetch-orders
200 OK
{
  "items": [
    {
      "namespace": "ws-1",
      "type": "fetch-orders",
      "version": "1.0.0",
      "digest": "sha256:...",
      "normalizedManifest": { /* full manifest, kilobytes */ },
      "parentDeprecated": false,
      "publishedAt": "2026-05-23T..."
    },
    ...
  ],
  "nextCursor": null
}
```

## After

```json
GET /v1/workspaces/ws-1/activity-types?namespace=ws-1&type=fetch-orders
200 OK
{
  "items": [
    {
      "namespace": "ws-1",
      "type": "fetch-orders",
      "version": "1.0.0",
      "digest": "sha256:..."
    },
    ...
  ],
  "nextCursor": null
}
```

The same projection applies to the connector-types list endpoint:
each item is `{ type, version, digest }`.

## Rationale

1. **Spec compliance.** The design's REST table calls the response
   `[ActivityTypeRef]` / `[ConnectorTypeRef]` explicitly. The
   implementation was wider than the contract.
2. **Payload size.** Activity manifests are typed manifests with full
   parameter schemas, capability lists, and side-effect declarations —
   easily several kilobytes each. Returning N of them on every list
   call multiplies that cost; refs-only is constant per item.
3. **Cache friendliness.** Ref payloads are stable per immutable
   row and tiny — easier to cache at the gateway than the larger
   version bodies.
4. **Discovery vs. resolution.** The list endpoint serves
   "what versions exist for this `(namespace, type)`"; the
   get-by-ref endpoint serves "give me the manifest for this exact
   version". Refs answer the first question without forcing callers
   to pay the cost of the second.

## Affected sections

- Code: `api/routes/activity_types.py` — added `_serialize_ref`,
  switched the list handler to use it.
- Code: `api/routes/connector_types.py` — added `_serialize_ref`,
  switched the list handler to use it.
- Code: `api/models.py` — `ActivityTypeListResponse.items` and
  `ConnectorTypeListResponse.items` retyped to `list[*RefBody]`.
- Tests: `tests/api/test_activity_types.py::test_list_returns_versions`
  and `tests/api/test_connector_types.py::test_list_returns_versions`
  extended to assert the projected ref-only key set.
- Design: no spec wording change needed — the contract was already
  `[Ref]`; this change records that the implementation now matches.

## Tests / Verification

71 API tests pass (additional ref-key-set assertions in the two list
tests). `ruff format --check`, `ruff check src tests`, and `mypy src`
all clean.
