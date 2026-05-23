# Change: rest-workflow-list-returns-refs-only

Date: 2026-05-23
Type: component-design
Component: catalog-service
Sequence: 008
GitHub Issue: #218
Status: open

## Summary

Same class of drift as change `007` (activity-types and connector-types
list endpoints) but for the workflow list endpoint. Per design
§ Public Interface, the response for
`GET /v1/workspaces/{ws}/workflows/{name}` is
`Workflow + [WorkflowVersionRef]` (refs only — no `document` per
version). The initial Phase G implementation emitted full
`WorkflowVersionBody` items, including the entire normalized
`document` field on every version, which inflated payload size
substantially (workflow documents are kilobytes-scale normalized
JSON) and diverged from the documented contract.

This change brings the implementation back to the spec: the list
endpoint now returns `WorkflowVersionRefBody` items only —
`{workspaceId, workflowName, version}` per item. The full normalized
document remains available on the workspaced get-by-ref endpoint
`GET /v1/workspaces/{ws}/workflows/{name}@{version}` (and on the
workspaceless triple-encoded id route).

The "+ Workflow" half of the design's response shape (the parent
`Workflow` row carrying `deprecated`, `createdAt`, etc.) is **not**
emitted at v1: the SPL provider currently has no `get_workflow`
metadata method — only `list_workflow_versions` — so there is no
backing storage operation to populate it. Each version already carries
`parentDeprecated`, which covers the most-commonly-needed parent flag
at the get-by-ref level. A future SPL extension that surfaces the
parent `Workflow` row would let us add a `workflow` field to
`WorkflowListResponse` without breaking existing clients.

## Before

```json
GET /v1/workspaces/ws-1/workflows/orders
200 OK
{
  "items": [
    {
      "workspaceId": "ws-1",
      "workflowName": "orders",
      "version": 1,
      "document": { /* full normalized doc, kilobytes */ },
      "derivedFromTemplateVersionId": null,
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
GET /v1/workspaces/ws-1/workflows/orders
200 OK
{
  "items": [
    {"workspaceId": "ws-1", "workflowName": "orders", "version": 1},
    {"workspaceId": "ws-1", "workflowName": "orders", "version": 2}
  ],
  "nextCursor": null
}
```

## Rationale

1. **Spec compliance.** The design row says `[WorkflowVersionRef]`
   explicitly; the implementation was wider than the contract.
2. **Payload size.** Workflow documents are the full normalized
   definition (CEL expressions, step DAG, parameter schemas,
   digest-pinned activity references). Returning N of them on every
   list call multiplies that cost; refs-only is constant per item.
3. **Cache friendliness.** Ref payloads are tiny and stable per
   immutable row — easier to cache at the gateway than full version
   bodies.
4. **Discovery vs. resolution.** The list endpoint serves "what
   versions of `<workflow_name>` exist?"; the workspaced get-by-ref
   endpoint serves "give me the document for this exact version".
   Refs answer the first question without forcing callers to pay
   the cost of the second.

## Affected sections

- Code: `api/models.py` — `WorkflowListResponse.items` retyped to
  `list[WorkflowVersionRefBody]`; docstring updated to point at the
  design contract.
- Code: `api/routes/workflows.py` — list branch of
  `list_or_get_workflow` now constructs `WorkflowVersionRefBody`
  items directly instead of going through
  `_serialize_workflow_version`. The latter is still used by both
  get-by-ref handlers.
- Tests: `tests/api/test_workflows.py::test_list_versions_returns_published_rows`
  rewritten to assert the strict `{workspaceId, workflowName, version}`
  key set on each item and to no longer assert on `document` /
  `parentDeprecated`.
- Design: the "+ Workflow" half of the response is documented as
  deferred to a future SPL evolution (above); no spec wording change
  is required since the table's `[WorkflowVersionRef]` half is now
  honored.

## Tests / Verification

71 API tests pass (`test_list_versions_returns_published_rows` now
verifies the ref-only key set explicitly). `ruff format --check`,
`ruff check src tests`, and `mypy src tests` (on both 3.11 and 3.13)
all clean.
