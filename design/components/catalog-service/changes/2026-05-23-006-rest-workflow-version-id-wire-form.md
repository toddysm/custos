# Change: rest-workflow-version-id-wire-form

Date: 2026-05-23
Type: component-design
Component: catalog-service
Sequence: 006
GitHub Issue: #218
Status: open

## Summary

The Catalog Service design described the workspaceless workflow GET
route as `GET /v1/workflows/{workflowVersionId}` and labelled the
column "Fetch by immutable ID", strongly implying the path segment
was the immutable UUID PK from the ER diagram. The implementation
has always accepted only the colon-free triple
`<workspaceId>/<workflowName>@<version>` — SPL does not surface the
UUID PK at v1, so a bare UUID has no resolver on the catalog side.
A client (or API Gateway route configuration) writing against the
spec would have built URLs the router could not parse, and would
have hit `catalog.workflow_version_id_invalid` 400s on every call.

This change makes the spec match the wire contract: the v1 route is
`GET /v1/workflows/{workspaceId}/{workflowName}@{version}`, and the
description and adjacent RPC table row are reworded to call the
triple the v1 wire form. The persistent UUID PK stays on the ER
diagram as the storage-layer truth; it is reserved for a future
evolution when SPL exposes it directly, at which point both forms
could be accepted equivalently.

## Before

```text
| GET | `/v1/workflows/{workflowVersionId}` | — | `WorkflowVersion` | Fetch by immutable ID. |
```

```text
| `GetWorkflowVersion(workflowVersionId)` | Workflow Service ... | Read-only fetch at `StartRun`. |
```

> "The reference form is the immutable `workflowVersionId` produced
> at publish time (or, equivalently, a `<workspace>/<name>@<version>`
> triple that resolves uniquely to one `workflowVersionId`)."

## After

```text
| GET | `/v1/workflows/{workspaceId}/{workflowName}@{version}` | — | `WorkflowVersion` | Fetch a workflow version by its workspaceless wire id. v1 wire form: the colon-free triple ... |
```

```text
| `GetWorkflowVersion(workflowVersionRef)` | Workflow Service ... | `workflowVersionRef` is the v1 wire triple ... |
```

> "The reference form on the wire (the v1 contract) is the colon-free
> triple `<workspaceId>/<workflowName>@<version>`, which resolves
> uniquely to one immutable `WorkflowVersion` row. The persistent
> model still has a `workflowVersionId` UUID PK (see ER diagram), but
> it is not exposed on the wire at v1 — a future evolution may
> surface it directly, at which point both forms could be accepted
> equivalently."

## Rationale

1. **Truth in advertising.** The implementation has never accepted
   the bare UUID at this route; the spec said it did. The spec was
   the misleading side, since (a) the SPL provider has no UUID
   surface for `WorkflowVersion` lookups in v1, and (b) the route's
   tenant-boundary check (added earlier in this PR) needs the
   workspace id to be present in the URL anyway.
2. **Internal consistency.** The workspaced routes already use
   `<name>@<version>` for `:deprecate`, `:extractTemplate`, and
   `:materialize` (see change `005`). The workspaceless GET route's
   triple form is just the same `<name>@<version>` with the
   workspace prefix prepended to make the URL self-contained.
3. **Forward-compatibility.** Calling out the UUID PK as
   "reserved for a future SPL evolution" keeps the door open for the
   immutable-id form without claiming it is already in the contract.

## Affected sections

- `design.md` § Public Interface — REST table row for workspaceless
  GET (rewritten).
- `design.md` § Public Interface — `:extractTemplate` row's
  workspaceless cross-reference (path updated).
- `design.md` § Public Interface — RPC table row
  `GetWorkflowVersion(...)` (argument and description updated).
- `design.md` § `workflow:` step kind narrative — wire-form
  paragraph rewritten.

## Tests / Verification

No behavior change in the implementation; the route's path matcher,
the regex, the 400 `catalog.workflow_version_id_invalid` error, the
403 `catalog.workspace_mismatch` envelope, and the
`test_get_by_id_*` regression suite are all already aligned with
the corrected spec. The existing 71-test catalog API suite continues
to pass.
