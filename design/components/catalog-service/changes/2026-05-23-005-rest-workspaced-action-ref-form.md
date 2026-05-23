# Change: rest-workspaced-action-ref-form

Date: 2026-05-23
Type: component-design
Component: catalog-service
Sequence: 005
GitHub Issue: #218
Status: open

## Summary

Align the Catalog Service's REST API table for the two workspaced action
endpoints — `:extractTemplate` on workflows and `:materialize` on
templates — with the actual identifier form accepted by the
implementation (and consistent with the existing `:deprecate` row).

Before this change the table listed both rows as accepting
`{workflowVersionId}` / `{templateVersionId}`, which read as the
immutable UUID PK form. The implementation parses `<name>@<version>`
and never resolves a bare UUID at those paths (the only UUID-keyed
route in the surface is the workspaceless `GET /v1/workflows/{workflow_version_id}`).
Callers wiring against the spec would have written URLs the router
does not route.

## Before

```text
POST /v1/workspaces/{ws}/workflows/{workflowVersionId}:extractTemplate
POST /v1/workspaces/{ws}/templates/{templateVersionId}:materialize
```

## After

```text
POST /v1/workspaces/{ws}/workflows/{name}@{version}:extractTemplate
POST /v1/workspaces/{ws}/templates/{name}@{version}:materialize
```

The workspace prefix in the URL makes the ref workspace-local by
construction; cross-workspace reads continue to flow through the
workspaceless `GET /v1/workflows/{workflow_version_id}` route (gated
on `catalog:workflows:read` with the workspace-match enforcement
added earlier in this PR).

## Rationale

1. **Internal consistency.** `:deprecate` already accepts
   `{name}@{version}` in the same table row family. Having two siblings
   under `/v1/workspaces/{ws}/workflows/...` use different identifier
   shapes is misleading.
2. **Workspace scoping.** When the workspace is already in the URL,
   embedding a fully-qualified UUID id in the suffix is redundant —
   the workspace would have to match the id's workspace, and a
   mismatch would be a 4xx the caller cannot recover from without
   re-deriving the URL. The triple form makes the workspace match
   structurally exact.
3. **Cross-workspace path stays distinct.** Internal callers that need
   to fetch by immutable id (workflow runtime, activity dispatcher) use
   the workspaceless RPC route, not these workspaced action routes.

## Affected sections

- `design.md` § Public Interface — REST API table (two rows updated).

## Tests / Verification

No behavior change in the implementation; this is a spec-alignment
change. The existing `:extractTemplate` and `:materialize` tests
already exercise the `<name>@<version>` form.
