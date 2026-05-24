# Developer Guide: Catalog API

Last Updated: 2026-05-23

The **Catalog Service** is Custos's authoritative registry for workflow
documents (workflows, templates, activity-types and connector-types).
This page documents the REST surface you will integrate against when
building publishing tools, CI gates, or extension developer
experiences. For the broader design rationale and component
boundaries see [`design/components/catalog-service/design.md`](../../design/components/catalog-service/design.md).

## Contents

- [Base URL and authentication](#base-url-and-authentication)
- [Reference grammar](#reference-grammar)
- [Two-gate validation model](#two-gate-validation-model)
- [Error envelope and error taxonomy](#error-envelope-and-error-taxonomy)
- [Immutability and deprecation](#immutability-and-deprecation)
- [Workflows](#workflows)
- [Templates](#templates)
- [Activity-types](#activity-types)
- [Connector-types](#connector-types)
- [Placeholder declaration reference](#placeholder-declaration-reference)
- [Internal RPC surface](#internal-rpc-surface)
- [Worked examples](#worked-examples)

---

## Base URL and authentication

All endpoints are served by the catalog-service deployment and routed
through the API gateway:

```
https://<gateway>/catalog/...
```

Within the cluster, the service exposes the paths below at port 8080.
This guide uses the in-cluster paths.

Authentication is mediated by the API gateway, which forwards a
**call-context header** (`x-custos-callctx`) carrying the
workspace-scoped principal and the set of granted permissions. The
catalog enforces fine-grained permissions on every endpoint; the
permission constants used below correspond to the strings emitted in
authz envelopes:

| Permission | Used by |
|---|---|
| `catalog:workflows:write` | publish workflow, deprecate workflow |
| `catalog:workflows:read` | get workflow, list workflows |
| `catalog:templates:write` | publish template, extract template, materialize template, deprecate template |
| `catalog:templates:read` | get template, list templates |
| `catalog:activity-types:write` | register / deprecate activity-type |
| `catalog:activity-types:read` | get / list activity-types |
| `catalog:connector-types:write` | register / deprecate connector-type |
| `catalog:connector-types:read` | get / list connector-types |
| `catalog:rpc:read` | internal RPC sub-tree |

---

## Reference grammar

The catalog uses an opinionated **reference (ref) grammar** whose
exact form depends on object kind:

| Kind | Form | Example | Meaning |
|---|---|---|---|
| Workflow / Template | `workspace/name@VERSION` | `ws-1/orders@42` | Exact pin to a specific published integer version |
| Activity-type | `workspace/name@MAJOR.MINOR.PATCH` | `ws-1/vuln-scan@2.1.0` | Exact semver pin |
| Activity-type | `workspace/name@MAJOR` | `ws-1/vuln-scan@2` | Major-version semver ref |
| Connector-type | `name@MAJOR.MINOR.PATCH` | `oci-registry@1.0.0` | Exact semver pin (no workspace) |
| Connector-type | `name@MAJOR` | `oci-registry@1` | Major-version semver ref |

Notes:

1. Workflows and templates use monotonic integer versions and are
   referenced as `workspace/name@<int>`.
2. Activity-types use the two-segment workspace-scoped form and semver
   versioning.
3. Connector-types are platform-scoped, use the one-segment form, and
   use semver versioning.
4. Bare names, `latest`-style tags, and unsupported partial versions
   are rejected. In particular, workflows/templates do not use semver
   shorthand such as `@MAJOR` or `@MAJOR.MINOR`.

---

## Two-gate validation model

Every publishing endpoint runs two independent validation passes
before any state is written. Either gate can reject the request with
a `4xx` error envelope.

### Gate 1: Structural validation

The document is parsed (YAML or JSON), normalized, and validated
against the **JSON Schema** for its `kind`. This catches syntax
problems and structural violations:

- Malformed YAML / JSON.
- Missing or unknown fields (schemas use `additionalProperties: false`
  in the spec sections).
- Wrong field types.
- Wrong `apiVersion` or `kind` values.

Error codes from this gate use the form
`catalog.publish.parse` and `catalog.publish.schema`.

### Gate 2: Semantic validation

The normalized document is checked for cross-reference and expression
correctness:

- Every `activity:` reference resolves to a non-deprecated
  activity-type version inside the workflow's workspace (or the
  `custos.builtin` namespace).
- Every connector reference resolves to a non-deprecated
  connector-type version.
- Every CEL expression compiles, references only declared inputs,
  outputs of earlier steps, or template placeholders, and respects
  the sandbox budget.
- Every placeholder declaration is consistent with the JSON path it
  binds (`integer` placeholders cannot bind to a string field, etc.).
- Round-trip stability: the canonical form of the document on the way
  in matches the canonical form re-serialised on the way out.

Error codes from this gate use the forms
`catalog.publish.resolve` (ref resolution) and
`catalog.publish.cel` (expression problems).

When publishing succeeds, the canonical hash of the **post-normalize**
document determines whether the request is idempotent (same hash →
returns the existing version row) or whether a new version is
allocated.

---

## Error envelope and error taxonomy

Every non-`2xx` response uses a uniform envelope:

```json
{
  "error": {
    "code": "catalog.publish.resolve",
    "detail": "activity-type ws-1/missing@1 is not registered",
    "fields": { "path": "spec.steps[0].activity" }
  }
}
```

`code` is always present. `detail` is human-readable. `fields` is an
optional bag of structured context (paths, refs, version
combinations, etc.). The full set of codes emitted by the catalog
service is:

| Code prefix / code | HTTP | Emitted by |
|---|---|---|
| `catalog.publish.parse` | 400 | YAML / JSON syntax errors |
| `catalog.publish.schema` | 400 | JSON-Schema violations |
| `catalog.publish.resolve` | 422 | Unresolvable refs |
| `catalog.publish.cel` | 422 | CEL compile / sandbox / unknown-binding errors |
| `catalog.workflow_not_found` | 404 | GET / deprecate on missing workflow ref |
| `catalog.template_not_found` | 404 | GET / materialize / deprecate on missing template |
| `catalog.workflow_immutability_violation` | 409 | Attempt to mutate a published version |
| `catalog.template_immutability_violation` | 409 | Attempt to mutate a published template version |
| `catalog.template_extract_failed.<cause>` | 400 / 409 | Bad selectors / forbidden paths during `:extractTemplate` |
| `catalog.template_materialization_failed.<cause>` | 400 / 409 | Missing required binding, type mismatch, etc. |
| `catalog.activity_manifest_invalid` | 400 | Activity manifest schema or rule failure |
| `catalog.activity_namespace_forbidden` | 403 | Non-platform-admin tries to register in `custos.builtin` |
| `catalog.activity_type_digest_conflict` | 409 | Re-register same (namespace, type, version) with a different digest |
| `catalog.activity_type_not_found` | 404 | Resolve an activity-type ref that does not exist |
| `catalog.connector_manifest_invalid` | 400 | Connector manifest schema or rule failure |
| `catalog.connector_type_digest_conflict` | 409 | Re-register same (type, version) with different digest |
| `catalog.connector_type_not_found` | 404 | Resolve a connector-type ref that does not exist |
| `catalog.activity_registry_internal_error` | 500 | Unexpected error in activity registry |
| `catalog.connector_registry_internal_error` | 500 | Unexpected error in connector registry |
| `catalog.request_invalid` | 400 | Generic Pydantic request validation failure |
| `catalog.http_<status>` | as `<status>` | Pass-through wrapper for raw HTTP exceptions |

---

## Immutability and deprecation

Published versions are **immutable**:

- Re-publishing a byte-identical workflow (same canonical hash)
  returns the existing version row with the same `version` integer.
  This is the idempotency contract.
- Re-publishing the same workflow `name` with a *different*
  canonical body allocates the next monotonically increasing version
  number.
- There is no public way to overwrite an existing
  `(workspace, name, version)` tuple. Attempting an out-of-band
  mutation (e.g. via the storage layer) raises
  `catalog.workflow_immutability_violation` on next read.

**Deprecation** is a soft-delete signal:

- A deprecated workflow/template/activity/connector remains readable
  by ref or id.
- The `@MAJOR` resolver skips deprecated versions when picking the
  "latest within major".
- Deprecating a parent (activity-type, connector-type) marks every
  version under that family as deprecated; the rows themselves are
  not touched but `parent_deprecated` is `true` in resolver results.

---

## Workflows

### Publish a workflow

```
POST /v1/workspaces/{ws}/workflows
```

Permissions: `catalog:workflow:publish`

Request body:

```json
{
  "definition": "<YAML or JSON string of the workflow document>"
}
```

Response `201 Created`:

```json
{
  "workspaceId": "ws-1",
  "name": "orders",
  "version": 1,
  "workflowVersionId": "ws-1/orders/1",
  "canonicalRef": "ws-1/orders@1.0.0",
  "canonicalHash": "sha256:..."
}
```

Errors: any code from the publish taxonomy table above.

### Get / list workflows

```
GET /v1/workspaces/{ws}/workflows                    # list latest non-deprecated
GET /v1/workspaces/{ws}/workflows/{name_or_ref}      # get specific
```

- `{name_or_ref}` accepts either the bare workflow name (returns the
  latest non-deprecated version) or a full `name@version` pin.
- Listing supports `?include_deprecated=true` to surface
  tombstoned versions.

### Get by workflow-version-id

```
GET /v1/workflows/{workflowVersionId}
```

This is the cross-workspace lookup used for fan-out sub-workflow
references. Workspace isolation is enforced: a caller in workspace
`A` cannot read a workflow-version-id belonging to workspace `B`
unless their call-context grants the cross-tenant read scope.

### Deprecate a workflow

```
POST /v1/workspaces/{ws}/workflows/{ref}:deprecate
```

Permissions: `catalog:workflow:deprecate`

Body: `{ "reason": "<freeform string>" }` (optional).

### Extract a template

```
POST /v1/workspaces/{ws}/workflows/{ref}:extractTemplate
```

Permissions: `catalog:workflow:publish`

Request body:

```json
{
  "templateName": "orders-template",
  "selectors": [
    {
      "path": "spec.steps[0].timeoutSeconds",
      "placeholderName": "timeout",
      "placeholderType": "integer",
      "required": true
    }
  ]
}
```

Each selector replaces a leaf in the workflow with a `${{ name }}`
placeholder reference. The returned document is a new
`WorkflowTemplate` ready to be published via the template
endpoints below.

Selectors targeting non-leaf, non-existent, or forbidden paths raise
`catalog.template_extract_failed.<cause>` where `<cause>` is one of
`forbidden_path`, `unknown_path`, `not_a_leaf`,
`duplicate_placeholder_name`.

---

## Templates

### Publish a template

```
POST /v1/workspaces/{ws}/templates
```

Permissions: `catalog:workflow:publish`

Request and response shapes mirror workflow publish. Templates are
versioned independently of the workflows they are extracted from.

### Get a template

```
GET /v1/workspaces/{ws}/templates/{ref}
```

### Materialize a template into a concrete workflow

```
POST /v1/workspaces/{ws}/templates/{ref}:materialize
```

Permissions: `catalog:workflow:publish`

Request body:

```json
{
  "targetName": "orders-prod",
  "bindings": {
    "timeout": 30,
    "channel": "ops-alerts"
  }
}
```

Behaviour:

1. Each placeholder declaration is bound to its value from
   `bindings`. Missing required bindings raise
   `catalog.template_materialization_failed.missing_required_binding`.
2. Type compatibility is enforced (integer placeholder bound to a
   string raises
   `catalog.template_materialization_failed.type_mismatch`).
3. The resulting concrete document is published as a workflow under
   `targetName` in the calling workspace. The response is a
   `WorkflowVersionRefBody`, identical to the publish-workflow
   response.

---

## Activity-types

### Register an activity-type version

```
POST /v1/workspaces/{ws}/activity-types
```

Permissions: `catalog:activity:write`

Body:

```json
{
  "manifest": {
    "apiVersion": "custos.dev/v1",
    "kind": "ActivityManifest",
    "metadata": {
      "namespace": "ws-1",
      "type": "fetch-orders",
      "version": "1.2.0"
    },
    "spec": {
      "contractVersion": "1",
      "runtime": {
        "kind": "oci-container",
        "image": "ghcr.io/acme/fetch-orders:1.2.0",
        "digest": "sha256:abc..."
      }
    }
  },
  "referrerRef": "ghcr.io/acme/fetch-orders:1.2.0@sha256:..."
}
```

Rules:

- `metadata.namespace` MUST equal the path parameter `{ws}` unless
  the caller is a platform admin registering into the reserved
  `custos.builtin` namespace.
- Re-registering the same `(namespace, type, version)` with the
  *same* digest is idempotent and returns the existing row.
- Re-registering with a *different* digest raises
  `catalog.activity_type_digest_conflict`.

### List, get, deprecate

```
GET    /v1/workspaces/{ws}/activity-types
GET    /v1/workspaces/{ws}/activity-types/{ref}
POST   /v1/workspaces/{ws}/activity-types/{ref}:deprecate
```

Deprecating a *parent* ref (e.g. `ws-1/fetch-orders`, no version)
deprecates every version under it. Deprecating a single
`name@version` only deprecates that version.

---

## Connector-types

### Register a connector-type version

```
POST /v1/catalog/connector-types
```

Permissions: `catalog:connector:write`

Connector-types live in the global, platform-scoped catalog (no
workspace component). The request body is the same shape as
activity-type register, with a `ConnectorManifest` document.

### List, get, deprecate

```
GET    /v1/catalog/connector-types
GET    /v1/catalog/connector-types/{ref}
POST   /v1/catalog/connector-types/{ref}:deprecate
```

---

## Placeholder declaration reference

Templates expose typed placeholders that get bound at
`:materialize`. The supported types are exactly:

| `type` | Bound value | Notes |
|---|---|---|
| `string` | JSON string | Type-checked only. Range / regex constraints are out of scope. |
| `integer` | JSON integer | |
| `number` | JSON number | Allows fractional values. |
| `boolean` | `true` / `false` | |
| `json` | Any JSON value | Useful for opaque pass-through bags. No type-checking beyond JSON validity. |
| `activityRef` | A canonical activity ref string | If `activityType` is set on the declaration, the bound ref's `(namespace, type)` MUST match. |
| `connectorRef` | A canonical connector ref string | If `connectorType` is set, the bound ref's `type` MUST match. |

A template `spec.placeholders` entry looks like:

```yaml
placeholders:
  - name: timeout
    type: integer
    required: true

  - name: scanner
    type: activityRef
    activityType: "custos.builtin/vuln-scan"
    required: true

  - name: registry
    type: connectorRef
    connectorType: "oci-registry"
    required: true

  - name: channel
    type: string
    required: false
    default: "default-alerts"

  - name: settings
    type: json
    required: false
    default: { "retries": 3 }
```

Placeholder references inside the template body use the `${{ name }}`
syntax and may appear anywhere a scalar is legal:

```yaml
spec:
  workflow:
    steps:
      - id: scan
        activity: "${{ scanner }}"
        with:
          target: "${{ registry }}"
          timeoutSeconds: "${{ timeout }}"
          alertChannel: "${{ channel }}"
          settings: "${{ settings }}"
```

---

## Internal RPC surface

The catalog also exposes a small RPC sub-tree at `/rpc/v1/...` for
**internal services** (workflow service, trigger service). External
callers will not have the `catalog:rpc:read` scope.

### `GET /rpc/v1/workflow-versions/{workflowVersionId}`

Returns the full row for a workflow-version-id including the canonical
document. The default policy is **same-workspace only**: the
call-context's `ws` MUST equal the workspace embedded in
`workflowVersionId`. Cross-workspace reads require a per-service
cross-tenant grant negotiated out of band.

### `GET /rpc/v1/connector-types/{ref}`

Resolves a connector-type ref to its current row, including the
parent-deprecated flag. Identical resolution semantics to the public
GET endpoint above, but skips the call-context workspace check
because connector-types are platform-scoped.

---

## Worked examples

### Example 1: Fan-out sub-workflow

This example publishes a child workflow and a parent that fans out
over a list, invoking the child as a step.

**Step 1 — register the activity:**

```yaml
# child-activity.yaml
apiVersion: custos.dev/v1
kind: ActivityManifest
metadata:
  namespace: ws-1
  type: fetch-orders
  version: 1.0.0
spec:
  contractVersion: "1"
  runtime:
    kind: oci-container
    image: ghcr.io/acme/fetch-orders:1.0.0
    digest: sha256:abc
```

```bash
curl -X POST .../v1/workspaces/ws-1/activity-types \
  -d '{"manifest": <yaml-as-json> }'
```

**Step 2 — publish the child workflow:**

```yaml
# child.yaml
apiVersion: custos.dev/v1
kind: Workflow
metadata: { name: process-order, workspace: ws-1 }
spec:
  inputs:
    orderId: { type: string }
  steps:
    - id: fetch
      activity: ws-1/fetch-orders@1
      with:
        id: "${{ inputs.orderId }}"
```

**Step 3 — publish the parent fan-out workflow:**

```yaml
apiVersion: custos.dev/v1
kind: Workflow
metadata: { name: process-batch, workspace: ws-1 }
spec:
  inputs:
    orderIds: { type: array }
  steps:
    - id: each-order
      forEach: "${{ inputs.orderIds }}"
      workflow: ws-1/process-order@1
      with:
        orderId: "${{ item }}"
```

Publishing the parent succeeds only if `ws-1/process-order@1`
resolves at publish time. The catalog records the resolved
workflow-version-id alongside the parent, so subsequent reads of the
parent surface the exact child that was selected.

### Example 2: Extract a template with two activity refs and a connector

Start with a published workflow that already uses two activities and
a connector-bound input:

```yaml
apiVersion: custos.dev/v1
kind: Workflow
metadata: { name: scan-and-promote, workspace: ws-1 }
spec:
  inputs:
    image:    { type: string }
    registry: { type: string }
  steps:
    - id: scan
      activity: custos.builtin/vuln-scan@2
      with: { image: "${{ inputs.image }}" }
    - id: promote
      activity: custos.builtin/image-promote@1
      with:
        image: "${{ inputs.image }}"
        registry: "${{ inputs.registry }}"
```

Extract a parameterised template:

```bash
curl -X POST .../v1/workspaces/ws-1/workflows/scan-and-promote@1:extractTemplate -d '{
  "templateName": "scan-and-promote-tmpl",
  "selectors": [
    {"path": "spec.steps[0].activity", "placeholderName": "scanner",  "placeholderType": "activityRef",  "required": true},
    {"path": "spec.steps[1].activity", "placeholderName": "promoter", "placeholderType": "activityRef",  "required": true},
    {"path": "spec.steps[1].with.registry", "placeholderName": "registry", "placeholderType": "connectorRef", "required": true}
  ]
}'
```

The resulting template has three required placeholders; any
materialise call must bind all three.

### Example 3: Materialise a template using every placeholder type

Given a template `ws-1/release-pipeline-tmpl@1` with placeholders for
every supported type:

```yaml
spec:
  placeholders:
    - { name: scanner,   type: activityRef,  activityType: "custos.builtin/vuln-scan",  required: true }
    - { name: registry,  type: connectorRef, connectorType: "oci-registry",             required: true }
    - { name: channel,   type: string,       required: true }
    - { name: timeout,   type: integer,      required: true }
    - { name: sample,    type: number,       required: true }
    - { name: enabled,   type: boolean,      required: true }
    - { name: extra,     type: json,         required: false, default: {} }
```

Materialise:

```bash
curl -X POST .../v1/workspaces/ws-1/templates/release-pipeline-tmpl@1:materialize -d '{
  "targetName": "release-pipeline-prod",
  "bindings": {
    "scanner":  "custos.builtin/vuln-scan@2",
    "registry": "oci-registry@1",
    "channel":  "ops-alerts",
    "timeout":  300,
    "sample":   0.1,
    "enabled":  true,
    "extra":    { "label": "weekly", "owner": "platform" }
  }
}'
```

The catalog will:

1. Resolve `scanner` to a non-deprecated activity-type version and
   confirm its `(namespace, type)` matches the placeholder's
   `activity_type` pin (if any).
2. Resolve `registry` similarly against the connector-type catalog.
3. Substitute every `${{ name }}` site inside the template body.
4. Run the full two-gate validation pipeline against the
   substituted document.
5. Publish the result as `ws-1/release-pipeline-prod@1`.

Failures in any of these steps surface as
`catalog.template_materialization_failed.<cause>` envelopes with
`fields.placeholder` and `fields.path` indicating exactly where the
problem was.
