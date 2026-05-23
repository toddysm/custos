# Change: rpc-cross-workspace-permission

Date: 2026-05-23
Type: component-design
Component: catalog-service
Sequence: 009
GitHub Issue: #218
Status: open

## Summary

The internal RPC route `GET /rpc/v1/workflow-versions/{id}` was
gated only on the `catalog:rpc:read` permission and then trusted
the workspace id embedded in the triple-encoded path. As written,
any principal holding `catalog:rpc:read` (for any workspace) could
craft an id pointing at a different workspace and resolve its
workflow rows — the permission check did not constrain *which*
workspace the caller could read.

This change splits the RPC read capability in two:

- `catalog:rpc:read` continues to authorise **same-workspace**
  internal reads (the common case: the workflow runtime resolving
  a workflow inside its own workspace, the activity dispatcher
  resolving a connector type that's globally addressable).
- `catalog:rpc:cross-workspace-read` is the **explicit** grant
  required to resolve a workflow whose workspace id differs from
  the call-context's own. The gateway issues this permission only
  to system principals that legitimately fan out across tenants
  (workflow runtime when executing tenant workflows under a system
  workspace, activity dispatcher when sweeping across tenants).

This matches the principle-of-least-privilege model already
established for the REST gets: the workspaced
`GET /v1/workspaces/{ws}/workflows/{name}@{v}` route uses
`require_workspace_access`, and the workspaceless id route
`GET /v1/workflows/{id}` enforces `ctx.workspace_id == parsed_workspace_id`
even when the caller carries `catalog:workflows:read` (change
established by commit `5b46783` on this branch).

## Before

```python
async def rpc_get_workflow_version(
    workflow_version_id: str = Path(...),
    _ctx: CallContext = Depends(require_permission_only("catalog:rpc:read")),
    manager: DefinitionManager = Depends(get_definition_manager),
) -> WorkflowVersionBody:
    match = _WORKFLOW_ID_RE.match(workflow_version_id)
    ...
    workspace_id = match.group("ws")
    row = await manager.get_workflow_version_by_ref(
        workspace_id=workspace_id,  # <-- trusted from path, never compared to ctx
        ...
    )
```

## After

```python
CROSS_WORKSPACE_RPC_READ = "catalog:rpc:cross-workspace-read"

async def rpc_get_workflow_version(
    workflow_version_id: str = Path(...),
    ctx: CallContext = Depends(require_permission_only("catalog:rpc:read")),
    manager: DefinitionManager = Depends(get_definition_manager),
) -> WorkflowVersionBody:
    ...
    workspace_id = match.group("ws")
    if ctx.workspace_id != workspace_id and not ctx.has_permission(
        CROSS_WORKSPACE_RPC_READ,
    ):
        raise CallContextError(
            403,
            "catalog.workspace_mismatch",
            (
                f"call context workspace {ctx.workspace_id!r} does not match "
                f"workflow version id workspace {workspace_id!r}; "
                f"cross-workspace reads require {CROSS_WORKSPACE_RPC_READ!r}"
            ),
        )
```

## Permission catalog

| Permission                              | Scope                                                | Granted to (typical)                                |
| --------------------------------------- | ---------------------------------------------------- | --------------------------------------------------- |
| `catalog:rpc:read`                      | Same-workspace RPC reads + global connector resolves | Any internal service that makes RPC calls           |
| `catalog:rpc:cross-workspace-read`      | Cross-tenant RPC reads                               | Workflow runtime, activity dispatcher (system only) |

The connector-type resolve route (`GET /rpc/v1/connector-types/{ref}`)
remains gated on `catalog:rpc:read` only — connector types are
globally addressable across workspaces by design (their refs do
not embed a workspace id), so there is no workspace boundary to
cross there.

## Why a separate permission rather than blanket-deny

Hard-coding `ctx.workspace_id == parsed_workspace_id` on the RPC
route would break legitimate cross-workspace traffic (workflow
runtime / activity dispatcher) without giving the gateway any way
to express the intent. Splitting the permission lets the gateway
encode the cross-workspace authority explicitly per principal,
gives audit-grade visibility into which contexts can cross
tenant boundaries, and keeps the default tightly scoped.

## Affected sections

- Code: `api/rpc.py` — module docstring, new
  `CROSS_WORKSPACE_RPC_READ` constant, workspace match enforcement
  inside `rpc_get_workflow_version`. The `_ctx` parameter is
  renamed to `ctx` because it's now read.
- Tests: `tests/api/test_rpc.py` — two new regression tests
  (`test_rpc_get_workflow_version_rejects_cross_workspace_without_explicit_permission`,
  `test_rpc_get_workflow_version_allows_cross_workspace_with_explicit_permission`).
- Design: this change note. The `catalog:rpc:cross-workspace-read`
  permission joins the catalog's published permission set; the
  Auth Service (COMP-002, CS-IMPL-024) will need to recognise it
  when issuing RPC contexts.

## Tests / Verification

73 API tests pass (71 prior + 2 new). `ruff format --check`,
`ruff check src tests`, and `mypy src tests` on both 3.11 and 3.13
all clean.
