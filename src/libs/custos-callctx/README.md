# custos-callctx

The `custos_callctx.bootstrap` module shares the existing HMAC-SHA256
version-one sidecar bootstrap format between issuer and verifier. Tokens are
limited to 16 KiB, carry exact run/step/attempt and integer issue/expiry times,
and do not contain the verification key. They are distinct from Auth-issued
call-context tokens. Private key delivery is the runtime's responsibility.

Shared call-context verifier for Custos (AS-IMPL-019).

Every Custos component (catalog, workflow, trigger, connector, …) plugs this
library into its request pipeline to validate the EdDSA-signed call-context
JWT carried in the `X-Custos-Callctx` header (the canonical name is the
lowercase form `x-custos-callctx`, exposed by the library as
`custos_callctx.CALLCTX_HEADER`; header lookup is case-insensitive).
The library fetches and caches
the Auth Service's JWKS, verifies the signature plus `iss`/`aud`/`exp`/`iat`,
and returns a typed `CallContext` object describing the acting principal,
workspace scope, and caller component.

See [`design/components/auth-service/design.md`](../../../design/components/auth-service/design.md)
§ Internal vs External Auth — Trust Model.

## Background execution identity

`custos_callctx.execution` defines credential-free `ExecutionIdentity` references
and an operator-owned `ExecutionBindings` registry. The registry is deployment
configuration, not a workflow/event input:

```json
{
  "bindings": [{
    "workspaceId": "ws-team",
    "principalId": "receiver-executor",
    "bindingId": "receiver-v1",
    "secretRef": "receiver-service-credential",
    "secretKey": "token"
  }],
  "defaults": {"ws-team": "receiver-v1"}
}
```

Select a default only for new work. Persist the returned reference with the
accepted target/run and resolve that exact reference during retry/replay;
changed defaults or account IDs cannot retarget it. Rotate the secret's value
without changing the binding identity. Missing original bindings fail closed.

Enabled deployments select exactly one source: legacy
`CUSTOS_EXECUTION_BINDINGS` (65,536 UTF-8 bytes), or an absolute
`CUSTOS_EXECUTION_BINDINGS_FILE` with mandatory
`CUSTOS_EXECUTION_BINDINGS_SHA256` (1,000,000 bytes). The file source verifies
one bounded regular-file snapshot before strict decoding and the same closed
semantic validation; projected symlinks are supported. Both retain the
1,024-binding/120-character limits. Ambiguous sources, invalid digests and
unreadable/invalid files fail closed, never falling back. Disabled mode ignores
unused source settings. There is no hot reload.

See the [managed provisioning, migration and recovery procedure](../../../docs/operations/execution-bindings.md)
before switching an existing Helm release to file mode.

`ExecutionContextClient.headers(identity, operation)` reads the credential through
Dapr Secrets and obtains a fresh Auth context. Call it only inside the actual
I/O boundary, never while building durable workflow history. The caller owns its
HTTP client. Credentials and context tokens are not cached or persisted; response
bodies are bounded, redirects rejected, and failures carry safe closed categories.
The client does not replace receiving-side signature, audience or permission checks.

Credential exchange signs an `executionOperation` claim that the generic signing
RPC cannot request. Execution receivers require the matching operation as well
as its permission; a legacy signed grant is not sufficient. The verifier rejects
unknown operations, broader grants, missing workspace scope and lifetimes above
300 seconds. Operations are `workflow-start`, `workflow-resume`, `catalog-read`,
`catalog-activity-read`, `connector-bind`, `connector-credentials` and
`activity-execute`. The runtime-only `connector-credentials` operation carries
the existing `connector:bind` permission; it never broadens workspace grants. Activity
metadata reads carry only `catalog:activity-types:read`; workflow reads retain
their separate `catalog:workflows:read` grant.

## Verifier usage

```python
from custos_callctx import CALLCTX_HEADER, CallContextVerifier, InvalidCallContextError

verifier = CallContextVerifier(
    jwks_url="http://auth-service.custos.svc/.well-known/jwks.json",
    audience="custos.internal",
    issuer="custos-auth",
)

try:
    # Pass the JWT under the canonical CALLCTX_HEADER ("x-custos-callctx").
    # Lookup is case-insensitive, so "X-Custos-Callctx" / "x-custos-callctx"
    # / "X-CUSTOS-CALLCTX" all work. Pass header=... to the constructor
    # only when integrating with a transport that already commits to a
    # different name.
    ctx = await verifier.verify(metadata={CALLCTX_HEADER: raw_header})
except InvalidCallContextError as exc:
    # The middleware emits `call-context.invalid` via its audit outbox
    # and renders a 401 to the caller.
    ...
```

## Authenticated service callers

`custos_callctx.mesh` supplies framework-independent, duplicate-preserving
sidecar-token and service-caller checks. `require_service_caller()` first
authenticates the local app hop, then compares the exact configured caller
application ID and namespace. Missing configuration fails closed.
These checks require Dapr 1.18.6 or newer with mutual Transport Layer Security;
older caller headers are not an authenticated identity. They do not authenticate
end users or turn forwarded webhook credentials into Custos permissions.
