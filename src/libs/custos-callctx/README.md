# custos-callctx

Shared call-context verifier for Custos (AS-IMPL-019).

Every Custos component (catalog, workflow, trigger, connector, …) plugs this
library into its request pipeline to validate the EdDSA-signed call-context
JWT carried in the `X-Call-Context` header. The library fetches and caches
the Auth Service's JWKS, verifies the signature plus `iss`/`aud`/`exp`/`iat`,
and returns a typed `CallContext` object describing the acting principal,
workspace scope, and caller component.

See [`design/components/auth-service/design.md`](../../../design/components/auth-service/design.md)
§ Internal vs External Auth — Trust Model.

## Usage

```python
from custos_callctx import CallContextVerifier, InvalidCallContextError

verifier = CallContextVerifier(
    jwks_url="http://auth-service.custos.svc/.well-known/jwks.json",
    audience="custos.internal",
    issuer="custos-auth",
)

try:
    ctx = await verifier.verify(metadata={"X-Call-Context": raw_header})
except InvalidCallContextError as exc:
    # The middleware emits `call-context.invalid` via its audit outbox
    # and renders a 401 to the caller.
    ...
```
