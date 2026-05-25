# Change: impl-callctx-permissions-claim

Date: 2026-05-28
Type: component-design
Component: auth-service
Sequence: 007
GitHub Issue: #265
Status: open

## Summary

PR1 of the two-PR sequence to deliver AS-IMPL-030 (#265 — "wire
catalog-service to the real call-context verifier"). This change
extends the platform call-context wire format with two enablers that
the catalog wire-up (PR2) and every other downstream component
needs:

1. **`permissions` claim — "fat call-context"** (Option D in the
   design review).  The API gateway pre-computes the effective grants
   for the principal × workspace at mint time and embeds them in the
   signed JWT as a `permissions` claim (JSON array of strings).
   Downstream components authorize a request locally by checking
   `CallContext.has_permission(...)` rather than calling back to
   `authz.authorize` on every hop. The claim is OPTIONAL on the wire
   — when absent (or empty), the verifier surfaces an empty
   `frozenset` and back-compat is preserved.

2. **Per-mint audience override.** `CallContextSigner.sign(...)` and
   `POST /rpc/callctx.sign` now accept an `audience` parameter that
   replaces the signer's configured default (`custos.internal`) for
   that single mint. When the API gateway dispatches an inbound
   request to a specific downstream component (e.g.
   catalog-service), it mints a JWT with `audience: custos.catalog`
   so the resulting token is rejected by every other component
   audience. `POST /rpc/callctx.verify` accepts a matching `audience`
   request field so the auth-service can verify per-component
   tokens.

The change is delivered jointly to **two packages**:

* `src/libs/custos-callctx` — the verifier library used by every
  downstream component. Adds the `permissions` field to
  `CallContext` and the extraction + malformed-claim handling to
  `_verifier.py`.
* `src/services/auth-service` — extends the signer
  (`callctx_signer.py`) with the two new kwargs, and propagates them
  through the `/rpc/callctx.sign` and `/rpc/callctx.verify` RPC
  surfaces.

PR1 does NOT close #265. It lands the wire-format enabler; PR2
(catalog-service wire-up, separate branch) consumes it and closes
the issue.

## Before

* `CallContext` had no `permissions` field. Downstream components
  that wanted to authorize a request had two choices: re-call
  `authz.authorize` on every hop (round-trip cost on the hot path)
  or hand-roll their own grant cache (M1 catalog and trigger
  services took this path during AS-IMPL-023/024 and AS-IMPL-026).
* `CallContextSigner.sign(...)` had no `audience` override. Every
  mint used the signer's constructor-configured audience
  (`custos.internal` by default), so a single auth-service instance
  could not target different audiences for different downstream
  components without spinning up a second signer.
* `POST /rpc/callctx.sign` accepted only the four legacy fields
  (`principal_id`, `workspace_id`, `caller_component`,
  `ttl_seconds`). `POST /rpc/callctx.verify` enforced exactly the
  signer's configured audience.
* The `permissions` claim was already mentioned in the design doc
  (`design/components/auth-service/design.md`) as a forward-looking
  field on the call-context model, and the dev-shim
  `x-custos-callctx` header already carried it for unit tests, but
  the on-wire JWT did not.

## After

### `src/libs/custos-callctx` library

* `_context.py` — `CallContext` gains
  `permissions: frozenset[str] = field(default_factory=frozenset)`
  and a `has_permission(name: str) -> bool` exact-match helper
  (no wildcards in M1 — matches the registry shape from AS-IMPL-029
  developer docs).
* `_verifier.py` — `_build_context` extracts the new claim via a
  new private static method `_extract_permissions(claims, *, kid)`:
  * absent / `None` → empty `frozenset` (legacy tokens still
    verify),
  * not a list → `MALFORMED_TOKEN`,
  * any non-string or empty-string entry → `MALFORMED_TOKEN`,
  * list of non-empty strings → `frozenset` (dedup is implicit; the
    on-wire order/duplication is intentionally discarded).
* `tests/_helpers.py` — `SigningKeyFixture.mint(...)` gains
  `permissions: list[str] | None = None` so fixtures can construct
  signed tokens carrying the claim.
* `tests/test_verifier.py` — 11 new tests under the
  "permissions claim (Option D fat call-context)" header cover the
  happy path, empty-claim semantics, malformed-shape failures, and
  the `has_permission` helper. Total library tests: 41 → 52.

### `src/services/auth-service`

* `callctx_signer.py` — `CallContextSigner.sign(...)` gains:
  * `permissions: list[str] | None = None`. None or empty list →
    claim is OMITTED from the JWT (back-compat). Otherwise the list
    is normalised through `_normalise_permissions` (a new
    staticmethod that rejects non-string and empty-string entries)
    and embedded verbatim on the wire.
  * `audience: str | None = None`. None → use the constructor-
    configured default (`custos.internal` in production). Empty
    string → `ValueError`. Otherwise the override replaces the JWT
    `aud` claim for that mint only.
* `api/routes/rpc.py`:
  * `CallctxSignRpcRequest` gains
    `permissions: list[str]` (default factory `[]`, max length 256,
    each entry 1..128 chars — enforced by pydantic) and
    `audience: str | None` (default `None`, 1..120 chars when
    present). The pydantic-422 path covers the
    invalid-shape branches the signer would have rejected at
    runtime.
  * `callctx_sign` handler forwards both new fields to
    `signer.sign(...)`.
  * `CallctxVerifyRpcRequest` gains `audience: str | None = None`
    and the handler uses `body.audience if body.audience is not
    None else DEFAULT_AUDIENCE` when calling `jwt.decode(...)`.
  * `CallctxVerifyRpcResponse` gains
    `permissions: list[str] | None = None`. On a successful verify
    the handler extracts the claim, re-validates the shape (list of
    non-empty strings, else `_invalid(REASON_MALFORMED)`), and
    populates the response field. A token minted without the claim
    surfaces as an empty list, distinguishing "no embedded grant"
    from "decode failure" (which is signalled by `valid: false`).

### Test counts

* `custos-callctx`: 41 → 52 (+11) — library happy/failure paths.
* `auth-service` (non-integration): 615 → 629 (+14):
  * `tests/test_callctx_signer.py`: 32 → 41 (+9) — permissions
    omission, embedding, ordering preservation, malformed entry
    rejection, audience override, default fallback, empty-override
    rejection.
  * `tests/test_rpc.py`: +5 — sign + verify happy path with both
    new fields, malformed-permission 422, empty-audience 422,
    audience-override round-trip.
* `auth-service` integration: 5 → 6 — new
  `test_callctx_sign_with_permissions_and_audience_round_trip`
  exercises the gateway → catalog target flow end-to-end.

### Developer docs

[`docs/developers/auth-api.md`](../../../../docs/developers/auth-api.md)
gains:

* `/rpc/callctx.sign` request schema documents the new
  `permissions` and `audience` fields and the JWT-claim shape now
  shows `permissions` as a decoded array. Plain prose calls out the
  empty-list semantics, the per-mint audience override behaviour,
  and the API-gateway-only trust model that pre-computes the
  permissions list.
* `/rpc/callctx.verify` request schema documents the new `audience`
  field, the response schema documents the new `permissions` array
  (always present on success), and prose documents the
  malformed-claim path.

The doc-example self-test (`tests/test_docs_examples.py`) reparses
every fenced \`\`\`yaml\`\`\` block; no count changes were needed
(the updated blocks were edited in place).

## Trade-offs and follow-ups

* **Claim size.** The `permissions` field is bounded by the RPC
  schema at 256 entries × 128 chars ≈ 32 KiB worst-case before
  EdDSA signing. With realistic grants (≤ 20 entries) the JWT
  stays well under the 4 KiB header soft limit.
* **No wildcards in `has_permission`.** Matches the registry shape
  documented in AS-IMPL-029. A future M2 change can introduce
  wildcard matching in the helper without a wire-format break.
* **Mandatory permissions on M2.** The claim is OPTIONAL in PR1 so
  the existing AS-IMPL-026 + AS-IMPL-024 callers keep working
  during the rollout. PR2 (catalog wire-up) will start consuming
  the claim. A future M2 step can flip the verifier to REQUIRE the
  claim for non-platform-global tokens, with a deprecation window.
* **Per-component audiences are advisory in PR1.** Only the
  catalog-service will pin `CAT_CALLCTX_AUDIENCE=custos.catalog` in
  PR2; every other component continues to verify against
  `custos.internal`. M2 will fan out per-component audiences as
  each downstream component is wired through.

Refs #265, #267.
