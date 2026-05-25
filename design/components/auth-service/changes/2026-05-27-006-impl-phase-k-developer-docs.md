# Change: impl-phase-k-developer-docs

Date: 2026-05-27
Type: component-design
Component: auth-service
Sequence: 006
GitHub Issue: #264
Status: closed

## Summary

Phase K (M1) of the auth-service implementation plan (AS-IMPL-029)
landed. The auth-service now ships a developer-facing reference
guide at [`docs/developers/auth-api.md`](../../../../docs/developers/auth-api.md),
modelled on the existing
[`docs/developers/catalog-api.md`](../../../../docs/developers/catalog-api.md).
The guide is the canonical on-wire contract for every REST endpoint
(`/v1/tenants`, `/v1/workspaces`, `/v1/service-accounts`,
`/v1/tokens`, `/v1/role-bindings`, `/v1/roles`, `/v1/permissions`,
`/v1/principals`, `/v1/auth/*`, `/v1/authz/*`, `/.well-known/jwks.json`)
and every internal RPC method (`authn.verifyToken`,
`authz.authorize`, `authz.verifyAndAuthorize`, `callctx.sign`,
`callctx.verify`). It documents the call-context header
(`x-custos-callctx`) and dev-shim JSON shape, the bypass list, the
full error-envelope taxonomy, the permission registry, the six
built-in roles, and three worked examples (service-account mint +
use, role-binding grant + authorize, gateway call-context
propagation). The doc is linked from
[`docs/developers/README.md`](../../../../docs/developers/README.md).

A new `tests/test_docs_examples.py` self-test runs as part of the
default `pytest -q` invocation and asserts that every fenced
\`\`\`yaml\`\`\` block in the guide parses with `yaml.safe_load`.
Failures pin to the exact starting line of the offending fence.
Refs #267 (auth-service implementation tracker).

## Before

* No developer-facing reference for the auth-service public surface
  existed in `docs/developers/`. The only documentation was the
  internal design doc at `design/components/auth-service/design.md`,
  which is engineering-facing and assumes deep familiarity with the
  Custos architecture.
* `docs/developers/README.md` listed three reference guides
  (Connections API, Catalog API, CEL Expressions) but had no entry
  for Auth.
* External integrators (gateway authors, internal service teams,
  platform tooling) had to read FastAPI source to determine the
  request/response shapes, permission requirements, and error codes.
* There was no doc-example self-test for the auth-service surface,
  so YAML examples added to internal design notes could drift out of
  shape without CI noticing.

## After

### `docs/developers/auth-api.md` (new, 1029 lines)

The reference guide is structured to mirror the catalog-service guide:

* **Base URL and authentication.** Describes the gateway routing
  prefix, bearer-token vs call-context split, and the bypass list.
* **Call-context header.** Documents the `x-custos-callctx` constant,
  the dev-shim JSON shape (`principal_id` required;
  `tenant_id`, `workspace_id`, `permissions`, `iat`, `exp` optional),
  the production JWT claim set (`aud=custos.internal`,
  `iss=custos-auth`), and the JWKS-driven local-verify model.
* **Error envelope and error taxonomy.** Documents the uniform
  `{"error": {"code", "detail", "issues"?}}` envelope and lists every
  code the service can emit with HTTP status and emitter (`not_found`,
  `conflict`, `invalid_request`, `invalid_role_scope`,
  `request_validation_failed`, `unauthenticated`, `permission_denied`,
  the three `callctx_*` middleware codes, the five `oidc_*` codes,
  `not_implemented`, `http_error`). Notes that
  `POST /rpc/callctx.verify` uses a structured 200-body verdict
  (closed-set reasons: `malformed`, `unknown_kid`, `bad_signature`,
  `expired`, `wrong_audience`, `wrong_issuer`).
* **Permission registry.** Lists every permission used across the
  platform with owning service and the built-in roles that grant it.
* **Built-in roles.** Lists the four workspace roles
  (`viewer/author/operator/admin`), `tenant.admin`, and
  `platform.admin` with their permission tuples and notes the
  platform-admin short-circuit.
* **Per-resource sections.** Tenants, Workspaces, Service Accounts,
  Service Tokens, Role Bindings, Roles / Permissions catalog,
  Principals, Auth (verify + OIDC callback), Authz
  (verify-and-authorize). Each documents path, permission, request
  body, response shape, error codes, and audit events.
* **JWKS / `.well-known`.** Documents the RFC 7517 + 8037 JWK Set
  shape, the 16-hex `kid` format, overlap-window inclusion, and the
  cache header.
* **Internal RPC surface.** Documents every method under `/rpc/`
  with request, response, bypass classification, and JWT claim set
  for `callctx.sign`.
* **Worked examples.** Three end-to-end flows derived verbatim from
  the AS-IMPL-029 issue:
  1. Service-account mint + use (create → mint → verify).
  2. Role-binding grant + authorize (`role:workspace.viewer` grants
     `workflow:read`; mismatched permission returns 200 with
     `allowed=false`).
  3. Gateway call-context propagation (bootstrap RPC verifyToken →
     callctx.sign → propagate via header → upstream verifies locally
     via JWKS; optional `callctx.verify` for diagnostics).

### `docs/developers/README.md` (updated)

* Added an `Auth API` row to the **Sections** table pointing at
  `auth-api.md`.
* Bumped `Last Updated:` to 2026-05-27.

### `src/services/auth-service/tests/test_docs_examples.py` (new)

* Walks up from the test file to locate the repo root, then opens
  `docs/developers/auth-api.md`.
* Extracts every \`\`\`yaml fence with a regex identical in shape to
  the catalog-service equivalent at
  `src/services/catalog-service/tests/test_docs_examples.py`.
* `test_auth_api_doc_exists` — sanity: the guide is checked in.
* `test_auth_api_doc_has_yaml_blocks` — guards against a silent pass
  if the regex stops matching (e.g. someone converts every fence to
  \`\`\`json).
* `test_yaml_block_is_well_formed` — parametrized per block, with
  `ids` set to the source line (e.g. `L73`, `L221`, ...) so a
  failure points at the exact starting line of the offending fence.
* Runs as part of default `pytest -q` (no integration marker).

## Impact

* **Developer ergonomics**: external integrators have an
  authoritative reference and three runnable worked examples without
  reading FastAPI source.
* **Contract drift**: every YAML fence in the guide is now a test
  case in the default suite. Renaming a field or shifting the
  envelope shape forces a doc update or breaks `pytest -q`.
* **Coverage**: no production code changed, so the 97.94 % coverage
  total established by sequence 005 is preserved.
* **CI cost**: ~0.03 s added to the default lane (43 parametrized
  YAML-parse cases). No new CI job required — the test runs in the
  existing `auth-service` unit lane.

## Verified

* `pytest tests/test_docs_examples.py -v --no-cov`:
  43 passed (2 sanity + 41 YAML blocks) in 0.03 s.
* `pytest -q`: 615 passed / 5 deselected (43 new cases on top of
  the 572 baseline from sequence 005).
* `ruff check tests/test_docs_examples.py`,
  `ruff format --check tests/test_docs_examples.py`,
  `mypy tests/test_docs_examples.py` — all clean.
* All 19 internal anchor references in `auth-api.md` resolve to a
  declared heading.
* Single external relative link
  (`../../design/components/auth-service/design.md`) resolves.

## Related Requirements

* `REQ-051` — auth-service must verify service tokens against a
  durable store. The guide documents the verify endpoint contract.
* `REQ-052` — auth-service must invalidate cached authorize decisions
  when a binding changes. The guide documents the role-binding
  endpoints and the binding-changed audit emission.

## Related Change Records

* `2026-05-27-005-impl-phase-k-integration-tests.md` — Phase K M2
  (#263). Sequence 005 established the integration-test surface
  that this doc references; the YAML examples here are shaped by
  the contracts pinned in `test_round_trip.py`.
* `2026-05-26-004-impl-phase-k-coverage-gate.md` — Phase K M1
  (#262). Established the coverage gate this PR preserves.
