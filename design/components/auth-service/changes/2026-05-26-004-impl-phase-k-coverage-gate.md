# Change: impl-phase-k-coverage-gate

Date: 2026-05-26
Type: component-design
Component: auth-service
Sequence: 004
GitHub Issue: #262
Status: closed

## Summary

Phase K of the M3 auth-service implementation plan (AS-IMPL-027) landed.
Direct unit tests for the FastAPI error handlers were authored, the
remaining sub-90% per-module gaps in `custos_auth.api.routes.oidc` and
`custos_auth.sweeper` were closed, and the ≥90% TOTAL coverage gate is
confirmed enforced on both Python 3.11 and 3.12 CI lanes. Refs #267 (M3
auth-service track).

## Before

* No direct unit tests for `custos_auth.api.errors`. The error envelope
  contract (`{"error": {"code": "...", "detail": "...", "issues"?: [...]}}`)
  was only verified end-to-end through route tests, so a regression in
  the renderer logic — particularly `handle_http_exception` and
  `handle_validation_error`'s `loc` coercion — could ship undetected
  until a downstream consumer noticed the wire shape had drifted.
  `handle_http_exception` itself was uncovered (`api/errors.py:174–176`).
* `custos_auth.api.routes.oidc` sat at 87.5 % per-module coverage. The
  uncovered branches were the exchange-failure subbranches (transport
  exception, non-JSON response body, response missing `id_token`), the
  `redirect_uri` forwarding path, the `client_id`/`client_secret_env`
  missing-pair check, the bare-app `oidc_not_implemented` fallback, and
  the `preset != None` path through `_preset_audit_extras`.
* `custos_auth.sweeper` sat at 89.5 %. The publisher-failure branch in
  `_emit_for` and the non-zero-deletion INFO log in `run_sweeper_loop`
  had no test exercising them.
* The `--cov-fail-under=90` CI gate was already wired in
  `.github/workflows/python-services.yml` (Phase A landed it alongside
  the catalog-service template), but with no Phase K reaffirmation the
  gate's behaviour-vs-reality margin had not been audited.

## After

### `tests/test_api_errors.py` (new)

Direct unit tests for every code path in `custos_auth.api.errors`:

| Behaviour pinned | Test |
|---|---|
| Each `AuthApiError` subclass carries the documented status + machine-readable code | `test_auth_api_error_subclass_pins_status_and_code` (parametrised) |
| Base class falls back to `500` / `internal_error` | `test_auth_api_error_base_defaults_to_500_internal_error` |
| Envelope keyset locked to `{code, detail}` (no `issues` when absent) | `test_handle_auth_api_error_envelope_keyset_is_locked` |
| `InvalidRoleScope` renders as `400` / `invalid_role_scope` | `test_handle_auth_api_error_renders_invalid_role_scope` |
| Pydantic `RequestValidationError` → per-field `issues[]` array; tuple `loc` elements coerced to `str` | `test_handle_validation_error_renders_issues_array` |
| Defensive defaults for missing `msg` / `type` keys | `test_handle_validation_error_uses_defaults_for_missing_fields` |
| `StarletteHTTPException` with string detail | `test_handle_http_exception_renders_string_detail` |
| `StarletteHTTPException` with dict detail → stringified | `test_handle_http_exception_coerces_non_string_detail` |
| `register_exception_handlers` installs the full set | `test_register_exception_handlers_installs_full_set` |

`handle_http_exception` is now 100 % covered; `api/errors.py` is at
100 %.

### Supplemental OIDC callback coverage (`tests/test_oidc_callback.py`)

Seven new tests closed every remaining branch in
`custos_auth.api.routes.oidc`:

* `test_oidc_callback_bare_app_returns_503_oidc_not_implemented` — bare
  `FastAPI` (no lifespan) exercises both the
  `parse_issuers_config(settings.oidc_issuers_raw)` fallback in
  `_get_oidc_state` and the `oidc_not_implemented` short-circuit.
* `test_oidc_callback_503_when_issuer_missing_client_id` — issuer entry
  with `token_endpoint` but no `client_id` returns
  `oidc_not_configured` (the parser permits the omission for
  workload-token issuers; the route enforces it for code-flow callers).
* `test_oidc_callback_502_on_exchange_transport_failure` — `httpx`
  transport-level exception during the code exchange surfaces as
  `502 oidc_exchange_failed` with an `authn.failure` audit row whose
  `reason=exchange_failed`.
* `test_oidc_callback_502_on_exchange_response_not_json` — non-JSON
  token-endpoint body (HTML maintenance page) collapses to the same
  closed-set reason.
* `test_oidc_callback_502_on_exchange_missing_id_token` — `200 OK` JSON
  body without an `id_token` field (e.g. provider returned only an
  access token when the `openid` scope was missing) likewise collapses
  to `exchange_failed`.
* `test_oidc_callback_forwards_redirect_uri_to_exchange` — when the
  caller supplies `redirect_uri`, the form-encoded payload sent to the
  token endpoint must carry it (GitHub OAuth and other providers
  reject the exchange when `redirect_uri` was present on
  `/authorize` but absent on the token call).
* `test_oidc_callback_github_preset_attaches_extras_to_audit` —
  `preset: github` lights up `_preset_audit_extras` and pulls
  `repository` / `repository_id` / `workflow` / `ref` onto the
  `authn.success` audit row.

`api/routes/oidc.py` is now 100 % covered.

### Sweeper publisher-failure + delete-count visibility tests (`tests/test_sweeper.py`)

* `test_sweep_once_emits_per_row_even_when_publish_fails` — pub/sub
  exceptions in `TokenRevokedPublisher.publish` are caught,
  WARNING-logged, and the SPL physical delete still runs (symmetric
  with the existing audit-fail test).
* `test_run_sweeper_loop_logs_count_after_nonzero_deletion` — pins
  the `deleted N expired token(s)` operator-visibility INFO line so
  log-search alerts keyed on the message keep working across
  refactors.

`sweeper.py` rises from 89.5 % to 96.5 %.

### Coverage gate

* `.github/workflows/python-services.yml` (auth-service job, lines
  144–145) runs `pytest -q --cov=custos_auth --cov-report=term-missing
  --cov-fail-under=90` on the Python 3.11 + 3.12 matrix lanes.
* Local enforcement remains opt-in (consistent with catalog-service's
  pattern); developers run `pytest --cov=custos_auth --cov-fail-under=90`
  to mirror CI behaviour.

## Impact

* **Coverage envelope**: TOTAL 97.3 % → 97.98 %, +0.68 pp.
* **Per-module floor**: lifted from 87.5 % (`oidc.py`) to 92.9 %
  (`custos_auth.__init__`). Every module except `custos_auth.__init__`
  is now ≥ 94 %.
* **Test count**: 550 → 572 (+22 tests across three files).
* **Pytest suite runtime**: 10.15 s → 10.67 s (well under the 30 s
  acceptance threshold).
* **CI cost**: marginal — the new tests use the same in-memory fakes
  and `httpx.MockTransport` infrastructure the existing OIDC tests
  already pay for.
* **Phase K downstream**: `AS-IMPL-028` (#263) can now stand up the
  integration suite on top of a green coverage gate; `AS-IMPL-029`
  (#264) can document the locked-in error envelope shape without fear
  of drift.

## Verified

* 572 / 572 tests pass.
* `pytest --cov=custos_auth --cov-fail-under=90`: gate green at
  97.98 % TOTAL.
* `ruff check`, `ruff format --check`, `mypy` (default 3.13), and
  `mypy --python-version 3.11` clean across `src/` + `tests/`.
* CI workflow (`.github/workflows/python-services.yml`) unchanged on
  the auth-service lanes — the gate it already enforces is now
  audited as honoured by every test path.

## Related Requirements

REQ-051 (audit trail), REQ-052 (telemetry / SLO signals).

## Related Change Records

* `design/components/auth-service/changes/2026-05-25-003-impl-phase-j-observability.md`
  — Phase J shipped the `_telemetry.py` surface and the audit
  emission paths whose error branches are exercised here.
* `design/components/auth-service/changes/2026-05-25-002-impl-phase-h-oidc.md`
  — Phase H landed the OIDC callback and verifier paths the
  supplemental coverage covers.
