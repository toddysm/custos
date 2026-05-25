# 2026-05-25 — Phase H landed: OIDC verifier + GitHub/Entra presets + provisioning policy

**Scope:** AS-IMPL-020 (#255), AS-IMPL-021 (#256), AS-IMPL-022 (#257), AS-IMPL-023 (#258). Refs M3 OIDC track (REQ-034/056/057/058).

## What shipped

`POST /v1/auth/login/oidc/callback` is now a fully-wired endpoint (no longer the M1 503 stub). When `CUSTOS_AUTH_OIDC_ENABLED=true` and `CUSTOS_AUTH_OIDC_ISSUERS` carries at least one issuer block, the handler:

1. Exchanges the gateway-supplied `code` at the issuer's token endpoint (server-side; the `client_secret` is sourced from a Dapr-projected env var named by `client_secret_env`, never written to the values file).
2. Verifies the returned `id_token` against the configured JWKS (RS256/ES256/EdDSA, audience pinning, issuer pinning, expiry, signing-kid enforcement).
3. Applies the matched preset's defaults (GitHub or Entra), then provisions a zero-binding `User` on first contact via the SPL `AuthStoreProvider`. Subsequent logins resolve to the same `User` through the stable `(issuer, subject) → userId` `OidcIdentity` record.
4. Emits `authn.success` (with preset-specific audit extras: GitHub `repository` / `workflow` / `event_name`; Entra `tid` / `appid` / `preferred_username`) or `authn.failure` with a closed-set reason.

## Closed-set OIDC failure reasons (verifier)

`malformed`, `unknown_kid`, `bad_signature`, `expired`, `immature`, `wrong_audience`, `wrong_issuer`, `wrong_algorithm`, `missing_claim`, `jwks_fetch_failed`. Exposed as `custos_auth.oidc.verifier.FAILURE_REASONS: frozenset[str]`.

## Configuration

`CUSTOS_AUTH_OIDC_ISSUERS` is a single-line JSON document of the form

```json
{
  "issuers": [
    {
      "id": "github",
      "preset": "github",
      "audiences": ["api://custos"],
      "token_endpoint": "https://github.com/login/oauth/access_token",
      "client_id": "Iv1.example",
      "client_secret_env": "CUSTOS_AUTH_OIDC_GITHUB_SECRET"
    },
    {
      "id": "entra",
      "preset": "entra",
      "issuer_url": "https://login.microsoftonline.com/{tenant}/v2.0",
      "jwks_uri": "https://login.microsoftonline.com/{tenant}/discovery/v2.0/keys",
      "audiences": ["api://custos"],
      "token_endpoint": "https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token",
      "client_id": "00000000-0000-0000-0000-000000000000",
      "client_secret_env": "CUSTOS_AUTH_OIDC_ENTRA_SECRET"
    }
  ]
}
```

Per-issuer fields (full schema in `src/custos_auth/oidc/config.py`):

| Field | Required | Preset-default? | Notes |
|---|---|---|---|
| `id` | yes | — | Stable opaque id the gateway echoes back on `/callback` — must be unique. |
| `preset` | optional | — | One of `github`, `entra`; supplies the omitted defaults. |
| `issuer_url` | yes (github preset supplies) | github only | `iss` claim value enforced on verification. |
| `jwks_uri` | yes (github preset supplies) | github only | HTTPS endpoint hosting the issuer's JWKS. |
| `audiences` | yes | — | Non-empty list; the `aud` claim must match exactly one entry. |
| `algorithms` | optional | both | Defaults to `["RS256"]` (extensible per issuer). |
| `subject_claim` | optional | both | `sub` (GitHub) / `oid` (Entra). |
| `token_endpoint` | required for `/callback` | — | OAuth 2.0 token endpoint URL. |
| `client_id` | required for `/callback` | — | OAuth 2.0 client id. |
| `client_secret_env` | required for `/callback` | — | **Name** of the env var carrying the secret (never the secret itself). |
| `group_claim` | optional | entra | Claim whose string-list value is intersected with `group_bindings`. |
| `group_bindings` | optional | — | `{claim_value → [role_binding_ref...]}` matched at authn-success time. |
| `provisioning_policy` | optional | — | `create_user_with_zero_bindings` (default, only v1 value). |

Strict parser: unknown fields, unknown preset names, unsupported provisioning policies, duplicate ids, and empty audiences fail-fast at startup.

## Helm

`deploy/helm/charts/auth-service/values.yaml` now ships:

```yaml
config:
  oidcEnabled: "false"   # AS-IMPL-024 flag; flip to "true" to enable verification
  oidcIssuers: ""        # Paste the single-line JSON document above
```

`CUSTOS_AUTH_OIDC_GITHUB_SECRET` / `CUSTOS_AUTH_OIDC_ENTRA_SECRET` (or any name chosen via `client_secret_env`) MUST be injected via the chart's `externalSecret` or the operator's secret-store path; they MUST NOT be set in values.yaml.

## Audit additions

`authn.success` and `authn.failure` now carry `authentication_type="oidc"`. On success the payload includes `issuer` (the issuer config id, e.g. `"github"`) plus the matched preset's extras (repository / workflow / event_name for GitHub; tid / preferred_username / appid for Entra). On failure the payload carries `reason` (one of the 10 closed-set strings above).

`oidc.identity-linked` continues to fire exactly once — at first-contact provisioning — and is the only event that proves the `(issuer, subject) → userId` binding was created in the current transaction.

## Verified

- 535 / 535 tests pass (86 new tests across 7 OIDC test files).
- 97.1 % coverage on `src/custos_auth` (gate: 90 %).
- `ruff check`, `ruff format --check`, and `mypy --python-version 3.11` clean.
- `helm lint` clean for both eval and HA profiles.

## Closes / refs

Closes #255, #256, #257, #258. Refs #267 (M3 OIDC track).
