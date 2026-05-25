"""Tests for ``custos_auth.oidc.config`` (AS-IMPL-020).

Exercises the strict JSON-issuers parser, preset merge logic, and the
closed-set error reporting that gates the lifespan on misconfigurations.
"""

from __future__ import annotations

import pytest

from custos_auth.oidc.config import (
    DEFAULT_ALGORITHMS,
    DEFAULT_SUBJECT_CLAIM,
    KNOWN_PRESETS,
    GroupBinding,
    IssuersConfig,
    OidcConfigError,
    parse_issuers_config,
)

# ---------------------------------------------------------------------------
# Empty / no-op input
# ---------------------------------------------------------------------------


def test_parse_empty_string_returns_empty_config() -> None:
    config = parse_issuers_config("")
    assert isinstance(config, IssuersConfig)
    assert config.issuers == ()


def test_parse_whitespace_only_returns_empty_config() -> None:
    config = parse_issuers_config("   \n\t  ")
    assert config.issuers == ()


def test_parse_explicit_empty_issuers_list_returns_empty_config() -> None:
    config = parse_issuers_config('{"issuers": []}')
    assert config.issuers == ()


# ---------------------------------------------------------------------------
# Happy paths
# ---------------------------------------------------------------------------


def test_parse_single_explicit_issuer() -> None:
    raw = """
    {
      "issuers": [
        {
          "id": "okta-prod",
          "issuer_url": "https://example.okta.com",
          "jwks_uri": "https://example.okta.com/oauth2/v1/keys",
          "audiences": ["api://custos"]
        }
      ]
    }
    """
    config = parse_issuers_config(raw)
    assert len(config.issuers) == 1
    entry = config.issuers[0]
    assert entry.id == "okta-prod"
    assert entry.preset is None
    assert entry.issuer_url == "https://example.okta.com"
    assert entry.audiences == ("api://custos",)
    # Defaults applied when fields are omitted.
    assert entry.algorithms == DEFAULT_ALGORITHMS
    assert entry.subject_claim == DEFAULT_SUBJECT_CLAIM
    assert entry.provisioning_policy == "zero-binding"
    assert entry.group_claim is None
    assert entry.group_bindings == ()
    assert entry.token_endpoint is None
    assert entry.client_id is None
    assert entry.client_secret_env is None


def test_parse_github_preset_fills_defaults() -> None:
    raw = """
    {
      "issuers": [
        {"id": "gh", "preset": "github"}
      ]
    }
    """
    config = parse_issuers_config(raw)
    entry = config.issuers[0]
    assert entry.preset == "github"
    assert entry.issuer_url == "https://token.actions.githubusercontent.com"
    assert entry.jwks_uri.endswith("/.well-known/jwks")
    assert entry.audiences == ("custos",)
    assert entry.algorithms == ("RS256",)
    assert entry.subject_claim == "sub"


def test_parse_entra_preset_requires_explicit_issuer_url() -> None:
    # Entra preset deliberately omits an issuer_url default; the parser
    # fails fast when the operator forgets to pin a tenant.
    raw = '{"issuers": [{"id": "entra-prod", "preset": "entra"}]}'
    with pytest.raises(OidcConfigError, match="issuer_url"):
        parse_issuers_config(raw)


def test_parse_entra_preset_with_explicit_issuer_url_fills_remaining_defaults() -> None:
    raw = """
    {
      "issuers": [
        {
          "id": "entra-prod",
          "preset": "entra",
          "issuer_url": "https://login.microsoftonline.com/tenantguid/v2.0",
          "audiences": ["api://custos"]
        }
      ]
    }
    """
    config = parse_issuers_config(raw)
    entry = config.issuers[0]
    assert entry.subject_claim == "oid"
    assert entry.group_claim == "groups"
    assert entry.algorithms == ("RS256",)
    assert "discovery/v2.0/keys" in entry.jwks_uri


def test_parse_preset_with_explicit_overrides_wins() -> None:
    # Operator-supplied fields always win over preset defaults.
    raw = """
    {
      "issuers": [
        {
          "id": "gh-custom",
          "preset": "github",
          "audiences": ["custos-prod", "custos-stage"],
          "algorithms": ["RS256", "ES256"]
        }
      ]
    }
    """
    config = parse_issuers_config(raw)
    entry = config.issuers[0]
    assert entry.audiences == ("custos-prod", "custos-stage")
    assert entry.algorithms == ("RS256", "ES256")
    # Untouched fields still default-filled from preset.
    assert entry.issuer_url == "https://token.actions.githubusercontent.com"


def test_parse_group_bindings_list() -> None:
    raw = """
    {
      "issuers": [
        {
          "id": "entra-with-groups",
          "preset": "entra",
          "issuer_url": "https://login.microsoftonline.com/tenant/v2.0",
          "audiences": ["api://custos"],
          "group_bindings": [
            {"claim_value": "admins-guid", "role": "platform-admin", "workspace_id": "ws-1"},
            {"claim_value": "ops-guid", "role": "viewer", "workspace_id": "ws-2"}
          ]
        }
      ]
    }
    """
    config = parse_issuers_config(raw)
    entry = config.issuers[0]
    assert len(entry.group_bindings) == 2
    assert entry.group_bindings[0] == GroupBinding(
        claim_value="admins-guid", role="platform-admin", workspace_id="ws-1"
    )


def test_issuers_config_by_id_lookup() -> None:
    config = parse_issuers_config('{"issuers": [{"id": "gh", "preset": "github"}]}')
    assert config.by_id("gh") is not None
    assert config.by_id("missing") is None


# ---------------------------------------------------------------------------
# Validation errors
# ---------------------------------------------------------------------------


def test_parse_invalid_json_raises() -> None:
    with pytest.raises(OidcConfigError, match="invalid JSON"):
        parse_issuers_config("{not json")


def test_parse_non_object_top_level_raises() -> None:
    with pytest.raises(OidcConfigError, match="top-level value must be an object"):
        parse_issuers_config("[]")


def test_parse_rejects_unknown_top_level_keys() -> None:
    with pytest.raises(OidcConfigError, match="unknown top-level keys"):
        parse_issuers_config('{"issuers": [], "garbage": 1}')


def test_parse_rejects_non_list_issuers() -> None:
    with pytest.raises(OidcConfigError, match="'issuers' must be a list"):
        parse_issuers_config('{"issuers": {}}')


def test_parse_rejects_unknown_issuer_keys() -> None:
    raw = """
    {
      "issuers": [
        {"id": "gh", "preset": "github", "extra": "nope"}
      ]
    }
    """
    with pytest.raises(OidcConfigError, match="unknown keys"):
        parse_issuers_config(raw)


def test_parse_rejects_unknown_preset() -> None:
    with pytest.raises(OidcConfigError, match="unknown preset"):
        parse_issuers_config('{"issuers": [{"id": "x", "preset": "auth0"}]}')


def test_parse_rejects_unsupported_provisioning_policy() -> None:
    raw = """
    {
      "issuers": [
        {
          "id": "gh", "preset": "github",
          "provisioning_policy": "auto-bind-admin"
        }
      ]
    }
    """
    with pytest.raises(OidcConfigError, match="provisioning_policy"):
        parse_issuers_config(raw)


def test_parse_rejects_missing_required_fields() -> None:
    # No preset, no issuer_url → fails post-merge validation.
    raw = """
    {
      "issuers": [
        {"id": "x", "jwks_uri": "https://example.com/keys", "audiences": ["a"]}
      ]
    }
    """
    with pytest.raises(OidcConfigError, match="issuer_url"):
        parse_issuers_config(raw)


def test_parse_rejects_missing_audiences() -> None:
    raw = """
    {
      "issuers": [
        {
          "id": "x",
          "issuer_url": "https://example.com",
          "jwks_uri": "https://example.com/keys"
        }
      ]
    }
    """
    with pytest.raises(OidcConfigError, match="audiences"):
        parse_issuers_config(raw)


def test_parse_rejects_duplicate_issuer_ids() -> None:
    raw = """
    {
      "issuers": [
        {"id": "dup", "preset": "github"},
        {"id": "dup", "preset": "github"}
      ]
    }
    """
    with pytest.raises(OidcConfigError, match="duplicate issuer id"):
        parse_issuers_config(raw)


def test_parse_rejects_empty_id() -> None:
    with pytest.raises(OidcConfigError, match="'id'"):
        parse_issuers_config('{"issuers": [{"id": "", "preset": "github"}]}')


def test_parse_rejects_non_string_audiences_entries() -> None:
    raw = """
    {
      "issuers": [
        {
          "id": "x", "preset": "github",
          "audiences": [123]
        }
      ]
    }
    """
    with pytest.raises(OidcConfigError, match="audiences"):
        parse_issuers_config(raw)


def test_parse_rejects_group_binding_with_unknown_keys() -> None:
    raw = """
    {
      "issuers": [
        {
          "id": "x", "preset": "entra",
          "issuer_url": "https://login.microsoftonline.com/t/v2.0",
          "audiences": ["a"],
          "group_bindings": [
            {"claim_value": "g", "role": "r", "workspace_id": "w", "bogus": true}
          ]
        }
      ]
    }
    """
    with pytest.raises(OidcConfigError, match="unknown keys"):
        parse_issuers_config(raw)


def test_known_presets_set_matches_modules() -> None:
    assert frozenset({"github", "entra"}) == KNOWN_PRESETS
