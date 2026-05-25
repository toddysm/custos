"""Tests for ``custos_auth.oidc.presets.github`` (AS-IMPL-021)."""

from __future__ import annotations

import pytest

from custos_auth.oidc.presets import github


def test_name_constant() -> None:
    assert github.name == "github"


def test_defaults_shape() -> None:
    defaults = github.defaults()
    assert defaults["issuer_url"] == "https://token.actions.githubusercontent.com"
    jwks_uri = defaults["jwks_uri"]
    assert isinstance(jwks_uri, str) and jwks_uri.endswith("/.well-known/jwks")
    assert defaults["audiences"] == ("custos",)
    assert defaults["algorithms"] == ("RS256",)
    assert defaults["subject_claim"] == "sub"


def test_extract_subject_returns_workload_sub() -> None:
    claims = {"sub": "repo:acme/sandbox:ref:refs/heads/main"}
    assert github.extract_subject(claims) == claims["sub"]


def test_extract_subject_rejects_missing_sub() -> None:
    with pytest.raises(ValueError, match="'sub' claim"):
        github.extract_subject({})


def test_extra_audit_payload_surfaces_workload_claims() -> None:
    claims = {
        "repository": "acme/sandbox",
        "repository_id": 123456,
        "workflow": "deploy",
        "ref": "refs/heads/main",
        "event_name": "push",
    }
    payload = github.extra_audit_payload(claims)
    assert payload["repository"] == "acme/sandbox"
    assert payload["repository_id"] == "123456"
    assert payload["workflow"] == "deploy"
    assert payload["ref"] == "refs/heads/main"
    assert payload["event_name"] == "push"


def test_extra_audit_payload_omits_absent_claims() -> None:
    payload = github.extra_audit_payload({"sub": "u"})
    assert payload == {}


def test_extra_audit_payload_skips_non_string_non_int() -> None:
    payload = github.extra_audit_payload({"repository": ["acme", "x"]})
    assert "repository" not in payload
