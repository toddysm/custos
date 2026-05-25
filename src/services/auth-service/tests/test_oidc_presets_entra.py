"""Tests for ``custos_auth.oidc.presets.entra`` (AS-IMPL-022)."""

from __future__ import annotations

import pytest

from custos_auth.oidc.presets import entra


def test_name_constant() -> None:
    assert entra.name == "entra"


def test_defaults_omit_issuer_url() -> None:
    # The Entra preset intentionally does NOT default issuer_url — an
    # implicit ``common`` issuer would silently accept any tenant.
    defaults = entra.defaults()
    assert "issuer_url" not in defaults
    assert defaults["jwks_uri"].endswith("/discovery/v2.0/keys")
    assert defaults["algorithms"] == ("RS256",)
    assert defaults["subject_claim"] == "oid"
    assert defaults["group_claim"] == "groups"


def test_extract_subject_prefers_oid_over_sub() -> None:
    claims = {"oid": "stable-guid", "sub": "pairwise"}
    assert entra.extract_subject(claims) == "stable-guid"


def test_extract_subject_falls_back_to_sub() -> None:
    claims = {"sub": "pairwise"}
    assert entra.extract_subject(claims) == "pairwise"


def test_extract_subject_raises_when_both_missing() -> None:
    with pytest.raises(ValueError, match="'oid' and 'sub'"):
        entra.extract_subject({})


def test_extra_audit_payload_surfaces_tenant_and_username() -> None:
    claims = {
        "tid": "tenant-guid",
        "preferred_username": "alice@example.com",
        "appid": "app-client-id",
    }
    payload = entra.extra_audit_payload(claims)
    assert payload["tid"] == "tenant-guid"
    assert payload["preferred_username"] == "alice@example.com"
    assert payload["appid"] == "app-client-id"


def test_extra_audit_payload_omits_groups() -> None:
    # Group memberships go through structured group_bindings, not audit.
    claims = {"tid": "t", "groups": ["g1", "g2"]}
    payload = entra.extra_audit_payload(claims)
    assert "groups" not in payload
