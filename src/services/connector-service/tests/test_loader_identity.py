"""Unit tests for :mod:`custos_connector.loader.identity` (CONN-IMPL-008).

Coverage:

* Each built-in ``authenticationType`` maps to the expected category.
* ``x-<vendor>`` tokens are accepted only when a registration-time
  override is supplied.
* Unknown non-vendor values are rejected with a stable code.
* ``BUILTIN_IDENTITY_CATEGORIES`` is read-only (mutation raises).
"""

from __future__ import annotations

import pytest

from custos_connector.loader import (
    BUILTIN_IDENTITY_CATEGORIES,
    IdentityCategory,
    LoaderError,
    LoaderErrorCode,
    derive_identity_category,
)


@pytest.mark.parametrize(
    ("authentication_type", "expected"),
    [
        ("azure-key-vault", IdentityCategory.KMS),
        ("amazon-kms", IdentityCategory.KMS),
        ("azure-managed-identity", IdentityCategory.WORKLOAD),
        ("oidc", IdentityCategory.FEDERATED),
    ],
)
def test_built_in_authentication_types_map_to_expected_category(
    authentication_type: str,
    expected: IdentityCategory,
) -> None:
    assert derive_identity_category(authentication_type) is expected


def test_vendor_override_resolves_x_token() -> None:
    overrides = {"x-acme-vault": IdentityCategory.KMS}
    assert (
        derive_identity_category("x-acme-vault", vendor_overrides=overrides) is IdentityCategory.KMS
    )


def test_vendor_token_without_override_rejected_with_vendor_code() -> None:
    with pytest.raises(LoaderError) as exc_info:
        derive_identity_category("x-acme-vault")
    assert exc_info.value.code is LoaderErrorCode.UNKNOWN_VENDOR_AUTH_TYPE
    assert "x-acme-vault" in exc_info.value.detail


def test_unknown_non_vendor_value_rejected_with_generic_code() -> None:
    with pytest.raises(LoaderError) as exc_info:
        derive_identity_category("not-a-real-auth-type")
    assert exc_info.value.code is LoaderErrorCode.UNKNOWN_AUTHENTICATION_TYPE
    assert "not-a-real-auth-type" in exc_info.value.detail


def test_overrides_cannot_shadow_built_in_tokens() -> None:
    # A caller passing an override for a built-in MUST NOT silently
    # change platform semantics — the built-in wins.
    overrides = {"oidc": IdentityCategory.KMS}
    assert (
        derive_identity_category("oidc", vendor_overrides=overrides) is IdentityCategory.FEDERATED
    )


def test_builtin_identity_categories_is_read_only() -> None:
    # MappingProxyType makes the registry read-only so a stray test
    # cannot pollute the platform mapping for sibling tests.
    with pytest.raises(TypeError):
        BUILTIN_IDENTITY_CATEGORIES["new-type"] = IdentityCategory.KMS  # type: ignore[index]
