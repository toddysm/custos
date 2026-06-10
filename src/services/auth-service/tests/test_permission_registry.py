"""Tests for the Phase D permission-registry loader (AS-IMPL-008).

Covers:

* YAML parsing happy path against the bundled platform-M1 registry.
* Multi-file merge: same name + same description merges declarers.
* Multi-file merge: same name + different description ⇒
  :class:`PermissionConflictError`.
* Missing file ⇒ :class:`PermissionFileError`.
* Malformed YAML / missing keys ⇒ :class:`PermissionFileError`.
* :func:`validate_roles_reference_only_declared` aggregates every
  undeclared name across every role rather than failing fast.
* :func:`seed_permissions_and_validate_roles` upserts every row via
  the auth_store and refuses to start when a role references an
  undeclared permission (no upsert side-effect when validation fails).
"""

from __future__ import annotations

from pathlib import Path

import pytest
from custos_spl.ids import RoleId
from custos_spl.interfaces.auth_store import Role

from custos_auth.permission_registry import (
    DECLARED_BY_SEPARATOR,
    DeclaredPermission,
    PermissionConflictError,
    PermissionFileError,
    UnknownPermissionError,
    load_permission_registry,
    seed_permissions_and_validate_roles,
    validate_roles_reference_only_declared,
)
from tests._fakes import FakeAuthAdapter


def _write_yaml(tmp_path: Path, name: str, body: str) -> str:
    p = tmp_path / name
    p.write_text(body, encoding="utf-8")
    return str(p)


# ---------------------------------------------------------------------------
# Bundled registry
# ---------------------------------------------------------------------------


def test_load_bundled_registry_returns_full_platform_m1_set() -> None:
    declared = load_permission_registry(())
    # The bundled file declares auth-service's own admin perms plus the
    # fine-grained cross-component perms referenced by every v1 built-in
    # role and required by the API Gateway route table.
    assert "admin:role-binding" in declared
    assert "admin:service-account" in declared
    assert "admin:workspace" in declared
    assert "audit:read" in declared
    assert "catalog:workflows:read" in declared
    assert "workflow:execute" in declared
    # Multi-declarer attribution is preserved.
    assert "auth-service" in declared["audit:read"].declared_by.split(DECLARED_BY_SEPARATOR)
    assert "observability-audit-service" in declared["audit:read"].declared_by.split(
        DECLARED_BY_SEPARATOR
    )


# ---------------------------------------------------------------------------
# YAML parsing happy path
# ---------------------------------------------------------------------------


def test_load_one_file_happy_path(tmp_path: Path) -> None:
    path = _write_yaml(
        tmp_path,
        "perms.yaml",
        """
permissions:
  - name: workflow:read
    description: Read workflows.
    declaredBy: workflow-service
  - name: workflow:execute
    description: Execute workflows.
    declaredBy: workflow-service
""",
    )
    declared = load_permission_registry((path,))
    assert set(declared.keys()) == {"workflow:read", "workflow:execute"}
    assert declared["workflow:read"].declared_by == "workflow-service"


def test_load_one_file_strips_description_whitespace(tmp_path: Path) -> None:
    path = _write_yaml(
        tmp_path,
        "perms.yaml",
        """
permissions:
  - name: workflow:read
    description: |
      Read workflows.
    declaredBy: workflow-service
""",
    )
    declared = load_permission_registry((path,))
    assert declared["workflow:read"].description == "Read workflows."


# ---------------------------------------------------------------------------
# Multi-file merge
# ---------------------------------------------------------------------------


def test_multi_file_same_description_merges_declarers(tmp_path: Path) -> None:
    a = _write_yaml(
        tmp_path,
        "a.yaml",
        """
permissions:
  - name: audit:read
    description: Read audit events.
    declaredBy: auth-service
""",
    )
    b = _write_yaml(
        tmp_path,
        "b.yaml",
        """
permissions:
  - name: audit:read
    description: Read audit events.
    declaredBy: observability-audit-service
""",
    )
    declared = load_permission_registry((a, b))
    parts = declared["audit:read"].declared_by.split(DECLARED_BY_SEPARATOR)
    assert "auth-service" in parts
    assert "observability-audit-service" in parts
    assert len(parts) == 2


def test_multi_file_same_description_deduplicates_declarers(tmp_path: Path) -> None:
    a = _write_yaml(
        tmp_path,
        "a.yaml",
        """
permissions:
  - name: audit:read
    description: Read audit events.
    declaredBy: auth-service
""",
    )
    b = _write_yaml(
        tmp_path,
        "b.yaml",
        """
permissions:
  - name: audit:read
    description: Read audit events.
    declaredBy: auth-service
""",
    )
    declared = load_permission_registry((a, b))
    parts = declared["audit:read"].declared_by.split(DECLARED_BY_SEPARATOR)
    assert parts == ["auth-service"]


def test_multi_file_different_description_raises(tmp_path: Path) -> None:
    a = _write_yaml(
        tmp_path,
        "a.yaml",
        """
permissions:
  - name: audit:read
    description: Read audit events.
    declaredBy: auth-service
""",
    )
    b = _write_yaml(
        tmp_path,
        "b.yaml",
        """
permissions:
  - name: audit:read
    description: A different description.
    declaredBy: observability-audit-service
""",
    )
    with pytest.raises(PermissionConflictError):
        load_permission_registry((a, b))


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------


def test_missing_file_raises_permission_file_error(tmp_path: Path) -> None:
    with pytest.raises(PermissionFileError, match="not found"):
        load_permission_registry((str(tmp_path / "does-not-exist.yaml"),))


def test_empty_file_raises(tmp_path: Path) -> None:
    path = _write_yaml(tmp_path, "empty.yaml", "")
    with pytest.raises(PermissionFileError, match="empty"):
        load_permission_registry((path,))


def test_non_mapping_top_level_raises(tmp_path: Path) -> None:
    path = _write_yaml(tmp_path, "bad.yaml", "- one\n- two\n")
    with pytest.raises(PermissionFileError, match="mapping"):
        load_permission_registry((path,))


def test_missing_permissions_key_raises(tmp_path: Path) -> None:
    path = _write_yaml(tmp_path, "bad.yaml", "other_key: value\n")
    with pytest.raises(PermissionFileError, match="'permissions' key is missing"):
        load_permission_registry((path,))


def test_permissions_not_a_list_raises(tmp_path: Path) -> None:
    path = _write_yaml(tmp_path, "bad.yaml", "permissions: not-a-list\n")
    with pytest.raises(PermissionFileError, match="must be a list"):
        load_permission_registry((path,))


def test_row_missing_name_raises(tmp_path: Path) -> None:
    path = _write_yaml(
        tmp_path,
        "bad.yaml",
        """
permissions:
  - description: A description.
    declaredBy: x
""",
    )
    with pytest.raises(PermissionFileError, match="'name'"):
        load_permission_registry((path,))


def test_row_empty_declared_by_raises(tmp_path: Path) -> None:
    path = _write_yaml(
        tmp_path,
        "bad.yaml",
        """
permissions:
  - name: workflow:read
    description: x
    declaredBy: "  "
""",
    )
    with pytest.raises(PermissionFileError, match="declaredBy"):
        load_permission_registry((path,))


def test_invalid_yaml_raises(tmp_path: Path) -> None:
    path = _write_yaml(tmp_path, "bad.yaml", "permissions: [unterminated\n")
    with pytest.raises(PermissionFileError, match="invalid YAML"):
        load_permission_registry((path,))


# ---------------------------------------------------------------------------
# validate_roles_reference_only_declared
# ---------------------------------------------------------------------------


def _role(role_id: str, perms: tuple[str, ...]) -> Role:
    return Role(
        role_id=RoleId(role_id),
        name=role_id,
        description="",
        permission_names=perms,
    )


def test_validate_roles_passes_when_all_declared() -> None:
    roles = [_role("r1", ("a", "b")), _role("r2", ("a",))]
    # Should not raise.
    validate_roles_reference_only_declared(roles, ["a", "b"])


def test_validate_roles_aggregates_every_undeclared_reference() -> None:
    roles = [_role("r1", ("a", "x", "y")), _role("r2", ("a", "z"))]
    with pytest.raises(UnknownPermissionError) as ei:
        validate_roles_reference_only_declared(roles, ["a"])
    # Every missing reference, not just the first, is surfaced.
    assert {(rid, name) for rid, name in ei.value.missing} == {
        ("r1", "x"),
        ("r1", "y"),
        ("r2", "z"),
    }


# ---------------------------------------------------------------------------
# seed_permissions_and_validate_roles
# ---------------------------------------------------------------------------


async def test_seed_upserts_every_row_when_roles_validate(tmp_path: Path) -> None:
    path = _write_yaml(
        tmp_path,
        "perms.yaml",
        """
permissions:
  - name: a
    description: a-desc
    declaredBy: x
  - name: b
    description: b-desc
    declaredBy: y
""",
    )
    store = FakeAuthAdapter()
    roles = [_role("r1", ("a", "b"))]
    declared = await seed_permissions_and_validate_roles(
        store,  # type: ignore[arg-type]
        paths=(path,),
        roles=roles,
    )
    assert set(declared.keys()) == {"a", "b"}
    assert set(store.permissions.keys()) == {"a", "b"}


async def test_seed_refuses_when_role_references_undeclared(
    tmp_path: Path,
) -> None:
    path = _write_yaml(
        tmp_path,
        "perms.yaml",
        """
permissions:
  - name: a
    description: a-desc
    declaredBy: x
""",
    )
    store = FakeAuthAdapter()
    roles = [_role("r1", ("a", "missing"))]
    with pytest.raises(UnknownPermissionError):
        await seed_permissions_and_validate_roles(
            store,  # type: ignore[arg-type]
            paths=(path,),
            roles=roles,
        )
    # Validation happens *before* upsert so the store stays untouched.
    assert store.permissions == {}


# ---------------------------------------------------------------------------
# DeclaredPermission projection
# ---------------------------------------------------------------------------


def test_declared_permission_to_spl_drops_declared_by() -> None:
    perm = DeclaredPermission(name="x", description="d", declared_by="a|b")
    spl = perm.to_spl()
    assert spl.name == "x"
    assert spl.description == "d"
    # The SPL Permission dataclass does not carry declared_by.
    assert not hasattr(spl, "declared_by")
