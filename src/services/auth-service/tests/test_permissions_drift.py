"""Permission-registry drift guards (#867).

These tests pin the two-way contract that broke deploy-smoke once: the
API Gateway refuses to become ready unless every permission its route
table declares is present in the registry auth-service seeds, and the
two delivery paths for that registry (the bundled aggregate embedded in
the ``custos_auth`` wheel vs. the per-service ``permissions.yaml`` files
baked into the deployed image) must never diverge.

Guards:

* The bundled aggregate equals the union of every component's
  per-service ``permissions.yaml`` (same names, descriptions, and
  declarers). This is what keeps the dev/test fallback in lockstep with
  the ``CUSTOS_AUTH_PERMISSIONS_PATHS`` delivery path.
* Every built-in role references only declared permissions.
* The bundled aggregate is a superset of the permission set the API
  Gateway route table requires (``GATEWAY_REQUIRED_PERMISSIONS``). This
  is the exact cross-check the gateway runs at startup; pinning it here
  catches taxonomy drift before it reaches a cluster.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from custos_auth.permission_registry import (
    DECLARED_BY_SEPARATOR,
    load_permission_registry,
    validate_roles_reference_only_declared,
)
from custos_auth.roles import BUILTIN_ROLES

#: Components that ship a per-service ``permissions.yaml`` whose union is
#: aggregated by auth-service at deploy time. Kept in sync with the
#: ``COPY`` lines in ``src/services/auth-service/Dockerfile`` and the
#: ``CUSTOS_AUTH_PERMISSIONS_PATHS`` default in the auth-service chart.
_PER_SERVICE_OWNERS = (
    "auth-service",
    "catalog-service",
    "workflow-service",
    "trigger-service",
    "connector-service",
    "observability-audit-service",
)

#: The distinct permission names the API Gateway route table requires
#: (``custos_gateway.routes.registry.registry_required_permissions``).
#: The gateway lives in a separate package that is not installed in the
#: auth-service test environment, so the set is mirrored here; the
#: gateway's own ``test_route_registry`` pins the source of truth, and
#: this guard ensures the auth-service registry remains a superset.
GATEWAY_REQUIRED_PERMISSIONS = frozenset(
    {
        "admin:connector",
        "admin:role-binding",
        "admin:service-account",
        "audit:read",
        "catalog:activity-types:read",
        "catalog:activity-types:write",
        "catalog:connector-types:read",
        "catalog:connector-types:write",
        "catalog:templates:read",
        "catalog:templates:write",
        "catalog:workflows:read",
        "catalog:workflows:write",
        "connector:read",
        "logs:read",
        "metrics:read",
        "run:cancel",
        "run:read",
        "trigger:subscriptions:delete",
        "trigger:subscriptions:fire",
        "trigger:subscriptions:read",
        "trigger:subscriptions:write",
        "workflow:execute",
    }
)


def _repo_services_root() -> Path | None:
    """Walk up from this test file to the repo's ``src/services`` dir.

    Returns ``None`` when the repo layout is not present (e.g. an
    installed-wheel-only test environment), in which case the
    per-service drift guard is skipped.
    """
    for parent in Path(__file__).resolve().parents:
        candidate = parent / "src" / "services"
        if candidate.is_dir():
            return candidate
    return None


def test_bundled_aggregate_equals_union_of_per_service_files() -> None:
    services_root = _repo_services_root()
    if services_root is None:
        pytest.skip("repo src/services layout not available")

    paths: list[str] = []
    for owner in _PER_SERVICE_OWNERS:
        path = services_root / owner / "permissions.yaml"
        assert path.is_file(), f"missing per-service permissions.yaml for {owner}: {path}"
        paths.append(str(path))

    union = load_permission_registry(tuple(paths))
    bundled = load_permission_registry(())

    assert set(union.keys()) == set(bundled.keys()), (
        "bundled aggregate and the union of per-service permissions.yaml "
        "files declare different permission names"
    )
    for name, perm in bundled.items():
        assert perm.description == union[name].description, (
            f"description for {name!r} differs between the bundled aggregate "
            "and the per-service files"
        )
        assert set(perm.declared_by.split(DECLARED_BY_SEPARATOR)) == set(
            union[name].declared_by.split(DECLARED_BY_SEPARATOR)
        ), f"declaredBy for {name!r} differs between the two delivery paths"


def test_builtin_roles_reference_only_declared_permissions() -> None:
    bundled = load_permission_registry(())
    roles_spl = [role.to_spl() for role in BUILTIN_ROLES]
    # Raises UnknownPermissionError if any role references an undeclared
    # name; a clean return is the assertion.
    validate_roles_reference_only_declared(roles_spl, bundled.keys())


def test_bundled_registry_covers_gateway_required_permissions() -> None:
    bundled = load_permission_registry(())
    missing = sorted(GATEWAY_REQUIRED_PERMISSIONS - set(bundled.keys()))
    assert not missing, (
        "auth-service registry is missing permissions the API Gateway route "
        f"table requires: {missing}"
    )
