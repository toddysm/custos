"""Permission-registry loader, upserter, and built-in-role validator.

Phase D / AS-IMPL-008. The loader reads one or more ``permissions.yaml``
files (the bundled platform-M1 file plus any extras configured through
:data:`custos_auth.settings.ENV_PERMISSIONS_PATHS`), upserts each
declared permission into the SPL ``Permission`` table via
``AuthStoreProvider.upsert_permission``, and refuses to start the
service if any built-in role references a name that no path declared.

YAML schema
-----------

.. code-block:: yaml

    permissions:
      - name: workflow:read
        description: |
          List workflows and read workflow definitions in a workspace.
        declaredBy: workflow-service
      - name: audit:read
        description: |
          Read audit-trail events.
        declaredBy: auth-service|observability-audit-service

* ``name``        — canonical permission identifier (required, non-empty).
* ``description`` — operator-facing human description (required,
                    non-empty).
* ``declaredBy``  — pipe-delimited list of owning components (required,
                    non-empty). When the same name appears in multiple
                    files, the loader merges the declarers with ``|``
                    and keeps the description of the **first**
                    occurrence (later occurrences must declare a
                    matching description, otherwise the loader raises
                    :class:`PermissionConflictError`).

Multi-declarer support is loader-side metadata only — the SPL
``Permission`` dataclass carries just ``(name, description)`` so the
``declared_by`` field is exposed through this module's
:class:`DeclaredPermission` view but does **not** round-trip through
``AuthStoreProvider``.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

import yaml
from custos_spl.interfaces.auth_store import Permission

if TYPE_CHECKING:
    from custos_spl import AuthStoreProvider
    from custos_spl.interfaces.auth_store import Role

_LOGGER = logging.getLogger("custos_auth.permission_registry")

#: Package-resource handle for the bundled platform-M1 registry. Used
#: when ``CUSTOS_AUTH_PERMISSIONS_PATHS`` is empty so the service can
#: bootstrap a complete registry without operator configuration.
_BUNDLED_REGISTRY_PACKAGE: Final[str] = "custos_auth._data"
_BUNDLED_REGISTRY_RESOURCE: Final[str] = "permissions.yaml"

#: Multi-declarer separator. Pipe rather than comma because the SPL
#: store uses commas in some other text columns and ops mode-switching
#: was easier when this carried a distinct sigil.
DECLARED_BY_SEPARATOR: Final[str] = "|"


@dataclass(frozen=True, slots=True)
class DeclaredPermission:
    """Loader-side view of a declared permission row.

    Carries the ``declared_by`` attribution that the SPL ``Permission``
    dataclass does not persist. The
    :func:`seed_permissions_and_validate_roles` entrypoint downcasts
    this to a SPL :class:`Permission` before upsert.
    """

    name: str
    description: str
    declared_by: str

    def to_spl(self) -> Permission:
        """Project to the SPL ``Permission`` dataclass for upsert."""
        return Permission(name=self.name, description=self.description)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class PermissionRegistryError(RuntimeError):
    """Base class for every loader/validator failure mode.

    A startup failure surfaces this through the lifespan re-raise so
    Kubernetes turns the misconfiguration into CrashLoopBackOff.
    """


class PermissionFileError(PermissionRegistryError):
    """The registry file is missing, unreadable, or malformed."""


class PermissionConflictError(PermissionRegistryError):
    """The same permission name was declared with mismatched descriptions."""


class UnknownPermissionError(PermissionRegistryError):
    """Built-in roles reference names that no registry file declared.

    Carries the full set of (role_id, missing_name) pairs so the
    startup diagnostic surfaces every undeclared reference at once
    rather than failing-fast on the first one.
    """

    def __init__(self, missing: Sequence[tuple[str, str]]) -> None:
        self.missing: tuple[tuple[str, str], ...] = tuple(missing)
        lines = ["The following built-in roles reference undeclared permissions:"]
        for role_id, name in sorted(self.missing):
            lines.append(f"  - role={role_id!r} references permission={name!r}")
        lines.append(
            "Add the missing permission(s) to a registry file or fix the "
            "built-in-role table in custos_auth.roles."
        )
        super().__init__("\n".join(lines))


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------


def _coerce_str(raw: Any, *, field: str, path: str) -> str:
    """Extract a non-empty string field from a YAML row."""
    if not isinstance(raw, str):
        raise PermissionFileError(
            f"{path}: permission field {field!r} must be a string, got {type(raw).__name__}"
        )
    value = raw.strip()
    if not value:
        raise PermissionFileError(f"{path}: permission field {field!r} must be non-empty")
    return value


def _parse_permission_row(row: Any, *, path: str) -> DeclaredPermission:
    """Validate one entry under ``permissions:`` in a registry file."""
    if not isinstance(row, dict):
        raise PermissionFileError(
            f"{path}: every entry under 'permissions:' must be a mapping, got {type(row).__name__}"
        )
    name = _coerce_str(row.get("name"), field="name", path=path)
    description = _coerce_str(row.get("description"), field="description", path=path)
    declared_by = _coerce_str(row.get("declaredBy"), field="declaredBy", path=path)
    # Strip whitespace around each declarer segment to be robust to
    # editor reflow without losing the canonical ``|`` separator.
    declarers = [part.strip() for part in declared_by.split(DECLARED_BY_SEPARATOR) if part.strip()]
    if not declarers:
        raise PermissionFileError(
            f"{path}: permission {name!r} 'declaredBy' must contain at least one component"
        )
    canonical_declared_by = DECLARED_BY_SEPARATOR.join(declarers)
    return DeclaredPermission(
        name=name,
        description=description.rstrip(),
        declared_by=canonical_declared_by,
    )


def _load_one_file(path: str) -> list[DeclaredPermission]:
    """Parse a single registry YAML and return the row list."""
    try:
        raw = Path(path).read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise PermissionFileError(f"permissions file not found: {path}") from exc
    except OSError as exc:
        raise PermissionFileError(f"could not read permissions file {path}: {exc}") from exc

    try:
        doc = yaml.safe_load(raw)
    except yaml.YAMLError as exc:
        raise PermissionFileError(f"{path}: invalid YAML: {exc}") from exc

    if doc is None:
        raise PermissionFileError(f"{path}: file is empty")
    if not isinstance(doc, dict):
        raise PermissionFileError(
            f"{path}: top-level YAML node must be a mapping with a 'permissions' key"
        )
    rows = doc.get("permissions")
    if rows is None:
        raise PermissionFileError(f"{path}: top-level 'permissions' key is missing")
    if not isinstance(rows, list):
        raise PermissionFileError(
            f"{path}: 'permissions' must be a list, got {type(rows).__name__}"
        )
    return [_parse_permission_row(row, path=path) for row in rows]


def _read_bundled_registry() -> str:
    """Return the YAML text for the bundled platform-M1 registry."""
    resource = resources.files(_BUNDLED_REGISTRY_PACKAGE).joinpath(
        _BUNDLED_REGISTRY_RESOURCE,
    )
    return resource.read_text(encoding="utf-8")


def _load_bundled() -> list[DeclaredPermission]:
    """Parse the bundled platform-M1 registry shipped with the package."""
    raw = _read_bundled_registry()
    try:
        doc = yaml.safe_load(raw)
    except yaml.YAMLError as exc:  # pragma: no cover - bundled file is checked in
        raise PermissionFileError(f"bundled registry is malformed YAML: {exc}") from exc
    if not isinstance(doc, dict):  # pragma: no cover - bundled file is checked in
        raise PermissionFileError("bundled registry: top-level node must be a mapping")
    rows = doc.get("permissions")
    if not isinstance(rows, list):  # pragma: no cover
        raise PermissionFileError("bundled registry: 'permissions' must be a list")
    return [
        _parse_permission_row(row, path=f"<bundled:{_BUNDLED_REGISTRY_RESOURCE}>") for row in rows
    ]


def load_permission_registry(
    paths: Sequence[str],
) -> dict[str, DeclaredPermission]:
    """Load + merge permission rows from every configured path.

    When ``paths`` is empty the bundled platform-M1 registry is used.
    Otherwise the bundled registry is **not** auto-included — operators
    that point at custom files take ownership of the entire registry
    surface, which keeps the precedence model explicit.

    Multi-file merge rules:

    * Same ``name`` + same ``description`` ⇒ the ``declared_by``
      attributions are concatenated with ``|`` (de-duplicated).
    * Same ``name`` + different ``description`` ⇒
      :class:`PermissionConflictError` (the loader will not silently
      pick one description over another).
    """
    if not paths:
        rows = _load_bundled()
    else:
        rows = []
        for path in paths:
            rows.extend(_load_one_file(path))

    merged: dict[str, DeclaredPermission] = {}
    for row in rows:
        existing = merged.get(row.name)
        if existing is None:
            merged[row.name] = row
            continue
        if existing.description != row.description:
            raise PermissionConflictError(
                f"permission {row.name!r} declared with conflicting descriptions: "
                f"{existing.description!r} vs {row.description!r}"
            )
        # Merge declarers, preserving order and de-duplicating.
        seen: list[str] = []
        for part in existing.declared_by.split(DECLARED_BY_SEPARATOR) + row.declared_by.split(
            DECLARED_BY_SEPARATOR
        ):
            part = part.strip()
            if part and part not in seen:
                seen.append(part)
        merged[row.name] = DeclaredPermission(
            name=row.name,
            description=row.description,
            declared_by=DECLARED_BY_SEPARATOR.join(seen),
        )
    return merged


# ---------------------------------------------------------------------------
# Validation + upsert
# ---------------------------------------------------------------------------


def validate_roles_reference_only_declared(
    roles: Iterable[Role],
    declared_names: Iterable[str],
) -> None:
    """Raise :class:`UnknownPermissionError` for any unknown reference.

    Collects **every** (role, missing_name) pair before raising so a
    single startup attempt surfaces the full diff to the operator.
    """
    known = set(declared_names)
    missing: list[tuple[str, str]] = []
    for role in roles:
        for perm in role.permission_names:
            if perm not in known:
                missing.append((str(role.role_id), perm))
    if missing:
        raise UnknownPermissionError(missing)


async def upsert_declared_permissions(
    auth_store: AuthStoreProvider,
    declared: Iterable[DeclaredPermission],
) -> None:
    """Upsert every declared permission into the SPL ``Permission`` table.

    Idempotent: ``upsert_permission`` is keyed on ``name``, so calling
    this on every startup just refreshes descriptions and produces no
    duplicate rows.
    """
    for perm in declared:
        await auth_store.upsert_permission(perm.to_spl())


async def seed_permissions_and_validate_roles(
    auth_store: AuthStoreProvider,
    *,
    paths: Sequence[str],
    roles: Iterable[Role],
) -> dict[str, DeclaredPermission]:
    """End-to-end startup hook for the permission registry.

    1. Load + merge every configured registry file (or the bundled
       fallback when ``paths`` is empty).
    2. Verify the built-in roles reference only declared names. If
       not, raise :class:`UnknownPermissionError` **before** any
       upsert touches the store so the store stays in a coherent
       state across crash-restart loops.
    3. Upsert every declared permission.

    Returns the merged registry so subsequent startup steps (the
    Phase D ``GET /v1/permissions`` route, the role-table seeder)
    can read attribution without re-reading the YAML.
    """
    declared = load_permission_registry(paths)
    materialised_roles = list(roles)
    validate_roles_reference_only_declared(materialised_roles, declared.keys())
    await upsert_declared_permissions(auth_store, declared.values())
    _LOGGER.info(
        "permission registry seeded permissions=%d roles=%d sources=%s",
        len(declared),
        len(materialised_roles),
        ",".join(paths) if paths else "<bundled>",
    )
    return declared


__all__ = [
    "DECLARED_BY_SEPARATOR",
    "DeclaredPermission",
    "PermissionConflictError",
    "PermissionFileError",
    "PermissionRegistryError",
    "UnknownPermissionError",
    "load_permission_registry",
    "seed_permissions_and_validate_roles",
    "upsert_declared_permissions",
    "validate_roles_reference_only_declared",
]
