"""Shared helpers for the read-back Query API route modules.

The log / metrics / audit routes share a small amount of cross-cutting logic:
the path-vs-call-context workspace guard, the ``404`` shape for a run that
resolves to a different workspace, the typed path parameters, and timezone-aware
ISO-8601 parsing for the ``from`` / ``to`` filter window. Centralising them here
keeps the security check (and the datetime normalisation it depends on) in one
place so the three route modules cannot drift apart.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Annotated

from fastapi import HTTPException, Path

from custos_obs.middleware import CallContextError

if TYPE_CHECKING:
    from custos_obs.middleware import CallContext

#: Path parameter for the owning workspace id (non-empty).
WorkspacePath = Annotated[str, Path(min_length=1, description="Owning workspace id.")]
#: Path parameter for the run id (non-empty).
RunPath = Annotated[str, Path(min_length=1, description="Run id.")]

__all__ = [
    "RunPath",
    "WorkspacePath",
    "ensure_workspace",
    "parse_iso_datetime",
    "require_iso_datetime",
    "run_not_found",
]


def ensure_workspace(ctx: CallContext, workspace_id: str) -> None:
    """Reject a request whose path workspace differs from the call context's.

    The path ``{workspace_id}`` is authoritative for the query while the call
    context governs RBAC; if they disagree the caller is reaching across
    workspace boundaries. Defense-in-depth — with the dev-shim's unsigned header
    this is the only check stopping a caller from naming an arbitrary workspace.
    """
    if ctx.workspace_id != workspace_id:
        raise CallContextError(
            403,
            "workspace_mismatch",
            "call context workspace does not match the request path workspace",
        )


def run_not_found(run_id: str) -> HTTPException:
    """A run that resolves to a different workspace surfaces as ``404``.

    Never ``403`` — disclosing cross-workspace existence would leak tenant
    information.
    """
    return HTTPException(status_code=404, detail=f"run not found: {run_id}")


def _parse(value: str) -> datetime:
    """Parse one ISO-8601 string into a timezone-aware ``datetime``.

    A trailing ``Z`` (UTC designator) is normalised to ``+00:00`` since
    :meth:`datetime.fromisoformat` only accepts it on newer interpreters. A
    *timezone-naive* value is rejected with ``400`` rather than silently assumed
    to be local time: the backend adapters convert to epoch seconds, so an
    ambiguous offset would produce wrong filter windows. A malformed value is
    likewise a client error (``400``).
    """
    normalized = f"{value[:-1]}+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"invalid datetime: {value!r}") from exc
    if parsed.tzinfo is None:
        raise HTTPException(
            status_code=400,
            detail=f"datetime must include a timezone offset: {value!r}",
        )
    return parsed


def parse_iso_datetime(value: str | None) -> datetime | None:
    """Parse an optional ISO-8601 query param (``None`` passes through)."""
    if value is None:
        return None
    return _parse(value)


def require_iso_datetime(value: str) -> datetime:
    """Parse a required ISO-8601 query param into a ``datetime``."""
    return _parse(value)
