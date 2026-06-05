"""RFC 7807 Problem Details handler for the Query API (OBS-IMPL-013).

The read-back routes raise the typed :class:`custos_obs.errors.ObsError`
subclasses (e.g. :class:`LogQueryUnavailable`) when an SPL provider is ``noop``
or its backend is unreachable. :func:`obs_error_handler` renders any such error
as an ``application/problem+json`` body with the carried HTTP status, so every
route surfaces failures through one consistent envelope. ``create_app``
registers it once for the whole app.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from starlette.responses import JSONResponse

from custos_obs.errors import PROBLEM_CONTENT_TYPE, ObsError

if TYPE_CHECKING:
    from starlette.requests import Request


async def obs_error_handler(_request: Request, exc: Exception) -> JSONResponse:
    """Render an :class:`ObsError` as an RFC 7807 Problem Details response.

    The signature accepts ``Exception`` so it matches
    :meth:`FastAPI.add_exception_handler` without an explicit ``# type: ignore``,
    then narrows back for attribute access.
    """
    assert isinstance(exc, ObsError)
    return JSONResponse(
        status_code=exc.status,
        content=exc.to_dict(),
        media_type=PROBLEM_CONTENT_TYPE,
    )


__all__ = ["obs_error_handler"]
