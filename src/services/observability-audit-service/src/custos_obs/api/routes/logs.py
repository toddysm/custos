"""Log read-back routes (Concern B, log side) — OBS-IMPL-013.

Two routes over the SPL :class:`LogQueryProvider`:

* ``GET /v1/workspaces/{ws}/runs/{runId}/logs/tail`` — a live Server-Sent Events
  stream of a run's logs (``text/event-stream``). Each new record is emitted as a
  ``data: {json}\\n\\n`` frame.
* ``GET /v1/workspaces/{ws}/runs/{runId}/logs`` — a paged historical query with
  the ``stepId`` / ``from`` / ``to`` / ``severity`` / ``cursor`` filters.

When the configured provider is ``noop`` (or the backend is unreachable) the SPL
layer raises :class:`QueryUnsupported` / :class:`BackendUnavailable`; both map to
``503 LogQueryUnavailable`` (RFC 7807) carrying the configured
``CUSTOS_LOGS_EXTERNAL_URL`` pointer so the UI can link out to the external log
system. A run that resolves to a different workspace surfaces as ``404`` (never
``403`` — disclosing cross-workspace existence would leak tenant information).

SSE resume (design TODO-007): the inbound ``Last-Event-ID`` header (or a
``cursor`` query param) is honoured and threaded into ``tail_run_logs`` as the
resume cursor. The outbound ``id:`` line encoding is still open in TODO-007 —
the SPL ``LogRecord`` carries no per-record cursor, so this route does not yet
fabricate one (doing so against an opaque, adapter-specific cursor scheme would
be wrong). Inbound resume works as soon as an adapter accepts its own cursors.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import TYPE_CHECKING, Annotated

from custos_spl import Cursor, RunId, StepId, WorkspaceId
from custos_spl.errors import BackendUnavailable, QueryUnsupported, WorkspaceMismatch
from custos_spl.interfaces.log_query import LogFilter, LogQueryProvider, Severity
from fastapi import APIRouter, Depends, Header, HTTPException, Path, Query
from starlette.responses import StreamingResponse

from custos_obs.api.dependencies import get_log_query_provider, get_settings
from custos_obs.api.models import LogPageModel, LogRecordModel
from custos_obs.errors import LogQueryUnavailable
from custos_obs.middleware import CallContext, CallContextError, require_permission
from custos_obs.settings import Settings

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from custos_spl.interfaces.log_query import LogRecord

__all__ = ["router"]

router = APIRouter(prefix="/v1/workspaces/{workspace_id}/runs/{run_id}", tags=["logs"])

#: Permission scope the call-context middleware enforces for log read-back.
PERM_READ = "logs:read"

_WorkspacePath = Annotated[str, Path(min_length=1, description="Owning workspace id.")]
_RunPath = Annotated[str, Path(min_length=1, description="Run id.")]

LogProviderDep = Annotated[LogQueryProvider, Depends(get_log_query_provider)]
SettingsDep = Annotated[Settings, Depends(get_settings)]
CtxDep = Annotated[CallContext, Depends(require_permission(PERM_READ))]


def _ensure_workspace(ctx: CallContext, workspace_id: str) -> None:
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


def _log_unavailable(settings: Settings) -> LogQueryUnavailable:
    """Build the ``503`` Problem Details, carrying the external-URL pointer."""
    extensions = (
        {"externalUrl": settings.logs_external_url}
        if settings.logs_external_url is not None
        else None
    )
    return LogQueryUnavailable(
        "the log query backend is not available; use the external log system",
        extensions=extensions,
    )


def _run_not_found(run_id: str) -> HTTPException:
    """A run that resolves to a different workspace surfaces as ``404``."""
    return HTTPException(status_code=404, detail=f"run not found: {run_id}")


def _data_frame(record: LogRecord) -> str:
    """Render one :class:`LogRecord` as an SSE ``data:`` frame."""
    payload = LogRecordModel.from_domain(record).model_dump_json(by_alias=True)
    return f"data: {payload}\n\n"


async def _sse_frames(
    iterator: AsyncIterator[LogRecord],
    settings: Settings,
) -> AsyncIterator[str]:
    """Yield SSE frames from the provider's tail iterator.

    A backend that becomes unavailable *after* the stream has opened (the
    iterator raises on a later batch) can no longer change the HTTP status, so
    the failure is surfaced as a terminal ``event: error`` frame carrying the
    same Problem Details body the pre-stream path would have returned.
    """
    try:
        async for record in iterator:
            yield _data_frame(record)
    except (QueryUnsupported, BackendUnavailable):
        problem = _log_unavailable(settings).to_dict()
        yield f"event: error\ndata: {json.dumps(problem)}\n\n"


@router.get("/logs/tail")
async def tail_run_logs(
    workspace_id: _WorkspacePath,
    run_id: _RunPath,
    provider: LogProviderDep,
    settings: SettingsDep,
    ctx: CtxDep,
    last_event_id: Annotated[str | None, Header(alias="Last-Event-ID")] = None,
    cursor: Annotated[str | None, Query(description="Resume cursor (SSE fallback).")] = None,
) -> StreamingResponse:
    """Stream a run's logs as Server-Sent Events.

    Honours an inbound ``Last-Event-ID`` header (or ``cursor`` query param) as
    the resume position. A ``noop``/unreachable backend returns ``503``.
    """
    _ensure_workspace(ctx, workspace_id)
    resume = last_event_id or cursor
    from_cursor = Cursor(token=resume) if resume else None
    try:
        iterator = provider.tail_run_logs(WorkspaceId(workspace_id), RunId(run_id), from_cursor)
    except (QueryUnsupported, BackendUnavailable) as exc:
        raise _log_unavailable(settings) from exc
    except WorkspaceMismatch as exc:
        raise _run_not_found(run_id) from exc
    return StreamingResponse(
        _sse_frames(iterator, settings),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/logs")
async def query_run_logs(
    workspace_id: _WorkspacePath,
    run_id: _RunPath,
    provider: LogProviderDep,
    settings: SettingsDep,
    ctx: CtxDep,
    step_id: Annotated[str | None, Query(alias="stepId")] = None,
    from_: Annotated[str | None, Query(alias="from", description="ISO-8601 start.")] = None,
    to: Annotated[str | None, Query(alias="to", description="ISO-8601 end.")] = None,
    severity: Annotated[Severity | None, Query(description="Minimum severity bucket.")] = None,
    cursor: Annotated[str | None, Query(description="Opaque continuation cursor.")] = None,
) -> LogPageModel:
    """Paged historical log query for one run (optionally narrowed to a step).

    All filters AND-combine. ``stepId`` routes to ``query_step_logs``; otherwise
    the whole run is queried. A ``noop``/unreachable backend returns ``503``.
    """
    _ensure_workspace(ctx, workspace_id)
    log_filter = LogFilter(
        step_id=StepId(step_id) if step_id is not None else None,
        start=_parse_dt(from_),
        end=_parse_dt(to),
        severity_at_least=severity,
    )
    page_cursor = Cursor(token=cursor) if cursor is not None else None
    workspace = WorkspaceId(workspace_id)
    run = RunId(run_id)
    try:
        if step_id is not None:
            page = await provider.query_step_logs(
                workspace, run, StepId(step_id), log_filter, page_cursor
            )
        else:
            page = await provider.query_run_logs(workspace, run, log_filter, page_cursor)
    except (QueryUnsupported, BackendUnavailable) as exc:
        raise _log_unavailable(settings) from exc
    except WorkspaceMismatch as exc:
        raise _run_not_found(run_id) from exc
    return LogPageModel.from_domain(page)


def _parse_dt(value: str | None) -> datetime | None:
    """Parse an ISO-8601 query param into a ``datetime`` (or ``None``).

    A malformed value is a client error, surfaced as ``400``.
    """
    if value is None:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"invalid datetime: {value!r}") from exc
