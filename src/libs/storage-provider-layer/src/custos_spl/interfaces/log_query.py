"""LogQueryProvider — inbound read-back facade over a customer log backend.

Holds no persistent state, owns no schema, and runs no migration. Exists
so the Custos UI/API per-run log tail and search can read from Loki,
OpenSearch/Elasticsearch, or a `noop` pointer adapter without the rest
of the platform knowing which.

**Concern separation**: outbound log streaming (Custos → customer's
sinks) is handled entirely by the OTel Collector configured by
Observability Service. This interface is **only** the inbound read-back
path.

See `design/components/storage-provider-layer/design.md` § LogQueryProvider.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import ClassVar, Literal, Protocol, runtime_checkable

from custos_spl.ids import RunId, StepId, WorkspaceId
from custos_spl.pagination import Cursor, Page

Severity = Literal["trace", "debug", "info", "warn", "error", "fatal"]
"""Severity bucket per OTel severity_text conventions.

Adapters MUST normalize backend-native severity strings into one of
these six values before returning a `LogRecord`. Backend-specific
extensions belong in `attributes`, not in this field.
"""


@dataclass(frozen=True, slots=True)
class LogRecord:
    """A single log line returned by a query.

    `attributes` carries OTel resource + log attributes (e.g.
    `service.name`, `trace.id`); adapters MAY include backend-specific
    fields here so callers that know the backend can still get at them.
    """

    timestamp: datetime
    severity: Severity
    message: str
    run_id: RunId
    step_id: StepId | None
    attributes: Mapping[str, str] = field(default_factory=dict)


LogPage = Page[LogRecord]
"""A paged result set of log records.

Type alias over the shared `Page[T]` primitive — same semantics:
`next_cursor` is `None` on the last page, an empty `items` tuple with
a non-`None` `next_cursor` means "no matches in this window, keep
paging".
"""


@dataclass(frozen=True, slots=True)
class LogFilter:
    """Optional predicates for `query_run_logs` / `query_step_logs`.

    `step_id` is ignored by `query_step_logs` (the step is in the path
    arg) and used to narrow `query_run_logs` to a single step's lines.
    All fields are AND-combined.
    """

    step_id: StepId | None = None
    start: datetime | None = None
    end: datetime | None = None
    severity_at_least: Severity | None = None
    message_contains: str | None = None


@runtime_checkable
class LogQueryProvider(Protocol):
    """Inbound read-back over the customer's log backend.

    Workspace-scoped: `workspace_id` is the first arg on every method.
    Adapters MUST add the workspace filter to every backend query;
    cross-workspace reads are not expressible.

    Failure surface:
      - `BackendUnavailable` — transient backend failure; retry with backoff.
      - `QueryUnsupported` — returned by the `noop` adapter (or any
        adapter the operator has stubbed out); the UI falls back to the
        `CUSTOS_LOGS_EXTERNAL_URL` pointer.
      - `WorkspaceMismatch` — run or step exists in a different
        workspace; caller maps to HTTP 404.

    No `SCHEMA_REVISION` is declared by query facades — they own no
    schema and run no migration.
    """

    SCHEMA_REVISION: ClassVar[int] = 0

    async def query_run_logs(
        self,
        workspace_id: WorkspaceId,
        run_id: RunId,
        filter: LogFilter,
        cursor: Cursor | None = None,
    ) -> LogPage:
        """Paged historical query for one run's logs.

        `cursor` is opaque — pass back the `next_cursor` from the
        previous page to continue. Pass `None` (the default) to start
        from the beginning of the result set.
        """
        ...

    def tail_run_logs(
        self,
        workspace_id: WorkspaceId,
        run_id: RunId,
        from_cursor: Cursor | None = None,
    ) -> AsyncIterator[LogRecord]:
        """Live tail of a run's logs.

        Returns an `AsyncIterator[LogRecord]` directly (declared as
        plain `def` so adapters implement it as an async generator).
        The adapter MUST stream new records as they arrive and MUST
        honor the caller's cancellation (closing the iterator
        terminates the upstream subscription). `from_cursor` resumes
        from a prior position; `None` starts at "now".
        """
        ...

    async def query_step_logs(
        self,
        workspace_id: WorkspaceId,
        run_id: RunId,
        step_id: StepId,
        filter: LogFilter,
        cursor: Cursor | None = None,
    ) -> LogPage:
        """Paged historical query scoped to a single step within a run.

        Same `filter` shape as `query_run_logs`; the `step_id` field on
        `filter` is ignored (the step is fixed by the path arg).
        """
        ...


__all__ = [
    "LogFilter",
    "LogPage",
    "LogQueryProvider",
    "LogRecord",
    "Severity",
]
