"""SPL error taxonomy.

Every error raised by the Storage Provider Layer derives from `SPLError`,
so callers can catch the base class to handle any provider failure
uniformly. Specific subclasses match the rows in the Failure Modes table
of `design/components/storage-provider-layer/design.md`.
"""

from __future__ import annotations


class SPLError(Exception):
    """Base class for every error raised by the Storage Provider Layer.

    Catching `SPLError` matches any provider failure, including:
      - `ImmutableViolation` / `ConflictDigest` (write-once violations)
      - `LeaseBusy`, `LeaseExpired` (cursor lease primitive failures)
      - `MigrationRequired` (startup-time schema gap)
      - `InvalidTransactionHandle` (transaction misuse)
      - `WorkspaceMismatch` (caller surfaces as HTTP 404)
      - `BackendUnavailable` (transient — retry with backoff)
      - `QueryUnsupported` (noop query-facade adapter)
      - `NotReserved` (idempotency-record state-machine misuse)
      - `WorkspaceScopingViolation` (middleware bypass attempt)
    """


class ImmutableViolation(SPLError):
    """Write-once violation.

    Raised when a caller attempts to mutate a row that is immutable by
    contract:
      - `DefinitionStoreProvider.put*Version` on an existing
        `(workflowId, version)` / `(templateId, version)` key
      - `MetadataStoreProvider.appendStepAttempt` on an existing
        `(runId, stepId, attempt)` triple
      - `AuthStoreProvider.putOidcIdentity` on an existing
        `(issuer, subject)`

    Even an idempotent re-put of identical content raises this — callers
    that want idempotence MUST check existence first. Caller maps to
    HTTP 409.
    """


class ConflictDigest(ImmutableViolation):
    """CatalogStore put with a different digest on an existing key.

    For `(namespace, type, version)` keys, identical-digest re-puts are
    idempotent and return success; only digest mismatches raise this.
    Subclasses `ImmutableViolation` so the generic 409-handler in
    callers picks it up automatically. Caller maps to HTTP 409.
    """


class LeaseBusy(SPLError):
    """`acquireCursorLease` could not take the lease.

    Another holder owns the lease and its TTL has not elapsed. Callers
    should wait and retry on their own schedule (do not busy-loop).
    """


class LeaseExpired(SPLError):
    """`commitCursor` called after the lease TTL elapsed or by the wrong holder.

    Callers MUST discard any work performed under the lease and re-acquire
    before retrying. Treating this as a soft failure risks duplicate work.
    """


class MigrationRequired(SPLError):
    """Platform refused to start because adapters are missing required revisions.

    Carries the per-interface gap so operator logs can list exactly what
    needs migrating. Operator resolves with `custos migrate up` and
    restarts. Read-only fallback mode is intentionally NOT supported in
    v1 (it would silently mask write failures).
    """

    def __init__(self, gaps: list[tuple[str, int]]) -> None:
        """
        Args:
            gaps: list of (interface_name, required_revision) tuples that
                the running adapter set does not satisfy.
        """
        super().__init__(self._format(gaps))
        self.gaps: list[tuple[str, int]] = list(gaps)

    @staticmethod
    def _format(gaps: list[tuple[str, int]]) -> str:
        if not gaps:
            return "MigrationRequired: no gaps reported"
        joined = ", ".join(f"{name}:rev{rev}" for name, rev in gaps)
        return f"MigrationRequired: missing revisions [{joined}]"


class InvalidTransactionHandle(SPLError):
    """A transaction handle was passed to a different provider than it was opened on.

    No cross-interface transactions exist by design — `withTransaction`
    handles are opaque to callers and tied to the provider that issued
    them. This is a programming error, not a retryable condition.
    """


class WorkspaceMismatch(SPLError):
    """The supplied identifier exists, but in a different workspace.

    Callers MUST surface this as HTTP 404, not 403 — disclosing
    cross-workspace existence would leak tenant information.
    """


class BackendUnavailable(SPLError):
    """The underlying backend is transiently unreachable.

    Adapters classify driver errors into this (transient — retry with
    backoff) versus domain failures (terminal — do not retry). The
    original driver exception is attached via `__cause__` for operator
    inspection.
    """

    def __init__(self, message: str, *, cause: BaseException | None = None) -> None:
        super().__init__(message)
        if cause is not None:
            self.__cause__ = cause


class QueryUnsupported(SPLError):
    """The configured query-facade adapter does not implement this operation.

    Returned by the `noop` LogQueryProvider/MetricsQueryProvider adapters
    when the customer's log/metrics backend has no first-party support.
    The UI falls back to `CUSTOS_LOGS_EXTERNAL_URL` /
    `CUSTOS_METRICS_EXTERNAL_URL`.
    """


class NotReserved(SPLError):
    """`completeIdempotencyRecord` called on a row that is not in-progress under this caller.

    The record either never existed, has already completed, or the
    caller does not match the reservation holder. Not retryable as-is —
    the caller must re-check the idempotency-record state machine.
    """


class ArtifactNotFound(SPLError):
    """`ArtifactStoreProvider.get` was called on an absent artifact ID.

    Distinct from `WorkspaceMismatch` so adapters can distinguish "no
    such row anywhere" from "exists in a different workspace". Both
    surface as HTTP 404 at the caller — never disclose cross-workspace
    existence.
    """


class WorkspaceScopingViolation(SPLError):
    """A workspace-scoped provider call was made without a valid `workspace_id`.

    Raised by the workspace-scoping middleware (`wrap_workspace_scoped`)
    when a wrapped method is invoked without a non-empty `WorkspaceId`
    as the first non-self argument. This is a programming error: the
    static type system already requires the parameter, so this
    middleware catches the cases the type checker cannot — `None`,
    empty strings, or callers using `**kwargs` to bypass the signature.
    """


__all__ = [
    "ArtifactNotFound",
    "BackendUnavailable",
    "ConflictDigest",
    "ImmutableViolation",
    "InvalidTransactionHandle",
    "LeaseBusy",
    "LeaseExpired",
    "MigrationRequired",
    "NotReserved",
    "QueryUnsupported",
    "SPLError",
    "WorkspaceMismatch",
    "WorkspaceScopingViolation",
]
