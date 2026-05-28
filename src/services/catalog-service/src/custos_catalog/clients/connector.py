"""Connector Service client (CONN-IMPL-034 / CS-IMPL-023).

The Catalog Service consults Connector Service at workflow publish time
to confirm every connector-instance reference in the workflow document
resolves to a live row in the target workspace. The contract surface is
narrow — a single :meth:`ConnectorClient.exists_connector_instance` call
per unique reference — so this module ships:

* :class:`ConnectorClient` — :class:`typing.Protocol` for the resolver
  pipeline (re-exported from :mod:`custos_catalog.resolve` for back-compat).
* :class:`StubConnectorClient` — in-process always-``True`` fake retained
  for offline test scenarios and gated behind the
  ``CAT_USE_STUB_CONNECTOR_CLIENT`` feature flag.
* :class:`HttpConnectorClient` — real client that invokes
  ``POST {endpoint}/internal/v1/connectors:validate`` (the
  ``ValidateConnector`` Internal RPC, CONN-IMPL-027) with the request's
  call-context header forwarded so workspace + ``connector:validate``
  permission flow with the caller's identity.
* :class:`ConnectorClientFactory` — owns the shared ``httpx.AsyncClient``
  pool and per-process negative-result cache, and produces a
  request-scoped :class:`HttpConnectorClient` bound to the inbound
  ``x-custos-callctx`` header.
* :class:`ConnectorServiceUnavailable` — raised on transport-level
  failures and 5xx responses; the API error handler maps it to a 503
  ``catalog.dependency_unavailable`` envelope (design § Failure Modes).

The endpoint URL comes from :data:`custos_catalog.settings.ENV_CONNECTOR_ENDPOINT`
(``CAT_CONNECTOR_ENDPOINT``); per-request timeout from
``CAT_CONNECTOR_TIMEOUT_SECONDS`` (default 2.0); negative-cache TTL from
``CAT_CONNECTOR_NEGATIVE_CACHE_TTL_SECONDS`` (default 5.0).
"""

from __future__ import annotations

import logging
import time
from collections.abc import Mapping
from typing import Any, Final, Protocol, runtime_checkable

import httpx

_LOGGER = logging.getLogger("custos_catalog.clients.connector")

#: Canonical wire header carrying the call-context document. Mirrors the
#: canonical lowercase form pinned by the API Gateway design (COMP-001)
#: and used by :mod:`custos_catalog.middleware.callctx`.
CALLCTX_HEADER: Final[str] = "x-custos-callctx"

#: Path of the ``ValidateConnector`` Internal RPC on Connector Service
#: (CONN-IMPL-027). The catalog passes ``mode: "instance"`` to drive the
#: existence + manifest-drift check; ``mode: "manifest"`` is reserved for
#: operator-facing "test before save" tooling and not used here.
_VALIDATE_PATH: Final[str] = "/internal/v1/connectors:validate"


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class ConnectorServiceUnavailable(RuntimeError):
    """Raised when Connector Service is unreachable or returns 5xx.

    Distinct from :class:`custos_catalog.resolve.ConnectorInstanceMissing`
    (a structured ``unresolved_reference`` 4xx). The API error handler
    maps this exception to a 503 response with code
    ``catalog.dependency_unavailable`` so publish callers can retry once
    Connector Service is healthy again (design § Failure Modes).
    """

    #: Stable error code for the wire envelope.
    code: str = "catalog.dependency_unavailable"

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class ConnectorClient(Protocol):
    """Connector Service client surface used by the resolver."""

    async def exists_connector_instance(
        self,
        workspace_id: str,
        name: str,
    ) -> bool:
        """Return ``True`` iff a connector instance with ``name`` exists in ``workspace_id``."""


# ---------------------------------------------------------------------------
# Stub (kept for offline test scenarios; feature-flag gated)
# ---------------------------------------------------------------------------


class StubConnectorClient:
    """Offline-test stub for the Connector Service existence check.

    Returns ``True`` for every name and tracks the per-batch call list.
    The first call in a batch emits a single ``WARNING`` log line so
    operators can see the stub is active in an offline / development
    environment; subsequent calls in the same batch are silent. Call
    :meth:`reset_batch` between batches when a new publish starts.

    No longer the production default: activated only when
    ``CAT_USE_STUB_CONNECTOR_CLIENT=true`` (see
    :data:`custos_catalog.settings.ENV_USE_STUB_CONNECTOR_CLIENT`). The
    live path is :class:`HttpConnectorClient`.
    """

    def __init__(self, *, logger: logging.Logger | None = None) -> None:
        self._logger = logger or _LOGGER
        self._calls: list[tuple[str, str]] = []
        self._warned: bool = False

    async def exists_connector_instance(self, workspace_id: str, name: str) -> bool:
        """Stub: always ``True``; logs ``WARNING`` once per batch."""
        self._calls.append((workspace_id, name))
        if not self._warned:
            self._logger.warning(
                "StubConnectorClient: connector-instance existence checks "
                "are stubbed (CAT_USE_STUB_CONNECTOR_CLIENT is enabled). "
                "First call this batch was workspace=%r name=%r.",
                workspace_id,
                name,
            )
            self._warned = True
        return True

    @property
    def calls(self) -> tuple[tuple[str, str], ...]:
        """Snapshot of all calls made in the current batch."""
        return tuple(self._calls)

    def reset_batch(self) -> None:
        """Begin a new batch.

        Resets the warning latch and the call log so the next call
        produces a fresh ``WARNING``.
        """
        self._calls.clear()
        self._warned = False


# ---------------------------------------------------------------------------
# Live HTTP client
# ---------------------------------------------------------------------------


#: Hard cap on the number of distinct (workspace, name) entries the
#: negative cache will hold. Bounds memory in the face of accidental
#: fan-out (a misconfigured workflow document referencing many distinct
#: missing names within the TTL window) or hostile input.
_NEGATIVE_CACHE_MAX_ENTRIES: Final[int] = 1024


class _NegativeCache:
    """Tiny TTL cache of negative existence results.

    Connector Service may receive a burst of identical existence checks
    when a misconfigured workflow document references the same missing
    connector-instance many times. We cache 404 results for a short
    window so the publish path does not hammer Connector Service when a
    single mistake fans out across many slots; per-batch correctness is
    preserved because the resolver de-duplicates inside
    :func:`custos_catalog.resolve.collect_connector_instance_calls`
    before this cache is consulted (the cache only matters across
    *distinct* batches issued within the TTL window).

    Memory is bounded two ways: every insertion opportunistically
    sweeps expired entries, and the cache enforces a hard
    :data:`_NEGATIVE_CACHE_MAX_ENTRIES` cap (evicting the
    soonest-to-expire entry once the cap is reached) so a runaway
    caller (or a hostile workflow stuffed with thousands of distinct
    bogus connector names) cannot grow the dict without limit.
    """

    def __init__(
        self,
        *,
        ttl_seconds: float,
        max_entries: int = _NEGATIVE_CACHE_MAX_ENTRIES,
    ) -> None:
        self._ttl = max(0.0, ttl_seconds)
        self._max_entries = max(0, max_entries)
        self._entries: dict[tuple[str, str], float] = {}

    def get(self, workspace_id: str, name: str) -> bool | None:
        """Return ``False`` if a fresh negative is cached; else ``None``."""
        if self._ttl <= 0.0:
            return None
        deadline = self._entries.get((workspace_id, name))
        if deadline is None:
            return None
        if deadline <= time.monotonic():
            # Expired — drop and miss.
            self._entries.pop((workspace_id, name), None)
            return None
        return False

    def record_missing(self, workspace_id: str, name: str) -> None:
        """Record a fresh negative result with TTL."""
        if self._ttl <= 0.0:
            return
        # Opportunistic prune: drop already-expired entries on every
        # write so the dict cannot grow unbounded purely on TTL churn.
        now = time.monotonic()
        if self._entries:
            expired = [key for key, deadline in self._entries.items() if deadline <= now]
            for key in expired:
                self._entries.pop(key, None)
        # Hard cap: if still at capacity after pruning, evict the entry
        # closest to expiry to make room. This is O(n) but n is bounded
        # to ``_NEGATIVE_CACHE_MAX_ENTRIES`` and only fires on the
        # cap-hit path.
        if (
            self._max_entries > 0
            and len(self._entries) >= self._max_entries
            and (workspace_id, name) not in self._entries
        ):
            evict_key = min(self._entries, key=lambda k: self._entries[k])
            self._entries.pop(evict_key, None)
        self._entries[(workspace_id, name)] = now + self._ttl


class ConnectorClientFactory:
    """Process-wide pool + per-request client factory.

    Owns the shared :class:`httpx.AsyncClient` (connection pool) and a
    short-TTL negative-result cache. :meth:`for_request` returns a
    cheap :class:`HttpConnectorClient` view bound to the inbound call
    context header so workspace + ``connector:validate`` permission
    flow naturally to Connector Service.

    The factory is constructed once during the FastAPI lifespan
    (:func:`custos_catalog.create_app`) and torn down on shutdown via
    :meth:`aclose`.
    """

    def __init__(
        self,
        *,
        endpoint: str,
        timeout_seconds: float,
        negative_cache_ttl_seconds: float,
        transport: httpx.AsyncBaseTransport | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        if not endpoint:
            raise ValueError(
                "ConnectorClientFactory requires a non-empty endpoint "
                "(see CAT_CONNECTOR_ENDPOINT in catalog-service design § Configuration)"
            )
        self._endpoint = endpoint.rstrip("/")
        self._timeout = httpx.Timeout(timeout_seconds)
        self._cache = _NegativeCache(ttl_seconds=negative_cache_ttl_seconds)
        self._logger = logger or _LOGGER
        # Tests inject an ``httpx.ASGITransport`` to mount a Connector
        # Service test double in the same event loop; production leaves
        # ``transport=None`` so httpx opens real sockets.
        self._http = httpx.AsyncClient(
            base_url=self._endpoint,
            timeout=self._timeout,
            transport=transport,
        )

    def for_request(self, *, callctx_header_value: str) -> HttpConnectorClient:
        """Return a per-request client bound to a call-context header."""
        return HttpConnectorClient(
            http=self._http,
            cache=self._cache,
            callctx_header_value=callctx_header_value,
            logger=self._logger,
        )

    async def aclose(self) -> None:
        """Close the shared HTTP client. Idempotent."""
        await self._http.aclose()


class HttpConnectorClient:
    """Live :class:`ConnectorClient` backed by ``httpx`` + the call context.

    Bound to a single inbound request's ``x-custos-callctx`` header so
    Connector Service applies the caller's workspace and
    ``connector:validate`` permission. Holds no per-instance state
    beyond the (cheap) header string and a reference to the shared
    pool + cache on :class:`ConnectorClientFactory`.
    """

    def __init__(
        self,
        *,
        http: httpx.AsyncClient,
        cache: _NegativeCache,
        callctx_header_value: str,
        logger: logging.Logger | None = None,
    ) -> None:
        self._http = http
        self._cache = cache
        self._callctx_header_value = callctx_header_value
        self._logger = logger or _LOGGER

    async def exists_connector_instance(self, workspace_id: str, name: str) -> bool:
        """Call ``ValidateConnector`` on Connector Service.

        Wire contract: ``POST /internal/v1/connectors:validate`` with
        body ``{"mode": "instance", "connectorInstanceId": <name>}``.
        Workspace is carried by the call-context header (Connector
        Service trusts the header, never the body, per
        :mod:`custos_connector.api.validate`).

        Returns:
            ``True`` on 200 (instance exists; manifest validation may
            have passed). ``True`` on 400 (instance exists but its
            config is invalid against the current manifest — the
            *existence* contract is satisfied; runtime re-validates
            per step). ``False`` on 404.

        Raises:
            ConnectorServiceUnavailable: On transport errors, 5xx
                responses, and any non-{200, 400, 404} status. The API
                handler renders this as a 503 envelope.
        """
        cached = self._cache.get(workspace_id, name)
        if cached is False:
            return False

        body: dict[str, Any] = {
            "mode": "instance",
            "connectorInstanceId": name,
        }
        headers: dict[str, str] = {}
        if self._callctx_header_value:
            headers[CALLCTX_HEADER] = self._callctx_header_value

        try:
            response = await self._http.post(
                _VALIDATE_PATH,
                json=body,
                headers=headers,
            )
        except httpx.TimeoutException as exc:
            raise ConnectorServiceUnavailable(
                f"connector-service ValidateConnector timed out: {exc}",
            ) from exc
        except httpx.TransportError as exc:
            raise ConnectorServiceUnavailable(
                f"connector-service is unreachable: {exc}",
            ) from exc

        status = response.status_code
        if status == 200:
            return True
        if status == 404:
            # 404 covers both ``connector.instance_not_found`` and
            # ``connector.instance_type_not_registered``; both mean the
            # connector reference does not resolve to a live row in
            # ``workspace_id``, which is what the existence contract
            # cares about.
            self._cache.record_missing(workspace_id, name)
            self._log_not_found(workspace_id, name, response)
            return False
        if status == 400:
            # Instance exists; config drift against the current manifest.
            # The publish-time existence contract is satisfied. Runtime
            # re-validates per step (CONN-IMPL-027 manifest re-check).
            self._logger.info(
                "connector-service ValidateConnector returned 400 for "
                "workspace=%r name=%r; treating as exists (config drift "
                "will surface at runtime).",
                workspace_id,
                name,
            )
            return True
        if 500 <= status < 600:
            # Log the body snippet server-side for operator triage but
            # keep it OUT of the exception message: the API error
            # handler renders ``str(exc)`` into the public 503 envelope
            # and we must not leak Connector Service internals to the
            # publish caller.
            self._logger.warning(
                "connector-service returned %s for ValidateConnector "
                "(workspace=%r name=%r); body=%s",
                status,
                workspace_id,
                name,
                _safe_snippet(response),
            )
            raise ConnectorServiceUnavailable(
                f"connector-service returned {status} for "
                f"ValidateConnector(workspace={workspace_id!r}, name={name!r})",
                status_code=status,
            )
        # 401/403 are catalog-service mis-wiring (missing perm) — surface
        # the same way as 5xx so the operator sees a clear 503 with
        # ``catalog.dependency_unavailable`` rather than a silent True.
        # Log the body snippet server-side; do NOT include it in the
        # raised exception message (it ends up in the 503 response body).
        self._logger.warning(
            "connector-service returned unexpected status %s for "
            "ValidateConnector (workspace=%r name=%r); body=%s",
            status,
            workspace_id,
            name,
            _safe_snippet(response),
        )
        raise ConnectorServiceUnavailable(
            f"connector-service returned unexpected status {status} for "
            f"ValidateConnector(workspace={workspace_id!r}, name={name!r})",
            status_code=status,
        )

    def _log_not_found(
        self,
        workspace_id: str,
        name: str,
        response: httpx.Response,
    ) -> None:
        """Log a debug-level breadcrumb when the instance is absent."""
        self._logger.debug(
            "connector-service reported instance %r does not exist in "
            "workspace %r (status=%s body=%s)",
            name,
            workspace_id,
            response.status_code,
            _safe_snippet(response),
        )


def _safe_snippet(response: httpx.Response) -> str:
    """Best-effort 200-char snippet of the response body for logs."""
    try:
        text = response.text
    except Exception:  # pragma: no cover - body access never raises in practice
        return "<unreadable>"
    snippet = text.strip()
    if len(snippet) > 200:
        return snippet[:200] + "…"
    return snippet


# ---------------------------------------------------------------------------
# Factory entry point used by the FastAPI app factory
# ---------------------------------------------------------------------------


def build_connector_client_factory(
    *,
    endpoint: str,
    timeout_seconds: float,
    negative_cache_ttl_seconds: float,
    use_stub: bool,
    transport: httpx.AsyncBaseTransport | None = None,
) -> ConnectorClientFactory | StubConnectorClient:
    """Build the lifespan-scoped Connector Service client.

    Returns a :class:`StubConnectorClient` when ``use_stub`` is true so
    offline test scenarios can opt out of network access; otherwise
    returns a :class:`ConnectorClientFactory` ready to produce
    per-request :class:`HttpConnectorClient` views.

    Args:
        endpoint: URL of the in-cluster Connector Service
            (``CAT_CONNECTOR_ENDPOINT``).
        timeout_seconds: Per-call timeout
            (``CAT_CONNECTOR_TIMEOUT_SECONDS``, default 2.0).
        negative_cache_ttl_seconds: TTL for cached 404 responses
            (``CAT_CONNECTOR_NEGATIVE_CACHE_TTL_SECONDS``, default 5.0).
        use_stub: When true (``CAT_USE_STUB_CONNECTOR_CLIENT=true``),
            return the offline stub instead of the live factory.
        transport: Optional ``httpx`` transport override used by tests
            to mount a Connector Service test double via
            ``httpx.ASGITransport``.
    """
    if use_stub:
        _LOGGER.warning(
            "CAT_USE_STUB_CONNECTOR_CLIENT is set; the offline "
            "StubConnectorClient will accept every connector-instance "
            "name. This is for development / offline tests only."
        )
        return StubConnectorClient()
    return ConnectorClientFactory(
        endpoint=endpoint,
        timeout_seconds=timeout_seconds,
        negative_cache_ttl_seconds=negative_cache_ttl_seconds,
        transport=transport,
    )


def request_callctx_header(headers: Mapping[str, str]) -> str:
    """Extract the raw call-context header value from inbound headers.

    Returns an empty string when the header is absent. The catalog
    middleware (:mod:`custos_catalog.middleware.callctx`) rejects
    requests without the header at 401 before any dependency runs, so
    in practice the empty-string branch only triggers in synthetic
    test setups that mount the resolver outside the full app stack.
    """
    return headers.get(CALLCTX_HEADER, "")


__all__ = [
    "CALLCTX_HEADER",
    "ConnectorClient",
    "ConnectorClientFactory",
    "ConnectorServiceUnavailable",
    "HttpConnectorClient",
    "StubConnectorClient",
    "build_connector_client_factory",
    "request_callctx_header",
]
