"""Auth Service client — Dapr service-invocation adapter (AGW-IMPL-004).

The gateway delegates every authentication and authorization decision to the
Auth Service and mints the signed call context that internal RPCs travel on. It
reaches the Auth Service (Dapr app id ``custos-auth``) through the local Dapr
sidecar over three Internal RPCs:

* ``verify_and_authorize`` → ``POST /rpc/authz.verifyAndAuthorize`` — the hot
  path: authenticate the bearer **and** decide ``permission`` against
  ``workspace_id`` in one round trip.
* ``callctx_sign`` → ``POST /rpc/callctx.sign`` — mint the signed call-context
  JWT the gateway forwards to the owning downstream component.
* ``get_permissions`` → ``GET /v1/permissions`` — the declared permission
  registry, used at startup to validate the gateway's required grants.

The request/response models mirror the Auth Service wire contracts
(``custos_auth.api.routes.rpc`` / ``custos_auth.api.models``), which are
snake_case. The transport mirrors the platform's ``_dapr_invoke`` precedent: a
lifespan-owned :class:`httpx.AsyncClient` posting to
``http://{host}:{port}/v1.0/invoke/{appId}/method/{method}``.

Transport failures and transient HTTP responses (``408``/``429``/``5xx``) raise
an :class:`AuthServiceClientError` with ``retryable=True``; permanent ``4xx``
responses and decode failures raise with ``retryable=False``.
:class:`NoopAuthServiceClient` and :class:`FakeAuthServiceClient` are test/dev
doubles.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Annotated, Final, Protocol, TypeVar, runtime_checkable

import httpx
from pydantic import BaseModel, ConfigDict, Field

__all__ = [
    "AUTH_APP_ID",
    "CALLCTX_SIGN_METHOD",
    "DEFAULT_DAPR_HTTP_HOST",
    "DEFAULT_DAPR_HTTP_PORT",
    "DEFAULT_RPC_TIMEOUT_SECONDS",
    "ENV_DAPR_HTTP_HOST",
    "ENV_DAPR_HTTP_PORT",
    "GET_PERMISSIONS_METHOD",
    "VERIFY_AND_AUTHORIZE_METHOD",
    "AuthServiceClient",
    "AuthServiceClientDecodeError",
    "AuthServiceClientError",
    "AuthServiceClientStatusError",
    "AuthServiceClientTransportError",
    "CallctxSignRequest",
    "CallctxSignResponse",
    "DaprAuthServiceClient",
    "DaprEndpoint",
    "DeclaredPermission",
    "FakeAuthServiceClient",
    "NoopAuthServiceClient",
    "VerifyAndAuthorizeRequest",
    "VerifyAndAuthorizeResponse",
    "build_invoke_url",
    "read_dapr_endpoint",
]

#: Dapr sidecar host/port env knobs (shared platform convention).
ENV_DAPR_HTTP_HOST: Final[str] = "DAPR_HTTP_HOST"
ENV_DAPR_HTTP_PORT: Final[str] = "DAPR_HTTP_PORT"
DEFAULT_DAPR_HTTP_HOST: Final[str] = "127.0.0.1"
DEFAULT_DAPR_HTTP_PORT: Final[int] = 3500
DEFAULT_RPC_TIMEOUT_SECONDS: Final[float] = 10.0

#: Dapr app id of the Auth Service.
AUTH_APP_ID: Final[str] = "custos-auth"

#: Auth Service inbound method paths (Dapr invoke method segment).
VERIFY_AND_AUTHORIZE_METHOD: Final[str] = "rpc/authz.verifyAndAuthorize"
CALLCTX_SIGN_METHOD: Final[str] = "rpc/callctx.sign"
GET_PERMISSIONS_METHOD: Final[str] = "v1/permissions"

_ModelT = TypeVar("_ModelT", bound=BaseModel)


# --- Wire models -------------------------------------------------------------


class VerifyAndAuthorizeRequest(BaseModel):
    """Body of ``POST /rpc/authz.verifyAndAuthorize``.

    Mirrors the Auth Service ``VerifyAndAuthorizeRpcRequest``: authenticate
    ``token`` then decide ``permission`` against ``workspace_id`` in one call.
    """

    model_config = ConfigDict(extra="forbid")

    token: str = Field(..., min_length=1, max_length=4096)
    permission: str = Field(..., min_length=1, max_length=255)
    workspace_id: str = Field(..., min_length=1, max_length=120)


class VerifyAndAuthorizeResponse(BaseModel):
    """Response of ``POST /rpc/authz.verifyAndAuthorize``.

    Returned with HTTP 200 whenever the bearer authenticated, even when the
    decision is ``deny`` — ``allowed`` carries the decision so the gateway maps
    a deny onto its own ``permission-denied`` 403. A failed *verify* is a
    non-200 (``401``), surfaced as an :class:`AuthServiceClientStatusError`.
    """

    model_config = ConfigDict(extra="ignore")

    principal_id: str
    allowed: bool
    reason: str
    audit_event_id: str


class CallctxSignRequest(BaseModel):
    """Body of ``POST /rpc/callctx.sign``.

    Mirrors the Auth Service ``CallctxSignRpcRequest``. ``audience`` is set to a
    per-component value (``custos.catalog``, …) so a token minted for one
    downstream cannot be replayed against another; ``permissions`` embeds the
    principal's RBAC grant so downstreams enforce without a second round trip.
    """

    model_config = ConfigDict(extra="forbid")

    principal_id: str = Field(..., min_length=1, max_length=120)
    caller_component: str = Field(..., min_length=1, max_length=64)
    workspace_id: str | None = Field(default=None, max_length=120)
    ttl_seconds: int | None = Field(default=None, ge=1, le=86_400)
    permissions: list[Annotated[str, Field(min_length=1, max_length=128)]] = Field(
        default_factory=list, max_length=256
    )
    audience: str | None = Field(default=None, min_length=1, max_length=120)


class CallctxSignResponse(BaseModel):
    """Response of ``POST /rpc/callctx.sign`` (Auth ``CallctxSignRpcResponse``).

    ``token`` is the signed call-context JWT the gateway forwards downstream;
    the remaining fields are exposed for diagnostics / audit correlation.
    """

    model_config = ConfigDict(extra="ignore")

    token: str
    kid: str
    jti: str
    iat: int
    exp: int


class DeclaredPermission(BaseModel):
    """One row of ``GET /v1/permissions`` (Auth ``PermissionResponse``).

    ``declared_by`` carries the loader-side multi-declarer attribution
    (pipe-delimited list of owning components).
    """

    model_config = ConfigDict(extra="ignore")

    name: str
    description: str
    declared_by: str


class _PermissionListResponse(BaseModel):
    """Envelope of ``GET /v1/permissions`` (Auth ``PermissionListResponse``)."""

    model_config = ConfigDict(extra="ignore")

    permissions: list[DeclaredPermission]


# --- Errors ------------------------------------------------------------------


class AuthServiceClientError(Exception):
    """A failure invoking the Auth Service.

    ``retryable`` tells the caller whether a backoff-and-retry can plausibly
    succeed (transport blips, ``408``/``429``/``5xx``) or whether the call is a
    permanent failure (contract ``4xx``, undecodable response).
    """

    def __init__(self, message: str, *, retryable: bool) -> None:
        super().__init__(message)
        self.retryable = retryable


class AuthServiceClientTransportError(AuthServiceClientError):
    """The HTTP request failed before a response arrived (always retryable)."""

    def __init__(self, message: str) -> None:
        super().__init__(message, retryable=True)


class AuthServiceClientStatusError(AuthServiceClientError):
    """The Auth Service returned a non-2xx response.

    ``408``/``429``/``5xx`` are retryable (transient); every other status is a
    permanent contract failure.
    """

    def __init__(self, message: str, *, status_code: int) -> None:
        retryable = status_code in (408, 429) or status_code // 100 == 5
        super().__init__(message, retryable=retryable)
        self.status_code = status_code


class AuthServiceClientDecodeError(AuthServiceClientError):
    """A 2xx response body could not be decoded into the expected shape."""

    def __init__(self, message: str) -> None:
        super().__init__(message, retryable=False)


# --- Endpoint ----------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DaprEndpoint:
    """A resolved Dapr service-invocation target (sidecar host/port + app id)."""

    host: str
    http_port: int
    app_id: str

    def __post_init__(self) -> None:
        if not self.host:
            raise ValueError("DaprEndpoint.host must be a non-empty string")
        if not self.app_id:
            raise ValueError("DaprEndpoint.app_id must be a non-empty string")
        if isinstance(self.http_port, bool) or self.http_port <= 0:
            raise ValueError(
                f"DaprEndpoint.http_port must be a positive integer (got {self.http_port!r})"
            )


def build_invoke_url(endpoint: DaprEndpoint, method: str) -> str:
    """Build the Dapr service-invocation URL for ``method`` on ``endpoint``.

    Raises:
        ValueError: If ``method`` is empty or only slashes.
    """
    normalized = method.lstrip("/")
    if not normalized:
        raise ValueError("method must be a non-empty Dapr invoke method path")
    return (
        f"http://{endpoint.host}:{endpoint.http_port}"
        f"/v1.0/invoke/{endpoint.app_id}/method/{normalized}"
    )


def read_dapr_endpoint(env: Mapping[str, str], *, app_id: str = AUTH_APP_ID) -> DaprEndpoint:
    """Resolve a :class:`DaprEndpoint` from the environment.

    ``app_id`` is the target service's Dapr app id (the Auth Service's
    ``custos-auth`` by default); the sidecar host and port come from
    ``DAPR_HTTP_HOST`` / ``DAPR_HTTP_PORT`` (falling back to the Dapr defaults).

    Raises:
        ValueError: If ``app_id`` is empty, or ``DAPR_HTTP_PORT`` is not an int.
    """
    if not app_id:
        raise ValueError("app_id is required to target a Dapr service-invocation endpoint")
    host = env.get(ENV_DAPR_HTTP_HOST, "").strip() or DEFAULT_DAPR_HTTP_HOST
    raw_port = env.get(ENV_DAPR_HTTP_PORT, "").strip()
    if raw_port == "":
        http_port = DEFAULT_DAPR_HTTP_PORT
    else:
        try:
            http_port = int(raw_port)
        except ValueError as exc:
            raise ValueError(f"{ENV_DAPR_HTTP_PORT} must be an integer (got {raw_port!r})") from exc
    return DaprEndpoint(host=host, http_port=http_port, app_id=app_id)


# --- Client interface --------------------------------------------------------


@runtime_checkable
class AuthServiceClient(Protocol):
    """The outbound Auth Service surface the gateway pipeline depends on."""

    async def verify_and_authorize(
        self, request: VerifyAndAuthorizeRequest
    ) -> VerifyAndAuthorizeResponse:
        """Authenticate the bearer and decide the permission (Auth 200)."""
        ...

    async def callctx_sign(self, request: CallctxSignRequest) -> CallctxSignResponse:
        """Mint a signed call-context JWT for a downstream component (Auth 200)."""
        ...

    async def get_permissions(self) -> list[DeclaredPermission]:
        """Return the declared permission registry (Auth 200)."""
        ...


# --- Real Dapr client --------------------------------------------------------


@dataclass(slots=True)
class DaprAuthServiceClient:
    """Calls the Auth Service Internal RPCs over the local Dapr sidecar.

    The ``http_client`` is owned by the app lifespan (not by this client) so it
    is shared and closed once at shutdown.
    """

    http_client: httpx.AsyncClient
    endpoint: DaprEndpoint
    timeout: float = DEFAULT_RPC_TIMEOUT_SECONDS

    async def verify_and_authorize(
        self, request: VerifyAndAuthorizeRequest
    ) -> VerifyAndAuthorizeResponse:
        url = build_invoke_url(self.endpoint, VERIFY_AND_AUTHORIZE_METHOD)
        response = await self._post(url, request.model_dump(), what="VerifyAndAuthorize")
        return self._decode(response, VerifyAndAuthorizeResponse, what="VerifyAndAuthorize")

    async def callctx_sign(self, request: CallctxSignRequest) -> CallctxSignResponse:
        url = build_invoke_url(self.endpoint, CALLCTX_SIGN_METHOD)
        response = await self._post(url, request.model_dump(), what="CallctxSign")
        return self._decode(response, CallctxSignResponse, what="CallctxSign")

    async def get_permissions(self) -> list[DeclaredPermission]:
        url = build_invoke_url(self.endpoint, GET_PERMISSIONS_METHOD)
        response = await self._get(url, what="GetPermissions")
        envelope = self._decode(response, _PermissionListResponse, what="GetPermissions")
        return envelope.permissions

    async def _post(self, url: str, body: object, *, what: str) -> httpx.Response:
        try:
            response = await self.http_client.post(
                url,
                json=body,
                timeout=self.timeout,
                headers={"Content-Type": "application/json"},
            )
        except httpx.HTTPError as exc:
            raise AuthServiceClientTransportError(f"{what} transport failure: {exc!r}") from exc
        return self._check_status(response, what=what)

    async def _get(self, url: str, *, what: str) -> httpx.Response:
        try:
            response = await self.http_client.get(url, timeout=self.timeout)
        except httpx.HTTPError as exc:
            raise AuthServiceClientTransportError(f"{what} transport failure: {exc!r}") from exc
        return self._check_status(response, what=what)

    @staticmethod
    def _check_status(response: httpx.Response, *, what: str) -> httpx.Response:
        if response.status_code // 100 != 2:
            preview = response.text[:200] if response.text else ""
            raise AuthServiceClientStatusError(
                f"{what} returned HTTP {response.status_code}: {preview!r}",
                status_code=response.status_code,
            )
        return response

    @staticmethod
    def _decode(response: httpx.Response, model: type[_ModelT], *, what: str) -> _ModelT:
        try:
            payload = response.json()
        except ValueError as exc:
            raise AuthServiceClientDecodeError(
                f"{what} response was not valid JSON: {exc!r}"
            ) from exc
        try:
            return model.model_validate(payload)
        except ValueError as exc:
            raise AuthServiceClientDecodeError(
                f"{what} response did not match {model.__name__}: {exc!r}"
            ) from exc


# --- Test / dev doubles ------------------------------------------------------


@dataclass(slots=True)
class NoopAuthServiceClient:
    """A permissive do-nothing client for dry runs / local dev.

    ``verify_and_authorize`` always allows; ``callctx_sign`` returns a synthetic
    unsigned placeholder token; ``get_permissions`` returns an empty registry.
    Never use in production — it performs no real authentication.
    """

    async def verify_and_authorize(
        self, request: VerifyAndAuthorizeRequest
    ) -> VerifyAndAuthorizeResponse:
        return VerifyAndAuthorizeResponse(
            principal_id="noop",
            allowed=True,
            reason="noop",
            audit_event_id="noop",
        )

    async def callctx_sign(self, request: CallctxSignRequest) -> CallctxSignResponse:
        return CallctxSignResponse(token="noop", kid="noop", jti="noop", iat=0, exp=0)

    async def get_permissions(self) -> list[DeclaredPermission]:
        return []


@dataclass(slots=True)
class FakeAuthServiceClient:
    """Recording double for tests.

    Records every call. Each method returns its canned value (or a synthetic
    default); set ``error`` to make every method raise it (to exercise the
    pipeline's retry / deny paths).
    """

    decision: VerifyAndAuthorizeResponse | None = None
    signed: CallctxSignResponse | None = None
    permissions: list[DeclaredPermission] = field(default_factory=list)
    error: AuthServiceClientError | None = None
    verify_calls: list[VerifyAndAuthorizeRequest] = field(default_factory=list)
    sign_calls: list[CallctxSignRequest] = field(default_factory=list)
    get_permissions_calls: int = 0

    async def verify_and_authorize(
        self, request: VerifyAndAuthorizeRequest
    ) -> VerifyAndAuthorizeResponse:
        self.verify_calls.append(request)
        if self.error is not None:
            raise self.error
        if self.decision is not None:
            return self.decision
        return VerifyAndAuthorizeResponse(
            principal_id="principal-fake",
            allowed=True,
            reason="allow",
            audit_event_id="evt-fake",
        )

    async def callctx_sign(self, request: CallctxSignRequest) -> CallctxSignResponse:
        self.sign_calls.append(request)
        if self.error is not None:
            raise self.error
        if self.signed is not None:
            return self.signed
        return CallctxSignResponse(token="token-fake", kid="kid-fake", jti="jti-fake", iat=1, exp=2)

    async def get_permissions(self) -> list[DeclaredPermission]:
        self.get_permissions_calls += 1
        if self.error is not None:
            raise self.error
        return list(self.permissions)
