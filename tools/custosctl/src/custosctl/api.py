"""Typed HTTP client for the Custos API Gateway (DEVCLI-IMPL-004).

The gateway fronts the Catalog and Workflow surfaces; the connector/activity/
workflow commands (#956-#958) call it through this one client. It handles the
base URL, bearer auth, the ``CUSTOS_INSECURE`` verify toggle, JSON decoding, and
maps the gateway's RFC 7807 ``application/problem+json`` error envelope onto a
typed :class:`ApiError`.

Tests inject an ``httpx.MockTransport`` via the ``transport`` argument, so no
network or extra test dependency is required.
"""

from __future__ import annotations

from types import TracebackType
from typing import Any

import httpx

from custosctl.config import Settings

_DEFAULT_TIMEOUT = 30.0


class ApiError(RuntimeError):
    """A non-2xx (or transport) failure from the gateway.

    Carries the parsed RFC 7807 fields when the body is ``problem+json``.
    ``status_code == 0`` denotes a transport-level failure (connection refused,
    DNS, TLS) rather than an HTTP response.
    """

    def __init__(
        self,
        *,
        status_code: int,
        title: str | None = None,
        detail: str | None = None,
        type: str | None = None,
        code: str | None = None,
    ) -> None:
        self.status_code = status_code
        self.title = title
        self.detail = detail
        self.type = type
        self.code = code
        message = detail or title or code or f"HTTP {status_code}"
        prefix = "API request failed" if status_code == 0 else f"API error {status_code}"
        super().__init__(f"{prefix}: {message}")

    @classmethod
    def from_response(cls, response: httpx.Response) -> ApiError:
        title = detail = type_ = code = None
        content_type = response.headers.get("content-type", "")
        if "json" in content_type:
            try:
                body = response.json()
            except ValueError:
                body = None
            if isinstance(body, dict):
                type_ = _as_str(body.get("type"))
                title = _as_str(body.get("title"))
                detail = _as_str(body.get("detail"))
                if type_ and "/" in type_:
                    code = type_.rsplit("/", 1)[-1]
        return cls(
            status_code=response.status_code,
            title=title,
            detail=detail,
            type=type_,
            code=code,
        )


def _as_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


class ApiClient:
    """A thin, typed wrapper over ``httpx.Client`` for gateway calls."""

    def __init__(
        self,
        *,
        base_url: str,
        token: str,
        verify: bool = True,
        timeout: float = _DEFAULT_TIMEOUT,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._client = httpx.Client(
            base_url=base_url.rstrip("/"),
            verify=verify,
            timeout=timeout,
            transport=transport,
            headers={"authorization": f"Bearer {token}"},
        )

    def __enter__(self) -> ApiClient:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()

    def request(
        self,
        method: str,
        path: str,
        *,
        json: Any = None,
        params: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> Any:
        """Send a request; return the decoded JSON body (or ``None``).

        Raises :class:`ApiError` on any non-2xx response or transport failure.
        """
        headers: dict[str, str] = {}
        if idempotency_key is not None:
            headers["idempotency-key"] = idempotency_key
        try:
            response = self._client.request(
                method,
                path,
                json=json,
                params=params,
                headers=headers or None,
            )
        except httpx.RequestError as exc:
            raise ApiError(status_code=0, detail=str(exc)) from exc
        if not response.is_success:
            raise ApiError.from_response(response)
        return _decode(response)

    def get(self, path: str, *, params: dict[str, Any] | None = None) -> Any:
        return self.request("GET", path, params=params)

    def post(
        self,
        path: str,
        *,
        json: Any = None,
        idempotency_key: str | None = None,
    ) -> Any:
        return self.request("POST", path, json=json, idempotency_key=idempotency_key)


def _decode(response: httpx.Response) -> Any:
    if response.status_code == 204 or not response.content:
        return None
    if "json" in response.headers.get("content-type", ""):
        return response.json()
    return None


def build_client(settings: Settings, *, transport: httpx.BaseTransport | None = None) -> ApiClient:
    """Construct an :class:`ApiClient` from :class:`Settings`.

    Raises :class:`RuntimeError` when the gateway URL or token is missing —
    the API-driven commands surface this as an actionable CLI error.
    """
    if not settings.gateway:
        raise RuntimeError("CUSTOS_GATEWAY is required for API commands (the gateway base URL)")
    if settings.token is None:
        raise RuntimeError("CUSTOS_TOKEN is required for API commands (a platform service token)")
    return ApiClient(
        base_url=settings.gateway,
        token=settings.token.get_secret_value(),
        verify=not settings.insecure,
        transport=transport,
    )


__all__ = ["ApiClient", "ApiError", "build_client"]
