"""Resolve an operator-owned credential reference and exchange it for an ephemeral call context."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from collections.abc import Mapping
from urllib.parse import quote, urlsplit

import httpx

from custos_callctx.execution import (
    EXECUTION_PERMISSIONS,
    ExecutionBindings,
    ExecutionIdentity,
    ExecutionOperation,
)

logger = logging.getLogger(__name__)
MAX_EXECUTION_RESPONSE_BYTES = 32 * 1024


class ExecutionAuthorizationError(RuntimeError):
    """Safe execution failure with no credential-bearing transport or decoder exception attached."""

    def __init__(self, *, unavailable: bool) -> None:
        """Distinguish unavailable dependencies from denied/invalid execution authority."""
        self.unavailable = unavailable
        super().__init__(
            "execution authorization is unavailable"
            if unavailable
            else "execution is not authorized"
        )


class ExecutionContextClient:
    """Borrow one HTTP client; acquire fresh credentials/contexts only at an actual I/O boundary."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        bindings: ExecutionBindings,
        *,
        sidecar_url: str = "http://127.0.0.1:3500",
        auth_app_id: str = "auth-service",
        secret_store: str = "custos-secretstore",
        api_token: str | None = None,
    ) -> None:
        """Validate deployment-owned destinations; event data never chooses an origin or secret."""
        parsed = urlsplit(sidecar_url)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("execution sidecar URL must be an origin without credentials")
        for value in (auth_app_id, secret_store):
            if not value.strip() or value in {".", ".."} or any(char in value for char in "/\\"):
                raise ValueError("execution transport resource names must be explicit")
        if api_token is not None and (
            not api_token or any(not 33 <= ord(char) <= 126 for char in api_token)
        ):
            raise ValueError("configured Dapr API token must be a nonblank HTTP-safe value")
        self.client, self.bindings = client, bindings
        origin = sidecar_url.rstrip("/")
        self._secret_base = origin + "/v1.0/secrets/" + quote(secret_store, safe="") + "/"
        self._exchange = (
            origin
            + "/v1.0/invoke/"
            + quote(auth_app_id, safe="")
            + "/method/rpc/callctx.exchangeExecution"
        )
        self._api_headers = {"dapr-api-token": api_token} if api_token is not None else {}
        self._sidecar_url, self._auth_app_id, self._secret_store = origin, auth_app_id, secret_store

    @classmethod
    def from_env(
        cls, client: httpx.AsyncClient, bindings: ExecutionBindings, env: Mapping[str, str]
    ) -> ExecutionContextClient:
        """Share explicit deployment addressing without taking ownership of the borrowed client."""
        port = int(env.get("DAPR_HTTP_PORT", "3500"))
        if not 1 <= port <= 65535:
            raise ValueError("DAPR_HTTP_PORT must be in 1..65535")
        host = env.get("DAPR_HTTP_HOST", "127.0.0.1")
        sidecar = str(httpx.URL(scheme="http", host=host.strip("[]"), port=port))
        return cls(
            client,
            bindings,
            sidecar_url=sidecar,
            auth_app_id=env.get("CUSTOS_EXECUTION_AUTH_APP_ID", "auth-service"),
            secret_store=env.get("CUSTOS_EXECUTION_SECRET_STORE", "custos-secretstore"),
            api_token=env.get("DAPR_API_TOKEN"),
        )

    async def _json(
        self, method: str, url: str, *, payload: dict[str, str] | None = None
    ) -> dict[str, object]:
        """Bound response bytes and reject redirects so credentials never change destination."""
        async with self.client.stream(
            method,
            url,
            json=payload,
            headers=self._api_headers,
            follow_redirects=False,
            timeout=15,
        ) as response:
            if response.status_code != 200:
                raise ExecutionAuthorizationError(
                    unavailable=response.status_code >= 500 or response.status_code == 429
                )
            raw = bytearray()
            async for chunk in response.aiter_bytes():
                if len(raw) + len(chunk) > MAX_EXECUTION_RESPONSE_BYTES:
                    raise ValueError("execution response exceeds its bound")
                raw.extend(chunk)
            value: object = json.loads(raw)
            if not isinstance(value, dict):
                raise ValueError("execution response must be an object")
            return value

    async def headers(
        self, identity: ExecutionIdentity, operation: ExecutionOperation
    ) -> dict[str, str]:
        """Return transient headers, never values to serialize into workflow state."""
        try:
            if operation not in EXECUTION_PERMISSIONS:
                raise ValueError("unsupported execution operation")
            binding = self.bindings.resolve(identity)
            async with asyncio.timeout(30):
                secret = await self._json(
                    "GET", self._secret_base + quote(binding.secret_ref, safe="")
                )
                credential = secret.get(binding.secret_key)
                if not isinstance(credential, str) or not 1 <= len(credential) <= 4096:
                    raise ValueError("execution credential is missing or invalid")
                signed = await self._json(
                    "POST",
                    self._exchange,
                    payload={
                        "token": credential,
                        "workspaceId": identity.workspace_id,
                        "principalId": identity.principal_id,
                        "operation": operation,
                    },
                )
                token, issued, expires = signed.get("token"), signed.get("iat"), signed.get("exp")
                if (
                    not isinstance(token, str)
                    or not 1 <= len(token) <= 8192
                    or re.fullmatch(r"[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+", token)
                    is None
                    or type(issued) is not int
                    or type(expires) is not int
                    or not 0 < expires - issued <= 300
                    or expires <= time.time()
                    or issued > time.time() + 60
                ):
                    raise ValueError("execution context response is invalid or expired")
                return {**self._api_headers, "x-custos-callctx": token}
        except ExecutionAuthorizationError as exc:
            logger.warning(
                "Execution authorization failed: %s", "unavailable" if exc.unavailable else "denied"
            )
            raise
        except (httpx.HTTPError, TimeoutError, OSError):
            logger.warning("Execution authorization dependency is unavailable")
            raise ExecutionAuthorizationError(unavailable=True) from None
        except (ValueError, TypeError, RecursionError):
            logger.warning("Execution identity or response is invalid")
            raise ExecutionAuthorizationError(unavailable=False) from None
