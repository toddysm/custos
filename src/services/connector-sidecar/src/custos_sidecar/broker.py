"""Production sidecar: bootstrap-authenticated UDS backed by the real per-attempt credential broker.

The private runtime bundle is mounted only in the trusted sidecar. No general
service credential or signed control context is installed in the activity pod.
Version-two responses preserve opaque provider material and unknown expiry.
"""

from __future__ import annotations

import logging
import os
import ssl
import stat
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated
from urllib.parse import urlsplit

import httpx
from custos_callctx.private_transport import PrivateTransportError, private_post
from fastapi import Depends, FastAPI, Request
from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator
from starlette.responses import JSONResponse, Response

from custos_sidecar.auth import BootstrapTokenVerifier, BoundTriple
from custos_sidecar.errors import SidecarError, problem_response
from custos_sidecar.router import _RefreshBody, _verify_bootstrap

logger = logging.getLogger(__name__)


class RuntimeBundle(BaseModel):
    """Closed private attempt addressing/proof, loaded before the UDS listener opens."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)
    workspace_id: str = Field(alias="workspaceId", min_length=1, max_length=120)
    run_id: str = Field(alias="runId", min_length=1, max_length=4096)
    step_id: str = Field(alias="stepId", min_length=1, max_length=4096)
    attempt: int = Field(ge=1)
    endpoint: str
    proof: SecretStr = Field(min_length=40, max_length=128, repr=False)

    @field_validator("endpoint")
    @classmethod
    def private_https_origin(cls, value: str) -> str:
        """Do not transmit an attempt proof over plaintext or a caller-selected URL path."""
        url = urlsplit(value)
        if (
            url.scheme != "https"
            or not url.hostname
            or url.username
            or url.password
            or url.path not in {"", "/"}
            or url.query
            or url.fragment
        ):
            raise ValueError("credential proxy must be a private HTTPS origin")
        return value.rstrip("/")


def read_private_file(path: Path, *, limit: int) -> bytes:
    """Require a bounded regular owner-only file and reject symlink replacement."""
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "rb") as stream:
        metadata = os.fstat(stream.fileno())
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_mode & 0o077
            or metadata.st_uid != os.getuid()
            or metadata.st_size > limit
        ):
            raise ValueError("unsafe sidecar private file")
        value = stream.read(limit + 1)
    if not value or len(value) > limit:
        raise ValueError("invalid sidecar private file size")
    return value


def load_runtime_bundle(path: Path) -> tuple[RuntimeBundle, BootstrapTokenVerifier]:
    """Read the injected private bundle/key; never fall back to a synthetic credential minter."""
    bundle = RuntimeBundle.model_validate_json(read_private_file(path, limit=16 * 1024))
    key = read_private_file(path.parent / "sidecar-key", limit=4096)
    if len(key) < 32:
        raise ValueError("sidecar bootstrap verification key is too short")
    verifier = BootstrapTokenVerifier(
        key=key,
        triple=BoundTriple(bundle.run_id, bundle.step_id, bundle.attempt),
    )
    return bundle, verifier


def create_broker_app(
    bundle: RuntimeBundle,
    verifier: BootstrapTokenVerifier,
    *,
    http_client: httpx.AsyncClient | None = None,
) -> FastAPI:
    """Own one HTTPS client on the serving loop, or borrow an explicit test transport."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        """Release only owned resources before the server loop closes."""
        if http_client is not None:
            app.state.http = http_client
            yield
        else:
            trust = ssl.create_default_context(cafile=os.environ.get("CUSTOS_SIDECAR_CA_BUNDLE"))
            async with httpx.AsyncClient(trust_env=False, verify=trust) as owned:
                app.state.http = owned
                yield

    app = FastAPI(lifespan=lifespan, openapi_url=None, docs_url=None, redoc_url=None)
    app.state.bootstrap_verifier = verifier

    @app.exception_handler(SidecarError)
    async def rejected(request: Request, exc: SidecarError) -> Response:
        """Preserve the established bootstrap authentication failure envelope."""
        return problem_response(exc, instance=request.url.path)

    async def forward(operation: str, **fields: object) -> JSONResponse:
        """Forward only exact immutable attempt coordinates and the narrowly scoped operation."""
        payload = {
            "workspaceId": bundle.workspace_id,
            "runId": bundle.run_id,
            "stepId": bundle.step_id,
            "attempt": bundle.attempt,
            "operation": operation,
            **fields,
        }
        try:
            result = await private_post(
                app.state.http,
                bundle.endpoint + "/internal/v1/activity-credentials",
                payload,
                {"Authorization": "Bearer " + bundle.proof.get_secret_value()},
            )
        except PrivateTransportError as exc:
            logger.warning("Activity credential broker request failed")
            return JSONResponse(
                {
                    "error": {
                        "code": "credential_unavailable",
                        "detail": "credential request failed",
                    }
                },
                status_code=exc.status,
            )
        lease = result.get("lease")
        valid = (
            result == {"released": True}
            if operation == "release"
            else (
                result.get("schemaVersion") == "2"
                and isinstance(result.get("credential"), dict)
                and isinstance(lease, dict)
                and (
                    lease.get("workspaceId"),
                    lease.get("runId"),
                    lease.get("stepId"),
                    lease.get("attempt"),
                )
                == (bundle.workspace_id, bundle.run_id, bundle.step_id, bundle.attempt)
                and type(lease.get("attempt")) is int
                and isinstance(lease.get("leaseId"), str)
                and bool(lease.get("leaseId"))
                and (
                    (lease.get("slot"), lease.get("capability"))
                    == (fields.get("slot"), fields.get("capability"))
                    if operation == "issue"
                    else lease.get("leaseId") == fields.get("leaseId")
                )
            )
        )
        if not valid:
            logger.error("Activity credential broker returned an invalid envelope")
            return JSONResponse({"error": {"code": "invalid_credential_response"}}, status_code=502)
        return JSONResponse(result, headers={"Cache-Control": "no-store"})

    @app.get("/v1/token")
    async def token(
        _: Annotated[None, Depends(_verify_bootstrap)],
        slot: str,
        purpose: str,
    ) -> JSONResponse:
        """Return the broker's exact material and separate lease/provider expiry metadata."""
        return await forward("issue", slot=slot, capability=purpose)

    @app.post("/v1/token/refresh")
    async def refresh(
        body: _RefreshBody, _: Annotated[None, Depends(_verify_bootstrap)]
    ) -> JSONResponse:
        """Refresh only the original lease authority; no new slot or capability is accepted."""
        return await forward("refresh", leaseId=body.lease_id)

    @app.post("/v1/token/release")
    async def release(
        body: _RefreshBody, _: Annotated[None, Depends(_verify_bootstrap)]
    ) -> JSONResponse:
        """Release through the authoritative broker rather than assuming local revocation."""
        return await forward("release", leaseId=body.lease_id)

    @app.get("/healthz")
    async def health() -> dict[str, str]:
        """Only report live after private bootstrap/configuration has been validated."""
        return {"status": "ok"}

    return app
