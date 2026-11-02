"""Plugin Runtime adapter (CONN-IMPL-014).

This module defines the connector-plugin hook contract that future
phases (activation, binding, listen loops) call through:

* ``bind()``   -> return a :class:`ConnectorContext`
* ``listen()`` -> return normalized events + next cursor / receiver endpoint
* ``health()`` -> return a synchronous probe result

The transport is intentionally split in two layers:

* :class:`HookRunner` owns *how* a plugin image is invoked.
* :class:`PluginInvoker` owns the JSON wire contract and maps structured
  plugin failures onto typed Python exceptions.

Development uses :class:`DockerCliHookRunner`; production wiring selects the
Kubernetes runner behind the same protocol. Both exchange one private JSON
request/response without promoting plugin diagnostics into errors or audits.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Final, Protocol, TypeVar
from uuid import uuid4

from custos_spl.interfaces.catalog_store import ConnectorTypeVersion
from custos_spl.interfaces.connector_instance_store import ConnectorInstance

DEFAULT_HOOK_TIMEOUT_SECONDS: Final[float] = 30.0
_WIRE_API_VERSION: Final[int] = 1


def _freeze_mapping(value: Mapping[str, Any] | None) -> Mapping[str, Any]:
    if value is None:
        return MappingProxyType({})
    return MappingProxyType(dict(value))


class ListenMode(StrEnum):
    PULL = "pull"
    PUSH = "push"


class PluginErrorCode(StrEnum):
    CURSOR_EXPIRED = "cursor-expired"
    CURSOR_ENCODING_MISMATCH = "cursor-encoding-mismatch"
    UPSTREAM_UNAUTHORIZED = "upstream-unauthorized"
    UPSTREAM_UNREACHABLE = "upstream-unreachable"
    HOOK_TIMEOUT = "hook-timeout"
    INVOCATION_FAILED = "invocation-failed"
    INVALID_RESPONSE = "invalid-response"
    UNKNOWN_PLUGIN_ERROR = "unknown-plugin-error"


@dataclass(frozen=True, slots=True)
class CursorEnvelope:
    encoding: str
    value: Any
    advanced_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class ConnectorContext:
    endpoint: str
    token_type_hint: str | None
    handle: Mapping[str, Any]
    extras: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class ListenResult:
    events: tuple[Mapping[str, Any], ...]
    next_cursor: CursorEnvelope | None
    receiver_endpoint: str | None


@dataclass(frozen=True, slots=True)
class HealthResult:
    healthy: bool
    detail: str | None
    checked_at: datetime
    extras: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class HookRunResult:
    exit_code: int
    stdout: bytes
    stderr: bytes


class PluginRuntimeError(Exception):
    code: PluginErrorCode = PluginErrorCode.UNKNOWN_PLUGIN_ERROR

    def __init__(self, detail: str, *, data: Mapping[str, Any] | None = None) -> None:
        self.detail = detail
        self.data = _freeze_mapping(data)
        super().__init__(detail)


class CursorExpired(PluginRuntimeError):
    code = PluginErrorCode.CURSOR_EXPIRED


class CursorEncodingMismatch(PluginRuntimeError):
    code = PluginErrorCode.CURSOR_ENCODING_MISMATCH

    def __init__(
        self,
        detail: str,
        *,
        persisted_encoding: str | None,
        plugin_encoding: str | None,
        data: Mapping[str, Any] | None = None,
    ) -> None:
        self.persisted_encoding = persisted_encoding
        self.plugin_encoding = plugin_encoding
        super().__init__(detail, data=data)


class UpstreamUnauthorized(PluginRuntimeError):
    code = PluginErrorCode.UPSTREAM_UNAUTHORIZED


class UpstreamUnreachable(PluginRuntimeError):
    code = PluginErrorCode.UPSTREAM_UNREACHABLE


class PluginHookTimeout(PluginRuntimeError):
    code = PluginErrorCode.HOOK_TIMEOUT


class PluginInvocationFailed(PluginRuntimeError):
    code = PluginErrorCode.INVOCATION_FAILED


class PluginCleanupFailed(PluginInvocationFailed):
    """Owned termination or execution outcome is unconfirmed; polling must fail closed."""

    def __init__(self, container_name: str, *, termination_confirmed: bool = False) -> None:
        """Retain only a generated resource name, never plugin output or credentials."""
        self.container_name = container_name
        self.termination_confirmed = termination_confirmed
        self.cancellation_requested = False
        super().__init__(
            "plugin invocation cleanup or outcome could not be confirmed",
            data={
                "containerName": container_name,
                **({"terminationConfirmed": True} if termination_confirmed else {}),
            },
        )


DOCKER_CLEANUP_TIMEOUT_SECONDS = 5
_TaskResult = TypeVar("_TaskResult")


async def _finish_owned_task(
    task: asyncio.Future[_TaskResult], container_name: str
) -> tuple[_TaskResult, bool]:
    """Wait through caller cancellation; cancelled owned tasks cannot confirm termination."""
    interrupted = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            interrupted = True
    try:
        return task.result(), interrupted
    except asyncio.CancelledError as exc:
        failure = PluginCleanupFailed(container_name)
        caller = asyncio.current_task()
        failure.cancellation_requested = caller is not None and caller.cancelling() > 0
        raise failure from exc


class PluginProtocolError(PluginRuntimeError):
    code = PluginErrorCode.INVALID_RESPONSE


class HookRunner(Protocol):
    async def run(
        self,
        *,
        image_ref: str,
        hook: str,
        payload: Mapping[str, Any],
        timeout_seconds: float,
        invocation_id: str | None = None,
    ) -> HookRunResult:
        """Return or raise only after termination, or raise PluginCleanupFailed if unconfirmed."""
        ...


def invocation_container_name(invocation_id: str) -> str:
    """Derive an operator-visible locator without exposing the opaque fencing token."""
    if not invocation_id or invocation_id.startswith("legacy:"):
        raise ValueError("a current invocation identity is required")
    return "custos-hook-" + hashlib.sha256(invocation_id.encode("utf-8")).hexdigest()[:32]


@dataclass(frozen=True, slots=True)
class DockerCliHookRunner:
    """Invoke plugin images through the local Docker CLI.

    The plugin image is expected to treat the first argv token after the
    image reference as the hook name and to exchange a single JSON document
    over stdin/stdout. A generated container name binds cleanup to this one
    invocation. Cancellation never detaches cleanup from the calling operation.
    """

    binary: str = "docker"
    extra_run_args: Sequence[str] = ()

    def __post_init__(self) -> None:
        """Snapshot trusted run options without permitting a container-ownership override."""
        if isinstance(self.extra_run_args, (str, bytes)):
            raise ValueError("Docker run options must be a sequence of arguments")
        arguments = tuple(self.extra_run_args)
        if any(
            not isinstance(argument, str)
            or argument in ("--", "--name")
            or argument.startswith("--name=")
            for argument in arguments
        ):
            raise ValueError("Docker run options cannot override owned container naming")
        object.__setattr__(self, "extra_run_args", arguments)

    async def run(
        self,
        *,
        image_ref: str,
        hook: str,
        payload: Mapping[str, Any],
        timeout_seconds: float,
        invocation_id: str | None = None,
    ) -> HookRunResult:
        """Own both the CLI process and named container until confirmed cleanup."""
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("plugin timeout must be positive and finite")
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        container_name = (
            invocation_container_name(invocation_id)
            if invocation_id is not None
            else "custos-hook-" + uuid4().hex
        )
        spawning = asyncio.create_task(
            asyncio.create_subprocess_exec(
                self.binary,
                "run",
                "--rm",
                "-i",
                "--name",
                container_name,
                *self.extra_run_args,
                image_ref,
                hook,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        )
        try:
            proc = await asyncio.shield(spawning)
        except asyncio.CancelledError as cancelled:
            try:
                proc, _ = await _finish_owned_task(spawning, container_name)
            except OSError:
                raise cancelled from None
            await self._finish_invocation(proc, container_name)
            raise
        communication = asyncio.create_task(proc.communicate(body))
        try:
            stdout, stderr = await asyncio.wait_for(asyncio.shield(communication), timeout_seconds)
        except TimeoutError as exc:
            raise TimeoutError(f"plugin hook {hook!r} timed out after {timeout_seconds}s") from exc
        finally:
            await self._finish_invocation(proc, container_name, communication)
        if proc.returncode is None:
            raise PluginInvocationFailed("plugin process did not finish")
        return HookRunResult(exit_code=proc.returncode, stdout=stdout, stderr=stderr)

    async def _finish_invocation(
        self,
        proc: asyncio.subprocess.Process,
        container_name: str,
        communication: asyncio.Task[tuple[bytes, bytes]] | None = None,
    ) -> None:
        """Do not propagate cancellation until the CLI and its container are accounted for."""
        cleanup = asyncio.create_task(self._cleanup_invocation(proc, container_name, communication))
        try:
            _, interrupted = await _finish_owned_task(cleanup, container_name)
        except (PluginCleanupFailed, OSError) as exc:
            failure = (
                exc if isinstance(exc, PluginCleanupFailed) else PluginCleanupFailed(container_name)
            )
            caller = asyncio.current_task()
            failure.cancellation_requested = caller is not None and caller.cancelling() > 0
            if failure is exc:
                raise
            raise failure from exc
        if interrupted:
            raise asyncio.CancelledError()

    async def _cleanup_invocation(
        self,
        proc: asyncio.subprocess.Process,
        container_name: str,
        communication: asyncio.Task[tuple[bytes, bytes]] | None,
    ) -> None:
        """Stop the owned CLI before force-removing its unique container, with bounded cleanup."""
        cleanup: asyncio.subprocess.Process | None = None
        try:
            async with asyncio.timeout(DOCKER_CLEANUP_TIMEOUT_SECONDS):
                if proc.returncode is None:
                    with suppress(ProcessLookupError):
                        proc.kill()
                    await proc.wait()
                cleanup = await asyncio.create_subprocess_exec(
                    self.binary,
                    "rm",
                    "--force",
                    container_name,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                _, stderr = await cleanup.communicate()
                absent = (
                    b"no such container" in stderr.lower() and container_name.encode() in stderr
                )
                if cleanup.returncode != 0 and not absent:
                    raise PluginCleanupFailed(container_name)
        except (OSError, TimeoutError) as exc:
            raise PluginCleanupFailed(container_name) from exc
        finally:
            if cleanup is not None and cleanup.returncode is None:
                with suppress(ProcessLookupError):
                    cleanup.kill()
                await cleanup.wait()
            if communication is not None:
                if not communication.done():
                    communication.cancel()
                await asyncio.gather(communication, return_exceptions=True)


class PluginInvoker:
    def __init__(
        self,
        runner: HookRunner,
        *,
        timeout_seconds: float = DEFAULT_HOOK_TIMEOUT_SECONDS,
    ) -> None:
        self._runner = runner
        self._timeout_seconds = timeout_seconds

    async def bind(
        self,
        *,
        connector: ConnectorTypeVersion,
        instance: ConnectorInstance,
        slot: str,
        capability: str,
        identity_material: Mapping[str, Any],
    ) -> ConnectorContext:
        result = await self._invoke(
            connector=connector,
            instance=instance,
            hook="bind",
            hook_input={
                "slot": slot,
                "capability": capability,
                "identityMaterial": dict(identity_material),
            },
        )
        endpoint = result.get("endpoint")
        if not isinstance(endpoint, str) or not endpoint:
            raise PluginProtocolError("bind result must carry a non-empty string endpoint")
        token_type_hint = result.get("tokenTypeHint")
        if token_type_hint is not None and not isinstance(token_type_hint, str):
            raise PluginProtocolError("bind result tokenTypeHint must be a string when present")
        handle = result.get("handle")
        extras = result.get("extras")
        if handle is not None and not isinstance(handle, Mapping):
            raise PluginProtocolError("bind result handle must be an object when present")
        if extras is not None and not isinstance(extras, Mapping):
            raise PluginProtocolError("bind result extras must be an object when present")
        return ConnectorContext(
            endpoint=endpoint,
            token_type_hint=token_type_hint,
            handle=_freeze_mapping(handle if isinstance(handle, Mapping) else None),
            extras=_freeze_mapping(extras if isinstance(extras, Mapping) else None),
        )

    async def listen(
        self,
        *,
        connector: ConnectorTypeVersion,
        instance: ConnectorInstance,
        mode: ListenMode,
        cursor: CursorEnvelope | None,
        timeout_seconds: float | None = None,
        invocation_id: str | None = None,
    ) -> ListenResult:
        """Return a usable response or preserve recovery for a fenced, unprocessable result."""
        try:
            result = await self._invoke(
                connector=connector,
                instance=instance,
                hook="listen",
                hook_input={
                    "mode": str(mode),
                    "cursor": _serialize_cursor(cursor),
                },
                timeout_seconds=timeout_seconds,
                invocation_id=invocation_id,
            )
            return self._parse_listen_result(result)
        except PluginProtocolError:
            if invocation_id is not None:
                raise PluginCleanupFailed(
                    invocation_container_name(invocation_id),
                    termination_confirmed=True,
                ) from None
            raise

    @staticmethod
    def _parse_listen_result(result: Mapping[str, Any]) -> ListenResult:
        """Validate the full listen result before its caller can publish or advance a cursor."""
        raw_events = result.get("events", ())
        if not isinstance(raw_events, list):
            raise PluginProtocolError("listen result events must be a JSON array")
        events: list[Mapping[str, Any]] = []
        for entry in raw_events:
            if not isinstance(entry, Mapping):
                raise PluginProtocolError("listen result events entries must be JSON objects")
            events.append(_freeze_mapping(entry))
        receiver_endpoint = result.get("receiverEndpoint")
        if receiver_endpoint is not None and not isinstance(receiver_endpoint, str):
            raise PluginProtocolError(
                "listen result receiverEndpoint must be a string when present"
            )
        next_cursor = result.get("nextCursor")
        if next_cursor is not None and not isinstance(next_cursor, Mapping):
            raise PluginProtocolError("listen result nextCursor must be an object when present")
        return ListenResult(
            events=tuple(events),
            next_cursor=_parse_cursor(next_cursor) if isinstance(next_cursor, Mapping) else None,
            receiver_endpoint=receiver_endpoint,
        )

    async def health(
        self,
        *,
        connector: ConnectorTypeVersion,
        instance: ConnectorInstance,
    ) -> HealthResult:
        result = await self._invoke(
            connector=connector,
            instance=instance,
            hook="health",
            hook_input={},
        )
        healthy = result.get("healthy")
        if not isinstance(healthy, bool):
            raise PluginProtocolError("health result healthy must be a boolean")
        detail = result.get("detail")
        if detail is not None and not isinstance(detail, str):
            raise PluginProtocolError("health result detail must be a string when present")
        checked_at_raw = result.get("checkedAt")
        if checked_at_raw is None:
            checked_at = datetime.now(UTC)
        elif isinstance(checked_at_raw, str):
            try:
                checked_at = datetime.fromisoformat(checked_at_raw.replace("Z", "+00:00"))
            except ValueError:
                raise PluginProtocolError(
                    "health result checkedAt must be an RFC3339 string"
                ) from None
            if checked_at.tzinfo is None:
                checked_at = checked_at.replace(tzinfo=UTC)
        else:
            raise PluginProtocolError("health result checkedAt must be an RFC3339 string")
        extras = result.get("extras")
        if extras is not None and not isinstance(extras, Mapping):
            raise PluginProtocolError("health result extras must be an object when present")
        return HealthResult(
            healthy=healthy,
            detail=detail,
            checked_at=checked_at,
            extras=_freeze_mapping(extras if isinstance(extras, Mapping) else None),
        )

    async def _invoke(
        self,
        *,
        connector: ConnectorTypeVersion,
        instance: ConnectorInstance,
        hook: str,
        hook_input: Mapping[str, Any],
        timeout_seconds: float | None = None,
        invocation_id: str | None = None,
    ) -> Mapping[str, Any]:
        """Bound execution without promoting plugin diagnostics into errors or audits."""
        timeout = self._timeout_seconds
        if timeout_seconds is not None:
            if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
                raise ValueError("plugin timeout override must be positive and finite")
            timeout = min(timeout, timeout_seconds)
        payload = {
            "apiVersion": _WIRE_API_VERSION,
            "hook": hook,
            "connector": {
                "type": connector.type,
                "version": connector.version,
                "imageRef": connector.image_ref,
                "digest": connector.digest,
                "manifest": dict(connector.normalized_manifest),
            },
            "instance": {
                "workspaceId": str(instance.workspace_id),
                "instanceId": str(instance.instance_id),
                "type": instance.type,
                "version": instance.version,
                "name": instance.name,
                "enabled": instance.enabled,
                "status": instance.status,
                "healthStatus": instance.health_status,
                "leaseTtlSeconds": instance.lease_ttl_seconds,
                "targetConfig": dict(instance.target_config),
                "credentialsAuthentication": dict(instance.credentials_authentication),
                "usedCapabilities": list(instance.used_capabilities or ()),
            },
            "input": dict(hook_input),
        }
        try:
            if invocation_id is None:
                completed = await self._runner.run(
                    image_ref=connector.image_ref,
                    hook=hook,
                    payload=payload,
                    timeout_seconds=timeout,
                )
            else:
                completed = await self._runner.run(
                    image_ref=connector.image_ref,
                    hook=hook,
                    payload=payload,
                    timeout_seconds=timeout,
                    invocation_id=invocation_id,
                )
        except PluginRuntimeError as exc:
            raise exc from None
        except TimeoutError:
            raise PluginHookTimeout("plugin hook timed out", data={"hook": hook}) from None
        except OSError:
            raise PluginInvocationFailed(
                "failed to start plugin image",
                data={"hook": hook, "image_ref": connector.image_ref},
            ) from None

        try:
            body = self._decode_body(completed.stdout, hook=hook, image_ref=connector.image_ref)
        except PluginProtocolError as exc:
            if completed.exit_code != 0:
                if invocation_id is not None:
                    raise PluginCleanupFailed(
                        invocation_container_name(invocation_id),
                        termination_confirmed=True,
                    ) from None
                raise PluginInvocationFailed(
                    (
                        f"plugin hook {hook!r} on {connector.image_ref!r} exited with "
                        f"status {completed.exit_code} without a valid response"
                    ),
                    data={
                        "hook": hook,
                        "image_ref": connector.image_ref,
                        "exit_code": completed.exit_code,
                    },
                ) from exc
            raise
        ok = body.get("ok")
        if not isinstance(ok, bool):
            raise PluginProtocolError("plugin response must carry boolean field 'ok'")
        if ok:
            result = body.get("result")
            if not isinstance(result, Mapping):
                raise PluginProtocolError("successful plugin response must carry object result")
            if completed.exit_code != 0:
                if invocation_id is not None:
                    raise PluginCleanupFailed(
                        invocation_container_name(invocation_id),
                        termination_confirmed=True,
                    )
                raise PluginInvocationFailed(
                    (
                        f"plugin hook {hook!r} exited with status {completed.exit_code} "
                        "despite ok=true"
                    ),
                    data={
                        "hook": hook,
                        "image_ref": connector.image_ref,
                    },
                )
            return _freeze_mapping(result)

        error = body.get("error")
        if not isinstance(error, Mapping):
            raise PluginProtocolError("failed plugin response must carry object error")
        self._raise_plugin_error(error)
        raise AssertionError("unreachable")

    @staticmethod
    def _decode_body(stdout: bytes, *, hook: str, image_ref: str) -> Mapping[str, Any]:
        """Decode private output without including malformed bytes in exception chains."""
        if not stdout:
            raise PluginProtocolError(
                f"plugin hook {hook!r} on {image_ref!r} returned empty stdout"
            )
        try:
            decoded = json.loads(stdout)
        except (ValueError, RecursionError):
            raise PluginProtocolError(
                f"plugin hook {hook!r} on {image_ref!r} returned invalid JSON"
            ) from None
        if not isinstance(decoded, Mapping):
            raise PluginProtocolError("plugin response root must be a JSON object")
        return decoded

    @staticmethod
    def _raise_plugin_error(error: Mapping[str, Any]) -> None:
        """Retain closed error categories, never untrusted detail/data in controller diagnostics."""
        raw_code = error.get("code")
        detail = error.get("detail")
        data = error.get("data")
        if not isinstance(raw_code, str):
            raise PluginProtocolError("plugin error.code must be a string")
        if not isinstance(detail, str):
            raise PluginProtocolError("plugin error.detail must be a string")
        if data is not None and not isinstance(data, Mapping):
            raise PluginProtocolError("plugin error.data must be an object when present")

        detail = "plugin hook reported a failure"
        match raw_code:
            case PluginErrorCode.CURSOR_EXPIRED:
                raise CursorExpired(detail)
            case PluginErrorCode.CURSOR_ENCODING_MISMATCH:
                raise CursorEncodingMismatch(
                    detail,
                    persisted_encoding=None,
                    plugin_encoding=None,
                )
            case PluginErrorCode.UPSTREAM_UNAUTHORIZED:
                raise UpstreamUnauthorized(detail)
            case PluginErrorCode.UPSTREAM_UNREACHABLE:
                raise UpstreamUnreachable(detail)
            case PluginErrorCode.HOOK_TIMEOUT:
                raise PluginHookTimeout(detail)
            case PluginErrorCode.INVOCATION_FAILED:
                raise PluginInvocationFailed(detail)
            case _:
                raise PluginRuntimeError(detail)


def _serialize_cursor(cursor: CursorEnvelope | None) -> Mapping[str, Any] | None:
    if cursor is None:
        return None
    payload: dict[str, Any] = {
        "encoding": cursor.encoding,
        "value": cursor.value,
    }
    if cursor.advanced_at is not None:
        payload["advancedAt"] = (
            cursor.advanced_at.astimezone(UTC).isoformat().replace("+00:00", "Z")
        )
    return payload


def _parse_cursor(payload: Mapping[str, Any]) -> CursorEnvelope:
    encoding = payload.get("encoding")
    if not isinstance(encoding, str) or not encoding:
        raise PluginProtocolError("cursor envelope must carry a non-empty string encoding")
    advanced_at_raw = payload.get("advancedAt")
    advanced_at: datetime | None = None
    if advanced_at_raw is not None:
        if not isinstance(advanced_at_raw, str):
            raise PluginProtocolError("cursor advancedAt must be an RFC3339 string when present")
        try:
            advanced_at = datetime.fromisoformat(advanced_at_raw.replace("Z", "+00:00"))
        except ValueError:
            raise PluginProtocolError(
                "cursor advancedAt must be an RFC3339 string when present"
            ) from None
        if advanced_at.tzinfo is None:
            advanced_at = advanced_at.replace(tzinfo=UTC)
    return CursorEnvelope(
        encoding=encoding,
        value=payload.get("value"),
        advanced_at=advanced_at,
    )


__all__ = [
    "DEFAULT_HOOK_TIMEOUT_SECONDS",
    "DOCKER_CLEANUP_TIMEOUT_SECONDS",
    "ConnectorContext",
    "CursorEncodingMismatch",
    "CursorEnvelope",
    "CursorExpired",
    "DockerCliHookRunner",
    "HealthResult",
    "HookRunResult",
    "HookRunner",
    "ListenMode",
    "ListenResult",
    "PluginCleanupFailed",
    "PluginErrorCode",
    "PluginHookTimeout",
    "PluginInvocationFailed",
    "PluginInvoker",
    "PluginProtocolError",
    "PluginRuntimeError",
    "UpstreamUnauthorized",
    "UpstreamUnreachable",
    "invocation_container_name",
]
