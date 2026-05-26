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

v1's concrete runner is :class:`DockerCliHookRunner`, which shells out to
``docker run --rm -i <image_ref> <hook>`` and exchanges JSON over
 stdin/stdout. This keeps the runtime adapter testable and transport-
 agnostic: a future Kubernetes Job runner can implement the same
 :class:`HookRunner` Protocol without changing any caller code.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Final, Protocol

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
    ) -> HookRunResult: ...


@dataclass(frozen=True, slots=True)
class DockerCliHookRunner:
    """Invoke plugin images through the local Docker CLI.

    The plugin image is expected to treat the first argv token after the
    image reference as the hook name and to exchange a single JSON document
    over stdin/stdout.
    """

    binary: str = "docker"
    extra_run_args: Sequence[str] = ()

    async def run(
        self,
        *,
        image_ref: str,
        hook: str,
        payload: Mapping[str, Any],
        timeout_seconds: float,
    ) -> HookRunResult:
        proc = await asyncio.create_subprocess_exec(
            self.binary,
            "run",
            "--rm",
            "-i",
            *self.extra_run_args,
            image_ref,
            hook,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(body), timeout_seconds)
        except TimeoutError as exc:
            proc.kill()
            await proc.wait()
            raise TimeoutError(f"plugin hook {hook!r} timed out after {timeout_seconds}s") from exc
        return HookRunResult(
            exit_code=int(proc.returncode or 0),
            stdout=stdout,
            stderr=stderr,
        )


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
    ) -> ListenResult:
        result = await self._invoke(
            connector=connector,
            instance=instance,
            hook="listen",
            hook_input={
                "mode": str(mode),
                "cursor": _serialize_cursor(cursor),
            },
        )
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
            except ValueError as exc:
                raise PluginProtocolError(
                    "health result checkedAt must be an RFC3339 string"
                ) from exc
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
    ) -> Mapping[str, Any]:
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
            completed = await self._runner.run(
                image_ref=connector.image_ref,
                hook=hook,
                payload=payload,
                timeout_seconds=self._timeout_seconds,
            )
        except TimeoutError as exc:
            raise PluginHookTimeout(str(exc), data={"hook": hook}) from exc
        except OSError as exc:
            raise PluginInvocationFailed(
                f"failed to start plugin image {connector.image_ref!r}: {exc}",
                data={"hook": hook, "image_ref": connector.image_ref},
            ) from exc

        body = self._decode_body(completed.stdout, hook=hook, image_ref=connector.image_ref)
        ok = body.get("ok")
        if not isinstance(ok, bool):
            raise PluginProtocolError("plugin response must carry boolean field 'ok'")
        if ok:
            result = body.get("result")
            if not isinstance(result, Mapping):
                raise PluginProtocolError("successful plugin response must carry object result")
            if completed.exit_code != 0:
                raise PluginInvocationFailed(
                    (
                        f"plugin hook {hook!r} exited with status {completed.exit_code} "
                        "despite ok=true"
                    ),
                    data={
                        "hook": hook,
                        "image_ref": connector.image_ref,
                        "stderr": completed.stderr.decode("utf-8", "replace"),
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
        if not stdout:
            raise PluginProtocolError(
                f"plugin hook {hook!r} on {image_ref!r} returned empty stdout"
            )
        try:
            decoded = json.loads(stdout)
        except json.JSONDecodeError as exc:
            raise PluginProtocolError(
                f"plugin hook {hook!r} on {image_ref!r} returned invalid JSON"
            ) from exc
        if not isinstance(decoded, Mapping):
            raise PluginProtocolError("plugin response root must be a JSON object")
        return decoded

    @staticmethod
    def _raise_plugin_error(error: Mapping[str, Any]) -> None:
        raw_code = error.get("code")
        detail = error.get("detail")
        data = error.get("data")
        if not isinstance(raw_code, str):
            raise PluginProtocolError("plugin error.code must be a string")
        if not isinstance(detail, str):
            raise PluginProtocolError("plugin error.detail must be a string")
        if data is not None and not isinstance(data, Mapping):
            raise PluginProtocolError("plugin error.data must be an object when present")

        payload = data if isinstance(data, Mapping) else None
        match raw_code:
            case PluginErrorCode.CURSOR_EXPIRED:
                raise CursorExpired(detail, data=payload)
            case PluginErrorCode.CURSOR_ENCODING_MISMATCH:
                raise CursorEncodingMismatch(
                    detail,
                    persisted_encoding=(
                        payload.get("persistedEncoding") if isinstance(payload, Mapping) else None
                    )
                    if isinstance(payload, Mapping)
                    else None,
                    plugin_encoding=(
                        payload.get("pluginEncoding") if isinstance(payload, Mapping) else None
                    )
                    if isinstance(payload, Mapping)
                    else None,
                    data=payload,
                )
            case PluginErrorCode.UPSTREAM_UNAUTHORIZED:
                raise UpstreamUnauthorized(detail, data=payload)
            case PluginErrorCode.UPSTREAM_UNREACHABLE:
                raise UpstreamUnreachable(detail, data=payload)
            case PluginErrorCode.HOOK_TIMEOUT:
                raise PluginHookTimeout(detail, data=payload)
            case PluginErrorCode.INVOCATION_FAILED:
                raise PluginInvocationFailed(detail, data=payload)
            case _:
                raise PluginRuntimeError(detail, data={"code": raw_code, **dict(payload or {})})


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
        advanced_at = datetime.fromisoformat(advanced_at_raw.replace("Z", "+00:00"))
    return CursorEnvelope(
        encoding=encoding,
        value=payload.get("value"),
        advanced_at=advanced_at,
    )


__all__ = [
    "DEFAULT_HOOK_TIMEOUT_SECONDS",
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
    "PluginErrorCode",
    "PluginHookTimeout",
    "PluginInvocationFailed",
    "PluginInvoker",
    "PluginProtocolError",
    "PluginRuntimeError",
    "UpstreamUnauthorized",
    "UpstreamUnreachable",
]
