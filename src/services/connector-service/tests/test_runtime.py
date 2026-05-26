from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

import pytest
from custos_spl.ids import ConnectorInstanceId, WorkspaceId
from custos_spl.interfaces.catalog_store import ConnectorTypeVersion
from custos_spl.interfaces.connector_instance_store import ConnectorInstance

from custos_connector.runtime import (
    ConnectorContext,
    CursorEncodingMismatch,
    CursorEnvelope,
    CursorExpired,
    HealthResult,
    HookRunResult,
    ListenMode,
    ListenResult,
    PluginHookTimeout,
    PluginInvocationFailed,
    PluginInvoker,
    PluginProtocolError,
    UpstreamUnreachable,
)


def _connector(*, image_ref: str = "example.test/stub@sha256:abc") -> ConnectorTypeVersion:
    return ConnectorTypeVersion(
        type="stub",
        version="1.0.0",
        digest="sha256:manifest",
        image_ref=image_ref,
        normalized_manifest={
            "metadata": {"type": "stub", "version": "1.0.0"},
            "spec": {
                "events": {
                    "pull": {
                        "cursorEncoding": "stub-cursor-v1",
                    }
                }
            },
        },
        parent_deprecated=False,
        published_at=datetime(2026, 1, 1, tzinfo=UTC),
    )


def _instance() -> ConnectorInstance:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    return ConnectorInstance(
        workspace_id=WorkspaceId("ws_123"),
        instance_id=ConnectorInstanceId("conn_123"),
        type="stub",
        version="1.0.0",
        name="stub connection",
        lease_ttl_seconds=600,
        enabled=True,
        status="enabled",
        health_status="healthy",
        target_config={"host": "registry.example.com"},
        credentials_authentication={"issuerUri": "https://issuer.example.com"},
        used_capabilities=("oci.pull",),
        created_at=now,
        updated_at=now,
    )


class _FakeRunner:
    def __init__(
        self,
        *,
        result: HookRunResult | None = None,
        exc: Exception | None = None,
    ) -> None:
        self._result = result
        self._exc = exc
        self.calls: list[dict[str, Any]] = []

    async def run(
        self,
        *,
        image_ref: str,
        hook: str,
        payload: Mapping[str, Any],
        timeout_seconds: float,
    ) -> HookRunResult:
        self.calls.append(
            {
                "image_ref": image_ref,
                "hook": hook,
                "payload": payload,
                "timeout_seconds": timeout_seconds,
            }
        )
        if self._exc is not None:
            raise self._exc
        assert self._result is not None
        return self._result


@pytest.mark.asyncio
async def test_bind_parses_success_response_and_passes_image_ref() -> None:
    runner = _FakeRunner(
        result=HookRunResult(
            exit_code=0,
            stdout=json.dumps(
                {
                    "ok": True,
                    "result": {
                        "endpoint": "https://registry.example.com",
                        "tokenTypeHint": "bearer",
                        "handle": {"leaseHint": "stub"},
                        "extras": {"region": "us-east-1"},
                    },
                }
            ).encode(),
            stderr=b"",
        )
    )
    invoker = PluginInvoker(runner)

    result = await invoker.bind(
        connector=_connector(),
        instance=_instance(),
        slot="source",
        capability="oci.pull",
        identity_material={"kind": "oidc"},
    )

    assert isinstance(result, ConnectorContext)
    assert result.endpoint == "https://registry.example.com"
    assert result.token_type_hint == "bearer"
    assert result.handle["leaseHint"] == "stub"
    assert runner.calls[0]["image_ref"] == "example.test/stub@sha256:abc"
    assert runner.calls[0]["payload"]["connector"]["imageRef"] == "example.test/stub@sha256:abc"


@pytest.mark.asyncio
async def test_listen_maps_cursor_expired_error() -> None:
    runner = _FakeRunner(
        result=HookRunResult(
            exit_code=0,
            stdout=json.dumps(
                {
                    "ok": False,
                    "error": {
                        "code": "cursor-expired",
                        "detail": "upstream cursor expired",
                    },
                }
            ).encode(),
            stderr=b"",
        )
    )
    invoker = PluginInvoker(runner)

    with pytest.raises(CursorExpired):
        await invoker.listen(
            connector=_connector(),
            instance=_instance(),
            mode=ListenMode.PULL,
            cursor=CursorEnvelope(encoding="stub-cursor-v1", value="expired"),
        )


@pytest.mark.asyncio
async def test_listen_maps_cursor_encoding_mismatch_error() -> None:
    runner = _FakeRunner(
        result=HookRunResult(
            exit_code=0,
            stdout=json.dumps(
                {
                    "ok": False,
                    "error": {
                        "code": "cursor-encoding-mismatch",
                        "detail": "cursor encoding mismatch",
                        "data": {
                            "persistedEncoding": "old-v1",
                            "pluginEncoding": "new-v2",
                        },
                    },
                }
            ).encode(),
            stderr=b"",
        )
    )
    invoker = PluginInvoker(runner)

    with pytest.raises(CursorEncodingMismatch) as excinfo:
        await invoker.listen(
            connector=_connector(),
            instance=_instance(),
            mode=ListenMode.PULL,
            cursor=CursorEnvelope(encoding="old-v1", value="cursor-1"),
        )

    assert excinfo.value.persisted_encoding == "old-v1"
    assert excinfo.value.plugin_encoding == "new-v2"


@pytest.mark.asyncio
async def test_health_returns_typed_result() -> None:
    runner = _FakeRunner(
        result=HookRunResult(
            exit_code=0,
            stdout=json.dumps(
                {
                    "ok": True,
                    "result": {
                        "healthy": True,
                        "detail": "ok",
                        "checkedAt": "2026-01-01T00:00:00Z",
                        "extras": {"source": "stub"},
                    },
                }
            ).encode(),
            stderr=b"",
        )
    )
    invoker = PluginInvoker(runner)

    result = await invoker.health(connector=_connector(), instance=_instance())

    assert isinstance(result, HealthResult)
    assert result.healthy is True
    assert result.detail == "ok"
    assert result.extras["source"] == "stub"


@pytest.mark.asyncio
async def test_timeout_maps_to_plugin_hook_timeout() -> None:
    invoker = PluginInvoker(_FakeRunner(exc=TimeoutError("timed out")))

    with pytest.raises(PluginHookTimeout):
        await invoker.health(connector=_connector(), instance=_instance())


@pytest.mark.asyncio
async def test_invalid_json_raises_protocol_error() -> None:
    invoker = PluginInvoker(
        _FakeRunner(result=HookRunResult(exit_code=0, stdout=b"not-json", stderr=b""))
    )

    with pytest.raises(PluginProtocolError):
        await invoker.health(connector=_connector(), instance=_instance())


@pytest.mark.asyncio
async def test_non_zero_exit_with_empty_stdout_raises_invocation_failed() -> None:
    invoker = PluginInvoker(
        _FakeRunner(
            result=HookRunResult(
                exit_code=125,
                stdout=b"",
                stderr=b"Unable to find image 'example.test/stub' locally\n",
            )
        )
    )

    with pytest.raises(PluginInvocationFailed) as excinfo:
        await invoker.health(connector=_connector(), instance=_instance())

    data = excinfo.value.data or {}
    assert data.get("exit_code") == 125
    assert "Unable to find image" in (data.get("stderr") or "")
    assert data.get("stdout") == ""
    assert data.get("hook") == "health"


@pytest.mark.asyncio
async def test_non_zero_exit_with_invalid_stdout_raises_invocation_failed() -> None:
    invoker = PluginInvoker(
        _FakeRunner(
            result=HookRunResult(
                exit_code=1,
                stdout=b"panic: runtime error\n",
                stderr=b"goroutine 1 [running]:\n",
            )
        )
    )

    with pytest.raises(PluginInvocationFailed) as excinfo:
        await invoker.health(connector=_connector(), instance=_instance())

    data = excinfo.value.data or {}
    assert data.get("exit_code") == 1
    assert "panic" in (data.get("stdout") or "")
    assert "goroutine" in (data.get("stderr") or "")


@pytest.mark.asyncio
async def test_health_maps_upstream_unreachable() -> None:
    invoker = PluginInvoker(
        _FakeRunner(
            result=HookRunResult(
                exit_code=0,
                stdout=json.dumps(
                    {
                        "ok": False,
                        "error": {
                            "code": "upstream-unreachable",
                            "detail": "dial tcp timeout",
                        },
                    }
                ).encode(),
                stderr=b"",
            )
        )
    )

    with pytest.raises(UpstreamUnreachable):
        await invoker.health(connector=_connector(), instance=_instance())


@pytest.mark.asyncio
async def test_listen_parses_success_result() -> None:
    invoker = PluginInvoker(
        _FakeRunner(
            result=HookRunResult(
                exit_code=0,
                stdout=json.dumps(
                    {
                        "ok": True,
                        "result": {
                            "events": [{"eventId": "evt-1", "type": "stub.event"}],
                            "nextCursor": {
                                "encoding": "stub-cursor-v1",
                                "value": "cursor-2",
                                "advancedAt": "2026-01-01T00:00:00Z",
                            },
                            "receiverEndpoint": None,
                        },
                    }
                ).encode(),
                stderr=b"",
            )
        )
    )

    result = await invoker.listen(
        connector=_connector(),
        instance=_instance(),
        mode=ListenMode.PULL,
        cursor=CursorEnvelope(encoding="stub-cursor-v1", value="cursor-1"),
    )

    assert isinstance(result, ListenResult)
    assert result.events[0]["eventId"] == "evt-1"
    assert result.next_cursor is not None
    assert result.next_cursor.value == "cursor-2"
