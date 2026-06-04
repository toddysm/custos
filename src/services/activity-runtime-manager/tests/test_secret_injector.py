"""Tests for the Secret Injector (ARM-IMPL-010)."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime

import httpx
import pytest

from custos_arm.contract import StepRef
from custos_arm.contract.errors import ErrorClass
from custos_arm.manifest import ConnectorSpec
from custos_arm.secrets import (
    SECRET_FILE_MODE,
    SIDECAR_TOKEN_FILENAME,
    ConnectorContext,
    ConnectorLeaseClient,
    ConnectorUnavailableError,
    DaprConnectorLeaseClient,
    InjectionResult,
    Lease,
    LeaseRefreshRejectedError,
    MissingConnectorError,
    MissingSecretError,
    SecretInjector,
    SecretInjectorError,
    SecretSink,
    SidecarToken,
    SidecarTokenMinter,
)

_CONNECTOR = "http://127.0.0.1:3500/v1.0/invoke/connector/method"


# ---------------------------------------------------------------------------
# Helpers / doubles
# ---------------------------------------------------------------------------


def _step(*, attempt: int = 1) -> StepRef:
    return StepRef(runId="run-1", stepId="step-1", attempt=attempt)


def _connector(name: str, *, required: bool = True) -> ConnectorSpec:
    return ConnectorSpec(name=name, type="oci-registry", required=required, capabilities=[])


class _FakeSink:
    """In-memory :class:`SecretSink` recording every write."""

    def __init__(self) -> None:
        self.files: dict[str, tuple[bytes, int]] = {}
        self.write_order: list[str] = []

    async def write_secret(self, *, relative_path: str, content: bytes, mode: int) -> None:
        self.files[relative_path] = (content, mode)
        self.write_order.append(relative_path)


class _FakeLeaseClient:
    """In-memory :class:`ConnectorLeaseClient` recording refresh calls."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, int | None, datetime | None]] = []

    async def refresh_lease(
        self,
        *,
        lease_id: str,
        requested_ttl_sec: int | None = None,
        step_deadline: datetime | None = None,
    ) -> Lease:
        self.calls.append((lease_id, requested_ttl_sec, step_deadline))
        return Lease(
            lease_id=lease_id,
            expires_at=datetime(2026, 5, 17, 12, 0, tzinfo=UTC),
            slot="registry",
            connector_instance_id="ci-1",
        )


_Handler = Callable[[httpx.Request], httpx.Response]


def _lease_client(handler: _Handler) -> DaprConnectorLeaseClient:
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return DaprConnectorLeaseClient(client, connector_endpoint=_CONNECTOR)


def _lease_envelope(**overrides: object) -> dict[str, object]:
    lease: dict[str, object] = {
        "leaseId": "lease-1",
        "expiresAt": "2026-05-17T12:00:00+00:00",
        "slot": "registry",
        "connectorInstanceId": "ci-1",
        "tokenType": "bearer",
    }
    lease.update(overrides)
    return {"lease": lease}


# ---------------------------------------------------------------------------
# ConnectorContext / SidecarToken models
# ---------------------------------------------------------------------------


def test_connector_context_valid() -> None:
    ctx = ConnectorContext(
        slot_name="registry",
        connector_type="oci-registry",
        connector_instance_id="ci-1",
        secrets={"username": "u", "password": "p"},
        lease_id="lease-1",
    )
    assert ctx.slot_name == "registry"
    assert ctx.secrets["password"] == "p"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"slot_name": ""},
        {"connector_type": ""},
        {"connector_instance_id": ""},
        {"secrets": {"": "v"}},
        {"lease_id": ""},
    ],
)
def test_connector_context_rejects_bad_fields(kwargs: dict[str, object]) -> None:
    base: dict[str, object] = {
        "slot_name": "registry",
        "connector_type": "oci-registry",
        "connector_instance_id": "ci-1",
    }
    base.update(kwargs)
    with pytest.raises(ValueError):
        ConnectorContext(**base)  # type: ignore[arg-type]


def test_sidecar_token_scope() -> None:
    token = SidecarToken(value="abc", run_id="run-1", step_id="step-1", attempt=2)
    assert token.scope == ("run-1", "step-1", 2)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"value": ""},
        {"run_id": ""},
        {"step_id": ""},
        {"attempt": 0},
        {"attempt": True},
        {"attempt": "1"},
    ],
)
def test_sidecar_token_rejects_bad_fields(kwargs: dict[str, object]) -> None:
    base: dict[str, object] = {
        "value": "abc",
        "run_id": "run-1",
        "step_id": "step-1",
        "attempt": 1,
    }
    base.update(kwargs)
    with pytest.raises(ValueError):
        SidecarToken(**base)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# SidecarTokenMinter
# ---------------------------------------------------------------------------


def test_minter_mints_scoped_token() -> None:
    minter = SidecarTokenMinter()
    token = minter.mint(step=_step())
    assert token.scope == ("run-1", "step-1", 1)
    assert minter.is_active(token) is True


def test_minter_is_idempotent_per_scope() -> None:
    minter = SidecarTokenMinter()
    first = minter.mint(step=_step())
    second = minter.mint(step=_step())
    assert first is second


def test_minter_distinct_tokens_per_attempt() -> None:
    minter = SidecarTokenMinter()
    first = minter.mint(step=_step(attempt=1))
    second = minter.mint(step=_step(attempt=2))
    assert first.value != second.value


def test_minter_revoke_deactivates_token() -> None:
    minter = SidecarTokenMinter()
    token = minter.mint(step=_step())
    minter.revoke(token)
    assert minter.is_active(token) is False
    # Idempotent: revoking again is a no-op.
    minter.revoke(token)


def test_minter_rejects_non_positive_bytes() -> None:
    with pytest.raises(ValueError):
        SidecarTokenMinter(token_bytes=0)


# ---------------------------------------------------------------------------
# SecretInjector.inject
# ---------------------------------------------------------------------------


async def test_inject_materializes_secrets_and_token() -> None:
    injector = SecretInjector(token_minter=SidecarTokenMinter())
    sink = _FakeSink()
    contexts = [
        ConnectorContext(
            slot_name="registry",
            connector_type="oci-registry",
            connector_instance_id="ci-1",
            secrets={"password": "p", "username": "u"},
        ),
        ConnectorContext(
            slot_name="audit",
            connector_type="s3",
            connector_instance_id="ci-2",
            secrets={"token": "t"},
        ),
    ]
    result = await injector.inject(
        sink=sink,
        step=_step(),
        connectors=[_connector("registry"), _connector("audit")],
        contexts=contexts,
    )

    assert isinstance(result, InjectionResult)
    # Deterministic order: slots sorted, then keys sorted, token written last.
    assert result.secret_files == (
        "secrets/audit/token",
        "secrets/registry/password",
        "secrets/registry/username",
    )
    assert sink.write_order[-1] == SIDECAR_TOKEN_FILENAME
    assert sink.files["secrets/registry/password"] == (b"p", SECRET_FILE_MODE)
    assert sink.files[SIDECAR_TOKEN_FILENAME] == (result.token.value.encode(), SECRET_FILE_MODE)
    # The token value never lands in a credential file.
    assert all(name.startswith("secrets/") for name in result.secret_files)


async def test_inject_token_only_connector_writes_no_secret_files() -> None:
    injector = SecretInjector(token_minter=SidecarTokenMinter())
    sink = _FakeSink()
    contexts = [
        ConnectorContext(
            slot_name="registry",
            connector_type="oci-registry",
            connector_instance_id="ci-1",
        )
    ]
    result = await injector.inject(
        sink=sink, step=_step(), connectors=[_connector("registry")], contexts=contexts
    )
    assert result.secret_files == ()
    assert list(sink.files) == [SIDECAR_TOKEN_FILENAME]


async def test_inject_no_connectors_writes_only_token() -> None:
    injector = SecretInjector(token_minter=SidecarTokenMinter())
    sink = _FakeSink()
    result = await injector.inject(sink=sink, step=_step(), connectors=[], contexts=[])
    assert result.secret_files == ()
    assert list(sink.files) == [SIDECAR_TOKEN_FILENAME]


async def test_inject_optional_connector_may_be_unbound() -> None:
    injector = SecretInjector(token_minter=SidecarTokenMinter())
    sink = _FakeSink()
    result = await injector.inject(
        sink=sink,
        step=_step(),
        connectors=[_connector("optional", required=False)],
        contexts=[],
    )
    assert result.secret_files == ()


async def test_inject_missing_required_connector_raises() -> None:
    injector = SecretInjector(token_minter=SidecarTokenMinter())
    with pytest.raises(MissingConnectorError) as excinfo:
        await injector.inject(
            sink=_FakeSink(),
            step=_step(),
            connectors=[_connector("registry"), _connector("audit")],
            contexts=[],
        )
    err = excinfo.value
    assert err.code == "input.missing_connector"
    assert err.error_class is ErrorClass.PERMANENT
    assert err.issues == ["audit", "registry"]
    envelope = err.to_error_envelope()
    assert envelope.code == "input.missing_connector"
    assert envelope.details == {"issues": ["audit", "registry"]}


async def test_inject_empty_secret_value_raises() -> None:
    injector = SecretInjector(token_minter=SidecarTokenMinter())
    contexts = [
        ConnectorContext(
            slot_name="registry",
            connector_type="oci-registry",
            connector_instance_id="ci-1",
            secrets={"password": ""},
        )
    ]
    with pytest.raises(MissingSecretError) as excinfo:
        await injector.inject(
            sink=_FakeSink(),
            step=_step(),
            connectors=[_connector("registry")],
            contexts=contexts,
        )
    assert excinfo.value.code == "input.missing_secret"
    assert excinfo.value.issues == ["registry/password"]


async def test_inject_duplicate_slot_raises_value_error() -> None:
    injector = SecretInjector(token_minter=SidecarTokenMinter())
    dup = ConnectorContext(
        slot_name="registry",
        connector_type="oci-registry",
        connector_instance_id="ci-1",
    )
    with pytest.raises(ValueError, match="duplicate connector context"):
        await injector.inject(
            sink=_FakeSink(),
            step=_step(),
            connectors=[_connector("registry")],
            contexts=[dup, dup],
        )


async def test_inject_replay_reuses_token() -> None:
    minter = SidecarTokenMinter()
    injector = SecretInjector(token_minter=minter)
    first = await injector.inject(sink=_FakeSink(), step=_step(), connectors=[], contexts=[])
    second = await injector.inject(sink=_FakeSink(), step=_step(), connectors=[], contexts=[])
    assert first.token.value == second.token.value


def test_inject_revoke_deactivates_token() -> None:
    minter = SidecarTokenMinter()
    injector = SecretInjector(token_minter=minter)
    token = minter.mint(step=_step())
    injector.revoke(token=token)
    assert minter.is_active(token) is False


# ---------------------------------------------------------------------------
# SecretInjector.refresh_leases
# ---------------------------------------------------------------------------


async def test_refresh_leases_extends_only_leased_contexts() -> None:
    fake = _FakeLeaseClient()
    injector = SecretInjector(token_minter=SidecarTokenMinter(), lease_client=fake)
    contexts = [
        ConnectorContext(
            slot_name="registry",
            connector_type="oci-registry",
            connector_instance_id="ci-1",
            lease_id="lease-1",
        ),
        ConnectorContext(
            slot_name="audit",
            connector_type="s3",
            connector_instance_id="ci-2",
        ),
    ]
    leases = await injector.refresh_leases(
        contexts=contexts, requested_ttl_sec=300, step_deadline=None
    )
    assert [lease.lease_id for lease in leases] == ["lease-1"]
    assert fake.calls == [("lease-1", 300, None)]


async def test_refresh_leases_no_leases_returns_empty_without_client() -> None:
    injector = SecretInjector(token_minter=SidecarTokenMinter())
    contexts = [
        ConnectorContext(
            slot_name="registry",
            connector_type="oci-registry",
            connector_instance_id="ci-1",
        )
    ]
    assert await injector.refresh_leases(contexts=contexts) == []


async def test_refresh_leases_requires_client_when_lease_present() -> None:
    injector = SecretInjector(token_minter=SidecarTokenMinter())
    contexts = [
        ConnectorContext(
            slot_name="registry",
            connector_type="oci-registry",
            connector_instance_id="ci-1",
            lease_id="lease-1",
        )
    ]
    with pytest.raises(ValueError, match="no lease_client is configured"):
        await injector.refresh_leases(contexts=contexts)


# ---------------------------------------------------------------------------
# DaprConnectorLeaseClient
# ---------------------------------------------------------------------------


def test_dapr_lease_client_satisfies_protocol() -> None:
    client = _lease_client(lambda _r: httpx.Response(200, json=_lease_envelope()))
    assert isinstance(client, ConnectorLeaseClient)


async def test_refresh_lease_success_parses_envelope() -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["body"] = request.read()
        return httpx.Response(200, json=_lease_envelope())

    client = _lease_client(handler)
    deadline = datetime(2026, 5, 17, 13, 0, tzinfo=UTC)
    lease = await client.refresh_lease(
        lease_id="lease-1", requested_ttl_sec=600, step_deadline=deadline
    )

    assert lease == Lease(
        lease_id="lease-1",
        expires_at=datetime(2026, 5, 17, 12, 0, tzinfo=UTC),
        slot="registry",
        connector_instance_id="ci-1",
    )
    assert captured["url"] == f"{_CONNECTOR}/internal/v1/leases:refresh"
    assert b'"leaseId":"lease-1"' in captured["body"]  # type: ignore[operator]
    assert b'"requestedTtlSec":600' in captured["body"]  # type: ignore[operator]
    assert b'"stepDeadline":"2026-05-17T13:00:00+00:00"' in captured["body"]  # type: ignore[operator]


async def test_refresh_lease_omits_optional_fields_when_none() -> None:
    captured: dict[str, bytes] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = request.read()
        return httpx.Response(200, json=_lease_envelope())

    client = _lease_client(handler)
    await client.refresh_lease(lease_id="lease-1")
    assert captured["body"] == b'{"leaseId":"lease-1"}'


async def test_refresh_lease_empty_id_raises() -> None:
    client = _lease_client(lambda _r: httpx.Response(200, json=_lease_envelope()))
    with pytest.raises(ValueError):
        await client.refresh_lease(lease_id="")


async def test_refresh_lease_4xx_is_permanent() -> None:
    client = _lease_client(lambda _r: httpx.Response(410))
    with pytest.raises(LeaseRefreshRejectedError) as excinfo:
        await client.refresh_lease(lease_id="lease-1")
    assert excinfo.value.code == "system.lease_refresh_rejected"
    assert excinfo.value.error_class is ErrorClass.PERMANENT
    assert excinfo.value.lease_id == "lease-1"


@pytest.mark.parametrize("status", [201, 503])
async def test_refresh_lease_unexpected_status_is_transient(status: int) -> None:
    client = _lease_client(lambda _r: httpx.Response(status, json=_lease_envelope()))
    with pytest.raises(ConnectorUnavailableError):
        await client.refresh_lease(lease_id="lease-1")


async def test_refresh_lease_transport_error_is_transient() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("boom")

    client = _lease_client(handler)
    with pytest.raises(ConnectorUnavailableError):
        await client.refresh_lease(lease_id="lease-1")


async def test_refresh_lease_non_json_body_is_transient() -> None:
    client = _lease_client(lambda _r: httpx.Response(200, content=b"not json"))
    with pytest.raises(ConnectorUnavailableError):
        await client.refresh_lease(lease_id="lease-1")


async def test_refresh_lease_non_object_payload_is_transient() -> None:
    client = _lease_client(lambda _r: httpx.Response(200, json=[1, 2, 3]))
    with pytest.raises(ConnectorUnavailableError):
        await client.refresh_lease(lease_id="lease-1")


async def test_refresh_lease_missing_envelope_is_transient() -> None:
    client = _lease_client(lambda _r: httpx.Response(200, json={"other": 1}))
    with pytest.raises(ConnectorUnavailableError):
        await client.refresh_lease(lease_id="lease-1")


async def test_refresh_lease_missing_field_is_transient() -> None:
    client = _lease_client(lambda _r: httpx.Response(200, json={"lease": {"leaseId": "lease-1"}}))
    with pytest.raises(ConnectorUnavailableError):
        await client.refresh_lease(lease_id="lease-1")


async def test_refresh_lease_non_string_expiry_is_transient() -> None:
    client = _lease_client(lambda _r: httpx.Response(200, json=_lease_envelope(expiresAt=123)))
    with pytest.raises(ConnectorUnavailableError):
        await client.refresh_lease(lease_id="lease-1")


async def test_refresh_lease_unparseable_expiry_is_transient() -> None:
    client = _lease_client(
        lambda _r: httpx.Response(200, json=_lease_envelope(expiresAt="not-a-date"))
    )
    with pytest.raises(ConnectorUnavailableError):
        await client.refresh_lease(lease_id="lease-1")


# ---------------------------------------------------------------------------
# Error rendering
# ---------------------------------------------------------------------------


def test_secret_injector_error_without_issues_renders_no_details() -> None:
    class _Boom(SecretInjectorError):
        code = "input.missing_secret"

    envelope = _Boom("boom").to_error_envelope()
    assert envelope.details is None
    assert envelope.error_class is ErrorClass.PERMANENT


def test_fake_sink_satisfies_protocol() -> None:
    assert isinstance(_FakeSink(), SecretSink)
