"""Tests for ``DaprActivityRuntimeClient`` (WF-IMPL-076 + WF-IMPL-077).

The adapter is the first production hop the Step Coordinator's
yielded :class:`ScheduleActivityCallToken` actually traverses, so
these tests cover the full transport-error → envelope-class matrix
locked in WF-IMPL-075 plus the wire-shape contract pinned in the
ARM design § *Internal RPCs*. The ``cancel_activity`` section
(WF-IMPL-077) exercises the idempotent-cancel surface separately
because it has no envelope mapping and a different status-code
taxonomy.

Coverage emphasis:

* Success envelopes pass through unchanged.
* Every locked :class:`~custos_workflow.clients._errors.OutboundRpcError`
  bucket maps to the right :class:`ActivityResultClass`.
* The ``Idempotency-Key`` header is always present on
  ``ScheduleActivity`` and built from the canonical
  :class:`IdempotencyTriple` wire form.
* The outbound request body uses the camelCase wire shape ARM
  expects.
* ``CancelActivity`` collapses 200 / 204 / 404 / 409 to a no-op
  and raises :class:`OutboundRpcStatusError` /
  :class:`OutboundRpcTransportError` for every other failure mode.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any, cast

import httpx
import pytest

from custos_workflow.clients._dapr_invoke import (
    DEFAULT_OUTBOUND_RPC_TIMEOUT_SECONDS,
    DaprInvokeEndpoint,
    build_invoke_url,
)
from custos_workflow.clients._errors import (
    OutboundRpcStatusError,
    OutboundRpcTransportError,
)
from custos_workflow.clients.activity_runtime import (
    CANCEL_ACTIVITY_DAPR_METHOD,
    IDEMPOTENCY_HEADER,
    SCHEDULE_ACTIVITY_DAPR_METHOD,
    ActivityResultEnvelope,
    DaprActivityRuntimeClient,
    ScheduleActivityRequest,
    _envelope_from_wire,
    _iso_utc,
    _request_to_wire,
    _serialize_connector_context,
)
from custos_workflow.clients.connector import ConnectorContext

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def endpoint() -> DaprInvokeEndpoint:
    return DaprInvokeEndpoint(host="127.0.0.1", http_port=3500, app_id="activity-runtime-manager")


@pytest.fixture
def schedule_url(endpoint: DaprInvokeEndpoint) -> str:
    return build_invoke_url(endpoint, SCHEDULE_ACTIVITY_DAPR_METHOD)


@pytest.fixture
def request_obj() -> ScheduleActivityRequest:
    return ScheduleActivityRequest(
        run_id="run-1",
        step_id="step-a",
        attempt=2,
        activity_ref="custos.builtin/scan-image@1",
        inputs={"image": "ghcr.io/acme/app@sha256:abc"},
        connector_contexts={
            "registry": ConnectorContext(
                slot_name="registry",
                handle="ctx-token-xyz",
                expires_at=datetime(2030, 1, 2, 3, 4, 5, tzinfo=UTC),
                connector_kind="oci-registry",
            ),
        },
        deadline=datetime(2030, 1, 2, 4, 0, 0, tzinfo=UTC),
    )


def _make_client(
    endpoint: DaprInvokeEndpoint,
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    timeout: float = DEFAULT_OUTBOUND_RPC_TIMEOUT_SECONDS,
) -> DaprActivityRuntimeClient:
    transport = httpx.MockTransport(handler)
    return DaprActivityRuntimeClient(
        http_client=httpx.AsyncClient(transport=transport),
        endpoint=endpoint,
        timeout=timeout,
    )


# ---------------------------------------------------------------------------
# Helpers (pure)
# ---------------------------------------------------------------------------


class TestIsoUtc:
    def test_aware_utc_renders_with_z_suffix(self) -> None:
        # ISO-8601 + ``Z`` per ARM § Internal RPCs.
        assert _iso_utc(datetime(2030, 1, 2, 3, 4, 5, tzinfo=UTC)) == "2030-01-02T03:04:05Z"

    def test_naive_datetime_rejected(self) -> None:
        with pytest.raises(ValueError, match="timezone-aware"):
            _iso_utc(datetime(2030, 1, 2, 3, 4, 5))


class TestSerializeConnectorContext:
    def test_connector_context_serialised_camelcase(self) -> None:
        ctx = ConnectorContext(
            slot_name="registry",
            handle="h",
            expires_at=datetime(2030, 1, 1, tzinfo=UTC),
            connector_kind="oci-registry",
        )
        wire = _serialize_connector_context(ctx)
        assert wire == {
            "slotName": "registry",
            "handle": "h",
            "expiresAt": "2030-01-01T00:00:00Z",
            "connectorKind": "oci-registry",
        }

    def test_dict_passthrough(self) -> None:
        wire = _serialize_connector_context({"slotName": "x", "handle": "h"})
        assert wire == {"slotName": "x", "handle": "h"}

    def test_unsupported_type_rejected(self) -> None:
        with pytest.raises(TypeError, match="ConnectorContext or Mapping"):
            _serialize_connector_context(object())


class TestRequestToWire:
    def test_camelcase_envelope_shape(self, request_obj: ScheduleActivityRequest) -> None:
        wire = _request_to_wire(request_obj)
        assert wire == {
            "runId": "run-1",
            "stepId": "step-a",
            "attempt": 2,
            "activityRef": "custos.builtin/scan-image@1",
            "inputs": {"image": "ghcr.io/acme/app@sha256:abc"},
            "connectorContexts": {
                "registry": {
                    "slotName": "registry",
                    "handle": "ctx-token-xyz",
                    "expiresAt": "2030-01-02T03:04:05Z",
                    "connectorKind": "oci-registry",
                },
            },
            "deadline": "2030-01-02T04:00:00Z",
        }


class TestEnvelopeFromWire:
    def test_success_envelope_constructed(self) -> None:
        env = _envelope_from_wire(
            {
                "class": "success",
                "outputs": {"ok": True},
                "error": None,
                "attempt": 2,
            },
            expected_attempt=2,
        )
        assert env.class_ == "success"
        assert env.outputs == {"ok": True}
        assert env.attempt == 2

    def test_non_mapping_body_decode_error(self) -> None:
        from custos_workflow.clients._errors import OutboundRpcDecodeError

        with pytest.raises(OutboundRpcDecodeError, match="must be a JSON object"):
            _envelope_from_wire([], expected_attempt=1)

    def test_missing_class_decode_error(self) -> None:
        from custos_workflow.clients._errors import OutboundRpcDecodeError

        with pytest.raises(OutboundRpcDecodeError, match="missing required 'class'"):
            _envelope_from_wire({"attempt": 1}, expected_attempt=1)

    def test_missing_attempt_decode_error(self) -> None:
        from custos_workflow.clients._errors import OutboundRpcDecodeError

        with pytest.raises(OutboundRpcDecodeError, match="missing required 'attempt'"):
            _envelope_from_wire({"class": "success"}, expected_attempt=1)

    def test_unknown_class_decode_error(self) -> None:
        from custos_workflow.clients._errors import OutboundRpcDecodeError

        with pytest.raises(OutboundRpcDecodeError, match="must be one of"):
            _envelope_from_wire(
                {"class": "weird", "attempt": 1, "outputs": {}, "error": None},
                expected_attempt=1,
            )

    def test_non_int_attempt_decode_error(self) -> None:
        from custos_workflow.clients._errors import OutboundRpcDecodeError

        with pytest.raises(OutboundRpcDecodeError, match="'attempt' must be an int"):
            _envelope_from_wire(
                {"class": "success", "attempt": "2", "outputs": {}, "error": None},
                expected_attempt=2,
            )

    def test_bool_attempt_decode_error(self) -> None:
        from custos_workflow.clients._errors import OutboundRpcDecodeError

        with pytest.raises(OutboundRpcDecodeError, match="'attempt' must be an int"):
            _envelope_from_wire(
                {"class": "success", "attempt": True, "outputs": {}, "error": None},
                expected_attempt=1,
            )

    def test_attempt_mismatch_decode_error(self) -> None:
        from custos_workflow.clients._errors import OutboundRpcDecodeError

        with pytest.raises(OutboundRpcDecodeError, match="expected 2"):
            _envelope_from_wire(
                {"class": "success", "attempt": 1, "outputs": {}, "error": None},
                expected_attempt=2,
            )

    def test_non_object_outputs_decode_error(self) -> None:
        from custos_workflow.clients._errors import OutboundRpcDecodeError

        with pytest.raises(OutboundRpcDecodeError, match="'outputs' must be a JSON object"):
            _envelope_from_wire(
                {"class": "success", "attempt": 1, "outputs": [], "error": None},
                expected_attempt=1,
            )

    def test_non_object_error_decode_error(self) -> None:
        from custos_workflow.clients._errors import OutboundRpcDecodeError

        with pytest.raises(OutboundRpcDecodeError, match="'error' must be a JSON object"):
            _envelope_from_wire(
                {"class": "permanent", "attempt": 1, "outputs": None, "error": "bad"},
                expected_attempt=1,
            )

    def test_failing_invariant_surfaces_decode_error(self) -> None:
        # Success carrying an error is rejected by
        # ActivityResultEnvelope.__post_init__; the adapter
        # re-raises as OutboundRpcDecodeError.
        from custos_workflow.clients._errors import OutboundRpcDecodeError

        with pytest.raises(OutboundRpcDecodeError, match="envelope invariants"):
            _envelope_from_wire(
                {
                    "class": "success",
                    "attempt": 1,
                    "outputs": {"x": 1},
                    "error": {"code": "x", "message": "y"},
                },
                expected_attempt=1,
            )


# ---------------------------------------------------------------------------
# DaprActivityRuntimeClient: success path
# ---------------------------------------------------------------------------


async def _drive(
    client: DaprActivityRuntimeClient, request: ScheduleActivityRequest
) -> ActivityResultEnvelope:
    try:
        return await client.schedule_activity(request)
    finally:
        await client.http_client.aclose()


async def test_success_envelope_passed_through(
    endpoint: DaprInvokeEndpoint,
    schedule_url: str,
    request_obj: ScheduleActivityRequest,
) -> None:
    captured: dict[str, httpx.Request] = {}

    def handler(req: httpx.Request) -> httpx.Response:
        captured["req"] = req
        return httpx.Response(
            200,
            json={
                "class": "success",
                "outputs": {"image": "ghcr.io/acme/app@sha256:def"},
                "error": None,
                "attempt": 2,
            },
        )

    client = _make_client(endpoint, handler)
    env = await _drive(client, request_obj)

    assert env.class_ == "success"
    assert env.outputs == {"image": "ghcr.io/acme/app@sha256:def"}
    assert env.attempt == 2

    req = captured["req"]
    assert req.method == "POST"
    assert str(req.url) == schedule_url
    assert req.headers["Content-Type"] == "application/json"
    # Canonical ``run|step|attempt`` triple per IdempotencyTriple.
    assert req.headers[IDEMPOTENCY_HEADER] == "run-1|step-a|2"


async def test_permanent_envelope_passed_through(
    endpoint: DaprInvokeEndpoint, request_obj: ScheduleActivityRequest
) -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "class": "permanent",
                "outputs": None,
                "error": {"code": "input.schema_violation", "message": "bad image"},
                "attempt": 2,
            },
        )

    client = _make_client(endpoint, handler)
    env = await _drive(client, request_obj)

    assert env.class_ == "permanent"
    assert env.outputs is None
    assert env.error is not None
    assert env.error["code"] == "input.schema_violation"


# ---------------------------------------------------------------------------
# DaprActivityRuntimeClient: HTTP status mapping
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("status_code", "expected_class"),
    [
        (400, "permanent"),
        (401, "permanent"),
        (404, "permanent"),
        (422, "permanent"),
        (408, "retryable"),
        (429, "retryable"),
        (500, "retryable"),
        (502, "retryable"),
        (503, "retryable"),
        (504, "retryable"),
    ],
)
async def test_http_status_mapped_to_envelope_class(
    endpoint: DaprInvokeEndpoint,
    request_obj: ScheduleActivityRequest,
    status_code: int,
    expected_class: str,
) -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code, text="upstream said no")

    client = _make_client(endpoint, handler)
    env = await _drive(client, request_obj)

    assert env.class_ == expected_class
    assert env.outputs is None
    assert env.error is not None
    assert env.error["code"] == "workflow.client.status"
    details = env.error["details"]
    assert isinstance(details, Mapping)
    assert details["status_code"] == status_code
    # Attempt is echoed from the request.
    assert env.attempt == request_obj.attempt


async def test_http_499_mapped_to_cancelled(
    endpoint: DaprInvokeEndpoint, request_obj: ScheduleActivityRequest
) -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(499)

    client = _make_client(endpoint, handler)
    env = await _drive(client, request_obj)

    assert env.class_ == "cancelled"
    assert env.error is not None
    assert env.error["code"] == "workflow.client.cancelled"


# ---------------------------------------------------------------------------
# DaprActivityRuntimeClient: transport + decode failures
# ---------------------------------------------------------------------------


async def test_transport_timeout_mapped_to_retryable(
    endpoint: DaprInvokeEndpoint, request_obj: ScheduleActivityRequest
) -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("connect timeout")

    client = _make_client(endpoint, handler)
    env = await _drive(client, request_obj)

    assert env.class_ == "retryable"
    assert env.error is not None
    assert env.error["code"] == "workflow.client.transport"
    # Original ``httpx`` exception is preserved on the cause chain
    # so audit consumers see the underlying transport failure.
    cause = env.error.get("cause")
    assert isinstance(cause, Mapping)
    assert cause["type"] == "ConnectTimeout"


async def test_arbitrary_http_error_mapped_to_transport(
    endpoint: DaprInvokeEndpoint, request_obj: ScheduleActivityRequest
) -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("dns fail")

    client = _make_client(endpoint, handler)
    env = await _drive(client, request_obj)
    assert env.class_ == "retryable"
    assert env.error is not None
    assert env.error["code"] == "workflow.client.transport"


async def test_invalid_json_body_mapped_to_permanent_decode(
    endpoint: DaprInvokeEndpoint, request_obj: ScheduleActivityRequest
) -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        # 200 with non-JSON body — sidecar contract violation.
        return httpx.Response(200, content=b"not json at all")

    client = _make_client(endpoint, handler)
    env = await _drive(client, request_obj)

    # Per the WF-IMPL-075 locked taxonomy: decode -> permanent (a
    # malformed response is a contract violation, not a
    # transient). The WF-IMPL-076 acceptance criteria bullet on
    # decode → retryable conflicts with the taxonomy lock; the
    # adapter follows the taxonomy.
    assert env.error is not None
    assert env.error["code"] == "workflow.client.decode"
    assert env.class_ == "permanent"


async def test_shape_mismatch_body_mapped_to_decode(
    endpoint: DaprInvokeEndpoint, request_obj: ScheduleActivityRequest
) -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"class": "success"})  # missing attempt

    client = _make_client(endpoint, handler)
    env = await _drive(client, request_obj)

    assert env.error is not None
    assert env.error["code"] == "workflow.client.decode"
    assert env.class_ == "permanent"


# ---------------------------------------------------------------------------
# Idempotency header invariants
# ---------------------------------------------------------------------------


async def test_idempotency_key_uses_canonical_triple(
    endpoint: DaprInvokeEndpoint, request_obj: ScheduleActivityRequest
) -> None:
    seen: list[str] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(req.headers[IDEMPOTENCY_HEADER])
        return httpx.Response(
            200,
            json={"class": "success", "outputs": {}, "error": None, "attempt": 2},
        )

    client = _make_client(endpoint, handler)
    await _drive(client, request_obj)
    assert seen == ["run-1|step-a|2"]


async def test_idempotency_key_changes_with_attempt(
    endpoint: DaprInvokeEndpoint,
) -> None:
    seen: list[str] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(req.headers[IDEMPOTENCY_HEADER])
        # Echo back whichever attempt the caller stamped on the request.
        body = req.read()
        import json as _json

        attempt = cast(int, _json.loads(body)["attempt"])
        return httpx.Response(
            200,
            json={"class": "success", "outputs": {}, "error": None, "attempt": attempt},
        )

    base = ScheduleActivityRequest(
        run_id="r",
        step_id="s",
        attempt=1,
        activity_ref="a",
        inputs={},
        connector_contexts={},
        deadline=datetime(2030, 1, 1, tzinfo=UTC),
    )
    request_attempt_2 = ScheduleActivityRequest(
        run_id="r",
        step_id="s",
        attempt=2,
        activity_ref="a",
        inputs={},
        connector_contexts={},
        deadline=datetime(2030, 1, 1, tzinfo=UTC),
    )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as http:
        client = DaprActivityRuntimeClient(http_client=http, endpoint=endpoint)
        await client.schedule_activity(base)
        await client.schedule_activity(request_attempt_2)

    assert seen == ["r|s|1", "r|s|2"]


async def test_outbound_url_targets_arm_app_id(
    endpoint: DaprInvokeEndpoint,
    schedule_url: str,
    request_obj: ScheduleActivityRequest,
) -> None:
    captured: list[str] = []

    def handler(req: httpx.Request) -> httpx.Response:
        captured.append(str(req.url))
        return httpx.Response(
            200,
            json={"class": "success", "outputs": {}, "error": None, "attempt": 2},
        )

    client = _make_client(endpoint, handler)
    await _drive(client, request_obj)

    assert captured == [schedule_url]
    assert "/v1.0/invoke/activity-runtime-manager/method/ScheduleActivity" in schedule_url


# ---------------------------------------------------------------------------
# cancel_activity adapter (WF-IMPL-077)
# ---------------------------------------------------------------------------


@pytest.fixture
def cancel_url(endpoint: DaprInvokeEndpoint) -> str:
    return build_invoke_url(endpoint, CANCEL_ACTIVITY_DAPR_METHOD)


def _cancel_handler(
    status: int, *, body: bytes | None = None
) -> Callable[[httpx.Request], httpx.Response]:
    def handler(req: httpx.Request) -> httpx.Response:
        if body is None:
            return httpx.Response(status)
        return httpx.Response(status, content=body)

    return handler


async def _run_cancel(
    endpoint: DaprInvokeEndpoint,
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    timeout: float = DEFAULT_OUTBOUND_RPC_TIMEOUT_SECONDS,
    run_id: str = "run-1",
    step_id: str = "step-a",
) -> None:
    client = _make_client(endpoint, handler, timeout=timeout)
    try:
        await client.cancel_activity(run_id, step_id)
    finally:
        await client.http_client.aclose()


@pytest.mark.parametrize("status", [200, 204])
async def test_cancel_activity_success_returns_none(
    endpoint: DaprInvokeEndpoint, status: int
) -> None:
    # 200 and 204 are both contractual successes per the issue's
    # acceptance criteria; neither must raise.
    await _run_cancel(endpoint, _cancel_handler(status))


async def test_cancel_activity_404_is_idempotent_noop(
    endpoint: DaprInvokeEndpoint, caplog: pytest.LogCaptureFixture
) -> None:
    # ARM has no record of the step \u2014 e.g. already purged or never
    # actually scheduled. Cancellation must succeed silently and
    # emit an INFO-level breadcrumb so operators can trace the
    # spurious cancel attempt without it looking like an error.
    caplog.set_level(logging.INFO, logger="custos_workflow.clients.activity_runtime")
    await _run_cancel(endpoint, _cancel_handler(404))

    matching = [
        rec
        for rec in caplog.records
        if rec.levelno == logging.INFO and "no record" in rec.getMessage()
    ]
    assert matching, "expected an INFO-level breadcrumb on the 404 path"
    record = matching[0]
    assert getattr(record, "run_id", None) == "run-1"
    assert getattr(record, "step_id", None) == "step-a"


async def test_cancel_activity_409_is_idempotent_noop(
    endpoint: DaprInvokeEndpoint, caplog: pytest.LogCaptureFixture
) -> None:
    # ARM reports the step has already terminated \u2014 success/failure
    # raced the cancel. Must collapse to a no-op with an INFO
    # breadcrumb, mirroring the 404 path.
    caplog.set_level(logging.INFO, logger="custos_workflow.clients.activity_runtime")
    await _run_cancel(endpoint, _cancel_handler(409))

    matching = [
        rec
        for rec in caplog.records
        if rec.levelno == logging.INFO and "already terminated" in rec.getMessage()
    ]
    assert matching, "expected an INFO-level breadcrumb on the 409 path"
    record = matching[0]
    assert getattr(record, "run_id", None) == "run-1"
    assert getattr(record, "step_id", None) == "step-a"


@pytest.mark.parametrize("status", [400, 401, 403, 422])
async def test_cancel_activity_other_4xx_raises_status_error(
    endpoint: DaprInvokeEndpoint, status: int
) -> None:
    # Anything in the 4xx range besides 404 / 409 is a contract
    # violation the caller must see (e.g. malformed body, auth
    # failure). Surface as OutboundRpcStatusError with the exact
    # status_code so RunController can decide whether to retry.
    with pytest.raises(OutboundRpcStatusError) as exc_info:
        await _run_cancel(endpoint, _cancel_handler(status, body=b"detail body"))
    assert exc_info.value.status_code == status
    # Body preview is included so the failure is debuggable.
    assert "detail body" in str(exc_info.value)


@pytest.mark.parametrize("status", [500, 502, 503, 504])
async def test_cancel_activity_5xx_raises_status_error(
    endpoint: DaprInvokeEndpoint, status: int
) -> None:
    # 5xx is always a status error \u2014 the run-cancel path turns it
    # into a RunControllerError. Specifically NOT a transport
    # error: a response *was* observed.
    with pytest.raises(OutboundRpcStatusError) as exc_info:
        await _run_cancel(endpoint, _cancel_handler(status))
    assert exc_info.value.status_code == status


async def test_cancel_activity_transport_timeout_raises_transport_error(
    endpoint: DaprInvokeEndpoint,
) -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        # ``httpx.MockTransport`` happily raises whatever the
        # handler does; ConnectTimeout is the canonical
        # transport-class failure.
        raise httpx.ConnectTimeout("simulated connect timeout")

    with pytest.raises(OutboundRpcTransportError) as exc_info:
        await _run_cancel(endpoint, handler)
    # The original httpx exception is preserved on __cause__ so
    # debuggers can drill into the root cause.
    assert isinstance(exc_info.value.__cause__, httpx.ConnectTimeout)


async def test_cancel_activity_connect_error_raises_transport_error(
    endpoint: DaprInvokeEndpoint,
) -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    with pytest.raises(OutboundRpcTransportError) as exc_info:
        await _run_cancel(endpoint, handler)
    assert isinstance(exc_info.value.__cause__, httpx.ConnectError)


async def test_cancel_activity_targets_arm_cancel_url(
    endpoint: DaprInvokeEndpoint, cancel_url: str
) -> None:
    captured: list[str] = []

    def handler(req: httpx.Request) -> httpx.Response:
        captured.append(str(req.url))
        return httpx.Response(204)

    await _run_cancel(endpoint, handler)

    assert captured == [cancel_url]
    # Sanity check: the URL targets the configured ARM app id and
    # the CancelActivity method, not ScheduleActivity.
    assert "/v1.0/invoke/activity-runtime-manager/method/CancelActivity" in captured[0]


async def test_cancel_activity_posts_camelcase_body(endpoint: DaprInvokeEndpoint) -> None:
    captured: list[dict[str, Any]] = []

    def handler(req: httpx.Request) -> httpx.Response:
        # Pull the JSON body off the wire so we can assert the
        # exact field-name casing ARM expects.
        import json as _json

        captured.append(_json.loads(req.content.decode("utf-8")))
        return httpx.Response(204)

    await _run_cancel(endpoint, handler, run_id="run-42", step_id="step-zeta")

    assert captured == [{"runId": "run-42", "stepId": "step-zeta"}]


async def test_cancel_activity_omits_idempotency_key_header(
    endpoint: DaprInvokeEndpoint,
) -> None:
    # Cancel is itself idempotent on ARM (404 / 409 are no-ops)
    # so the adapter doesn't need to (and per the design must
    # not) attach an Idempotency-Key header \u2014 retried cancels are
    # functionally identical to the first.
    captured_headers: list[httpx.Headers] = []

    def handler(req: httpx.Request) -> httpx.Response:
        captured_headers.append(req.headers)
        return httpx.Response(204)

    await _run_cancel(endpoint, handler)

    assert captured_headers
    assert IDEMPOTENCY_HEADER.lower() not in {k.lower() for k in captured_headers[0]}
    assert captured_headers[0].get("content-type") == "application/json"


async def test_cancel_activity_timeout_propagated_to_post(
    endpoint: DaprInvokeEndpoint,
) -> None:
    captured: list[Any] = []

    def handler(req: httpx.Request) -> httpx.Response:
        captured.append(req.extensions.get("timeout"))
        return httpx.Response(204)

    await _run_cancel(endpoint, handler, timeout=3.25)

    assert captured
    extension = captured[0]
    assert isinstance(extension, dict)
    assert all(value == 3.25 for value in extension.values())


# ---------------------------------------------------------------------------
# Constructor + defaults
# ---------------------------------------------------------------------------


async def test_default_timeout_matches_constant(endpoint: DaprInvokeEndpoint) -> None:
    transport = httpx.MockTransport(lambda req: httpx.Response(200))
    async with httpx.AsyncClient(transport=transport) as http:
        client = DaprActivityRuntimeClient(http_client=http, endpoint=endpoint)
        assert client.timeout == DEFAULT_OUTBOUND_RPC_TIMEOUT_SECONDS


async def test_timeout_override_honoured(endpoint: DaprInvokeEndpoint) -> None:
    transport = httpx.MockTransport(lambda req: httpx.Response(200))
    async with httpx.AsyncClient(transport=transport) as http:
        client = DaprActivityRuntimeClient(
            http_client=http,
            endpoint=endpoint,
            timeout=0.5,
        )
        assert client.timeout == 0.5


async def test_timeout_propagated_to_post(
    endpoint: DaprInvokeEndpoint, request_obj: ScheduleActivityRequest
) -> None:
    captured: list[Any] = []

    def handler(req: httpx.Request) -> httpx.Response:
        # ``httpx`` resolves the timeout into ``req.extensions``
        # when the user passes ``timeout=`` per-call.
        captured.append(req.extensions.get("timeout"))
        return httpx.Response(
            200,
            json={"class": "success", "outputs": {}, "error": None, "attempt": 2},
        )

    client = _make_client(endpoint, handler, timeout=2.5)
    await _drive(client, request_obj)

    assert captured
    # ``timeout`` extension is a dict with ``connect``/``read``/etc.
    # keys when set per-call; assert the values match our override.
    extension = captured[0]
    assert isinstance(extension, dict)
    assert all(value == 2.5 for value in extension.values())
