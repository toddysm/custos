"""WF-IMPL-083 — docs/developers/workflow-outbound-rpc.md examples test.

Pins the outbound-RPC developer documentation to the running code:

* Every fenced ```json``` request / response block validates against
  the in-code envelope dataclasses (via the adapters' own wire
  marshallers / parsers).
* The documented error-taxonomy ``kind`` set equals
  :data:`custos_workflow.clients._errors.LOCKED_OUTBOUND_RPC_KINDS`
  and its suggested-status column equals
  :data:`~custos_workflow.clients._errors.LOCKED_OUTBOUND_RPC_KIND_TO_STATUS`.
* The documented outcome label set equals
  :data:`custos_workflow._telemetry.LOCKED_OUTBOUND_RPC_OUTCOMES`.
* The documented span-attribute set equals
  :data:`custos_workflow._telemetry.LOCKED_OUTBOUND_RPC_SPAN_ATTRIBUTES`.
* The documented endpoint paths match
  :func:`~custos_workflow.clients._dapr_invoke.build_invoke_url`
  output for each adapter method.

Sibling pins live at ``tests/test_docs_examples_api.py`` (WF-IMPL-072)
and ``tests/test_docs_examples_step_coordinator.py`` (WF-IMPL-060).
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Iterator
from datetime import datetime
from pathlib import Path
from typing import Any, Final

import pytest

from custos_workflow._telemetry import (
    LOCKED_OUTBOUND_RPC_OUTCOMES,
    LOCKED_OUTBOUND_RPC_SPAN_ATTRIBUTES,
)
from custos_workflow.clients._dapr_invoke import (
    DaprInvokeEndpoint,
    build_invoke_url,
)
from custos_workflow.clients._errors import (
    LOCKED_OUTBOUND_RPC_KIND_TO_STATUS,
    LOCKED_OUTBOUND_RPC_KINDS,
)
from custos_workflow.clients.activity_runtime import (
    CANCEL_ACTIVITY_DAPR_METHOD,
    SCHEDULE_ACTIVITY_DAPR_METHOD,
    ScheduleActivityRequest,
)
from custos_workflow.clients.activity_runtime import (
    _envelope_from_wire as _schedule_envelope_from_wire,
)
from custos_workflow.clients.activity_runtime import (
    _request_to_wire as _schedule_request_to_wire,
)
from custos_workflow.clients.connector import (
    BIND_FOR_STEP_DAPR_METHOD,
    BindForStepRequest,
    ConnectorContext,
    SlotSpec,
)
from custos_workflow.clients.connector import (
    _request_to_wire as _bind_request_to_wire,
)
from custos_workflow.clients.connector import (
    _response_from_wire as _bind_response_from_wire,
)

# ---------------------------------------------------------------------------
# Doc location and parsing
# ---------------------------------------------------------------------------


#: Repo-root-relative path to the doc; the workflow-service test
#: tree is four levels deep from the repo root.
_DOC_PATH: Final[Path] = (
    Path(__file__).resolve().parents[4] / "docs" / "developers" / "workflow-outbound-rpc.md"
)


def _read_doc() -> str:
    assert _DOC_PATH.is_file(), f"developer doc missing at {_DOC_PATH}"
    return _DOC_PATH.read_text(encoding="utf-8")


_JSON_BLOCK_RE: Final[re.Pattern[str]] = re.compile(
    r"```json\n(.*?)\n```",
    re.DOTALL,
)


def _iter_json_blocks(doc_text: str) -> Iterator[tuple[int, str]]:
    """Yield ``(1-based-block-index, body)`` for every fenced ```json``` block."""

    for idx, match in enumerate(_JSON_BLOCK_RE.finditer(doc_text), start=1):
        yield idx, match.group(1)


def _parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value[:-1] + "+00:00" if value.endswith("Z") else value)


# ---------------------------------------------------------------------------
# Per-block validators against the in-code dataclasses
# ---------------------------------------------------------------------------


def _validate_schedule_request(body: dict[str, Any]) -> None:
    """Round-trip the documented ScheduleActivity request through the marshaller."""
    request = ScheduleActivityRequest(
        run_id=body["runId"],
        step_id=body["stepId"],
        attempt=body["attempt"],
        activity_ref=body["activityRef"],
        inputs=body["inputs"],
        connector_contexts={
            slot: ConnectorContext(
                slot_name=ctx["slotName"],
                handle=ctx["handle"],
                expires_at=_parse_iso(ctx["expiresAt"]),
                connector_kind=ctx["connectorKind"],
            )
            for slot, ctx in body["connectorContexts"].items()
        },
        deadline=_parse_iso(body["deadline"]),
    )
    assert _schedule_request_to_wire(request) == body, (
        "documented ScheduleActivity request does not match _request_to_wire output"
    )


def _validate_schedule_success(body: dict[str, Any]) -> None:
    envelope = _schedule_envelope_from_wire(body, expected_attempt=body["attempt"])
    assert envelope.class_ == "success"


def _validate_schedule_error(body: dict[str, Any]) -> None:
    envelope = _schedule_envelope_from_wire(body, expected_attempt=body["attempt"])
    assert envelope.class_ in {"retryable", "permanent", "cancelled"}
    assert envelope.outputs is None
    assert envelope.error is not None


def _validate_cancel_request(body: dict[str, Any]) -> None:
    # Mirrors the literal wire shape the adapter builds:
    # ``{"runId": run_id, "stepId": step_id}``.
    assert set(body) == {"runId", "stepId"}
    assert all(isinstance(body[key], str) for key in body)


def _validate_bind_request(body: dict[str, Any]) -> None:
    request = BindForStepRequest(
        step_key=body["stepKey"],
        slots=tuple(
            SlotSpec(
                name=slot["name"],
                connector_ref=slot["connectorRef"],
                capabilities=tuple(slot["capabilities"]),
            )
            for slot in body["slots"]
        ),
    )
    assert _bind_request_to_wire(request) == body, (
        "documented BindForStep request does not match _request_to_wire output"
    )


def _validate_bind_response(body: dict[str, Any]) -> None:
    response = _bind_response_from_wire(body)
    assert set(response.contexts) == set(body["contexts"])


#: Per-block-index validator. The block ordering mirrors the doc's
#: prose ordering; adding or reordering a fenced ```json``` block must
#: update this table or the exhaustiveness guard fails.
_BLOCK_VALIDATORS: Final[dict[int, Callable[[dict[str, Any]], None]]] = {
    1: _validate_schedule_request,  # § ScheduleActivity request
    2: _validate_schedule_success,  # § ScheduleActivity success response
    3: _validate_schedule_error,  # § ScheduleActivity error response
    4: _validate_cancel_request,  # § CancelActivity request
    5: _validate_bind_request,  # § BindForStep request
    6: _validate_bind_response,  # § BindForStep response
}


def test_every_json_block_has_a_validator() -> None:
    """Doc's fenced JSON blocks MUST map 1:1 onto the validator table."""
    blocks = dict(_iter_json_blocks(_read_doc()))
    assert set(blocks) == set(_BLOCK_VALIDATORS), (
        "fenced ```json``` block-set drifted from the validator table: "
        f"missing_validator={sorted(set(blocks) - set(_BLOCK_VALIDATORS))} "
        f"missing_block={sorted(set(_BLOCK_VALIDATORS) - set(blocks))}"
    )


@pytest.mark.parametrize("index", sorted(_BLOCK_VALIDATORS))
def test_json_block_validates_against_code(index: int) -> None:
    """Each documented JSON example MUST validate against the in-code dataclasses."""
    blocks = dict(_iter_json_blocks(_read_doc()))
    body = json.loads(blocks[index])
    _BLOCK_VALIDATORS[index](body)


# ---------------------------------------------------------------------------
# Locked error taxonomy
# ---------------------------------------------------------------------------


#: Mirror of the doc's "## Locked outbound-RPC error taxonomy" table.
#: ``None`` status means the doc renders ``—`` (status carried on the
#: exception). The locked table uses ``0`` as that placeholder.
_DOCUMENTED_KIND_TO_STATUS: Final[dict[str, int]] = {
    "workflow.client.transport": 503,
    "workflow.client.status": 0,
    "workflow.client.decode": 502,
    "workflow.client.cancelled": 499,
}

_DOCUMENTED_KINDS: Final[frozenset[str]] = frozenset(_DOCUMENTED_KIND_TO_STATUS)


def test_documented_kind_set_is_exhaustive() -> None:
    assert _DOCUMENTED_KINDS == LOCKED_OUTBOUND_RPC_KINDS, (
        "LOCKED_OUTBOUND_RPC_KINDS drifted from the doc's error taxonomy: "
        f"locked={sorted(LOCKED_OUTBOUND_RPC_KINDS)} documented={sorted(_DOCUMENTED_KINDS)}"
    )


def test_documented_kind_status_matches_locked_table() -> None:
    assert dict(LOCKED_OUTBOUND_RPC_KIND_TO_STATUS) == _DOCUMENTED_KIND_TO_STATUS, (
        "LOCKED_OUTBOUND_RPC_KIND_TO_STATUS drifted from the doc's status column: "
        f"locked={dict(LOCKED_OUTBOUND_RPC_KIND_TO_STATUS)} "
        f"documented={_DOCUMENTED_KIND_TO_STATUS}"
    )


# ---------------------------------------------------------------------------
# Locked outcomes + span attributes
# ---------------------------------------------------------------------------


#: Mirror of the doc's "### Instruments" + outcome prose.
_DOCUMENTED_OUTCOMES: Final[frozenset[str]] = frozenset(
    {"success", "transport", "retryable", "permanent", "cancelled"}
)

#: Mirror of the doc's "### Span" attribute table.
_DOCUMENTED_SPAN_ATTRIBUTES: Final[frozenset[str]] = frozenset(
    {
        "wf.client",
        "wf.method",
        "wf.run.id",
        "wf.step.id",
        "wf.attempt",
        "http.method",
        "http.url",
        "http.status_code",
        "wf.outcome",
        "wf.error.kind",
    }
)


def test_documented_outcomes_are_exhaustive() -> None:
    assert _DOCUMENTED_OUTCOMES == LOCKED_OUTBOUND_RPC_OUTCOMES, (
        "LOCKED_OUTBOUND_RPC_OUTCOMES drifted from the doc's observability section: "
        f"locked={sorted(LOCKED_OUTBOUND_RPC_OUTCOMES)} documented={sorted(_DOCUMENTED_OUTCOMES)}"
    )


def test_documented_span_attributes_are_exhaustive() -> None:
    assert _DOCUMENTED_SPAN_ATTRIBUTES == LOCKED_OUTBOUND_RPC_SPAN_ATTRIBUTES, (
        "LOCKED_OUTBOUND_RPC_SPAN_ATTRIBUTES drifted from the doc's span table: "
        f"locked={sorted(LOCKED_OUTBOUND_RPC_SPAN_ATTRIBUTES)} "
        f"documented={sorted(_DOCUMENTED_SPAN_ATTRIBUTES)}"
    )


# ---------------------------------------------------------------------------
# Endpoint paths
# ---------------------------------------------------------------------------


#: Mirror of the doc's "## Endpoints" table: ``(client, method)`` and
#: the documented path tail (sans scheme/host/port, with placeholder
#: app-id).
_DOCUMENTED_ENDPOINTS: Final[dict[tuple[str, str], str]] = {
    ("arm", SCHEDULE_ACTIVITY_DAPR_METHOD): "/v1.0/invoke/<arm-app-id>/method/ScheduleActivity",
    ("arm", CANCEL_ACTIVITY_DAPR_METHOD): "/v1.0/invoke/<arm-app-id>/method/CancelActivity",
    (
        "connector",
        BIND_FOR_STEP_DAPR_METHOD,
    ): "/v1.0/invoke/<connector-app-id>/method/BindForStep",
}


@pytest.mark.parametrize(("client", "method"), sorted(_DOCUMENTED_ENDPOINTS))
def test_documented_endpoint_path_matches_build_invoke_url(client: str, method: str) -> None:
    """Documented endpoint path tail MUST match build_invoke_url output."""
    app_id = "<arm-app-id>" if client == "arm" else "<connector-app-id>"
    endpoint = DaprInvokeEndpoint(host="127.0.0.1", http_port=3500, app_id=app_id)
    url = build_invoke_url(endpoint, method)
    tail = url.removeprefix("http://127.0.0.1:3500")
    assert tail == _DOCUMENTED_ENDPOINTS[(client, method)], (
        f"build_invoke_url tail {tail!r} drifted from documented path for {client}/{method}"
    )


def test_documented_endpoint_paths_appear_in_doc() -> None:
    """Every documented path tail MUST literally appear in the doc body."""
    doc = _read_doc()
    for path in _DOCUMENTED_ENDPOINTS.values():
        assert path in doc, f"documented endpoint path {path!r} not found in the doc body"
