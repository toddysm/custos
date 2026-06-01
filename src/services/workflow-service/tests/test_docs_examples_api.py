"""WF-IMPL-072 — docs/developers/workflow-api.md examples test.

Pins the API Adapter + Validator developer documentation to the
running code:

* Every documented endpoint in the REST / Internal RPC tables is
  asserted to be a live FastAPI route on the assembled
  :func:`custos_workflow.create_app` app.
* The set of documented endpoints is asserted to be exhaustive
  against the live router-set — adding a route without
  documenting it (or vice-versa) fails the build.
* Every documented ``code`` in the locked error-taxonomy table
  is asserted to be a member of
  :data:`custos_workflow.api.errors.LOCKED_API_KINDS`; the
  exhaustiveness guard fails the build on any drift.
* Every documented model in the REST / RPC tables is asserted to
  be an importable public name of
  :mod:`custos_workflow.api`; the doc cannot reference a model
  the wire surface doesn't expose.
* Every fenced ```json``` block in the doc is parsed and validated
  against the matching live Pydantic model when the block carries
  a documented model name on the preceding heading or paragraph;
  Problem+JSON blocks validate against
  :class:`custos_workflow.api.errors.ProblemDetail` and assert
  the ``code`` is in :data:`LOCKED_API_KINDS`.

Sibling pin for the Step Coordinator doc lives at
``tests/test_docs_examples_step_coordinator.py`` (WF-IMPL-060).
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Final

import pytest
from fastapi.routing import APIRoute
from pydantic import BaseModel

from custos_workflow import create_app
from custos_workflow.api import (
    CancelRunRequest,
    RaiseExternalEventRequest,
    RunListResponse,
    RunRefResponse,
    RunResponse,
    StartRunRequest,
    StepResponse,
)
from custos_workflow.api.errors import (
    LOCKED_API_KIND_TO_STATUS,
    LOCKED_API_KINDS,
    ProblemDetail,
)
from custos_workflow.api.models import (
    InternalCancelRunRequest,
    InternalStartRunRequest,
)

# ---------------------------------------------------------------------------
# Doc location and parsing
# ---------------------------------------------------------------------------


#: Repo-root-relative path to the doc; the workflow-service test
#: tree is four levels deep from the repo root.
_DOC_PATH: Final[Path] = (
    Path(__file__).resolve().parents[4] / "docs" / "developers" / "workflow-api.md"
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


# ---------------------------------------------------------------------------
# Documented endpoints (REST + Internal RPC)
# ---------------------------------------------------------------------------


#: Mirror of the doc's "## REST API" and "## Internal RPC API"
#: tables. Each entry pairs ``(method, path)`` with the documented
#: success status. The exhaustiveness guard below pairs this set
#: against the live router-set so adding a route without
#: documenting it (or vice-versa) fails the build.
_DOCUMENTED_ENDPOINTS: Final[frozenset[tuple[str, str, int]]] = frozenset(
    {
        # Public REST surface.
        ("POST", "/v1/workspaces/{ws}/runs", 202),
        ("GET", "/v1/workspaces/{ws}/runs", 200),
        ("GET", "/v1/workspaces/{ws}/runs/{run_id}", 200),
        ("POST", "/v1/workspaces/{ws}/runs/{run_id}:cancel", 202),
        ("GET", "/v1/workspaces/{ws}/runs/{run_id}/steps/{step_id}", 200),
        ("GET", "/v1/workspaces/{ws}/runs/{run_id}/steps/{step_id}/logs", 501),
        # Internal RPC surface.
        ("POST", "/internal/runs:start", 202),
        ("POST", "/internal/runs/{run_id}:cancel", 202),
        ("POST", "/internal/runs/{run_id}/steps/{step_id}:raiseEvent", 202),
    }
)


def _live_routes() -> frozenset[tuple[str, str, int]]:
    """Reflect the assembled app's route table to ``(method, path, status)``.

    Uses ``create_app(require_call_context=False)`` so the app
    assembles without standing up any real backend; the route
    table is the same shape in dev and prod modes.

    Filters out the Kubernetes liveness / readiness probes
    (``/healthz`` and ``/readyz``) — those are operational
    surfaces owned by ``custos_workflow.healthz``, not part of
    the public API contract this doc covers.
    """
    app = create_app(require_call_context=False)
    out: set[tuple[str, str, int]] = set()
    for route in app.routes:
        if not isinstance(route, APIRoute):
            continue
        if route.path in {"/healthz", "/readyz"}:
            continue
        for method in route.methods or set():
            # FastAPI auto-mounts HEAD for GET; the doc never lists
            # HEAD so we filter it here to keep the exhaustiveness
            # guard meaningful.
            if method == "HEAD":
                continue
            out.add((method, route.path, route.status_code or 200))
    return frozenset(out)


@pytest.mark.parametrize(
    "method,path,status",
    sorted(_DOCUMENTED_ENDPOINTS),
)
def test_documented_endpoint_is_live(method: str, path: str, status: int) -> None:
    """Every documented endpoint MUST be a live FastAPI route."""

    live = _live_routes()
    assert (method, path, status) in live, (
        f"documented endpoint {method} {path} (status {status}) is not "
        f"a live FastAPI route on the assembled app; closest live "
        f"matches: {sorted(r for r in live if r[1] == path)}"
    )


def test_documented_endpoint_set_is_exhaustive() -> None:
    """Doc's endpoint tables MUST cover every live FastAPI route."""

    live = _live_routes()
    assert live == _DOCUMENTED_ENDPOINTS, (
        "live FastAPI route-set drifted from "
        "docs/developers/workflow-api.md REST + Internal RPC tables: "
        f"missing_from_doc={sorted(live - _DOCUMENTED_ENDPOINTS)} "
        f"missing_from_app={sorted(_DOCUMENTED_ENDPOINTS - live)}"
    )


# ---------------------------------------------------------------------------
# Locked error taxonomy
# ---------------------------------------------------------------------------


#: Mirror of the doc's "## Locked error taxonomy" table.
#: Each entry pairs the documented ``code`` with the documented
#: HTTP status. Asserted to equal
#: :data:`LOCKED_API_KIND_TO_STATUS` exactly (drift fails the
#: build) AND every documented kind is asserted to be a member
#: of :data:`LOCKED_API_KINDS`.
_DOCUMENTED_API_KIND_TO_STATUS: Final[dict[str, int]] = {
    "workflow.run_not_found": 404,
    "workflow.run_state_conflict": 409,
    "workflow.workflow_runtime_unavailable": 503,
    "workflow.validator.workflow_version_not_found": 404,
    "workflow.validator.inputs_schema_error": 422,
    "workflow.validator.idempotency_conflict": 409,
    "workflow.validator.workspace_unauthorized": 403,
    "workflow.step_not_found": 404,
    "workflow.api.not_implemented": 501,
    "workflow.api.bad_request": 400,
}

_DOCUMENTED_API_KINDS: Final[frozenset[str]] = frozenset(_DOCUMENTED_API_KIND_TO_STATUS)


@pytest.mark.parametrize("kind", sorted(_DOCUMENTED_API_KINDS))
def test_documented_api_kind_is_locked(kind: str) -> None:
    assert kind in LOCKED_API_KINDS, (
        f"documented error kind {kind!r} is not a member of LOCKED_API_KINDS"
    )


def test_documented_api_kind_status_matches_locked_table() -> None:
    """Doc's error-taxonomy status column MUST equal LOCKED_API_KIND_TO_STATUS."""

    assert _DOCUMENTED_API_KIND_TO_STATUS == LOCKED_API_KIND_TO_STATUS, (
        "LOCKED_API_KIND_TO_STATUS drifted from "
        "docs/developers/workflow-api.md § Locked error taxonomy: "
        f"locked={LOCKED_API_KIND_TO_STATUS} "
        f"documented={_DOCUMENTED_API_KIND_TO_STATUS}"
    )


def test_documented_api_kind_set_is_exhaustive() -> None:
    """Doc's error taxonomy MUST list every locked API kind."""

    assert _DOCUMENTED_API_KINDS == LOCKED_API_KINDS, (
        "LOCKED_API_KINDS drifted from "
        "docs/developers/workflow-api.md § Locked error taxonomy: "
        f"locked={sorted(LOCKED_API_KINDS)} "
        f"documented={sorted(_DOCUMENTED_API_KINDS)}"
    )


# ---------------------------------------------------------------------------
# Documented model surface
# ---------------------------------------------------------------------------


#: Mirror of the doc's REST + Internal RPC tables. Each entry
#: pairs the documented model name with the module that
#: re-exports it: the **public REST** models live on
#: ``custos_workflow.api.__all__`` (the wire surface external
#: callers import from); the **Internal RPC** models live one
#: module deeper on ``custos_workflow.api.models`` because they
#: are not part of the cluster-external public surface. The
#: ``test_documented_model_is_a_live_import`` test reflects
#: ``__all__`` membership against the documented source-of-truth
#: module so a model silently moving (e.g. a public model
#: dropped from the top-level re-exports) fails the build.
_DOCUMENTED_MODELS: Final[dict[str, tuple[type[BaseModel], str]]] = {
    "StartRunRequest": (StartRunRequest, "custos_workflow.api"),
    "RunRefResponse": (RunRefResponse, "custos_workflow.api"),
    "RunResponse": (RunResponse, "custos_workflow.api"),
    "RunListResponse": (RunListResponse, "custos_workflow.api"),
    "CancelRunRequest": (CancelRunRequest, "custos_workflow.api"),
    "StepResponse": (StepResponse, "custos_workflow.api"),
    "RaiseExternalEventRequest": (
        RaiseExternalEventRequest,
        "custos_workflow.api",
    ),
    "InternalStartRunRequest": (
        InternalStartRunRequest,
        "custos_workflow.api.models",
    ),
    "InternalCancelRunRequest": (
        InternalCancelRunRequest,
        "custos_workflow.api.models",
    ),
}


@pytest.mark.parametrize("name", sorted(_DOCUMENTED_MODELS))
def test_documented_model_is_a_live_import(name: str) -> None:
    """Every documented model MUST be live at its documented source-of-truth.

    "Live" means: the documented module re-exports the name on
    its ``__all__`` AND the imported object is the same class
    object the json-block validation tests bind against. Drift
    catches both "model deleted" and "model silently moved out
    of the documented source-of-truth module" failure modes.
    """

    import importlib

    expected, module_name = _DOCUMENTED_MODELS[name]
    module = importlib.import_module(module_name)
    assert name in getattr(module, "__all__", []), (
        f"documented model {name!r} is not a member of {module_name}.__all__"
    )
    live = getattr(module, name, None)
    assert live is not None, f"documented model {name!r} is not importable from {module_name}"
    assert live is expected, (
        f"documented model {name!r} re-exported from {module_name} is not "
        f"the same class object as the one this test validates json blocks "
        f"against"
    )


# ---------------------------------------------------------------------------
# Fenced JSON blocks — validation against live models
# ---------------------------------------------------------------------------


#: Per-block-index expected model. The block ordering mirrors the
#: doc's prose ordering; adding or reordering a fenced ```json```
#: block must update this table or the exhaustiveness guard fails.
#:
#: ``None`` means "free-form example, no model to bind to" (used
#: for, e.g., the design.md Configuration example envelope which
#: is not a wire payload). The block still must parse as JSON.
_BLOCK_MODELS: Final[dict[int, type[BaseModel] | None]] = {
    1: StartRunRequest,  # § Request envelope: StartRunRequest
    2: RunRefResponse,  # § Response envelope: RunRefResponse
    3: RunResponse,  # § Response envelope: RunResponse
    4: CancelRunRequest,  # § Cancel envelope: CancelRunRequest
    5: RunListResponse,  # § List envelope: RunListResponse
    6: StepResponse,  # § Step envelope: StepResponse
    7: ProblemDetail,  # § Step log stream stub
    8: InternalStartRunRequest,  # § InternalStartRunRequest
    9: InternalCancelRunRequest,  # § InternalCancelRunRequest
    10: RaiseExternalEventRequest,  # § RaiseExternalEventRequest
    11: ProblemDetail,  # § Envelope shape (RFC 7807 + extensions)
    12: RunRefResponse,  # § Example 1 response body
    13: ProblemDetail,  # § Example 3 422 envelope
}


@pytest.mark.parametrize(
    "block_idx,model",
    sorted(_BLOCK_MODELS.items()),
)
def test_documented_json_block_parses_under_its_model(
    block_idx: int, model: type[BaseModel] | None
) -> None:
    """Every documented ```json``` block MUST parse + validate."""

    blocks = dict(_iter_json_blocks(_read_doc()))
    assert block_idx in blocks, (
        f"doc json-block #{block_idx} missing from docs/developers/workflow-api.md"
    )
    body = blocks[block_idx]
    # First: every documented json block MUST be valid JSON.
    payload: Any = json.loads(body)
    if model is None:
        return
    # Pydantic validates against the live model; the camelCase
    # alias generator on `_CamelModel` means the doc's wire
    # spelling round-trips through `model_validate` without any
    # snake_case translation in this test.
    instance = model.model_validate(payload)
    # ProblemDetail's `code` MUST be a documented kind; the
    # generic validation above lets it through (the field is just
    # a string) so we tighten the contract here.
    if model is ProblemDetail:
        assert isinstance(instance, ProblemDetail)
        assert instance.code in LOCKED_API_KINDS, (
            f"doc json-block #{block_idx} carries `code={instance.code!r}` "
            f"which is not a member of LOCKED_API_KINDS"
        )


def test_every_doc_json_block_has_an_asserted_model() -> None:
    """Exhaustiveness guard — no doc json snippet runs un-exercised."""

    blocks = dict(_iter_json_blocks(_read_doc()))
    assert set(blocks) == set(_BLOCK_MODELS), (
        "doc json-block count drifted from the test's expected "
        f"model table: blocks={sorted(blocks)} "
        f"expected_keys={sorted(_BLOCK_MODELS)}"
    )
