"""Tests for the StartRunValidator orchestrator (WF-IMPL-063).

Pins the four-gate algorithm documented in design.md § Operation:
Start Run:

1. **Workspace authorization** — call-context workspace must match
   the path workspace.
2. **Catalog lookup** — ``LookupError`` / pre-existing
   :class:`WorkflowVersionNotFoundError` translate to
   :class:`WorkflowVersionNotFoundError`.
3. **Inputs schema match** — raises :class:`InputsSchemaError`
   with stable JSON Pointer ``loc``.
4. **Idempotency ledger** — only consulted when a non-empty
   ``idempotency_key`` was supplied; a divergent fingerprint
   within TTL → :class:`IdempotencyConflictError`.

Each test injects in-memory :class:`CatalogClient` and
:class:`IdempotencyLedger` fakes so the validator runs hermetically.
"""

from __future__ import annotations

from typing import Any

import pytest
import yaml

from custos_workflow.call_context import CallContext
from custos_workflow.document.models import WorkflowDocument
from custos_workflow.runs.controller import WorkflowVersion
from custos_workflow.validator.errors import (
    IdempotencyConflictError,
    InputsSchemaError,
    WorkflowVersionNotFoundError,
    WorkspaceUnauthorizedError,
)
from custos_workflow.validator.idempotency_ledger import (
    InMemoryIdempotencyLedger,
    compute_request_fingerprint,
)
from custos_workflow.validator.service import StartRunValidator, ValidatedStartRun

WORKFLOW_ID = "wf-1"
WORKFLOW_VERSION_ID = "wfv-1"
WORKSPACE_ID = "ws-1"


def _doc_yaml(*, inputs_block: str = "") -> dict[str, Any]:
    """Render a minimal valid WorkflowDocument YAML with optional inputs block."""
    parsed: dict[str, Any] = yaml.safe_load(
        f"""
        apiVersion: custos.dev/v1
        kind: Workflow
        metadata:
          name: pipeline
          workspace: {WORKSPACE_ID}
        spec:
          {inputs_block}
          steps:
            - id: a
              let: {{x: '${{{{ true }}}}'}}
        """
    )
    return parsed


def _workflow_version(*, inputs_block: str = "") -> WorkflowVersion:
    doc = WorkflowDocument.model_validate(_doc_yaml(inputs_block=inputs_block))
    return WorkflowVersion(
        id=WORKFLOW_VERSION_ID,
        workflow_id=WORKFLOW_ID,
        name="pipeline",
        version_label="v1",
        document=doc,
    )


class _RecordingCatalogClient:
    """Catalog Protocol fake; records calls and optionally raises."""

    def __init__(
        self,
        version: WorkflowVersion | None = None,
        *,
        raise_on_call: Exception | None = None,
    ) -> None:
        self._version = version
        self._raise = raise_on_call
        self.calls: list[tuple[str, str]] = []

    async def get_workflow_version(
        self, workspace_id: str, workflow_version_id: str
    ) -> WorkflowVersion:
        self.calls.append((workspace_id, workflow_version_id))
        if self._raise is not None:
            raise self._raise
        assert self._version is not None
        return self._version


def _validator(
    *,
    version: WorkflowVersion | None = None,
    catalog_raises: Exception | None = None,
    ledger: InMemoryIdempotencyLedger | None = None,
) -> tuple[StartRunValidator, _RecordingCatalogClient, InMemoryIdempotencyLedger]:
    """Build a validator wired with in-memory fakes."""
    if version is None and catalog_raises is None:
        version = _workflow_version()
    catalog = _RecordingCatalogClient(version, raise_on_call=catalog_raises)
    ledger = ledger if ledger is not None else InMemoryIdempotencyLedger()
    return StartRunValidator(catalog=catalog, ledger=ledger), catalog, ledger


# ---------------------------------------------------------------------------
# Constructor argument validation
# ---------------------------------------------------------------------------


async def test_validate_rejects_empty_workspace_id() -> None:
    validator, _, _ = _validator()
    with pytest.raises(ValueError, match="workspace_id"):
        await validator.validate_start_run(
            workspace_id="",
            workflow_version_id=WORKFLOW_VERSION_ID,
        )


async def test_validate_rejects_empty_workflow_version_id() -> None:
    validator, _, _ = _validator()
    with pytest.raises(ValueError, match="workflow_version_id"):
        await validator.validate_start_run(
            workspace_id=WORKSPACE_ID,
            workflow_version_id="",
        )


# ---------------------------------------------------------------------------
# Gate 1: workspace authorization
# ---------------------------------------------------------------------------


async def test_workspace_mismatch_raises_workspace_unauthorized() -> None:
    validator, catalog, _ = _validator()
    with pytest.raises(WorkspaceUnauthorizedError) as info:
        await validator.validate_start_run(
            workspace_id=WORKSPACE_ID,
            workflow_version_id=WORKFLOW_VERSION_ID,
            call_context=CallContext(workspace="ws-other", principal="alice"),
        )
    err = info.value
    assert err.workspace_id == WORKSPACE_ID
    assert err.principal == "alice"
    # Catalog must not be hit when authorization fails.
    assert catalog.calls == []


async def test_matching_workspace_call_context_is_accepted() -> None:
    validator, _, _ = _validator()
    result = await validator.validate_start_run(
        workspace_id=WORKSPACE_ID,
        workflow_version_id=WORKFLOW_VERSION_ID,
        call_context=CallContext(workspace=WORKSPACE_ID, principal="alice"),
    )
    assert isinstance(result, ValidatedStartRun)


async def test_call_context_with_none_workspace_is_dev_mode_passthrough() -> None:
    """In dev mode the middleware emits ``workspace=None`` — must not gate."""
    validator, _, _ = _validator()
    result = await validator.validate_start_run(
        workspace_id=WORKSPACE_ID,
        workflow_version_id=WORKFLOW_VERSION_ID,
        call_context=CallContext(workspace=None, principal=None),
    )
    assert result.workspace_id == WORKSPACE_ID


async def test_no_call_context_is_dev_mode_passthrough() -> None:
    """Validator can run without a CallContext (e.g. internal RPC tests)."""
    validator, _, _ = _validator()
    result = await validator.validate_start_run(
        workspace_id=WORKSPACE_ID,
        workflow_version_id=WORKFLOW_VERSION_ID,
    )
    assert result.workspace_id == WORKSPACE_ID


# ---------------------------------------------------------------------------
# Gate 2: Catalog lookup
# ---------------------------------------------------------------------------


async def test_catalog_lookup_error_translates_to_not_found() -> None:
    """A generic ``LookupError`` (e.g. KeyError) maps to validator NotFound."""
    validator, _, _ = _validator(catalog_raises=KeyError("nope"))
    with pytest.raises(WorkflowVersionNotFoundError) as info:
        await validator.validate_start_run(
            workspace_id=WORKSPACE_ID,
            workflow_version_id=WORKFLOW_VERSION_ID,
        )
    assert info.value.workspace_id == WORKSPACE_ID
    assert info.value.workflow_version == WORKFLOW_VERSION_ID


async def test_catalog_existing_validator_not_found_passes_through() -> None:
    """A pre-formed :class:`WorkflowVersionNotFoundError` is not rewrapped."""
    sentinel = WorkflowVersionNotFoundError(
        "from upstream",
        workspace_id=WORKSPACE_ID,
        workflow_id=WORKFLOW_ID,
        workflow_version=WORKFLOW_VERSION_ID,
    )
    validator, _, _ = _validator(catalog_raises=sentinel)
    with pytest.raises(WorkflowVersionNotFoundError) as info:
        await validator.validate_start_run(
            workspace_id=WORKSPACE_ID,
            workflow_version_id=WORKFLOW_VERSION_ID,
        )
    assert info.value is sentinel


async def test_catalog_arbitrary_exception_propagates_unchanged() -> None:
    """Non-Lookup errors (e.g. transport failure) bubble for the catch-all."""

    class _TransportError(RuntimeError):
        pass

    validator, _, _ = _validator(catalog_raises=_TransportError("boom"))
    with pytest.raises(_TransportError):
        await validator.validate_start_run(
            workspace_id=WORKSPACE_ID,
            workflow_version_id=WORKFLOW_VERSION_ID,
        )


# ---------------------------------------------------------------------------
# Gate 3: inputs schema match
# ---------------------------------------------------------------------------


_NAME_REQUIRED_INPUTS_BLOCK = (
    "inputs:\n            name:\n              type: string\n              required: true"
)


async def test_inputs_failing_schema_raises_inputs_schema_error() -> None:
    version = _workflow_version(inputs_block=_NAME_REQUIRED_INPUTS_BLOCK)
    validator, _, _ = _validator(version=version)
    with pytest.raises(InputsSchemaError) as info:
        await validator.validate_start_run(
            workspace_id=WORKSPACE_ID,
            workflow_version_id=WORKFLOW_VERSION_ID,
            inputs={"name": 42},
        )
    issue = info.value.validation[0]
    assert issue["loc"] == "/name"
    assert info.value.workspace_id == WORKSPACE_ID


async def test_inputs_conforming_schema_passes_through() -> None:
    version = _workflow_version(inputs_block=_NAME_REQUIRED_INPUTS_BLOCK)
    validator, _, _ = _validator(version=version)
    result = await validator.validate_start_run(
        workspace_id=WORKSPACE_ID,
        workflow_version_id=WORKFLOW_VERSION_ID,
        inputs={"name": "alice"},
    )
    assert result.inputs == {"name": "alice"}


async def test_none_inputs_normalised_to_empty_dict() -> None:
    """``None`` inputs become ``{}`` in the result so the controller agrees."""
    validator, _, _ = _validator()
    result = await validator.validate_start_run(
        workspace_id=WORKSPACE_ID,
        workflow_version_id=WORKFLOW_VERSION_ID,
        inputs=None,
    )
    assert result.inputs == {}


# ---------------------------------------------------------------------------
# Gate 4: idempotency ledger
# ---------------------------------------------------------------------------


async def test_no_idempotency_key_skips_ledger() -> None:
    """Without a key the ledger is never consulted; replayed=False."""
    ledger = InMemoryIdempotencyLedger()
    validator, _, _ = _validator(ledger=ledger)
    result = await validator.validate_start_run(
        workspace_id=WORKSPACE_ID,
        workflow_version_id=WORKFLOW_VERSION_ID,
    )
    assert result.replayed is False
    assert result.idempotency_key is None
    assert ledger._snapshot() == {}


async def test_empty_idempotency_key_coerced_to_none_and_skips_ledger() -> None:
    """An empty string means "no key" — matches the controller's contract."""
    ledger = InMemoryIdempotencyLedger()
    validator, _, _ = _validator(ledger=ledger)
    result = await validator.validate_start_run(
        workspace_id=WORKSPACE_ID,
        workflow_version_id=WORKFLOW_VERSION_ID,
        idempotency_key="",
    )
    assert result.idempotency_key is None
    assert ledger._snapshot() == {}


async def test_first_call_records_second_call_replays() -> None:
    ledger = InMemoryIdempotencyLedger()
    validator, _, _ = _validator(ledger=ledger)
    first = await validator.validate_start_run(
        workspace_id=WORKSPACE_ID,
        workflow_version_id=WORKFLOW_VERSION_ID,
        inputs={},
        idempotency_key="k-1",
    )
    second = await validator.validate_start_run(
        workspace_id=WORKSPACE_ID,
        workflow_version_id=WORKFLOW_VERSION_ID,
        inputs={},
        idempotency_key="k-1",
    )
    assert first.replayed is False
    assert second.replayed is True
    assert second.request_fingerprint == first.request_fingerprint


async def test_divergent_fingerprint_raises_idempotency_conflict() -> None:
    """Ledger conflict requires the schema to accept both payloads; use a
    workflow version that declares the diverging input slot."""
    version = _workflow_version(
        inputs_block="inputs:\n            a:\n              type: integer",
    )
    ledger = InMemoryIdempotencyLedger()
    validator, _, _ = _validator(version=version, ledger=ledger)
    await validator.validate_start_run(
        workspace_id=WORKSPACE_ID,
        workflow_version_id=WORKFLOW_VERSION_ID,
        inputs={"a": 1},
        idempotency_key="k-1",
    )
    with pytest.raises(IdempotencyConflictError) as info:
        await validator.validate_start_run(
            workspace_id=WORKSPACE_ID,
            workflow_version_id=WORKFLOW_VERSION_ID,
            inputs={"a": 2},
            idempotency_key="k-1",
        )
    assert info.value.workspace_id == WORKSPACE_ID
    assert info.value.idempotency_key == "k-1"


async def test_validated_start_run_fingerprint_matches_helper() -> None:
    """The returned fingerprint is byte-equal with the standalone helper."""
    version = _workflow_version(
        inputs_block=(
            "inputs:\n            a:\n              type: integer\n"
            "            b:\n              type: string"
        ),
    )
    validator, _, _ = _validator(version=version)
    payload: dict[str, Any] = {"a": 1, "b": "x"}
    result = await validator.validate_start_run(
        workspace_id=WORKSPACE_ID,
        workflow_version_id=WORKFLOW_VERSION_ID,
        inputs=payload,
    )
    assert result.request_fingerprint == compute_request_fingerprint(WORKFLOW_VERSION_ID, payload)
