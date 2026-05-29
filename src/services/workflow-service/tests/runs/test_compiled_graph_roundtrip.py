"""WF-IMPL-033 tests — compiled ``ExecutionGraph`` JSON round-trip on Run.

Exercises the byte-equal :func:`to_json` ↔ :func:`from_json`
contract through the :class:`InProcessRunStore` adapter:

* **Round-trip property** — 200 Hypothesis-generated
  :class:`ExecutionGraph` instances (re-using the
  :func:`tests.test_determinism_property._well_typed_workflow_documents`
  document strategy + compiler so the graphs cover the same
  step-kind / dependency surface the compiler emits in
  production) round-trip byte-equal through
  ``compile`` → ``store.put_run`` → ``store.get_run`` →
  re-serialize. A regression here means corruption is being
  introduced on the persistence boundary, not at the WF-IMPL-018
  encoder (which already has its own determinism tests).

* **Corruption fixtures** — three classes of corrupted JSON
  injected directly into the store's private side-map raise
  :class:`RunStateCorruptError` with the offending ``run_id`` on
  :meth:`store.get_run` and :meth:`store.list_runs`:

  1. Truncated JSON (parser fails).
  2. Schema-version mismatch (envelope decoded but rejected by
     the WF-IMPL-018 schema-version gate).
  3. Garbage payload (valid JSON but does not satisfy the
     :class:`ExecutionGraph` shape).

The corruption tests reach into the private ``_graphs``
side-map intentionally — the public API does not expose a way
to construct a corrupt row, and we want the corruption surface
exercised end-to-end through :meth:`store.get_run` rather than
unit-testing :func:`from_json` (which is already covered by
``tests/test_graph_serialize.py``).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import cast

import pytest
from custos_spl.interfaces.metadata_store import MetadataStoreProvider
from hypothesis import HealthCheck, given, settings

from custos_workflow.compiler import compile as compile_workflow
from custos_workflow.document import WorkflowDocument
from custos_workflow.graph import ExecutionGraph, to_json
from custos_workflow.runs import (
    InProcessRunStore,
    RunId,
    RunRecord,
    RunStateCorruptError,
    RunStatus,
    derive_run_id,
)
from tests.runs._fakes import FakeMetadataStoreProvider
from tests.test_determinism_property import (
    _registry,
    _run_meta,
    _well_typed_workflow_documents,
)

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


WORKSPACE = "ws-001"
WORKFLOW = "wf-roundtrip"
VERSION = "1"


def _ts(seconds: int = 0) -> datetime:
    return datetime(2026, 1, 1, 0, 0, seconds, tzinfo=UTC)


def _store() -> InProcessRunStore:
    return InProcessRunStore(cast(MetadataStoreProvider, FakeMetadataStoreProvider()))


def _record(
    *,
    run_id: RunId,
    graph: ExecutionGraph | None,
) -> RunRecord:
    return RunRecord(
        workspace_id=WORKSPACE,
        run_id=run_id,
        workflow_id=WORKFLOW,
        workflow_version=VERSION,
        status=RunStatus.QUEUED,
        reason=None,
        started_at=_ts(0),
        updated_at=_ts(0),
        compiled_graph=graph,
    )


def _compile_doc(doc: WorkflowDocument) -> ExecutionGraph:
    return compile_workflow(doc, _run_meta(), _registry())


# ---------------------------------------------------------------------------
# Round-trip property (200 Hypothesis examples)
# ---------------------------------------------------------------------------


@settings(
    max_examples=200,
    deadline=None,
    suppress_health_check=(HealthCheck.too_slow, HealthCheck.filter_too_much),
)
@given(doc=_well_typed_workflow_documents())
@pytest.mark.asyncio
async def test_compiled_graph_round_trips_byte_equal_through_store(
    doc: WorkflowDocument,
) -> None:
    """``compile → put_run → get_run`` re-serializes byte-equal to the original.

    The store boundary MUST NOT mutate the compiled graph in any
    way; the test re-serializes via :func:`to_json` after the round
    trip and compares byte-for-byte against the pre-store payload.
    """

    store = _store()
    graph = _compile_doc(doc)
    original_json = to_json(graph)

    run_id = derive_run_id(WORKSPACE, f"key-{hash(original_json)}")
    record = _record(run_id=run_id, graph=graph)

    persisted = await store.put_run(record)
    assert persisted.compiled_graph is not None
    assert to_json(persisted.compiled_graph) == original_json

    fetched = await store.get_run(WORKSPACE, run_id)
    assert fetched is not None
    assert fetched.compiled_graph is not None
    assert to_json(fetched.compiled_graph) == original_json
    assert fetched.compiled_graph == graph


# ---------------------------------------------------------------------------
# Missing graph (no graph attached) is benign
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_without_compiled_graph_round_trips_with_none() -> None:
    store = _store()
    run_id = derive_run_id(WORKSPACE, "no-graph")
    record = _record(run_id=run_id, graph=None)

    await store.put_run(record)
    fetched = await store.get_run(WORKSPACE, run_id)
    assert fetched is not None
    assert fetched.compiled_graph is None


# ---------------------------------------------------------------------------
# Corruption fixtures
# ---------------------------------------------------------------------------


# Minimal payloads that exercise each corruption class against
# the WF-IMPL-018 ``from_json`` envelope gate.
#
# * ``_GARBAGE_PAYLOAD`` — valid JSON, no envelope fields. Hits
#   the ``graph_schema_version`` mismatch branch (``None != 1``).
# * ``_WRONG_SCHEMA_VERSION`` — explicit ``graph_schema_version``
#   set to a value the current build does not understand.
# * ``_TRUNCATED_PAYLOAD`` — invalid JSON (json.JSONDecodeError).
_GARBAGE_PAYLOAD = '{"hello": "world"}'
_WRONG_SCHEMA_VERSION = (
    '{"graph_schema_version": 999, "ast_schema_version": 1, '
    '"nodes": [], "edges": [], "topological_order": [], "metadata": {}}'
)
_TRUNCATED_PAYLOAD = '{"graph_schema_version": 1, "nodes": [{"id"'


CORRUPTION_CASES: list[tuple[str, str]] = [
    ("truncated", _TRUNCATED_PAYLOAD),
    ("schema-version-mismatch", _WRONG_SCHEMA_VERSION),
    ("garbage-payload", _GARBAGE_PAYLOAD),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(("label", "payload"), CORRUPTION_CASES)
async def test_corrupt_graph_raises_run_state_corrupt_on_get_run(label: str, payload: str) -> None:
    store = _store()
    run_id = derive_run_id(WORKSPACE, f"corrupt-{label}")
    # Seed a normal row so the SPL get_run succeeds; then poison
    # the side-map directly so the WF-IMPL-018 from_json gate
    # surfaces the corruption.
    await store.put_run(_record(run_id=run_id, graph=None))
    store._graphs[(WORKSPACE, str(run_id))] = payload

    with pytest.raises(RunStateCorruptError) as exc_info:
        await store.get_run(WORKSPACE, run_id)

    err = exc_info.value
    assert err.run_id == run_id
    assert err.kind == "run.state_corrupt"
    assert err.cause is not None and err.cause != ""


@pytest.mark.asyncio
async def test_corrupt_graph_raises_run_state_corrupt_on_list_runs() -> None:
    store = _store()
    run_id = derive_run_id(WORKSPACE, "corrupt-on-list")
    await store.put_run(_record(run_id=run_id, graph=None))
    store._graphs[(WORKSPACE, str(run_id))] = _TRUNCATED_PAYLOAD

    with pytest.raises(RunStateCorruptError) as exc_info:
        await store.list_runs(WORKSPACE)

    assert exc_info.value.run_id == run_id
    assert exc_info.value.kind == "run.state_corrupt"
