"""WF-IMPL-046 \u2014 docs/developers/workflow-run-controller.md examples test.

Pins the Run Controller developer documentation to the running code:

* Every fenced ```yaml``` block in the doc is parsed, compiled
  through :func:`custos_workflow.compiler.compile`, and pushed
  through the in-memory :func:`tests.integration._harness.make_harness`.
* Every documented :class:`RunController` method name in the
  "Public API" table is asserted to exist on the live class
  (reflection-based; signature drift surfaces immediately).
* Every documented ``run.*`` kind in the "Error taxonomy" table is
  asserted to be a member of
  :data:`custos_workflow.runs.LOCKED_RUN_KINDS`.

The docs cannot drift away from the code; CI catches it here.

Sibling pin for the upstream compilation doc lives at
``tests/test_docs_examples.py`` (WF-IMPL-028).
"""

from __future__ import annotations

import inspect
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Final

import pytest

from custos_workflow.runs import (
    LIFECYCLE_KIND_WORKFLOW_CANCELLED,
    LIFECYCLE_KIND_WORKFLOW_STARTED,
    LOCKED_RUN_KINDS,
    RunController,
    RunOutput,
    RunStatus,
)
from tests.integration._harness import (
    WORKFLOW_VERSION_ID,
    WORKSPACE,
    make_harness,
)

# ---------------------------------------------------------------------------
# Doc location and parsing
# ---------------------------------------------------------------------------


#: Repo-root-relative path to the doc; the workflow-service test
#: tree is four levels deep from the repo root.
_DOC_PATH: Final[Path] = (
    Path(__file__).resolve().parents[4] / "docs" / "developers" / "workflow-run-controller.md"
)


def _read_doc() -> str:
    assert _DOC_PATH.is_file(), f"developer doc missing at {_DOC_PATH}"
    return _DOC_PATH.read_text(encoding="utf-8")


_YAML_BLOCK_RE: Final[re.Pattern[str]] = re.compile(
    r"```yaml\n(.*?)\n```",
    re.DOTALL,
)


def _iter_yaml_blocks(doc_text: str) -> Iterator[tuple[int, str]]:
    """Yield ``(1-based-block-index, body)`` for every fenced ```yaml``` block."""

    for idx, match in enumerate(_YAML_BLOCK_RE.finditer(doc_text), start=1):
        yield idx, match.group(1)


# ---------------------------------------------------------------------------
# Per-example expected terminal status
# ---------------------------------------------------------------------------


#: The doc's "## Worked examples" section pins three examples in
#: this exact order; this table is the test-side mirror of the
#: prose under each snippet. The exhaustiveness assertion below
#: pairs a documented snippet with an asserted outcome 1:1, so
#: adding a fourth worked example without extending this table
#: surfaces here as a clear failure.
_EXAMPLE_TERMINAL_STATUSES: Final[dict[int, str]] = {
    1: "succeeded",
    2: "cancelled",
    3: "succeeded",
}


@pytest.mark.parametrize(
    "block_idx,expected_status",
    sorted(_EXAMPLE_TERMINAL_STATUSES.items()),
)
@pytest.mark.asyncio
async def test_doc_example_yaml_runs_to_documented_terminal_status(
    block_idx: int, expected_status: str
) -> None:
    blocks = dict(_iter_yaml_blocks(_read_doc()))
    assert block_idx in blocks, (
        f"doc block #{block_idx} missing from docs/developers/workflow-run-controller.md"
    )
    doc_yaml = blocks[block_idx]

    h = make_harness(doc_yaml=doc_yaml)
    ref = await h.controller.start_run(
        workspace_id=WORKSPACE,
        workflow_version_id=WORKFLOW_VERSION_ID,
        inputs={},
        idempotency_key=f"doc-example-{block_idx}",
    )

    if expected_status == "cancelled":
        # Example 2 in the doc: caller cancels after start_run
        # returns. start_run always transitions the persisted row
        # queued -> running before returning (gate 6 of the start
        # algorithm), so the cancel call here moves the persisted
        # row running -> cancelling -> cancelled. The fake's
        # orchestrator completes synchronously during _schedule,
        # so the cancel-poll budget short-circuits on the first
        # poll (the runtime instance is already terminal).
        cancel_ref = await h.controller.cancel_run(
            workspace_id=WORKSPACE,
            run_id=ref.run_id,
            reason="operator stop",
        )
        assert cancel_ref.status is RunStatus.CANCELLED
        # The doc pins the lifecycle event sequence as
        # ``[workflow.started, workflow.cancelled]`` with the
        # cancellation event carrying the reason on ``extra``.
        kinds = [e.kind for e in h.publisher.events]
        assert kinds == [
            LIFECYCLE_KIND_WORKFLOW_STARTED,
            LIFECYCLE_KIND_WORKFLOW_CANCELLED,
        ]
        cancelled_event = h.publisher.events[-1]
        assert cancelled_event.extra == {"reason": "operator stop"}
        return

    # The succeeded path: the fake runtime drives the orchestrator
    # inline during ``_schedule``; by the time ``start_run`` returns,
    # the instance has already produced a terminal :class:`RunOutput`.
    state = h.runtime.instance(str(ref.run_id))
    assert isinstance(state.output, RunOutput), (
        f"doc example #{block_idx} did not reach a terminal RunOutput "
        f"(state.output={state.output!r})"
    )
    assert state.output.status == expected_status, (
        f"doc example #{block_idx} terminated at "
        f"{state.output.status!r}, expected {expected_status!r}"
    )
    # The doc pins lifecycle events on the succeeded path as
    # ``[workflow.started]`` only \u2014 terminal events are owned by
    # the reconciler (WF-IMPL-042) and out-of-scope here.
    kinds = [e.kind for e in h.publisher.events]
    assert kinds == [LIFECYCLE_KIND_WORKFLOW_STARTED]


def test_every_doc_yaml_block_has_an_asserted_terminal_status() -> None:
    """Exhaustiveness guard \u2014 no doc snippet runs un-exercised."""

    blocks = dict(_iter_yaml_blocks(_read_doc()))
    assert set(blocks) == set(_EXAMPLE_TERMINAL_STATUSES), (
        "doc yaml-block count drifted from the test's expected "
        f"terminal-status table: blocks={sorted(blocks)} "
        f"expected_keys={sorted(_EXAMPLE_TERMINAL_STATUSES)}"
    )


# ---------------------------------------------------------------------------
# Reflection-based contract pins
# ---------------------------------------------------------------------------


#: Mirror of the doc's "Public API" table. Every entry MUST resolve
#: to a public coroutine method on :class:`RunController`; the
#: exhaustiveness guard below fails the build if the table grows
#: without a paired source method (or vice versa).
_DOCUMENTED_PUBLIC_METHODS: Final[frozenset[str]] = frozenset(
    {
        "start_run",
        "cancel_run",
        "pause_run",
        "resume_run",
        "raise_external_event",
        "get_run",
        "list_runs",
    }
)


@pytest.mark.parametrize("method_name", sorted(_DOCUMENTED_PUBLIC_METHODS))
def test_documented_public_method_exists_on_run_controller(
    method_name: str,
) -> None:
    method = getattr(RunController, method_name, None)
    assert method is not None, f"documented method {method_name!r} missing from RunController"
    # Every documented method MUST be a coroutine function so the
    # doc's "All six methods are ``async``" claim is contract.
    assert inspect.iscoroutinefunction(method), (
        f"documented method {method_name!r} is not async; "
        "doc claim 'All six methods are async' has drifted"
    )


def test_documented_method_set_is_exhaustive() -> None:
    """The doc's Public API table MUST list every public coroutine method."""

    observed_async_publics = {
        name
        for name, member in inspect.getmembers(RunController)
        if inspect.iscoroutinefunction(member) and not name.startswith("_")
    }
    assert observed_async_publics == _DOCUMENTED_PUBLIC_METHODS, (
        "RunController gained or lost a public async method without "
        "updating docs/developers/workflow-run-controller.md \u00a7 Public API: "
        f"observed={sorted(observed_async_publics)} "
        f"documented={sorted(_DOCUMENTED_PUBLIC_METHODS)}"
    )


#: Mirror of the doc's "Error taxonomy" table. Every entry MUST
#: appear in :data:`LOCKED_RUN_KINDS`; the exhaustiveness guard
#: below fails the build on any drift.
_DOCUMENTED_RUN_KINDS: Final[frozenset[str]] = frozenset(
    {
        "run.not_found",
        "run.state_conflict",
        "run.state_corrupt",
        "run.runtime_unavailable",
    }
)


@pytest.mark.parametrize("kind", sorted(_DOCUMENTED_RUN_KINDS))
def test_documented_run_kind_is_locked(kind: str) -> None:
    assert kind in LOCKED_RUN_KINDS, (
        f"documented error kind {kind!r} is not a member of LOCKED_RUN_KINDS"
    )


def test_documented_run_kinds_match_locked_set() -> None:
    """The doc's Error taxonomy table MUST mirror :data:`LOCKED_RUN_KINDS`."""

    assert _DOCUMENTED_RUN_KINDS == LOCKED_RUN_KINDS, (
        "doc's Error taxonomy table drifted from LOCKED_RUN_KINDS: "
        f"documented={sorted(_DOCUMENTED_RUN_KINDS)} "
        f"locked={sorted(LOCKED_RUN_KINDS)}"
    )
