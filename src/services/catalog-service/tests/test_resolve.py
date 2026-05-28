"""Tests for :mod:`custos_catalog.resolve` (CS-IMPL-008)."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import pytest

from custos_catalog.normalize import (
    NormalizedWorkflow,
    RefResolutionSlot,
    normalize_workflow,
)
from custos_catalog.resolve import (
    ActivityTypeDeprecated,
    ActivityTypeNotFound,
    ConnectorInstanceMissing,
    CrossWorkspaceSubworkflowRejected,
    InvalidReferenceFormat,
    MajorMinorRefRejected,
    ResolvedActivityRef,
    ResolvedSubworkflowRef,
    ShortFormRefRejected,
    StubConnectorClient,
    SubworkflowDeprecated,
    SubworkflowNotFound,
    apply_resolutions,
    collect_connector_instance_calls,
    resolve_activity_ref,
    resolve_connector_instance,
    resolve_subworkflow_ref,
)

# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _ActivityRow:
    """Lightweight stand-in for `custos_spl.ActivityTypeVersion`."""

    namespace: str
    type: str
    version: str
    digest: str
    parent_deprecated: bool = False


class FakeActivityRegistry:
    """Hand-rolled fake satisfying :class:`ActivityTypeRegistry`."""

    def __init__(self, rows: list[_ActivityRow]) -> None:
        self.rows = rows

    async def resolve(self, namespace: str, type: str, semver_range: str) -> _ActivityRow | None:
        """Return the highest row whose version satisfies the PEP 440 spec.

        ``resolve_activity_ref`` translates the catalog ref grammar's
        ``@MAJOR`` form into a ``">=N,<N+1"`` specifier before
        forwarding to a registry, so the fake must speak PEP 440.
        """
        from packaging.specifiers import SpecifierSet
        from packaging.version import Version

        spec = SpecifierSet(semver_range)

        def _ok(row: _ActivityRow) -> bool:
            return row.namespace == namespace and row.type == type and Version(row.version) in spec

        matches = [r for r in self.rows if _ok(r) and not r.parent_deprecated]
        if not matches:
            # The store would still return a deprecated match if it's
            # the only one — we return it so the resolver can surface
            # ActivityTypeDeprecated.
            deprecated = [r for r in self.rows if _ok(r)]
            if not deprecated:
                return None
            return sorted(deprecated, key=lambda r: _semver_key(r.version))[-1]
        return sorted(matches, key=lambda r: _semver_key(r.version))[-1]

    async def get_activity_type_version(
        self,
        namespace: str,
        type: str,
        version: str,
    ) -> _ActivityRow | None:
        for r in self.rows:
            if r.namespace == namespace and r.type == type and r.version == version:
                return r
        return None


def _semver_key(v: str) -> tuple[int, ...]:
    return tuple(int(p) for p in v.split("."))


@dataclass(frozen=True, slots=True)
class _WorkflowVersionRow:
    workspace_id: str
    workflow_id: str
    version: str
    parent_deprecated: bool = False


class FakeDefinitionStore:
    """Hand-rolled fake satisfying :class:`SubworkflowResolver`."""

    def __init__(
        self,
        by_id: dict[UUID, _WorkflowVersionRow] | None = None,
        by_name: dict[tuple[str, str, str], _WorkflowVersionRow] | None = None,
    ) -> None:
        self.by_id = by_id or {}
        self.by_name = by_name or {}

    async def get_workflow_version_by_id(
        self,
        workflow_version_id: UUID,
    ) -> _WorkflowVersionRow | None:
        return self.by_id.get(workflow_version_id)

    async def get_workflow_version_by_name(
        self,
        workspace: str,
        name: str,
        version: str,
    ) -> _WorkflowVersionRow | None:
        return self.by_name.get((workspace, name, version))


# ---------------------------------------------------------------------------
# Activity ref resolution
# ---------------------------------------------------------------------------


async def test_resolve_activity_ref_major_pin_picks_latest_within_major() -> None:
    registry = FakeActivityRegistry(
        [
            _ActivityRow("custos.builtin", "vuln-scan", "2.0.0", "sha:a"),
            _ActivityRow("custos.builtin", "vuln-scan", "2.4.1", "sha:b"),
            _ActivityRow("custos.builtin", "vuln-scan", "1.0.0", "sha:c"),
        ],
    )
    resolved = await resolve_activity_ref(
        "custos.builtin/vuln-scan@2",
        registry=registry,
    )
    assert resolved == ResolvedActivityRef(
        canonical_ref="custos.builtin/vuln-scan@2.4.1",
        namespace="custos.builtin",
        type="vuln-scan",
        version="2.4.1",
        digest="sha:b",
    )


async def test_resolve_activity_ref_exact_version_returns_that_row() -> None:
    registry = FakeActivityRegistry(
        [_ActivityRow("ns", "t", "1.2.3", "sha:x")],
    )
    resolved = await resolve_activity_ref("ns/t@1.2.3", registry=registry)
    assert resolved.canonical_ref == "ns/t@1.2.3"
    assert resolved.digest == "sha:x"


async def test_resolve_activity_ref_rejects_major_minor() -> None:
    registry = FakeActivityRegistry([])
    with pytest.raises(MajorMinorRefRejected) as exc:
        await resolve_activity_ref("ns/t@1.2", registry=registry)
    assert exc.value.code == "resolve.major_minor_rejected"


async def test_resolve_activity_ref_rejects_short_form() -> None:
    registry = FakeActivityRegistry([])
    with pytest.raises(ShortFormRefRejected) as exc:
        await resolve_activity_ref("vuln-scan@2", registry=registry)
    assert exc.value.code == "resolve.short_form_rejected"


async def test_resolve_activity_ref_rejects_garbage() -> None:
    registry = FakeActivityRegistry([])
    with pytest.raises(InvalidReferenceFormat):
        await resolve_activity_ref("nonsense", registry=registry)


async def test_resolve_activity_ref_not_found_for_unknown_major() -> None:
    registry = FakeActivityRegistry(
        [_ActivityRow("ns", "t", "1.0.0", "sha:x")],
    )
    with pytest.raises(ActivityTypeNotFound):
        await resolve_activity_ref("ns/t@9", registry=registry)


async def test_resolve_activity_ref_rejects_deprecated() -> None:
    registry = FakeActivityRegistry(
        [_ActivityRow("ns", "t", "1.0.0", "sha:x", parent_deprecated=True)],
    )
    with pytest.raises(ActivityTypeDeprecated):
        await resolve_activity_ref("ns/t@1.0.0", registry=registry)


async def test_resolve_activity_ref_rejects_bad_version_shape() -> None:
    registry = FakeActivityRegistry([])
    with pytest.raises(InvalidReferenceFormat):
        await resolve_activity_ref("ns/t@latest", registry=registry)


# ---------------------------------------------------------------------------
# Sub-workflow ref resolution
# ---------------------------------------------------------------------------


async def test_resolve_subworkflow_uuid_path() -> None:
    wf_uuid = UUID("12345678-1234-1234-1234-123456789abc")
    store = FakeDefinitionStore(
        by_id={
            wf_uuid: _WorkflowVersionRow("ws-1", "child", "1.0.0"),
        },
    )
    resolved = await resolve_subworkflow_ref(
        str(wf_uuid),
        store=store,
        workspace_id="ws-1",
    )
    assert resolved == ResolvedSubworkflowRef(
        canonical_ref="ws-1/child@1.0.0",
        workspace_id="ws-1",
        workflow_id="child",
        version="1.0.0",
    )


async def test_resolve_subworkflow_uuid_rejects_cross_workspace() -> None:
    wf_uuid = UUID("12345678-1234-1234-1234-123456789abc")
    store = FakeDefinitionStore(
        by_id={
            wf_uuid: _WorkflowVersionRow("other-ws", "child", "1.0.0"),
        },
    )
    with pytest.raises(CrossWorkspaceSubworkflowRejected):
        await resolve_subworkflow_ref(
            str(wf_uuid),
            store=store,
            workspace_id="ws-1",
        )


async def test_resolve_subworkflow_uuid_not_found() -> None:
    store = FakeDefinitionStore()
    with pytest.raises(SubworkflowNotFound):
        await resolve_subworkflow_ref(
            "12345678-1234-1234-1234-123456789abc",
            store=store,
            workspace_id="ws-1",
        )


async def test_resolve_subworkflow_triple_path() -> None:
    store = FakeDefinitionStore(
        by_name={
            ("ws-1", "child", "1.0.0"): _WorkflowVersionRow("ws-1", "child", "1.0.0"),
        },
    )
    resolved = await resolve_subworkflow_ref(
        "ws-1/child@1.0.0",
        store=store,
        workspace_id="ws-1",
    )
    assert resolved.canonical_ref == "ws-1/child@1.0.0"


async def test_resolve_subworkflow_triple_rejects_cross_workspace() -> None:
    store = FakeDefinitionStore()
    with pytest.raises(CrossWorkspaceSubworkflowRejected):
        await resolve_subworkflow_ref(
            "other-ws/child@1.0.0",
            store=store,
            workspace_id="ws-1",
        )


async def test_resolve_subworkflow_triple_not_found() -> None:
    store = FakeDefinitionStore()
    with pytest.raises(SubworkflowNotFound):
        await resolve_subworkflow_ref(
            "ws-1/child@1.0.0",
            store=store,
            workspace_id="ws-1",
        )


async def test_resolve_subworkflow_rejects_deprecated() -> None:
    store = FakeDefinitionStore(
        by_name={
            ("ws-1", "child", "1.0.0"): _WorkflowVersionRow(
                "ws-1",
                "child",
                "1.0.0",
                parent_deprecated=True,
            ),
        },
    )
    with pytest.raises(SubworkflowDeprecated):
        await resolve_subworkflow_ref(
            "ws-1/child@1.0.0",
            store=store,
            workspace_id="ws-1",
        )


async def test_resolve_subworkflow_rejects_garbage() -> None:
    store = FakeDefinitionStore()
    with pytest.raises(InvalidReferenceFormat):
        await resolve_subworkflow_ref("garbage", store=store, workspace_id="ws-1")


# ---------------------------------------------------------------------------
# Stub connector client
# ---------------------------------------------------------------------------


async def test_stub_connector_client_returns_true(caplog: pytest.LogCaptureFixture) -> None:
    client = StubConnectorClient()
    with caplog.at_level(logging.WARNING, logger="custos_catalog.clients.connector"):
        assert await client.exists_connector_instance("ws-1", "name-a") is True
        assert await client.exists_connector_instance("ws-1", "name-b") is True
        assert await client.exists_connector_instance("ws-1", "name-c") is True
    # Exactly one WARNING for the batch, not per call.
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert client.calls == (("ws-1", "name-a"), ("ws-1", "name-b"), ("ws-1", "name-c"))


async def test_stub_connector_client_resets_batch(caplog: pytest.LogCaptureFixture) -> None:
    client = StubConnectorClient()
    with caplog.at_level(logging.WARNING, logger="custos_catalog.clients.connector"):
        await client.exists_connector_instance("ws-1", "a")
        client.reset_batch()
        await client.exists_connector_instance("ws-1", "b")
    # Two warnings — one per batch.
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 2


async def test_resolve_connector_instance_raises_when_client_returns_false() -> None:
    class _NegativeClient:
        async def exists_connector_instance(self, workspace_id: str, name: str) -> bool:
            return False

    with pytest.raises(ConnectorInstanceMissing) as exc:
        await resolve_connector_instance(
            "missing",
            client=_NegativeClient(),
            workspace_id="ws-1",
        )
    assert exc.value.code == "resolve.connector_instance_missing"


# ---------------------------------------------------------------------------
# apply_resolutions
# ---------------------------------------------------------------------------


def _workflow_doc() -> dict[str, Any]:
    return {
        "apiVersion": "custos.dev/v1",
        "kind": "Workflow",
        "metadata": {"name": "wf"},
        "spec": {
            "triggers": [{"type": "x", "connector": "prod-registry"}],
            "steps": [
                {
                    "id": "scan",
                    "activity": "custos.builtin/vuln-scan@2",
                    "connector": "prod-registry",
                },
            ],
        },
    }


async def test_apply_resolutions_substitutes_activity_ref() -> None:
    norm = normalize_workflow(_workflow_doc())
    registry = FakeActivityRegistry(
        [
            _ActivityRow("custos.builtin", "vuln-scan", "2.4.1", "sha:abc"),
        ],
    )
    store = FakeDefinitionStore()
    client = StubConnectorClient()
    result = await apply_resolutions(
        norm,
        activity_registry=registry,
        definition_store=store,
        connector_client=client,
        workspace_id="ws-1",
    )
    assert isinstance(result, NormalizedWorkflow)
    assert result.slots == ()
    assert result.document["spec"]["steps"][0]["activity"] == "custos.builtin/vuln-scan@2.4.1"
    # Connector position is NOT rewritten.
    assert result.document["spec"]["steps"][0]["connector"] == "prod-registry"


async def test_apply_resolutions_does_not_mutate_input() -> None:
    norm = normalize_workflow(_workflow_doc())
    original_activity = norm.document["spec"]["steps"][0]["activity"]
    registry = FakeActivityRegistry(
        [_ActivityRow("custos.builtin", "vuln-scan", "2.4.1", "sha:abc")],
    )
    await apply_resolutions(
        norm,
        activity_registry=registry,
        definition_store=FakeDefinitionStore(),
        connector_client=StubConnectorClient(),
        workspace_id="ws-1",
    )
    assert norm.document["spec"]["steps"][0]["activity"] == original_activity


async def test_apply_resolutions_surfaces_first_resolve_error() -> None:
    norm = normalize_workflow(_workflow_doc())
    # No matching activity row → ActivityTypeNotFound.
    with pytest.raises(ActivityTypeNotFound):
        await apply_resolutions(
            norm,
            activity_registry=FakeActivityRegistry([]),
            definition_store=FakeDefinitionStore(),
            connector_client=StubConnectorClient(),
            workspace_id="ws-1",
        )


def test_collect_connector_instance_calls_dedupes_and_preserves_order() -> None:
    slots = [
        RefResolutionSlot(
            kind="connector_instance",
            path=("spec", "triggers", 0, "connector"),
            original_ref="a",
        ),
        RefResolutionSlot(
            kind="activity",
            path=("spec", "steps", 0, "activity"),
            original_ref="ns/t@1",
        ),
        RefResolutionSlot(
            kind="connector_instance",
            path=("spec", "steps", 0, "connector"),
            original_ref="b",
        ),
        RefResolutionSlot(
            kind="connector_instance",
            path=("spec", "steps", 1, "connector"),
            original_ref="a",
        ),
    ]
    assert collect_connector_instance_calls(slots) == ["a", "b"]
