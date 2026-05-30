"""Tests for the ``ConnectorClient`` Protocol + ``ConnectorContext`` (WF-IMPL-050)."""

from __future__ import annotations

import dataclasses
from datetime import UTC, datetime
from types import MappingProxyType

import pytest

from custos_workflow.clients import (
    BindForStepRequest,
    BindForStepResponse,
    ConnectorClient,
    ConnectorContext,
    FakeConnectorClient,
    NoopConnectorClient,
    SlotSpec,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ctx(
    slot_name: str = "primary",
    handle: str = "h-1",
    connector_kind: str = "oci-registry",
    expires_at: datetime | None = None,
) -> ConnectorContext:
    return ConnectorContext(
        slot_name=slot_name,
        handle=handle,
        expires_at=expires_at or datetime(2026, 6, 1, tzinfo=UTC),
        connector_kind=connector_kind,
    )


def _slot(
    name: str = "primary",
    connector_ref: str = "conn://oci/default",
    capabilities: tuple[str, ...] = ("pull",),
) -> SlotSpec:
    return SlotSpec(name=name, connector_ref=connector_ref, capabilities=capabilities)


# ---------------------------------------------------------------------------
# SlotSpec
# ---------------------------------------------------------------------------


class TestSlotSpec:
    def test_construct_minimal(self) -> None:
        spec = _slot()
        assert spec.name == "primary"
        assert spec.connector_ref == "conn://oci/default"
        assert spec.capabilities == ("pull",)

    def test_default_capabilities_is_empty_tuple(self) -> None:
        spec = SlotSpec(name="p", connector_ref="r")
        assert spec.capabilities == ()
        assert isinstance(spec.capabilities, tuple)

    def test_is_frozen(self) -> None:
        spec = _slot()
        with pytest.raises((AttributeError, dataclasses.FrozenInstanceError)):
            spec.name = "other"  # type: ignore[misc]

    def test_slots_blocks_new_attributes(self) -> None:
        spec = _slot()
        with pytest.raises((AttributeError, TypeError, dataclasses.FrozenInstanceError)):
            spec.extra = "x"  # type: ignore[attr-defined]

    def test_is_hashable(self) -> None:
        a = _slot()
        b = _slot()
        assert hash(a) == hash(b)
        # set membership works
        assert {a, b} == {a}

    def test_rejects_empty_name(self) -> None:
        with pytest.raises(ValueError, match=r"SlotSpec\.name"):
            SlotSpec(name="", connector_ref="r")

    def test_rejects_empty_connector_ref(self) -> None:
        with pytest.raises(ValueError, match=r"SlotSpec\.connector_ref"):
            SlotSpec(name="n", connector_ref="")


# ---------------------------------------------------------------------------
# ConnectorContext
# ---------------------------------------------------------------------------


class TestConnectorContext:
    def test_construct_minimal(self) -> None:
        ctx = _ctx()
        assert ctx.slot_name == "primary"
        assert ctx.handle == "h-1"
        assert ctx.connector_kind == "oci-registry"
        assert ctx.expires_at == datetime(2026, 6, 1, tzinfo=UTC)

    def test_is_frozen(self) -> None:
        ctx = _ctx()
        with pytest.raises((AttributeError, dataclasses.FrozenInstanceError)):
            ctx.handle = "h-2"  # type: ignore[misc]

    def test_slots_blocks_new_attributes(self) -> None:
        ctx = _ctx()
        with pytest.raises((AttributeError, TypeError, dataclasses.FrozenInstanceError)):
            ctx.extra = "x"  # type: ignore[attr-defined]

    def test_is_hashable_per_acceptance_criteria(self) -> None:
        a = _ctx()
        b = _ctx()
        # Same fields → same hash → set / dict deduplication works.
        assert hash(a) == hash(b)
        assert {a, b} == {a}
        assert {a: "v"}[b] == "v"

    def test_rejects_empty_slot_name(self) -> None:
        with pytest.raises(ValueError, match=r"ConnectorContext\.slot_name"):
            ConnectorContext(
                slot_name="",
                handle="h",
                expires_at=datetime(2026, 6, 1, tzinfo=UTC),
                connector_kind="k",
            )

    def test_rejects_empty_handle(self) -> None:
        with pytest.raises(ValueError, match=r"ConnectorContext\.handle"):
            ConnectorContext(
                slot_name="s",
                handle="",
                expires_at=datetime(2026, 6, 1, tzinfo=UTC),
                connector_kind="k",
            )

    def test_rejects_empty_connector_kind(self) -> None:
        with pytest.raises(ValueError, match=r"ConnectorContext\.connector_kind"):
            ConnectorContext(
                slot_name="s",
                handle="h",
                expires_at=datetime(2026, 6, 1, tzinfo=UTC),
                connector_kind="",
            )

    def test_rejects_naive_expires_at(self) -> None:
        with pytest.raises(ValueError, match="timezone-aware"):
            ConnectorContext(
                slot_name="s",
                handle="h",
                expires_at=datetime(2026, 6, 1),
                connector_kind="k",
            )


# ---------------------------------------------------------------------------
# BindForStepRequest
# ---------------------------------------------------------------------------


class TestBindForStepRequest:
    def test_construct_minimal(self) -> None:
        req = BindForStepRequest(step_key="step-1", slots=(_slot(),))
        assert req.step_key == "step-1"
        assert len(req.slots) == 1
        assert req.slots[0].name == "primary"

    def test_construct_with_empty_slots_is_allowed(self) -> None:
        # A step with zero connector slots is a real shape — for example a
        # ``let:`` step that piggy-backs through the same machinery. The
        # contract doesn't forbid an empty bind request.
        req = BindForStepRequest(step_key="step-1", slots=())
        assert req.slots == ()

    def test_is_frozen(self) -> None:
        req = BindForStepRequest(step_key="step-1", slots=(_slot(),))
        with pytest.raises((AttributeError, dataclasses.FrozenInstanceError)):
            req.step_key = "other"  # type: ignore[misc]

    def test_is_hashable(self) -> None:
        a = BindForStepRequest(step_key="step-1", slots=(_slot(),))
        b = BindForStepRequest(step_key="step-1", slots=(_slot(),))
        assert hash(a) == hash(b)

    def test_rejects_empty_step_key(self) -> None:
        with pytest.raises(ValueError, match="step_key"):
            BindForStepRequest(step_key="", slots=(_slot(),))

    def test_rejects_duplicate_slot_names(self) -> None:
        with pytest.raises(ValueError, match="duplicate slot name"):
            BindForStepRequest(
                step_key="step-1",
                slots=(
                    _slot(name="dup"),
                    _slot(name="dup", connector_ref="conn://other"),
                ),
            )


# ---------------------------------------------------------------------------
# BindForStepResponse
# ---------------------------------------------------------------------------


class TestBindForStepResponse:
    def test_construct_from_plain_dict_snapshots_as_mappingproxy(self) -> None:
        resp = BindForStepResponse(contexts={"primary": _ctx()})
        # Acceptance criteria: contexts is a MappingProxyType snapshot.
        assert isinstance(resp.contexts, MappingProxyType)
        assert resp.contexts["primary"].handle == "h-1"

    def test_existing_mappingproxy_is_reused_without_re_wrapping(self) -> None:
        snapshot = MappingProxyType({"primary": _ctx()})
        resp = BindForStepResponse(contexts=snapshot)
        # Same object identity preserved when the caller already
        # passed in a snapshot.
        assert resp.contexts is snapshot

    def test_contexts_cannot_be_mutated_by_caller(self) -> None:
        resp = BindForStepResponse(contexts={"primary": _ctx()})
        with pytest.raises(TypeError):
            resp.contexts["other"] = _ctx(slot_name="other", handle="h-2")  # type: ignore[index]

    def test_caller_mutating_original_dict_does_not_leak_into_response(self) -> None:
        original: dict[str, ConnectorContext] = {"primary": _ctx()}
        resp = BindForStepResponse(contexts=original)
        original["primary"] = _ctx(handle="mutated")
        # The snapshot is independent of the caller's dict.
        assert resp.contexts["primary"].handle == "h-1"

    def test_is_frozen(self) -> None:
        resp = BindForStepResponse(contexts={"primary": _ctx()})
        with pytest.raises((AttributeError, dataclasses.FrozenInstanceError)):
            resp.contexts = MappingProxyType({})  # type: ignore[misc]

    def test_rejects_slot_name_key_mismatch(self) -> None:
        with pytest.raises(ValueError, match="slot_name"):
            BindForStepResponse(contexts={"primary": _ctx(slot_name="other")})

    def test_empty_response_is_allowed(self) -> None:
        # Empty bind requests get empty bind responses — symmetric
        # with the request side.
        resp = BindForStepResponse(contexts={})
        assert dict(resp.contexts) == {}
        assert isinstance(resp.contexts, MappingProxyType)


# ---------------------------------------------------------------------------
# ConnectorClient Protocol
# ---------------------------------------------------------------------------


class TestConnectorClientProtocol:
    def test_protocol_is_runtime_checkable(self) -> None:
        # Acceptance criteria: Protocol is runtime_checkable. Both
        # the Noop client and the Fake client must pass the
        # ``isinstance`` Protocol check structurally.
        assert isinstance(NoopConnectorClient(), ConnectorClient)
        assert isinstance(FakeConnectorClient(), ConnectorClient)

    def test_arbitrary_object_does_not_satisfy_protocol(self) -> None:
        class Empty:
            pass

        assert not isinstance(Empty(), ConnectorClient)

    def test_duck_typed_object_satisfies_protocol(self) -> None:
        class DuckClient:
            def bind_for_step(self, request: BindForStepRequest) -> BindForStepResponse:
                return BindForStepResponse(contexts={})

        assert isinstance(DuckClient(), ConnectorClient)


# ---------------------------------------------------------------------------
# NoopConnectorClient
# ---------------------------------------------------------------------------


class TestNoopConnectorClient:
    def test_bind_for_step_raises_notimplementederror(self) -> None:
        client = NoopConnectorClient()
        with pytest.raises(NotImplementedError, match="deferred sub-module"):
            client.bind_for_step(BindForStepRequest(step_key="s", slots=()))


# ---------------------------------------------------------------------------
# FakeConnectorClient
# ---------------------------------------------------------------------------


class TestFakeConnectorClient:
    def test_returns_canned_responses_in_fifo_order(self) -> None:
        r1 = BindForStepResponse(contexts={"primary": _ctx(handle="h-1")})
        r2 = BindForStepResponse(contexts={"primary": _ctx(handle="h-2")})
        client = FakeConnectorClient(responses=[r1, r2])

        out1 = client.bind_for_step(BindForStepRequest(step_key="s1", slots=()))
        out2 = client.bind_for_step(BindForStepRequest(step_key="s2", slots=()))

        assert out1 is r1
        assert out2 is r2

    def test_records_every_call(self) -> None:
        client = FakeConnectorClient(
            responses=[BindForStepResponse(contexts={})],
        )
        req = BindForStepRequest(step_key="s1", slots=(_slot(),))
        client.bind_for_step(req)
        assert client.calls == [req]

    def test_raises_indexerror_when_queue_empty(self) -> None:
        client = FakeConnectorClient()
        with pytest.raises(IndexError, match="no more canned responses"):
            client.bind_for_step(BindForStepRequest(step_key="s", slots=()))

    def test_default_init_has_empty_queues(self) -> None:
        client = FakeConnectorClient()
        assert client.responses == []
        assert client.calls == []
