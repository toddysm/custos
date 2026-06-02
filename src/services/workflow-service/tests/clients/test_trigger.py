"""Tests for the ``TriggerServiceClient`` Protocol + RPC models (WF-IMPL-101)."""

from __future__ import annotations

import dataclasses

import pytest

from custos_workflow.clients import (
    CancelResumeSubscriptionRequest,
    FakeTriggerServiceClient,
    NoopTriggerServiceClient,
    RegisterResumeSubscriptionRequest,
    RegisterResumeSubscriptionResponse,
    TriggerServiceClient,
)
from custos_workflow.clients.trigger import (
    CANCEL_RESUME_SUBSCRIPTION_DAPR_METHOD,
    REGISTER_RESUME_SUBSCRIPTION_DAPR_METHOD,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _register(
    run_id: str = "run-1",
    step_id: str = "step-1",
    event_key: str = "evt-1",
    ttl: str = "PT24H",
    selector: str | None = None,
) -> RegisterResumeSubscriptionRequest:
    return RegisterResumeSubscriptionRequest(
        run_id=run_id,
        step_id=step_id,
        event_key=event_key,
        ttl=ttl,
        selector=selector,
    )


def _cancel(
    run_id: str = "run-1",
    step_id: str = "step-1",
    event_key: str = "evt-1",
) -> CancelResumeSubscriptionRequest:
    return CancelResumeSubscriptionRequest(run_id=run_id, step_id=step_id, event_key=event_key)


# ---------------------------------------------------------------------------
# Pinned Dapr method-name constants
# ---------------------------------------------------------------------------


class TestDaprMethodConstants:
    def test_register_method_name_is_pinned(self) -> None:
        assert REGISTER_RESUME_SUBSCRIPTION_DAPR_METHOD == "RegisterResumeSubscription"

    def test_cancel_method_name_is_pinned(self) -> None:
        assert CANCEL_RESUME_SUBSCRIPTION_DAPR_METHOD == "CancelResumeSubscription"


# ---------------------------------------------------------------------------
# RegisterResumeSubscriptionRequest
# ---------------------------------------------------------------------------


class TestRegisterResumeSubscriptionRequest:
    def test_construct_minimal(self) -> None:
        req = _register()
        assert req.run_id == "run-1"
        assert req.step_id == "step-1"
        assert req.event_key == "evt-1"
        assert req.ttl == "PT24H"
        assert req.selector is None

    def test_construct_with_selector(self) -> None:
        req = _register(selector="${{ event.kind == 'approved' }}")
        assert req.selector == "${{ event.kind == 'approved' }}"

    def test_idempotency_key_is_run_step_event_triple(self) -> None:
        req = _register(run_id="r", step_id="s", event_key="e")
        assert req.idempotency_key == ("r", "s", "e")

    def test_is_frozen(self) -> None:
        req = _register()
        with pytest.raises((AttributeError, dataclasses.FrozenInstanceError)):
            req.run_id = "other"  # type: ignore[misc]

    def test_slots_blocks_new_attributes(self) -> None:
        req = _register()
        with pytest.raises((AttributeError, TypeError, dataclasses.FrozenInstanceError)):
            req.extra = "x"  # type: ignore[attr-defined]

    def test_is_hashable(self) -> None:
        a = _register()
        b = _register()
        assert hash(a) == hash(b)
        assert {a, b} == {a}

    def test_rejects_empty_run_id(self) -> None:
        with pytest.raises(ValueError, match=r"run_id"):
            _register(run_id="")

    def test_rejects_empty_step_id(self) -> None:
        with pytest.raises(ValueError, match=r"step_id"):
            _register(step_id="")

    def test_rejects_empty_event_key(self) -> None:
        with pytest.raises(ValueError, match=r"event_key"):
            _register(event_key="")

    def test_rejects_empty_ttl(self) -> None:
        with pytest.raises(ValueError, match=r"ttl"):
            _register(ttl="")

    def test_rejects_empty_selector_string(self) -> None:
        with pytest.raises(ValueError, match=r"selector"):
            _register(selector="")


# ---------------------------------------------------------------------------
# RegisterResumeSubscriptionResponse
# ---------------------------------------------------------------------------


class TestRegisterResumeSubscriptionResponse:
    def test_construct(self) -> None:
        resp = RegisterResumeSubscriptionResponse(ts_subscription_id="ts-1")
        assert resp.ts_subscription_id == "ts-1"

    def test_is_frozen(self) -> None:
        resp = RegisterResumeSubscriptionResponse(ts_subscription_id="ts-1")
        with pytest.raises((AttributeError, dataclasses.FrozenInstanceError)):
            resp.ts_subscription_id = "ts-2"  # type: ignore[misc]

    def test_slots_blocks_new_attributes(self) -> None:
        resp = RegisterResumeSubscriptionResponse(ts_subscription_id="ts-1")
        with pytest.raises((AttributeError, TypeError, dataclasses.FrozenInstanceError)):
            resp.extra = "x"  # type: ignore[attr-defined]

    def test_rejects_empty_id(self) -> None:
        with pytest.raises(ValueError, match=r"ts_subscription_id"):
            RegisterResumeSubscriptionResponse(ts_subscription_id="")


# ---------------------------------------------------------------------------
# CancelResumeSubscriptionRequest
# ---------------------------------------------------------------------------


class TestCancelResumeSubscriptionRequest:
    def test_construct(self) -> None:
        req = _cancel()
        assert req.run_id == "run-1"
        assert req.step_id == "step-1"
        assert req.event_key == "evt-1"

    def test_idempotency_key_is_run_step_event_triple(self) -> None:
        req = _cancel(run_id="r", step_id="s", event_key="e")
        assert req.idempotency_key == ("r", "s", "e")

    def test_is_frozen(self) -> None:
        req = _cancel()
        with pytest.raises((AttributeError, dataclasses.FrozenInstanceError)):
            req.run_id = "other"  # type: ignore[misc]

    def test_slots_blocks_new_attributes(self) -> None:
        req = _cancel()
        with pytest.raises((AttributeError, TypeError, dataclasses.FrozenInstanceError)):
            req.extra = "x"  # type: ignore[attr-defined]

    def test_rejects_empty_run_id(self) -> None:
        with pytest.raises(ValueError, match=r"run_id"):
            _cancel(run_id="")

    def test_rejects_empty_step_id(self) -> None:
        with pytest.raises(ValueError, match=r"step_id"):
            _cancel(step_id="")

    def test_rejects_empty_event_key(self) -> None:
        with pytest.raises(ValueError, match=r"event_key"):
            _cancel(event_key="")


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


class TestTriggerServiceClientProtocol:
    def test_is_runtime_checkable(self) -> None:
        assert isinstance(FakeTriggerServiceClient(), TriggerServiceClient)
        assert isinstance(NoopTriggerServiceClient(), TriggerServiceClient)

    def test_non_conforming_object_fails_isinstance(self) -> None:
        assert not isinstance(object(), TriggerServiceClient)


# ---------------------------------------------------------------------------
# NoopTriggerServiceClient
# ---------------------------------------------------------------------------


class TestNoopTriggerServiceClient:
    def test_register_raises_not_implemented(self) -> None:
        client = NoopTriggerServiceClient()
        with pytest.raises(NotImplementedError, match=r"register_resume_subscription"):
            client.register_resume_subscription(_register())

    def test_cancel_raises_not_implemented(self) -> None:
        client = NoopTriggerServiceClient()
        with pytest.raises(NotImplementedError, match=r"cancel_resume_subscription"):
            client.cancel_resume_subscription(_cancel())


# ---------------------------------------------------------------------------
# FakeTriggerServiceClient
# ---------------------------------------------------------------------------


class TestFakeTriggerServiceClient:
    def test_register_mints_deterministic_id(self) -> None:
        client = FakeTriggerServiceClient()
        resp = client.register_resume_subscription(_register())
        assert resp.ts_subscription_id == "ts-sub-1"

    def test_register_is_idempotent_for_same_key(self) -> None:
        client = FakeTriggerServiceClient()
        first = client.register_resume_subscription(_register())
        # A replay with a *different* selector but the same idempotency
        # key must return the original id (original-wins).
        second = client.register_resume_subscription(_register(selector="${{ diverged }}"))
        assert first.ts_subscription_id == second.ts_subscription_id == "ts-sub-1"

    def test_distinct_keys_get_distinct_ids(self) -> None:
        client = FakeTriggerServiceClient()
        a = client.register_resume_subscription(_register(event_key="evt-a"))
        b = client.register_resume_subscription(_register(event_key="evt-b"))
        assert a.ts_subscription_id == "ts-sub-1"
        assert b.ts_subscription_id == "ts-sub-2"

    def test_custom_id_prefix(self) -> None:
        client = FakeTriggerServiceClient(id_prefix="sub/")
        resp = client.register_resume_subscription(_register())
        assert resp.ts_subscription_id == "sub/1"

    def test_records_register_calls(self) -> None:
        client = FakeTriggerServiceClient()
        req = _register()
        client.register_resume_subscription(req)
        assert client.register_calls == [req]

    def test_subscriptions_map_exposes_current_state(self) -> None:
        client = FakeTriggerServiceClient()
        client.register_resume_subscription(_register(run_id="r", step_id="s", event_key="e"))
        assert client.subscriptions == {("r", "s", "e"): "ts-sub-1"}

    def test_cancel_forgets_key_so_reregister_mints_fresh_id(self) -> None:
        client = FakeTriggerServiceClient()
        first = client.register_resume_subscription(_register())
        client.cancel_resume_subscription(_cancel())
        # After cancel, the key is gone — re-registering models a
        # genuine fresh registration (e.g. after TTL expiry).
        second = client.register_resume_subscription(_register())
        assert first.ts_subscription_id == "ts-sub-1"
        assert second.ts_subscription_id == "ts-sub-2"

    def test_cancel_unknown_key_is_noop(self) -> None:
        client = FakeTriggerServiceClient()
        # Must not raise even though nothing is registered.
        client.cancel_resume_subscription(_cancel(event_key="never-registered"))
        assert client.subscriptions == {}

    def test_records_cancel_calls(self) -> None:
        client = FakeTriggerServiceClient()
        req = _cancel()
        client.cancel_resume_subscription(req)
        assert client.cancel_calls == [req]
