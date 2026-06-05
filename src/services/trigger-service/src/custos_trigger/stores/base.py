"""The narrow SPL surface the Trigger Service store adapters drive.

:class:`TriggerMetadataStore` pins exactly the eight ``Subscription`` /
``SubscriptionSelector`` / ``ResumeSubscription`` / ``DedupKey`` / ``Schedule``
write methods from :class:`custos_spl.interfaces.metadata_store.MetadataStoreProvider`
that Trigger Service owns (design ``§ Data Models``). Keeping the surface
narrow lets the in-process test/dev backend implement only the trigger
families while the production Postgres adapter — which implements the full
``MetadataStoreProvider`` — satisfies it structurally.

The method signatures mirror the SPL Protocol verbatim so a conformant
``MetadataStoreProvider`` is assignable to a ``TriggerMetadataStore`` without
adaptation.
"""

from __future__ import annotations

from datetime import datetime
from typing import Protocol, runtime_checkable

from custos_spl.ids import SubscriptionId, WorkspaceId
from custos_spl.interfaces.metadata_store import (
    PutDedupKeyResult,
)
from custos_spl.interfaces.metadata_store import (
    ResumeSubscription as SplResumeSubscription,
)
from custos_spl.interfaces.metadata_store import (
    Schedule as SplSchedule,
)
from custos_spl.interfaces.metadata_store import (
    Subscription as SplSubscription,
)
from custos_spl.interfaces.metadata_store import (
    SubscriptionSelector as SplSubscriptionSelector,
)

__all__ = ["SubscriptionReadable", "TriggerMetadataStore"]


@runtime_checkable
class TriggerMetadataStore(Protocol):
    """The subset of ``MetadataStoreProvider`` the Trigger Service writes to.

    Every method is workspace-scoped and ``async`` to match the SPL provider
    surface. The selector / resume rows carry the rich Trigger Service
    metadata the minimal SPL rows have no column for inside their free-form
    JSON blobs — see :mod:`custos_trigger.models`.
    """

    async def put_subscription(
        self, workspace_id: WorkspaceId, subscription: SplSubscription
    ) -> SplSubscription: ...

    async def update_subscription_state(
        self,
        workspace_id: WorkspaceId,
        subscription_id: SubscriptionId,
        state: str,
    ) -> SplSubscription: ...

    async def append_subscription_selector(
        self,
        workspace_id: WorkspaceId,
        subscription_id: SubscriptionId,
        selector: SplSubscriptionSelector,
    ) -> SplSubscriptionSelector: ...

    async def put_resume_subscription(
        self, workspace_id: WorkspaceId, resume: SplResumeSubscription
    ) -> SplResumeSubscription: ...

    async def delete_resume_subscription(
        self, workspace_id: WorkspaceId, resume_id: str
    ) -> None: ...

    async def put_dedup_key(
        self, workspace_id: WorkspaceId, key: str, ttl_seconds: int
    ) -> PutDedupKeyResult: ...

    async def put_schedule(
        self, workspace_id: WorkspaceId, schedule: SplSchedule
    ) -> SplSchedule: ...

    async def update_schedule_next_fire(
        self,
        workspace_id: WorkspaceId,
        schedule_id: str,
        next_fire_at: datetime,
    ) -> SplSchedule: ...


@runtime_checkable
class SubscriptionReadable(Protocol):
    """Optional read-back capability for subscription rows + selector blobs.

    The locked SPL ``MetadataStoreProvider`` is a *write* surface — it exposes
    no subscription read or list method (design ``§ Data Models``). The REST
    surface (TS-IMPL-015) nonetheless needs to read a single subscription back
    by id for ``GET`` / ``PATCH`` / ``DELETE`` / ``:fire``, so this narrow
    capability Protocol is probed structurally (``runtime_checkable``) by
    :meth:`custos_trigger.stores.SubscriptionStore.get`.

    The in-process backend satisfies it via its dev/test read accessors; the
    Postgres adapter gains its own query surface in a later task, at which
    point it satisfies this Protocol too. Until then a backend that does not
    implement it surfaces a clear ``SubscriptionReadUnsupportedError`` rather
    than a silent miss.

    The accessors are synchronous to match the in-process backend's existing
    read helpers; a future async query surface can widen this without breaking
    the consumer, which already awaits the wrapping ``SubscriptionStore.get``.
    """

    def subscription(self, workspace_id: str, subscription_id: str) -> SplSubscription | None: ...

    def subscription_selectors(
        self, workspace_id: str, subscription_id: str
    ) -> tuple[SplSubscriptionSelector, ...]: ...
