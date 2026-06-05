"""Thin store adapters over the SPL ``MetadataStoreProvider`` (TS-IMPL-008).

The Trigger Service persists everything through the contract-locked
storage-provider-layer (SPL). This package exposes the narrow
:class:`~custos_trigger.stores.base.TriggerMetadataStore` Protocol — the
subset of ``MetadataStoreProvider`` the service writes to — plus three
domain-facing adapters that map the wire/domain models in
:mod:`custos_trigger.models` onto the SPL rows and delegate to the provider:

* :class:`~custos_trigger.stores.subscriptions.SubscriptionStore`
* :class:`~custos_trigger.stores.resume.ResumeSubscriptionStore`
* :class:`~custos_trigger.stores.schedules.ScheduleStore`

Both the Postgres adapter (``custos_pg.PgMetadataAdapter``) and the in-process
:class:`custos_trigger.providers.InMemoryTriggerMetadataStore` satisfy the
Protocol, so the host swaps backends via the ``TRIGGER_METADATA_STORE`` env
knob (see :func:`custos_trigger.providers.load_providers`) without touching
the adapters.
"""

from __future__ import annotations

from custos_trigger.stores.base import (
    ResumeReadable,
    SubscriptionListable,
    SubscriptionReadable,
    TriggerMetadataStore,
)
from custos_trigger.stores.resume import (
    ResumeReadUnsupportedError,
    ResumeSubscriptionStore,
    StoredResumeRegistration,
)
from custos_trigger.stores.schedules import ScheduleStore
from custos_trigger.stores.subscriptions import (
    SubscriptionListUnsupportedError,
    SubscriptionReadUnsupportedError,
    SubscriptionStore,
)

__all__ = [
    "ResumeReadUnsupportedError",
    "ResumeReadable",
    "ResumeSubscriptionStore",
    "ScheduleStore",
    "StoredResumeRegistration",
    "SubscriptionListUnsupportedError",
    "SubscriptionListable",
    "SubscriptionReadUnsupportedError",
    "SubscriptionReadable",
    "SubscriptionStore",
    "TriggerMetadataStore",
]
