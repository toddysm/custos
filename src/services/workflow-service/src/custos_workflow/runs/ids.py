"""Deterministic ``runId`` derivation for the Run Controller (WF-IMPL-030).

This module locks the wire contract for ``StartRun`` idempotency. Once
published, the namespace UUID and the canonical input encoding here
**must not** change — clients downstream rely on byte-equal ``runId``
values for the same ``(workspace_id, idempotency_key)`` pair.

Contract:

* With a non-empty ``idempotency_key``:
  ``runId = uuid5(RUN_ID_NAMESPACE, f"{workspace_id}|{idempotency_key}")``.
* Without an ``idempotency_key`` (``None`` or empty string):
  ``runId = uuid4()``.

The empty-string key is treated as "no key supplied" to match the
service's existing input-validation idioms (see WF-IMPL-024 error
taxonomy + design.md § Idempotency Model).
"""

from __future__ import annotations

from typing import Final, NewType
from uuid import UUID, uuid4, uuid5

__all__ = ["RUN_ID_NAMESPACE", "RunId", "derive_run_id"]


#: A deterministic-or-random workflow instance identifier.
#:
#: Exposed as a :class:`typing.NewType` over :class:`str` because the
#: Dapr Workflow SDK requires the instance id as a plain ``str``. Tests
#: and downstream code should treat this as opaque.
RunId = NewType("RunId", str)


#: Fixed UUID namespace for the deterministic ``runId`` derivation.
#:
#: **Locked.** Changing this value would invalidate every previously
#: issued deterministic ``runId``. Do not edit without a coordinated
#: design change and a corresponding bump to the public Run Controller
#: contract.
RUN_ID_NAMESPACE: Final[UUID] = UUID("d8e6c1a4-0f3a-4f8a-9f1d-1c9b6e6a9c2d")


def derive_run_id(workspace_id: str, idempotency_key: str | None) -> RunId:
    """Derive a workflow ``runId`` from a workspace + optional idempotency key.

    Same ``(workspace_id, idempotency_key)`` pair always returns the same
    id (UUIDv5 over ``RUN_ID_NAMESPACE``); same key under a different
    workspace returns a different id. With no key (or an empty-string
    key) a fresh random UUIDv4 is generated.

    :param workspace_id: The owning workspace identifier; must be
        non-empty.
    :param idempotency_key: Caller-supplied idempotency token, or
        ``None``. An empty string is treated as "no key supplied".
    :returns: The derived :class:`RunId`.
    :raises ValueError: If ``workspace_id`` is empty.
    """

    if not workspace_id:
        raise ValueError("workspace_id must be a non-empty string")
    if idempotency_key:
        return RunId(str(uuid5(RUN_ID_NAMESPACE, f"{workspace_id}|{idempotency_key}")))
    return RunId(str(uuid4()))
