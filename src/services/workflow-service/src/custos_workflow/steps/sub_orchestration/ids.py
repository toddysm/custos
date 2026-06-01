"""Deterministic child-instance-id derivation (WF-IMPL-085).

This module locks the wire contract for the **child sub-orchestration
instance id** the Sub-Orchestration Manager (ADR-007) assigns to every
child Dapr Workflow instance it spawns for a parent step. Once a
workflow has executed against this format the encoding **must not**
change — Dapr replay re-derives child instance ids byte-for-byte to
reproduce the exact child set, and parent expression scope addresses
child outputs through ``steps.<stepId>.outputs`` keyed on the same
``iterationKey``.

Canonical id form (``design.md`` § *Sub-Orchestration Manager*)::

    <parentRunId>/<stepId>/<iterationKey>

* Loop (``for:``) → ``iterationKey`` derived from the item via
  :func:`iteration_key`.
* Approval gate (``approval:``) → the reserved key
  :data:`APPROVAL_ITERATION_KEY`.
* Sub-workflow (``workflow:``) → the reserved key
  :data:`WORKFLOW_ITERATION_KEY`.

Reserved separator
------------------

The component separator is :data:`CHILD_INSTANCE_ID_SEPARATOR`
(``"/"``). It is **rejected** in the ``step_id`` and ``iteration_key``
arguments to :func:`child_instance_id` so the resulting id is
unambiguously parseable. Keys produced by :func:`iteration_key` are
guaranteed separator-free because that function percent-escapes the
reserved character (and the escape character itself), so the two
boundaries together satisfy "rejected or escaped".

Collision rule
--------------

Two distinct items can derive the *same* ``iteration_key`` (for
example two mappings sharing the same ``id``). When that happens their
child instance ids are identical and Dapr would treat them as one
instance. :func:`iteration_key` itself never silently de-duplicates;
the loop-expansion layer (WF-IMPL-089/090) is responsible for
detecting duplicate keys across an iteration set and failing with
``step.loop_expansion_error``. The index-fallback path (used for items
without a stable identity) cannot collide because list indices are
unique within a single expansion.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Final

__all__ = [
    "APPROVAL_ITERATION_KEY",
    "CHILD_INSTANCE_ID_SEPARATOR",
    "WORKFLOW_ITERATION_KEY",
    "ChildInstanceIdError",
    "child_instance_id",
    "iteration_key",
]


#: Canonical separator between the three id components on the wire.
#:
#: **Locked.** Changing this would invalidate every previously issued
#: child instance id and break Dapr replay determinism.
CHILD_INSTANCE_ID_SEPARATOR: Final[str] = "/"

#: Reserved ``iterationKey`` for an ``approval:`` gate's single child.
APPROVAL_ITERATION_KEY: Final[str] = "approval"

#: Reserved ``iterationKey`` for a ``workflow:`` sub-workflow's single child.
WORKFLOW_ITERATION_KEY: Final[str] = "workflow"

#: Mapping fields, in priority order, that supply an item's stable
#: identity for :func:`iteration_key`.
_IDENTITY_FIELDS: Final[tuple[str, ...]] = ("id", "key")


class ChildInstanceIdError(ValueError):
    """Raised when a child instance id cannot be constructed.

    Inherits from :class:`ValueError` so callers that already catch
    validation failures pick this up uniformly. The Sub-Orchestration
    Manager error taxonomy (WF-IMPL-086) wraps these with a stable
    ``kind`` string when emitting lifecycle events.
    """


def child_instance_id(parent_run_id: str, step_id: str, iteration_key: str) -> str:
    """Derive the deterministic ``<parentRunId>/<stepId>/<iterationKey>`` id.

    Byte-equal for identical ``(parent_run_id, step_id, iteration_key)``
    inputs across processes, replays, and Python versions.

    :param parent_run_id: The parent Run's workflow instance id; must
        be non-empty and contain no separator.
    :param step_id: The spawning step's id; must be non-empty and
        contain no separator.
    :param iteration_key: The per-child iteration key — either a value
        returned by :func:`iteration_key` or one of the reserved keys
        (:data:`APPROVAL_ITERATION_KEY`, :data:`WORKFLOW_ITERATION_KEY`).
        Must be non-empty and contain no separator.
    :returns: The canonical child instance id string.
    :raises ChildInstanceIdError: If any component is empty or contains
        :data:`CHILD_INSTANCE_ID_SEPARATOR`.
    """

    _validate_component("parent_run_id", parent_run_id)
    _validate_component("step_id", step_id)
    _validate_component("iteration_key", iteration_key)
    return (
        f"{parent_run_id}"
        f"{CHILD_INSTANCE_ID_SEPARATOR}{step_id}"
        f"{CHILD_INSTANCE_ID_SEPARATOR}{iteration_key}"
    )


def iteration_key(item: Any, index: int) -> str:
    """Derive a stable, separator-free iteration key for a loop item.

    The rule, in priority order:

    1. If ``item`` is a :class:`~collections.abc.Mapping` carrying a
       stable identity field (``"id"`` then ``"key"``) whose value is a
       ``str``, ``int``, or ``bool``, the key derives from that value.
    2. Else if ``item`` is itself a ``str``, ``int``, or ``bool``, the
       key derives from the item.
    3. Else the key falls back to the positional ``index``.

    The derived value is percent-escaped (``%`` → ``%25``, ``/`` →
    ``%2F``) so the result never contains :data:`CHILD_INSTANCE_ID_SEPARATOR`
    and is therefore always a valid :func:`child_instance_id` component.

    An *empty* derived identity (e.g. ``item == ""`` or ``{"id": ""}``)
    is treated as "no stable identity" and also falls back to the
    index, so the returned key is always non-empty.

    :param item: The loop item to derive a key from.
    :param index: The item's 0-based position in the expanded iterable;
        used as the fallback identity. Must be a non-negative ``int``
        (``bool`` is rejected).
    :returns: A non-empty, separator-free iteration key.
    :raises ChildInstanceIdError: If ``index`` is not an ``int``, is a
        ``bool``, or is negative.
    """

    if isinstance(index, bool) or not isinstance(index, int):
        raise ChildInstanceIdError(f"index must be a non-negative int, got {type(index).__name__}")
    if index < 0:
        raise ChildInstanceIdError(f"index must be >= 0, got {index}")

    derived = _derive_identity(item)
    if not derived:
        # ``None`` (no stable identity) or an empty string both fall
        # back to the index so the key is never empty.
        derived = str(index)
    return _escape(derived)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _derive_identity(item: Any) -> str | None:
    """Return the stable string identity of ``item``, or ``None``.

    ``None`` signals "no stable identity" so the caller falls back to
    the positional index.
    """

    if isinstance(item, Mapping):
        for field in _IDENTITY_FIELDS:
            if field in item:
                value = item[field]
                if _is_primitive(value) and str(value):
                    return str(value)
        return None
    if _is_primitive(item) and str(item):
        return str(item)
    return None


def _is_primitive(value: Any) -> bool:
    """Whether ``value`` has a byte-stable ``str()`` representation.

    ``bool`` is included (it is a subclass of ``int``); ``float`` is
    intentionally excluded because its textual form is not a reliable
    identity. ``None`` is excluded so a present-but-null identity field
    falls through to the next field / index fallback.
    """

    return isinstance(value, str | int)  # bool is a subclass of int


def _escape(value: str) -> str:
    """Percent-escape the reserved separator and escape character.

    Order matters: ``%`` is escaped first so the ``/`` escape's own
    percent sign is not double-encoded.
    """

    return value.replace("%", "%25").replace(CHILD_INSTANCE_ID_SEPARATOR, "%2F")


def _validate_component(name: str, value: str) -> None:
    if not value:
        raise ChildInstanceIdError(f"{name} must be a non-empty string")
    if CHILD_INSTANCE_ID_SEPARATOR in value:
        raise ChildInstanceIdError(
            f"{name} must not contain the reserved separator "
            f"{CHILD_INSTANCE_ID_SEPARATOR!r}; got {value!r}"
        )
