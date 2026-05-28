"""Per-step :class:`SchemaBindings` derivation (WF-IMPL-017).

Public surface:

- :class:`ActivityTypeRegistry` Protocol and
  :class:`InMemoryActivityTypeRegistry` (test impl).
- :class:`ActivityTypeNotFoundError`.
- :func:`derive_bindings` — turn a parsed :class:`WorkflowDocument`
  into ``{step_id: SchemaBindings}``.

See :mod:`custos_workflow.bindings.derive` for the design notes
(ordering, sub-workflow stub, forEach ``item.*`` gap).
"""

from __future__ import annotations

from custos_workflow.bindings.derive import derive_bindings
from custos_workflow.bindings.registry import (
    ActivityTypeNotFoundError,
    ActivityTypeRegistry,
    InMemoryActivityTypeRegistry,
)

__all__ = [
    "ActivityTypeNotFoundError",
    "ActivityTypeRegistry",
    "InMemoryActivityTypeRegistry",
    "derive_bindings",
]
