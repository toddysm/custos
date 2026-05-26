"""``BindForStep`` package (CONN-IMPL-016, Phase G).

Re-exports the public surface of the binder so callers depend on
``custos_connector.binding`` rather than the sub-modules. The split
keeps the service orchestration (:mod:`.service`), error taxonomy
(:mod:`.errors`), wire/domain models (:mod:`.models`), and FastAPI
router (:mod:`.router`) independently testable.
"""

from __future__ import annotations

from custos_connector.binding.errors import BindError, BindErrorCode, http_status_for
from custos_connector.binding.models import (
    BindForStepRequest,
    BindForStepResponse,
    BindSlotRequest,
)
from custos_connector.binding.router import router as binding_router
from custos_connector.binding.service import BindForStepService, PluginBinder

__all__ = [
    "BindError",
    "BindErrorCode",
    "BindForStepRequest",
    "BindForStepResponse",
    "BindForStepService",
    "BindSlotRequest",
    "PluginBinder",
    "binding_router",
    "http_status_for",
]
