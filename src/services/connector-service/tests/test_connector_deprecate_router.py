"""Tests for the connector-type deprecation HTTP router (CONN-REG-T5 / #903).

Exercises ``POST /internal/v1/connectors:deprecate`` end-to-end through
:func:`create_app` so the call-context middleware, the
:data:`CONNECTOR_DEPRECATE` permission gate, and the Loader dispatch onto
the (idempotent) SPL ``set_connector_type_deprecated`` upsert are all
under test.

The registration harness from :mod:`tests.test_connector_register_router`
is reused to build a wired ``Loader`` plus a ``FakeCatalogAdapter`` whose
in-memory deprecation set can be inspected directly.
"""

from __future__ import annotations

import json

from fastapi.testclient import TestClient
from httpx import Response

from custos_connector import create_app
from custos_connector.middleware import CALLCTX_HEADER
from custos_connector.permissions import CONNECTOR_DEPRECATE, CONNECTOR_READ
from tests.test_connector_register_router import (
    _BASE_SETTINGS,
    _build_providers,
    _loader_for,
)
from tests.test_loader_registry import _baseline_payload

_TYPE = "custos-oci-registry"


def _ctx_header(*, permissions: list[str] | None = None) -> dict[str, str]:
    perms = permissions if permissions is not None else [CONNECTOR_DEPRECATE]
    return {
        CALLCTX_HEADER: json.dumps(
            {
                "workspace_id": "ws-1",
                "principal_id": "svc:control-plane",
                "permissions": perms,
            }
        )
    }


def _post(
    client: TestClient,
    body: dict[str, object],
    *,
    headers: dict[str, str] | None = None,
) -> Response:
    return client.post(
        "/internal/v1/connectors:deprecate",
        json=body,
        headers=headers if headers is not None else _ctx_header(),
    )


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_deprecate_sets_flag_and_returns_applied_state() -> None:
    loader, catalog = _loader_for(_baseline_payload(), vendor_overrides=None)
    app = create_app(settings=_BASE_SETTINGS, providers=_build_providers(loader=loader))
    with TestClient(app) as client:
        response = _post(client, {"type": _TYPE, "deprecated": True})
    assert response.status_code == 200, response.text
    assert response.json() == {"type": _TYPE, "deprecated": True}
    assert _TYPE in catalog._deprecated_types


def test_deprecate_defaults_to_true_when_omitted() -> None:
    loader, catalog = _loader_for(_baseline_payload(), vendor_overrides=None)
    app = create_app(settings=_BASE_SETTINGS, providers=_build_providers(loader=loader))
    with TestClient(app) as client:
        response = _post(client, {"type": _TYPE})
    assert response.status_code == 200, response.text
    assert response.json() == {"type": _TYPE, "deprecated": True}
    assert _TYPE in catalog._deprecated_types


def test_deprecate_is_idempotent() -> None:
    loader, catalog = _loader_for(_baseline_payload(), vendor_overrides=None)
    app = create_app(settings=_BASE_SETTINGS, providers=_build_providers(loader=loader))
    with TestClient(app) as client:
        first = _post(client, {"type": _TYPE, "deprecated": True})
        second = _post(client, {"type": _TYPE, "deprecated": True})
    assert first.status_code == 200, first.text
    assert second.status_code == 200, second.text
    assert catalog._deprecated_types == {_TYPE}


def test_deprecate_can_toggle_off() -> None:
    loader, catalog = _loader_for(_baseline_payload(), vendor_overrides=None)
    app = create_app(settings=_BASE_SETTINGS, providers=_build_providers(loader=loader))
    with TestClient(app) as client:
        on = _post(client, {"type": _TYPE, "deprecated": True})
        off = _post(client, {"type": _TYPE, "deprecated": False})
    assert on.status_code == 200, on.text
    assert off.status_code == 200, off.text
    assert off.json() == {"type": _TYPE, "deprecated": False}
    assert _TYPE not in catalog._deprecated_types


# ---------------------------------------------------------------------------
# Guards + validation
# ---------------------------------------------------------------------------


def test_deprecate_requires_connector_deprecate_permission() -> None:
    loader, _ = _loader_for(_baseline_payload(), vendor_overrides=None)
    app = create_app(settings=_BASE_SETTINGS, providers=_build_providers(loader=loader))
    with TestClient(app) as client:
        response = _post(
            client,
            {"type": _TYPE, "deprecated": True},
            headers=_ctx_header(permissions=[CONNECTOR_READ]),
        )
    assert response.status_code == 403, response.text


def test_deprecate_when_loader_unwired_is_500() -> None:
    app = create_app(settings=_BASE_SETTINGS, providers=_build_providers(loader=None))
    with TestClient(app, raise_server_exceptions=False) as client:
        response = _post(client, {"type": _TYPE, "deprecated": True})
    assert response.status_code == 500


def test_deprecate_rejects_unknown_body_fields() -> None:
    loader, _ = _loader_for(_baseline_payload(), vendor_overrides=None)
    app = create_app(settings=_BASE_SETTINGS, providers=_build_providers(loader=loader))
    with TestClient(app) as client:
        response = _post(client, {"type": _TYPE, "deprecated": True, "unexpected": "x"})
    assert response.status_code == 422, response.text


def test_deprecate_missing_type_is_422() -> None:
    loader, _ = _loader_for(_baseline_payload(), vendor_overrides=None)
    app = create_app(settings=_BASE_SETTINGS, providers=_build_providers(loader=loader))
    with TestClient(app) as client:
        response = _post(client, {"deprecated": True})
    assert response.status_code == 422, response.text
