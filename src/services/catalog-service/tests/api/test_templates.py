"""Template REST route tests (CS-IMPL-017 — templates.py)."""

from __future__ import annotations

import json

from custos_cel import Literal, parse
from fastapi.testclient import TestClient

from tests.api.conftest import (
    FakeCatalogStore,
    FakeConnectorReferences,
    FakeDefinitionStore,
    FakeMetadataStore,
    admin_header,
    callctx_header,
    minimal_template,
    minimal_workflow,
    seed_builtin_echo,
)

# ---------------------------------------------------------------------------
# Publish
# ---------------------------------------------------------------------------


def test_publish_template_returns_201_and_ref(
    client: TestClient,
    stores: tuple[FakeDefinitionStore, FakeCatalogStore, FakeMetadataStore],
) -> None:
    _, catalog_store, _ = stores
    seed_builtin_echo(catalog_store)

    resp = client.post(
        "/v1/workspaces/ws-1/templates",
        json={"definition": json.dumps(minimal_template())},
        headers=admin_header(),
    )

    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body == {"workspaceId": "ws-1", "templateName": "orders-tmpl", "version": 1}


def test_publish_template_requires_write_permission(client: TestClient) -> None:
    resp = client.post(
        "/v1/workspaces/ws-1/templates",
        json={"definition": json.dumps(minimal_template())},
        headers=callctx_header(workspace_id="ws-1", permissions=["catalog:templates:read"]),
    )
    assert resp.status_code == 403


def test_sourced_wait_template_materializes_with_scoped_reference_checks(
    client: TestClient,
    connector_references: FakeConnectorReferences,
    stores: tuple[FakeDefinitionStore, FakeCatalogStore, FakeMetadataStore],
) -> None:
    """Template and materialized Workflow publication both use the scoped source reader."""
    connector_references.rows.add(("ws-1", "registry"))
    source = {"connectorInstanceId": "registry", "deliveryModes": ["push"]}
    doc = minimal_template()
    doc["spec"]["placeholders"].append(
        {
            "name": "selector",
            "type": "string",
            "default": "event.data.key == 'one'",
        }
    )
    doc["spec"]["workflow"]["steps"] = [
        {
            "id": "await",
            "waitFor": {
                "eventKey": "${{ placeholders.topic }}",
                "selector": "${{ placeholders.selector }}",
                "source": source,
            },
        }
    ]
    headers = callctx_header(
        permissions=[
            "catalog:templates:write",
            "catalog:templates:read",
            "catalog:workflows:write",
        ]
    )
    published = client.post(
        "/v1/workspaces/ws-1/templates", json={"definition": doc}, headers=headers
    )
    assert published.status_code == 201, published.text
    bound_topic = 'orders"}}\\\n\U0001f680'
    materialized = client.post(
        "/v1/workspaces/ws-1/templates/orders-tmpl@1:materialize",
        json={"targetName": "source-prod", "bindings": {"topic": bound_topic}},
        headers=headers,
    )
    assert materialized.status_code == 201, materialized.text
    stored = stores[0].workflows[("ws-1", "source-prod")][0]
    assert stored.normalized_doc["spec"]["steps"][0]["waitFor"]["source"] == source
    expression = stored.normalized_doc["spec"]["steps"][0]["waitFor"]["eventKey"]
    assert expression == "${{ " + json.dumps(bound_topic, ensure_ascii=False) + " }}"
    literal = parse(expression[3:-2].strip())
    assert isinstance(literal, Literal) and isinstance(literal.value, str)
    assert literal.value == bound_topic
    assert literal.value.encode("utf-8") == bound_topic.encode("utf-8")
    assert connector_references.calls == [("ws-1", "registry"), ("ws-1", "registry")]


def test_wait_json_placeholder_rejects_non_string_binding(client: TestClient) -> None:
    """A polymorphic JSON placeholder must still produce a string in a wait expression slot."""
    doc = minimal_template()
    doc["spec"]["placeholders"][0]["type"] = "json"
    doc["spec"]["workflow"]["steps"] = [
        {
            "id": "await",
            "waitFor": {"eventKey": "${{ placeholders.topic }}"},
        }
    ]
    published = client.post(
        "/v1/workspaces/ws-1/templates", json={"definition": doc}, headers=admin_header()
    )
    assert published.status_code == 201, published.text
    response = client.post(
        "/v1/workspaces/ws-1/templates/orders-tmpl@1:materialize",
        json={"targetName": "invalid-wait", "bindings": {"topic": True}},
        headers=admin_header(),
    )
    assert response.status_code == 400
    assert "must bind to strings" in response.text


# ---------------------------------------------------------------------------
# Get-by-ref
# ---------------------------------------------------------------------------


def test_get_template_by_ref_returns_body(
    client: TestClient,
    stores: tuple[FakeDefinitionStore, FakeCatalogStore, FakeMetadataStore],
) -> None:
    _, catalog_store, _ = stores
    seed_builtin_echo(catalog_store)
    client.post(
        "/v1/workspaces/ws-1/templates",
        json={"definition": json.dumps(minimal_template())},
        headers=admin_header(),
    )

    resp = client.get(
        "/v1/workspaces/ws-1/templates/orders-tmpl@1",
        headers=admin_header(),
    )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["templateName"] == "orders-tmpl"
    assert body["version"] == 1
    assert body["document"]["kind"] == "WorkflowTemplate"


def test_get_template_by_ref_404_when_missing(client: TestClient) -> None:
    resp = client.get(
        "/v1/workspaces/ws-1/templates/missing@1",
        headers=admin_header(),
    )
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "catalog.template_not_found"


def test_get_template_by_ref_400_on_malformed(client: TestClient) -> None:
    resp = client.get(
        "/v1/workspaces/ws-1/templates/not-a-ref",
        headers=admin_header(),
    )
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Materialize
# ---------------------------------------------------------------------------


def test_materialize_publishes_a_workflow(
    client: TestClient,
    stores: tuple[FakeDefinitionStore, FakeCatalogStore, FakeMetadataStore],
) -> None:
    _, catalog_store, _ = stores
    seed_builtin_echo(catalog_store)
    # Publish the source template.
    client.post(
        "/v1/workspaces/ws-1/templates",
        json={"definition": json.dumps(minimal_template())},
        headers=admin_header(),
    )

    resp = client.post(
        "/v1/workspaces/ws-1/templates/orders-tmpl@1:materialize",
        json={"targetName": "orders-prod", "bindings": {"topic": "events"}},
        headers=admin_header(),
    )

    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["workflowName"] == "orders-prod"
    assert body["version"] == 1


def test_materialize_uses_default_bindings_when_omitted(
    client: TestClient,
    stores: tuple[FakeDefinitionStore, FakeCatalogStore, FakeMetadataStore],
) -> None:
    _, catalog_store, _ = stores
    seed_builtin_echo(catalog_store)
    client.post(
        "/v1/workspaces/ws-1/templates",
        json={"definition": json.dumps(minimal_template())},
        headers=admin_header(),
    )

    resp = client.post(
        "/v1/workspaces/ws-1/templates/orders-tmpl@1:materialize",
        json={"targetName": "orders-prod"},
        headers=admin_header(),
    )

    assert resp.status_code == 201, resp.text


def test_materialize_404_on_missing_source(client: TestClient) -> None:
    resp = client.post(
        "/v1/workspaces/ws-1/templates/missing@1:materialize",
        json={"targetName": "orders-prod"},
        headers=admin_header(),
    )
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Extract template (routed through workflows.py but lives in this file because
# it produces a template)
# ---------------------------------------------------------------------------


def test_extract_publishes_a_template(
    client: TestClient,
    stores: tuple[FakeDefinitionStore, FakeCatalogStore, FakeMetadataStore],
) -> None:
    _, catalog_store, _ = stores
    seed_builtin_echo(catalog_store)
    # Publish the source workflow.
    client.post(
        "/v1/workspaces/ws-1/workflows",
        json={"definition": json.dumps(minimal_workflow())},
        headers=admin_header(),
    )

    resp = client.post(
        "/v1/workspaces/ws-1/workflows/orders@1:extractTemplate",
        json={
            "selectors": [
                {
                    "path": "spec.steps[0].with.message",
                    "placeholderName": "msg",
                    "placeholderType": "string",
                },
            ],
            "templateName": "orders-tmpl",
        },
        headers=admin_header(),
    )

    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["templateName"] == "orders-tmpl"
    assert body["version"] == 1


def test_extract_404_when_source_workflow_missing(client: TestClient) -> None:
    resp = client.post(
        "/v1/workspaces/ws-1/workflows/missing@1:extractTemplate",
        json={
            "selectors": [
                {
                    "path": "spec.steps[0].with.message",
                    "placeholderName": "msg",
                    "placeholderType": "string",
                },
            ],
            "templateName": "orders-tmpl",
        },
        headers=admin_header(),
    )
    assert resp.status_code == 404
