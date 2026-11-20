"""Orchestrate real receiver effects in the disposable managed deployment.

The seed job uses production providers, never test SQL or application fakes.
Public clients have only workspace/upstream credentials and Gateway access.
The runner separately observes real wait registrations and supplies synthetic
upstream events through Connector's real producer. Reports distinguish executed
scenarios from the still-required full matrix. No credentials or response bodies
are printed.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import hmac
import ipaddress
import json
import os
import re
import secrets
import socket
import subprocess
import sys
import textwrap
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Literal

WORKSPACE = "workspace-default"
OPERATOR = "receiver-certification-operator"
OPERATOR_SECRET = "receiver-certification-operator"
WORKFLOW = "receiver-certification"
WAIT_WORKFLOW = "receiver-certification-wait"
WEBHOOK_WORKFLOW = "receiver-certification-webhook"
VENDOR_WORKFLOW = "receiver-certification-vendor-push"
PULL_WORKFLOW = "receiver-certification-pull"
PULL_WAIT_WORKFLOW = "receiver-certification-pull-wait"
CONNECTOR = "receiver-fixture"
PULL_CONNECTOR = "receiver-pull-fixture"
PULL_SELECTOR = "event.data.subject.tag != ''"
EVENT_KEY = "oci.image.pushed"
CREDENTIAL_NAMESPACE = "custos-connectors"
WEBHOOK_SECRET = "receiver-certification-webhook"
OTHER_WORKSPACE = "receiver-other-tenant"
REPORT_PREFIX = "RECEIVER_EVIDENCE "
CLIENT_PHASES = (
    "wait-setup",
    "exercise",
    "rotation",
    "vendor-setup",
    "vendor-observe",
    "vendor-redelivery",
    "pull-setup",
    "pull-resume",
    "pull-start",
    "pull-pause",
)


class CertificationFailure(RuntimeError):
    """Carry only a fixed scenario label and HTTP status, never a response or credential."""


def check_network_isolation(address: str) -> None:
    """Require dropped backend connections, not DNS errors, refusals or HTTP authentication denial.

    The runner supplies a Ready Trigger pod's private IPv4 address. A successful
    public Gateway request is the client connectivity control; only timeouts at
    the app, local Dapr HTTP/gRPC and remote invocation ports count as isolation.
    """
    ip = ipaddress.IPv4Address(address)
    if not ip.is_private or ip.is_loopback or ip.is_unspecified:
        raise CertificationFailure("network-policy: invalid backend pod address")
    for port in (8080, 3500, 50001, 50002):
        try:
            with socket.create_connection((str(ip), port), timeout=3):
                raise CertificationFailure(
                    f"network-policy: backend port {port} is reachable"
                )
        except TimeoutError:
            continue
        except OSError as error:
            raise CertificationFailure(
                f"network-policy: backend port {port} was not proven filtered"
            ) from error


def workflow_document(
    *,
    waiting: bool = False,
    name: str | None = None,
    delivery_mode: Literal["push", "pull"] = "push",
) -> dict[str, Any]:
    """Build a pure workflow, optionally parked on a real source-associated durable wait."""
    steps: list[dict[str, Any]] = [{"id": "effect", "let": {"observed": "${{ true }}"}}]
    if waiting:
        steps.insert(
            0,
            {
                "id": "await-source",
                "waitFor": {
                    "eventKey": "${{ '" + EVENT_KEY + "' }}",
                    "selector": '${{ "'
                    + (
                        PULL_SELECTOR
                        if delivery_mode == "pull"
                        else "event.data.route == 'match'"
                    )
                    + '" }}',
                    "ttl": "PT1H",
                    "source": {
                        "connectorInstanceId": (
                            PULL_CONNECTOR if delivery_mode == "pull" else CONNECTOR
                        ),
                        "deliveryModes": [delivery_mode],
                    },
                },
            },
        )
        steps[1]["needs"] = ["await-source"]
    return {
        "apiVersion": "custos.dev/v1",
        "kind": "Workflow",
        "metadata": {
            "name": name or (WAIT_WORKFLOW if waiting else WORKFLOW),
            "workspace": WORKSPACE,
        },
        "spec": {"steps": steps},
    }


async def seed() -> None:
    """Seed real provider rows, reference connector instances and a workspace-only operator grant."""
    from datetime import UTC, datetime, timedelta

    from custos_auth.roles import ROLE_WORKSPACE_OPERATOR
    from custos_auth.tokens import hash_token
    from custos_bootstrap.__main__ import resolve_dsn
    from custos_pg import (
        PgAuthAdapter,
        PgCatalogAdapter,
        PgConnectorInstanceAdapter,
        PgDefinitionAdapter,
    )
    from custos_pg.pool import LazyPool, register_jsonb_codec
    from custos_spl.ids import (
        ConnectorInstanceId,
        PrincipalId,
        RoleBindingId,
        ServiceTokenId,
        WorkspaceId,
    )
    from custos_spl.interfaces.auth_store import (
        RoleBinding,
        ServiceAccount,
        ServiceToken,
        WorkspaceScope,
    )
    from custos_spl.interfaces.connector_instance_store import ConnectorInstance

    dsn = resolve_dsn(os.environ)
    if not dsn:
        raise CertificationFailure("seed: database reference unavailable")
    pool = LazyPool(dsn, init=register_jsonb_codec)
    auth, definitions = PgAuthAdapter(lazy=pool), PgDefinitionAdapter(lazy=pool)
    now = datetime.now(UTC)
    try:
        await auth.put_principal(
            ServiceAccount(
                kind="serviceAccount",
                principal_id=PrincipalId(OPERATOR),
                workspace_id=WorkspaceId(WORKSPACE),
                display_name="Disposable receiver certification operator",
                disabled_at=None,
                disabled_reason=None,
                created_at=now,
            )
        )
        await auth.put_role_binding(
            RoleBinding(
                binding_id=RoleBindingId(OPERATOR),
                principal_id=PrincipalId(OPERATOR),
                role_id=ROLE_WORKSPACE_OPERATOR,
                scope=WorkspaceScope(WorkspaceId(WORKSPACE)),
                bound_at=now,
                bound_by=PrincipalId(OPERATOR),
            )
        )
        await auth.put_service_token(
            ServiceToken(
                token_id=ServiceTokenId(OPERATOR),
                service_account_id=PrincipalId(OPERATOR),
                hash=hash_token(os.environ["CUSTOS_TEST_TOKEN"]),
                issued_at=now,
                expires_at=now + timedelta(hours=2),
                revoked_at=None,
                revoked_by=None,
                revoked_reason=None,
            )
        )
        for name, waiting in (
            (WORKFLOW, False),
            (WAIT_WORKFLOW, True),
            (WEBHOOK_WORKFLOW, False),
            (VENDOR_WORKFLOW, False),
            (PULL_WORKFLOW, False),
        ):
            await definitions.put_workflow_version(
                WORKSPACE, name, "1", workflow_document(waiting=waiting, name=name)
            )
        await definitions.put_workflow_version(
            WORKSPACE,
            PULL_WAIT_WORKFLOW,
            "1",
            workflow_document(
                waiting=True, name=PULL_WAIT_WORKFLOW, delivery_mode="pull"
            ),
        )
        manifest = json.loads(Path("/smoke/connector-manifest.json").read_text())
        await PgCatalogAdapter(lazy=pool).put_connector_type_version(
            type=manifest["metadata"]["type"],
            version=manifest["metadata"]["version"],
            digest="sha256:"
            + hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest(),
            image_ref=os.environ["SMOKE_CONNECTOR_IMAGE"],
            normalized_manifest=manifest,
        )
        instances = PgConnectorInstanceAdapter(lazy=pool)
        for workspace, connector_id, secret_name in (
            (WORKSPACE, CONNECTOR, WEBHOOK_SECRET),
            (OTHER_WORKSPACE, CONNECTOR, WEBHOOK_SECRET + "-other"),
            (WORKSPACE, PULL_CONNECTOR, WEBHOOK_SECRET),
        ):
            await instances.put_connector_instance(
                WorkspaceId(workspace),
                ConnectorInstance(
                    workspace_id=WorkspaceId(workspace),
                    instance_id=ConnectorInstanceId(connector_id),
                    type=manifest["metadata"]["type"],
                    version=manifest["metadata"]["version"],
                    name="Disposable reference connector",
                    lease_ttl_seconds=60,
                    enabled=True,
                    status="active",
                    health_status=None,
                    target_config={},
                    credentials_authentication={
                        "webhookVerification": {
                            "scheme": "hmac-sha256",
                            "authenticationType": "x-dapr-secret",
                            "authentication": {
                                "secretName": secret_name,
                                "namespace": CREDENTIAL_NAMESPACE,
                                "store": "custos-secretstore",
                            },
                            "materialKey": "token",
                        }
                    },
                    used_capabilities=None,
                    created_at=now,
                    updated_at=now,
                ),
            )
    finally:
        await pool.aclose()
    print("Synthetic definitions and workspace-only operator grant seeded.", flush=True)


class Gateway:
    """Exercise deployed public contracts without database access or fabricated call contexts."""

    def __init__(self) -> None:
        """Read only explicitly projected test inputs; never acquire platform-wide authority."""
        self.base = os.environ["CUSTOS_GATEWAY_URL"].rstrip("/")
        if self.base != "http://custos-api-gateway:8080":
            raise CertificationFailure(
                "client must target the disposable public Gateway"
            )
        self.token = os.environ["CUSTOS_TEST_TOKEN"]
        self.phase = os.environ["CUSTOS_TEST_PHASE"]
        self.sequence = 0

    def request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | list[object] | None = None,
        *,
        expected: tuple[int, ...] = (200,),
        authenticated: bool = True,
        extra_headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Require the exact expected status and JSON shape without publishing response content."""
        self.sequence += 1
        headers = {"accept": "application/json"}
        if authenticated:
            headers["authorization"] = "Bearer " + self.token
        data = None
        if body is not None:
            data = json.dumps(body).encode()
            headers["content-type"] = "application/json"
        if method not in ("GET", "HEAD"):
            headers["idempotency-key"] = (
                f"receiver-certification-{self.phase}-{self.sequence}"
            )
        headers.update(extra_headers or {})
        request = urllib.request.Request(
            self.base + path, data=data, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(request, timeout=90) as response:
                status, raw = response.status, response.read()
        except urllib.error.HTTPError as error:
            with error:
                status, raw = error.code, error.read()
        if status not in expected:
            try:
                problem = json.loads(raw)
            except (ValueError, UnicodeError):
                problem = None
            code = "unclassified"
            if isinstance(problem, dict) and problem.get("code") in (
                "trigger.dispatch_unavailable",
                "trigger.dispatch_failed",
                "trigger.source_unavailable",
                "callctx_unavailable",
                "workflow.runtime_unavailable",
            ):
                code = problem["code"]
            raise CertificationFailure(f"{method} {path}: HTTP {status} ({code})")
        if not raw:
            return {}
        decoded: object = json.loads(raw)
        if not isinstance(decoded, dict):
            raise CertificationFailure(f"{method} {path}: invalid response shape")
        return decoded

    def runs(self, workflow: str | None = None) -> list[dict[str, Any]]:
        """Page actual public runs, optionally isolated from other continuously active receivers."""
        query = {"workflowVersionId": f"{WORKSPACE}/{workflow}@1"} if workflow else {}
        rows: list[dict[str, Any]] = []
        seen: set[str] = set()
        while True:
            path = f"/v1/workspaces/{WORKSPACE}/runs?" + urllib.parse.urlencode(query)
            result = self.request("GET", path)
            items = result.get("items")
            if not isinstance(items, list) or not all(
                isinstance(row, dict) and isinstance(row.get("runId"), str)
                for row in items
            ):
                raise CertificationFailure("runs: invalid list response shape")
            rows.extend(items)
            cursor = result.get("nextCursor")
            if cursor is None:
                return rows
            if not isinstance(cursor, str) or not cursor or cursor in seen:
                raise CertificationFailure("runs: invalid pagination cursor")
            seen.add(cursor)
            query["cursor"] = cursor

    def wait_terminal(self, run_id: str, *, timeout: float = 180) -> None:
        """Require a successful real workflow; failures and expiry are not skipped scenarios."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            row = self.request("GET", f"/v1/workspaces/{WORKSPACE}/runs/{run_id}")
            state = row.get("status")
            if state == "succeeded":
                return
            if state in ("failed", "cancelled"):
                raise CertificationFailure(f"workflow: terminal {state}")
            time.sleep(1)
        raise CertificationFailure("workflow: completion deadline exceeded")

    def subscription(
        self,
        source: str,
        *,
        workflow: str = WORKFLOW,
        connector: str = CONNECTOR,
    ) -> str:
        """Configure a real receiver while preserving normal Gateway authorization and validation."""
        body: dict[str, Any] = {
            "sourceType": source,
            "workflowId": f"{WORKSPACE}/{workflow}",
            "targetWorkflowVersionId": f"{WORKSPACE}/{workflow}@1",
        }
        if source == "scheduled":
            body["sourceConfig"] = {"intervalSeconds": 10}
        elif source in ("webhook", "vendor-push", "pull"):
            body["sourceConfig"] = {"connectorInstanceId": connector}
            body["selector"] = (
                PULL_SELECTOR if source == "pull" else "event.data.route == 'match'"
            )
            if source == "pull":
                body["sourceConfig"]["pollIntervalSeconds"] = 10
        result = self.request(
            "POST", f"/v1/workspaces/{WORKSPACE}/triggers", body, expected=(201,)
        )
        identifier = result.get("subscriptionId")
        if not isinstance(identifier, str) or not identifier:
            raise CertificationFailure("subscription: missing identifier")
        return identifier

    def start_run(self, workflow: str) -> str:
        """Start an immutable fixture version publicly and require its real run identifier."""
        result = self.request(
            "POST",
            f"/v1/workspaces/{WORKSPACE}/runs",
            {"workflowVersionId": f"{WORKSPACE}/{workflow}@1", "inputs": {}},
            expected=(202,),
        )
        run_id = result.get("runId")
        if not isinstance(run_id, str) or not run_id:
            raise CertificationFailure("wait: missing workflow run identifier")
        return run_id

    def pull_cursor(
        self, *, changed_from: str | None = None, settled: bool = False
    ) -> str:
        """Require a redacted committed cursor, optionally advanced or without a live lease.

        A not-yet-created cursor is allowed only while waiting for the first
        real tick. Pause acknowledgement alone does not imply an admitted hook
        finished; settled reads wait for its ordinary lease release.
        """
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            row = self.request(
                "GET",
                f"/v1/workspaces/{WORKSPACE}/connectors/{PULL_CONNECTOR}/cursor",
                expected=(200, 404),
            )
            error = row.get("error")
            if (
                isinstance(error, dict)
                and error.get("code") == "connector.cursor.not_found"
            ):
                time.sleep(1)
                continue
            fingerprint = row.get("valueFingerprint")
            if (
                row.get("encoding") == "oci-list-tags-v1"
                and fingerprint is None
                and row.get("valueLength") is None
                and "leaseHolder" in row
            ):
                time.sleep(1)
                continue
            if (
                row.get("workspaceId") != WORKSPACE
                or row.get("instanceId") != PULL_CONNECTOR
                or row.get("encoding") != "oci-list-tags-v1"
                or not isinstance(fingerprint, str)
                or re.fullmatch("[0-9a-f]{64}", fingerprint) is None
                or not isinstance(row.get("valueLength"), int)
                or row["valueLength"] <= 0
                or "leaseHolder" not in row
            ):
                raise CertificationFailure("pull: invalid redacted cursor response")
            if fingerprint != changed_from and (
                not settled or row["leaseHolder"] is None
            ):
                return fingerprint
            time.sleep(1)
        raise CertificationFailure(
            "pull: committed cursor observation deadline exceeded"
        )

    def webhook(
        self,
        batch: list[object],
        key: str,
        *,
        workspace: str = WORKSPACE,
        expected: tuple[int, ...] = (200,),
    ) -> dict[str, Any]:
        """Sign exact public request bytes using only an upstream test credential, never a mesh token."""
        signature = hmac.new(
            key.encode(), json.dumps(batch).encode(), hashlib.sha256
        ).hexdigest()
        return self.request(
            "POST",
            f"/v1/webhooks/workspaces/{workspace}/connectors/{CONNECTOR}/events",
            batch,
            expected=expected,
            authenticated=False,
            extra_headers={"x-custos-signature": "sha256=" + signature},
        )


def exercise_webhook(gateway: Gateway, *, rotating: bool) -> list[str]:
    """Require indexed mixed-batch counts, real completion and stable effects across redelivery.

    Rotation uses a fresh runner-updated Kubernetes Secret, while both clients
    retain the old upstream key. The other tenant has the same instance ID and
    a different actual secret, not an absent-instance denial.
    """
    initial_key = os.environ["CUSTOS_WEBHOOK_KEY"]
    key = os.environ["CUSTOS_ROTATED_WEBHOOK_KEY"] if rotating else initial_key
    batch: list[object] = [
        {"eventId": gateway.phase + "-match", "eventType": EVENT_KEY, "route": "match"},
        {
            "eventId": gateway.phase + "-ignore",
            "eventType": EVENT_KEY,
            "route": "ignore",
        },
        {"eventType": EVENT_KEY},
        {"eventId": gateway.phase + "-missing-type"},
        {"eventId": gateway.phase + "-unknown", "eventType": "not.produced"},
        "malformed-event",
    ]
    if not rotating:
        gateway.subscription("webhook", workflow=WEBHOOK_WORKFLOW)
    before = {row["runId"] for row in gateway.runs(WEBHOOK_WORKFLOW)}
    gateway.webhook(
        batch, initial_key if rotating else key + "-invalid", expected=(401,)
    )
    gateway.webhook(batch, key, workspace=OTHER_WORKSPACE, expected=(401,))
    if gateway.webhook(batch, key) != {"accepted": 2, "rejected": 4}:
        raise CertificationFailure("webhook: incorrect mixed-batch accounting")
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        new = [
            row for row in gateway.runs(WEBHOOK_WORKFLOW) if row["runId"] not in before
        ]
        if len(new) > 1:
            raise CertificationFailure("webhook: unexpected workflow effect count")
        if new:
            gateway.wait_terminal(new[0]["runId"])
            if not rotating:
                waiting_run = os.environ.get("CUSTOS_WAIT_RUN_ID")
                if not waiting_run:
                    raise CertificationFailure(
                        "webhook: missing registered wait control"
                    )
                gateway.wait_terminal(waiting_run)
            break
        time.sleep(1)
    else:
        raise CertificationFailure("webhook: no workflow effect before deadline")
    stable = {row["runId"] for row in gateway.runs(WEBHOOK_WORKFLOW)}
    if gateway.webhook(batch, key) != {"accepted": 2, "rejected": 4}:
        raise CertificationFailure("webhook: incorrect redelivery accounting")
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if {row["runId"] for row in gateway.runs(WEBHOOK_WORKFLOW)} != stable:
            raise CertificationFailure("webhook: redelivery created an extra workflow")
        time.sleep(1)
    return [
        "webhook-rotated-secret-old-key-denied-new-key-completes"
        if rotating
        else "webhook-mixed-batch-real-workflow-start-and-source-resume-completion",
        "webhook-same-instance-id-cross-tenant-signature-denied",
        "webhook-redelivery-no-extra-workflow-over-10-seconds",
    ]


def exercise(*, rotating: bool = False) -> None:
    """Emit only completed public-client scenarios; rotation runs after the runner changes the key."""
    gateway = Gateway()
    started = time.monotonic()
    completed: list[str] = []
    if rotating:
        completed.extend(exercise_webhook(gateway, rotating=True))
        emit_report(completed, started)
        return
    gateway.request(
        "GET",
        f"/v1/workspaces/{WORKSPACE}/runs",
        expected=(401,),
        authenticated=False,
    )
    gateway.request("GET", "/v1/workspaces/receiver-other-tenant/runs", expected=(403,))
    completed.append("public-gateway-authentication-and-workspace-denial")
    gateway.runs()
    check_network_isolation(os.environ["CUSTOS_BACKEND_POD_IP"])
    completed.append("public-client-denied-backend-and-dapr-ports")
    manual = gateway.subscription("manual")
    fired = gateway.request(
        "POST",
        f"/v1/workspaces/{WORKSPACE}/triggers/{manual}:fire",
        {},
    )
    run_id = fired.get("runId")
    if not isinstance(run_id, str) or not run_id:
        raise CertificationFailure("manual: missing run identifier")
    gateway.wait_terminal(run_id)
    completed.append("manual-trigger-real-workflow-completion")
    before = {row["runId"] for row in gateway.runs()}
    scheduled = gateway.subscription("scheduled")
    try:
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            new = [row for row in gateway.runs() if row["runId"] not in before]
            if new:
                for row in new:
                    gateway.wait_terminal(row["runId"])
                completed.append("scheduled-receiver-real-workflow-completion")
                break
            time.sleep(1)
        else:
            raise CertificationFailure("scheduled: no workflow effect before deadline")
    finally:
        gateway.request(
            "PATCH",
            f"/v1/workspaces/{WORKSPACE}/triggers/{scheduled}",
            {"state": "paused"},
        )
    completed.extend(exercise_webhook(gateway, rotating=False))
    emit_report(completed, started)


def emit_report(
    completed: list[str],
    started: float,
    *,
    waiting_run: str | None = None,
) -> None:
    """Publish controlled scenario names and elapsed time, never test credentials or payloads."""
    report: dict[str, Any] = {
        "sha": os.environ["GITHUB_SHA"],
        "phase": os.environ["CUSTOS_TEST_PHASE"],
        "completedScenarios": completed,
        "elapsedSeconds": round(time.monotonic() - started, 3),
        "fullReceiverCertification": False,
    }
    if waiting_run is not None:
        report["waitingRunId"] = waiting_run
    print(
        REPORT_PREFIX + json.dumps(report),
        flush=True,
    )


def setup_wait(*, vendor: bool = False) -> None:
    """Start a public source-associated wait and optional vendor start intent, not a stored test wait."""
    gateway = Gateway()
    started = time.monotonic()
    if vendor:
        gateway.subscription("vendor-push", workflow=VENDOR_WORKFLOW)
    run_id = gateway.start_run(WAIT_WORKFLOW)
    emit_report(["workflow-source-wait-start-accepted"], started, waiting_run=run_id)


def publish_vendor_event() -> None:
    """Publish a synthetic upstream event using Connector's real normalizer and configured Dapr producer.

    Only the external producer input is synthetic. No broker callback, Trigger
    dispatch, Workflow client or provider is replaced. Repeated calls preserve
    event identity while receive timestamps change, like actual redelivery.
    """
    import runtime as smoke

    script = textwrap.dedent("""
        import asyncio, sys
        from custos_connector.listen.normalizer import EventNormalizer
        from custos_connector.listen.publisher import DaprPubSubEventPublisher
        from custos_connector.providers import load_event_publisher
        from custos_connector.settings import load_settings

        async def publish() -> None:
            \"\"\"Use the production publisher factory and close its independently owned client.\"\"\"
            publisher, client = load_event_publisher(settings=load_settings())
            try:
                if not isinstance(publisher, DaprPubSubEventPublisher) or client is None:
                    raise RuntimeError("real Dapr event publisher is required")
                event = EventNormalizer(produced_event_types=[sys.argv[3]]).normalize(
                    {"eventId": "receiver-certification-vendor-event",
                     "eventType": sys.argv[3], "route": "match"},
                    workspace_id=sys.argv[1], instance_id=sys.argv[2], delivery_mode="push",
                )
                await publisher.publish(event)
                print("VENDOR_EVENT_PUBLISHED")
            finally:
                if client is not None:
                    await client.aclose()

        asyncio.run(publish())
    """)
    published = smoke.command(
        [
            "kubectl",
            "-n",
            os.environ["NS"],
            "exec",
            "deployment/custos-connector-service",
            "-c",
            "connector-service",
            "--",
            "python",
            "-c",
            script,
            WORKSPACE,
            CONNECTOR,
            EVENT_KEY,
        ],
        timeout=60,
    )
    if published.strip() != "VENDOR_EVENT_PUBLISHED":
        raise CertificationFailure(
            "vendor-push: real broker publication was not acknowledged"
        )


def exercise_vendor(*, redelivery: bool) -> None:
    """Require public Workflow start/resume effects independently of the real producer's acknowledgement."""
    gateway = Gateway()
    started = time.monotonic()
    if redelivery:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if len(gateway.runs(VENDOR_WORKFLOW)) != 1:
                raise CertificationFailure(
                    "vendor-push: unexpected redelivery workflow count"
                )
            time.sleep(1)
        emit_report(
            ["vendor-push-redelivery-no-extra-workflow-over-10-seconds"], started
        )
        return
    waiting_run = os.environ.get("CUSTOS_WAIT_RUN_ID")
    if not waiting_run:
        raise CertificationFailure("vendor-push: missing registered wait control")
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        runs = gateway.runs(VENDOR_WORKFLOW)
        if len(runs) > 1:
            raise CertificationFailure("vendor-push: unexpected workflow effect count")
        if runs:
            gateway.wait_terminal(runs[0]["runId"])
            gateway.wait_terminal(waiting_run)
            emit_report(
                ["vendor-push-real-broker-workflow-start-and-source-resume-completion"],
                started,
            )
            return
        time.sleep(1)
    raise CertificationFailure("vendor-push: no workflow effect before deadline")


def exercise_pull(phase: str) -> None:
    """Require real hook/broker effects, resume-only demand and a settled operator pause.

    The runner makes two real Connector replicas ready before these phases.
    Only the upstream plugin data is deterministic; no hook, cursor provider,
    broker callback or Workflow implementation is replaced.
    """
    gateway = Gateway()
    started = time.monotonic()
    path = f"/v1/workspaces/{WORKSPACE}/connectors/{PULL_CONNECTOR}/pull-loop:"
    if phase == "pull-setup":
        gateway.request("POST", path + "pause")
        registered_run = gateway.start_run(PULL_WAIT_WORKFLOW)
        emit_report(
            ["pull-source-wait-start-accepted"], started, waiting_run=registered_run
        )
        return
    if phase == "pull-resume":
        waiting_run = os.environ.get("CUSTOS_WAIT_RUN_ID")
        if not waiting_run:
            raise CertificationFailure("pull: missing registered resume-only wait")
        gateway.request("POST", path + "resume")
        gateway.wait_terminal(waiting_run)
        gateway.pull_cursor()
        waits = gateway.runs(PULL_WAIT_WORKFLOW)
        if (
            gateway.runs(PULL_WORKFLOW)
            or len(waits) != 1
            or waits[0]["runId"] != waiting_run
        ):
            raise CertificationFailure(
                "pull: resume-only demand created a start effect"
            )
        emit_report(
            ["pull-real-hook-broker-source-resume-without-start-intent"], started
        )
        return
    if phase == "pull-start":
        previous = gateway.pull_cursor()
        gateway.subscription("pull", workflow=PULL_WORKFLOW, connector=PULL_CONNECTOR)
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            runs = gateway.runs(PULL_WORKFLOW)
            if runs:
                gateway.wait_terminal(runs[0]["runId"])
                gateway.pull_cursor(changed_from=previous)
                emit_report(
                    ["pull-real-hook-broker-workflow-start-and-cursor-progress"],
                    started,
                )
                return
            time.sleep(1)
        raise CertificationFailure("pull: no start effect before deadline")
    if phase == "pull-pause":
        gateway.request("POST", path + "pause")
        previous = gateway.pull_cursor(settled=True)
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if gateway.pull_cursor(settled=True) != previous:
                raise CertificationFailure(
                    "pull: cursor advanced while operator-paused"
                )
            time.sleep(1)
        emit_report(
            ["pull-paused-cursor-stable-over-20-seconds-with-two-ready-replicas"],
            started,
        )
        return
    raise CertificationFailure("pull: unsupported certification phase")


def observe_wait(
    run_id: str,
    *,
    connector: str = CONNECTOR,
    delivery_mode: Literal["push", "pull"] = "push",
) -> None:
    """Require the actual Trigger provider to expose this live wait before admitting a source event.

    The runner observes storage inside the owned Trigger deployment. The public
    test client never receives database access, and no wait record is inserted
    by the test. Registration must come from the deployed Workflow worker.
    """
    import runtime as smoke

    script = textwrap.dedent("""
        import asyncio, os, sys
        from datetime import UTC, datetime
        from custos_pg import PgMetadataAdapter
        from custos_pg.pool import LazyPool, register_jsonb_codec

        async def observe() -> None:
            \"\"\"Bound the observation and close its independent real-provider pool.\"\"\"
            pool = LazyPool(os.environ["TRIGGER_METADATA_STORE"], init=register_jsonb_codec)
            store = PgMetadataAdapter(lazy=pool)
            try:
                async with asyncio.timeout(180):
                    while True:
                        page = await store.query_resume_subscriptions(
                            sys.argv[2], connector_instance_id=sys.argv[3],
                            delivery_mode=sys.argv[4], as_of=datetime.now(UTC), limit=100,
                        )
                        matches = [row for row in page.items if str(row.run_id) == sys.argv[1]]
                        if len(matches) == 1 and str(matches[0].step_id) == "await-source":
                            print("SOURCE_WAIT_REGISTERED")
                            return
                        await asyncio.sleep(1)
            finally:
                await pool.aclose()

        asyncio.run(observe())
    """)
    observed = smoke.command(
        [
            "kubectl",
            "-n",
            os.environ["NS"],
            "exec",
            "deployment/custos-trigger-service",
            "-c",
            "trigger-service",
            "--",
            "python",
            "-c",
            script,
            run_id,
            WORKSPACE,
            connector,
            delivery_mode,
        ],
        timeout=210,
    )
    if observed.strip() != "SOURCE_WAIT_REGISTERED":
        raise CertificationFailure("wait: durable source registration was not observed")


def prepare() -> None:
    """Create test-only Secret/script and seed through the actual bootstrap image before admission."""
    import runtime as smoke
    import yaml
    from custos_auth.tokens import mint_token

    namespace, tag = os.environ["NS"], os.environ["GITHUB_SHA"]
    config = Path(os.environ["RUNNER_TEMP"]) / "runtime-values.yaml"
    documents = [
        row
        for row in yaml.safe_load_all(
            smoke.command(
                [
                    "helm",
                    "template",
                    "custos",
                    os.environ["CHART"],
                    "-n",
                    namespace,
                    "-f",
                    os.environ["VALUES"],
                    "-f",
                    str(config),
                ]
            )
        )
        if row
    ]
    token, _ = mint_token()
    smoke.command(
        ["kubectl", "-n", namespace, "create", "-f", "-"],
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": OPERATOR_SECRET},
            "stringData": {
                "token": token,
                "webhook-key": secrets.token_hex(32),
                "rotated-webhook-key": secrets.token_hex(32),
                "other-webhook-key": secrets.token_hex(32),
            },
        },
    )
    smoke.command(
        ["kubectl", "-n", namespace, "create", "-f", "-"],
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "receiver-certification-script"},
            "data": {
                "receivers.py": Path(__file__).read_text(),
                "connector-manifest.json": (
                    Path(__file__).resolve().parents[3]
                    / "extensions/connectors/oci-registry/connector-manifest.json"
                ).read_text(),
            },
        },
    )
    job = smoke.seed_job(documents, tag)
    job["metadata"]["name"] = "receiver-certification-seed"
    pod = job["spec"]["template"]["spec"]
    pod["serviceAccountName"] = "smoke-custos-bootstrap"
    pod["volumes"][0]["configMap"]["name"] = "receiver-certification-script"
    container = pod["containers"][0]
    container["command"] = ["python", "/smoke/receivers.py", "seed"]
    container["env"] = [
        row for row in container["env"] if row["name"] != "SMOKE_EXECUTION_TOKEN"
    ]
    container["env"].append(
        {
            "name": "CUSTOS_TEST_TOKEN",
            "valueFrom": {"secretKeyRef": {"name": OPERATOR_SECRET, "key": "token"}},
        }
    )
    container["env"].append(
        {"name": "SMOKE_CONNECTOR_IMAGE", "value": os.environ["SMOKE_CONNECTOR_IMAGE"]}
    )
    smoke.command(["kubectl", "-n", namespace, "create", "-f", "-"], job)
    smoke.command(
        [
            "kubectl",
            "-n",
            namespace,
            "wait",
            "--for=condition=complete",
            "job/receiver-certification-seed",
            "--timeout=3m",
        ]
    )


def client_job(
    image: str,
    backend_ip: str,
    *,
    phase: str = "exercise",
    waiting_run: str = "",
) -> dict[str, Any]:
    """Project only a workspace token and upstream keys into the public-only client for one phase."""
    if phase not in CLIENT_PHASES:
        raise CertificationFailure("client: unsupported certification phase")
    job_name = "receiver-certification-" + ("client" if phase == "exercise" else phase)
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": job_name},
        "spec": {
            "backoffLimit": 0,
            "activeDeadlineSeconds": 600,
            "template": {
                "metadata": {
                    "labels": {
                        "app.kubernetes.io/instance": "custos",
                        "app.kubernetes.io/component": "smoke-test",
                    }
                },
                "spec": {
                    "automountServiceAccountToken": False,
                    "restartPolicy": "Never",
                    "securityContext": {
                        "runAsNonRoot": True,
                        "runAsUser": 1000,
                        "seccompProfile": {"type": "RuntimeDefault"},
                    },
                    "containers": [
                        {
                            "name": "client",
                            "image": image,
                            "imagePullPolicy": "IfNotPresent",
                            "command": ["python", "/smoke/receivers.py", phase],
                            "securityContext": {
                                "allowPrivilegeEscalation": False,
                                "readOnlyRootFilesystem": True,
                                "capabilities": {"drop": ["ALL"]},
                            },
                            "env": [
                                {
                                    "name": "CUSTOS_WEBHOOK_KEY",
                                    "valueFrom": {
                                        "secretKeyRef": {
                                            "name": OPERATOR_SECRET,
                                            "key": "webhook-key",
                                        }
                                    },
                                },
                                {
                                    "name": "CUSTOS_ROTATED_WEBHOOK_KEY",
                                    "valueFrom": {
                                        "secretKeyRef": {
                                            "name": OPERATOR_SECRET,
                                            "key": "rotated-webhook-key",
                                        }
                                    },
                                },
                                {"name": "CUSTOS_TEST_PHASE", "value": phase},
                                {"name": "CUSTOS_WAIT_RUN_ID", "value": waiting_run},
                                {
                                    "name": "CUSTOS_TEST_TOKEN",
                                    "valueFrom": {
                                        "secretKeyRef": {
                                            "name": OPERATOR_SECRET,
                                            "key": "token",
                                        }
                                    },
                                },
                                {
                                    "name": "CUSTOS_GATEWAY_URL",
                                    "value": "http://custos-api-gateway:8080",
                                },
                                {"name": "CUSTOS_BACKEND_POD_IP", "value": backend_ip},
                                {
                                    "name": "GITHUB_SHA",
                                    "value": os.environ["GITHUB_SHA"],
                                },
                                {"name": "PYTHONDONTWRITEBYTECODE", "value": "1"},
                            ],
                            "volumeMounts": [
                                {
                                    "name": "script",
                                    "mountPath": "/smoke",
                                    "readOnly": True,
                                }
                            ],
                            "resources": {
                                "requests": {"cpu": "50m", "memory": "64Mi"},
                                "limits": {"cpu": "500m", "memory": "256Mi"},
                            },
                        }
                    ],
                    "volumes": [
                        {
                            "name": "script",
                            "configMap": {"name": "receiver-certification-script"},
                        }
                    ],
                },
            },
        },
    }


def run_client(
    image: str,
    backend_ip: str,
    *,
    phase: str,
    waiting_run: str = "",
) -> dict[str, Any]:
    """Require Job success and exactly one matching, sanitized real-effect report for this phase."""
    import runtime as smoke

    namespace = os.environ["NS"]
    manifest = client_job(image, backend_ip, phase=phase, waiting_run=waiting_run)
    name = "job/" + manifest["metadata"]["name"]
    smoke.command(
        ["kubectl", "-n", namespace, "create", "-f", "-"],
        manifest,
    )
    deadline = time.monotonic() + 600
    succeeded = False
    while time.monotonic() < deadline:
        job = json.loads(
            smoke.command(
                [
                    "kubectl",
                    "-n",
                    namespace,
                    "get",
                    name,
                    "-o",
                    "json",
                ]
            )
        )
        status = job.get("status", {})
        if status.get("succeeded"):
            succeeded = True
            break
        if status.get("failed"):
            break
        time.sleep(2)
    logs = smoke.command(["kubectl", "-n", namespace, "logs", name])
    reports = [
        json.loads(line.removeprefix(REPORT_PREFIX))
        for line in logs.splitlines()
        if line.startswith(REPORT_PREFIX)
    ]
    if not succeeded or len(reports) != 1:
        failures = [
            line
            for line in logs.splitlines()
            if line.startswith("CERTIFICATION_FAILED ")
        ]
        for failure in failures:
            print(failure, file=sys.stderr)
        raise CertificationFailure(
            "receiver client job failed; no certification claimed"
        )
    report: dict[str, Any] = reports[0]
    if (
        not isinstance(report, dict)
        or report.get("fullReceiverCertification") is not False
        or not isinstance(report.get("completedScenarios"), list)
        or not report["completedScenarios"]
        or not all(
            isinstance(item, str) and item for item in report["completedScenarios"]
        )
        or not isinstance(report.get("elapsedSeconds"), (int, float))
        or report["elapsedSeconds"] < 0
    ):
        raise CertificationFailure(
            "receiver evidence has an invalid partial-report shape"
        )
    if report.get("sha") != os.environ["GITHUB_SHA"] or report.get("phase") != phase:
        raise CertificationFailure("receiver evidence does not match deployed commit")
    return report


def run(output: Path) -> None:
    """Run public and real-producer phases against actual Secrets, registrations and Workflow effects.

    Completed-phase evidence is retained if a later phase fails; every report
    deliberately remains partial certification. Private Secret bytes never
    enter command arguments or diagnostic output.
    """
    import runtime as smoke

    namespace = os.environ["NS"]
    started = time.monotonic()
    image = os.environ["IMAGE_PREFIX"] + "/api-gateway:" + os.environ["GITHUB_SHA"]
    pods = json.loads(
        smoke.command(
            [
                "kubectl",
                "-n",
                namespace,
                "get",
                "pods",
                "-l",
                "app.kubernetes.io/instance=custos,app.kubernetes.io/name=trigger-service",
                "-o",
                "json",
            ]
        )
    )
    ready_addresses = [
        pod["status"]["podIP"]
        for pod in pods["items"]
        if any(
            condition["type"] == "Ready" and condition["status"] == "True"
            for condition in pod.get("status", {}).get("conditions", [])
        )
    ]
    if not ready_addresses:
        raise CertificationFailure("network-policy: no Ready backend pod")
    credentials = json.loads(
        smoke.command(
            [
                "kubectl",
                "-n",
                namespace,
                "get",
                "secret/" + OPERATOR_SECRET,
                "-o",
                "json",
            ]
        )
    )["data"]
    for name, key in (
        (WEBHOOK_SECRET, "webhook-key"),
        (WEBHOOK_SECRET + "-other", "other-webhook-key"),
    ):
        smoke.command(
            ["kubectl", "-n", CREDENTIAL_NAMESPACE, "create", "-f", "-"],
            {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {"name": name},
                "data": {"username": "Y2VydGlmaWNhdGlvbg==", "token": credentials[key]},
            },
        )
    setup = run_client(image, ready_addresses[0], phase="wait-setup")
    waiting_run = setup.get("waitingRunId")
    if not isinstance(waiting_run, str) or not waiting_run:
        raise CertificationFailure("wait: setup evidence is missing the run identifier")
    output.write_text(json.dumps(setup, indent=2))
    observe_wait(waiting_run)
    report = run_client(
        image,
        ready_addresses[0],
        phase="exercise",
        waiting_run=waiting_run,
    )
    report["completedScenarios"][:0] = [
        *setup["completedScenarios"],
        "real-workflow-source-wait-registration-observed-in-trigger-provider",
    ]
    output.write_text(json.dumps(report, indent=2))
    current = json.loads(
        smoke.command(
            [
                "kubectl",
                "-n",
                CREDENTIAL_NAMESPACE,
                "get",
                "secret/" + WEBHOOK_SECRET,
                "-o",
                "json",
            ]
        )
    )
    current["data"]["token"] = credentials["rotated-webhook-key"]
    smoke.command(
        ["kubectl", "-n", CREDENTIAL_NAMESPACE, "replace", "-f", "-"],
        current,
    )
    rotation = run_client(image, ready_addresses[0], phase="rotation")
    report["phase"] = "exercise-and-rotation"
    report["completedScenarios"].extend(rotation["completedScenarios"])
    output.write_text(json.dumps(report, indent=2))
    vendor_setup = run_client(image, ready_addresses[0], phase="vendor-setup")
    vendor_wait = vendor_setup.get("waitingRunId")
    if not isinstance(vendor_wait, str) or not vendor_wait:
        raise CertificationFailure(
            "vendor-push: setup evidence is missing the run identifier"
        )
    observe_wait(vendor_wait)
    publish_vendor_event()
    vendor = run_client(
        image,
        ready_addresses[0],
        phase="vendor-observe",
        waiting_run=vendor_wait,
    )
    report["completedScenarios"].extend(vendor["completedScenarios"])
    report["phase"] = "exercise-rotation-and-vendor"
    output.write_text(json.dumps(report, indent=2))
    publish_vendor_event()
    redelivery = run_client(image, ready_addresses[0], phase="vendor-redelivery")
    report["completedScenarios"].extend(redelivery["completedScenarios"])
    output.write_text(json.dumps(report, indent=2))
    smoke.command(
        [
            "kubectl",
            "-n",
            namespace,
            "scale",
            "deployment/custos-connector-service",
            "--replicas=2",
        ]
    )
    smoke.command(
        [
            "kubectl",
            "-n",
            namespace,
            "rollout",
            "status",
            "deployment/custos-connector-service",
            "--timeout=240s",
        ],
        timeout=270,
    )
    connector_pods = json.loads(
        smoke.command(
            [
                "kubectl",
                "-n",
                namespace,
                "get",
                "pods",
                "-l",
                "app.kubernetes.io/component=connector-service",
                "-o",
                "json",
            ]
        )
    )
    ready_connectors = [
        pod
        for pod in connector_pods["items"]
        if not pod["metadata"].get("deletionTimestamp")
        and any(
            condition.get("type") == "Ready" and condition.get("status") == "True"
            for condition in pod.get("status", {}).get("conditions", [])
        )
    ]
    if len(ready_connectors) != 2:
        raise CertificationFailure("pull: two real Connector replicas are not Ready")
    pull_setup = run_client(image, ready_addresses[0], phase="pull-setup")
    pull_wait = pull_setup.get("waitingRunId")
    if not isinstance(pull_wait, str) or not pull_wait:
        raise CertificationFailure("pull: setup evidence is missing the run identifier")
    observe_wait(pull_wait, connector=PULL_CONNECTOR, delivery_mode="pull")
    for phase in ("pull-resume", "pull-start", "pull-pause"):
        pull_report = run_client(
            image, ready_addresses[0], phase=phase, waiting_run=pull_wait
        )
        report["completedScenarios"].extend(pull_report["completedScenarios"])
        report["phase"] = "exercise-rotation-vendor-and-pull"
        output.write_text(json.dumps(report, indent=2))
    report["elapsedSeconds"] = round(time.monotonic() - started, 3)
    output.write_text(json.dumps(report, indent=2))
    print(
        "Real deployed receiver effect evidence captured; full matrix remains required."
    )


def main() -> None:
    """Separate runner orchestration, real-provider seeding and public-only application probing."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "phase",
        choices=("prepare", "seed", "run", *CLIENT_PHASES),
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.phase == "prepare":
        prepare()
    elif args.phase == "seed":
        asyncio.run(seed())
    elif args.phase in ("wait-setup", "vendor-setup"):
        setup_wait(vendor=args.phase == "vendor-setup")
    elif args.phase in ("exercise", "rotation"):
        exercise(rotating=args.phase == "rotation")
    elif args.phase in ("vendor-observe", "vendor-redelivery"):
        exercise_vendor(redelivery=args.phase == "vendor-redelivery")
    elif args.phase.startswith("pull-"):
        exercise_pull(args.phase)
    elif args.output is None:
        parser.error("--output is required for run")
    else:
        run(args.output)


if __name__ == "__main__":
    try:
        main()
    except CertificationFailure as error:
        print("CERTIFICATION_FAILED " + str(error), file=sys.stderr, flush=True)
        sys.exit(1)
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as error:
        print(
            f"CERTIFICATION_FAILED {type(error).__name__}; inspect owned workload status",
            file=sys.stderr,
            flush=True,
        )
        sys.exit(1)
