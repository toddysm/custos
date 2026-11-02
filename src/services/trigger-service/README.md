# trigger-service

Custos Trigger Service (COMP-004). The platform's event ingestion and dispatch
broker: it receives signals (manual, scheduled, webhook, vendor-push, pull,
internal), normalizes them into a `NormalizedEvent` envelope, classifies them
as workflow-start or step-resume, matches them to `Subscription` rows via CEL
selectors, deduplicates them, and dispatches to the Workflow Service
(`StartRun` / `RaiseExternalEvent`).

Design: [`design/components/trigger-service/design.md`](../../../design/components/trigger-service/design.md).
Implementation plan: [`design/components/trigger-service/implementation-plan.md`](../../../design/components/trigger-service/implementation-plan.md).

## Status

**Milestone 1 implemented; Milestone 2 in progress.**
The `TS-IMPL-000-TRIGGER-SERVICE` milestone
([#652](https://github.com/toddysm/custos/issues/652)) is complete; all 21 child
tasks (TS-IMPL-001 … TS-IMPL-021) are merged and the tracking issue is closed.
The M1 service includes typed settings + the locked `trigger.*`
error taxonomy, the `custos-cel` event-binding root + canonical event-taxonomy
registry, the `NormalizedEvent` envelope + subscription/domain models, SPL store
adapters over `MetadataStoreProvider` + `providers.py` wiring, reserve-before-
dispatch dedup / idempotency, the Event Normalizer, the CEL selector evaluator,
the Classifier + Start Matcher + Resume Matcher, the `WorkflowServiceClient`
Dapr Service-Invocation adapter (`StartRun` / `RaiseExternalEvent`), the
Dispatcher, the Manual Receiver + REST CRUD (REQ-004), the Register / Cancel
ResumeSubscription RPCs (REQ-081), the Internal Event Receiver (REQ-080 /
REQ-081) + Dapr subscription + app wiring, and OTel observability + audit
events. Backed by a unit + integration suite at the ≥90 % coverage gate
(TS-IMPL-020) and developer docs at
[`docs/developers/trigger-api.md`](../../../docs/developers/trigger-api.md)
(TS-IMPL-021).

Tracking issue: [#652](https://github.com/toddysm/custos/issues/652)
(`TS-IMPL-000`).

Receiver configuration and scoped external-wait contracts are added by
[#1006](https://github.com/toddysm/custos/issues/1006). Receiver workers and
deployed durable wait execution remain part of
[#988](https://github.com/toddysm/custos/issues/988).
New receiver writes are default-off until every writer has been upgraded:
`TRIGGER_RECEIVER_CONFIGURATION_ENABLED=true` is an explicit rollout acknowledgment.
Scoped resume operations additionally require a configured Workflow caller
authenticator. Production verifies signed user contexts using
`TRIGGER_AUTHZ_ENDPOINT` plus its `/.well-known/jwks.json` endpoint. Mesh routes
independently require `APP_API_TOKEN`; resume routes also check the Workflow
Dapr app ID and namespace (`TRIGGER_WORKFLOW_APP_ID` and
`TRIGGER_WORKFLOW_NAMESPACE`), independently of legacy endpoint addresses.
Dapr >=1.18.6 with mutual TLS is required; the deployment pin is stable 1.18.9.
Missing or forged mesh credentials fail
before tenant state access. Deployment secret, access-control and network-policy
activation remain part of [#1017](https://github.com/toddysm/custos/issues/1017).

Dead-letter administration exposes workspace-scoped list, inspect, replay,
discard and bounded bulk replay under `/v1/workspaces/{workspaceId}/triggers/deadletter`.
Every route requires `trigger:admin` and enabled durable dispatch. Storage
commits lifecycle audit/outbox events atomically with capture, replay completion,
discard and expiration. Metadata revision 12 persists bulk selection/progress;
the same operator/key resumes that original command rather than selecting new
pending entries. Current administration requires revision 13, which retains
single-entry request/generation bindings and freezes broker target selections
before effects. A retry cannot mutate a recreated entry or rematch newly added
waits; pending targets remain recoverable after their registrations disappear.
Version-2 resumes require Workflow's 204 consumer acknowledgement;
202 submission alone never marks replay successful. The paged retention collaborator preserves live replay
claims; the default-off lifecycle drivers below run bounded periodic sweeps.
See the [administration contract](../../../docs/developers/trigger-api.md#dead-letter-administration)
for limits, error codes, original-target replay and timeout recovery.

## Receiver worker lifecycle

`TRIGGER_RECEIVER_WORKERS_ENABLED=true` activates lifespan-owned scheduled
publication, pending-delivery reconciliation, Connector demand hints and audited
retention. It is separate from configuration authoring, source consumption and
durable dispatch; those prerequisites must already be enabled. Set a stable
`TRIGGER_DEPLOYMENT_ID` shared by replicas of the same scheduler deployment.
Receiver discovery requires metadata revision 13 and includes retained recovery
state even when the original subscription is gone. It is an internal storage
capability, not the unbounded Auth workspace-list API or an authorization bypass.

Each driver holds at most 50 workspace IDs and one workspace's continuation.
Scheduled due pages and within-selection delivery continuations are preserved
between passes. A null cursor ends a scan pass, not outstanding work: subsequent
passes revisit pending targets. Retention uses the existing audited operations,
protects pending selections and does not erase receipt identities or schedule
replay proofs. Runtime audit persists safe identifiers/counts rather than raw
events, credentials or exception diagnostics; metric labels use fixed operation
and outcome categories, not workspace IDs.

`TRIGGER_RECEIVER_DRAIN_SECONDS` defaults to 330 (valid range 1..3600).
Shutdown first stops admission, drains current passes, then cancels and joins
overdue work before closing borrowed transports or factory-owned pools.
Readiness becomes false during drain or when a worker fails. Disabling workers
does not delete persisted state or disable existing manual/internal routes.
Deployment activation and deployed resilience/capacity certification remain
separate gates within [#988](https://github.com/toddysm/custos/issues/988).

## Layout

```
src/custos_trigger/
  __init__.py    # package marker + version (create_app lands in TS-IMPL-003)
  __main__.py    # `python -m custos_trigger` / console-script entry point
  py.typed       # PEP 561 typing marker
tests/
  test_scaffold.py  # import + version smoke test
```

## Development

From this directory (`src/services/trigger-service`):

```bash
pip install -e ../../libs/storage-provider-layer[dev]
pip install -e ../../libs/custos-postgres[dev]
pip install -e ../../libs/custos-cel[dev]
pip install -e ../../libs/custos-callctx[dev]
pip install -e ../workflow-service
pip install -e ../api-gateway
pip install -e ".[dev]"

ruff format . && ruff check . && mypy src tests
pytest -q --cov=custos_trigger --cov-report=term-missing --cov-fail-under=90
```

The Workflow package is a development dependency for cross-service contract
tests and their type checking, not a Trigger runtime dependency. Database
integration tests also require `CUSTOS_PG_DSN` or local Docker.
