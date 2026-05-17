# Change: incon-011-014-013-cursor-pubsub-taxonomy

Date: 2026-05-17
Type: component-design
Component: trigger-service
Sequence: 001
GitHub Issues: #36, #39, #38
Status: open

## Summary

Bundle three related fixes to the Trigger Service component design:

- **INCON-011 (#36)** — Pull cursors are owned by the **Connector Service** per `ConnectorInstance`, not by the Trigger Service per `Subscription`. Removes the `Cursor` entity and `Cursor Store` from the Trigger Service data model and internal structure; sequence diagrams now name the cursor store as a Connector Service participant.
- **INCON-014 (#39)** — `PublishWorkflowEvent` is **not** an internal RPC. It is a Dapr Pub/Sub publication to `custos.workflow.events`, consumed asynchronously by the Internal Event Receiver. Removed from the Internal RPC table; new "Dapr Pub/Sub subscriptions" section documents the topic, publisher, subscriber, envelope, and delivery semantics.
- **INCON-013 (#38)** — TODO-001 (Trigger Service taxonomy) and TODO-009 (ARM activity lifecycle taxonomy) were unlinked. Expanded TODO-001's scope and cross-referenced from ARM TODO-009 so a single dot-namespaced `kind` namespace covers connector event kinds and ARM-emitted activity/step lifecycle audit events.

## Before

- ER diagram included `Cursor { subscriptionId, position, updatedAt } || Subscription : checkpoints`.
- Internal Structure graph included a `Cursors[(Cursor Store)]` node fed by Pollers.
- Polling and resume sequence diagrams used a generic `participant Cursor as Cursor Store` (ambiguous owner).
- Boundaries section said the Trigger Service owns "Poller cursors and schedule state, persisted via MetadataStoreProvider."
- Internal RPC table included `PublishWorkflowEvent(event) | WF → Bus`.
- Dependencies row listed `Persistence for Subscription, Schedule, Cursor, DedupKey, ResumeSubscription.`
- Trigger Service TODO-001 was scoped to trigger kinds only; ARM TODO-009 was unlinked.

## After

- `Cursor` entity removed from ER diagram; an explanatory note below the diagram points at Connector Service § Cursor Ownership.
- Internal Structure graph: `Cursor Store` node removed; the `Pollers -.driven by.-> ConnSvc2[Connector Service listen-pull + cursor]` edge now makes Connector Service ownership explicit.
- Polling and resume sequence diagrams: `participant Cursor as Cursor Store (Connector Service)`.
- Boundaries section explicitly lists "Connector pull cursors" under **Does NOT own**, with a forward reference to Connector Service § Cursor Ownership.
- Internal RPC table no longer mentions `PublishWorkflowEvent`.
- New § "Dapr Pub/Sub subscriptions" documents the `custos.workflow.events` topic (publisher: Workflow Service; subscriber: Trigger Service Internal Event Receiver; at-least-once delivery; envelope subset).
- Dependencies row reads `Persistence for Subscription, Schedule, DedupKey, ResumeSubscription. (Pull cursors live with the Connector Service, not here.)`
- `todos.md` TODO-001 rewritten as a unified taxonomy work item with explicit cross-link to ARM TODO-009; ARM TODO-009 cross-links back.

## Impact

- Single source of truth for pull cursor position: Connector Service runs one pull loop per `ConnectorInstance`, fans normalized events out to N subscriptions. Eliminates N×M upstream API load and the cursor-write race that an unspecified ownership boundary would have enabled.
- Workflow Service implementers will publish to a Dapr Pub/Sub topic, not call a Trigger Service RPC. Operators will provision the `custos.workflow.events` Dapr component at deployment time.
- The unified taxonomy work item ensures `workflow.completed`, `step.completed`, `activity.failed` etc. carry one canonical `kind` from emission through trigger matching, audit storage, and consumer dashboards.

## Related Requirements

- `design/components/connector-service/design.md` § Cursor Ownership (authoritative for cursor model — added in the same PR)
- `design/components/trigger-service/design.md` § Internal Structure, § Data Models, § Public Interface (this change)
- REQ-079 (multi-source pull/push), REQ-080 (internal triggers), REQ-081 (dual delivery)
- Issues: #36 (INCON-011), #39 (INCON-014), #38 (INCON-013); related: #18 (TS-TODO-001)
