# Change: resume-subscription-idempotency-and-todo-004-close

Date: 2026-05-17
Type: component-design
Component: trigger-service
Sequence: 002
GitHub Issue: #40
Status: open

## Summary

Companion edit to the initial Workflow Service component design (`design/components/workflow-service/design.md`, INCON-015 / #40). Three changes to `design/components/trigger-service/design.md`:

1. **Internal RPC table** — added idempotency semantics to the `RegisterResumeSubscription` and `CancelResumeSubscription` rows so the TS-side contract matches the WF-side replay protocol now specified in the Workflow Service design § Resume Subscription Replay Protocol.
2. **TODO-004 closed** — the TODO "Specify resume-subscription registration as a Workflow Service responsibility in that component's design (cross-component) — REQ-081" is resolved by the new Workflow Service design owning the registration lifecycle. Moved to a new Closed TODOs section.
3. **Change History** — new row referencing this resolution and #40.

## Before

TS Internal RPC table rows for the resume-subscription methods:

```
| `RegisterResumeSubscription(runId, stepId, eventKey, selector, ttl)` | WF → TS | Register a one-shot resume wait. |
| `CancelResumeSubscription(runId, stepId, eventKey)` | WF → TS | Cancel a wait (timeout, run cancelled). |
```

No idempotency semantics specified on either row. The Trigger Service design carried TODO-004 open because there was no Workflow Service design to own the registration lifecycle.

## After

```
| `RegisterResumeSubscription(runId, stepId, eventKey, selector, ttl)` | WF → TS | Register a one-shot resume wait. Idempotent on `(runId, stepId, eventKey)` — re-registration returns the existing `subscriptionId` rather than creating a duplicate. On divergent `selector` between original and replay, original wins and TS emits a `resume.subscription.divergent` audit event. After `expiresAt` TS GCs the subscription; a re-registration after TTL expiry is treated as a fresh registration. See Workflow Service design § Resume Subscription Replay Protocol for the full WF-side protocol. |
| `CancelResumeSubscription(runId, stepId, eventKey)` | WF → TS | Cancel a wait (timeout, run cancelled). Idempotent — cancelling an unknown or already-expired key is a no-op. |
```

TODO-004 moved to a new "Closed TODOs" section with a cross-link to the Workflow Service design sections that resolve it.

TS design version bumped 2 → 3.

## Impact

- The contract between WF and TS for resume-subscription registration is now fully specified end-to-end. WF owns the replay-driven re-registration logic (`ResumeSubscriptionMirror` persisted, idempotent re-register on replay); TS owns idempotent-on-`(runId, stepId, eventKey)` semantics for accepting those calls.
- Closes the cross-component coordination loop that was deliberately left open when the Trigger Service design landed in #17 — at that time no Workflow Service design existed, so the registration lifecycle could not be specified.
- TS-TODO-004 closes; remaining TS open TODOs unchanged: TODO-001 (event taxonomy, joint with ARM and WF), TODO-002 (selector language), TODO-003 (scheduler leader election), TODO-005 (dead-letter / replay UX), TODO-006 (webhook signing key ownership).

## Related Requirements

- REQ-081 (step resume via external event) — now specified on both ends.
- `design/components/workflow-service/design.md` § Step Resume on External Event and § Resume Subscription Replay Protocol (the authoritative spec for the WF side).
- Issues: #40 (this change), #17 (original Trigger Service design that opened TODO-004).
