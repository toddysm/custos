---
name: trigger-service-scope-additions
description: Add REQ-080 (internal workflow-to-workflow triggers) and REQ-081 (dual-purpose event delivery for workflow start and in-flight activity resume)
type: requirement
---

# Change: trigger-service-scope-additions

Date: 2026-05-16
Type: requirement
Sequence: 005
GitHub Issue: #15, #16
Status: open

## Summary

While starting the Trigger Service component design, two scope gaps surfaced:

1. The architecture had no requirement covering workflows triggering other workflows internally.
2. The architecture treated the trigger pipeline as workflow-start-only, but several real scenarios require the same ingestion pipeline (push or polled) to deliver events that resume in-flight activities or sub-orchestrations (man-in-the-middle approvals, GitHub PR state changes, external signing callbacks).

REQ-080 and REQ-081 close both gaps without expanding Trigger Service into orchestration control. Workflow Service remains authoritative for in-flight execution; Trigger Service handles ingestion and dispatch only.

## Before

```
| REQ-079 | Trigger ingestion must support both push (event/webhook) and pull (polling) modes ... | High | Open | 2026-05-14 |
```

## After

```
| REQ-079 | Trigger ingestion must support both push (event/webhook) and pull (polling) modes ... | High | Open | 2026-05-14 |
| REQ-080 | Internal workflow-to-workflow triggering ... first-class trigger type (e.g. `type: workflow.completed`) | High | Open | 2026-05-16 |
| REQ-081 | Pollers and push receivers must be able to deliver events either as workflow-start signals or as resume signals to in-flight activities ... | High | Open | 2026-05-16 |
```

## Impact

- Trigger Service component design (this session) must include an Internal Event Receiver, a Classifier splitting workflow-start vs. step-resume events, a Resume Subscription store, and a Resume Matcher.
- Workflow Service must expose two new internal APIs: register-resume-subscription and emit-internal-workflow-event. These are listed as cross-component TODOs for the Workflow Service design session.
- No change to Dapr Workflow primitives: resume events are still delivered into Dapr via `raise_event`; Trigger Service is the correlator that maps inbound external events to the right `(runId, stepId)`.

## Related Requirements

REQ-004, REQ-005, REQ-006, REQ-079, REQ-080, REQ-081
