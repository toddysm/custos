# Change: bundle-j-cursor-rewound

Date: 2026-05-18
Type: component-design
Component: trigger-service
Sequence: 005
GitHub Issue: #103
Status: open

## Summary

Added TODO-007 (deferred to M2+) covering a future selective `DedupKey` clear admin API on Trigger Service. This is the API that Connector Service's admin-rewind procedure historically referenced as if it existed in v1; it does not. Documenting it as an explicit TODO makes the dependency visible and keeps the Connector Service rewind procedure honest about the v1 workaround (wait for `DedupKey` TTL or rewind past the dedup window).

## Before

- Trigger Service design did not document a selective `DedupKey` clear admin API and made no statement about whether one was planned, even though Connector Service's admin rewind procedure described operators "clearing matching `DedupKey` entries" — implying such an API existed.

## After

- New TODO-007: "Selective `DedupKey` clear admin API (e.g. `POST /v1/workspaces/{ws}/triggers/dedup:clear` with selectors over `subscriptionId`, `connectorInstanceId`, `eventId`, time window) to support operator-initiated rewind replay without waiting for `DedupKey` TTL. Deferred to M2+ — in v1, the rewind playbook documents waiting for the dedup window TTL or rewinding past the window."

## Impact

- Operator runbooks should note that the v1 rewind workflow does not bypass the dedup window.
- No API or schema changes in v1.

## Files changed

- `design/components/trigger-service/design.md` v5 → v6 (Open TODOs § TODO-007; Change History)

## Related Change Records

- Connector Service: `2026-05-18-014-bundle-j-cursor-rewound.md` (companion — rewrites the rewind procedure to remove the dedup-clear assumption and reference this TODO).
