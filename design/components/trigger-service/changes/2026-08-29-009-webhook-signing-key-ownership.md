# Change: webhook-signing-key-ownership

Date: 2026-08-29
Type: component-design
Component: trigger-service
Sequence: 009
GitHub Issue: #23
Status: closed

## Summary

Resolves TODO-006. The generic webhook receiver needs HMAC/token verification
(REQ-006). This locks **ownership of the signing material**: it is owned by the
**Connector Service, per connector instance** — not by the Trigger Service per
subscription. The Trigger Service **delegates** verification to the Connector
Service, so raw signing secrets never enter the Trigger Service.

## Before

The design assumed webhook verification happened in the Trigger Service's Generic
Webhook Receiver "per connector-instance config", and a dependency row credited
the **Auth Service** with "webhook receiver HMAC/token verification". Neither
stated **who owns the secret**, leaving two candidates open: per-subscription in
the Trigger Service, or per-instance in the Connector Service (TODO-006).

## After

### Decision

Signing/verification material is owned by the **Connector Service, per
`ConnectorInstance`**, as part of the instance credential model (resolved via the
Secret Bridge / an identity resolver such as `x-dapr-secret`). The Generic Webhook
Receiver delegates verification to the Connector Service and only ever sees the
verified outcome plus the normalized event — never the secret.

### Why not per-subscription in the Trigger Service

1. **Instance-scoped URLs make per-subscription keys impossible.** One webhook URL
   is shared by every subscription on a `ConnectorInstance` (INCON-025), so the
   sender signs with a single instance secret — there is no per-subscription
   secret for it to use.
2. **The Connector Service already owns per-instance credentials** and the
   `listen(push)` webhook wiring (Listen Manager). Duplicating a signing-secret
   store in the Trigger Service would fork secret management and break the rule
   that plaintext credentials never traverse service APIs (plugins get opaque
   handles).
3. **Verification is vendor-specific** (GitHub `X-Hub-Signature-256`, Slack signing
   secret, generic HMAC), so it belongs to the connector plugin, not the
   source-agnostic Trigger pipeline.

### Flow

1. Gateway forwards raw body + headers to the Generic Webhook Receiver (no gateway
   verification).
2. The receiver asks the Connector Service to verify for `connectorInstanceId`;
   the Connector Service resolves the instance secret via the Secret Bridge and
   runs the plugin verifier.
3. On success → normalize + demux to matching subscriptions → dedup → dispatch.
4. On failure → `401` + audit `trigger.webhook.rejected`
   (`signature_invalid` / `signature_missing`); no dispatch.

## Impact

- Trigger design: added § Webhook Signature Verification; the Generic Webhook
  Receiver row and the webhook-ingest route notes now say verification is
  delegated to the Connector Service; the **Connector Service** dependency row
  gains "owns per-instance webhook signing material and verifies inbound webhook
  signatures", and the **Auth Service** row is trimmed to manual-trigger RBAC.
- Connector design: a reciprocal note records the Connector Service's ownership of
  webhook signing material and the verification seam it exposes.
- Implementation follow-up: the Connector Service verification seam and the
  receiver's delegation land with the M2 Generic Webhook Receiver (#988).

## References

- `design/components/trigger-service/design.md` § Webhook Signature Verification
- `design/components/connector-service/design.md` § Identity and Credential Model
- REQ-006 (registry/generic webhook triggers); INCON-025 (instance-scoped webhook URL)
- Follow-up implementation: #988 (M2 receivers)
