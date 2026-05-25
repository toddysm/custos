# SPIFFE/SPIRE Workload-Identity Cutover Plan

Component: auth-service (COMP-002)
Tracking issue: AS-IMPL-031 (#266)
Requirement: REQ-059 (workload identity for service-to-service calls)
Target milestone: M3
Status: planning (no SPIFFE code lands under this issue — only the
fail-fast stub described in **Step 0** below).

## 1. Why

In M1 the platform secures **internal** service-to-service calls
(WF→Connector, ARM→SPL, TS→WF, Gateway→every component, …) with a
short-lived signed JWT — the "call context" — minted by auth-service
and verified locally by the `custos-callctx` library shipped with
every other component. That model works, but it has two structural
weaknesses we want to retire by M3:

1. **A long-lived signing key.** The signer holds the call-context
   key for ~7 days (`CUSTOS_AUTH_CALL_CONTEXT_KEY_ROTATION` default)
   and republishes it through the JWKS endpoint. Every component
   trusts every JWT signed by that key for ~30 s after rotation. A
   key exfiltration is therefore a platform-wide compromise window.
2. **No workload attestation.** A compromised pod that can reach
   auth-service can request a call-context for any
   `(principal, workspace, caller_component)` tuple it wants. There
   is no platform-enforced binding between "the pod that asked" and
   "the `caller_component` the pod claimed".

The M3 target — SPIFFE/SPIRE — addresses both at once. SPIRE attests
each workload from a node-agent (Kubernetes PSAT + container image
hash) and issues a short-lived SVID (≤1 h, typically minutes). The
SVID's SPIFFE ID encodes the workload's identity in a way the pod
cannot forge:

```
spiffe://custos.example.com/ns/custos/sa/api-gateway
spiffe://custos.example.com/ns/custos/sa/catalog-service
spiffe://custos.example.com/ns/custos/sa/workflow-service
…
```

The platform call-context interface stays the same:

```python
callctx.verify(metadata) -> CallContext
```

Only the implementation under the hood swaps from "verify a JWT
against the JWKS" to "verify an SVID against the SPIRE trust
bundle". No component-side code change is required outside
`custos-callctx`.

## 2. Bridge Mode

A flag day is not realistic — the platform has eight services and
each component is owned by a different track. We bridge the cutover
with a runtime switch:

| Env var | Values | Default | Behaviour |
| --- | --- | --- | --- |
| `CUSTOS_AUTH_INTERNAL_IDENTITY_MODE` | `jwt` \| `spiffe` | `jwt` | Selects which verifier `custos-callctx` instantiates at boot. `jwt` retains the M1 path (signed JWT + JWKS). `spiffe` selects the SPIRE Workload-API verifier (M3, not implemented yet). |

The flag is **read at every component**, not only at auth-service —
the verifier lives inside `custos-callctx` and runs on the receiving
side of every RPC. Co-existence is therefore per-component, not
global; while AS-IMPL-031's M3 work is in flight, individual
components can be flipped from `jwt` to `spiffe` independently as
their SPIRE registration entries land.

### Step 0 — what this issue actually ships (AS-IMPL-031)

Per the issue scope, **no SPIFFE code lands in this issue**. What
lands is the negative space that lets M3 land cleanly:

1. **The env var is parsed.** auth-service's settings module
   recognises `CUSTOS_AUTH_INTERNAL_IDENTITY_MODE`, defaults to
   `jwt`, and accepts `jwt` as a no-op pass-through.
2. **`spiffe` refuses to boot.** Setting the flag to `spiffe` in
   M1/M2 raises `SettingsError` at process start with a message
   pointing the operator at this document. The auth-service refuses
   to come up rather than running half-wired. Same fail-fast
   pattern we use for `CALLCTX_VERIFIER_URL` in production
   (see `DevShimDisabledInProductionError` in
   `src/services/auth-service/src/custos_auth/middleware/callctx.py`).
3. **Unknown values refuse to boot.** Typos like `mtls`, `oauth2`,
   etc. raise `SettingsError` and list the legal values.
4. **The verifier library (`custos-callctx`) is _not_ touched in
   this issue.** Only auth-service knows about the flag for now;
   when M3 lands the verifier swap, `custos-callctx` will pick up
   the same env var and we will revise this plan with the wire
   contract for the SVID-mode `CallContext`.

This is deliberately conservative. The scope statement in #266 is
explicit that "no code lands in this issue beyond a stub that emits
not-implemented for spiffe mode" — and we read "stub" narrowly. We
do not want `spiffe` to be a half-working code path during the M2
window where the SPIRE infrastructure is still being installed.

## 3. Rollout Sequencing (M3)

Once SPIRE Server + Agent are deployed (see §4), components can
adopt `spiffe` mode in the following order. The order is dictated
by call-context **producers** vs **consumers**:

| Phase | Component(s) | Why this phase |
| --- | --- | --- |
| M3-A | SPIRE Server + Agent (Helm) | Trust domain `custos.example.com`; node-attestation via Kubernetes PSAT. No platform components flipped yet. |
| M3-B | Workload registration entries for every component | One `spiffe://custos.example.com/ns/custos/sa/<component>` per `ServiceAccount`, attested by container-image hash + namespace + service-account. |
| M3-C | `custos-callctx` verifier — dual-mode support | Library learns to instantiate the SPIRE Workload-API verifier when `CUSTOS_AUTH_INTERNAL_IDENTITY_MODE=spiffe`. JWT path unchanged. |
| M3-D | auth-service flipped to `spiffe` mode | auth-service is the only producer of call contexts. Once it mints SVIDs, every downstream component must either still accept JWTs (mixed mode) OR be flipped together. Mixed mode is the safer rollout. |
| M3-E | Per-component flip, leaf services first | catalog-service, observability-audit-service, … (services with no further outbound calls) — flip first. A leaf service flipping does not break its callers because the **caller** still produces what the **callee** asked for. |
| M3-F | Mid-tier services next | workflow-service, trigger-service, connector-service, activity-runtime-manager. |
| M3-G | API gateway last | Gateway is the entry-point for every external request; it is the largest producer of call contexts. Flipping it last means we only switch the producer side after every consumer is on `spiffe`. |
| M3-H | Decommission JWT signer | Remove `CUSTOS_AUTH_CALL_CONTEXT_KEY_REF`, `CUSTOS_AUTH_CALL_CONTEXT_KEY_ROTATION`, JWKS endpoint, and the JWT verifier branch from `custos-callctx`. Tracking issue at the time of decommission. |

Mixed-mode safety property during M3-E → M3-G: the verifier picks
the mode from its env var, so a receiving component on `spiffe`
mode will only accept SVIDs, never JWTs. The sequencing therefore
requires the producer (gateway / auth-service) to be aware of which
consumers expect which envelope. The cleanest implementation is for
the producer to **always mint both** during the transition window
and let the verifier accept whichever it understands — but this
requires the producer to know both the SPIFFE ID of the consumer
and the consumer's audience claim, which adds operator burden. The
M3 design will pick between (a) "always mint both" and (b)
"per-consumer mode registry" once the SPIRE infrastructure work
under M3-A/B nails down the registration-entry shape.

## 4. Operational Prerequisites (M3-A / M3-B)

1. **SPIRE Server deployment.** One per cluster; backed by Postgres
   for HA registration-entry storage. Planned for M3-A as a Helm
   subchart at `deploy/helm/charts/spire-server/`. Trust domain
   configured per environment (`custos.example.com` for connected,
   `custos.internal` for air-gapped — both already documented in
   `design/architecture/reference-deployment.md`).
2. **SPIRE Agent DaemonSet.** One per node; uses the Kubernetes PSAT
   node-attestor (the cluster's projected-service-account-token
   issuer). Workload-attestor uses `k8s` + `unix` for namespace,
   service-account, container image, and (where available)
   container image hash.
3. **Workload registration entries.** One per component
   `ServiceAccount`. Planned future work: a one-shot bootstrap job
   under the proposed path `src/jobs/bootstrap/spire-register/`
   would read the reference-deployment manifest and emit
   `spire-server entry create` commands. Selectors required:
   - `k8s:ns:custos`
   - `k8s:sa:<component>` (e.g. `k8s:sa:catalog-service`)
   - `k8s:container-image:<expected-digest>` (defence in depth)
4. **Trust-bundle distribution.** SPIRE Agent fetches the bundle
   over the Workload API and re-fetches on rotation; no manual
   distribution needed. We do NOT mount the trust bundle as a
   ConfigMap or Secret — that would defeat the rotation guarantees.
   The fallback path (offline / air-gapped clusters where the agent
   cannot reach a remote SPIRE Server) is also covered by SPIRE
   itself: the agent is configured to talk to the in-cluster
   server, and air-gap is already an in-cluster topology.
5. **Workload API socket mount.** Every component pod mounts the
   Workload API UDS (`/run/spire/sockets/agent.sock`) read-only.
   This is an M3 chart deliverable: the
   `deploy/helm/charts/<component>/` charts will either add the
   mount directly or consume a new shared library template (planned
   location:
   `deploy/helm/charts/custos-common/templates/_spire.tpl`) once
   that chart artifact is introduced.
6. **Health and readiness.** `custos-callctx` exposes a `ready`
   probe that turns red if the Workload API is unreachable for more
   than the SVID half-life. The probe is wired into the existing
   `/readyz` endpoint on every component.

## 5. Required Changes in `callctx.verify` (M3-C)

The interface stays:

```python
def verify(metadata: Mapping[str, str]) -> CallContext: ...
```

What changes inside the library:

1. **A new `SpiffeVerifier` class** alongside the existing
   `JwtVerifier`. The factory `get_verifier(settings)` returns one
   or the other based on `CUSTOS_AUTH_INTERNAL_IDENTITY_MODE`.
2. **Wire envelope.** In `spiffe` mode the call-context still
   travels in Dapr service-invocation metadata, but the bearer is
   the SVID (X.509 SVID in the TLS handshake, OR JWT-SVID in a
   metadata header — TBD by M3). Application-level fields the M1
   JWT carried as claims (`actingPrincipalId`, `workspaceId`,
   `callerComponent`, `permissions`) are NOT part of the SVID — the
   SVID only proves _which workload_ is calling. The application
   payload is carried separately and **signed by auth-service**
   with a short-lived JWT bound to the SVID's SPIFFE ID. This
   sub-design is the largest open item for M3 and will be resolved
   under a dedicated issue when M3-C starts.
3. **`CallContext` shape stays identical** so component-side code
   does not change. New fields, if any, are additive.
4. **Failure modes** stay `InvalidCallContextError` — the verifier
   library does not leak whether the failure was "SVID expired"
   vs. "trust-bundle mismatch" vs. "JWT signature invalid" to the
   caller. Detailed failure reason goes to the audit event
   `call-context.invalid` (already wired in M1).

## 6. Acceptance Criteria (when M3 lands and replaces this stub)

- [ ] `CUSTOS_AUTH_INTERNAL_IDENTITY_MODE=spiffe` boots auth-service
      without raising `SettingsError`.
- [ ] `custos-callctx` library reads the same env var and selects
      the SPIRE Workload-API verifier when set to `spiffe`.
- [ ] Every component's Helm chart mounts the Workload API socket
      and ships with a SPIRE registration entry by default.
- [ ] The JWT signer code path (`callctx_signer.py`) is retired or
      gated behind the legacy `jwt` mode for one milestone, then
      removed in M3-H.
- [ ] `docs/developers/auth-api.md` documents the new mode and the
      operator steps to flip a component.

## 7. Decisions Recorded

- **2026-05-28** — AS-IMPL-031 scope is planning + a fail-fast stub
  in auth-service only. `custos-callctx` is **not** touched in this
  issue; the verifier-library work is M3-C. Rationale: scoping the
  flag to auth-service avoids shipping a half-implemented
  `SpiffeVerifier` into every other component during M2.

## 8. References

- Requirement: REQ-059 (workload identity for service-to-service
  calls — `design/requirements/requirements.md`).
- Design doc: `design/components/auth-service/design.md` §
  "Internal vs External Auth — Trust Model" and "Migration path to
  SPIFFE/SPIRE".
- Sibling: M1 internal call-context shipping under AS-IMPL-017 →
  AS-IMPL-019 (`changes/2026-05-28-007-impl-callctx-permissions-claim.md`).
- SPIRE Kubernetes deployment guide: <https://spiffe.io/docs/latest/deploying/install-server/>
  (consult the version pinned in `deploy/helm/charts/spire-server/`
  at the time M3-A lands).
