# Change: impl-spiffe-cutover-plan

Date: 2026-05-28
Type: component-design
Component: auth-service
Sequence: 008
GitHub Issue: #266
Status: open

## Summary

Delivers **AS-IMPL-031 — SPIFFE/SPIRE workload identity cutover
plan + `CUSTOS_AUTH_INTERNAL_IDENTITY_MODE` (M3, REQ-059)**.

Per the issue scope, **no SPIFFE verifier code lands in this
change**. What lands is:

1. **The planning doc.** New file
   `design/components/auth-service/spiffe-cutover-plan.md` covers
   the bridge-mode design, per-component rollout sequencing,
   SPIRE Server + Agent operational prerequisites, the required
   verifier-library changes in `custos-callctx` (deferred to M3-C),
   and the decommission path for the M1 JWT signer.
2. **A fail-fast settings stub** in auth-service so the M3 cutover
   has a stable env-var surface to land on without surprising
   operators in M1/M2:

   * `CUSTOS_AUTH_INTERNAL_IDENTITY_MODE` is now parsed by
     `custos_auth.settings`. Empty / unset defaults to `jwt`
     (the only fully wired mode).
   * `jwt` passes through (no-op in M1).
   * `spiffe` raises `SettingsError` at process start with a
     message pointing the operator at the cutover plan. This is
     the same fail-fast pattern we use for
     `CALLCTX_VERIFIER_URL` in production
     (see `DevShimDisabledInProductionError` in
     `src/services/auth-service/src/custos_auth/middleware/callctx.py`).
   * Any other value raises `SettingsError` listing the legal
     values.

This change does NOT touch `src/libs/custos-callctx`. Per the
decision recorded in the plan doc (§7), scoping the flag to
auth-service avoids shipping a half-implemented `SpiffeVerifier`
into every other component during M2. The library-side cutover
is M3-C and will land under a future issue.

## Before

* `CUSTOS_AUTH_INTERNAL_IDENTITY_MODE` was mentioned in the
  auth-service design doc as an anticipated M2/M3 feature flag
  but was not parsed anywhere. Setting it had no effect.
* The migration path to SPIFFE/SPIRE was described in two short
  paragraphs in the design doc; there was no operational
  cutover plan, no rollout sequencing, no enumeration of the
  SPIRE deployment prerequisites, and no statement of what the
  `custos-callctx` library would need to change.
* The auth-service `Open TODOs` and `todos.md` still listed
  "SPIFFE/SPIRE cutover plan (M2/M3)" as an unresolved item.

## After

### Settings module (`src/services/auth-service/src/custos_auth/settings.py`)

* New constants: `ENV_INTERNAL_IDENTITY_MODE`,
  `INTERNAL_IDENTITY_MODE_JWT`, `INTERNAL_IDENTITY_MODE_SPIFFE`,
  `DEFAULT_INTERNAL_IDENTITY_MODE`,
  `_INTERNAL_IDENTITY_MODES` (private frozenset).
* New field on the `Settings` frozen dataclass:
  `internal_identity_mode: str`.
* New parser `_parse_internal_identity_mode(raw: str) -> str`:
  case-insensitive match against the closed `{jwt, spiffe}` set;
  empty falls back to `jwt`; `spiffe` raises
  `SettingsError("...AS-IMPL-031 (#266); the cutover plan lives
  at design/components/auth-service/spiffe-cutover-plan.md...")`;
  unknown values raise `SettingsError` listing the legal modes.
* `load_settings()` wires `internal_identity_mode` from
  `CUSTOS_AUTH_INTERNAL_IDENTITY_MODE`.
* The four public names are exported in `__all__`.

### Settings tests (`src/services/auth-service/tests/test_settings.py`)

Seven new tests under the AS-IMPL-031 section:

* `test_internal_identity_mode_defaults_to_jwt`
* `test_internal_identity_mode_accepts_jwt_case_insensitively`
* `test_internal_identity_mode_empty_string_falls_back_to_jwt`
* `test_internal_identity_mode_spiffe_refuses_to_boot` — asserts
  the error message references `AS-IMPL-031` and the doc filename.
* `test_internal_identity_mode_spiffe_case_insensitive_still_refuses`
* `test_internal_identity_mode_rejects_unknown_value` — asserts
  the error message lists both legal values.

### Planning doc (`design/components/auth-service/spiffe-cutover-plan.md`)

NEW. Sections:

1. Why — the two structural weaknesses of the M1 JWT call-context
   (long-lived signing key, no workload attestation) that
   SPIFFE/SPIRE addresses.
2. Bridge mode — `CUSTOS_AUTH_INTERNAL_IDENTITY_MODE=jwt|spiffe`,
   read by every component's verifier, with **Step 0** describing
   exactly what AS-IMPL-031 ships (auth-service stub, no
   `custos-callctx` changes).
3. Rollout sequencing — M3-A (SPIRE Server + Agent) through M3-H
   (decommission the JWT signer), with explicit reasoning for the
   producer-vs-consumer ordering (leaves first; gateway last).
4. Operational prerequisites — SPIRE Server Helm subchart, Agent
   DaemonSet, workload registration entries, Workload-API socket
   mount, trust-bundle distribution (handled by SPIRE itself, not
   ConfigMap), health/readiness wiring.
5. Required changes in `callctx.verify` — new `SpiffeVerifier`
   class alongside `JwtVerifier`, factory selection by env var,
   `CallContext` shape preserved (additive only), failure modes
   unchanged at the library boundary.
6. Acceptance criteria for M3.
7. Decisions recorded — including the 2026-05-28 decision to
   scope this issue to auth-service only.
8. References — REQ-059, design doc cross-links, sibling change
   record for the M1 call-context permissions claim.

### Design doc (`design/components/auth-service/design.md`)

* "Migration path to SPIFFE/SPIRE" subsection now links the new
  plan doc and notes that AS-IMPL-031 landed the env-var stub.
* Configuration table row for `CUSTOS_AUTH_INTERNAL_IDENTITY_MODE`
  now links the plan doc and notes the fail-fast behaviour.
* "Open TODOs" item for the cutover plan marked `[x]` with a
  pointer to the plan doc.

### Tracking (`design/components/auth-service/todos.md`)

* `Last Updated` bumped to 2026-05-28.
* The "SPIFFE/SPIRE cutover plan (M2/M3)" item in the **Open**
  section is now marked **Resolved** with the standard pattern
  used for prior shipped items.

## Why this is the right scope

The issue scope was deliberately narrow — _"No code lands in this
issue beyond a stub that emits not-implemented for spiffe mode"_.
The temptation was to use AS-IMPL-031 to also land the
`custos-callctx` verifier-mode plumbing so M3 only needs to fill
in the `SpiffeVerifier` class. We rejected that for two reasons:

1. **Honest defaults.** A flag whose `spiffe` value half-works
   inside the verifier library is worse than a flag that
   crisply refuses to boot. M2 deployments will see the env var
   in their config templates; we want any operator who flips it
   prematurely to fail at start, not run with a degraded
   verifier.
2. **M3-C is a real design problem.** The wire envelope for
   SPIFFE mode (X.509 SVID vs JWT-SVID, how the call-context
   payload travels alongside the SVID, how the SVID's SPIFFE ID
   maps to `callerComponent`) is not settled. That design needs
   its own issue with its own review. The plan doc records this
   as the largest M3 open item rather than pre-empting it here.

## Acceptance Criteria (this issue, AS-IMPL-031)

- [x] Planning doc landed and linked from the design doc.
- [x] `CUSTOS_AUTH_INTERNAL_IDENTITY_MODE` is parsed; defaults to
      `jwt`; case-insensitive.
- [x] `spiffe` value raises `SettingsError` at boot with a
      pointer to the plan doc.
- [x] Unknown values raise `SettingsError` listing the legal
      modes.
- [x] Tests cover all five behaviours (default, jwt pass-through,
      empty, spiffe-refuses, unknown-refuses).
- [x] Open-TODOs items in `design.md` and `todos.md` resolved
      with cross-references.

## References

- Requirement: REQ-059 (workload identity).
- Design: `design/components/auth-service/spiffe-cutover-plan.md`
  (this change).
- Sibling: `changes/2026-05-28-007-impl-callctx-permissions-claim.md`
  (the M1 call-context shape AS-IMPL-031 plans to retire in M3-H).
- Issue: #266.
