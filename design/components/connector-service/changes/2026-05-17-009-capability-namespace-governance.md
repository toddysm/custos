# Change: capability-namespace-governance

Date: 2026-05-17
Type: component-design
Component: connector-service
Sequence: 009
GitHub Issue: #61
Status: open

## Summary

Locks the governance model and compatibility policy for connector capability tokens. Introduces a two-tier namespace (curated Tier 1 core prefixes + open `x-<vendor>.<verb>` Tier 2 extensions), a strict-superset semver compatibility rule within a major version, a deprecation flow, and a new architecture-level curated registry at `design/architecture/capabilities.md`. Closes the corresponding open item in `connector-service/todos.md`.

## Before

`design.md` § Capabilities and Events described capability tokens by example (`oci.pull`, `s3.read`, …) but never said who decides what tokens are valid, whether third parties may invent their own, or what guarantees a connector type makes about its capabilities across versions. A connector type could silently drop `oci.push` between `2.3.0` and `2.3.1` and break every activity bound to it. Open Questions listed "Capability namespace governance model (strict curated list vs extensible custom prefixes)". `todos.md` carried the matching open item.

This left plugin authors with no clear contract for what to declare and no compatibility guarantees activities could rely on, and made the activity-side "required capabilities" declaration nearly meaningless across version bumps.

## After

§ Capabilities and Events gains a new sub-section "Namespace governance" with sub-sections for the two-tier namespace, plugin-registration validation, compatibility policy, and deprecation flow.

A new architecture-level file `design/architecture/capabilities.md` is created as the **single source of truth for Tier 1 tokens**. It lists the seven reserved prefixes (`oci.*`, `s3.*`, `blob.*`, `http.*`, `sql.*`, `event.*`, `notification.*`) and the initial token set under each, seeded from the connector type examples already in the codebase.

The capability list in manifests gains a richer entry shape: either a plain string (live capability) or an object `{ name, deprecated, since, removeIn }` (deprecated capability). The Binder treats both equivalently for matching; the difference is whether `connector.capability.deprecated` fires on each bind.

Two new audit events: `connector.registration.rejected` (with failure codes `unknown-core-capability`, `invalid-capability-syntax`, `capability-regression`) and `connector.capability.deprecated`.

`todos.md`: closes the capability governance item. Open Questions: removes the matching bullet.

## Key Decisions Locked This Session

1. **Two tiers, not one.** A strict curated registry would block third-party innovation; a fully open namespace would make activity "required capabilities" meaningless. The Tier 1 / Tier 2 split lets the platform guarantee semantics for core verbs while letting vendors invent freely under `x-*` with the trade-off (vendor coupling) made explicit.
2. **`x-<vendor>.<verb>` syntax for extensions, no central approval.** Lower friction than a registration queue; the coupling is visible in activity manifests, in the catalog UI, and in the bind audit, so reviewers can see it without platform-side gating.
3. **Strict-superset rule within a major version.** Dropping a capability mid-major silently breaks activity bindings — there is no way for an activity author to defend against it. Major bump is the existing signal for "re-validate bindings"; honoring that signal makes capability requirements meaningful.
4. **Registration-time enforcement, not bind-time.** A `capability-regression` rejection at registration time fails fast for the connector author who can fix it. Catching it at bind time would only surface the failure on workflow runs, far from the cause.
5. **Deprecation flow with `since` and `removeIn`.** Lets a connector author signal "this capability is going away in v3" without breaking v2 consumers, and produces visible audit signal for operators to track migration progress. Removal is hard-gated to the next major bump.
6. **Capability entries may be string OR object.** Avoids forcing all manifests to switch to the verbose form; only deprecated tokens carry metadata. Schema accepts both at the same array position.
7. **Registry lives under `design/architecture/`, not the component.** Capability tokens are an architectural cross-component contract (activities depend on them too, not just Connector Service). Putting the file under `architecture/` reflects that and makes additions visible in cross-component review.
8. **Vendor-shaped tokens like `slack.post` stay Tier 1 in v1.** They already appear in existing connector examples; reclassifying them to `x-slack.post` would be churn. A future tightening pass may move them, but v1 keeps the existing examples valid.

## Impact

- Plugin authors have a concrete contract for what tokens to declare and what changes between versions are safe.
- Activity authors can rely on "required capabilities" being honored across the lifetime of a connector type's major version.
- Catalog/UI can surface `x-*` requirements as vendor-coupling warnings.
- Adding a new core verb is now a discoverable process (PR against `capabilities.md`).
- Connector Service `todos.md` reduced from 4 open items to 3. The 3 remaining (fallback tag naming, lease expiry operator UX, connector test harness) determine the `Defined → Designed` flip; the test-harness item is implementation-track and does not block the flip.

## Out of Scope (Deferred)

- Programmatic API to read the Tier 1 registry from the Connector Service (operators inspect it via the repo for now).
- Automated cross-check that activity manifest `required capabilities` are all Tier 1 or declared `x-*` syntactically valid — manual review at activity registration in v1.
- Reclassifying vendor-shaped Tier 1 tokens (`slack.post`, `teams.post`, `email.send`) into `x-<vendor>.*` — deferred to avoid churning existing connector examples.
- Per-token semver: today the unit of versioning is the connector type version, not individual capabilities. Per-capability deprecation works via the `since`/`removeIn` annotations on the connector type's lifecycle.
- Cross-platform capability federation (sharing the registry with other Custos deployments via OCI artifact) — M3+.

## Related Requirements

- ADR-011 (capability-based binding model) — fully realized: a "required capability" now has a binding governance contract.
- REQ-037 (secrets management) — unaffected; capability governance is data-plane verbs, not credentials.
- INCON-012 (sink connectors omitting events) — preserved; sink connector example in design.md is unchanged and still valid.
