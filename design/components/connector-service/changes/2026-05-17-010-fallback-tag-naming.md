# Change: fallback-tag-naming

Date: 2026-05-17
Type: component-design
Component: connector-service
Sequence: 010
GitHub Issue: #62
Status: open

## Summary

Locks the v1 fallback-tag scheme used to discover connector manifests when the OCI Referrers API is unavailable. Format is `custos-connector-manifest-v1_<algorithm>-<hex>` with strict digest-normalization rules. v1 restricts the registered digest algorithms set to sha256; the format itself is algorithm-agnostic so sha512 or other algorithms can be added in M2+ behind a scheme version bump if their tag length exceeds the OCI 128-char cap. Closes the corresponding open item in `connector-service/todos.md`.

## Before

`design.md` § Manifest artifact rules described the fallback path in one line — "fetch fallback tag `<digest>.custos-connector-manifest-v1` where digest is normalized `sha256-<hex>`" — without specifying separator semantics, character-set constraints, length budget under the OCI 128-char tag cap, behavior for non-sha256 digests, normalization rules for malformed digests, or audit event vocabulary. Open Questions listed "Fallback tag naming finalization and normalization edge cases for non-sha256 digests". `todos.md` carried the matching open item.

This left registry-interop work without a deterministic algorithm and risked silent breakage when a registry happened to support non-sha256 digests.

## After

§ Plugin Packaging and Discovery gains a new sub-section "Fallback tag naming" with:

- **Tag format**: `custos-connector-manifest-v1_<algorithm>-<hex>`. Scheme version (`v1`) is part of the prefix; bumping the format requires `v2` and a transition window.
- **Separator**: `_`, not `.`, to avoid collisions with file-extension parsers in registry tooling.
- **Normalization rules**: lowercase hex, `:` → `-`, reject non-`[0-9a-f]` chars, reject incorrect hex length for the algorithm. Failure code: `invalid-digest-format`.
- **Registered digest algorithms set**: configurable platform set. v1 contains `sha256` only. Other algorithms rejected with `unsupported-digest-algorithm`.
- **No platform-side hash substitution**: the fallback tag must match the digest the registry advertises; computing our own hash would point to a manifest the registry cannot resolve.
- **Extensibility table**: shows sha256 (101 chars, fits), sha512 (165, requires v2), sha384 (133, requires v2). The format itself is algorithm-agnostic; what gates support is the OCI 128-char tag cap.
- **Length budget diagram**: visualizes the 101-char tag with 27 chars of headroom.
- **Collision policy**: by construction, one digest yields one tag; multi-manifest collisions still hit the "exactly one valid manifest" rule already in the selection algorithm.
- **Three new audit events**: `connector.manifest.fallback-used` (operational signal), `connector.manifest.fallback-ignored` (Referrers won), `connector.manifest.fallback-rejected` (validation failed).

The original manifest selection algorithm step 2 is updated to reference the new sub-section. `todos.md`: closes the fallback-tag item. Open Questions: removes the matching bullet.

## Key Decisions Locked This Session

1. **`_` separator, not `.`.** Many registry tools parse `<name>.<ext>` semantics from tag names. `.` between hex and suffix produced an ambiguous filename-shaped tag. `_` is unambiguous within the OCI tag character set.
2. **Prefix-first, hex-suffix layout.** Putting `custos-connector-manifest-v1` first makes the tag self-identifying when an operator lists tags on a repo (sort by tag → all Custos fallback tags cluster together). The previous layout buried the scheme behind a 64-char hex blob.
3. **Scheme version baked into the prefix (`v1` literal).** Lets us evolve the tag format (separator, algorithm, encoding) without ambiguity. v2 tags coexist with v1 tags during a transition; the platform tries both schemes when resolving fallback in mixed-state registries (deferred to whenever v2 actually lands).
4. **v1 is sha256-only via a registered-algorithms set.** Locking sha256 only for v1 reflects three realities: the OCI Distribution Spec mandates sha256 support, multi-algorithm fallback expands the attack surface (weak algorithms), and we have no concrete need for sha512 today. The *format* is algorithm-agnostic, so the limitation is operational, not architectural.
5. **No platform-computed hash substitution.** Tempting to "just compute sha256 ourselves if the registry only gives us sha512" — but then the fallback tag wouldn't match anything the registry can resolve. Hard reject is the only correct option.
6. **Length-budget table is part of the spec, not an aside.** sha512 fails the 128-char cap (165 chars). Adding sha512 in M2+ is not a one-line change to the algorithm set — it requires a format change (v2). Documenting this now prevents a future surprise.
7. **Three audit events, not one.** Separating `fallback-used` (informational) from `fallback-ignored` (Referrers won) from `fallback-rejected` (failure) lets operators alert on the right signal: a spike in `fallback-used` indicates a registry regression; `fallback-rejected` is a hard error needing investigation.

## Impact

- Connector Service plugin loader can implement the fallback path with a complete spec.
- Registry-interop test matrix can target the exact tag format; no more guessing across registries.
- Operators get a clear escalation signal (`fallback-used` spike → registry regressed on Referrers API support).
- sha512/other-algorithm support is a discoverable future task with a documented contract (bump scheme to v2 with new format).
- Connector Service `todos.md` reduced from 3 open items to 2.

## Out of Scope (Deferred)

- v2 scheme actually being defined — only relevant when a real sha512-only registry shows up.
- Per-registry fallback-tag opt-out (some registries may block tag listing for performance reasons) — would require a workspace-level config knob; not needed in v1.
- Cross-registry fallback tag mirroring (publishing fallback tags to a mirror registry) — operational tooling, M2+.
- Automatic discovery of registry Referrers API support (probe once, cache the result) — performance optimization that doesn't change the contract.
- Migration tooling for users coming from a hypothetical earlier `.custos-connector-manifest-v1`-suffix scheme — never released, no migration needed.

## Related Requirements

- OCI Distribution Spec v1.1 — Referrers API and tag character set / length constraints.
- INCON-012 (sink connectors omitting events) — unaffected.
- ADR-008 — `unsupported-digest-algorithm` and `invalid-digest-format` map to permanent plugin-load failures, not retryable.
