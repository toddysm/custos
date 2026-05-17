# Change: incon-002-activity-refs

Date: 2026-05-17
Type: architecture
Sequence: 002
GitHub Issue: #27
Status: open

## Summary

Update the workflow and template YAML examples in `design/architecture/overview.md` to use fully-qualified activity references (`<namespace>/<type>@<major>`), as required by the ARM-locked v1 namespace model. Add an explanatory note that short-form aliases are a planned post-M1 feature. Without this change, anyone copying the overview's YAML as authoritative syntax would author workflows that fail Catalog validation, workflow save, and ARM dispatch.

## Before

`overview.md` § Workflow and Template Schema:

```yaml
# workflow
steps:
  - id: scan
    activity: vuln-scan@2
  - id: gate
    activity: quarantine@1

# template
placeholders:
  - name: scanActivity
    type: activityRef
    activityType: vuln-scan
    default: vuln-scan@2
```

No accompanying note on the namespace model or short-form deferral.

## After

```yaml
# workflow
steps:
  - id: scan
    activity: custos.builtin/vuln-scan@2
  - id: gate
    activity: custos.builtin/quarantine@1

# template
placeholders:
  - name: scanActivity
    type: activityRef
    activityType: vuln-scan
    default: custos.builtin/vuln-scan@2
```

Added paragraph after the template example:

> _Activity references are **fully qualified** in v1: `<namespace>/<type>@<major>` (e.g. `custos.builtin/vuln-scan@2`, `snyk/container-scan@1`, `acme-corp/custom-gate@1`). Short-form aliases (e.g. `vuln-scan@2` resolving to the highest-trust namespace match) are a planned post-M1 feature — see ARM design § Activity Manifest v1 for the namespace model._

## Impact

- Removes the second of five HIGH inconsistencies and aligns the overview's workflow authoring examples with the system as designed.
- Stops a foreseeable failure mode where copy-pasted examples would be rejected by Catalog publish validation, the workflow validator, and ARM dispatch.
- Sets the expected style for the upcoming Workflow Service detailed design — the compile-time type checker can assume fully-qualified refs.
- `activityType: vuln-scan` in the placeholder schema (a type *constraint*, not a resolved ref) is left as-is; that field's exact semantics are still owned by the upcoming Catalog/Template Service design.

## Related Requirements

- ARM § Activity Manifest v1, Namespace model (authoritative)
- ADR-008
- Issues: #27 (this change), #26 (INCON-001, prior fix in same overview section), ARM TODO-005 (short-form refs deferred)
