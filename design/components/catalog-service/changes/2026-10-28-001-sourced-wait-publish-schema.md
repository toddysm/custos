# Sourced wait publication schema

Date: 2026-10-28

Related work: [#1006](https://github.com/toddysm/custos/issues/1006),
[pull request #1024](https://github.com/toddysm/custos/pull/1024).

## Contract

The normative Workflow and WorkflowTemplate schema identifiers advance to
`workflow.v1.1.schema.json` and `workflow-template.v1.1.schema.json`.
The authored `apiVersion` remains `custos.dev/v1`; existing activity, let and
sub-workflow forms remain valid.

Both schemas accept `waitFor` with a required Common Expression Language (CEL)
`eventKey` token, optional selector token and positive constant time-to-live.
Waits accept explicit `needs` dependencies but not activity-only `retry` or
`on_error` fields. Fractional durations retain Workflow's microsecond precision
when registered with Trigger.
Missing, self and forward dependencies and unrepresentable durations are
rejected before publication. Runtime checks also guard actual deadline overflow.
An external `source` requires a constant, nonblank `connectorInstanceId` and a
nonempty, duplicate-free subset of `push`/`pull` delivery modes. Its selector
is mandatory. A polling interval requires pull delivery and an integer of at
least ten seconds. Workspace and credential fields are forbidden.

Optional legacy fields may be absent or null. Runtime connector expressions,
including template placeholders in the source identifier, are not supported
by this constant-source contract.
Whitespace-padded and empty expression wrappers are also rejected as connector
identifiers. String placeholders in wait event-key/selector slots are safely
quoted during materialization; compound placeholder expressions are rejected
before publication instead of creating unusable template versions.

## Publication gates

- Structural validation admits the new step form in workflows and templates.
- The shared source codec prevents JavaScript Object Notation (JSON) Schema's acceptance of integral floats
  from weakening the runtime's strict integer contract.
- Normalization emits a dedicated source-reference slot. Its reader selects
  only existence in the trusted publication workspace, independent of installed
  connector type versions and the stronger `connector:validate` permission.
  PostgreSQL wiring shares the metadata pool; custom deployments inject a
  scoped reader. Missing wiring or backend failure is an explicit dependency
  error, never a false missing-instance result.
- The CEL gate parses and checks name bindings for both wait expressions.
- Published versions preserve the source association.

This change enables normal publication of sourced waits. It does not complete
the deferred deployed Workflow wait driver or durable mirror repository;
those boundaries remain tracked on
[#1017](https://github.com/toddysm/custos/issues/1017) and
[#1018](https://github.com/toddysm/custos/issues/1018).

## Verification

Regression coverage lives in
[schema tests](../../../../src/services/catalog-service/tests/test_schema_validate.py),
[normalization tests](../../../../src/services/catalog-service/tests/test_normalize.py),
[CEL tests](../../../../src/services/catalog-service/tests/test_cel_validate.py), and
[publication route tests](../../../../src/services/catalog-service/tests/api/test_workflows.py).
