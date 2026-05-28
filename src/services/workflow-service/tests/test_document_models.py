"""Unit tests for :mod:`custos_workflow.document.models`."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from custos_workflow.document import (
    ActivityStep,
    BackoffPolicy,
    BackoffStrategy,
    Defaults,
    JitterStrategy,
    LetStep,
    OnErrorAction,
    OnErrorArm,
    OnErrorMatch,
    RetryPolicy,
    WorkflowDocument,
    WorkflowSpec,
    WorkflowStep,
)

# ---------------------------------------------------------------------------
# Retry / backoff
# ---------------------------------------------------------------------------


class TestRetryPolicy:
    def test_all_fields_optional(self) -> None:
        policy = RetryPolicy()
        assert policy.max_attempts is None
        assert policy.backoff is None
        assert policy.jitter is None
        assert policy.respect_retry_after is None

    def test_alias_round_trip(self) -> None:
        policy = RetryPolicy.model_validate(
            {
                "maxAttempts": 5,
                "backoff": {
                    "strategy": "exponential",
                    "initialDelay": "PT1S",
                    "maxDelay": "PT5M",
                    "multiplier": 2.0,
                },
                "jitter": "full",
                "respectRetryAfter": False,
            }
        )
        assert policy.max_attempts == 5
        assert policy.backoff is not None
        assert policy.backoff.strategy is BackoffStrategy.EXPONENTIAL
        assert policy.backoff.initial_delay == "PT1S"
        assert policy.jitter is JitterStrategy.FULL
        assert policy.respect_retry_after is False
        # Dump must round-trip with the wire aliases preserved.
        dumped = policy.model_dump(by_alias=True, exclude_none=True)
        assert dumped["maxAttempts"] == 5
        assert dumped["respectRetryAfter"] is False
        assert dumped["backoff"]["initialDelay"] == "PT1S"

    def test_max_attempts_must_be_positive(self) -> None:
        with pytest.raises(ValidationError):
            RetryPolicy.model_validate({"maxAttempts": 0})

    def test_unknown_field_rejected(self) -> None:
        with pytest.raises(ValidationError):
            RetryPolicy.model_validate({"maxAttempts": 3, "deadline": "PT1M"})

    def test_backoff_multiplier_must_be_positive(self) -> None:
        with pytest.raises(ValidationError):
            BackoffPolicy(strategy=BackoffStrategy.EXPONENTIAL, multiplier=0.0)


# ---------------------------------------------------------------------------
# on_error
# ---------------------------------------------------------------------------


class TestOnErrorMatch:
    def test_exactly_one_of_required(self) -> None:
        with pytest.raises(ValidationError):
            OnErrorMatch()

    def test_two_branches_rejected(self) -> None:
        with pytest.raises(ValidationError):
            OnErrorMatch.model_validate({"code": "X", "codePrefix": "X."})

    def test_class_alias(self) -> None:
        m = OnErrorMatch.model_validate({"class": "Transient"})
        assert m.cls == "Transient"
        assert m.code is None
        # Dump uses the wire alias.
        assert m.model_dump(by_alias=True, exclude_none=True) == {"class": "Transient"}

    def test_code_prefix_alias(self) -> None:
        m = OnErrorMatch.model_validate({"codePrefix": "HTTP_5"})
        assert m.code_prefix == "HTTP_5"


class TestOnErrorArm:
    def test_happy_path(self) -> None:
        arm = OnErrorArm.model_validate(
            {
                "match": {"codePrefix": "HTTP_5"},
                "do": "retry",
                "maxAttempts": 5,
            }
        )
        assert arm.do is OnErrorAction.RETRY
        assert arm.max_attempts == 5
        assert arm.retry is None

    def test_unknown_action_rejected(self) -> None:
        with pytest.raises(ValidationError):
            OnErrorArm.model_validate({"match": {"code": "X"}, "do": "explode"})


# ---------------------------------------------------------------------------
# Step common modifiers (CEL slot shape)
# ---------------------------------------------------------------------------


def _valid_activity_payload(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "id": "scan",
        "activity": "security/scan@1",
    }
    base.update(overrides)
    return base


class TestStepCommonCelSlots:
    @pytest.mark.parametrize(
        "field",
        ["if", "when", "unless", "forEach", "where"],
    )
    def test_cel_token_required(self, field: str) -> None:
        # Plain string (no ``${{ ... }}`` wrapper) is rejected.
        with pytest.raises(ValidationError):
            ActivityStep.model_validate(_valid_activity_payload(**{field: "ctx.go"}))

    def test_cel_token_accepted(self) -> None:
        step = ActivityStep.model_validate(_valid_activity_payload(**{"if": "${{ ctx.enabled }}"}))
        assert step.if_ == "${{ ctx.enabled }}"

    def test_step_id_pattern(self) -> None:
        with pytest.raises(ValidationError):
            ActivityStep.model_validate(_valid_activity_payload(id="Scan-Step"))

    def test_unknown_modifier_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ActivityStep.model_validate(_valid_activity_payload(parallel=[{"id": "x"}]))


class TestStepCommonNeeds:
    def test_accepts_well_formed_list(self) -> None:
        step = ActivityStep.model_validate(
            _valid_activity_payload(id="b", needs=["a", "scan-1"]),
        )
        assert step.needs == ["a", "scan-1"]

    def test_none_when_omitted(self) -> None:
        step = ActivityStep.model_validate(_valid_activity_payload())
        assert step.needs is None

    def test_empty_list_rejected(self) -> None:
        # ``min_length=1``: \"needs:\" must either be absent or have
        # at least one entry; an empty list is a YAML smell and is
        # blocked at parse time.
        with pytest.raises(ValidationError):
            ActivityStep.model_validate(_valid_activity_payload(needs=[]))

    def test_invalid_step_id_grammar_rejected(self) -> None:
        # ``Scan-Step`` violates the DNS-1123-style step-id grammar
        # (capital letter). The validator surfaces the offending
        # entry verbatim.
        with pytest.raises(ValidationError, match="does not match the step-id grammar"):
            ActivityStep.model_validate(
                _valid_activity_payload(id="b", needs=["Scan-Step"]),
            )

    def test_self_reference_rejected(self) -> None:
        # A step listing its own id in ``needs:`` is rejected at
        # parse time so the topology layer never sees a self-loop.
        with pytest.raises(ValidationError, match="refers to itself"):
            ActivityStep.model_validate(
                _valid_activity_payload(id="scan", needs=["scan"]),
            )

    def test_duplicate_entries_rejected(self) -> None:
        with pytest.raises(ValidationError, match="duplicated"):
            ActivityStep.model_validate(
                _valid_activity_payload(id="b", needs=["a", "a"]),
            )


# ---------------------------------------------------------------------------
# Activity step
# ---------------------------------------------------------------------------


class TestActivityStep:
    def test_singular_connector(self) -> None:
        step = ActivityStep.model_validate(
            _valid_activity_payload(
                connector="primary",
                **{"with": {"image": "alpine:3.20"}},
            )
        )
        assert step.connector == "primary"
        assert step.connectors is None
        assert step.with_ == {"image": "alpine:3.20"}

    def test_connectors_map(self) -> None:
        step = ActivityStep.model_validate(
            _valid_activity_payload(
                activity="image-promote/copy@1",
                connectors={"source": "src-reg", "destination": "dest-reg"},
            )
        )
        assert step.connector is None
        assert step.connectors == {"source": "src-reg", "destination": "dest-reg"}

    def test_connector_xor_connectors(self) -> None:
        with pytest.raises(ValidationError):
            ActivityStep.model_validate(
                _valid_activity_payload(
                    connector="primary",
                    connectors={"source": "alt"},
                )
            )

    def test_activity_ref_must_be_fully_qualified(self) -> None:
        with pytest.raises(ValidationError):
            ActivityStep.model_validate(_valid_activity_payload(activity="scan"))

    def test_activity_ref_cel_token_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ActivityStep.model_validate(
                _valid_activity_payload(activity="${{ placeholders.scan }}")
            )

    def test_no_binding_is_allowed(self) -> None:
        step = ActivityStep.model_validate(_valid_activity_payload())
        assert step.connector is None
        assert step.connectors is None

    def test_empty_connector_string_rejected(self) -> None:
        # Mirrors Catalog schema's ``minLength: 1`` on connector
        # strings so an empty value fails the defensive re-check.
        with pytest.raises(ValidationError):
            ActivityStep.model_validate(_valid_activity_payload(connector=""))

    def test_empty_connectors_map_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ActivityStep.model_validate(_valid_activity_payload(connectors={}))

    def test_empty_connectors_map_value_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ActivityStep.model_validate(_valid_activity_payload(connectors={"source": ""}))


# ---------------------------------------------------------------------------
# Let / Workflow steps
# ---------------------------------------------------------------------------


class TestLetStep:
    def test_happy_path(self) -> None:
        step = LetStep.model_validate(
            {
                "id": "compute",
                "let": {"total": "${{ steps.scan.findings.size() }}"},
            }
        )
        assert step.let["total"] == "${{ steps.scan.findings.size() }}"

    def test_empty_let_rejected(self) -> None:
        with pytest.raises(ValidationError):
            LetStep.model_validate({"id": "compute", "let": {}})


class TestWorkflowStep:
    def test_uuid_reference(self) -> None:
        step = WorkflowStep.model_validate(
            {
                "id": "child",
                "workflow": "11111111-2222-3333-4444-555555555555",
            }
        )
        assert step.workflow == "11111111-2222-3333-4444-555555555555"

    def test_triple_reference(self) -> None:
        step = WorkflowStep.model_validate(
            {
                "id": "child",
                "workflow": "platform/cleanup@1.0.0",
                "with": {"target": "${{ inputs.env }}"},
            }
        )
        assert step.workflow == "platform/cleanup@1.0.0"
        assert step.with_ == {"target": "${{ inputs.env }}"}

    def test_invalid_reference_rejected(self) -> None:
        with pytest.raises(ValidationError):
            WorkflowStep.model_validate({"id": "child", "workflow": "cleanup"})

    def test_workflow_ref_cel_token_rejected(self) -> None:
        # Template materialisation must precede compilation; a raw
        # ``${{ ... }}`` workflow ref reaching the compiler is a bug
        # in the publish pipeline and must fail loudly.
        with pytest.raises(ValidationError):
            WorkflowStep.model_validate({"id": "child", "workflow": "${{ placeholders.next }}"})


# ---------------------------------------------------------------------------
# Step discriminator
# ---------------------------------------------------------------------------


def _spec_with_step(step: dict[str, object]) -> WorkflowSpec:
    return WorkflowSpec.model_validate({"steps": [step]})


class TestStepDiscriminator:
    def test_activity_kind(self) -> None:
        spec = _spec_with_step(_valid_activity_payload())
        assert isinstance(spec.steps[0], ActivityStep)

    def test_let_kind(self) -> None:
        spec = _spec_with_step({"id": "x", "let": {"a": 1}})
        assert isinstance(spec.steps[0], LetStep)

    def test_workflow_kind(self) -> None:
        spec = _spec_with_step({"id": "x", "workflow": "11111111-2222-3333-4444-555555555555"})
        assert isinstance(spec.steps[0], WorkflowStep)

    def test_no_kind_keyword_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _spec_with_step({"id": "x"})

    def test_multiple_kind_keywords_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _spec_with_step({"id": "x", "activity": "ns/t@1", "let": {"a": 1}})

    def test_constructed_instance_round_trip(self) -> None:
        # Exercise the model-instance branch of the discriminator
        # (e.g. building a spec programmatically from already-validated
        # step instances).
        activity = ActivityStep.model_validate(_valid_activity_payload())
        let = LetStep.model_validate({"id": "compute", "let": {"a": 1}})
        workflow = WorkflowStep.model_validate(
            {"id": "next", "workflow": "11111111-2222-3333-4444-555555555555"}
        )
        spec = WorkflowSpec.model_validate({"steps": [activity, let, workflow]})
        assert isinstance(spec.steps[0], ActivityStep)
        assert isinstance(spec.steps[1], LetStep)
        assert isinstance(spec.steps[2], WorkflowStep)

    def test_non_dict_non_step_value_rejected(self) -> None:
        # Scalars / lists are not valid step values.
        with pytest.raises(ValidationError):
            _spec_with_step("not a step")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Spec + root
# ---------------------------------------------------------------------------


class TestWorkflowSpec:
    def test_steps_required(self) -> None:
        with pytest.raises(ValidationError):
            WorkflowSpec.model_validate({"steps": []})

    def test_duplicate_step_ids_rejected(self) -> None:
        with pytest.raises(ValidationError) as exc_info:
            WorkflowSpec.model_validate(
                {
                    "steps": [
                        _valid_activity_payload(id="scan"),
                        _valid_activity_payload(id="scan"),
                    ]
                }
            )
        assert "duplicate step id" in str(exc_info.value)

    def test_defaults_block(self) -> None:
        spec = WorkflowSpec.model_validate(
            {
                "defaults": {"retry": {"maxAttempts": 4}},
                "steps": [_valid_activity_payload()],
            }
        )
        assert isinstance(spec.defaults, Defaults)
        assert spec.defaults.retry is not None
        assert spec.defaults.retry.max_attempts == 4


class TestTrigger:
    def test_happy_path(self) -> None:
        from custos_workflow.document import Trigger

        trig = Trigger.model_validate({"type": "schedule", "connector": "sched"})
        assert trig.type == "schedule"
        assert trig.connector == "sched"

    def test_empty_connector_string_rejected(self) -> None:
        from custos_workflow.document import Trigger

        with pytest.raises(ValidationError):
            Trigger.model_validate({"type": "schedule", "connector": ""})


class TestWorkflowDocument:
    def test_minimal_document(self) -> None:
        doc = WorkflowDocument.model_validate(
            {
                "apiVersion": "custos.dev/v1",
                "kind": "Workflow",
                "metadata": {"name": "demo", "workspace": "default"},
                "spec": {"steps": [_valid_activity_payload()]},
            }
        )
        assert doc.api_version == "custos.dev/v1"
        assert doc.metadata.name == "demo"
        assert doc.metadata.workspace == "default"
        assert len(doc.spec.steps) == 1

    def test_metadata_workspace_optional(self) -> None:
        # Workspace is supplied by the API path at publish time.
        doc = WorkflowDocument.model_validate(
            {
                "apiVersion": "custos.dev/v1",
                "kind": "Workflow",
                "metadata": {"name": "demo"},
                "spec": {"steps": [_valid_activity_payload()]},
            }
        )
        assert doc.metadata.workspace is None

    def test_metadata_name_pattern(self) -> None:
        with pytest.raises(ValidationError):
            WorkflowDocument.model_validate(
                {
                    "apiVersion": "custos.dev/v1",
                    "kind": "Workflow",
                    "metadata": {"name": "Demo"},
                    "spec": {"steps": [_valid_activity_payload()]},
                }
            )

    def test_unknown_top_level_key_rejected(self) -> None:
        with pytest.raises(ValidationError):
            WorkflowDocument.model_validate(
                {
                    "apiVersion": "custos.dev/v1",
                    "kind": "Workflow",
                    "metadata": {"name": "demo"},
                    "spec": {"steps": [_valid_activity_payload()]},
                    "status": {"phase": "Pending"},
                }
            )

    def test_wrong_api_version_rejected(self) -> None:
        with pytest.raises(ValidationError):
            WorkflowDocument.model_validate(
                {
                    "apiVersion": "custos.dev/v2",
                    "kind": "Workflow",
                    "metadata": {"name": "demo"},
                    "spec": {"steps": [_valid_activity_payload()]},
                }
            )

    def test_wrong_kind_rejected(self) -> None:
        # WorkflowTemplate is a separate document type owned by
        # Catalog; the workflow-service compiler only sees Workflow.
        with pytest.raises(ValidationError):
            WorkflowDocument.model_validate(
                {
                    "apiVersion": "custos.dev/v1",
                    "kind": "WorkflowTemplate",
                    "metadata": {"name": "demo"},
                    "spec": {"steps": [_valid_activity_payload()]},
                }
            )
