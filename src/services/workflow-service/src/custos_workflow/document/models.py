"""Pydantic v2 models for the Workflow YAML document (WF-IMPL-016).

The shape mirrors ``src/services/catalog-service/src/custos_catalog/schema/workflow.py``
(`WORKFLOW_SCHEMA`) and the design lock in ``design/architecture/overview.md``
§ Workflow and Template Schema. Catalog is the on-the-wire source of
truth; this module is the typed Python view the Definition Compiler
walks. Keep the two in lockstep — any field added to the JSON Schema
MUST appear here in the same PR.

Step kinds covered (v1 wire schema):

- :class:`ActivityStep` — ``activity: <ref>`` + ``connector:`` or
  ``connectors:`` map binding.
- :class:`LetStep` — ``let: {…}`` inline expression bindings.
- :class:`WorkflowStep` — ``workflow: <id>`` sub-workflow invocation.
- :class:`WaitStep` — ``wait: <ISO-8601 duration>`` durable sleep.
  The Run Controller handles this kind directly via a Dapr durable
  timer (design.md § Workflow Schema: Step Kinds Handled — Wait /
  sleep → Run Controller → Durable timer); no Step Coordinator
  handler is involved.

Step *modifiers* (`if` / `when` / `unless` / `forEach` / `where` /
`retry` / `on_error`) are shared properties on every kind, not
separate kinds — they correspond to the "Step forms" table in
``design/architecture/overview.md`` rather than the "Step Kinds
Handled" table in the Workflow Service design. The latter table
also mentions ``parallel:`` / ``approval:`` / ``waitFor:`` which
are not yet in the v1 wire schema (Catalog rejects them) and
therefore not modelled here; they will land alongside the
corresponding Catalog schema extension.
"""

from __future__ import annotations

import re
from enum import StrEnum
from typing import Annotated, Any, Final, Literal, NewType

from pydantic import (
    BaseModel,
    ConfigDict,
    Discriminator,
    Field,
    Tag,
    model_validator,
)

#: A CEL expression source string, preserved verbatim from YAML.
#:
#: Workflow authors wrap CEL in ``${{ ... }}`` tokens; we keep the
#: full token (including the wrapper) so the call-site collector
#: (WF-IMPL-020) and the type-checker (WF-IMPL-022) can parse the
#: exact source the author wrote. This is a typing-only newtype —
#: at runtime it is a plain ``str``.
CelSource = NewType("CelSource", str)

#: Pattern matching the CEL ``${{ ... }}`` wrapper. Mirrors the
#: Catalog schema's ``_CEL_TOKEN_PATTERN``.
_CEL_TOKEN_PATTERN: Final[re.Pattern[str]] = re.compile(r"^\$\{\{[\s\S]+\}\}$")

#: Step id grammar (DNS-1123-like). Mirrors the Catalog schema.
_STEP_ID_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[a-z][a-z0-9-]{0,62}$")

#: Workspace / workflow / template name grammar. Same as step id.
_NAME_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[a-z][a-z0-9-]{0,62}$")

#: Fully-qualified activity reference: ``<ns>/<type>@<version>``.
_ACTIVITY_REF_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^[a-z][a-z0-9._-]*/[a-z][a-z0-9._-]*@"
    r"(?:[0-9]+|[0-9]+\.[0-9]+|[0-9]+\.[0-9]+\.[0-9]+)$"
)

#: UUID4 pattern for ``workflowVersionId`` references.
_UUID_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)

#: ISO-8601 duration grammar for ``wait:`` step durations.
#:
#: The Catalog publish gate validates the exact same shape on the
#: wire; the document model rejects malformed strings at parse time
#: so the compiler never sees an unparseable duration. The regex
#: matches the common ``PnDTnHnMnS`` subset (no months / years —
#: those are calendar-dependent and incompatible with a durable
#: timer) and ``PnW`` weeks form. At least one component is required.
_ISO8601_DURATION_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^P(?:"
    r"(?P<weeks>\d+)W"  # weeks form: ``PnW``
    r"|"
    r"(?:(?P<days>\d+)D)?"
    r"(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+(?:\.\d+)?)S)?)?"
    r")$"
)

#: ``<workspace>/<name>@<version>`` triple for sub-workflow refs.
_WORKFLOW_TRIPLE_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^[a-z][a-z0-9-]{0,62}/[a-z][a-z0-9-]{0,62}@[0-9]+(?:\.[0-9]+){0,2}$"
)


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class BackoffStrategy(StrEnum):
    """Backoff curve for the retry mechanics layer."""

    CONSTANT = "constant"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"


class JitterStrategy(StrEnum):
    """Jitter strategy applied on top of the backoff curve."""

    NONE = "none"
    FULL = "full"
    EQUAL = "equal"
    DECORRELATED = "decorrelated"


class OnErrorAction(StrEnum):
    """The action a matched ``on_error`` arm takes."""

    SKIP = "skip"
    RETRY = "retry"
    FAIL = "fail"


# ---------------------------------------------------------------------------
# Base model
# ---------------------------------------------------------------------------


class _StrictModel(BaseModel):
    """Base for every WorkflowDocument node.

    ``extra="forbid"`` mirrors the Catalog JSON Schema's
    ``additionalProperties: false`` so an unknown key fails the same
    way at publish time and at ``StartRun`` defensive re-validation.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=False)


# ---------------------------------------------------------------------------
# Retry policy + on-error routing
# ---------------------------------------------------------------------------


class BackoffPolicy(_StrictModel):
    """Backoff curve. All fields optional — overlays fill from defaults."""

    strategy: BackoffStrategy | None = None
    initial_delay: str | None = Field(default=None, alias="initialDelay")
    max_delay: str | None = Field(default=None, alias="maxDelay")
    multiplier: float | None = Field(default=None, gt=0.0)

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
    )


class RetryPolicy(_StrictModel):
    """Workflow retry mechanics (overlay-merged at runtime).

    Field-by-field optionality is intentional: the precedence overlay
    described in ``design/components/workflow-service/design.md``
    § Retry Policy fills in unset fields from the next layer down
    (per-match → step → ``spec.defaults`` → platform defaults).
    """

    max_attempts: int | None = Field(default=None, alias="maxAttempts", ge=1)
    backoff: BackoffPolicy | None = None
    jitter: JitterStrategy | None = None
    respect_retry_after: bool | None = Field(default=None, alias="respectRetryAfter")

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
    )


class OnErrorMatch(_StrictModel):
    """Match clause for an ``on_error[]`` arm.

    Exactly one of ``code`` / ``code_prefix`` / ``cls`` MUST be set;
    the Catalog schema enforces this via ``oneOf`` and the
    :func:`_exactly_one_of_match` validator below mirrors that rule.
    """

    code: str | None = None
    code_prefix: str | None = Field(default=None, alias="codePrefix")
    # ``class`` is a Python keyword. The wire field stays ``class``;
    # the Python attribute is ``cls`` so the rest of the codebase can
    # use ordinary attribute access.
    cls: str | None = Field(default=None, alias="class")

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
    )

    @model_validator(mode="after")
    def _exactly_one_of_match(self) -> OnErrorMatch:
        present = sum(1 for v in (self.code, self.code_prefix, self.cls) if v is not None)
        if present != 1:
            raise ValueError(
                "on_error[].match must specify exactly one of: code, codePrefix, class"
            )
        return self


class OnErrorArm(_StrictModel):
    """One entry in a step or workflow-level ``on_error:`` list."""

    match: OnErrorMatch
    do: OnErrorAction
    retry: RetryPolicy | None = None
    # Shorthand for ``retry: { maxAttempts: N }`` on a ``do: retry``
    # arm. Both the shorthand and a structured ``retry:`` can be
    # present at the same time; the merge resolution is the
    # responsibility of WF-IMPL-019+, not this model.
    max_attempts: int | None = Field(default=None, alias="maxAttempts", ge=1)

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
    )


# ---------------------------------------------------------------------------
# Spec-level surface
# ---------------------------------------------------------------------------


class InputDefinition(_StrictModel):
    """One entry under ``spec.inputs.<name>``."""

    type: Literal["string", "integer", "number", "boolean", "object", "array"]
    required: bool | None = None
    # ``default`` may be a literal scalar/object OR a CEL expression
    # token. We keep the open shape; downstream code (WF-IMPL-017)
    # owns the per-input value validation.
    default: Any = None
    description: str | None = None


class Defaults(_StrictModel):
    """``spec.defaults`` — currently only ``retry`` is defined."""

    retry: RetryPolicy | None = None


class Trigger(_StrictModel):
    """One entry under ``spec.triggers[]``."""

    type: str = Field(min_length=1)
    # Connector name or CEL expression token. Mirrors the Catalog
    # schema's ``minLength: 1`` so an empty connector reference fails
    # the defensive re-check rather than silently propagating. The
    # structural validator does not parse the CEL — see WF-IMPL-022
    # for the type checker.
    connector: str | None = Field(default=None, min_length=1)


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------


class _StepCommon(_StrictModel):
    """Properties shared by every step kind.

    These mirror the ``_step_common_properties()`` block in the
    Catalog schema. Each CEL slot is typed as :data:`CelSource` so
    grepping for expression sites is straightforward.
    """

    id: str = Field(pattern=_STEP_ID_PATTERN.pattern)
    description: str | None = None
    needs: list[str] | None = Field(default=None, min_length=1)
    if_: CelSource | None = Field(default=None, alias="if")
    when: CelSource | None = None
    unless: CelSource | None = None
    for_each: CelSource | None = Field(default=None, alias="forEach")
    where: CelSource | None = None
    retry: RetryPolicy | None = None
    on_error: list[OnErrorArm] | None = None

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
    )

    @model_validator(mode="after")
    def _check_cel_wrappers(self) -> _StepCommon:
        for field_name, value in (
            ("if", self.if_),
            ("when", self.when),
            ("unless", self.unless),
            ("forEach", self.for_each),
            ("where", self.where),
        ):
            if value is not None and not _CEL_TOKEN_PATTERN.match(value):
                raise ValueError(
                    f"step {self.id!r}: {field_name!r} must be a CEL "
                    "expression token of the form '${{ ... }}'"
                )
        if self.needs is not None:
            seen: set[str] = set()
            for dep in self.needs:
                if not _STEP_ID_PATTERN.match(dep):
                    raise ValueError(
                        f"step {self.id!r}: needs entry {dep!r} does not match the step-id grammar"
                    )
                if dep == self.id:
                    raise ValueError(f"step {self.id!r}: needs entry refers to itself")
                if dep in seen:
                    raise ValueError(f"step {self.id!r}: needs entry {dep!r} is duplicated")
                seen.add(dep)
        return self


class ActivityStep(_StepCommon):
    """Activity step: bind a containerized activity to one or more connectors.

    Exactly one binding form is permitted at a time: ``connector:``
    (singular string) OR ``connectors:`` (alias map). Both absent is
    allowed for connectorless activities.
    """

    activity: str
    # Mirror the Catalog schema's ``minLength: 1`` on the singular
    # binding and ``minProperties: 1`` (plus non-empty values) on the
    # map binding so an empty connector reference fails the defensive
    # re-check rather than reaching the compiler.
    connector: str | None = Field(default=None, min_length=1)
    connectors: dict[str, Annotated[str, Field(min_length=1)]] | None = Field(
        default=None,
        min_length=1,
    )
    with_: dict[str, Any] | None = Field(default=None, alias="with")

    @model_validator(mode="after")
    def _connector_xor_connectors(self) -> ActivityStep:
        if self.connector is not None and self.connectors is not None:
            raise ValueError(
                f"step {self.id!r}: 'connector' and 'connectors' are "
                "mutually exclusive; choose one binding form"
            )
        return self

    @model_validator(mode="after")
    def _activity_ref_shape(self) -> ActivityStep:
        # CEL tokens (``${{ placeholders.scanActivity }}``) are NOT
        # accepted here — template materialisation must have happened
        # by the time the compiler walks the document, so a CEL token
        # in ``activity:`` is a publish-pipeline bug and is rejected.
        if _CEL_TOKEN_PATTERN.match(self.activity):
            raise ValueError(
                f"step {self.id!r}: activity reference is still a CEL "
                "token; template materialisation must precede compilation"
            )
        if not _ACTIVITY_REF_PATTERN.match(self.activity):
            raise ValueError(
                f"step {self.id!r}: activity reference must be "
                "fully-qualified '<namespace>/<type>@<version>'"
            )
        return self


class LetStep(_StepCommon):
    """Pure-data step: inline expression bindings evaluated by the CEL engine."""

    let: dict[str, Any] = Field(min_length=1)


class WorkflowStep(_StepCommon):
    """Sub-workflow invocation.

    ``workflow:`` references must be either a UUID
    ``workflowVersionId`` or a ``<workspace>/<name>@<version>`` triple
    (REQ-025 immutability — no name-only references).
    """

    workflow: str
    with_: dict[str, Any] | None = Field(default=None, alias="with")

    @model_validator(mode="after")
    def _workflow_ref_shape(self) -> WorkflowStep:
        if _CEL_TOKEN_PATTERN.match(self.workflow):
            raise ValueError(
                f"step {self.id!r}: workflow reference is still a CEL "
                "token; template materialisation must precede compilation"
            )
        if not (
            _UUID_PATTERN.match(self.workflow) or _WORKFLOW_TRIPLE_PATTERN.match(self.workflow)
        ):
            raise ValueError(
                f"step {self.id!r}: workflow reference must be a "
                "workflowVersionId UUID or '<workspace>/<name>@<version>'"
            )
        return self


class WaitStep(_StepCommon):
    """Durable sleep step: pause the run for the configured ISO-8601 duration.

    Per design.md § Workflow Schema — ``Wait / sleep`` is the one step
    kind the Run Controller handles directly via a Dapr durable timer;
    no Step Coordinator handler is involved (the orchestrator dispatches
    inline). The duration is a constant string, not a CEL expression,
    so the durable-timer payload is fully resolved at compile time and
    survives a worker restart byte-identically on replay.

    ``retry:`` and ``on_error:`` are inherited from :class:`_StepCommon`
    for parsing uniformity but the compiler rejects them on wait steps
    (the durable timer is non-failing by definition — design.md §
    Where ``retry:`` may appear).
    """

    wait: str = Field(min_length=2)

    @model_validator(mode="after")
    def _wait_duration_shape(self) -> WaitStep:
        if _CEL_TOKEN_PATTERN.match(self.wait):
            raise ValueError(
                f"step {self.id!r}: 'wait:' must be a constant ISO-8601 "
                "duration string, not a CEL expression — the durable "
                "timer payload is resolved at compile time"
            )
        match = _ISO8601_DURATION_PATTERN.match(self.wait)
        if match is None:
            raise ValueError(
                f"step {self.id!r}: 'wait:' {self.wait!r} is not a valid "
                "ISO-8601 duration (expected 'P[nD][T[nH][nM][nS]]' or 'PnW')"
            )
        # The regex permits structurally-empty shapes (``P``, ``PT``)
        # and zero values (``PT0S``, ``P0D``) because each component
        # is optional. Reject them here so publish-time validation
        # matches the runtime guarantee: a durable timer requires a
        # positive duration, anything else is a configuration bug.
        weeks = int(match.group("weeks") or 0)
        days = int(match.group("days") or 0)
        hours = int(match.group("hours") or 0)
        minutes = int(match.group("minutes") or 0)
        seconds = float(match.group("seconds") or 0.0)
        if weeks == 0 and days == 0 and hours == 0 and minutes == 0 and seconds == 0.0:
            raise ValueError(
                f"step {self.id!r}: 'wait:' {self.wait!r} must specify a "
                "positive duration (at least one non-zero component)"
            )
        return self


def _step_discriminator(v: Any) -> str | None:
    """Pick the step kind by keyword presence (no ``kind:`` field).

    Returns the tag for the present kind, or ``None`` when the input
    is ambiguous or missing — Pydantic translates ``None`` into a
    clean :class:`ValidationError` with the union's tag list. A
    friendlier diagnostic ("exactly one of…") will be reinstated by
    the error-taxonomy work in WF-IMPL-024.
    """
    if isinstance(v, dict):
        present = [k for k in ("activity", "let", "workflow", "wait") if k in v]
        if len(present) == 1:
            return present[0]
        return None
    # Already-constructed model instance (e.g. round-tripping).
    if isinstance(v, ActivityStep):
        return "activity"
    if isinstance(v, LetStep):
        return "let"
    if isinstance(v, WorkflowStep):
        return "workflow"
    if isinstance(v, WaitStep):
        return "wait"
    return None


#: Discriminated union over the four step kinds. The Catalog schema
#: enforces the same ``oneOf`` shape; we mirror it so a mistyped
#: workflow document fails at parse time with a precise error
#: pointing at the offending branch.
Step = Annotated[
    Annotated[ActivityStep, Tag("activity")]
    | Annotated[LetStep, Tag("let")]
    | Annotated[WorkflowStep, Tag("workflow")]
    | Annotated[WaitStep, Tag("wait")],
    Discriminator(_step_discriminator),
]


# ---------------------------------------------------------------------------
# Spec + root
# ---------------------------------------------------------------------------


class WorkflowSpec(_StrictModel):
    """``spec`` block of a WorkflowDocument."""

    inputs: dict[str, InputDefinition] | None = None
    defaults: Defaults | None = None
    triggers: list[Trigger] | None = None
    steps: list[Step] = Field(min_length=1)
    on_error: list[OnErrorArm] | None = None

    @model_validator(mode="after")
    def _step_ids_unique(self) -> WorkflowSpec:
        seen: set[str] = set()
        for step in self.steps:
            if step.id in seen:
                raise ValueError(f"duplicate step id: {step.id!r}")
            seen.add(step.id)
        return self


class Metadata(_StrictModel):
    """``metadata`` block of a WorkflowDocument."""

    name: str = Field(pattern=_NAME_PATTERN.pattern)
    workspace: str | None = Field(default=None, pattern=_NAME_PATTERN.pattern)
    description: str | None = None
    labels: dict[str, str] | None = None


class WorkflowDocument(_StrictModel):
    """The root WorkflowDocument."""

    api_version: Literal["custos.dev/v1"] = Field(alias="apiVersion")
    kind: Literal["Workflow"]
    metadata: Metadata
    spec: WorkflowSpec

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
    )
