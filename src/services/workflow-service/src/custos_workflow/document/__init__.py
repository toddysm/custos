"""Workflow Service ``WorkflowDocument`` Pydantic models (WF-IMPL-016).

This subpackage owns the typed Python view of the YAML document body
stored in ``WorkflowVersion.document``. The Catalog Service validates
the on-the-wire schema at publish time (CS-IMPL-005 / CS-IMPL-006);
:func:`parse_document` is a defensive contract re-check the Definition
Compiler runs at ``StartRun`` so a tampered or schema-skewed document
fails loudly before any orchestration state is created.

Public exports:

- :class:`WorkflowDocument` and its nested models.
- :func:`parse_document` (YAML text → typed model).
- :class:`DocumentParseError` (raised on any parse / validation
  failure; a placeholder until WF-IMPL-024 lands the full Workflow
  Service error taxonomy and this is split into ``YamlSyntaxError`` /
  ``SchemaMismatchError`` subclasses).
- :data:`CelSource` — newtype around ``str`` for CEL expression slots.

Implementation notes:

- Pydantic v2 ``ConfigDict(extra="forbid")`` mirrors the Catalog
  schema's ``additionalProperties: false`` so unknown keys fail at the
  same boundary they fail at publish time.
- The :class:`Step` union is discriminated by **presence** of the
  ``activity`` / ``let`` / ``workflow`` / ``wait`` / ``approval``
  keyword (not by a ``kind:`` field, because the YAML contract has
  no such field).
  Pydantic v2's ``Discriminator(callable)`` makes this clean.
- CEL expression strings are preserved verbatim as :data:`CelSource`
  instances. Parsing into ASTs is the call-site collector's job
  (WF-IMPL-020).
"""

from __future__ import annotations

from custos_workflow.document.loader import DocumentParseError, parse_document
from custos_workflow.document.models import (
    ActivityStep,
    ApprovalSpec,
    ApprovalStep,
    BackoffPolicy,
    BackoffStrategy,
    CelSource,
    Defaults,
    InputDefinition,
    JitterStrategy,
    LetStep,
    Metadata,
    OnErrorAction,
    OnErrorArm,
    OnErrorMatch,
    RetryPolicy,
    Step,
    Trigger,
    WaitStep,
    WorkflowDocument,
    WorkflowSpec,
    WorkflowStep,
)

__all__ = [
    "ActivityStep",
    "ApprovalSpec",
    "ApprovalStep",
    "BackoffPolicy",
    "BackoffStrategy",
    "CelSource",
    "Defaults",
    "DocumentParseError",
    "InputDefinition",
    "JitterStrategy",
    "LetStep",
    "Metadata",
    "OnErrorAction",
    "OnErrorArm",
    "OnErrorMatch",
    "RetryPolicy",
    "Step",
    "Trigger",
    "WaitStep",
    "WorkflowDocument",
    "WorkflowSpec",
    "WorkflowStep",
    "parse_document",
]
