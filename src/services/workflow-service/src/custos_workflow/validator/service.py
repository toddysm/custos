"""``StartRunValidator`` — orchestrator for the API Adapter Validator.

This module implements the orchestrator leg of WF-IMPL-063 (issue
#449). The :class:`StartRunValidator` glues the three other
validator pieces — Catalog lookup, inputs JSON-Schema match,
``(workspaceId, idempotencyKey)`` ledger — into one entry point the
public REST and internal RPC surfaces call exactly once per
``StartRun`` request.

Surface
=======

* :class:`ValidatedStartRun` — the immutable value object the
  validator hands to
  :meth:`custos_workflow.runs.controller.RunController.start_run`
  on the happy path. Carries the resolved :class:`WorkflowVersion`,
  the (normalised) inputs, the fingerprint, and a ``replayed`` flag
  signalling whether the ledger short-circuited the request.
* :class:`StartRunValidator` — the orchestrator. Constructor takes
  the narrow :class:`~custos_workflow.runs.controller.CatalogClient`
  and :class:`~custos_workflow.validator.idempotency_ledger.IdempotencyLedger`
  Protocols so tests inject in-memory fakes. The single async
  :meth:`validate_start_run` method runs the four gates in the
  order documented in design.md § Operation: Start Run.

Gates (in order)
================

1. **Workspace authorization.** If the request arrived with a
   :class:`~custos_workflow.call_context.CallContext` carrying a
   workspace, that workspace must match the path workspace.
   Failure → :class:`WorkspaceUnauthorizedError`.
2. **Catalog lookup.** Fetch the
   :class:`~custos_workflow.runs.controller.WorkflowVersion` from the
   bound :class:`CatalogClient`. ``LookupError`` (and the
   pre-existing :class:`WorkflowVersionNotFoundError`) translate to
   :class:`WorkflowVersionNotFoundError`; every other exception
   propagates unchanged (the API exception handler renders it as
   a 502/503 via the catch-all path).
3. **Inputs schema match.** Derive a JSON Schema from the workflow
   version's typed ``spec.inputs`` block and evaluate the caller's
   payload. Failure → :class:`InputsSchemaError`.
4. **Idempotency ledger.** When the caller supplied a non-empty
   ``idempotency_key``, consult the bound ledger. A replay collapses
   the response; a divergent fingerprint within the TTL window
   surfaces an :class:`IdempotencyConflictError`. When no key was
   supplied, the ledger is skipped and ``replayed`` stays ``False``.

The validator deliberately does **not** call
:meth:`RunController.start_run`. The controller's own six-gate dedup
remains authoritative; the validator's job is to fail fast on the
inputs the controller would otherwise have to reject after a Catalog
round-trip.

See the issue: https://github.com/toddysm/custos/issues/449
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from custos_workflow.validator.errors import (
    WorkflowVersionNotFoundError,
    WorkspaceUnauthorizedError,
)
from custos_workflow.validator.idempotency_ledger import (
    compute_request_fingerprint,
)
from custos_workflow.validator.inputs import (
    derive_inputs_schema,
    validate_inputs_against_schema,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from custos_workflow.call_context import CallContext
    from custos_workflow.runs.controller import CatalogClient, WorkflowVersion
    from custos_workflow.validator.idempotency_ledger import IdempotencyLedger


__all__ = [
    "StartRunValidator",
    "ValidatedStartRun",
]


@dataclass(frozen=True, slots=True)
class ValidatedStartRun:
    """Result of a successful :meth:`StartRunValidator.validate_start_run`.

    Hands the API route everything it needs to call
    :meth:`RunController.start_run` without re-doing any of the
    Validator's work. ``replayed`` lets the route distinguish a
    fresh ``StartRun`` (``False``) from a ledger hit (``True``) so
    it can surface a ``201`` versus ``200`` response status code
    when the routes ship in WF-IMPL-065.

    Attributes:
        workspace_id: The owning workspace.
        workflow_version_id: The Catalog ``WorkflowVersion`` UUID
            the caller requested. Always equal to
            ``workflow_version.id`` on success.
        workflow_version: The resolved
            :class:`~custos_workflow.runs.controller.WorkflowVersion`
            the controller will pass through its compile gate.
        inputs: Normalised ``inputs`` mapping (a ``None`` request
            payload is materialised as an empty ``dict`` so the
            controller's :func:`_fingerprint_inputs` agrees with
            the validator's :func:`compute_request_fingerprint`).
        idempotency_key: The caller-supplied opaque key, or
            ``None`` when the request did not carry one.
        request_fingerprint: Hex-encoded SHA-256 of the
            ``(workflow_version_id, inputs)`` pair, computed by
            :func:`compute_request_fingerprint`. The route passes
            this through to the controller for audit emission.
        replayed: ``True`` if the idempotency ledger replayed an
            existing entry; ``False`` if the ledger minted a new
            one or the caller did not supply a key.
    """

    workspace_id: str
    workflow_version_id: str
    workflow_version: WorkflowVersion
    inputs: dict[str, Any]
    idempotency_key: str | None
    request_fingerprint: str
    replayed: bool


class StartRunValidator:
    """Pre-execution checks gating every ``StartRun`` request.

    Args:
        catalog: A :class:`CatalogClient` Protocol instance — the
            same one the :class:`RunController` is built with.
            Reusing the binding keeps the workflow-service from
            opening a second Catalog connection per request.
        ledger: An :class:`IdempotencyLedger` Protocol instance.
            Production deployments inject the durable adapter;
            tests inject :class:`InMemoryIdempotencyLedger`.

    The constructor is side-effect free. The four gates run
    sequentially inside :meth:`validate_start_run`; the method is
    safe to call concurrently from multiple FastAPI worker tasks
    because each leg's underlying component (Catalog client,
    ledger) is itself responsible for its own concurrency story.
    """

    def __init__(self, *, catalog: CatalogClient, ledger: IdempotencyLedger) -> None:
        self._catalog: CatalogClient = catalog
        self._ledger: IdempotencyLedger = ledger

    async def validate_start_run(
        self,
        *,
        workspace_id: str,
        workflow_version_id: str,
        inputs: Mapping[str, Any] | None = None,
        idempotency_key: str | None = None,
        call_context: CallContext | None = None,
    ) -> ValidatedStartRun:
        """Run the four validator gates against a ``StartRun`` request.

        Args:
            workspace_id: The path workspace the caller targeted.
                Must be non-empty.
            workflow_version_id: The Catalog ``WorkflowVersion``
                UUID the caller requested. Must be non-empty.
            inputs: The caller-supplied ``inputs`` mapping, or
                ``None`` (treated as ``{}``).
            idempotency_key: The caller-supplied opaque key, or
                ``None``. Empty strings are coerced to ``None`` so
                the ledger is skipped — mirrors the controller's
                contract for "no key supplied".
            call_context: The per-request
                :class:`~custos_workflow.call_context.CallContext`
                lifted off ``request.state``. ``None`` is permitted
                so non-FastAPI callers (and the in-process tests
                here) can drive the validator without standing up
                a Starlette request scope; in that mode the
                workspace authorization gate is a no-op.

        Returns:
            A :class:`ValidatedStartRun` carrying the resolved
            workflow version, normalised inputs, fingerprint, and
            replay flag.

        Raises:
            ValueError: ``workspace_id`` or ``workflow_version_id``
                is empty.
            WorkspaceUnauthorizedError: The
                :class:`CallContext` workspace disagrees with the
                path workspace.
            WorkflowVersionNotFoundError: Catalog returned a
                ``LookupError`` (or
                :class:`WorkflowVersionNotFoundError`) for the
                requested ``workflow_version_id``.
            InputsSchemaError: The payload failed the workflow's
                inputs schema.
            IdempotencyConflictError: A live ledger entry for
                ``(workspace_id, idempotency_key)`` carries a
                different request fingerprint.
        """
        if not workspace_id:
            raise ValueError("workspace_id must be non-empty")
        if not workflow_version_id:
            raise ValueError("workflow_version_id must be non-empty")

        normalised_key = idempotency_key or None  # coerce ``""`` to ``None``
        normalised_inputs: dict[str, Any] = dict(inputs or {})

        # ---- Gate 1: workspace authorization ----------------------------
        if (
            call_context is not None
            and call_context.workspace is not None
            and call_context.workspace != workspace_id
        ):
            raise WorkspaceUnauthorizedError(
                (
                    f"call-context workspace "
                    f"{call_context.workspace!r} is not authorized "
                    f"for path workspace {workspace_id!r}"
                ),
                workspace_id=workspace_id,
                principal=call_context.principal,
            )

        # ---- Gate 2: Catalog lookup -------------------------------------
        try:
            workflow_version = await self._catalog.get_workflow_version(
                workspace_id, workflow_version_id
            )
        except WorkflowVersionNotFoundError:
            raise
        except LookupError as exc:
            raise WorkflowVersionNotFoundError(
                (
                    f"workflow version {workflow_version_id!r} not found in "
                    f"workspace {workspace_id!r}"
                ),
                workspace_id=workspace_id,
                workflow_id=None,
                workflow_version=workflow_version_id,
            ) from exc

        # ---- Gate 3: inputs schema match --------------------------------
        schema = derive_inputs_schema(workflow_version.document.spec.inputs)
        validate_inputs_against_schema(
            normalised_inputs,
            schema,
            workspace_id=workspace_id,
        )

        # ---- Gate 4: idempotency ledger ---------------------------------
        fingerprint = compute_request_fingerprint(workflow_version_id, normalised_inputs)
        replayed = False
        if normalised_key is not None:
            entry = await self._ledger.record_or_replay(
                workspace_id=workspace_id,
                idempotency_key=normalised_key,
                request_fingerprint=fingerprint,
            )
            replayed = entry.replayed

        return ValidatedStartRun(
            workspace_id=workspace_id,
            workflow_version_id=workflow_version_id,
            workflow_version=workflow_version,
            inputs=normalised_inputs,
            idempotency_key=normalised_key,
            request_fingerprint=fingerprint,
            replayed=replayed,
        )
