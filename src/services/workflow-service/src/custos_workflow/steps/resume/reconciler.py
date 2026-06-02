"""Replay reconciler — idempotent re-registration of open mirrors (WF-IMPL-105).

The Dapr Workflow runtime re-drives an orchestrator function from the
top whenever a worker reloads its state (pod restart, host migration,
lease loss). The Trigger Service's resume-subscription map is itself
in-memory and lost on a Trigger Service pod restart, so on every
orchestrator entry the Step Coordinator MUST re-register the run's open
resume subscriptions. :class:`ResumeSubscriptionReplayReconciler` is the
production :class:`~custos_workflow.runs.replay.ReplayReconciler` that
does this — bound to the orchestrator's
:data:`~custos_workflow.runs.orchestrator.ReplayHook` slot (the wiring
into ``providers.py`` lands in WF-IMPL-108).

``design.md`` § *Resume Subscription Replay Protocol* pins the
behaviour this reconciler implements:

* **Idempotency** (rule 1): ``list_open(runId)`` then re-register each
  mirror. ``RegisterResumeSubscription`` is idempotent on
  ``(runId, stepId, eventKey)`` — a re-registration of an identical key
  returns the existing ``subscriptionId`` rather than creating a
  duplicate.
* **Divergence policy** (rule 2): the ``selector`` is re-evaluated from
  the compiled graph against the replayed scope and compared to the
  value persisted on the original
  :class:`~custos_workflow.steps.resume.ResumeSubscriptionMirror`. If
  they differ the **original wins** — the reconciler re-registers with
  the original selector and emits a
  ``step.resume_subscription_divergent`` audit event so the divergence
  (a workflow-definition / evaluator bug, because Dapr replay must be
  deterministic) is observable.
* **TTL expiry** (rule 3 / rule 4): if the Trigger Service returns a
  different ``subscriptionId`` than the one on the mirror (e.g. the old
  subscription was garbage-collected on ``expiresAt`` and the
  re-registration minted a fresh one), the reconciler updates the
  mirror row to point at the new id.

Async core, sync hook
---------------------

The orchestrator's :data:`ReplayHook` is **synchronous** and MUST NOT
raise — it fires inside the Dapr Workflow orchestrator generator (a
sync worker thread), and any exception propagates into Dapr's
non-determinism handling and aborts the run. The
:class:`ResumeSubscriptionMirrorRepository` and the audit publisher are
both ``async`` (the production adapters await a store / HTTP boundary),
so the reconciler is split:

* :meth:`~ResumeSubscriptionReplayReconciler.reconcile` — the ``async``
  core that does the real work and returns a
  :class:`ReplayReconcileReport`. Directly unit-tested.
* :meth:`~ResumeSubscriptionReplayReconciler.on_replay` — the sync
  :class:`~custos_workflow.runs.replay.ReplayReconciler` Protocol
  method bound to the hook. It drives the async core via
  :func:`asyncio.run` and swallows + logs every exception so a flapping
  reconcile dependency cannot wedge the orchestrator.

The ``step.resume_subscription_divergent`` event is emitted through the
dedicated :class:`ResumeSubscriptionAuditPublisher` rather than the
locked ``StepLifecyclePublisher`` taxonomy: divergence is a
reconcile-time audit signal, not a per-attempt step lifecycle
transition. The production wire transport for the audit publisher is
wired in WF-IMPL-108; the in-process default
:class:`NoopResumeSubscriptionAuditPublisher` drops the event.

Acceptance criteria (mirrored from #544):

* Re-register of an identical key returns the existing id (no
  duplicate).
* A divergent selector keeps the original + emits the audit event.
* A new id after TTL expiry updates the mirror row.
* Coverage ≥ 90 %; ``ruff`` + ``mypy --strict`` clean.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime
from types import MappingProxyType
from typing import Any, Protocol, runtime_checkable

from custos_cel.scope import BindingScope

from custos_workflow.clients.trigger import (
    RegisterResumeSubscriptionRequest,
    TriggerServiceClient,
)
from custos_workflow.document.models import WaitForStep
from custos_workflow.graph.model import CallSiteKind, ExecutionGraph, ExecutionNode, StepKind
from custos_workflow.runs.step_handler import StepExecutionContext
from custos_workflow.steps.errors import (
    ResumeSubscriptionDivergentError,
    WithInputResolutionError,
)

# The reconciler re-uses the resume sub-module's package-internal
# scope / CEL helpers so a ``${{ inputs.x }}`` selector resolves to the
# exact same value the WaitForStepHandler would resolve it to — sharing
# one definition of the binding scope keeps divergence detection honest.
from custos_workflow.steps.resume.handler import (
    _SELECTOR_SLOT,
    DEFAULT_RESUME_SUB_TTL,
    _build_scope,
    _resolve_cel_string,
)
from custos_workflow.steps.resume.mirror import (
    ResumeSubscriptionMirror,
    ResumeSubscriptionMirrorRepository,
)

__all__ = [
    "NoopResumeSubscriptionAuditPublisher",
    "ReplayReconcileReport",
    "ResumeSubscriptionAuditPublisher",
    "ResumeSubscriptionReplayReconciler",
]

_LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Audit publisher surface
# ---------------------------------------------------------------------------


@runtime_checkable
class ResumeSubscriptionAuditPublisher(Protocol):
    """Sink for the ``step.resume_subscription_divergent`` audit event.

    Kept deliberately separate from the locked
    :class:`~custos_workflow.steps.events.StepLifecyclePublisher`
    taxonomy: a divergent selector is a reconcile-time audit signal the
    replay reconciler emits, not a per-attempt step lifecycle
    transition. The production Dapr Pub/Sub transport is wired in
    WF-IMPL-108; tests and pre-wiring deployments use
    :class:`NoopResumeSubscriptionAuditPublisher`.

    The method is ``async`` because the production transport awaits an
    outgoing publish; the reconciler emits it best-effort (a publish
    failure is logged and swallowed, never allowed to abort the
    reconcile).
    """

    async def emit_resume_subscription_divergent(
        self,
        *,
        workspace_id: str,
        occurred_at: datetime,
        envelope: Mapping[str, Any],
    ) -> None:
        """Publish the ``step.resume_subscription_divergent`` audit event.

        :param workspace_id: The owning workspace.
        :param occurred_at: The replay-deterministic instant the
            divergence was observed.
        :param envelope: The canonical Custos error envelope
            (:meth:`ResumeSubscriptionDivergentError.to_dict`) carrying
            ``kind`` / ``run_id`` / ``step_id`` / ``event_key`` /
            ``original_selector`` / ``replay_selector``.
        """
        ...


@dataclass(frozen=True, slots=True)
class NoopResumeSubscriptionAuditPublisher:
    """Default :class:`ResumeSubscriptionAuditPublisher` — drops the event.

    Frozen + slots so two instances compare equal and the dataclass
    hash works for dependency-injection container de-duplication, the
    same convention :class:`~custos_workflow.runs.replay.NoopReplayReconciler`
    uses.
    """

    async def emit_resume_subscription_divergent(
        self,
        *,
        workspace_id: str,
        occurred_at: datetime,
        envelope: Mapping[str, Any],
    ) -> None:
        """Intentionally drop the event."""
        return None


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ReplayReconcileReport:
    """Structured outcome of one
    :meth:`ResumeSubscriptionReplayReconciler.reconcile` pass.

    Each field is a tuple of ``mirror_id`` values so callers and tests
    can assert exactly which mirrors took which path; the tuples are
    ordered by the repository's deterministic ``list_open`` ordering.

    Attributes:
        reregistered: Every mirror successfully re-registered with the
            Trigger Service this pass.
        divergent: Mirrors whose replayed ``selector`` diverged from the
            persisted one — the original was kept and an audit event was
            emitted (a subset of :attr:`reregistered`).
        mirror_updated: Mirrors whose row was rewritten because the
            Trigger Service returned a fresh ``tsSubscriptionId`` (a
            subset of :attr:`reregistered`).
        failed: Mirrors whose re-registration raised; the error was
            logged and swallowed so the remaining mirrors still
            reconcile.
    """

    reregistered: tuple[str, ...] = field(default_factory=tuple)
    divergent: tuple[str, ...] = field(default_factory=tuple)
    mirror_updated: tuple[str, ...] = field(default_factory=tuple)
    failed: tuple[str, ...] = field(default_factory=tuple)


# ---------------------------------------------------------------------------
# Reconciler
# ---------------------------------------------------------------------------


class ResumeSubscriptionReplayReconciler:
    """Production :class:`~custos_workflow.runs.replay.ReplayReconciler`.

    Re-registers every open :class:`ResumeSubscriptionMirror` for a run
    on each orchestrator entry, applying the
    ``design.md`` § *Resume Subscription Replay Protocol* divergence and
    TTL-expiry rules. Stateless apart from the injected collaborators —
    a single instance is safe to share across runs.

    :param mirror_repo: The
        :class:`ResumeSubscriptionMirrorRepository` the run's open
        subscriptions are read from / written back to.
    :param trigger_client: The
        :class:`~custos_workflow.clients.trigger.TriggerServiceClient`
        each subscription is re-registered through.
    :param default_ttl: TTL applied when a step pins no
        ``waitFor.ttl`` (or its node can no longer be found in the
        graph). Defaults to :data:`DEFAULT_RESUME_SUB_TTL`.
    :param audit_publisher: Sink for the
        ``step.resume_subscription_divergent`` audit event. Defaults to
        :class:`NoopResumeSubscriptionAuditPublisher`.
    """

    __slots__ = ("_audit_publisher", "_default_ttl", "_mirror_repo", "_trigger_client")

    def __init__(
        self,
        mirror_repo: ResumeSubscriptionMirrorRepository,
        trigger_client: TriggerServiceClient,
        *,
        default_ttl: str = DEFAULT_RESUME_SUB_TTL,
        audit_publisher: ResumeSubscriptionAuditPublisher | None = None,
    ) -> None:
        self._mirror_repo = mirror_repo
        self._trigger_client = trigger_client
        self._default_ttl = default_ttl
        self._audit_publisher: ResumeSubscriptionAuditPublisher = (
            audit_publisher
            if audit_publisher is not None
            else NoopResumeSubscriptionAuditPublisher()
        )

    @property
    def mirror_repo(self) -> ResumeSubscriptionMirrorRepository:
        """The repository open subscriptions are reconciled against."""
        return self._mirror_repo

    # ------------------------------------------------------------------
    # ReplayReconciler Protocol — sync hook bound to the orchestrator
    # ------------------------------------------------------------------

    def on_replay(self, ctx: StepExecutionContext, graph: ExecutionGraph) -> None:
        """Drive :meth:`reconcile` from the synchronous orchestrator hook.

        Satisfies the :class:`~custos_workflow.runs.replay.ReplayReconciler`
        Protocol. Runs the async core via :func:`asyncio.run` (the
        orchestrator fires this on a Dapr worker thread with no running
        event loop) and swallows + logs every exception so a flapping
        reconcile dependency cannot wedge the orchestrator — the
        Protocol mandates this method MUST NOT raise.
        """
        try:
            asyncio.run(self.reconcile(ctx, graph))
        except Exception:
            # Boundary swallow: the ReplayReconciler Protocol mandates
            # on_replay MUST NOT raise.
            _LOGGER.exception(
                "resume replay reconcile failed for run %s; swallowed to keep the "
                "orchestrator deterministic",
                ctx.run_id,
            )

    # ------------------------------------------------------------------
    # Async core
    # ------------------------------------------------------------------

    async def reconcile(
        self, ctx: StepExecutionContext, graph: ExecutionGraph
    ) -> ReplayReconcileReport:
        """Re-register every open mirror for ``ctx``'s run.

        Reads the run's open subscriptions, re-registers each
        idempotently, applies the divergence / TTL-expiry policy, and
        returns a :class:`ReplayReconcileReport`. A failure registering
        one mirror is logged and recorded under
        :attr:`ReplayReconcileReport.failed` so the remaining mirrors
        still reconcile.

        :raises Exception: If ``list_open`` itself fails — the caller
            (:meth:`on_replay`) is the no-raise boundary, so this
            surfaces infrastructure failures to it for swallowing.
        """
        run_id = str(ctx.run_id)
        mirrors = await self._mirror_repo.list_open(run_id)
        if not mirrors:
            return ReplayReconcileReport()

        scope = _build_scope(ctx, graph)
        nodes_by_id = {node.step_id: node for node in graph.nodes}
        occurred_at = ctx.workflow_context.current_utc_datetime

        reregistered: list[str] = []
        divergent: list[str] = []
        mirror_updated: list[str] = []
        failed: list[str] = []

        for mirror in mirrors:
            node = nodes_by_id.get(mirror.step_id)
            try:
                was_divergent, was_updated = await self._reconcile_one(
                    ctx, scope, node, mirror, occurred_at
                )
            except Exception:
                # Isolate one mirror's failure so the rest still reconcile.
                _LOGGER.exception(
                    "resume replay reconcile failed for mirror %s (run %s, step %s)",
                    mirror.mirror_id,
                    mirror.run_id,
                    mirror.step_id,
                )
                failed.append(mirror.mirror_id)
                continue
            reregistered.append(mirror.mirror_id)
            if was_divergent:
                divergent.append(mirror.mirror_id)
            if was_updated:
                mirror_updated.append(mirror.mirror_id)

        return ReplayReconcileReport(
            reregistered=tuple(reregistered),
            divergent=tuple(divergent),
            mirror_updated=tuple(mirror_updated),
            failed=tuple(failed),
        )

    # ------------------------------------------------------------------
    # Per-mirror reconciliation
    # ------------------------------------------------------------------

    async def _reconcile_one(
        self,
        ctx: StepExecutionContext,
        scope: BindingScope,
        node: ExecutionNode | None,
        mirror: ResumeSubscriptionMirror,
        occurred_at: datetime,
    ) -> tuple[bool, bool]:
        """Re-register one mirror; return ``(was_divergent, was_updated)``.

        The original selector (the one persisted on ``mirror``) always
        wins on re-registration (Replay Protocol rule 2). Re-registration
        runs *before* the best-effort divergence audit so a flaky audit
        sink can never block the critical idempotent re-registration.
        """
        ttl = self._resolve_ttl(node)
        replay_selector = self._resolve_replay_selector(ctx, scope, node, mirror)
        is_divergent = replay_selector != mirror.selector

        # Original wins: re-register with the persisted selector.
        request = RegisterResumeSubscriptionRequest(
            run_id=mirror.run_id,
            step_id=mirror.step_id,
            event_key=mirror.event_key,
            ttl=ttl,
            selector=mirror.selector,
        )
        response = self._trigger_client.register_resume_subscription(request)

        was_updated = False
        if response.ts_subscription_id != mirror.ts_subscription_id:
            # TTL expiry minted a fresh id (rule 3 / rule 4) — point the
            # mirror row at the new subscription.
            await self._mirror_repo.put(
                ResumeSubscriptionMirror(
                    mirror_id=mirror.mirror_id,
                    run_id=mirror.run_id,
                    step_id=mirror.step_id,
                    event_key=mirror.event_key,
                    ts_subscription_id=response.ts_subscription_id,
                    registered_at=mirror.registered_at,
                    expires_at=mirror.expires_at,
                    selector=mirror.selector,
                )
            )
            was_updated = True

        if is_divergent:
            await self._emit_divergent(ctx, mirror, replay_selector, occurred_at)

        return is_divergent, was_updated

    def _resolve_ttl(self, node: ExecutionNode | None) -> str:
        """Resolve the re-registration TTL from the node, else the default.

        A mirror whose node is gone from the graph (or is no longer a
        ``waitFor:`` step) falls back to the configured default so the
        subscription is still kept alive.
        """
        if node is not None and node.kind is StepKind.WAIT_FOR:
            wait_for_step = node.step_source
            if isinstance(wait_for_step, WaitForStep):
                return wait_for_step.wait_for.ttl or self._default_ttl
        return self._default_ttl

    def _resolve_replay_selector(
        self,
        ctx: StepExecutionContext,
        scope: BindingScope,
        node: ExecutionNode | None,
        mirror: ResumeSubscriptionMirror,
    ) -> str | None:
        """Re-evaluate the step's ``waitFor.selector`` for divergence detection.

        Returns the selector the graph would produce on this replay, or
        the persisted ``mirror.selector`` (treated as *no divergence*)
        when the node cannot be re-evaluated — a missing / non-``waitFor``
        node, or a CEL evaluation failure. Re-evaluation should be
        deterministic; if it is not, the safe choice is to keep the
        original rather than churn the subscription.
        """
        if node is None or node.kind is not StepKind.WAIT_FOR:
            return mirror.selector
        try:
            return _resolve_cel_string(
                node,
                _SELECTOR_SLOT,
                "selector",
                scope,
                ctx.clock,
                mirror.run_id,
                expected_kind=CallSiteKind.WAIT_FOR_SELECTOR,
                required=False,
            )
        except WithInputResolutionError:
            _LOGGER.warning(
                "resume replay reconcile could not re-evaluate selector for mirror "
                "%s (run %s, step %s); keeping the original",
                mirror.mirror_id,
                mirror.run_id,
                mirror.step_id,
            )
            return mirror.selector

    async def _emit_divergent(
        self,
        ctx: StepExecutionContext,
        mirror: ResumeSubscriptionMirror,
        replay_selector: str | None,
        occurred_at: datetime,
    ) -> None:
        """Emit the ``step.resume_subscription_divergent`` audit event (best-effort).

        Builds the canonical envelope from
        :class:`ResumeSubscriptionDivergentError` and publishes it. A
        publish failure is logged and swallowed — the divergence is
        already handled (original kept) and a flaky audit sink must not
        fail the reconcile.
        """
        error = ResumeSubscriptionDivergentError(
            f"resume subscription selector for step {mirror.step_id!r} diverged on "
            "replay; the original registration wins",
            run_id=mirror.run_id,
            step_id=mirror.step_id,
            event_key=mirror.event_key,
            original_selector=mirror.selector,
            replay_selector=replay_selector,
        )
        with suppress(Exception):
            await self._audit_publisher.emit_resume_subscription_divergent(
                workspace_id=ctx.workspace_id,
                occurred_at=occurred_at,
                envelope=MappingProxyType(error.to_dict()),
            )
