"""Own one Kubernetes hook invocation without adding a polling or replay authority.

The static bridge is inert until private exec admission. Once exec might have
been accepted, only observed termination of the owned UID can release uncertainty;
resource disappearance is not proof that an executor on a lost node stopped.
"""

from __future__ import annotations

import asyncio
import logging
import math
from collections.abc import Mapping
from typing import Any
from uuid import uuid4

from custos_connector.hook_sandbox import (
    HookImageResolver,
    HookPod,
    HookSandboxSettings,
    HookSandboxTransport,
    build_hook_pod,
    decode_hook_reply,
    encode_hook_request,
)
from custos_connector.runtime import (
    HookRunResult,
    PluginCleanupFailed,
    PluginInvocationFailed,
    PluginProtocolError,
    _finish_owned_task,
    invocation_container_name,
)

logger = logging.getLogger(__name__)


class KubernetesHookRunner:
    """Caller-owned Kubernetes collaborators; execution authority remains in the existing fence."""

    def __init__(
        self,
        transport: HookSandboxTransport,
        resolver: HookImageResolver,
        settings: HookSandboxSettings,
    ) -> None:
        """Bind explicit deployment identity without opening sockets or launching work."""
        self.transport, self.resolver, self.settings = transport, resolver, settings

    def _uncertain(self, name: str) -> PluginCleanupFailed:
        """Return a safe operator locator without copying request or backend diagnostics."""
        failure = PluginCleanupFailed(f"{self.settings.namespace}/{name}")
        caller = asyncio.current_task()
        failure.cancellation_requested = caller is not None and caller.cancelling() > 0
        return failure

    @staticmethod
    def _require_owner(current: HookPod, owned: HookPod) -> None:
        """Reject name reuse, changed admission or a different creation attempt."""
        if (
            current.name != owned.name
            or current.uid != owned.uid
            or current.creation_nonce != owned.creation_nonce
        ):
            raise PluginCleanupFailed(owned.name)

    async def _runnable(self, owned: HookPod) -> HookPod:
        """Wait for the inert container, never executing a changed or already-terminal Pod."""
        while True:
            current = await self.transport.observe(owned.name)
            if current is None:
                raise PluginInvocationFailed("hook sandbox disappeared before execution")
            self._require_owner(current, owned)
            if not current.spec_verified:
                raise PluginInvocationFailed("hook sandbox admission changed its security contract")
            if current.terminated:
                raise PluginInvocationFailed("hook sandbox terminated before execution")
            if current.running:
                return current
            await asyncio.sleep(0.1)

    async def _cleanup(self, owned: HookPod, *, exec_possible: bool, retain_evidence: bool) -> None:
        """Classify every observed cleanup failure without losing its uncertainty fence."""
        outcomes = await asyncio.gather(
            self._cleanup_owned(
                owned, exec_possible=exec_possible, retain_evidence=retain_evidence
            ),
            return_exceptions=True,
        )
        failure = outcomes[0]
        if isinstance(failure, PluginCleanupFailed):
            raise failure
        if isinstance(failure, BaseException):
            logger.error("Hook sandbox cleanup is unconfirmed: %s", type(failure).__name__)
            raise self._uncertain(owned.name) from failure

    async def _cleanup_owned(
        self, owned: HookPod, *, exec_possible: bool, retain_evidence: bool
    ) -> None:
        """Retain the finalizer until positive termination or proof of no exec admission."""
        async with asyncio.timeout(self.settings.cleanup_seconds):
            await self.transport.stop(owned)
            while True:
                current = await self.transport.observe(owned.name)
                if current is None:
                    if exec_possible or not owned.spec_verified:
                        raise self._uncertain(owned.name)
                    return
                self._require_owner(current, owned)
                if current.spec_verified and (current.terminated or not exec_possible):
                    break
                await asyncio.sleep(0.1)
            if retain_evidence:
                return
            await self.transport.release(current)
            while True:
                current = await self.transport.observe(owned.name)
                if current is None:
                    return
                self._require_owner(current, owned)
                await asyncio.sleep(0.1)

    async def run(
        self,
        *,
        image_ref: str,
        hook: str,
        payload: Mapping[str, Any],
        timeout_seconds: float,
        invocation_id: str | None = None,
    ) -> HookRunResult:
        """Return only after owned cleanup; cancellation never detaches a live exec operation."""
        if not math.isfinite(timeout_seconds) or not 0 < timeout_seconds <= 300:
            raise ValueError("hook deadline must be positive and at most 300 seconds")
        name = invocation_container_name(invocation_id or uuid4().hex)
        nonce = uuid4().hex
        owned: HookPod | None = None
        creating = False
        exec_possible = False
        outcome_known = False
        retain_evidence = False
        try:
            async with asyncio.timeout(timeout_seconds):
                image = await self.resolver.resolve(image_ref)
                request = encode_hook_request(nonce, image, hook, payload, timeout_seconds)
                manifest = build_hook_pod(self.settings, image, name, nonce, timeout_seconds)
                creating = True
                outcomes = await asyncio.gather(
                    self.transport.create(manifest),
                    return_exceptions=True,
                )
                candidate = outcomes[0]
                if isinstance(candidate, PluginInvocationFailed):
                    raise candidate
                if isinstance(candidate, BaseException):
                    logger.error(
                        "Hook creation outcome is unconfirmed: %s", type(candidate).__name__
                    )
                    raise self._uncertain(name) from candidate
                if not isinstance(candidate, HookPod):
                    raise self._uncertain(name)
                creating = False
                if candidate.name != name or candidate.creation_nonce != nonce or not candidate.uid:
                    raise self._uncertain(name)
                owned = candidate
                owned = await self._runnable(owned)
                exec_possible = True
                replies = await asyncio.gather(
                    self.transport.execute(owned, request),
                    return_exceptions=True,
                )
                response = replies[0]
                if isinstance(response, BaseException):
                    retain_evidence = True
                    logger.error(
                        "Hook execution outcome is unconfirmed: %s", type(response).__name__
                    )
                    raise self._uncertain(name) from None
                try:
                    result = decode_hook_reply(response)
                except PluginProtocolError:
                    retain_evidence = True
                    raise self._uncertain(name) from None
                except (PluginInvocationFailed, TimeoutError):
                    outcome_known = True
                    raise
                outcome_known = True
        except asyncio.CancelledError as exc:
            if creating or owned is not None:
                retain_evidence = True
                raise self._uncertain(name) from exc
            raise
        except (TimeoutError, OSError) as exc:
            if creating or (exec_possible and not outcome_known):
                retain_evidence = True
                raise self._uncertain(name) from exc
            raise
        finally:
            if owned is not None:
                cleanup = asyncio.create_task(
                    self._cleanup(
                        owned,
                        exec_possible=exec_possible,
                        retain_evidence=retain_evidence,
                    )
                )
                try:
                    _, interrupted = await _finish_owned_task(cleanup, name)
                except PluginCleanupFailed as failure:
                    caller = asyncio.current_task()
                    failure.cancellation_requested = caller is not None and caller.cancelling() > 0
                    raise
                if interrupted:
                    if retain_evidence:
                        raise self._uncertain(name) from None
                    raise asyncio.CancelledError()
        return result
