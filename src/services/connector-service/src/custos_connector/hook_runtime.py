"""In-cluster hook wiring with per-invocation API ownership and explicit readiness checks."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from typing import Any

from kubernetes_asyncio import client

from custos_connector.hook_images import RegistryHookImageResolver
from custos_connector.hook_kubernetes_transport import KubernetesHookTransport
from custos_connector.hook_sandbox import HookSandboxSettings
from custos_connector.kubernetes_hooks import KubernetesHookRunner
from custos_connector.runtime import HookRunResult, PluginCleanupFailed, _finish_owned_task

logger = logging.getLogger(__name__)


class InClusterHookRunner:
    """Share registry metadata while independently owning each invocation's API client."""

    def __init__(
        self,
        configuration: client.Configuration,
        resolver: RegistryHookImageResolver,
        settings: HookSandboxSettings,
    ) -> None:
        """Bind explicit in-cluster credentials without opening a session or running a hook."""
        self.configuration, self.resolver, self.settings = configuration, resolver, settings

    async def check_ready(self) -> None:
        """Check required namespaced authority without creating a plugin or reading any payload."""
        async with asyncio.timeout(15), client.ApiClient(self.configuration) as api:
            core = client.CoreV1Api(api)
            await core.list_namespaced_pod(self.settings.namespace, limit=1, _request_timeout=5)
            await core.read_namespaced_service_account(
                self.settings.service_account,
                self.settings.namespace,
                _request_timeout=5,
            )
            authorization = client.AuthorizationV1Api(api)
            for resource, subresource, verbs in (
                ("pods", "", ("create", "get", "list", "delete", "patch")),
                ("pods", "exec", ("get", "create")),
            ):
                for verb in verbs:
                    review = await authorization.create_self_subject_access_review(
                        {
                            "apiVersion": "authorization.k8s.io/v1",
                            "kind": "SelfSubjectAccessReview",
                            "spec": {
                                "resourceAttributes": {
                                    "namespace": self.settings.namespace,
                                    "verb": verb,
                                    "group": "",
                                    "resource": resource,
                                    "subresource": subresource,
                                }
                            },
                        },
                        _request_timeout=5,
                    )
                    if review.status is None or review.status.allowed is not True:
                        raise RuntimeError("Kubernetes hook controller lacks required permissions")

    async def run(
        self,
        *,
        image_ref: str,
        hook: str,
        payload: Mapping[str, Any],
        timeout_seconds: float,
        invocation_id: str | None = None,
    ) -> HookRunResult:
        """Close the invocation's API session only after owned execution cleanup has settled."""
        api = client.ApiClient(self.configuration)
        pending_failure: PluginCleanupFailed | None = None
        try:
            runner = KubernetesHookRunner(
                KubernetesHookTransport(
                    client.CoreV1Api(api), self.configuration, self.settings.namespace
                ),
                self.resolver,
                self.settings,
            )
            return await runner.run(
                image_ref=image_ref,
                hook=hook,
                payload=payload,
                timeout_seconds=timeout_seconds,
                invocation_id=invocation_id,
            )
        except PluginCleanupFailed as failure:
            pending_failure = failure
            raise
        finally:
            outcomes, interrupted = await _finish_owned_task(
                asyncio.gather(api.close(), return_exceptions=True),
                invocation_id or "hook-api-session",
            )
            close_failure = outcomes[0]
            if pending_failure is not None:
                caller = asyncio.current_task()
                pending_failure.cancellation_requested |= (
                    interrupted
                    or isinstance(close_failure, asyncio.CancelledError)
                    or (caller is not None and caller.cancelling() > 0)
                )
            if isinstance(close_failure, BaseException):
                logger.error("Hook API client cleanup failed: %s", type(close_failure).__name__)
                if pending_failure is not None:
                    raise pending_failure from close_failure
                raise close_failure
            if interrupted and pending_failure is None:
                raise asyncio.CancelledError()
