"""Platform-default retry policy (layer 4 in the overlay chain).

The defaults below come from
``design/components/workflow-service/design.md`` § Retry Policy
→ § Precedence (Platform defaults). They are the values the
Step Coordinator falls back to when every more-specific layer
(per-match arm, step-level, ``spec.defaults.retry``) leaves a
field unset.

The constant uses the **document** model
(:class:`~custos_workflow.document.RetryPolicy`) rather than the
runtime :class:`~custos_workflow.graph.ResolvedRetryPolicy` so the
overlay code can treat platform defaults exactly the same way it
treats every other layer — every field is the optional Pydantic
shape, and the overlay folds layer-by-layer.
"""

from __future__ import annotations

from custos_workflow.document import BackoffPolicy, BackoffStrategy, JitterStrategy, RetryPolicy

#: Platform-default retry policy. Used as the lowest-priority
#: layer in the overlay chain — any field not set by the
#: per-match arm, step-level, or ``spec.defaults.retry`` layers
#: comes from here. The values mirror design.md § Retry Policy
#: → § Precedence verbatim.
PLATFORM_RETRY_DEFAULTS: RetryPolicy = RetryPolicy(
    maxAttempts=3,
    backoff=BackoffPolicy(
        strategy=BackoffStrategy.EXPONENTIAL,
        initialDelay="PT1S",
        maxDelay="PT5M",
        multiplier=2.0,
    ),
    jitter=JitterStrategy.FULL,
    respectRetryAfter=True,
)
