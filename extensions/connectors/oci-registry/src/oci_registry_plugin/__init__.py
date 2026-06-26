"""Reference Custos OCI-registry connector plugin (``custos-oci-registry@1.0.0``).

The package exposes a single module-level entry point — :func:`handle` —
that maps a v1 plugin request envelope onto the matching hook handler
and returns the response payload the runtime expects.

The plugin is intentionally **transport-agnostic**: it does not import
``httpx``, ``oras``, or any registry SDK at import time. The hook
handlers stub out the upstream interaction with deterministic, audit-
quality responses so the integration suite can drive the plugin without
needing live cloud credentials. Real plugin implementations replace the
stub bodies with their upstream calls; the JSON envelope shape and the
error taxonomy are identical.
"""

from __future__ import annotations

from .plugin import handle

__all__ = ["handle"]
__version__ = "1.0.0"
