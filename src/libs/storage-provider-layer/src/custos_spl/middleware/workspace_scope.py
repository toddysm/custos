"""Workspace-scoping middleware — single chokepoint for tenant isolation.

Wraps a workspace-scoped provider so that every method call is validated
to carry a non-empty `WorkspaceId` as its first non-self argument before
the call reaches the adapter.

The static type system already enforces the *position* of `workspace_id`
(every method on `DefinitionStoreProvider`, `MetadataStoreProvider`, and
`ArtifactStoreProvider` declares it as the first parameter; tests pin
this). This middleware enforces the runtime invariants the type checker
cannot:

  - `workspace_id` is not `None`.
  - `workspace_id` is not the empty string (the `WorkspaceId` `NewType`
    is structurally a `str`, so an empty literal would type-check).
  - Callers cannot bypass the signature with `**kwargs` indirection.

It does **NOT** wrap `CatalogStoreProvider` (platform-wide), nor
`AuthStoreProvider` (exempt — Auth Service is the sole caller and owns
authorization), nor the query facades (`LogQueryProvider`,
`MetricsQueryProvider`, which are already workspace-scoped by signature
but read-only; wrapping is optional and they are typically used
directly).

See `design/components/storage-provider-layer/design.md` § Workspace
Scoping for the contract this enforces.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any, TypeVar

from custos_spl.errors import WorkspaceScopingViolation

T = TypeVar("T")

_WORKSPACE_ID_PARAM = "workspace_id"


def _extract_workspace_id(
    params: list[str],
    args: tuple[object, ...],
    kwargs: dict[str, object],
) -> object:
    """Pull `workspace_id` from either positional args or kwargs.

    `params` is the method's parameter list with `self` already stripped.
    """
    if _WORKSPACE_ID_PARAM in kwargs:
        return kwargs[_WORKSPACE_ID_PARAM]
    try:
        workspace_id_index = params.index(_WORKSPACE_ID_PARAM)
    except ValueError:
        return None
    if workspace_id_index < len(args):
        return args[workspace_id_index]
    return None


def _validate_workspace_id(value: object, method_name: str) -> None:
    if value is None:
        raise WorkspaceScopingViolation(
            f"{method_name}: workspace_id is required but was None"
        )
    if not isinstance(value, str):
        raise WorkspaceScopingViolation(
            f"{method_name}: workspace_id must be a WorkspaceId (str), got "
            f"{type(value).__name__}"
        )
    if value == "":
        raise WorkspaceScopingViolation(
            f"{method_name}: workspace_id must be non-empty"
        )


def _is_workspace_scoped_method(fn: Callable[..., object]) -> bool:
    """A method is workspace-scoped iff its first non-self parameter is `workspace_id`."""
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return False
    params = list(sig.parameters)
    # Bound methods have `self` already stripped; unbound do not. Handle both.
    non_self = [p for p in params if p != "self"]
    return bool(non_self) and non_self[0] == _WORKSPACE_ID_PARAM


def _wrap_async_method(
    fn: Callable[..., Any], method_name: str
) -> Callable[..., Any]:
    async def wrapper(*args: object, **kwargs: object) -> object:
        ws = _extract_workspace_id(
            [p for p in inspect.signature(fn).parameters if p != "self"],
            args,
            kwargs,
        )
        _validate_workspace_id(ws, method_name)
        return await fn(*args, **kwargs)

    wrapper.__wrapped__ = fn  # type: ignore[attr-defined]
    wrapper.__name__ = getattr(fn, "__name__", method_name)
    return wrapper


def _wrap_sync_method(
    fn: Callable[..., Any], method_name: str
) -> Callable[..., Any]:
    """Wrap a plain `def` that returns (e.g.) an `AsyncIterator`.

    The validation happens synchronously at call-site; the returned
    iterator is forwarded unchanged.
    """

    def wrapper(*args: object, **kwargs: object) -> object:
        ws = _extract_workspace_id(
            [p for p in inspect.signature(fn).parameters if p != "self"],
            args,
            kwargs,
        )
        _validate_workspace_id(ws, method_name)
        return fn(*args, **kwargs)

    wrapper.__wrapped__ = fn  # type: ignore[attr-defined]
    wrapper.__name__ = getattr(fn, "__name__", method_name)
    return wrapper


class _WorkspaceScopedProxy:
    """Runtime proxy that validates `workspace_id` on every wrapped call.

    Forwards attribute access to the inner provider. Methods whose first
    non-self parameter is `workspace_id` are wrapped to validate before
    forwarding; all other attributes pass through unchanged so the proxy
    satisfies the same `@runtime_checkable` Protocol as the inner.
    """

    __slots__ = ("_cache", "_inner")

    def __init__(self, inner: object) -> None:
        object.__setattr__(self, "_inner", inner)
        object.__setattr__(self, "_cache", {})

    def __getattr__(self, name: str) -> Any:
        cache: dict[str, Any] = object.__getattribute__(self, "_cache")
        if name in cache:
            return cache[name]
        inner = object.__getattribute__(self, "_inner")
        attr = getattr(inner, name)
        if not callable(attr) or not _is_workspace_scoped_method(attr):
            return attr
        if inspect.iscoroutinefunction(attr):
            wrapped = _wrap_async_method(attr, f"{type(inner).__name__}.{name}")
        else:
            wrapped = _wrap_sync_method(attr, f"{type(inner).__name__}.{name}")
        cache[name] = wrapped
        return wrapped


def wrap_workspace_scoped(provider: T) -> T:
    """Wrap a workspace-scoped provider in the scoping middleware.

    Returns a proxy that conforms to the same Protocol as `provider`
    (every attribute, including `SCHEMA_REVISION`, is forwarded). Every
    method whose first non-self parameter is `workspace_id` is intercepted
    to validate the argument before the call reaches the adapter.

    Apply this to `DefinitionStoreProvider`, `MetadataStoreProvider`, and
    `ArtifactStoreProvider` instances at composition time. Do NOT apply
    to `AuthStoreProvider` (exempt) or `CatalogStoreProvider` (platform-
    wide); applying it is harmless but provides no value.

    Raises:
        WorkspaceScopingViolation: at call time, if a wrapped method is
            invoked without a non-empty `workspace_id`.
    """
    return _WorkspaceScopedProxy(provider)  # type: ignore[return-value]


__all__ = [
    "wrap_workspace_scoped",
]
