"""Validate authored templates and resolve event-root inputs without mutating configuration."""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Mapping
from datetime import datetime
from typing import Any

from custos_cel import CelError, FixedClock
from custos_cel.placeholders import extract_placeholders
from pydantic import TypeAdapter

from custos_trigger.events import NormalizedEvent
from custos_trigger.models import SourceType
from custos_trigger.selector import SelectorEvaluator, SelectorInvalidError
from custos_trigger.source_config import SourceConfigurationError

logger = logging.getLogger(__name__)
_JSON_MAPPING = TypeAdapter(dict[str, Any])
EVENT_MAPPED_SOURCES = frozenset(
    {SourceType.SCHEDULED, SourceType.WEBHOOK, SourceType.VENDOR_PUSH, SourceType.PULL}
)


def _transform_strings(value: Any, transform: Callable[[str], Any], depth: int = 0) -> Any:
    """Walk bounded authored containers once; never recursively reinterpret transformed results."""
    if depth > 64:
        raise ValueError("input mapping nesting exceeds 64 levels")
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise ValueError("input mapping keys must be strings")
        return {
            key: _transform_strings(child, transform, depth + 1) for key, child in value.items()
        }
    if isinstance(value, list):
        return [_transform_strings(child, transform, depth + 1) for child in value]
    return transform(value) if isinstance(value, str) else value


def validate_input_mapping(
    mapping: Mapping[str, Any],
    *,
    evaluator: SelectorEvaluator,
    subscription_id: str,
    source_type: SourceType,
) -> None:
    """Parse/type-check every authored placeholder without requiring or evaluating an event."""
    try:

        def validate(value: str) -> str:
            """Compile each placeholder but preserve its original authored text."""
            for segment in extract_placeholders(value):
                evaluator.compile(
                    segment.inner.strip(),
                    subscription_id=subscription_id,
                    source_type=source_type,
                )
            return value

        authored = _transform_strings(mapping, validate)
        json.dumps(_JSON_MAPPING.dump_python(authored, mode="json"), allow_nan=False)
    except (CelError, SelectorInvalidError, ValueError, TypeError) as exc:
        logger.warning(
            "Input mapping validation failed for %s: %s", subscription_id, type(exc).__name__
        )
        raise SourceConfigurationError("input mapping could not be validated safely") from exc


def resolve_input_mapping(
    mapping: Mapping[str, Any],
    event: NormalizedEvent,
    *,
    evaluator: SelectorEvaluator,
    subscription_id: str,
    source_type: SourceType | None = None,
) -> dict[str, Any]:
    """Resolve against the event clock, optionally retaining an explicitly fired source's schema."""
    try:
        instant = datetime.fromisoformat(event.source.occurred_at)
        if instant.utcoffset() is None:
            raise ValueError("input mapping requires an aware event instant")
        clock = FixedClock(instant)

        def resolve(value: str) -> Any:
            """Resolve one authored string while retaining whole-placeholder value types."""
            segments = extract_placeholders(value)
            if not segments:
                return value
            results = [
                evaluator.evaluate_value(
                    evaluator.compile(
                        segment.inner.strip(),
                        subscription_id=subscription_id,
                        source_type=source_type if source_type is not None else event.source.type,
                    ),
                    event,
                    clock=clock,
                )
                for segment in segments
            ]
            if len(segments) == 1 and value.strip() == segments[0].token:
                return results[0]
            parts: list[str] = []
            offset = 0
            for segment, result in zip(segments, results, strict=True):
                parts.extend((value[offset : segment.start], str(result)))
                offset = segment.end
            parts.append(value[offset:])
            return "".join(parts)

        resolved = _JSON_MAPPING.dump_python(_transform_strings(mapping, resolve), mode="json")
        return _JSON_MAPPING.validate_json(json.dumps(resolved, allow_nan=False))
    except (CelError, SelectorInvalidError, ValueError, TypeError) as exc:
        logger.warning(
            "Input mapping resolution failed for %s: %s",
            subscription_id,
            type(exc).__name__,
        )
        raise SourceConfigurationError("input mapping could not be resolved safely") from exc
