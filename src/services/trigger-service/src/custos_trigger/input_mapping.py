"""Resolve event-root input templates without mutating subscription configuration."""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from custos_cel import CelError, FixedClock
from custos_cel.placeholders import extract_placeholders
from pydantic import TypeAdapter

from custos_trigger.events import NormalizedEvent
from custos_trigger.selector import SelectorEvaluator, SelectorInvalidError
from custos_trigger.source_config import SourceConfigurationError

logger = logging.getLogger(__name__)
_JSON_MAPPING = TypeAdapter(dict[str, Any])


def resolve_input_mapping(
    mapping: Mapping[str, Any],
    event: NormalizedEvent,
    *,
    evaluator: SelectorEvaluator,
    subscription_id: str,
) -> dict[str, Any]:
    """Resolve templates against the occurrence clock and return detached JSON values."""
    try:
        instant = datetime.fromisoformat(event.source.occurred_at)
        if instant.utcoffset() is None:
            raise ValueError("input mapping requires an aware event instant")
        clock = FixedClock(instant)

        def resolve(value: Any, depth: int = 0) -> Any:
            """Walk authored containers, but never reinterpret expression results as templates."""
            if depth > 64:
                raise ValueError("input mapping nesting exceeds 64 levels")
            if isinstance(value, Mapping):
                if any(not isinstance(key, str) for key in value):
                    raise ValueError("input mapping keys must be strings")
                return {key: resolve(child, depth + 1) for key, child in value.items()}
            if isinstance(value, list):
                return [resolve(child, depth + 1) for child in value]
            if not isinstance(value, str):
                return value
            segments = extract_placeholders(value)
            if not segments:
                return value
            results = [
                evaluator.evaluate_value(
                    evaluator.compile(segment.inner.strip(), subscription_id=subscription_id),
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

        resolved = _JSON_MAPPING.dump_python(resolve(mapping), mode="json")
        return _JSON_MAPPING.validate_json(json.dumps(resolved, allow_nan=False))
    except (CelError, SelectorInvalidError, ValueError, TypeError) as exc:
        logger.warning(
            "Input mapping resolution failed for %s: %s",
            subscription_id,
            type(exc).__name__,
        )
        raise SourceConfigurationError("input mapping could not be resolved safely") from exc
