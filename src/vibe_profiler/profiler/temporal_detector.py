"""Detect temporal columns and their likely roles."""

from __future__ import annotations

import re

from pyspark.sql import DataFrame
from pyspark.sql.types import DateType, TimestampType, TimestampNTZType

from vibe_profiler.models.temporal import TemporalColumn

# Pattern → role mapping, ordered by specificity.
_ROLE_PATTERNS: list[tuple[str, str]] = [
    (r"valid_from|start_d(at)?e|effective_from|eff_from|begin_d(at)?e", "valid_from"),
    (r"valid_to|end_d(at)?e|effective_to|eff_to|expir(y|ation)_d(at)?e", "valid_to"),
    (r"effective_d(at)?e|eff_d(at)?e|as_of_d(at)?e", "effective_date"),
    (r"load_d(at)?e|load_ts|loaded_at|ingestion_d(at)?e|etl_d(at)?e", "load_date"),
    (r"snapshot_d(at)?e|snap_d(at)?e|batch_d(at)?e", "snapshot_date"),
    (r"created_(at|d(at)?e|ts|on)|insert_d(at)?e", "created_date"),
    (r"updated_(at|d(at)?e|ts|on)|modified_(at|d(at)?e)|last_change", "updated_date"),
]

_TEMPORAL_TYPES = (DateType, TimestampType, TimestampNTZType)


def detect_temporal_columns(
    df: DataFrame,
    table_name: str,
) -> tuple[TemporalColumn, ...]:
    """Identify temporal columns and assign roles based on name + type heuristics."""
    results: list[TemporalColumn] = []

    for field in df.schema.fields:
        is_temporal_type = isinstance(field.dataType, _TEMPORAL_TYPES)
        col_lower = field.name.lower()

        matched_role: str | None = None
        confidence = 0.0

        for pattern, role in _ROLE_PATTERNS:
            if re.search(pattern, col_lower):
                matched_role = role
                confidence = 0.9 if is_temporal_type else 0.5
                break

        if matched_role is None and is_temporal_type:
            # Temporal type but no name match — mark as generic
            if any(kw in col_lower for kw in ("date", "time", "ts", "dt", "_at")):
                matched_role = "unknown_temporal"
                confidence = 0.4

        if matched_role is not None:
            results.append(
                TemporalColumn(
                    table_name=table_name,
                    column_name=field.name,
                    role=matched_role,
                    confidence=confidence,
                )
            )

    return tuple(results)
