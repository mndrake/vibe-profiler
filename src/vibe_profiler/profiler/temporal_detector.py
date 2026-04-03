"""Detect temporal columns and their likely roles."""

from __future__ import annotations

import re

from pyspark.sql import DataFrame
from pyspark.sql.types import DateType, TimestampType, TimestampNTZType

from vibe_profiler.models.profile import ColumnProfile, PatternType
from vibe_profiler.models.temporal import TemporalColumn

# Pattern → role mapping, ordered by specificity.
# These match against the full lowercased column name.
_ROLE_PATTERNS: list[tuple[str, str]] = [
    # Explicit valid_from / valid_to
    (r"valid_from|effective_from|eff_from|begin_d(at)?e", "valid_from"),
    (r"valid_to|effective_to|eff_to|expir(y|ation)_d(at)?e", "valid_to"),
    # Start / end (covers d_hldg_start, start_date, period_start, etc.)
    (r"_start$|start_d(at)?e|start_dt|start_ts|^start$", "valid_from"),
    (r"_end$|end_d(at)?e|end_dt|end_ts|^end$", "valid_to"),
    # Effective date
    (r"effective_d(at)?e|eff_d(at)?e|as_of_d(at)?e", "effective_date"),
    # Load / ingestion
    (r"load_d(at)?e|load_ts|loaded_at|ingestion_d(at)?e|etl_d(at)?e", "load_date"),
    # Snapshot
    (r"snapshot_d(at)?e|snap_d(at)?e|batch_d(at)?e", "snapshot_date"),
    # Created / updated
    (r"created_(at|d(at)?e|ts|on)|insert_d(at)?e|creat(e|ion)_d(at)?e", "created_date"),
    (r"updated_(at|d(at)?e|ts|on)|modified_(at|d(at)?e)|last_change|upd(t)?_d(at)?e", "updated_date"),
]

_TEMPORAL_TYPES = (DateType, TimestampType, TimestampNTZType)


def detect_temporal_columns(
    df: DataFrame,
    table_name: str,
    column_profiles: tuple[ColumnProfile, ...] | None = None,
) -> tuple[TemporalColumn, ...]:
    """Identify temporal columns and assign roles based on name + type heuristics.

    When *column_profiles* are provided, also considers inferred types and
    date-like patterns to detect temporal string columns (common in raw/landing
    tables where everything is stored as strings).
    """
    # Build a lookup for inferred types and patterns from profiling
    inferred_temporal: set[str] = set()
    date_pattern_cols: set[str] = set()
    if column_profiles:
        for cp in column_profiles:
            if cp.inferred_type and cp.inferred_type.spark_target_type in ("date", "timestamp"):
                inferred_temporal.add(cp.column_name)
            if cp.dominant_pattern == PatternType.DATE_ISO:
                date_pattern_cols.add(cp.column_name)

    results: list[TemporalColumn] = []

    for field in df.schema.fields:
        is_temporal_type = isinstance(field.dataType, _TEMPORAL_TYPES)
        is_inferred_temporal = field.name in inferred_temporal
        is_date_pattern = field.name in date_pattern_cols
        is_temporal = is_temporal_type or is_inferred_temporal or is_date_pattern

        col_lower = field.name.lower()

        matched_role: str | None = None
        confidence = 0.0

        for pattern, role in _ROLE_PATTERNS:
            if re.search(pattern, col_lower):
                matched_role = role
                if is_temporal_type:
                    confidence = 0.9
                elif is_inferred_temporal or is_date_pattern:
                    confidence = 0.75
                else:
                    confidence = 0.5
                break

        # Fallback: temporal type/pattern but no name match
        if matched_role is None and is_temporal:
            if any(kw in col_lower for kw in ("date", "time", "ts", "dt", "_at", "_d_")):
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
