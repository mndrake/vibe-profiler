"""Regex-based pattern classification for string columns."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from vibe_profiler.models.profile import PatternType

# Ordered by specificity — first match wins.
_PATTERNS: list[tuple[PatternType, str]] = [
    (
        PatternType.UUID,
        r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
    ),
    (
        PatternType.EMAIL,
        r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$",
    ),
    (
        PatternType.DATE_ISO,
        r"^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$",
    ),
    (
        PatternType.PHONE,
        r"^[\+]?[\d\s\-\(\)]{7,20}$",
    ),
    (
        PatternType.BOOLEAN,
        r"^(true|false|yes|no|0|1|y|n|t|f)$",
    ),
    (
        PatternType.NUMERIC_CODE,
        r"^\d{2,20}$",
    ),
    (
        PatternType.ALPHANUMERIC_CODE,
        r"^[A-Za-z0-9\-_]{2,30}$",
    ),
]


def detect_pattern(
    df: DataFrame,
    column_name: str,
    sample_limit: int = 50_000,
) -> tuple[PatternType, float]:
    """Return (dominant_pattern, coverage_fraction) for a string column.

    Evaluates regex patterns on a sample of non-null values.
    """
    col = F.col(column_name)
    non_null = df.filter(col.isNotNull()).select(col.cast("string").alias("_val"))
    total = non_null.count()
    if total == 0:
        return PatternType.UNKNOWN, 0.0

    if total > sample_limit:
        non_null = non_null.limit(sample_limit)
        total = sample_limit

    best_pattern = PatternType.UNKNOWN
    best_coverage = 0.0

    for ptype, regex in _PATTERNS:
        match_count = non_null.filter(F.col("_val").rlike(regex)).count()
        coverage = match_count / total
        if coverage > best_coverage:
            best_pattern = ptype
            best_coverage = coverage
        if coverage >= 0.90:
            break  # good enough — skip remaining patterns

    if best_coverage < 0.30:
        # Fall back to free text if nothing matches well
        best_pattern = PatternType.FREE_TEXT
        best_coverage = 1.0 - best_coverage

    return best_pattern, round(best_coverage, 4)
