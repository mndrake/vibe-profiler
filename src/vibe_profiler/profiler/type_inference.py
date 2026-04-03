"""Infer semantic types for string columns (dates, numbers, timestamps, booleans).

Samples non-null values and tests parse-ability against known patterns and
format strings.  Returns the best-matching Spark target type and, for temporal
columns, the format string needed for casting.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from vibe_profiler.models.profile import InferredType

# ---------------------------------------------------------------------------
# Date / timestamp format candidates, ordered by frequency
# ---------------------------------------------------------------------------

_TIMESTAMP_FORMATS: list[str] = [
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd HH:mm:ss.SSS",
    "yyyy-MM-dd HH:mm:ss.SSSSSS",
    "yyyy-MM-dd-HH.mm.ss.SSSSSS",
    "yyyy-MM-dd-HH.mm.ss.SSS",
    "yyyy-MM-dd-HH.mm.ss",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss.SSS",
    "yyyy-MM-dd'T'HH:mm:ss.SSSSSS",
    "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
    "yyyy-MM-dd'T'HH:mm:ssZ",
    "MM/dd/yyyy HH:mm:ss",
    "MM/dd/yyyy HH:mm:ss.SSS",
    "dd/MM/yyyy HH:mm:ss",
    "dd/MM/yyyy HH:mm:ss.SSS",
    "MM-dd-yyyy HH:mm:ss",
    "dd-MM-yyyy HH:mm:ss",
    "yyyyMMddHHmmss",
    "yyyy/MM/dd HH:mm:ss",
    "dd-MMM-yyyy HH:mm:ss",
    "MMM dd, yyyy HH:mm:ss",
    "dd.MM.yyyy HH:mm:ss",
    "yyyy.MM.dd HH:mm:ss",
]

_DATE_FORMATS: list[str] = [
    "yyyy-MM-dd",
    "MM/dd/yyyy",
    "dd/MM/yyyy",
    "MM-dd-yyyy",
    "dd-MM-yyyy",
    "yyyy/MM/dd",
    "yyyyMMdd",
    "dd-MMM-yyyy",
    "MMM dd, yyyy",
    "dd.MM.yyyy",
    "yyyy.MM.dd",
]

_BOOLEAN_VALUES = {"true", "false", "yes", "no", "1", "0", "y", "n", "t", "f"}

# Minimum fraction of values that must parse for a type to be accepted
_MIN_CONFIDENCE = 0.80


def infer_column_type(
    df: DataFrame,
    column_name: str,
    sample_limit: int = 10_000,
) -> InferredType | None:
    """Infer the semantic type of a string column.

    Returns ``None`` if the column is not a string type or if no clear
    type could be inferred (stays as string).
    """
    col = F.col(f"`{column_name}`")
    non_null = (
        df.select(col.cast("string").alias("_val"))
        .filter(F.col("_val").isNotNull())
        .filter(F.trim(F.col("_val")) != "")
    )

    total = non_null.count()
    if total == 0:
        return None

    sample = non_null.limit(sample_limit) if total > sample_limit else non_null
    sample_count = min(total, sample_limit)

    # Collect a few example values for the report
    examples = tuple(
        r["_val"] for r in sample.limit(5).collect()
    )

    # --- Try boolean ---
    result = _try_boolean(sample, sample_count, examples)
    if result:
        return result

    # --- Try integer ---
    result = _try_integer(sample, sample_count, examples)
    if result:
        return result

    # --- Try decimal ---
    result = _try_decimal(sample, sample_count, examples)
    if result:
        return result

    # --- Try timestamp (before date, since timestamps are more specific) ---
    result = _try_timestamp(sample, sample_count, examples)
    if result:
        return result

    # --- Try date ---
    result = _try_date(sample, sample_count, examples)
    if result:
        return result

    # No clear type — leave as string
    return None


def _try_boolean(
    sample: DataFrame, total: int, examples: tuple[str, ...]
) -> InferredType | None:
    lower_vals = sample.select(F.lower(F.trim(F.col("_val"))).alias("_val"))
    match_count = lower_vals.filter(
        F.col("_val").isin(list(_BOOLEAN_VALUES))
    ).count()
    confidence = match_count / total if total else 0
    if confidence >= _MIN_CONFIDENCE:
        return InferredType("boolean", None, round(confidence, 4), examples)
    return None


def _try_integer(
    sample: DataFrame, total: int, examples: tuple[str, ...]
) -> InferredType | None:
    trimmed = sample.select(F.trim(F.col("_val")).alias("_val"))
    match_count = trimmed.filter(
        F.col("_val").rlike(r"^\-?\d{1,19}$")
    ).count()
    confidence = match_count / total if total else 0
    if confidence >= _MIN_CONFIDENCE:
        return InferredType("bigint", None, round(confidence, 4), examples)
    return None


def _try_decimal(
    sample: DataFrame, total: int, examples: tuple[str, ...]
) -> InferredType | None:
    trimmed = sample.select(F.trim(F.col("_val")).alias("_val"))
    match_count = trimmed.filter(
        F.col("_val").rlike(r"^\-?\d+\.?\d*$")
    ).count()
    confidence = match_count / total if total else 0
    if confidence >= _MIN_CONFIDENCE:
        return InferredType("double", None, round(confidence, 4), examples)
    return None


def _try_timestamp(
    sample: DataFrame, total: int, examples: tuple[str, ...]
) -> InferredType | None:
    best_fmt = None
    best_confidence = 0.0

    trimmed = sample.select(F.trim(F.col("_val")).alias("_val"))

    for fmt in _TIMESTAMP_FORMATS:
        parsed = trimmed.select(
            F.try_to_timestamp(F.col("_val"), F.lit(fmt)).alias("_parsed")
        )
        success = parsed.filter(F.col("_parsed").isNotNull()).count()
        confidence = success / total if total else 0
        if confidence > best_confidence:
            best_confidence = confidence
            best_fmt = fmt
        if confidence >= 0.95:
            break

    if best_confidence >= _MIN_CONFIDENCE and best_fmt:
        return InferredType(
            "timestamp", best_fmt, round(best_confidence, 4), examples
        )
    return None


def _try_date(
    sample: DataFrame, total: int, examples: tuple[str, ...]
) -> InferredType | None:
    best_fmt = None
    best_confidence = 0.0

    trimmed = sample.select(F.trim(F.col("_val")).alias("_val"))

    for fmt in _DATE_FORMATS:
        parsed = trimmed.select(
            F.try_to_timestamp(F.col("_val"), F.lit(fmt)).alias("_parsed")
        )
        success = parsed.filter(F.col("_parsed").isNotNull()).count()
        confidence = success / total if total else 0
        if confidence > best_confidence:
            best_confidence = confidence
            best_fmt = fmt
        if confidence >= 0.95:
            break

    if best_confidence >= _MIN_CONFIDENCE and best_fmt:
        return InferredType(
            "date", best_fmt, round(best_confidence, 4), examples
        )
    return None
