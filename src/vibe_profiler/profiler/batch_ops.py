"""Batched profiling operations: pattern detection, type inference, top values.

Combines multiple per-column Spark jobs into single aggregation queries
that process all columns simultaneously, dramatically reducing the number
of Spark jobs for wide tables.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from vibe_profiler.models.profile import InferredType, PatternType
from vibe_profiler.profiler.pattern_detector import _PATTERNS
from vibe_profiler.profiler.type_inference import (
    _BOOLEAN_VALUES,
    _DATE_FORMATS,
    _MIN_CONFIDENCE,
    _TIMESTAMP_FORMATS,
)


def batch_detect_patterns(
    df: DataFrame,
    string_columns: list[str],
    sample_limit: int = 50_000,
) -> dict[str, tuple[PatternType, float]]:
    """Detect dominant pattern for all string columns in a single aggregation.

    Instead of running 7 regex checks per column sequentially, builds one
    ``df.agg(...)`` with all regex match counts for all columns.

    Returns ``{column_name: (pattern_type, coverage)}``.
    """
    if not string_columns:
        return {}

    # Limit sample size
    sample = df.limit(sample_limit) if df.count() > sample_limit else df

    # Build aggregation: for each column × pattern, count matches
    agg_exprs = []
    col_totals: dict[str, str] = {}

    for col_name in string_columns:
        c = F.col(f"`{col_name}`").cast("string")
        non_null = F.when(c.isNotNull(), F.lit(1)).otherwise(F.lit(0))
        agg_exprs.append(F.sum(non_null).alias(f"{col_name}__total"))
        col_totals[col_name] = f"{col_name}__total"

        for ptype, regex in _PATTERNS:
            match_expr = F.when(c.isNotNull() & c.rlike(regex), F.lit(1)).otherwise(F.lit(0))
            agg_exprs.append(F.sum(match_expr).alias(f"{col_name}__{ptype.value}"))

    if not agg_exprs:
        return {}

    row = sample.agg(*agg_exprs).collect()[0]

    results: dict[str, tuple[PatternType, float]] = {}
    for col_name in string_columns:
        total = row[f"{col_name}__total"] or 0
        if total == 0:
            results[col_name] = (PatternType.UNKNOWN, 0.0)
            continue

        best_pattern = PatternType.UNKNOWN
        best_coverage = 0.0

        for ptype, _ in _PATTERNS:
            match_count = row[f"{col_name}__{ptype.value}"] or 0
            coverage = match_count / total
            if coverage > best_coverage:
                best_pattern = ptype
                best_coverage = coverage

        if best_coverage < 0.30:
            best_pattern = PatternType.FREE_TEXT
            best_coverage = 1.0 - best_coverage

        results[col_name] = (best_pattern, round(best_coverage, 4))

    return results


def _try_cache(df: DataFrame) -> tuple[DataFrame, bool]:
    """Attempt to cache a DataFrame; return (df, was_cached).

    Databricks serverless does not support PERSIST TABLE, so we fall
    back to the uncached DataFrame if caching fails.
    """
    try:
        cached = df.cache()
        cached.count()  # materialize the cache
        return cached, True
    except Exception:
        return df, False


def _try_unpersist(df: DataFrame, was_cached: bool) -> None:
    if was_cached:
        try:
            df.unpersist()
        except Exception:
            pass


def batch_top_values(
    df: DataFrame,
    columns: list[str],
    n: int = 20,
) -> dict[str, tuple[tuple[str, int], ...]]:
    """Compute top-N values for multiple columns.

    Attempts to cache the DataFrame to avoid re-scanning per column.
    Falls back gracefully on serverless compute where caching is unsupported.
    """
    df_work, was_cached = _try_cache(df)
    results: dict[str, tuple[tuple[str, int], ...]] = {}

    try:
        for col_name in columns:
            c = F.col(f"`{col_name}`")
            rows = (
                df_work.filter(c.isNotNull())
                .groupBy(c.cast("string").alias("_val"))
                .agg(F.count("*").alias("_cnt"))
                .orderBy(F.desc("_cnt"))
                .limit(n)
                .collect()
            )
            results[col_name] = tuple((r["_val"], r["_cnt"]) for r in rows)
    finally:
        _try_unpersist(df_work, was_cached)

    return results


def batch_infer_types(
    df: DataFrame,
    string_columns: list[str],
    sample_limit: int = 10_000,
) -> dict[str, InferredType | None]:
    """Infer semantic types for all string columns with minimal Spark jobs.

    Strategy:
    1. Single agg to test boolean/integer/decimal regex for ALL columns (1 job)
    2. For columns that aren't boolean/integer/decimal, test timestamp formats
       in batches (1 job per format across all remaining columns)
    3. Same for date formats

    This replaces N_columns × M_formats individual jobs with M_formats jobs.
    """
    if not string_columns:
        return {}

    sample = df.limit(sample_limit)
    sample_work, was_cached = _try_cache(sample)

    try:
        results: dict[str, InferredType | None] = {}

        # Collect example values for each column (one job)
        examples: dict[str, tuple[str, ...]] = {}
        for col_name in string_columns:
            c = F.col(f"`{col_name}`").cast("string")
            rows = (
                sample_work.select(c.alias("v"))
                .filter(F.col("v").isNotNull())
                .filter(F.trim(F.col("v")) != "")
                .limit(5)
                .collect()
            )
            examples[col_name] = tuple(r["v"] for r in rows)

        # --- Step 1: Test boolean, integer, decimal in one aggregation ---
        agg_exprs = []
        for col_name in string_columns:
            c = F.lower(F.trim(F.col(f"`{col_name}`").cast("string")))
            non_null = F.when(
                F.col(f"`{col_name}`").isNotNull()
                & (F.trim(F.col(f"`{col_name}`").cast("string")) != ""),
                F.lit(1),
            ).otherwise(F.lit(0))

            agg_exprs.append(F.sum(non_null).alias(f"{col_name}__total"))
            # Boolean
            agg_exprs.append(
                F.sum(F.when(c.isin(list(_BOOLEAN_VALUES)), 1).otherwise(0)).alias(
                    f"{col_name}__bool"
                )
            )
            # Integer
            agg_exprs.append(
                F.sum(
                    F.when(
                        F.trim(F.col(f"`{col_name}`").cast("string")).rlike(r"^\-?\d{1,19}$"), 1
                    ).otherwise(0)
                ).alias(f"{col_name}__int")
            )
            # Decimal
            agg_exprs.append(
                F.sum(
                    F.when(
                        F.trim(F.col(f"`{col_name}`").cast("string")).rlike(r"^\-?\d+\.?\d*$"), 1
                    ).otherwise(0)
                ).alias(f"{col_name}__dec")
            )

        row = sample_work.agg(*agg_exprs).collect()[0]

        # Process results and identify which columns still need temporal checking
        needs_temporal: list[str] = []
        for col_name in string_columns:
            total = row[f"{col_name}__total"] or 0
            if total == 0:
                results[col_name] = None
                continue

            # Boolean
            bool_conf = (row[f"{col_name}__bool"] or 0) / total
            if bool_conf >= _MIN_CONFIDENCE:
                results[col_name] = InferredType(
                    "boolean", None, round(bool_conf, 4), examples.get(col_name, ())
                )
                continue

            # Integer
            int_conf = (row[f"{col_name}__int"] or 0) / total
            if int_conf >= _MIN_CONFIDENCE:
                results[col_name] = InferredType(
                    "bigint", None, round(int_conf, 4), examples.get(col_name, ())
                )
                continue

            # Decimal
            dec_conf = (row[f"{col_name}__dec"] or 0) / total
            if dec_conf >= _MIN_CONFIDENCE:
                results[col_name] = InferredType(
                    "double", None, round(dec_conf, 4), examples.get(col_name, ())
                )
                continue

            needs_temporal.append(col_name)

        # --- Step 2: Test timestamp formats (1 job per format, all columns) ---
        if needs_temporal:
            _batch_temporal_check(
                sample_work, needs_temporal, _TIMESTAMP_FORMATS, "timestamp",
                row, examples, results,
            )

        # --- Step 3: Date formats for remaining unresolved columns ---
        still_unresolved = [c for c in needs_temporal if c not in results]
        if still_unresolved:
            _batch_temporal_check(
                sample_work, still_unresolved, _DATE_FORMATS, "date",
                row, examples, results,
            )

        # Mark remaining as None (stays string)
        for col_name in string_columns:
            if col_name not in results:
                results[col_name] = None

        return results
    finally:
        _try_unpersist(sample_work, was_cached)


def _batch_temporal_check(
    df: DataFrame,
    columns: list[str],
    formats: list[str],
    target_type: str,
    totals_row,
    examples: dict[str, tuple[str, ...]],
    results: dict[str, InferredType | None],
) -> None:
    """Test temporal formats across multiple columns simultaneously.

    One Spark job per format string, testing all columns at once.
    """
    # Track best format per column
    best: dict[str, tuple[str, float]] = {}  # col -> (format, confidence)

    for fmt in formats:
        # Skip columns already resolved
        cols_to_test = [c for c in columns if c not in results]
        if not cols_to_test:
            break

        # One aggregation per format, testing all remaining columns
        agg_exprs = []
        for col_name in cols_to_test:
            c = F.trim(F.col(f"`{col_name}`").cast("string"))
            parsed = F.try_to_timestamp(c, F.lit(fmt))
            agg_exprs.append(
                F.sum(F.when(parsed.isNotNull(), 1).otherwise(0)).alias(
                    f"{col_name}__match"
                )
            )

        row = df.agg(*agg_exprs).collect()[0]

        for col_name in cols_to_test:
            total = totals_row[f"{col_name}__total"] or 0
            if total == 0:
                continue
            match_count = row[f"{col_name}__match"] or 0
            confidence = match_count / total

            prev_best = best.get(col_name, ("", 0.0))
            if confidence > prev_best[1]:
                best[col_name] = (fmt, confidence)

            # Early exit for this column if we found a great match
            if confidence >= 0.95:
                results[col_name] = InferredType(
                    target_type, fmt, round(confidence, 4),
                    examples.get(col_name, ()),
                )

    # Resolve columns that didn't hit 95% but passed threshold
    for col_name in columns:
        if col_name in results:
            continue
        if col_name in best:
            fmt, confidence = best[col_name]
            if confidence >= _MIN_CONFIDENCE:
                results[col_name] = InferredType(
                    target_type, fmt, round(confidence, 4),
                    examples.get(col_name, ()),
                )
