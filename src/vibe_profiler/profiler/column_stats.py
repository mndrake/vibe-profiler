"""Spark-based column-level statistical computations.

Builds a single aggregation query to collect all column statistics in one pass.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ByteType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
)

_NUMERIC_TYPES = (
    ByteType,
    ShortType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    DecimalType,
)


def is_numeric_type(spark_type) -> bool:
    return isinstance(spark_type, _NUMERIC_TYPES)


def is_string_type(spark_type) -> bool:
    return isinstance(spark_type, StringType)


def compute_basic_stats(
    df: DataFrame,
    approx_distinct: bool = True,
    column_batch_size: int = 50,
) -> dict[str, dict]:
    """Compute per-column stats in a single-pass aggregation.

    Returns ``{column_name: {stat_name: value}}`` dict.
    """
    schema = df.schema
    columns = [f.name for f in schema.fields]
    total_count = df.count()

    if total_count == 0:
        return {
            col: _empty_stats(col, str(schema[col].dataType), total_count)
            for col in columns
        }

    all_stats: dict[str, dict] = {}

    # Batch columns to avoid blowing up Spark's catalyst plan
    for batch_start in range(0, len(columns), column_batch_size):
        batch_cols = columns[batch_start : batch_start + column_batch_size]
        agg_exprs = []

        for col_name in batch_cols:
            c = F.col(f"`{col_name}`")
            field = schema[col_name]

            agg_exprs.append(
                F.sum(F.when(c.isNull(), 1).otherwise(0)).alias(f"{col_name}__nulls")
            )

            if approx_distinct:
                agg_exprs.append(
                    F.approx_count_distinct(c).alias(f"{col_name}__distinct")
                )
            else:
                agg_exprs.append(
                    F.countDistinct(c).alias(f"{col_name}__distinct")
                )

            agg_exprs.append(F.min(c.cast("string")).alias(f"{col_name}__min"))
            agg_exprs.append(F.max(c.cast("string")).alias(f"{col_name}__max"))

            if is_string_type(field.dataType):
                agg_exprs.append(
                    F.mean(F.length(c)).alias(f"{col_name}__mean_len")
                )
                agg_exprs.append(
                    F.max(F.length(c)).alias(f"{col_name}__max_len")
                )

        row = df.agg(*agg_exprs).collect()[0]

        for col_name in batch_cols:
            field = schema[col_name]
            null_count = row[f"{col_name}__nulls"] or 0
            distinct_count = row[f"{col_name}__distinct"] or 0
            non_null = total_count - null_count
            uniqueness = min(distinct_count / non_null, 1.0) if non_null > 0 else 0.0

            stats: dict = {
                "spark_type": str(field.dataType),
                "row_count": total_count,
                "null_count": null_count,
                "null_rate": round(null_count / total_count, 6) if total_count else 0.0,
                "distinct_count": distinct_count,
                "uniqueness": round(uniqueness, 6),
                "min_value": row[f"{col_name}__min"],
                "max_value": row[f"{col_name}__max"],
                "is_numeric": is_numeric_type(field.dataType),
                "mean_length": None,
                "max_length": None,
            }

            if is_string_type(field.dataType):
                stats["mean_length"] = (
                    round(row[f"{col_name}__mean_len"], 2)
                    if row[f"{col_name}__mean_len"] is not None
                    else None
                )
                stats["max_length"] = row[f"{col_name}__max_len"]

            all_stats[col_name] = stats

    return all_stats


def compute_top_values(
    df: DataFrame,
    column_name: str,
    n: int = 20,
) -> tuple[tuple[str, int], ...]:
    """Return the top-N most frequent values for a column."""
    c = F.col(f"`{column_name}`")
    rows = (
        df.filter(c.isNotNull())
        .groupBy(c.cast("string").alias("_val"))
        .agg(F.count("*").alias("_cnt"))
        .orderBy(F.desc("_cnt"))
        .limit(n)
        .collect()
    )
    return tuple((r["_val"], r["_cnt"]) for r in rows)


def compute_approx_quantiles(
    df: DataFrame,
    column_name: str,
    error: float = 0.01,
) -> tuple[float, ...] | None:
    """Return (p25, p50, p75) for a numeric column, or None if not applicable."""
    field = df.schema[column_name]
    if not is_numeric_type(field.dataType):
        return None
    try:
        result = df.stat.approxQuantile(column_name, [0.25, 0.5, 0.75], error)
        if result and len(result) == 3:
            return tuple(result)
    except Exception:
        pass
    return None


def _empty_stats(col_name: str, spark_type: str, total: int) -> dict:
    return {
        "spark_type": spark_type,
        "row_count": total,
        "null_count": 0,
        "null_rate": 0.0,
        "distinct_count": 0,
        "uniqueness": 0.0,
        "min_value": None,
        "max_value": None,
        "is_numeric": False,
        "mean_length": None,
        "max_length": None,
    }
