"""Lightweight pre-scan to auto-tune profiling config per table.

Collects cheap metadata (schema + optional plan stats) and returns
a ``ProfilingConfig`` with settings adapted to the table's shape.
"""

from __future__ import annotations

from dataclasses import dataclass, replace

from pyspark.sql import DataFrame

from vibe_profiler.config import AnalysisConfig, ProfilingConfig


# ---------------------------------------------------------------------------
# Table metrics (cheap to collect)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TableMetrics:
    """Lightweight scan result — no full data read required."""

    row_count: int  # -1 if unknown (will be resolved later by auto_sample)
    column_count: int


def collect_table_metrics(df: DataFrame) -> TableMetrics:
    """Gather table metadata with minimal cost.

    1. Column count — from schema (free).
    2. Row count — from Spark plan statistics when available (free on
       Delta / Databricks).  Falls back to ``df.count()`` which Spark
       will cache for subsequent use by ``auto_sample``.
    """
    column_count = len(df.schema.fields)

    row_count = _try_plan_stats_row_count(df)
    if row_count is None:
        try:
            row_count = df.count()
        except Exception:
            row_count = -1

    return TableMetrics(row_count=row_count, column_count=column_count)


def _try_plan_stats_row_count(df: DataFrame) -> int | None:
    """Attempt to read row count from Spark's optimized plan statistics.

    Available for Delta tables on Databricks without triggering a scan.
    Returns ``None`` if stats are unavailable or the API is not present.
    """
    try:
        java_stats = df._jdf.queryExecution().optimizedPlan().stats()  # type: ignore[union-attr]
        big_int = java_stats.rowCount()
        if big_int.isDefined():
            count = int(big_int.get().longValue())
            if count >= 0:
                return count
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Profiling config auto-tuning
# ---------------------------------------------------------------------------

def auto_tune_config(
    base: ProfilingConfig,
    metrics: TableMetrics,
) -> ProfilingConfig:
    """Return a copy of *base* with settings adapted to *metrics*.

    Only overrides fields that the user did **not** explicitly set (i.e. fields
    not in ``base._user_set_fields``).
    """
    if not base.auto_tune:
        return base

    overrides: dict = {}
    user_set = base._user_set_fields

    if "column_batch_size" not in user_set:
        overrides["column_batch_size"] = _tune_column_batch_size(metrics.column_count)

    if metrics.row_count >= 0:
        if "approx_distinct" not in user_set:
            overrides["approx_distinct"] = _tune_approx_distinct(metrics.row_count)

        if "approx_quantile_error" not in user_set:
            overrides["approx_quantile_error"] = _tune_quantile_error(metrics.row_count)

        if "max_top_values" not in user_set:
            overrides["max_top_values"] = _tune_max_top_values(metrics.row_count)

    if not overrides:
        return base

    return replace(base, **overrides)


def _tune_column_batch_size(column_count: int) -> int:
    if column_count <= 20:
        return 50
    if column_count <= 100:
        return 30
    if column_count <= 300:
        return 15
    if column_count <= 1000:
        return 8
    return 5


def _tune_approx_distinct(row_count: int) -> bool:
    return row_count >= 100_000


def _tune_quantile_error(row_count: int) -> float:
    if row_count < 50_000:
        return 0.001
    if row_count < 1_000_000:
        return 0.005
    if row_count < 50_000_000:
        return 0.01
    return 0.05


def _tune_max_top_values(row_count: int) -> int:
    if row_count < 1_000:
        return 50
    if row_count < 100_000:
        return 30
    if row_count < 10_000_000:
        return 20
    return 10


# ---------------------------------------------------------------------------
# Analysis config auto-tuning
# ---------------------------------------------------------------------------

def auto_tune_analysis_config(
    base: AnalysisConfig,
    table_metrics: dict[str, TableMetrics],
) -> AnalysisConfig:
    """Adjust ``AnalysisConfig.value_sample_size`` based on table sizes."""
    known_counts = [m.row_count for m in table_metrics.values() if m.row_count >= 0]
    if not known_counts:
        return base

    min_rows = min(known_counts)

    if min_rows < 10_000:
        value_sample_size = min_rows
    elif min_rows < 1_000_000:
        value_sample_size = 10_000
    else:
        value_sample_size = 50_000

    if value_sample_size == base.value_sample_size:
        return base

    return replace(base, value_sample_size=value_sample_size)
