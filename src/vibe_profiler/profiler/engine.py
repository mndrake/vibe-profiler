"""ProfileEngine: orchestrates column-level profiling of Spark DataFrames."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from vibe_profiler.config import ProfilingConfig
from vibe_profiler.models.profile import ColumnProfile, PatternType, ProfileResult, TableProfile
from vibe_profiler.profiler.column_stats import (
    compute_approx_quantiles,
    compute_basic_stats,
    compute_top_values,
)
from vibe_profiler.profiler.auto_config import auto_tune_config, collect_table_metrics
from vibe_profiler.profiler.pattern_detector import detect_pattern
from vibe_profiler.profiler.sampling import auto_sample
from vibe_profiler.profiler.type_inference import infer_column_type
from vibe_profiler.progress import ProgressCallback, ProgressTracker


class ProfileEngine:
    """Scan one or more Spark tables/DataFrames to produce detailed column profiles."""

    def __init__(
        self,
        spark: SparkSession,
        config: ProfilingConfig | None = None,
        progress_callback: ProgressCallback | None = None,
    ) -> None:
        self.spark = spark
        self.config = config or ProfilingConfig()
        self.progress_callback = progress_callback

    def profile_table(
        self,
        df: DataFrame,
        table_name: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        _tracker: ProgressTracker | None = None,
        _table_index: int = 0,
    ) -> TableProfile:
        """Profile every column of *df* and return a ``TableProfile``."""
        # Auto-tune config per table when enabled
        if self.config.auto_tune:
            metrics = collect_table_metrics(df)
            table_config = auto_tune_config(self.config, metrics)
        else:
            table_config = self.config

        sampled_df, was_sampled, sample_frac = auto_sample(
            df,
            threshold_rows=table_config.sample_threshold_rows,
            forced_fraction=table_config.sample_fraction,
        )

        basic = compute_basic_stats(
            sampled_df,
            approx_distinct=table_config.approx_distinct,
            column_batch_size=table_config.column_batch_size,
        )

        fields = [f for f in df.schema.fields if f.name in basic]
        total_cols = len(fields)
        col_profiles: list[ColumnProfile] = []

        # Column-level progress tracker (nested inside the table-level one)
        col_tracker = ProgressTracker(
            stage="profiling",
            total=total_cols,
            callback=self.progress_callback,
        ) if self.progress_callback else None

        for col_idx, field in enumerate(fields, 1):
            col_name = field.name
            stats = basic.get(col_name)
            if stats is None:
                continue

            if col_tracker:
                col_tracker.update(
                    col_idx,
                    step=f"column:{col_name}",
                    message=f"Table '{table_name}' — column {col_idx}/{total_cols}: {col_name}",
                )

            # Pattern detection (string columns only)
            if stats.get("mean_length") is not None:
                pattern, coverage = detect_pattern(sampled_df, col_name)
            else:
                pattern, coverage = PatternType.UNKNOWN, 0.0

            top_vals = compute_top_values(
                sampled_df, col_name, n=table_config.max_top_values
            )

            quantiles = compute_approx_quantiles(
                sampled_df, col_name, error=table_config.approx_quantile_error
            )

            # Type inference for string columns — detect hidden dates, numbers, etc.
            inferred = None
            if stats.get("mean_length") is not None:  # string column
                inferred = infer_column_type(sampled_df, col_name)

            col_profiles.append(
                ColumnProfile(
                    table_name=table_name,
                    column_name=col_name,
                    spark_type=stats["spark_type"],
                    row_count=stats["row_count"],
                    null_count=stats["null_count"],
                    null_rate=stats["null_rate"],
                    distinct_count=stats["distinct_count"],
                    uniqueness=stats["uniqueness"],
                    min_value=stats["min_value"],
                    max_value=stats["max_value"],
                    mean_length=stats["mean_length"],
                    max_length=stats["max_length"],
                    dominant_pattern=pattern,
                    pattern_coverage=coverage,
                    top_values=top_vals,
                    is_numeric=stats["is_numeric"],
                    approx_quantiles=quantiles,
                    inferred_type=inferred,
                )
            )

        row_count = basic[df.schema.fields[0].name]["row_count"] if df.schema.fields else 0

        # Verify uniqueness for potential BK columns using exact count on the
        # full (unsampled) DataFrame.  Approximate distinct + sampling can
        # undercount by 2-5%, which causes wrong BK selection downstream.
        if was_sampled or table_config.approx_distinct:
            col_profiles = self._verify_bk_uniqueness(df, col_profiles, row_count)

        return TableProfile(
            table_name=table_name,
            catalog=catalog,
            schema=schema,
            row_count=row_count,
            column_profiles=tuple(col_profiles),
            sampled=was_sampled,
            sample_fraction=sample_frac,
        )

    @staticmethod
    def _verify_bk_uniqueness(
        df: DataFrame,
        col_profiles: list[ColumnProfile],
        row_count: int,
    ) -> list[ColumnProfile]:
        """Re-check uniqueness with exact countDistinct for potential BK columns.

        Columns with approximate uniqueness >= 85% and low null rate are
        likely BK candidates.  Their uniqueness is re-verified on the full
        (unsampled) DataFrame to avoid approx_count_distinct errors that
        cause wrong BK selection.
        """
        if row_count == 0:
            return col_profiles

        # Identify columns worth re-checking
        cols_to_verify = [
            cp for cp in col_profiles
            if cp.uniqueness >= 0.85 and cp.null_rate <= 0.10
        ]
        if not cols_to_verify:
            return col_profiles

        # Single aggregation for all candidate columns
        from pyspark.sql import functions as _F

        agg_exprs = []
        for cp in cols_to_verify:
            c = _F.col(f"`{cp.column_name}`")
            agg_exprs.append(_F.countDistinct(c).alias(f"{cp.column_name}__exact"))
            agg_exprs.append(
                _F.sum(_F.when(c.isNull(), 1).otherwise(0)).alias(f"{cp.column_name}__nulls")
            )

        row = df.agg(*agg_exprs).collect()[0]

        # Build corrected profiles
        verified_names: dict[str, tuple[int, int, float, float]] = {}
        for cp in cols_to_verify:
            exact_distinct = row[f"{cp.column_name}__exact"] or 0
            exact_nulls = row[f"{cp.column_name}__nulls"] or 0
            non_null = row_count - exact_nulls
            exact_uniqueness = min(exact_distinct / non_null, 1.0) if non_null > 0 else 0.0
            exact_null_rate = round(exact_nulls / row_count, 6) if row_count else 0.0
            verified_names[cp.column_name] = (
                exact_distinct, exact_nulls, exact_uniqueness, exact_null_rate
            )

        result: list[ColumnProfile] = []
        for cp in col_profiles:
            if cp.column_name in verified_names:
                exact_distinct, exact_nulls, exact_uniqueness, exact_null_rate = (
                    verified_names[cp.column_name]
                )
                cp = ColumnProfile(
                    table_name=cp.table_name,
                    column_name=cp.column_name,
                    spark_type=cp.spark_type,
                    row_count=cp.row_count,
                    null_count=exact_nulls,
                    null_rate=exact_null_rate,
                    distinct_count=exact_distinct,
                    uniqueness=round(exact_uniqueness, 6),
                    min_value=cp.min_value,
                    max_value=cp.max_value,
                    mean_length=cp.mean_length,
                    max_length=cp.max_length,
                    dominant_pattern=cp.dominant_pattern,
                    pattern_coverage=cp.pattern_coverage,
                    top_values=cp.top_values,
                    is_numeric=cp.is_numeric,
                    approx_quantiles=cp.approx_quantiles,
                    inferred_type=cp.inferred_type,
                )
            result.append(cp)
        return result

    def profile_tables(
        self,
        tables: dict[str, DataFrame],
    ) -> ProfileResult:
        """Profile multiple tables and return an aggregated ``ProfileResult``.

        When ``max_parallel_tables > 1``, profiles tables concurrently using
        a thread pool.  Spark handles concurrent jobs from different threads.
        """
        max_workers = self.config.max_parallel_tables

        tracker = ProgressTracker(
            stage="profiling",
            total=len(tables),
            callback=self.progress_callback,
        )

        profiles: list[TableProfile] = []

        if max_workers > 1 and len(tables) > 1:
            from concurrent.futures import ThreadPoolExecutor, as_completed

            completed = 0
            with ThreadPoolExecutor(max_workers=min(max_workers, len(tables))) as pool:
                futures = {
                    pool.submit(self.profile_table, df, name): name
                    for name, df in tables.items()
                }
                for future in as_completed(futures):
                    completed += 1
                    name = futures[future]
                    tp = future.result()
                    profiles.append(tp)
                    tracker.update(
                        completed,
                        step=f"table:{name}",
                        message=(
                            f"Profiled table {completed}/{len(tables)}: "
                            f"{name} ({len(tp.column_profiles)} columns)"
                        ),
                    )
        else:
            for idx, (name, df) in enumerate(tables.items(), 1):
                tracker.update(
                    idx,
                    step=f"table:{name}",
                    message=(
                        f"Profiling table {idx}/{len(tables)}: "
                        f"{name} ({len(df.columns)} columns)"
                    ),
                )
                profiles.append(
                    self.profile_table(df, table_name=name, _tracker=tracker, _table_index=idx)
                )

        tracker.complete(f"Profiled {len(tables)} tables")

        return ProfileResult(
            tables=tuple(profiles),
            profiled_at=datetime.now(timezone.utc).isoformat(),
        )
