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
)
from vibe_profiler.profiler.auto_config import auto_tune_config, collect_table_metrics
from vibe_profiler.profiler.batch_ops import batch_detect_patterns, batch_infer_types, batch_top_values
from vibe_profiler.profiler.sampling import auto_sample
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
        all_col_names = [f.name for f in fields]
        string_cols = [
            f.name for f in fields if basic.get(f.name, {}).get("mean_length") is not None
        ]
        numeric_cols = [
            f.name for f in fields if basic.get(f.name, {}).get("is_numeric")
        ]

        # --- Batched operations (few Spark jobs instead of N per column) ---
        if self.progress_callback:
            ProgressTracker(
                stage="profiling", total=4, callback=self.progress_callback
            ).update(1, "batch:patterns",
                     f"Table '{table_name}' — detecting patterns ({len(string_cols)} string cols)")

        # 1. Pattern detection: 1 aggregation for all string columns
        patterns = batch_detect_patterns(sampled_df, string_cols)

        if self.progress_callback:
            ProgressTracker(
                stage="profiling", total=4, callback=self.progress_callback
            ).update(2, "batch:types",
                     f"Table '{table_name}' — inferring types ({len(string_cols)} string cols)")

        # 2. Type inference: 1 job for bool/int/dec + 1 per timestamp format
        inferred_types = batch_infer_types(sampled_df, string_cols)

        if self.progress_callback:
            ProgressTracker(
                stage="profiling", total=4, callback=self.progress_callback
            ).update(3, "batch:top_values",
                     f"Table '{table_name}' — computing top values ({total_cols} cols)")

        # 3. Top values: cached scan, 1 job per column but on cached data
        top_values = batch_top_values(
            sampled_df, all_col_names, n=table_config.max_top_values
        )

        if self.progress_callback:
            ProgressTracker(
                stage="profiling", total=4, callback=self.progress_callback
            ).update(4, "batch:quantiles",
                     f"Table '{table_name}' — computing quantiles ({len(numeric_cols)} numeric cols)")

        # 4. Quantiles: still per-column (Spark API limitation)
        quantiles_map: dict[str, tuple[float, ...] | None] = {}
        for col_name in numeric_cols:
            quantiles_map[col_name] = compute_approx_quantiles(
                sampled_df, col_name, error=table_config.approx_quantile_error
            )

        # --- Assemble column profiles ---
        col_profiles: list[ColumnProfile] = []
        for field in fields:
            col_name = field.name
            stats = basic.get(col_name)
            if stats is None:
                continue

            pattern, coverage = patterns.get(col_name, (PatternType.UNKNOWN, 0.0))

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
                    top_values=top_values.get(col_name, ()),
                    is_numeric=stats["is_numeric"],
                    approx_quantiles=quantiles_map.get(col_name),
                    inferred_type=inferred_types.get(col_name),
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
