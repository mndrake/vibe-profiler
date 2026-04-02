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
from vibe_profiler.profiler.pattern_detector import detect_pattern
from vibe_profiler.profiler.sampling import auto_sample


class ProfileEngine:
    """Scan one or more Spark tables/DataFrames to produce detailed column profiles."""

    def __init__(
        self,
        spark: SparkSession,
        config: ProfilingConfig | None = None,
    ) -> None:
        self.spark = spark
        self.config = config or ProfilingConfig()

    def profile_table(
        self,
        df: DataFrame,
        table_name: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> TableProfile:
        """Profile every column of *df* and return a ``TableProfile``."""
        sampled_df, was_sampled, sample_frac = auto_sample(
            df,
            threshold_rows=self.config.sample_threshold_rows,
            forced_fraction=self.config.sample_fraction,
        )

        basic = compute_basic_stats(
            sampled_df,
            approx_distinct=self.config.approx_distinct,
            column_batch_size=self.config.column_batch_size,
        )

        col_profiles: list[ColumnProfile] = []
        for field in df.schema.fields:
            col_name = field.name
            stats = basic.get(col_name)
            if stats is None:
                continue

            # Pattern detection (string columns only)
            if stats.get("mean_length") is not None:
                pattern, coverage = detect_pattern(sampled_df, col_name)
            else:
                pattern, coverage = PatternType.UNKNOWN, 0.0

            top_vals = compute_top_values(
                sampled_df, col_name, n=self.config.max_top_values
            )

            quantiles = compute_approx_quantiles(
                sampled_df, col_name, error=self.config.approx_quantile_error
            )

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
                )
            )

        row_count = basic[df.schema.fields[0].name]["row_count"] if df.schema.fields else 0

        return TableProfile(
            table_name=table_name,
            catalog=catalog,
            schema=schema,
            row_count=row_count,
            column_profiles=tuple(col_profiles),
            sampled=was_sampled,
            sample_fraction=sample_frac,
        )

    def profile_tables(
        self,
        tables: dict[str, DataFrame],
    ) -> ProfileResult:
        """Profile multiple tables and return an aggregated ``ProfileResult``."""
        profiles: list[TableProfile] = []
        for name, df in tables.items():
            profiles.append(self.profile_table(df, table_name=name))

        return ProfileResult(
            tables=tuple(profiles),
            profiled_at=datetime.now(timezone.utc).isoformat(),
        )
