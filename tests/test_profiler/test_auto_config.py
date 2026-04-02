"""Tests for the auto-config pre-scan tuning logic."""

import pytest

from vibe_profiler.config import AnalysisConfig, ProfilingConfig
from vibe_profiler.profiler.auto_config import (
    TableMetrics,
    auto_tune_analysis_config,
    auto_tune_config,
    _tune_column_batch_size,
    _tune_approx_distinct,
    _tune_quantile_error,
    _tune_max_top_values,
)


# -------------------------------------------------------------------
# Column batch size
# -------------------------------------------------------------------


class TestTuneColumnBatchSize:
    def test_narrow_table(self):
        assert _tune_column_batch_size(5) == 50

    def test_medium_table(self):
        assert _tune_column_batch_size(50) == 30

    def test_wide_table(self):
        assert _tune_column_batch_size(200) == 15

    def test_very_wide_table(self):
        assert _tune_column_batch_size(500) == 8

    def test_extremely_wide_table(self):
        assert _tune_column_batch_size(2000) == 5


# -------------------------------------------------------------------
# Approx distinct
# -------------------------------------------------------------------


class TestTuneApproxDistinct:
    def test_small_table_uses_exact(self):
        assert _tune_approx_distinct(50_000) is False

    def test_large_table_uses_approx(self):
        assert _tune_approx_distinct(500_000) is True

    def test_boundary(self):
        assert _tune_approx_distinct(99_999) is False
        assert _tune_approx_distinct(100_000) is True


# -------------------------------------------------------------------
# Quantile error
# -------------------------------------------------------------------


class TestTuneQuantileError:
    def test_tiny_table(self):
        assert _tune_quantile_error(10_000) == 0.001

    def test_medium_table(self):
        assert _tune_quantile_error(500_000) == 0.005

    def test_large_table(self):
        assert _tune_quantile_error(10_000_000) == 0.01

    def test_huge_table(self):
        assert _tune_quantile_error(100_000_000) == 0.05


# -------------------------------------------------------------------
# Max top values
# -------------------------------------------------------------------


class TestTuneMaxTopValues:
    def test_tiny_table(self):
        assert _tune_max_top_values(500) == 50

    def test_small_table(self):
        assert _tune_max_top_values(50_000) == 30

    def test_medium_table(self):
        assert _tune_max_top_values(1_000_000) == 20

    def test_large_table(self):
        assert _tune_max_top_values(50_000_000) == 10


# -------------------------------------------------------------------
# Full auto_tune_config
# -------------------------------------------------------------------


class TestAutoTuneConfig:
    def test_tunes_for_small_narrow_table(self):
        base = ProfilingConfig()
        metrics = TableMetrics(row_count=5_000, column_count=6)
        tuned = auto_tune_config(base, metrics)

        assert tuned.column_batch_size == 50
        assert tuned.approx_distinct is False
        assert tuned.approx_quantile_error == 0.001
        assert tuned.max_top_values == 30  # 5K rows = 1K-100K bucket

    def test_tunes_for_large_wide_table(self):
        base = ProfilingConfig()
        metrics = TableMetrics(row_count=100_000_000, column_count=400)
        tuned = auto_tune_config(base, metrics)

        assert tuned.column_batch_size == 8
        assert tuned.approx_distinct is True
        assert tuned.approx_quantile_error == 0.05
        assert tuned.max_top_values == 10

    def test_user_override_preserved(self):
        base = ProfilingConfig.create(approx_distinct=False, max_top_values=100)
        metrics = TableMetrics(row_count=50_000_000, column_count=200)
        tuned = auto_tune_config(base, metrics)

        # User-set fields should NOT be overridden
        assert tuned.approx_distinct is False
        assert tuned.max_top_values == 100
        # Non-user-set fields SHOULD be tuned
        assert tuned.column_batch_size == 15
        assert tuned.approx_quantile_error == 0.05  # 50M rows = >50M bucket

    def test_auto_tune_disabled(self):
        base = ProfilingConfig(auto_tune=False)
        metrics = TableMetrics(row_count=100, column_count=3)
        tuned = auto_tune_config(base, metrics)

        # Should return base unchanged
        assert tuned is base

    def test_unknown_row_count_still_tunes_columns(self):
        base = ProfilingConfig()
        metrics = TableMetrics(row_count=-1, column_count=500)
        tuned = auto_tune_config(base, metrics)

        # Column-based tuning works
        assert tuned.column_batch_size == 8
        # Row-based tuning skipped (defaults preserved)
        assert tuned.approx_distinct == base.approx_distinct
        assert tuned.max_top_values == base.max_top_values


# -------------------------------------------------------------------
# Analysis config tuning
# -------------------------------------------------------------------


class TestAutoTuneAnalysisConfig:
    def test_small_tables(self):
        base = AnalysisConfig()
        metrics = {
            "t1": TableMetrics(row_count=5_000, column_count=5),
            "t2": TableMetrics(row_count=8_000, column_count=3),
        }
        tuned = auto_tune_analysis_config(base, metrics)
        assert tuned.value_sample_size == 5_000  # min row count

    def test_medium_tables(self):
        base = AnalysisConfig()
        metrics = {
            "t1": TableMetrics(row_count=500_000, column_count=10),
            "t2": TableMetrics(row_count=200_000, column_count=8),
        }
        tuned = auto_tune_analysis_config(base, metrics)
        assert tuned.value_sample_size == 10_000

    def test_large_tables(self):
        base = AnalysisConfig()
        metrics = {
            "t1": TableMetrics(row_count=5_000_000, column_count=10),
            "t2": TableMetrics(row_count=10_000_000, column_count=15),
        }
        tuned = auto_tune_analysis_config(base, metrics)
        assert tuned.value_sample_size == 50_000

    def test_no_known_counts(self):
        base = AnalysisConfig()
        metrics = {
            "t1": TableMetrics(row_count=-1, column_count=5),
        }
        tuned = auto_tune_analysis_config(base, metrics)
        assert tuned is base  # unchanged


# -------------------------------------------------------------------
# Integration: auto-tune through ProfileEngine
# -------------------------------------------------------------------


class TestProfileEngineAutoTune:
    def test_auto_tune_enabled_by_default(self, spark, customers_df):
        from vibe_profiler.profiler.engine import ProfileEngine

        engine = ProfileEngine(spark)
        assert engine.config.auto_tune is True

        # Should profile without error
        tp = engine.profile_table(customers_df, "customers")
        assert tp.row_count == 5

    def test_auto_tune_disabled(self, spark, customers_df):
        from vibe_profiler.profiler.engine import ProfileEngine

        config = ProfilingConfig(auto_tune=False)
        engine = ProfileEngine(spark, config=config)
        tp = engine.profile_table(customers_df, "customers")
        assert tp.row_count == 5
