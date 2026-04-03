"""Tests for temporal column detection."""

from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, StructField, StructType

from vibe_profiler.models.profile import ColumnProfile, InferredType, PatternType
from vibe_profiler.profiler.temporal_detector import detect_temporal_columns


class TestTemporalDetector:
    def test_detects_valid_from_valid_to(self, orders_history_df: DataFrame):
        cols = detect_temporal_columns(orders_history_df, "order_history")
        roles = {tc.role for tc in cols}
        assert "valid_from" in roles
        assert "valid_to" in roles

    def test_detects_created_at(self, customers_df: DataFrame):
        cols = detect_temporal_columns(customers_df, "customers")
        roles = {tc.role for tc in cols}
        assert "created_date" in roles

    def test_confidence_higher_for_temporal_types(self, orders_history_df: DataFrame):
        cols = detect_temporal_columns(orders_history_df, "order_history")
        for tc in cols:
            if tc.role in ("valid_from", "valid_to"):
                assert tc.confidence >= 0.8

    def test_detects_start_end_suffix(self, spark):
        """Columns like d_hldg_start / d_hldg_end should be detected."""
        schema = StructType([
            StructField("record_id", StringType()),
            StructField("d_hldg_start", StringType()),
            StructField("d_hldg_end", StringType()),
            StructField("status", StringType()),
        ])
        df = spark.createDataFrame(
            [("1", "2024-01-01", "9999-12-31", "A")],
            schema,
        )
        # Provide column profiles with date_iso pattern
        profiles = (
            ColumnProfile(
                "t", "d_hldg_start", "string", 1, 0, 0.0, 1, 1.0,
                "2024-01-01", "2024-01-01", 10.0, 10,
                PatternType.DATE_ISO, 1.0, (), False, None,
                InferredType("date", "yyyy-MM-dd", 1.0, ("2024-01-01",)),
            ),
            ColumnProfile(
                "t", "d_hldg_end", "string", 1, 0, 0.0, 1, 1.0,
                "9999-12-31", "9999-12-31", 10.0, 10,
                PatternType.DATE_ISO, 1.0, (), False, None,
                InferredType("date", "yyyy-MM-dd", 1.0, ("9999-12-31",)),
            ),
        )

        cols = detect_temporal_columns(df, "test_table", column_profiles=profiles)
        roles = {tc.role for tc in cols}
        assert "valid_from" in roles, f"Expected valid_from, got roles: {roles}"
        assert "valid_to" in roles, f"Expected valid_to, got roles: {roles}"

    def test_detects_period_start_end(self, spark):
        """Columns like period_start / period_end should be detected."""
        schema = StructType([
            StructField("id", StringType()),
            StructField("period_start", StringType()),
            StructField("period_end", StringType()),
        ])
        df = spark.createDataFrame([("1", "2024-01-01", "2024-12-31")], schema)
        profiles = (
            ColumnProfile(
                "t", "period_start", "string", 1, 0, 0.0, 1, 1.0,
                None, None, 10.0, 10,
                PatternType.DATE_ISO, 1.0, (), False, None,
                InferredType("date", "yyyy-MM-dd", 1.0, ()),
            ),
            ColumnProfile(
                "t", "period_end", "string", 1, 0, 0.0, 1, 1.0,
                None, None, 10.0, 10,
                PatternType.DATE_ISO, 1.0, (), False, None,
                InferredType("date", "yyyy-MM-dd", 1.0, ()),
            ),
        )
        cols = detect_temporal_columns(df, "t", column_profiles=profiles)
        roles = {tc.role for tc in cols}
        assert "valid_from" in roles
        assert "valid_to" in roles


class TestHistorizationWithStringDates:
    def test_scd2_with_string_date_columns(self, spark):
        """Tables with string date columns should still be detected as SCD2."""
        from vibe_profiler.profiler.engine import ProfileEngine
        from vibe_profiler.analyzer.business_key import BusinessKeyAnalyzer
        from vibe_profiler.analyzer.historization import HistorizationAnalyzer
        from vibe_profiler.models.temporal import SCDType

        schema = StructType([
            StructField("hldg_id", StringType()),
            StructField("account", StringType()),
            StructField("value", StringType()),
            StructField("d_hldg_start", StringType()),
            StructField("d_hldg_end", StringType()),
        ])
        data = [
            ("H1", "A001", "100", "2024-01-01", "2024-06-30"),
            ("H1", "A001", "150", "2024-07-01", "9999-12-31"),
            ("H2", "A002", "200", "2024-01-01", "9999-12-31"),
            ("H3", "A003", "300", "2024-01-01", "2024-03-31"),
            ("H3", "A003", "350", "2024-04-01", "9999-12-31"),
        ]
        df = spark.createDataFrame(data, schema)

        engine = ProfileEngine(spark)
        tp = engine.profile_table(df, "holdings")

        bk_analyzer = BusinessKeyAnalyzer()
        bk_candidates = bk_analyzer.analyze(tp)

        hist = HistorizationAnalyzer()
        info = hist.analyze(tp, df, bk_candidates)

        assert info.scd_type == SCDType.TYPE2, (
            f"Expected TYPE2, got {info.scd_type}. "
            f"Temporal cols: {[(tc.column_name, tc.role) for tc in info.temporal_columns]}"
        )
