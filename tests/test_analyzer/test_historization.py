"""Tests for historization / SCD2 detection."""

import pytest
from pyspark.sql import DataFrame

from vibe_profiler.analyzer.business_key import BusinessKeyAnalyzer
from vibe_profiler.analyzer.historization import HistorizationAnalyzer
from vibe_profiler.models.temporal import SCDType
from vibe_profiler.profiler.engine import ProfileEngine


class TestHistorizationAnalyzer:
    def test_detects_scd2(self, spark, orders_history_df: DataFrame):
        engine = ProfileEngine(spark)
        tp = engine.profile_table(orders_history_df, "order_history")

        bk_analyzer = BusinessKeyAnalyzer()
        bk_candidates = bk_analyzer.analyze(tp)

        hist = HistorizationAnalyzer()
        info = hist.analyze(tp, orders_history_df, bk_candidates)

        assert info.scd_type == SCDType.TYPE2
        assert info.confidence > 0.5
        assert len(info.version_key_columns) > 0

    def test_no_historization_on_flat_table(self, spark, customers_df: DataFrame):
        engine = ProfileEngine(spark)
        tp = engine.profile_table(customers_df, "customers")

        bk_analyzer = BusinessKeyAnalyzer()
        bk_candidates = bk_analyzer.analyze(tp)

        hist = HistorizationAnalyzer()
        info = hist.analyze(tp, customers_df, bk_candidates)

        # customers has created_at but no valid_from/valid_to -> TYPE1 at most
        assert info.scd_type in (SCDType.NONE, SCDType.TYPE1)
