"""Tests for business key detection."""

import pytest
from pyspark.sql import DataFrame

from vibe_profiler.analyzer.business_key import BusinessKeyAnalyzer
from vibe_profiler.profiler.engine import ProfileEngine


class TestBusinessKeyAnalyzer:
    def test_customer_id_detected(self, spark, customers_df: DataFrame):
        engine = ProfileEngine(spark)
        tp = engine.profile_table(customers_df, "customers")

        analyzer = BusinessKeyAnalyzer()
        candidates = analyzer.analyze(tp)

        assert len(candidates) > 0
        top = candidates[0]
        assert top.column_name == "customer_id"
        assert top.score >= 0.7

    def test_order_id_detected(self, spark, orders_df: DataFrame):
        engine = ProfileEngine(spark)
        tp = engine.profile_table(orders_df, "orders")

        analyzer = BusinessKeyAnalyzer()
        candidates = analyzer.analyze(tp)

        assert len(candidates) > 0
        top = candidates[0]
        assert top.column_name == "order_id"

    def test_low_uniqueness_excluded(self, spark, orders_df: DataFrame):
        engine = ProfileEngine(spark)
        tp = engine.profile_table(orders_df, "orders")

        analyzer = BusinessKeyAnalyzer()
        candidates = analyzer.analyze(tp)

        # quantity has low uniqueness, should not be top
        names = [c.column_name for c in candidates[:2]]
        assert "quantity" not in names
