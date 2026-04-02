"""Tests for the ProfileEngine."""

import pytest
from pyspark.sql import DataFrame

from vibe_profiler.models.profile import PatternType
from vibe_profiler.profiler.engine import ProfileEngine


class TestProfileEngine:
    def test_profile_single_table(self, spark, customers_df: DataFrame):
        engine = ProfileEngine(spark)
        tp = engine.profile_table(customers_df, "customers")

        assert tp.table_name == "customers"
        assert tp.row_count == 5
        assert len(tp.column_profiles) == 6
        assert tp.sampled is False

    def test_column_stats(self, spark, customers_df: DataFrame):
        engine = ProfileEngine(spark)
        tp = engine.profile_table(customers_df, "customers")

        # customer_id should be highly unique
        cid = next(c for c in tp.column_profiles if c.column_name == "customer_id")
        assert cid.uniqueness == 1.0
        assert cid.null_rate == 0.0
        assert cid.distinct_count == 5

    def test_profile_multiple_tables(self, spark, sample_tables):
        engine = ProfileEngine(spark)
        result = engine.profile_tables(sample_tables)

        assert len(result.tables) == 2
        names = {t.table_name for t in result.tables}
        assert names == {"customers", "orders"}
        assert result.profiled_at is not None

    def test_email_pattern_detection(self, spark, customers_df: DataFrame):
        engine = ProfileEngine(spark)
        tp = engine.profile_table(customers_df, "customers")

        email_col = next(c for c in tp.column_profiles if c.column_name == "email")
        assert email_col.dominant_pattern == PatternType.EMAIL
        assert email_col.pattern_coverage >= 0.8

    def test_top_values(self, spark, orders_df: DataFrame):
        engine = ProfileEngine(spark)
        tp = engine.profile_table(orders_df, "orders")

        product = next(c for c in tp.column_profiles if c.column_name == "product_code")
        assert len(product.top_values) > 0
        # PROD-A appears 3 times
        prod_a = next((v for v, cnt in product.top_values if v == "PROD-A"), None)
        assert prod_a is not None
