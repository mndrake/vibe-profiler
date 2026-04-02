"""Tests for temporal column detection."""

import pytest
from pyspark.sql import DataFrame

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
