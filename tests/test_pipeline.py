"""Integration tests for the full pipeline."""

import pytest
from pyspark.sql import DataFrame

from vibe_profiler.pipeline import VibeProfilerPipeline
from vibe_profiler.models.temporal import SCDType


class TestPipeline:
    def test_full_pipeline(self, spark, sample_tables):
        pipeline = VibeProfilerPipeline(spark, sample_tables)
        files = pipeline.run_all()

        assert len(files) > 0
        assert pipeline.profile_result is not None
        assert pipeline.analysis_result is not None
        assert pipeline.vault_spec is not None

    def test_override_business_keys(self, spark, sample_tables):
        pipeline = VibeProfilerPipeline(spark, sample_tables)
        pipeline.run_analysis()
        pipeline.override_business_keys({"customers": ["email"]})

        bk = pipeline.analysis_result.business_keys["customers"]
        assert bk[0].column_name == "email"
        assert bk[0].reasoning == ("Manually specified",)

    def test_historized_data(self, spark, orders_history_df):
        tables = {"order_history": orders_history_df}
        pipeline = VibeProfilerPipeline(spark, tables)
        pipeline.run_profiling()
        pipeline.run_analysis()

        hist = pipeline.analysis_result.historization["order_history"]
        assert hist.scd_type == SCDType.TYPE2

    def test_write_dbt_project(self, spark, sample_tables, tmp_path):
        pipeline = VibeProfilerPipeline(spark, sample_tables)
        pipeline.run_all()
        written = pipeline.write_dbt_project(str(tmp_path))
        assert len(written) > 0
