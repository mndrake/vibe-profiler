"""Tests for the Data Vault suggestion engine."""

import pytest
from pyspark.sql import DataFrame

from vibe_profiler.config import PipelineConfig
from vibe_profiler.pipeline import VibeProfilerPipeline


class TestVaultSuggestionEngine:
    def test_generates_hubs(self, spark, sample_tables):
        pipeline = VibeProfilerPipeline(spark, sample_tables)
        pipeline.run_profiling()
        pipeline.run_analysis()
        spec = pipeline.run_suggestion()

        assert len(spec.hubs) > 0
        hub_names = [h.hub_name for h in spec.hubs]
        # Should have at least a customer and order hub
        assert any("customer" in n for n in hub_names) or any("order" in n for n in hub_names)

    def test_generates_satellites(self, spark, sample_tables):
        pipeline = VibeProfilerPipeline(spark, sample_tables)
        pipeline.run_profiling()
        pipeline.run_analysis()
        spec = pipeline.run_suggestion()

        assert len(spec.satellites) > 0
        for sat in spec.satellites:
            assert sat.parent_name  # has a parent hub or link

    def test_source_mappings(self, spark, sample_tables):
        pipeline = VibeProfilerPipeline(spark, sample_tables)
        spec = pipeline.run_suggestion()

        assert len(spec.source_mappings) > 0
        for src, targets in spec.source_mappings.items():
            assert src in sample_tables
            assert len(targets) > 0
