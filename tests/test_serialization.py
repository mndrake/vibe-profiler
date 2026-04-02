"""Tests for serialization round-trips and incremental profiling."""

from vibe_profiler.models.analysis import (
    AnalysisResult,
    BusinessKeyCandidate,
    Relationship,
    SimilarityMatch,
)
from vibe_profiler.models.profile import ColumnProfile, PatternType, ProfileResult, TableProfile
from vibe_profiler.models.temporal import HistorizationInfo, SCDType, TemporalColumn
from vibe_profiler.serialization import (
    analysis_result_from_dict,
    analysis_result_to_dict,
    load_results,
    profile_result_from_dict,
    profile_result_to_dict,
    save_results,
)


def _sample_column_profile() -> ColumnProfile:
    return ColumnProfile(
        table_name="customers",
        column_name="customer_id",
        spark_type="string",
        row_count=100,
        null_count=0,
        null_rate=0.0,
        distinct_count=100,
        uniqueness=1.0,
        min_value="C001",
        max_value="C100",
        mean_length=4.0,
        max_length=4,
        dominant_pattern=PatternType.ALPHANUMERIC_CODE,
        pattern_coverage=0.95,
        top_values=(("C001", 1), ("C002", 1)),
        is_numeric=False,
        approx_quantiles=None,
    )


def _sample_profile_result() -> ProfileResult:
    return ProfileResult(
        tables=(
            TableProfile(
                table_name="customers",
                catalog="main",
                schema="raw",
                row_count=100,
                column_profiles=(_sample_column_profile(),),
                sampled=False,
                sample_fraction=None,
            ),
        ),
        profiled_at="2026-04-02T15:00:00+00:00",
    )


def _sample_analysis_result() -> AnalysisResult:
    return AnalysisResult(
        business_keys={
            "customers": (
                BusinessKeyCandidate(
                    table_name="customers",
                    column_name="customer_id",
                    score=0.95,
                    uniqueness=1.0,
                    null_rate=0.0,
                    pattern_stability=0.95,
                    reasoning=("High uniqueness (100.00%)", "Name suggests key"),
                ),
            ),
        },
        similarity_matches=(
            SimilarityMatch(
                table_a="customers",
                column_a="customer_id",
                table_b="orders",
                column_b="cust_id",
                name_similarity=0.7,
                statistical_similarity=0.9,
                value_overlap=0.85,
                composite_score=0.82,
                match_type="value_overlap",
            ),
        ),
        relationships=(
            Relationship(
                parent_table="customers",
                parent_column="customer_id",
                child_table="orders",
                child_column="cust_id",
                confidence=0.92,
                cardinality="1:N",
            ),
        ),
        historization={
            "customers": HistorizationInfo(
                table_name="customers",
                scd_type=SCDType.TYPE2,
                temporal_columns=(
                    TemporalColumn(
                        table_name="customers",
                        column_name="valid_from",
                        role="valid_from",
                        confidence=0.9,
                    ),
                ),
                version_key_columns=("customer_id",),
                confidence=0.85,
            ),
        },
    )


class TestProfileResultSerialization:
    def test_round_trip(self):
        original = _sample_profile_result()
        data = profile_result_to_dict(original)
        restored = profile_result_from_dict(data)

        assert restored.profiled_at == original.profiled_at
        assert len(restored.tables) == len(original.tables)

        tp = restored.tables[0]
        assert tp.table_name == "customers"
        assert tp.row_count == 100
        assert tp.catalog == "main"

        cp = tp.column_profiles[0]
        assert cp.column_name == "customer_id"
        assert cp.dominant_pattern == PatternType.ALPHANUMERIC_CODE
        assert cp.top_values == (("C001", 1), ("C002", 1))
        assert cp.uniqueness == 1.0
        assert cp.approx_quantiles is None

    def test_with_quantiles(self):
        cp = ColumnProfile(
            table_name="t", column_name="amount", spark_type="double",
            row_count=50, null_count=0, null_rate=0.0, distinct_count=40,
            uniqueness=0.8, min_value="1.0", max_value="100.0",
            mean_length=None, max_length=None,
            dominant_pattern=PatternType.UNKNOWN, pattern_coverage=0.0,
            top_values=(), is_numeric=True,
            approx_quantiles=(25.0, 50.0, 75.0),
        )
        pr = ProfileResult(
            tables=(TableProfile("t", None, None, 50, (cp,), False, None),),
            profiled_at="2026-01-01",
        )
        restored = profile_result_from_dict(profile_result_to_dict(pr))
        assert restored.tables[0].column_profiles[0].approx_quantiles == (25.0, 50.0, 75.0)


class TestAnalysisResultSerialization:
    def test_round_trip(self):
        original = _sample_analysis_result()
        data = analysis_result_to_dict(original)
        restored = analysis_result_from_dict(data)

        # Business keys
        assert "customers" in restored.business_keys
        bk = restored.business_keys["customers"][0]
        assert bk.column_name == "customer_id"
        assert bk.score == 0.95
        assert bk.reasoning == ("High uniqueness (100.00%)", "Name suggests key")

        # Similarity
        assert len(restored.similarity_matches) == 1
        sm = restored.similarity_matches[0]
        assert sm.table_a == "customers"
        assert sm.composite_score == 0.82

        # Relationships
        assert len(restored.relationships) == 1
        rel = restored.relationships[0]
        assert rel.parent_table == "customers"
        assert rel.cardinality == "1:N"

        # Historization
        assert "customers" in restored.historization
        hist = restored.historization["customers"]
        assert hist.scd_type == SCDType.TYPE2
        assert len(hist.temporal_columns) == 1
        assert hist.temporal_columns[0].role == "valid_from"
        assert hist.version_key_columns == ("customer_id",)


class TestJsonFileIO:
    def test_save_and_load(self, tmp_path):
        path = str(tmp_path / "results.json")
        pr = _sample_profile_result()
        ar = _sample_analysis_result()

        save_results(path, profile_result=pr, analysis_result=ar)
        loaded_pr, loaded_ar = load_results(path)

        assert loaded_pr is not None
        assert loaded_ar is not None
        assert loaded_pr.tables[0].table_name == "customers"
        assert loaded_ar.business_keys["customers"][0].score == 0.95

    def test_save_profile_only(self, tmp_path):
        path = str(tmp_path / "profile_only.json")
        pr = _sample_profile_result()

        save_results(path, profile_result=pr)
        loaded_pr, loaded_ar = load_results(path)

        assert loaded_pr is not None
        assert loaded_ar is None

    def test_save_analysis_only(self, tmp_path):
        path = str(tmp_path / "analysis_only.json")
        ar = _sample_analysis_result()

        save_results(path, analysis_result=ar)
        loaded_pr, loaded_ar = load_results(path)

        assert loaded_pr is None
        assert loaded_ar is not None


class TestIncrementalProfiling:
    def test_incremental_with_new_table(self, spark, sample_tables):
        from vibe_profiler.pipeline import VibeProfilerPipeline

        # First run: profile customers and orders
        pipeline = VibeProfilerPipeline(spark, sample_tables)
        pipeline.run_profiling()

        assert len(pipeline.profile_result.tables) == 2

        original_customers = next(
            t for t in pipeline.profile_result.tables if t.table_name == "customers"
        )

        # Simulate adding a third table by re-profiling only orders
        pipeline.run_profiling(tables_to_refresh={"orders"})

        assert len(pipeline.profile_result.tables) == 2
        # Customers profile should be unchanged (cached)
        cached_customers = next(
            t for t in pipeline.profile_result.tables if t.table_name == "customers"
        )
        assert cached_customers.row_count == original_customers.row_count

    def test_save_load_incremental(self, spark, sample_tables, tmp_path):
        from vibe_profiler.pipeline import VibeProfilerPipeline

        path = str(tmp_path / "cache.json")

        # Run 1: full profiling
        pipeline1 = VibeProfilerPipeline(spark, sample_tables)
        pipeline1.run_all()
        pipeline1.save_results(path)

        # Run 2: load previous, only refresh one table
        pipeline2 = VibeProfilerPipeline(spark, sample_tables)
        pipeline2.load_results(path)

        assert pipeline2.profile_result is not None
        pipeline2.run_profiling(tables_to_refresh={"orders"})
        assert len(pipeline2.profile_result.tables) == 2
