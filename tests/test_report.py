"""Tests for the findings report generator."""

from vibe_profiler.models.analysis import (
    AnalysisResult,
    BusinessKeyCandidate,
    Relationship,
    SimilarityMatch,
)
from vibe_profiler.models.profile import ColumnProfile, PatternType, ProfileResult, TableProfile
from vibe_profiler.models.temporal import HistorizationInfo, SCDType, TemporalColumn
from vibe_profiler.models.vault_spec import DataVaultSpec, HubSpec, LinkSpec, SatelliteSpec
from vibe_profiler.report import ReportGenerator


def _make_profile() -> ProfileResult:
    cp = ColumnProfile(
        table_name="customers", column_name="customer_id", spark_type="string",
        row_count=1000, null_count=0, null_rate=0.0, distinct_count=1000,
        uniqueness=1.0, min_value="C001", max_value="C999",
        mean_length=4.0, max_length=4, dominant_pattern=PatternType.ALPHANUMERIC_CODE,
        pattern_coverage=0.95, top_values=(("C001", 1),), is_numeric=False,
        approx_quantiles=None,
    )
    tp = TableProfile("customers", "main", "raw", 1000, (cp,), False, None)
    return ProfileResult(tables=(tp,), profiled_at="2026-04-02T15:00:00")


def _make_analysis() -> AnalysisResult:
    return AnalysisResult(
        business_keys={
            "customers": (
                BusinessKeyCandidate(
                    "customers", "customer_id", 0.95, 1.0, 0.0, 0.95,
                    ("High uniqueness", "Name suggests key"),
                ),
            ),
        },
        similarity_matches=(
            SimilarityMatch(
                "customers", "customer_id", "orders", "cust_id",
                0.7, 0.9, 0.85, 0.82, "value_overlap",
            ),
        ),
        relationships=(
            Relationship("customers", "customer_id", "orders", "cust_id", 0.92, "1:N"),
        ),
        historization={
            "customers": HistorizationInfo(
                "customers", SCDType.TYPE2,
                (TemporalColumn("customers", "valid_from", "valid_from", 0.9),),
                ("customer_id",), 0.85,
            ),
        },
    )


def _make_vault_spec() -> DataVaultSpec:
    return DataVaultSpec(
        hubs=(HubSpec("hub_customer", ("customer_id",), ("customers",), "customer_hk"),),
        links=(
            LinkSpec(
                "lnk_customer_order", ("hub_customer", "hub_order"),
                {"hub_customer": ("cust_id",), "hub_order": ("order_id",)},
                ("orders",), "customer_order_hk",
            ),
        ),
        satellites=(
            SatelliteSpec(
                "sat_customer_customers", "hub_customer", "hub",
                ("name", "email", "phone"), "customers", "hashdiff",
                False, SCDType.NONE,
            ),
        ),
    )


def _make_files() -> dict[str, str]:
    return {
        "models/staging/stg_customers.sql": "...",
        "models/raw_vault/hubs/hub_customer.sql": "...",
        "models/raw_vault/links/lnk_customer_order.sql": "...",
        "models/raw_vault/satellites/sat_customer_customers.sql": "...",
        "models/staging/sources.yml": "...",
        "models/raw_vault/schema.yml": "...",
    }


class TestTextReport:
    def test_contains_profiling_summary(self):
        rg = ReportGenerator(profile_result=_make_profile())
        text = rg.to_text()
        assert "PROFILING SUMMARY" in text
        assert "customers" in text
        assert "1,000" in text

    def test_contains_business_keys(self):
        rg = ReportGenerator(
            profile_result=_make_profile(), analysis_result=_make_analysis()
        )
        text = rg.to_text()
        assert "BUSINESS KEY CANDIDATES" in text
        assert "customer_id" in text
        assert "0.95" in text

    def test_contains_cross_table(self):
        rg = ReportGenerator(analysis_result=_make_analysis())
        text = rg.to_text()
        assert "CROSS-TABLE MATCHES" in text
        assert "cust_id" in text
        assert "1:N" in text

    def test_contains_historization(self):
        rg = ReportGenerator(analysis_result=_make_analysis())
        text = rg.to_text()
        assert "HISTORIZATION" in text
        assert "type2" in text
        assert "valid_from" in text

    def test_contains_vault_spec(self):
        rg = ReportGenerator(vault_spec=_make_vault_spec())
        text = rg.to_text()
        assert "DATA VAULT SUGGESTION" in text
        assert "hub_customer" in text
        assert "lnk_customer_order" in text
        assert "sat_customer_customers" in text

    def test_contains_generated_files(self):
        rg = ReportGenerator(generated_files=_make_files())
        text = rg.to_text()
        assert "GENERATED FILES" in text
        assert "stg_customers.sql" in text
        assert "hub_customer.sql" in text

    def test_full_report(self):
        rg = ReportGenerator(
            profile_result=_make_profile(),
            analysis_result=_make_analysis(),
            vault_spec=_make_vault_spec(),
            generated_files=_make_files(),
        )
        text = rg.to_text()
        assert "VIBE PROFILER REPORT" in text
        # All sections present
        for section in [
            "PROFILING SUMMARY",
            "BUSINESS KEY CANDIDATES",
            "CROSS-TABLE MATCHES",
            "HISTORIZATION",
            "DATA VAULT SUGGESTION",
            "GENERATED FILES",
        ]:
            assert section in text


class TestHtmlReport:
    def test_contains_html_structure(self):
        rg = ReportGenerator(
            profile_result=_make_profile(),
            analysis_result=_make_analysis(),
            vault_spec=_make_vault_spec(),
            generated_files=_make_files(),
        )
        html = rg.to_html()
        assert "<h1" in html
        assert "<table" in html
        assert "hub_customer" in html
        assert "customer_id" in html

    def test_confidence_color_coding(self):
        rg = ReportGenerator(analysis_result=_make_analysis())
        html = rg.to_html()
        # High confidence should use green
        assert "#16a34a" in html

    def test_collapsible_column_details(self):
        rg = ReportGenerator(profile_result=_make_profile())
        html = rg.to_html()
        assert "<details>" in html
        assert "<summary" in html


class TestPipelineReport:
    def test_pipeline_report_text(self, spark, sample_tables):
        from vibe_profiler.pipeline import VibeProfilerPipeline

        pipeline = VibeProfilerPipeline(spark, sample_tables)
        pipeline.run_all()
        text = pipeline.report()
        assert "VIBE PROFILER REPORT" in text
        assert "customers" in text

    def test_pipeline_report_html(self, spark, sample_tables):
        from vibe_profiler.pipeline import VibeProfilerPipeline

        pipeline = VibeProfilerPipeline(spark, sample_tables)
        pipeline.run_all()
        html = pipeline.report(format="html")
        assert "<h1" in html
        assert "customers" in html
