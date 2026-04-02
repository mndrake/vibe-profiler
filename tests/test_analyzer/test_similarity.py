"""Tests for cross-table column similarity detection."""


from vibe_profiler.analyzer.similarity import CrossTableSimilarity
from vibe_profiler.profiler.engine import ProfileEngine


class TestCrossTableSimilarity:
    def test_finds_customer_id_match(self, spark, sample_tables):
        engine = ProfileEngine(spark)
        profile = engine.profile_tables(sample_tables)

        sim = CrossTableSimilarity(spark)
        matches = sim.find_matches(profile, sample_tables)

        # customer_id <-> cust_id should be detected
        found = any(
            ("customer_id" in (m.column_a, m.column_b) and "cust_id" in (m.column_a, m.column_b))
            for m in matches
        )
        assert found, f"Expected customer_id <-> cust_id match. Got: {matches}"

    def test_no_self_table_matches(self, spark, sample_tables):
        engine = ProfileEngine(spark)
        profile = engine.profile_tables(sample_tables)

        sim = CrossTableSimilarity(spark)
        matches = sim.find_matches(profile, sample_tables)

        for m in matches:
            assert m.table_a != m.table_b
