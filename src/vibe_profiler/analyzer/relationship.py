"""Detect foreign-key-like relationships between tables."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from vibe_profiler.models.analysis import BusinessKeyCandidate, Relationship
from vibe_profiler.models.profile import ProfileResult


class RelationshipAnalyzer:
    """Identify FK-like relationships by checking value containment between columns."""

    def __init__(
        self,
        spark: SparkSession,
        containment_threshold: float = 0.85,
        sample_size: int = 10_000,
    ) -> None:
        self.spark = spark
        self.containment_threshold = containment_threshold
        self.sample_size = sample_size

    def analyze(
        self,
        profile_result: ProfileResult,
        business_keys: dict[str, tuple[BusinessKeyCandidate, ...]],
        tables: dict[str, DataFrame],
    ) -> tuple[Relationship, ...]:
        """Find relationships where one table's column values are a subset of another's BK."""
        # Collect the top BK candidate per table
        bk_columns: dict[str, str] = {}
        for table_name, candidates in business_keys.items():
            if candidates:
                bk_columns[table_name] = candidates[0].column_name

        if len(bk_columns) < 2:
            return ()

        relationships: list[Relationship] = []

        # For each non-BK column in each table, check if its values are contained
        # in some other table's BK column
        for tp in profile_result.tables:
            for cp in tp.column_profiles:
                if cp.column_name == bk_columns.get(tp.table_name):
                    continue  # skip the table's own BK
                if cp.uniqueness < 0.01 or cp.null_rate > 0.50:
                    continue  # unlikely FK

                for parent_table, parent_bk in bk_columns.items():
                    if parent_table == tp.table_name:
                        continue

                    # Quick type check
                    parent_tp = next(
                        (t for t in profile_result.tables if t.table_name == parent_table),
                        None,
                    )
                    if parent_tp is None:
                        continue
                    parent_cp = next(
                        (c for c in parent_tp.column_profiles if c.column_name == parent_bk),
                        None,
                    )
                    if parent_cp is None:
                        continue
                    if cp.is_numeric != parent_cp.is_numeric:
                        continue

                    containment = self._check_containment(
                        tables[tp.table_name],
                        cp.column_name,
                        tables[parent_table],
                        parent_bk,
                    )

                    if containment >= self.containment_threshold:
                        cardinality = self._estimate_cardinality(
                            cp.uniqueness, parent_cp.uniqueness
                        )
                        relationships.append(
                            Relationship(
                                parent_table=parent_table,
                                parent_column=parent_bk,
                                child_table=tp.table_name,
                                child_column=cp.column_name,
                                confidence=round(containment, 4),
                                cardinality=cardinality,
                            )
                        )

        relationships.sort(key=lambda r: r.confidence, reverse=True)
        return tuple(relationships)

    def _check_containment(
        self,
        child_df: DataFrame,
        child_col: str,
        parent_df: DataFrame,
        parent_col: str,
    ) -> float:
        """Fraction of child column's distinct values that exist in parent column."""
        child_vals = (
            child_df.select(F.col(f"`{child_col}`").cast("string").alias("v"))
            .filter(F.col("v").isNotNull())
            .distinct()
            .limit(self.sample_size)
        )
        parent_vals = (
            parent_df.select(F.col(f"`{parent_col}`").cast("string").alias("v"))
            .filter(F.col("v").isNotNull())
            .distinct()
            .limit(self.sample_size)
        )

        child_count = child_vals.count()
        if child_count == 0:
            return 0.0

        contained = child_vals.intersect(parent_vals).count()
        return contained / child_count

    @staticmethod
    def _estimate_cardinality(child_uniqueness: float, parent_uniqueness: float) -> str:
        if child_uniqueness >= 0.95 and parent_uniqueness >= 0.95:
            return "1:1"
        if parent_uniqueness >= 0.95:
            return "1:N"
        return "N:M"
