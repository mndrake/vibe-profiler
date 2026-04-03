"""Detect foreign-key-like relationships between tables."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from vibe_profiler.config import AnalysisConfig
from vibe_profiler.models.analysis import BusinessKeyCandidate, Relationship, SimilarityMatch
from vibe_profiler.models.profile import ProfileResult
from vibe_profiler.utils import levenshtein_ratio, normalize_name


class RelationshipAnalyzer:
    """Identify FK-like relationships using three strategies:

    1. Similarity matches (non-BK column similar to another table's BK)
    2. FK name heuristic (column named ``cust_id`` → ``customer_id`` BK)
    3. Value containment (column values are a subset of another table's BK)
    """

    def __init__(
        self,
        spark: SparkSession,
        config: AnalysisConfig | None = None,
        sample_size: int = 10_000,
    ) -> None:
        self.spark = spark
        self.cfg = config or AnalysisConfig()
        self.sample_size = sample_size

    def analyze(
        self,
        profile_result: ProfileResult,
        business_keys: dict[str, tuple[BusinessKeyCandidate, ...]],
        tables: dict[str, DataFrame],
        similarity_matches: tuple[SimilarityMatch, ...] = (),
        changed_tables: set[str] | None = None,
        previous_relationships: tuple[Relationship, ...] = (),
    ) -> tuple[Relationship, ...]:
        # Collect the top BK candidate per table
        bk_columns: dict[str, str] = {}
        for table_name, candidates in business_keys.items():
            if candidates:
                bk_columns[table_name] = candidates[0].column_name

        # Carry forward relationships between unchanged tables
        relationships: list[Relationship] = []
        if changed_tables is not None and previous_relationships:
            for r in previous_relationships:
                if r.parent_table not in changed_tables and r.child_table not in changed_tables:
                    relationships.append(r)
        seen: set[tuple[str, str, str, str]] = set()

        # --- Strategy 1: Similarity matches that link a non-BK to a BK ---
        for match in similarity_matches:
            a_is_bk = bk_columns.get(match.table_a) == match.column_a
            b_is_bk = bk_columns.get(match.table_b) == match.column_b

            if a_is_bk and not b_is_bk:
                parent_table, parent_col = match.table_a, match.column_a
                child_table, child_col = match.table_b, match.column_b
            elif b_is_bk and not a_is_bk:
                parent_table, parent_col = match.table_b, match.column_b
                child_table, child_col = match.table_a, match.column_a
            elif a_is_bk and b_is_bk:
                continue  # hub merge, not a link
            else:
                continue

            key = (parent_table, parent_col, child_table, child_col)
            if key in seen:
                continue
            seen.add(key)

            relationships.append(
                Relationship(
                    parent_table=parent_table,
                    parent_column=parent_col,
                    child_table=child_table,
                    child_column=child_col,
                    confidence=round(match.composite_score, 4),
                    cardinality="1:N",
                )
            )

        # --- Strategy 2: FK name heuristic ---
        # If a non-BK column is named like a known BK (e.g. cust_id -> customer_id),
        # treat it as a FK relationship without needing value overlap.
        for tp in profile_result.tables:
            for cp in tp.column_profiles:
                if cp.column_name == bk_columns.get(tp.table_name):
                    continue
                if cp.null_rate > 0.80:
                    continue

                parent_table = self._name_suggests_fk(
                    cp.column_name, tp.table_name, bk_columns
                )
                if parent_table is None:
                    continue

                parent_bk = bk_columns[parent_table]
                key = (parent_table, parent_bk, tp.table_name, cp.column_name)
                if key in seen:
                    continue
                # Skip unchanged pairs
                if changed_tables is not None:
                    if tp.table_name not in changed_tables and parent_table not in changed_tables:
                        continue
                seen.add(key)

                relationships.append(
                    Relationship(
                        parent_table=parent_table,
                        parent_column=parent_bk,
                        child_table=tp.table_name,
                        child_column=cp.column_name,
                        confidence=0.70,  # name heuristic = moderate confidence
                        cardinality="1:N",
                    )
                )

        # --- Strategy 3: Value containment for non-BK columns ---
        if len(bk_columns) >= 2:
            for tp in profile_result.tables:
                for cp in tp.column_profiles:
                    if cp.column_name == bk_columns.get(tp.table_name):
                        continue
                    if cp.null_rate > 0.80:
                        continue

                    for parent_table, parent_bk in bk_columns.items():
                        if changed_tables is not None:
                            if (
                                tp.table_name not in changed_tables
                                and parent_table not in changed_tables
                            ):
                                continue
                        if parent_table == tp.table_name:
                            continue

                        key = (parent_table, parent_bk, tp.table_name, cp.column_name)
                        if key in seen:
                            continue

                        parent_tp = next(
                            (t for t in profile_result.tables
                             if t.table_name == parent_table),
                            None,
                        )
                        if parent_tp is None:
                            continue
                        parent_cp = next(
                            (c for c in parent_tp.column_profiles
                             if c.column_name == parent_bk),
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

                        if containment >= self.cfg.containment_threshold:
                            seen.add(key)
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

    @staticmethod
    def _name_suggests_fk(
        col_name: str,
        col_table: str,
        bk_columns: dict[str, str],
    ) -> str | None:
        """Check if *col_name* looks like a FK to any known BK.

        Strips common suffixes (_id, _key, _code, _no, _ref, _num, _number)
        from both the column and known BKs, then compares stems.
        """
        _suffixes = ("_id", "_key", "_code", "_no", "_ref", "_num", "_number")
        col_norm = normalize_name(col_name)
        col_stem = col_norm
        for sfx in _suffixes:
            if col_stem.endswith(sfx):
                col_stem = col_stem[: -len(sfx)]
                break

        if not col_stem:
            return None

        for parent_table, parent_bk in bk_columns.items():
            if parent_table == col_table:
                continue
            bk_norm = normalize_name(parent_bk)
            bk_stem = bk_norm
            for sfx in _suffixes:
                if bk_stem.endswith(sfx):
                    bk_stem = bk_stem[: -len(sfx)]
                    break

            if not bk_stem:
                continue

            # Exact stem match or high Levenshtein similarity
            if col_stem == bk_stem or levenshtein_ratio(col_stem, bk_stem) >= 0.75:
                return parent_table

        return None

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
