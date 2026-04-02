"""Detect columns across tables that likely represent the same business concept."""

from __future__ import annotations

from itertools import combinations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from vibe_profiler.config import AnalysisConfig
from vibe_profiler.models.analysis import SimilarityMatch
from vibe_profiler.models.profile import ColumnProfile, ProfileResult
from vibe_profiler.utils import levenshtein_ratio, name_tokens, jaccard_similarity, normalize_name


class CrossTableSimilarity:
    """Find matching columns across different source tables."""

    def __init__(
        self,
        spark: SparkSession,
        config: AnalysisConfig | None = None,
    ) -> None:
        self.spark = spark
        self.cfg = config or AnalysisConfig()

    def find_matches(
        self,
        profile_result: ProfileResult,
        tables: dict[str, DataFrame],
    ) -> tuple[SimilarityMatch, ...]:
        """Compare every column pair across tables and return matches above threshold."""
        # Build a flat list of (table_name, ColumnProfile) for cross-table pairs
        all_columns: list[tuple[str, ColumnProfile]] = []
        for tp in profile_result.tables:
            for cp in tp.column_profiles:
                all_columns.append((tp.table_name, cp))

        matches: list[SimilarityMatch] = []

        for (t_a, cp_a), (t_b, cp_b) in combinations(all_columns, 2):
            if t_a == t_b:
                continue
            # Quick type-compatibility filter
            if cp_a.is_numeric != cp_b.is_numeric:
                continue
            if cp_a.dominant_pattern != cp_b.dominant_pattern:
                # Allow mismatch only if both are code-like
                code_patterns = {"numeric_code", "alphanumeric_code", "uuid"}
                if not (
                    cp_a.dominant_pattern.value in code_patterns
                    and cp_b.dominant_pattern.value in code_patterns
                ):
                    continue

            name_sim = self._name_similarity(cp_a.column_name, cp_b.column_name)
            stat_sim = self._stat_similarity(cp_a, cp_b)

            # Only compute expensive value overlap if cheap checks look promising
            cheap_score = (
                self.cfg.name_similarity_weight * name_sim
                + self.cfg.stat_similarity_weight * stat_sim
            )
            if cheap_score < self.cfg.min_composite_score * 0.4:
                continue

            val_overlap = self._value_overlap(
                tables.get(t_a), cp_a.column_name,
                tables.get(t_b), cp_b.column_name,
            )

            composite = (
                self.cfg.name_similarity_weight * name_sim
                + self.cfg.stat_similarity_weight * stat_sim
                + self.cfg.value_overlap_weight * val_overlap
            )

            if composite >= self.cfg.min_composite_score:
                match_type = self._classify_match(name_sim, stat_sim, val_overlap)
                matches.append(
                    SimilarityMatch(
                        table_a=t_a,
                        column_a=cp_a.column_name,
                        table_b=t_b,
                        column_b=cp_b.column_name,
                        name_similarity=round(name_sim, 4),
                        statistical_similarity=round(stat_sim, 4),
                        value_overlap=round(val_overlap, 4),
                        composite_score=round(composite, 4),
                        match_type=match_type,
                    )
                )

        matches.sort(key=lambda m: m.composite_score, reverse=True)
        return tuple(matches)

    # ------------------------------------------------------------------
    # Internal scoring helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _name_similarity(name_a: str, name_b: str) -> float:
        """Combine Levenshtein on full names + Jaccard on tokens."""
        lev = levenshtein_ratio(normalize_name(name_a), normalize_name(name_b))
        jac = jaccard_similarity(name_tokens(name_a), name_tokens(name_b))
        return max(lev, jac)

    @staticmethod
    def _stat_similarity(a: ColumnProfile, b: ColumnProfile) -> float:
        """Compare statistical profiles (uniqueness, null rate, length)."""
        scores: list[float] = []
        scores.append(1.0 - abs(a.uniqueness - b.uniqueness))
        scores.append(1.0 - abs(a.null_rate - b.null_rate))

        if a.mean_length is not None and b.mean_length is not None:
            max_len = max(a.mean_length, b.mean_length, 1.0)
            scores.append(1.0 - abs(a.mean_length - b.mean_length) / max_len)

        if a.max_length is not None and b.max_length is not None:
            max_ml = max(a.max_length, b.max_length, 1)
            scores.append(1.0 - abs(a.max_length - b.max_length) / max_ml)

        return sum(scores) / len(scores) if scores else 0.0

    def _value_overlap(
        self,
        df_a: DataFrame | None,
        col_a: str,
        df_b: DataFrame | None,
        col_b: str,
    ) -> float:
        """Estimate Jaccard similarity of actual values via sampling."""
        if df_a is None or df_b is None:
            return 0.0

        n = self.cfg.value_sample_size
        vals_a = (
            df_a.select(F.col(f"`{col_a}`").cast("string").alias("v"))
            .filter(F.col("v").isNotNull())
            .distinct()
            .limit(n)
        )
        vals_b = (
            df_b.select(F.col(f"`{col_b}`").cast("string").alias("v"))
            .filter(F.col("v").isNotNull())
            .distinct()
            .limit(n)
        )

        intersection_count = vals_a.intersect(vals_b).count()
        union_count = vals_a.union(vals_b).distinct().count()

        return intersection_count / union_count if union_count else 0.0

    @staticmethod
    def _classify_match(name_sim: float, stat_sim: float, val_overlap: float) -> str:
        if name_sim >= 0.95:
            return "exact_name"
        if val_overlap >= 0.70:
            return "value_overlap"
        if stat_sim >= 0.85:
            return "statistical"
        return "semantic"
