"""Score columns on likelihood of being a business key."""

from __future__ import annotations

import re
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from vibe_profiler.config import AnalysisConfig
from vibe_profiler.models.profile import PatternType, TableProfile
from vibe_profiler.models.analysis import BusinessKeyCandidate
from vibe_profiler.models.temporal import TemporalColumn

# Column-name fragments that strongly suggest a business key.
_BK_NAME_HINTS = re.compile(
    r"(_id|_key|_code|_no|_num|_number|_ref|_identifier)$|^id$|^key$",
    re.IGNORECASE,
)

# Patterns frequently seen in business keys.
_BK_PATTERNS = {
    PatternType.UUID,
    PatternType.NUMERIC_CODE,
    PatternType.ALPHANUMERIC_CODE,
}


class BusinessKeyAnalyzer:
    """Rank columns within a table by their likelihood of being a business key.

    For SCD Type 2 tables (with start/end date columns), uniqueness is
    computed after deduplication — grouping by the candidate column and
    checking if each value maps to multiple versioned rows.
    """

    def __init__(self, config: AnalysisConfig | None = None) -> None:
        self.cfg = config or AnalysisConfig()

    def analyze(
        self,
        table_profile: TableProfile,
        df: Optional[DataFrame] = None,
        temporal_columns: tuple[TemporalColumn, ...] = (),
    ) -> tuple[BusinessKeyCandidate, ...]:
        """Score columns as BK candidates.

        Args:
            df: The source DataFrame.  Required for SCD2 dedup uniqueness.
            temporal_columns: Detected temporal columns.  When start/end
                columns are present, uniqueness is re-computed on the
                deduplicated (current-version-only) data.
        """
        # Detect if this is an SCD2 table with versioned rows
        dedup_uniqueness = self._compute_dedup_uniqueness(
            table_profile, df, temporal_columns
        )

        candidates: list[BusinessKeyCandidate] = []

        # Temporal column names to exclude from BK consideration
        temporal_names = {tc.column_name for tc in temporal_columns}

        for cp in table_profile.column_profiles:
            # Skip temporal columns — they're never BKs
            if cp.column_name in temporal_names:
                continue

            reasons: list[str] = []

            # --- Hard disqualifiers ---
            if cp.null_rate > 0.10:
                continue

            # --- uniqueness score ---
            # Use deduplicated uniqueness if available (SCD2 tables)
            uniqueness = dedup_uniqueness.get(cp.column_name, cp.uniqueness)

            if uniqueness >= self.cfg.uniqueness_threshold:
                u_score = 1.0
                if uniqueness != cp.uniqueness:
                    reasons.append(
                        f"High uniqueness after dedup ({uniqueness:.2%}, "
                        f"raw: {cp.uniqueness:.2%})"
                    )
                else:
                    reasons.append(f"High uniqueness ({uniqueness:.2%})")
            else:
                u_score = uniqueness / self.cfg.uniqueness_threshold

            # --- null rate score (lower is better) ---
            if cp.null_rate <= self.cfg.null_rate_threshold:
                n_score = 1.0
                reasons.append("Low null rate")
            else:
                n_score = max(0.0, 1.0 - cp.null_rate)

            # --- pattern score ---
            if cp.dominant_pattern in _BK_PATTERNS and cp.pattern_coverage >= 0.80:
                p_score = cp.pattern_coverage
                reasons.append(f"BK-typical pattern ({cp.dominant_pattern.value})")
            else:
                p_score = 0.0

            # --- name heuristic ---
            if _BK_NAME_HINTS.search(cp.column_name):
                name_score = 1.0
                reasons.append(f"Name suggests key ({cp.column_name})")
            else:
                name_score = 0.0

            score = (
                self.cfg.uniqueness_weight * u_score
                + self.cfg.null_rate_weight * n_score
                + self.cfg.pattern_weight * p_score
                + self.cfg.name_heuristic_weight * name_score
            )

            if score >= 0.30:
                candidates.append(
                    BusinessKeyCandidate(
                        table_name=table_profile.table_name,
                        column_name=cp.column_name,
                        score=round(score, 4),
                        uniqueness=uniqueness,
                        null_rate=cp.null_rate,
                        pattern_stability=cp.pattern_coverage,
                        reasoning=tuple(reasons),
                    )
                )

        candidates.sort(key=lambda c: c.score, reverse=True)
        return tuple(candidates)

    @staticmethod
    def _compute_dedup_uniqueness(
        table_profile: TableProfile,
        df: Optional[DataFrame],
        temporal_columns: tuple[TemporalColumn, ...],
    ) -> dict[str, float]:
        """For SCD2 tables, compute uniqueness on deduplicated data.

        Groups by candidate column and checks distinct count against the
        number of unique "entity" rows (rows with distinct BK values,
        ignoring version duplicates).

        Returns ``{column_name: dedup_uniqueness}`` for columns that
        differ from their raw uniqueness.  Empty dict if not SCD2.
        """
        if df is None:
            return {}

        # Need both a start and end temporal column to identify SCD2
        roles = {tc.role for tc in temporal_columns}
        has_start = "valid_from" in roles or "effective_date" in roles
        has_end = "valid_to" in roles

        if not (has_start and has_end):
            return {}

        # Find the end-date column to filter for current records
        end_col_name = next(
            (tc.column_name for tc in temporal_columns if tc.role == "valid_to"),
            None,
        )
        if end_col_name is None:
            return {}

        # Deduplicate: take only current records (max end date per entity)
        # Strategy: count distinct values of each candidate column,
        # then count distinct values when grouped (= number of unique entities)
        temporal_names = {tc.column_name for tc in temporal_columns}
        candidate_cols = [
            cp.column_name
            for cp in table_profile.column_profiles
            if cp.column_name not in temporal_names
            and cp.null_rate <= 0.10
        ]

        if not candidate_cols:
            return {}

        # Count distinct values for each candidate column
        # This gives us "how many unique values does this column have?"
        agg_exprs = [
            F.countDistinct(F.col(f"`{col}`")).alias(f"{col}__distinct")
            for col in candidate_cols
        ]
        row = df.agg(*agg_exprs).collect()[0]

        # Total unique entities = count of distinct groups when you pick
        # one row per entity.  We approximate this as the max distinct count
        # among high-uniqueness columns — but a better approach is to count
        # rows after dedup by end_col.
        #
        # For SCD2: current records have end_date = max(end_date) for their group.
        # Simpler: just count distinct values and divide by distinct count of
        # a reference (the total deduped entity count).
        #
        # Approach: the number of unique entities is the number of distinct
        # values of the most-unique non-temporal column.  For each column,
        # dedup_uniqueness = distinct_count / entity_count.
        # If a column has distinct_count == entity_count, it's the BK.

        total_rows = table_profile.row_count
        distinct_counts = {
            col: row[f"{col}__distinct"] or 0 for col in candidate_cols
        }

        # Entity count = max distinct count (the true BK has the most distinct values)
        entity_count = max(distinct_counts.values()) if distinct_counts else 0
        if entity_count == 0:
            return {}

        result: dict[str, float] = {}
        for col in candidate_cols:
            raw_uniqueness = distinct_counts[col] / total_rows if total_rows else 0.0
            dedup_uniqueness = min(
                distinct_counts[col] / entity_count, 1.0
            ) if entity_count else 0.0

            # Only report dedup uniqueness if it differs meaningfully from raw
            if abs(dedup_uniqueness - raw_uniqueness) > 0.05:
                result[col] = round(dedup_uniqueness, 6)

        return result
