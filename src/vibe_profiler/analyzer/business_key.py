"""Score columns on likelihood of being a business key."""

from __future__ import annotations

import re

from vibe_profiler.config import AnalysisConfig
from vibe_profiler.models.profile import PatternType, TableProfile
from vibe_profiler.models.analysis import BusinessKeyCandidate

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
    """Rank columns within a table by their likelihood of being a business key."""

    def __init__(self, config: AnalysisConfig | None = None) -> None:
        self.cfg = config or AnalysisConfig()

    def analyze(self, table_profile: TableProfile) -> tuple[BusinessKeyCandidate, ...]:
        candidates: list[BusinessKeyCandidate] = []

        for cp in table_profile.column_profiles:
            reasons: list[str] = []
            # --- uniqueness score ---
            if cp.uniqueness >= self.cfg.uniqueness_threshold:
                u_score = 1.0
                reasons.append(f"High uniqueness ({cp.uniqueness:.2%})")
            else:
                u_score = cp.uniqueness / self.cfg.uniqueness_threshold

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
                        uniqueness=cp.uniqueness,
                        null_rate=cp.null_rate,
                        pattern_stability=cp.pattern_coverage,
                        reasoning=tuple(reasons),
                    )
                )

        candidates.sort(key=lambda c: c.score, reverse=True)
        return tuple(candidates)
