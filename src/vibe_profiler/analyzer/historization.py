"""Detect SCD Type 2 patterns and snapshot-style historization."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from vibe_profiler.models.analysis import BusinessKeyCandidate
from vibe_profiler.models.profile import TableProfile
from vibe_profiler.models.temporal import HistorizationInfo, SCDType
from vibe_profiler.profiler.temporal_detector import detect_temporal_columns


class HistorizationAnalyzer:
    """Detect whether a table uses SCD Type 2 versioning or snapshot patterns."""

    def __init__(
        self,
        version_ratio_threshold: float = 1.3,
        snapshot_distinct_threshold: int = 5,
    ) -> None:
        self.version_ratio_threshold = version_ratio_threshold
        self.snapshot_distinct_threshold = snapshot_distinct_threshold

    def analyze(
        self,
        table_profile: TableProfile,
        df: DataFrame,
        business_key_candidates: tuple[BusinessKeyCandidate, ...],
    ) -> HistorizationInfo:
        temporal_cols = detect_temporal_columns(df, table_profile.table_name)

        if not temporal_cols:
            return HistorizationInfo(
                table_name=table_profile.table_name,
                scd_type=SCDType.NONE,
                temporal_columns=(),
                version_key_columns=(),
                confidence=0.9,
            )

        roles = {tc.role for tc in temporal_cols}

        # --- Check for SCD Type 2 (valid_from / valid_to pairs) ---
        has_valid_from = "valid_from" in roles or "effective_date" in roles
        has_valid_to = "valid_to" in roles

        if has_valid_from and has_valid_to and business_key_candidates:
            bk_col = business_key_candidates[0].column_name
            is_versioned, version_confidence = self._check_versioning(df, bk_col)
            if is_versioned:
                return HistorizationInfo(
                    table_name=table_profile.table_name,
                    scd_type=SCDType.TYPE2,
                    temporal_columns=temporal_cols,
                    version_key_columns=(bk_col,),
                    confidence=version_confidence,
                )

        # --- Check for snapshot pattern ---
        snapshot_cols = [tc for tc in temporal_cols if tc.role == "snapshot_date"]
        if snapshot_cols:
            snap_col = snapshot_cols[0].column_name
            is_snapshot, snap_confidence = self._check_snapshot(df, snap_col)
            if is_snapshot:
                bk_col = business_key_candidates[0].column_name if business_key_candidates else ""
                return HistorizationInfo(
                    table_name=table_profile.table_name,
                    scd_type=SCDType.SNAPSHOT,
                    temporal_columns=temporal_cols,
                    version_key_columns=(bk_col,) if bk_col else (),
                    confidence=snap_confidence,
                )

        # --- Check versioning even without explicit valid_to ---
        if has_valid_from and business_key_candidates:
            bk_col = business_key_candidates[0].column_name
            is_versioned, version_confidence = self._check_versioning(df, bk_col)
            if is_versioned:
                return HistorizationInfo(
                    table_name=table_profile.table_name,
                    scd_type=SCDType.TYPE2,
                    temporal_columns=temporal_cols,
                    version_key_columns=(bk_col,),
                    confidence=version_confidence * 0.8,
                )

        # Temporal columns exist but no clear historization pattern
        return HistorizationInfo(
            table_name=table_profile.table_name,
            scd_type=SCDType.TYPE1,
            temporal_columns=temporal_cols,
            version_key_columns=(),
            confidence=0.5,
        )

    def _check_versioning(
        self, df: DataFrame, bk_column: str
    ) -> tuple[bool, float]:
        """Check if grouping by BK yields multiple rows (versioned records)."""
        try:
            stats = (
                df.groupBy(F.col(f"`{bk_column}`"))
                .agg(F.count("*").alias("_cnt"))
                .agg(
                    F.mean("_cnt").alias("avg_versions"),
                    F.max("_cnt").alias("max_versions"),
                )
                .collect()[0]
            )
            avg = stats["avg_versions"] or 1.0
            max_v = stats["max_versions"] or 1

            if avg >= self.version_ratio_threshold or max_v > 1:
                confidence = min(1.0, 0.5 + (avg - 1.0) * 0.3)
                return True, round(confidence, 4)
        except Exception:
            pass
        return False, 0.0

    def _check_snapshot(
        self, df: DataFrame, snapshot_col: str
    ) -> tuple[bool, float]:
        """Check if a snapshot date column has a small number of distinct dates."""
        try:
            n_dates = df.select(F.col(f"`{snapshot_col}`")).distinct().count()
            if n_dates >= self.snapshot_distinct_threshold:
                return True, min(1.0, 0.6 + n_dates * 0.02)
        except Exception:
            pass
        return False, 0.0
