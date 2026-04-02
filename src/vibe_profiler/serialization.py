"""Serialize and deserialize profiling/analysis results to/from JSON."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from vibe_profiler._version import __version__
from vibe_profiler.models.analysis import (
    AnalysisResult,
    BusinessKeyCandidate,
    Relationship,
    SimilarityMatch,
)
from vibe_profiler.models.profile import ColumnProfile, PatternType, ProfileResult, TableProfile
from vibe_profiler.models.temporal import HistorizationInfo, SCDType, TemporalColumn


# ---------------------------------------------------------------------------
# ProfileResult serialization
# ---------------------------------------------------------------------------


def profile_result_to_dict(result: ProfileResult) -> dict:
    return {
        "tables": [_table_profile_to_dict(tp) for tp in result.tables],
        "profiled_at": result.profiled_at,
    }


def profile_result_from_dict(data: dict) -> ProfileResult:
    return ProfileResult(
        tables=tuple(_table_profile_from_dict(t) for t in data["tables"]),
        profiled_at=data["profiled_at"],
    )


def _table_profile_to_dict(tp: TableProfile) -> dict:
    return {
        "table_name": tp.table_name,
        "catalog": tp.catalog,
        "schema": tp.schema,
        "row_count": tp.row_count,
        "column_profiles": [_column_profile_to_dict(cp) for cp in tp.column_profiles],
        "sampled": tp.sampled,
        "sample_fraction": tp.sample_fraction,
    }


def _table_profile_from_dict(d: dict) -> TableProfile:
    return TableProfile(
        table_name=d["table_name"],
        catalog=d.get("catalog"),
        schema=d.get("schema"),
        row_count=d["row_count"],
        column_profiles=tuple(
            _column_profile_from_dict(cp) for cp in d["column_profiles"]
        ),
        sampled=d["sampled"],
        sample_fraction=d.get("sample_fraction"),
    )


def _column_profile_to_dict(cp: ColumnProfile) -> dict:
    return {
        "table_name": cp.table_name,
        "column_name": cp.column_name,
        "spark_type": cp.spark_type,
        "row_count": cp.row_count,
        "null_count": cp.null_count,
        "null_rate": cp.null_rate,
        "distinct_count": cp.distinct_count,
        "uniqueness": cp.uniqueness,
        "min_value": cp.min_value,
        "max_value": cp.max_value,
        "mean_length": cp.mean_length,
        "max_length": cp.max_length,
        "dominant_pattern": cp.dominant_pattern.value,
        "pattern_coverage": cp.pattern_coverage,
        "top_values": [list(pair) for pair in cp.top_values],
        "is_numeric": cp.is_numeric,
        "approx_quantiles": list(cp.approx_quantiles) if cp.approx_quantiles else None,
    }


def _column_profile_from_dict(d: dict) -> ColumnProfile:
    top_values = tuple(tuple(pair) for pair in d["top_values"])
    quantiles = tuple(d["approx_quantiles"]) if d.get("approx_quantiles") else None
    return ColumnProfile(
        table_name=d["table_name"],
        column_name=d["column_name"],
        spark_type=d["spark_type"],
        row_count=d["row_count"],
        null_count=d["null_count"],
        null_rate=d["null_rate"],
        distinct_count=d["distinct_count"],
        uniqueness=d["uniqueness"],
        min_value=d.get("min_value"),
        max_value=d.get("max_value"),
        mean_length=d.get("mean_length"),
        max_length=d.get("max_length"),
        dominant_pattern=PatternType(d["dominant_pattern"]),
        pattern_coverage=d["pattern_coverage"],
        top_values=top_values,
        is_numeric=d["is_numeric"],
        approx_quantiles=quantiles,
    )


# ---------------------------------------------------------------------------
# AnalysisResult serialization
# ---------------------------------------------------------------------------


def analysis_result_to_dict(result: AnalysisResult) -> dict:
    return {
        "business_keys": {
            table: [_bk_to_dict(bk) for bk in candidates]
            for table, candidates in result.business_keys.items()
        },
        "similarity_matches": [_sim_to_dict(m) for m in result.similarity_matches],
        "relationships": [_rel_to_dict(r) for r in result.relationships],
        "historization": {
            table: _hist_to_dict(h)
            for table, h in result.historization.items()
        },
    }


def analysis_result_from_dict(data: dict) -> AnalysisResult:
    return AnalysisResult(
        business_keys={
            table: tuple(_bk_from_dict(bk) for bk in candidates)
            for table, candidates in data["business_keys"].items()
        },
        similarity_matches=tuple(
            _sim_from_dict(m) for m in data["similarity_matches"]
        ),
        relationships=tuple(
            _rel_from_dict(r) for r in data["relationships"]
        ),
        historization={
            table: _hist_from_dict(h)
            for table, h in data["historization"].items()
        },
    )


def _bk_to_dict(bk: BusinessKeyCandidate) -> dict:
    return {
        "table_name": bk.table_name,
        "column_name": bk.column_name,
        "score": bk.score,
        "uniqueness": bk.uniqueness,
        "null_rate": bk.null_rate,
        "pattern_stability": bk.pattern_stability,
        "reasoning": list(bk.reasoning),
    }


def _bk_from_dict(d: dict) -> BusinessKeyCandidate:
    return BusinessKeyCandidate(
        table_name=d["table_name"],
        column_name=d["column_name"],
        score=d["score"],
        uniqueness=d["uniqueness"],
        null_rate=d["null_rate"],
        pattern_stability=d["pattern_stability"],
        reasoning=tuple(d["reasoning"]),
    )


def _sim_to_dict(m: SimilarityMatch) -> dict:
    return {
        "table_a": m.table_a,
        "column_a": m.column_a,
        "table_b": m.table_b,
        "column_b": m.column_b,
        "name_similarity": m.name_similarity,
        "statistical_similarity": m.statistical_similarity,
        "value_overlap": m.value_overlap,
        "composite_score": m.composite_score,
        "match_type": m.match_type,
    }


def _sim_from_dict(d: dict) -> SimilarityMatch:
    return SimilarityMatch(**d)


def _rel_to_dict(r: Relationship) -> dict:
    return {
        "parent_table": r.parent_table,
        "parent_column": r.parent_column,
        "child_table": r.child_table,
        "child_column": r.child_column,
        "confidence": r.confidence,
        "cardinality": r.cardinality,
    }


def _rel_from_dict(d: dict) -> Relationship:
    return Relationship(**d)


def _hist_to_dict(h: HistorizationInfo) -> dict:
    return {
        "table_name": h.table_name,
        "scd_type": h.scd_type.value,
        "temporal_columns": [
            {
                "table_name": tc.table_name,
                "column_name": tc.column_name,
                "role": tc.role,
                "confidence": tc.confidence,
            }
            for tc in h.temporal_columns
        ],
        "version_key_columns": list(h.version_key_columns),
        "confidence": h.confidence,
    }


def _hist_from_dict(d: dict) -> HistorizationInfo:
    return HistorizationInfo(
        table_name=d["table_name"],
        scd_type=SCDType(d["scd_type"]),
        temporal_columns=tuple(
            TemporalColumn(
                table_name=tc["table_name"],
                column_name=tc["column_name"],
                role=tc["role"],
                confidence=tc["confidence"],
            )
            for tc in d["temporal_columns"]
        ),
        version_key_columns=tuple(d["version_key_columns"]),
        confidence=d["confidence"],
    )


# ---------------------------------------------------------------------------
# JSON file I/O
# ---------------------------------------------------------------------------


def save_results(
    path: str,
    profile_result: ProfileResult | None = None,
    analysis_result: AnalysisResult | None = None,
) -> None:
    """Save profiling and/or analysis results to a JSON file."""
    payload: dict[str, Any] = {"version": __version__}
    if profile_result is not None:
        payload["profile_result"] = profile_result_to_dict(profile_result)
    if analysis_result is not None:
        payload["analysis_result"] = analysis_result_to_dict(analysis_result)

    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(payload, indent=2))


def load_results(
    path: str,
) -> tuple[ProfileResult | None, AnalysisResult | None]:
    """Load profiling and/or analysis results from a JSON file.

    Returns ``(profile_result, analysis_result)`` — either may be ``None``
    if the file does not contain that section.
    """
    data = json.loads(Path(path).read_text())

    pr = None
    if "profile_result" in data:
        pr = profile_result_from_dict(data["profile_result"])

    ar = None
    if "analysis_result" in data:
        ar = analysis_result_from_dict(data["analysis_result"])

    return pr, ar
