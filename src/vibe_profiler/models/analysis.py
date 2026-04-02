"""Analysis-stage result models."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BusinessKeyCandidate:
    table_name: str
    column_name: str
    score: float
    uniqueness: float
    null_rate: float
    pattern_stability: float
    reasoning: tuple[str, ...]


@dataclass(frozen=True)
class SimilarityMatch:
    table_a: str
    column_a: str
    table_b: str
    column_b: str
    name_similarity: float
    statistical_similarity: float
    value_overlap: float
    composite_score: float
    match_type: str


@dataclass(frozen=True)
class Relationship:
    parent_table: str
    parent_column: str
    child_table: str
    child_column: str
    confidence: float
    cardinality: str


@dataclass(frozen=True)
class AnalysisResult:
    """Aggregated output of all analyzers."""

    business_keys: dict[str, tuple[BusinessKeyCandidate, ...]]
    similarity_matches: tuple[SimilarityMatch, ...]
    relationships: tuple[Relationship, ...]
    historization: dict[str, "HistorizationInfo"]

    def __post_init__(self) -> None:
        # Allow late import to avoid circular dependency
        pass


# Avoid circular import at module level
from vibe_profiler.models.temporal import HistorizationInfo as HistorizationInfo  # noqa: E402, F811
