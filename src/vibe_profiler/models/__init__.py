"""Metadata models shared across pipeline stages."""

from vibe_profiler.models.profile import (
    ColumnProfile,
    InferredType,
    PatternType,
    ProfileResult,
    TableProfile,
)
from vibe_profiler.models.analysis import (
    AnalysisResult,
    BusinessKeyCandidate,
    Relationship,
    SimilarityMatch,
)
from vibe_profiler.models.temporal import HistorizationInfo, SCDType, TemporalColumn
from vibe_profiler.models.vault_spec import (
    DataVaultSpec,
    HubSpec,
    LinkSpec,
    SatelliteSpec,
)

__all__ = [
    "AnalysisResult",
    "BusinessKeyCandidate",
    "ColumnProfile",
    "InferredType",
    "DataVaultSpec",
    "HistorizationInfo",
    "HubSpec",
    "LinkSpec",
    "PatternType",
    "ProfileResult",
    "Relationship",
    "SatelliteSpec",
    "SCDType",
    "SimilarityMatch",
    "TableProfile",
    "TemporalColumn",
]
