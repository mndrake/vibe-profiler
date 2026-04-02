"""Global configuration for the profiling pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ProfilingConfig:
    """Controls how raw data is profiled."""

    sample_fraction: float | None = None
    sample_threshold_rows: int = 10_000_000
    max_top_values: int = 20
    approx_quantile_error: float = 0.01
    approx_distinct: bool = True
    column_batch_size: int = 50


@dataclass
class AnalysisConfig:
    """Controls business-key scoring and similarity detection."""

    # Business key scoring weights
    uniqueness_weight: float = 0.40
    null_rate_weight: float = 0.20
    pattern_weight: float = 0.20
    name_heuristic_weight: float = 0.20
    uniqueness_threshold: float = 0.95
    null_rate_threshold: float = 0.01

    # Similarity detection
    name_similarity_weight: float = 0.25
    stat_similarity_weight: float = 0.25
    value_overlap_weight: float = 0.50
    min_composite_score: float = 0.60
    value_sample_size: int = 10_000


@dataclass
class VaultConfig:
    """Controls Data Vault naming and structure."""

    hub_prefix: str = "hub_"
    link_prefix: str = "lnk_"
    sat_prefix: str = "sat_"
    hash_function: str = "md5"
    hash_key_suffix: str = "_hk"
    hashdiff_name: str = "hashdiff"


@dataclass
class PipelineConfig:
    """Top-level configuration aggregating all sub-configs."""

    profiling: ProfilingConfig = field(default_factory=ProfilingConfig)
    analysis: AnalysisConfig = field(default_factory=AnalysisConfig)
    vault: VaultConfig = field(default_factory=VaultConfig)
