"""Column and table profiling result models."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class PatternType(Enum):
    UUID = "uuid"
    EMAIL = "email"
    PHONE = "phone"
    DATE_ISO = "date_iso"
    NUMERIC_CODE = "numeric_code"
    ALPHANUMERIC_CODE = "alphanumeric_code"
    FREE_TEXT = "free_text"
    BOOLEAN = "boolean"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class InferredType:
    """Result of semantic type inference for a string column."""

    spark_target_type: str  # "bigint", "double", "date", "timestamp", "boolean", "string"
    format_string: Optional[str]  # e.g. "MM/dd/yyyy" for dates, None otherwise
    confidence: float  # fraction of non-null values that parsed successfully
    sample_values: tuple[str, ...]  # a few example raw values


@dataclass(frozen=True)
class ColumnProfile:
    table_name: str
    column_name: str
    spark_type: str
    row_count: int
    null_count: int
    null_rate: float
    distinct_count: int
    uniqueness: float
    min_value: Optional[str]
    max_value: Optional[str]
    mean_length: Optional[float]
    max_length: Optional[int]
    dominant_pattern: PatternType
    pattern_coverage: float
    top_values: tuple[tuple[str, int], ...]
    is_numeric: bool
    approx_quantiles: Optional[tuple[float, ...]]
    inferred_type: Optional[InferredType] = None


@dataclass(frozen=True)
class TableProfile:
    table_name: str
    catalog: Optional[str]
    schema: Optional[str]
    row_count: int
    column_profiles: tuple[ColumnProfile, ...]
    sampled: bool
    sample_fraction: Optional[float]


@dataclass(frozen=True)
class ProfileResult:
    """Top-level result from the profiling stage."""

    tables: tuple[TableProfile, ...]
    profiled_at: str
