"""Temporal and historization detection models."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class SCDType(Enum):
    NONE = "none"
    TYPE1 = "type1"
    TYPE2 = "type2"
    SNAPSHOT = "snapshot"


@dataclass(frozen=True)
class TemporalColumn:
    table_name: str
    column_name: str
    role: str  # "valid_from", "valid_to", "effective_date", "load_date", "snapshot_date"
    confidence: float


@dataclass(frozen=True)
class HistorizationInfo:
    table_name: str
    scd_type: SCDType
    temporal_columns: tuple[TemporalColumn, ...]
    version_key_columns: tuple[str, ...]
    confidence: float
