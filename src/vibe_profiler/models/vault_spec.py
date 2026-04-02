"""Data Vault 2.0 specification models."""

from __future__ import annotations

from dataclasses import dataclass, field

from vibe_profiler.models.temporal import SCDType


@dataclass(frozen=True)
class HubSpec:
    hub_name: str
    business_key_columns: tuple[str, ...]
    source_tables: tuple[str, ...]
    hash_key_name: str


@dataclass(frozen=True)
class LinkSpec:
    link_name: str
    hub_references: tuple[str, ...]
    foreign_key_columns: dict[str, tuple[str, ...]]
    source_tables: tuple[str, ...]
    hash_key_name: str


@dataclass(frozen=True)
class SatelliteSpec:
    satellite_name: str
    parent_name: str
    parent_type: str  # "hub" or "link"
    descriptive_columns: tuple[str, ...]
    source_table: str
    hashdiff_name: str
    is_effectivity: bool
    scd_type: SCDType


@dataclass(frozen=True)
class DataVaultSpec:
    hubs: tuple[HubSpec, ...]
    links: tuple[LinkSpec, ...]
    satellites: tuple[SatelliteSpec, ...]
    source_mappings: dict[str, list[str]] = field(default_factory=dict)
