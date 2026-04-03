"""Configuration for dbt / automate-dv code generation."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class DbtConfig:
    """Controls naming conventions and automate-dv macro parameters."""

    project_name: str = "dv_project"
    target_schema_raw: str = "raw_vault"
    target_schema_business: str = "business_vault"
    source_name: str = "staging"

    # Hashing
    hash_function: str = "MD5"  # MD5 or SHA-256 (automate-dv notation)
    hash_concat_separator: str = "||"
    hashdiff_naming: str = "hashdiff"

    # Standard column names
    load_date_column: str = "load_datetime"
    record_source_column: str = "record_source"
    effective_from_column: str = "effective_from"

    # automate-dv version (affects macro signatures)
    automate_dv_version: str = "0.10"

    # Output
    generate_pre_stage: bool = True
    generate_staging: bool = True
    generate_sources_yml: bool = True
    generate_schema_yml: bool = True
    output_dir: str = "./dbt_output"

    # Custom dbt config block for models
    model_config: dict[str, str] = field(default_factory=lambda: {
        "materialized": "incremental",
        "schema": "raw_vault",
    })
