# vibe-profiler

Data Vault 2.0 profiling and dbt code generation for Databricks.

## Features

- **Data Profiling** — Scan PySpark DataFrames to compute column-level statistics (cardinality, uniqueness, null rates, patterns, quantiles) in a single-pass Spark aggregation.
- **Business Key Detection** — Score columns on likelihood of being a business key using uniqueness, null rate, naming patterns, and value patterns.
- **Column Similarity** — Find columns across tables that represent the same business concept, even if named differently (name similarity + statistical profile + value overlap).
- **Historized Data Support** — Detect SCD Type 2 patterns (valid_from/valid_to), snapshot-style historization, and temporal column roles.
- **Data Vault Suggestion** — Automatically propose Hub, Link, and Satellite tables based on profiling and analysis results.
- **dbt Code Generation** — Generate dbt SQL models compatible with [automate-dv](https://automate-dv.readthedocs.io/) macros, including staging, hubs, links, satellites, effectivity satellites, and YAML schema/sources files.

## Quick Start

```python
from vibe_profiler import VibeProfilerPipeline

# Pass your Spark DataFrames
tables = {
    "customers": spark.table("raw.customers"),
    "orders": spark.table("raw.orders"),
    "order_history": spark.table("raw.order_history"),
}

pipeline = VibeProfilerPipeline(spark, tables)

# Run the full pipeline
files = pipeline.run_all()

# Write dbt project to disk
pipeline.write_dbt_project("/Workspace/Users/you/dbt_output")
```

## Step-by-Step Usage

```python
from vibe_profiler import VibeProfilerPipeline
from vibe_profiler.codegen.config import DbtConfig

pipeline = VibeProfilerPipeline(spark, tables)

# 1. Profile
profile = pipeline.run_profiling()
for tp in profile.tables:
    print(f"{tp.table_name}: {tp.row_count} rows, {len(tp.column_profiles)} columns")

# 2. Analyze
analysis = pipeline.run_analysis()
for table, bks in analysis.business_keys.items():
    print(f"{table} BK candidates: {[(b.column_name, b.score) for b in bks[:3]]}")

for match in analysis.similarity_matches:
    print(f"  {match.table_a}.{match.column_a} ~ {match.table_b}.{match.column_b} "
          f"(score={match.composite_score})")

# 3. Override if needed
pipeline.override_business_keys({"customers": ["customer_id"]})

# 4. Suggest vault structure
spec = pipeline.run_suggestion()
for hub in spec.hubs:
    print(f"Hub: {hub.hub_name} (BK: {hub.business_key_columns})")
for link in spec.links:
    print(f"Link: {link.link_name} -> {link.hub_references}")
for sat in spec.satellites:
    print(f"Sat: {sat.satellite_name} (parent: {sat.parent_name})")

# 5. Generate dbt models
dbt_cfg = DbtConfig(hash_function="MD5", load_date_column="load_datetime")
files = pipeline.run_codegen()
for path, content in files.items():
    print(f"--- {path} ---")
    print(content[:200])
```

## Installation

```bash
pip install vibe-profiler
```

Or for development:

```bash
pip install -e ".[dev]"
```

## Configuration

```python
from vibe_profiler.config import PipelineConfig, ProfilingConfig, AnalysisConfig, VaultConfig
from vibe_profiler.codegen.config import DbtConfig

config = PipelineConfig(
    profiling=ProfilingConfig(
        sample_fraction=0.1,           # sample 10% of large tables
        sample_threshold_rows=5_000_000,
    ),
    analysis=AnalysisConfig(
        uniqueness_threshold=0.95,
        min_composite_score=0.5,       # lower threshold for similarity matches
    ),
    vault=VaultConfig(
        hub_prefix="hub_",
        link_prefix="lnk_",
        sat_prefix="sat_",
        hash_function="md5",
    ),
)

dbt_config = DbtConfig(
    target_schema_raw="raw_vault",
    hash_function="MD5",
    load_date_column="load_datetime",
    record_source_column="record_source",
)

pipeline = VibeProfilerPipeline(spark, tables, config=config, dbt_config=dbt_config)
```

## Generated dbt Project Structure

```
dbt_output/
  models/
    staging/
      stg_customers.sql      # automate-dv staging with hashing
      stg_orders.sql
      sources.yml
    raw_vault/
      hubs/
        hub_customer.sql      # automate_dv.hub()
        hub_order.sql
      links/
        lnk_customer_order.sql  # automate_dv.link()
      satellites/
        sat_customer_customers.sql  # automate_dv.sat()
        sat_order_orders.sql
      schema.yml
```

## License

Apache-2.0
