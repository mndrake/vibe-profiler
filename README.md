# vibe-profiler

Data Vault 2.0 profiling and dbt code generation for Databricks.

## Features

- **Data Profiling** -- Scan PySpark DataFrames to compute column-level statistics (cardinality, uniqueness, null rates, patterns, quantiles) in a single-pass Spark aggregation.
- **Type Inference** -- Detect semantic types hidden in raw string columns (integers, decimals, dates, timestamps, booleans) with automatic date/timestamp format detection across 25+ formats.
- **Business Key Detection** -- Score columns on likelihood of being a business key using uniqueness, null rate, naming patterns, and value patterns.
- **Column Similarity** -- Find columns across tables that represent the same business concept, even if named differently (name similarity + statistical profile + value overlap).
- **Historized Data Support** -- Detect SCD Type 2 patterns (valid_from/valid_to), snapshot-style historization, and temporal column roles.
- **Data Vault Suggestion** -- Automatically propose Hub, Link, and Satellite tables based on profiling and analysis results.
- **dbt Code Generation** -- Generate dbt SQL models compatible with [automate-dv](https://automate-dv.readthedocs.io/) macros, including pre-stage type casting, staging, hubs, links, satellites, effectivity satellites, and YAML configs.
- **Findings Report** -- Text and HTML reports summarizing all pipeline findings.
- **Incremental Profiling** -- Save/load results to JSON; only re-profile new or changed tables.
- **Auto-Tuning** -- Adapts profiling settings (batch size, quantile error, sampling) per table based on actual data shape.
- **Progress Reporting** -- Real-time callbacks with ETA for notebook and CLI use.

## Installation

```bash
pip install vibe-profiler
```

Or install from a GitHub Release wheel:

```bash
pip install https://github.com/mndrake/vibe-profiler/releases/download/v0.1.0/vibe_profiler-0.1.0-py3-none-any.whl
```

For development:

```bash
pip install -e ".[dev]"
```

## Quick Start

```python
from vibe_profiler import VibeProfilerPipeline

tables = {
    "customers": spark.table("raw.customers"),
    "orders": spark.table("raw.orders"),
}

pipeline = VibeProfilerPipeline(spark, tables, verbose=True)
files = pipeline.run_all()

# Review findings before writing
print(pipeline.report())

# Write dbt project
pipeline.write_dbt_project("/Workspace/Users/you/dbt_output")
```

## Complete Walkthrough

### 1. Profile your tables

```python
from vibe_profiler import VibeProfilerPipeline

tables = {
    "customers": spark.table("raw.customers"),
    "orders": spark.table("raw.orders"),
    "order_history": spark.table("raw.order_history"),
}

pipeline = VibeProfilerPipeline(spark, tables, verbose=True)
profile = pipeline.run_profiling()

# View table summaries
for tp in profile.tables:
    print(f"{tp.table_name}: {tp.row_count:,} rows, {len(tp.column_profiles)} columns")
    for cp in tp.column_profiles:
        inferred = ""
        if cp.inferred_type:
            inferred = f" -> {cp.inferred_type.spark_target_type}"
            if cp.inferred_type.format_string:
                inferred += f" ({cp.inferred_type.format_string})"
        print(f"  {cp.column_name}: {cp.spark_type}{inferred}, "
              f"uniqueness={cp.uniqueness:.0%}, nulls={cp.null_rate:.1%}")
```

The profiler automatically:
- Computes column statistics in a single Spark aggregation pass
- Detects patterns (UUID, email, phone, date, numeric code, etc.)
- Infers semantic types for string columns (dates, numbers, timestamps, booleans)
- Detects date/timestamp formats (ISO, US, EU, compact, and more)
- Auto-tunes settings per table based on row count and column count

### 2. Analyze for Data Vault patterns

```python
analysis = pipeline.run_analysis()

# Business key candidates (ranked by score)
for table, bks in analysis.business_keys.items():
    print(f"\n{table}:")
    for bk in bks[:3]:
        print(f"  {bk.column_name} (score: {bk.score:.2f}) - {', '.join(bk.reasoning)}")

# Cross-table column similarity
for m in analysis.similarity_matches:
    print(f"{m.table_a}.{m.column_a} ~ {m.table_b}.{m.column_b} "
          f"(score: {m.composite_score:.2f}, type: {m.match_type})")

# FK relationships
for r in analysis.relationships:
    print(f"{r.parent_table}.{r.parent_column} -> {r.child_table}.{r.child_column} "
          f"({r.cardinality}, confidence: {r.confidence:.2f})")

# Historization patterns
for table, hist in analysis.historization.items():
    print(f"{table}: {hist.scd_type.value} (confidence: {hist.confidence:.2f})")
```

### 3. Override business keys if needed

```python
# The profiler auto-detects BKs, but you can override
pipeline.override_business_keys({
    "customers": ["customer_id"],
    "orders": ["order_id"],
})
```

### 4. Generate Data Vault suggestions

```python
spec = pipeline.run_suggestion()

for hub in spec.hubs:
    print(f"Hub: {hub.hub_name} (BK: {', '.join(hub.business_key_columns)})")
for link in spec.links:
    print(f"Link: {link.link_name} -> {', '.join(link.hub_references)}")
for sat in spec.satellites:
    scd = f" SCD: {sat.scd_type.value}" if sat.scd_type.value != "none" else ""
    print(f"Sat: {sat.satellite_name} (parent: {sat.parent_name}{scd})")
```

### 5. Generate dbt models

```python
files = pipeline.run_codegen()

# Preview what will be generated
for path in sorted(files.keys()):
    print(path)
```

### 6. Review the findings report

```python
# Text report (terminal / notebook print)
print(pipeline.report())

# HTML report (Databricks notebook)
displayHTML(pipeline.report(format="html"))
```

The report covers:
- Profiling summary with inferred types
- Business key candidates with reasoning
- Cross-table similarity matches and FK relationships
- Historization patterns (SCD Type 2, snapshots)
- Suggested Data Vault structure (hubs, links, satellites)
- Generated file listing

### 7. Write dbt project to disk

```python
pipeline.write_dbt_project("/Workspace/Users/you/dbt_output")
```

### 8. Save results for later

```python
# Cache results so you don't re-profile unchanged tables
pipeline.save_results("/Workspace/cache/my_project.json")
```

### 9. Incremental profiling (add new tables)

```python
# Next session -- added a "products" table
tables["products"] = spark.table("raw.products")

pipeline = VibeProfilerPipeline(spark, tables, verbose=True)
pipeline.load_results("/Workspace/cache/my_project.json")

# Automatically detects "products" is new, only profiles that table
pipeline.run_profiling()

# Cross-table analysis only compares new table pairs
pipeline.run_analysis()

# Re-generate vault spec and dbt models
pipeline.run_suggestion()
pipeline.run_codegen()

# Save updated cache
pipeline.save_results("/Workspace/cache/my_project.json")
```

## Generated dbt Project Structure

```
dbt_output/
  models/
    staging/
      pre_stg_customers.sql     # Type casting (raw strings -> proper types)
      pre_stg_orders.sql
      stg_customers.sql         # automate-dv staging (hashing)
      stg_orders.sql
      sources.yml
    raw_vault/
      hubs/
        hub_customer.sql        # automate_dv.hub()
        hub_order.sql
      links/
        lnk_customer_order.sql  # automate_dv.link()
      satellites/
        sat_customer_customers.sql  # automate_dv.sat()
        sat_order_orders.sql
      schema.yml
```

The data flow is:

```
Raw source -> pre_stg_xxx (type casting) -> stg_xxx (hashing) -> DV models (hub/link/sat)
```

Pre-stage models are only generated when string columns have inferred types that need casting. Tables with already-typed columns skip the pre-stage.

## Configuration

```python
from vibe_profiler.config import PipelineConfig, ProfilingConfig, AnalysisConfig, VaultConfig
from vibe_profiler.codegen.config import DbtConfig

config = PipelineConfig(
    profiling=ProfilingConfig(
        sample_fraction=0.1,           # sample 10% of large tables
        sample_threshold_rows=5_000_000,
        auto_tune=True,                # auto-adjust settings per table (default)
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
    generate_pre_stage=True,           # generate type-casting models (default)
)

pipeline = VibeProfilerPipeline(
    spark, tables,
    config=config,
    dbt_config=dbt_config,
    verbose=True,  # enable progress output
)
```

### Auto-Tuning

When `auto_tune=True` (default), the profiler pre-scans each table and adapts:

| Setting | Small table (<100K rows) | Large table (>10M rows) |
|---------|------------------------|------------------------|
| `approx_distinct` | Exact (False) | Approximate (True) |
| `column_batch_size` | 50 | 5-15 (based on width) |
| `max_top_values` | 30-50 | 10 |
| `approx_quantile_error` | 0.001 | 0.05 |

Override any setting explicitly to prevent auto-tuning for that field:

```python
config = ProfilingConfig.create(approx_distinct=False)  # always use exact distinct
```

## Building

```bash
pip install build
python -m build
# Output: dist/vibe_profiler-0.1.0-py3-none-any.whl
```

## Testing

```bash
pip install -e ".[dev]"
python -m pytest tests/ -v
```

## License

Apache-2.0
