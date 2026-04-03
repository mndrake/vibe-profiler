"""Generate dbt model files from a DataVaultSpec using automate-dv macros."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from vibe_profiler.codegen.config import DbtConfig
from vibe_profiler.models.profile import ProfileResult
from vibe_profiler.models.vault_spec import DataVaultSpec, HubSpec, LinkSpec, SatelliteSpec
from vibe_profiler.utils import normalize_name

_TEMPLATE_DIR = Path(__file__).parent / "templates"


@dataclass
class _SchemaColumn:
    name: str
    description: str
    is_primary_key: bool = False


@dataclass
class _SchemaModel:
    name: str
    description: str
    columns: list[_SchemaColumn]


class DbtGenerator:
    """Render dbt SQL models and YAML configs from a ``DataVaultSpec``."""

    def __init__(
        self,
        vault_spec: DataVaultSpec,
        dbt_config: DbtConfig | None = None,
        profile_result: ProfileResult | None = None,
    ) -> None:
        self.spec = vault_spec
        self.cfg = dbt_config or DbtConfig()
        self.profile_result = profile_result
        self._env = Environment(
            loader=FileSystemLoader(str(_TEMPLATE_DIR)),
            keep_trailing_newline=True,
            trim_blocks=True,
            lstrip_blocks=True,
        )
        self._files: dict[str, str] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate_all(self) -> dict[str, str]:
        """Generate every file and return ``{relative_path: content}``."""
        self._files.clear()

        # Collect all source tables that need models
        all_source_tables: set[str] = set()
        for hub in self.spec.hubs:
            all_source_tables.update(hub.source_tables)
        for link in self.spec.links:
            all_source_tables.update(link.source_tables)

        # Pre-stage models (type casting from raw strings)
        pre_stage_tables: set[str] = set()
        if self.cfg.generate_pre_stage and self.profile_result:
            for src in all_source_tables:
                if self._table_needs_pre_stage(src):
                    self._generate_pre_stage(src)
                    pre_stage_tables.add(src)

        # Staging models (automate-dv hashing)
        if self.cfg.generate_staging:
            for src in all_source_tables:
                has_pre_stage = src in pre_stage_tables
                self._generate_staging(src, from_pre_stage=has_pre_stage)

        for hub in self.spec.hubs:
            self._generate_hub(hub)

        for link in self.spec.links:
            self._generate_link(link)

        for sat in self.spec.satellites:
            if sat.is_effectivity:
                self._generate_eff_satellite(sat)
            else:
                self._generate_satellite(sat)

        if self.cfg.generate_sources_yml:
            self._generate_sources_yml()

        if self.cfg.generate_schema_yml:
            self._generate_schema_yml()

        return dict(self._files)

    def write_to_disk(self, output_dir: str | None = None) -> list[str]:
        """Write all generated files to disk. Returns list of written paths."""
        base = Path(output_dir or self.cfg.output_dir)
        if not self._files:
            self.generate_all()

        written: list[str] = []
        for rel_path, content in self._files.items():
            full = base / rel_path
            full.parent.mkdir(parents=True, exist_ok=True)
            full.write_text(content)
            written.append(str(full))
        return written

    # ------------------------------------------------------------------
    # Pre-stage (type casting)
    # ------------------------------------------------------------------

    def _table_needs_pre_stage(self, source_table: str) -> bool:
        """Check if any string column in this table has an inferred type."""
        if not self.profile_result:
            return False
        tp = next(
            (t for t in self.profile_result.tables if t.table_name == source_table),
            None,
        )
        if tp is None:
            return False
        return any(cp.inferred_type is not None for cp in tp.column_profiles)

    def _generate_pre_stage(self, source_table: str) -> None:
        """Generate a pre-stage model that casts raw strings to proper types."""
        assert self.profile_result is not None
        tp = next(
            t for t in self.profile_result.tables if t.table_name == source_table
        )

        columns: list[dict] = []
        for cp in tp.column_profiles:
            cast_expr = None
            if cp.inferred_type is not None:
                it = cp.inferred_type
                if it.spark_target_type == "timestamp" and it.format_string:
                    cast_expr = f"TO_TIMESTAMP(`{cp.column_name}`, '{it.format_string}')"
                elif it.spark_target_type == "date" and it.format_string:
                    cast_expr = f"TO_DATE(`{cp.column_name}`, '{it.format_string}')"
                elif it.spark_target_type == "boolean":
                    cast_expr = (
                        f"CASE LOWER(TRIM(`{cp.column_name}`)) "
                        f"WHEN 'true' THEN TRUE WHEN 'yes' THEN TRUE "
                        f"WHEN 'y' THEN TRUE WHEN 't' THEN TRUE WHEN '1' THEN TRUE "
                        f"ELSE FALSE END"
                    )
                elif it.spark_target_type in ("bigint", "double"):
                    cast_expr = f"CAST(`{cp.column_name}` AS {it.spark_target_type.upper()})"

            columns.append({
                "name": cp.column_name,
                "cast_expr": cast_expr,
            })

        tpl = self._env.get_template("pre_stage.sql.j2")
        content = tpl.render(
            source_name=self.cfg.source_name,
            source_table=source_table,
            columns=columns,
        )
        path = f"models/staging/pre_stg_{normalize_name(source_table)}.sql"
        self._files[path] = content

    # ------------------------------------------------------------------
    # Staging
    # ------------------------------------------------------------------

    def _generate_staging(self, source_table: str, from_pre_stage: bool = False) -> None:
        hashed_columns = self._staging_hashed_columns(source_table)

        # When a pre-stage exists, the staging model reads from it
        # instead of directly from the raw source
        if from_pre_stage:
            staging_source_name = f"pre_stg_{normalize_name(source_table)}"
        else:
            staging_source_name = source_table

        tpl = self._env.get_template("staging.sql.j2")
        content = tpl.render(
            source_name=self.cfg.source_name,
            source_table=staging_source_name,
            record_source_column=self.cfg.record_source_column,
            load_date_column=self.cfg.load_date_column,
            effective_from_column=self.cfg.effective_from_column,
            hashed_columns=hashed_columns,
        )
        path = f"models/staging/stg_{normalize_name(source_table)}.sql"
        self._files[path] = content

    def _staging_hashed_columns(self, source_table: str) -> list[dict]:
        """Build the hashed_columns list for a staging model."""
        entries: list[dict] = []

        # Hash keys for hubs sourced from this table
        for hub in self.spec.hubs:
            if source_table in hub.source_tables:
                entries.append({
                    "name": hub.hash_key_name,
                    "columns": list(hub.business_key_columns),
                    "is_hashdiff": False,
                })

        # Hash keys for links sourced from this table
        for link in self.spec.links:
            if source_table in link.source_tables:
                # Individual FK hash keys for each referenced hub
                # Use the link's foreign_key_columns (the actual source column names)
                # rather than the hub's business_key_columns (which may differ)
                for hub_name in link.hub_references:
                    hub = next((h for h in self.spec.hubs if h.hub_name == hub_name), None)
                    if hub and hub.hash_key_name not in [e["name"] for e in entries]:
                        # Prefer the link's FK mapping for this hub (source column names)
                        fk_cols = link.foreign_key_columns.get(hub_name)
                        columns = list(fk_cols) if fk_cols else list(hub.business_key_columns)
                        entries.append({
                            "name": hub.hash_key_name,
                            "columns": columns,
                            "is_hashdiff": False,
                        })

                # Link hash key = composite of all FK source columns
                all_fk_cols: list[str] = []
                for hub_name in link.hub_references:
                    fk_cols = link.foreign_key_columns.get(hub_name, ())
                    all_fk_cols.extend(fk_cols)
                entries.append({
                    "name": link.hash_key_name,
                    "columns": list(dict.fromkeys(all_fk_cols)),  # dedupe, preserve order
                    "is_hashdiff": False,
                })

        # Hashdiffs for satellites sourced from this table
        for sat in self.spec.satellites:
            if sat.source_table == source_table and not sat.is_effectivity and sat.descriptive_columns:
                entries.append({
                    "name": sat.hashdiff_name,
                    "columns": list(sat.descriptive_columns),
                    "is_hashdiff": True,
                })

        return entries

    # ------------------------------------------------------------------
    # Hubs
    # ------------------------------------------------------------------

    def _generate_hub(self, hub: HubSpec) -> None:
        staging_models = [
            f"stg_{normalize_name(src)}" for src in hub.source_tables
        ]
        tpl = self._env.get_template("hub.sql.j2")
        content = tpl.render(
            materialized=self.cfg.model_config.get("materialized", "incremental"),
            schema=self.cfg.target_schema_raw,
            hash_key=hub.hash_key_name,
            business_keys=list(hub.business_key_columns),
            load_date_column=self.cfg.load_date_column,
            record_source_column=self.cfg.record_source_column,
            staging_models=staging_models,
        )
        path = f"models/raw_vault/hubs/{hub.hub_name}.sql"
        self._files[path] = content

    # ------------------------------------------------------------------
    # Links
    # ------------------------------------------------------------------

    def _generate_link(self, link: LinkSpec) -> None:
        # FK hash keys
        fk_hash_keys: list[str] = []
        for hub_name in link.hub_references:
            hub = next((h for h in self.spec.hubs if h.hub_name == hub_name), None)
            if hub:
                fk_hash_keys.append(hub.hash_key_name)

        staging_models = [
            f"stg_{normalize_name(src)}" for src in link.source_tables
        ]
        tpl = self._env.get_template("link.sql.j2")
        content = tpl.render(
            materialized=self.cfg.model_config.get("materialized", "incremental"),
            schema=self.cfg.target_schema_raw,
            hash_key=link.hash_key_name,
            foreign_keys=fk_hash_keys,
            load_date_column=self.cfg.load_date_column,
            record_source_column=self.cfg.record_source_column,
            staging_models=staging_models,
        )
        path = f"models/raw_vault/links/{link.link_name}.sql"
        self._files[path] = content

    # ------------------------------------------------------------------
    # Satellites
    # ------------------------------------------------------------------

    def _generate_satellite(self, sat: SatelliteSpec) -> None:
        # Find parent hash key
        parent_hk = self._resolve_parent_hash_key(sat)
        tpl = self._env.get_template("satellite.sql.j2")
        content = tpl.render(
            materialized=self.cfg.model_config.get("materialized", "incremental"),
            schema=self.cfg.target_schema_raw,
            parent_hash_key=parent_hk,
            descriptive_columns=list(sat.descriptive_columns),
            effective_from_column=self.cfg.effective_from_column,
            load_date_column=self.cfg.load_date_column,
            record_source_column=self.cfg.record_source_column,
            staging_model=f"stg_{normalize_name(sat.source_table)}",
        )
        path = f"models/raw_vault/satellites/{sat.satellite_name}.sql"
        self._files[path] = content

    def _generate_eff_satellite(self, sat: SatelliteSpec) -> None:
        link = next(
            (lnk for lnk in self.spec.links if lnk.link_name == sat.parent_name), None
        )
        if link is None:
            return

        # Driving FK = first hub's hash key, secondary = rest
        hub_hks: list[str] = []
        for hub_name in link.hub_references:
            hub = next((h for h in self.spec.hubs if h.hub_name == hub_name), None)
            if hub:
                hub_hks.append(hub.hash_key_name)

        driving_fk = hub_hks[0] if hub_hks else ""
        secondary_fks = hub_hks[1:] if len(hub_hks) > 1 else []

        tpl = self._env.get_template("effectivity_satellite.sql.j2")
        content = tpl.render(
            materialized=self.cfg.model_config.get("materialized", "incremental"),
            schema=self.cfg.target_schema_raw,
            link_hash_key=link.hash_key_name,
            driving_fk=driving_fk,
            secondary_fks=secondary_fks,
            effective_from_column=self.cfg.effective_from_column,
            effective_to_column="effective_to",
            load_date_column=self.cfg.load_date_column,
            record_source_column=self.cfg.record_source_column,
            staging_model=f"stg_{normalize_name(sat.source_table)}",
        )
        path = f"models/raw_vault/satellites/{sat.satellite_name}.sql"
        self._files[path] = content

    # ------------------------------------------------------------------
    # YAML
    # ------------------------------------------------------------------

    def _generate_sources_yml(self) -> None:
        all_sources = set()
        for hub in self.spec.hubs:
            all_sources.update(hub.source_tables)
        for link in self.spec.links:
            all_sources.update(link.source_tables)

        tpl = self._env.get_template("sources.yml.j2")
        content = tpl.render(
            source_name=self.cfg.source_name,
            source_schema=self.cfg.target_schema_raw,
            source_tables=sorted(all_sources),
        )
        self._files["models/staging/sources.yml"] = content

    def _generate_schema_yml(self) -> None:
        models: list[_SchemaModel] = []

        for hub in self.spec.hubs:
            cols = [_SchemaColumn(hub.hash_key_name, "Hash key", True)]
            for bk in hub.business_key_columns:
                cols.append(_SchemaColumn(bk, "Business key"))
            cols.append(_SchemaColumn(self.cfg.load_date_column, "Load timestamp"))
            cols.append(_SchemaColumn(self.cfg.record_source_column, "Record source"))
            models.append(_SchemaModel(hub.hub_name, f"Hub for {hub.hub_name}", cols))

        for link in self.spec.links:
            cols = [_SchemaColumn(link.hash_key_name, "Link hash key", True)]
            for hub_name in link.hub_references:
                hub = next((h for h in self.spec.hubs if h.hub_name == hub_name), None)
                if hub:
                    cols.append(_SchemaColumn(hub.hash_key_name, f"FK to {hub.hub_name}"))
            cols.append(_SchemaColumn(self.cfg.load_date_column, "Load timestamp"))
            cols.append(_SchemaColumn(self.cfg.record_source_column, "Record source"))
            models.append(_SchemaModel(link.link_name, f"Link: {link.link_name}", cols))

        for sat in self.spec.satellites:
            parent_hk = self._resolve_parent_hash_key(sat)
            cols = [_SchemaColumn(parent_hk, "Parent hash key")]
            if not sat.is_effectivity:
                cols.append(_SchemaColumn(sat.hashdiff_name, "Hashdiff"))
                for dc in sat.descriptive_columns:
                    cols.append(_SchemaColumn(dc, "Descriptive attribute"))
            cols.append(_SchemaColumn(self.cfg.effective_from_column, "Effective from"))
            cols.append(_SchemaColumn(self.cfg.load_date_column, "Load timestamp"))
            cols.append(_SchemaColumn(self.cfg.record_source_column, "Record source"))
            models.append(_SchemaModel(sat.satellite_name, f"Satellite: {sat.satellite_name}", cols))

        tpl = self._env.get_template("schema.yml.j2")
        content = tpl.render(models=models)
        self._files["models/raw_vault/schema.yml"] = content

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _resolve_parent_hash_key(self, sat: SatelliteSpec) -> str:
        if sat.parent_type == "hub":
            hub = next((h for h in self.spec.hubs if h.hub_name == sat.parent_name), None)
            return hub.hash_key_name if hub else "unknown_hk"
        link = next((lnk for lnk in self.spec.links if lnk.link_name == sat.parent_name), None)
        return link.hash_key_name if link else "unknown_hk"
