"""End-to-end orchestration: profile -> analyze -> suggest -> generate."""

from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from vibe_profiler.analyzer.business_key import BusinessKeyAnalyzer
from vibe_profiler.analyzer.historization import HistorizationAnalyzer
from vibe_profiler.analyzer.relationship import RelationshipAnalyzer
from vibe_profiler.analyzer.similarity import CrossTableSimilarity
from vibe_profiler.codegen.config import DbtConfig
from vibe_profiler.codegen.dbt_generator import DbtGenerator
from vibe_profiler.config import PipelineConfig
from vibe_profiler.models.analysis import AnalysisResult
from vibe_profiler.models.profile import ProfileResult
from vibe_profiler.models.vault_spec import DataVaultSpec
from vibe_profiler.profiler.engine import ProfileEngine
from vibe_profiler.progress import ProgressCallback, ProgressTracker
from vibe_profiler.vault.suggestion_engine import VaultSuggestionEngine


class VibeProfilerPipeline:
    """Run the full profiling pipeline or individual stages.

    Usage on Databricks::

        from vibe_profiler import VibeProfilerPipeline

        tables = {
            "customers": spark.table("raw.customers"),
            "orders": spark.table("raw.orders"),
        }
        pipeline = VibeProfilerPipeline(spark, tables)
        files = pipeline.run_all()  # returns {path: sql_content}
        pipeline.write_dbt_project("/Workspace/dbt_output")
    """

    def __init__(
        self,
        spark: SparkSession,
        tables: dict[str, DataFrame],
        config: PipelineConfig | None = None,
        dbt_config: DbtConfig | None = None,
        progress_callback: ProgressCallback | None = None,
        verbose: bool = False,
    ) -> None:
        self.spark = spark
        self.tables = tables
        self.config = config or PipelineConfig()
        self.dbt_config = dbt_config or DbtConfig()

        if verbose and progress_callback is None:
            from vibe_profiler.progress import default_progress_callback

            progress_callback = default_progress_callback
        self.progress_callback = progress_callback

        # Intermediate results (populated as stages run)
        self._profile_result: Optional[ProfileResult] = None
        self._analysis_result: Optional[AnalysisResult] = None
        self._vault_spec: Optional[DataVaultSpec] = None
        self._generated_files: Optional[dict[str, str]] = None

    # ------------------------------------------------------------------
    # Stage runners
    # ------------------------------------------------------------------

    def run_profiling(
        self, tables_to_refresh: set[str] | None = None
    ) -> ProfileResult:
        """Stage 1: Profile tables.

        When cached results exist (via :meth:`load_results`), automatically
        detects new tables and only profiles those.  You can also force
        specific tables to be re-profiled via *tables_to_refresh*.

        Args:
            tables_to_refresh: Explicit set of tables to (re-)profile.  If
                ``None`` and cached results exist, automatically profiles
                tables not present in the cache.  If ``None`` and no cache
                exists, profiles everything.

        When ``auto_tune`` is enabled, a lightweight pre-scan adjusts the
        analysis config (e.g. ``value_sample_size``) based on table sizes.
        """
        if self.config.profiling.auto_tune:
            from dataclasses import replace as _replace
            from vibe_profiler.profiler.auto_config import (
                auto_tune_analysis_config,
                collect_table_metrics,
            )

            all_metrics = {
                name: collect_table_metrics(df) for name, df in self.tables.items()
            }
            tuned_analysis = auto_tune_analysis_config(
                self.config.analysis, all_metrics
            )
            self.config = _replace(self.config, analysis=tuned_analysis)

        engine = ProfileEngine(
            self.spark,
            config=self.config.profiling,
            progress_callback=self.progress_callback,
        )

        # Auto-detect which tables need profiling
        if self._profile_result is not None:
            cached_names = {tp.table_name for tp in self._profile_result.tables}
            current_names = set(self.tables.keys())

            if tables_to_refresh is None:
                # Auto: profile tables not already in cache
                tables_to_refresh = current_names - cached_names

            # Always include explicitly requested refreshes
            subset = {k: v for k, v in self.tables.items() if k in tables_to_refresh}
            if subset:
                new_profiles = engine.profile_tables(subset)
                self._profile_result = self._merge_profiles(
                    self._profile_result, new_profiles
                )

            # Drop cached profiles for tables no longer in self.tables
            self._profile_result = ProfileResult(
                tables=tuple(
                    tp for tp in self._profile_result.tables
                    if tp.table_name in current_names
                ),
                profiled_at=self._profile_result.profiled_at,
            )
        else:
            self._profile_result = engine.profile_tables(self.tables)

        return self._profile_result

    def run_analysis(
        self,
        profile_result: Optional[ProfileResult] = None,
        tables_to_refresh: set[str] | None = None,
    ) -> AnalysisResult:
        """Stage 2: Analyze profiles for BKs, similarity, relationships, historization.

        When cached analysis results exist, automatically detects which tables
        are new or changed and only re-analyzes those.  You can also force
        specific tables via *tables_to_refresh*.

        Args:
            tables_to_refresh: Explicit set of tables to re-analyze.  If
                ``None`` and cached results exist, auto-detects new/changed
                tables by comparing current table set with cached analysis.
                If ``None`` and no cache exists, analyzes everything.
        """
        pr = profile_result or self._profile_result
        if pr is None:
            pr = self.run_profiling()

        prev = self._analysis_result

        # Auto-detect which tables need analysis
        if tables_to_refresh is None and prev is not None:
            cached_tables = set(prev.business_keys.keys())
            current_tables = {tp.table_name for tp in pr.tables}
            tables_to_refresh = current_tables - cached_tables
            # If no new tables at all, still run full analysis
            if not tables_to_refresh:
                tables_to_refresh = None

        # 4 analysis sub-steps
        tracker = ProgressTracker(
            stage="analysis", total=4, callback=self.progress_callback
        )

        # Business keys — per-table, can reuse cached
        tracker.update(1, "business_keys", "Detecting business keys")
        bk_analyzer = BusinessKeyAnalyzer(config=self.config.analysis)
        business_keys: dict = {}
        for tp in pr.tables:
            if (
                tables_to_refresh is not None
                and prev is not None
                and tp.table_name not in tables_to_refresh
                and tp.table_name in prev.business_keys
            ):
                business_keys[tp.table_name] = prev.business_keys[tp.table_name]
            else:
                business_keys[tp.table_name] = bk_analyzer.analyze(tp)

        # Cross-table similarity — incremental: only new pairs
        n_changed = len(tables_to_refresh) if tables_to_refresh else len(self.tables)
        tracker.update(
            2, "similarity",
            f"Detecting cross-table column similarity ({n_changed} table(s) to compare)",
        )
        sim_analyzer = CrossTableSimilarity(self.spark, config=self.config.analysis)
        similarity_matches = sim_analyzer.find_matches(
            pr,
            self.tables,
            changed_tables=tables_to_refresh,
            previous_matches=prev.similarity_matches if prev else (),
        )

        # Relationships — incremental: only new pairs
        tracker.update(3, "relationships", "Detecting FK relationships")
        rel_analyzer = RelationshipAnalyzer(self.spark)
        relationships = rel_analyzer.analyze(
            pr,
            business_keys,
            self.tables,
            similarity_matches,
            changed_tables=tables_to_refresh,
            previous_relationships=prev.relationships if prev else (),
        )

        # Historization — per-table, can reuse cached
        tracker.update(4, "historization", "Detecting historization patterns")
        hist_analyzer = HistorizationAnalyzer()
        historization: dict = {}
        for tp in pr.tables:
            if (
                tables_to_refresh is not None
                and prev is not None
                and tp.table_name not in tables_to_refresh
                and tp.table_name in prev.historization
            ):
                historization[tp.table_name] = prev.historization[tp.table_name]
            else:
                historization[tp.table_name] = hist_analyzer.analyze(
                    tp, self.tables[tp.table_name],
                    business_keys.get(tp.table_name, ()),
                )

        tracker.complete("Analysis complete")

        self._analysis_result = AnalysisResult(
            business_keys=business_keys,
            similarity_matches=similarity_matches,
            relationships=relationships,
            historization=historization,
        )
        return self._analysis_result

    def run_suggestion(
        self, analysis_result: Optional[AnalysisResult] = None
    ) -> DataVaultSpec:
        """Stage 3: Suggest Data Vault 2.0 entities."""
        ar = analysis_result or self._analysis_result
        if ar is None:
            ar = self.run_analysis()

        pr = self._profile_result
        if pr is None:
            pr = self.run_profiling()

        engine = VaultSuggestionEngine(config=self.config.vault)
        self._vault_spec = engine.suggest(pr, ar)
        return self._vault_spec

    def run_codegen(
        self, vault_spec: Optional[DataVaultSpec] = None
    ) -> dict[str, str]:
        """Stage 4: Generate dbt model files (automate-dv compatible)."""
        vs = vault_spec or self._vault_spec
        if vs is None:
            vs = self.run_suggestion()

        gen = DbtGenerator(vs, dbt_config=self.dbt_config)
        self._generated_files = gen.generate_all()
        return self._generated_files

    def run_all(self) -> dict[str, str]:
        """Execute the full pipeline and return generated dbt files."""
        self.run_profiling()
        self.run_analysis()
        self.run_suggestion()
        return self.run_codegen()

    # ------------------------------------------------------------------
    # Interactive overrides
    # ------------------------------------------------------------------

    def override_business_keys(self, overrides: dict[str, list[str]]) -> None:
        """Manually set business key columns per table.

        Call before ``run_suggestion`` to override auto-detected BKs.

        Args:
            overrides: ``{"table_name": ["col1", "col2"]}``
        """
        if self._analysis_result is None:
            self.run_analysis()

        from vibe_profiler.models.analysis import BusinessKeyCandidate

        assert self._analysis_result is not None
        for table_name, columns in overrides.items():
            manual = tuple(
                BusinessKeyCandidate(
                    table_name=table_name,
                    column_name=col,
                    score=1.0,
                    uniqueness=1.0,
                    null_rate=0.0,
                    pattern_stability=1.0,
                    reasoning=("Manually specified",),
                )
                for col in columns
            )
            self._analysis_result.business_keys[table_name] = manual

    def override_vault_spec(self, spec: DataVaultSpec) -> None:
        """Replace the auto-suggested vault spec with a manually adjusted one."""
        self._vault_spec = spec

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save_results(self, path: str) -> None:
        """Save profiling and analysis results to a JSON file.

        Allows resuming later without re-profiling unchanged tables::

            pipeline.run_all()
            pipeline.save_results("/Workspace/cache/my_project.json")
        """
        from vibe_profiler.serialization import save_results

        save_results(
            path,
            profile_result=self._profile_result,
            analysis_result=self._analysis_result,
        )

    def load_results(self, path: str) -> None:
        """Load previously saved profiling and analysis results.

        After loading, use ``run_profiling(tables_to_refresh={"new_table"})``
        to incrementally profile only new or changed tables::

            pipeline.load_results("/Workspace/cache/my_project.json")
            pipeline.run_profiling(tables_to_refresh={"products"})
            pipeline.run_analysis()
        """
        from vibe_profiler.serialization import load_results

        pr, ar = load_results(path)
        if pr is not None:
            self._profile_result = pr
        if ar is not None:
            self._analysis_result = ar

    @staticmethod
    def _merge_profiles(
        previous: ProfileResult, new_partial: ProfileResult
    ) -> ProfileResult:
        """Merge cached profiles with newly profiled tables."""
        by_name = {tp.table_name: tp for tp in previous.tables}
        for tp in new_partial.tables:
            by_name[tp.table_name] = tp
        return ProfileResult(
            tables=tuple(sorted(by_name.values(), key=lambda t: t.table_name)),
            profiled_at=new_partial.profiled_at,
        )

    # ------------------------------------------------------------------
    # Output helpers
    # ------------------------------------------------------------------

    def write_dbt_project(self, output_dir: str | None = None) -> list[str]:
        """Write generated files to disk. Returns list of written file paths."""
        if self._generated_files is None:
            self.run_all()
        assert self._vault_spec is not None
        gen = DbtGenerator(self._vault_spec, dbt_config=self.dbt_config)
        gen._files = self._generated_files or {}
        return gen.write_to_disk(output_dir)

    @property
    def profile_result(self) -> Optional[ProfileResult]:
        return self._profile_result

    @property
    def analysis_result(self) -> Optional[AnalysisResult]:
        return self._analysis_result

    @property
    def vault_spec(self) -> Optional[DataVaultSpec]:
        return self._vault_spec

    @property
    def generated_files(self) -> Optional[dict[str, str]]:
        return self._generated_files
