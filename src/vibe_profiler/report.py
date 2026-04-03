"""Generate human-readable reports from pipeline results."""

from __future__ import annotations

from typing import Optional

from vibe_profiler.models.analysis import AnalysisResult
from vibe_profiler.models.profile import ProfileResult
from vibe_profiler.models.temporal import SCDType
from vibe_profiler.models.vault_spec import DataVaultSpec


class ReportGenerator:
    """Build text or HTML reports from pipeline results."""

    def __init__(
        self,
        profile_result: Optional[ProfileResult] = None,
        analysis_result: Optional[AnalysisResult] = None,
        vault_spec: Optional[DataVaultSpec] = None,
        generated_files: Optional[dict[str, str]] = None,
    ) -> None:
        self.pr = profile_result
        self.ar = analysis_result
        self.vs = vault_spec
        self.files = generated_files

    # ------------------------------------------------------------------
    # Text report
    # ------------------------------------------------------------------

    def to_text(self) -> str:
        w = 65
        lines: list[str] = []
        lines.append("=" * w)
        lines.append("VIBE PROFILER REPORT".center(w))
        if self.pr:
            lines.append(f"Profiled at: {self.pr.profiled_at}".center(w))
        lines.append("=" * w)

        if self.pr:
            lines.append("")
            lines.append(self._section_header("PROFILING SUMMARY"))
            lines.extend(self._text_profiling_summary())

        if self.ar:
            lines.append("")
            lines.append(self._section_header("BUSINESS KEY CANDIDATES"))
            lines.extend(self._text_business_keys())

            lines.append("")
            lines.append(self._section_header("CROSS-TABLE MATCHES"))
            lines.extend(self._text_cross_table())

            lines.append("")
            lines.append(self._section_header("HISTORIZATION"))
            lines.extend(self._text_historization())

        if self.vs:
            lines.append("")
            lines.append(self._section_header("DATA VAULT SUGGESTION"))
            lines.extend(self._text_vault_spec())

        if self.files:
            lines.append("")
            lines.append(self._section_header("GENERATED FILES"))
            lines.extend(self._text_generated_files())

        lines.append("")
        return "\n".join(lines)

    @staticmethod
    def _section_header(title: str) -> str:
        return f"-- {title} " + "-" * max(0, 62 - len(title))

    def _text_profiling_summary(self) -> list[str]:
        assert self.pr is not None
        lines: list[str] = []
        lines.append("")
        lines.append(f"  {'Table':<25} {'Rows':>10}  {'Columns':>7}  Sampled")
        lines.append(f"  {'-'*25} {'-'*10}  {'-'*7}  -------")
        for tp in self.pr.tables:
            sampled = "No"
            if tp.sampled and tp.sample_fraction is not None:
                sampled = f"Yes ({tp.sample_fraction:.0%})"
            lines.append(
                f"  {tp.table_name:<25} {tp.row_count:>10,}  {len(tp.column_profiles):>7}  {sampled}"
            )

        # Per-table column details
        for tp in self.pr.tables:
            lines.append("")
            lines.append(f"  {tp.table_name} columns:")
            lines.append(
                f"    {'Column':<25} {'Type':<12} {'Unique':>7}  {'Null%':>6}  Pattern"
            )
            lines.append(f"    {'-'*25} {'-'*12} {'-'*7}  {'-'*6}  -------")
            for cp in tp.column_profiles:
                lines.append(
                    f"    {cp.column_name:<25} {cp.spark_type:<12} "
                    f"{cp.uniqueness:>6.0%}  {cp.null_rate:>5.1%}  "
                    f"{cp.dominant_pattern.value}"
                )
        return lines

    def _text_business_keys(self) -> list[str]:
        assert self.ar is not None
        lines: list[str] = []
        for table_name, candidates in sorted(self.ar.business_keys.items()):
            lines.append("")
            lines.append(f"  {table_name}:")
            if not candidates:
                lines.append("    (no candidates detected)")
                continue
            for i, bk in enumerate(candidates[:3], 1):
                lines.append(
                    f"    {i}. {bk.column_name:<20} score: {bk.score:.2f}  "
                    f"uniqueness: {bk.uniqueness:.0%}  null: {bk.null_rate:.1%}"
                )
                if bk.reasoning:
                    lines.append(f"       -> {', '.join(bk.reasoning)}")
        return lines

    def _text_cross_table(self) -> list[str]:
        assert self.ar is not None
        lines: list[str] = []

        lines.append("")
        lines.append("  Column Similarity:")
        if not self.ar.similarity_matches:
            lines.append("    (none detected)")
        for m in self.ar.similarity_matches:
            lines.append(
                f"    {m.table_a}.{m.column_a} ~ {m.table_b}.{m.column_b}"
            )
            lines.append(
                f"      score: {m.composite_score:.2f}  type: {m.match_type}  "
                f"name: {m.name_similarity:.2f}  stats: {m.statistical_similarity:.2f}  "
                f"values: {m.value_overlap:.2f}"
            )

        lines.append("")
        lines.append("  FK Relationships:")
        if not self.ar.relationships:
            lines.append("    (none detected)")
        for r in self.ar.relationships:
            lines.append(
                f"    {r.parent_table}.{r.parent_column} -> "
                f"{r.child_table}.{r.child_column}"
            )
            lines.append(
                f"      confidence: {r.confidence:.2f}  cardinality: {r.cardinality}"
            )
        return lines

    def _text_historization(self) -> list[str]:
        assert self.ar is not None
        lines: list[str] = []
        for table_name, hist in sorted(self.ar.historization.items()):
            lines.append("")
            if hist.scd_type == SCDType.NONE:
                lines.append(f"  {table_name}: none")
                continue
            lines.append(
                f"  {table_name}: {hist.scd_type.value} "
                f"(confidence: {hist.confidence:.2f})"
            )
            for tc in hist.temporal_columns:
                lines.append(
                    f"    {tc.column_name:<25} role: {tc.role} "
                    f"(confidence: {tc.confidence:.2f})"
                )
            if hist.version_key_columns:
                lines.append(
                    f"    version key: {', '.join(hist.version_key_columns)}"
                )
        return lines

    def _text_vault_spec(self) -> list[str]:
        assert self.vs is not None
        lines: list[str] = []

        lines.append("")
        lines.append(f"  Hubs ({len(self.vs.hubs)}):")
        for h in self.vs.hubs:
            bk = ", ".join(h.business_key_columns)
            src = ", ".join(h.source_tables)
            lines.append(f"    {h.hub_name:<30} BK: {bk:<20} Sources: {src}")

        lines.append("")
        lines.append(f"  Links ({len(self.vs.links)}):")
        if not self.vs.links:
            lines.append("    (none)")
        for lnk in self.vs.links:
            lines.append(f"    {lnk.link_name}")
            for hub_name, cols in lnk.foreign_key_columns.items():
                lines.append(f"      {hub_name} <- {', '.join(cols)}")
            lines.append(f"      Sources: {', '.join(lnk.source_tables)}")

        lines.append("")
        lines.append(f"  Satellites ({len(self.vs.satellites)}):")
        for sat in self.vs.satellites:
            if sat.is_effectivity:
                lines.append(
                    f"    {sat.satellite_name:<35} Parent: {sat.parent_name} (effectivity)"
                )
            else:
                cols = ", ".join(sat.descriptive_columns[:5])
                if len(sat.descriptive_columns) > 5:
                    cols += f", ... (+{len(sat.descriptive_columns) - 5} more)"
                scd = f"  SCD: {sat.scd_type.value}" if sat.scd_type != SCDType.NONE else ""
                lines.append(
                    f"    {sat.satellite_name}"
                )
                lines.append(
                    f"      Parent: {sat.parent_name}  Columns: {cols}{scd}"
                )
        return lines

    def _text_generated_files(self) -> list[str]:
        assert self.files is not None
        lines: list[str] = []

        categories: dict[str, list[str]] = {
            "Staging": [],
            "Hubs": [],
            "Links": [],
            "Satellites": [],
            "YAML": [],
        }
        for path in sorted(self.files.keys()):
            name = path.rsplit("/", 1)[-1]
            if "staging/" in path and name.endswith(".sql"):
                categories["Staging"].append(name)
            elif "hubs/" in path:
                categories["Hubs"].append(name)
            elif "links/" in path:
                categories["Links"].append(name)
            elif "satellites/" in path:
                categories["Satellites"].append(name)
            elif name.endswith(".yml"):
                categories["YAML"].append(name)

        lines.append("")
        for cat, files in categories.items():
            if files:
                lines.append(f"  {cat} ({len(files)}): {', '.join(files)}")
        return lines

    # ------------------------------------------------------------------
    # HTML report
    # ------------------------------------------------------------------

    def to_html(self) -> str:
        parts: list[str] = [self._html_head()]

        if self.pr:
            parts.append(self._html_profiling_summary())

        if self.ar:
            parts.append(self._html_business_keys())
            parts.append(self._html_cross_table())
            parts.append(self._html_historization())

        if self.vs:
            parts.append(self._html_vault_spec())

        if self.files:
            parts.append(self._html_generated_files())

        parts.append("</div>")
        return "\n".join(parts)

    def _html_head(self) -> str:
        ts = self.pr.profiled_at if self.pr else ""
        return f"""<div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                    max-width: 960px; margin: 0 auto; padding: 20px; color: #1a1a1a;">
<h1 style="border-bottom: 3px solid #2563eb; padding-bottom: 8px;">Vibe Profiler Report</h1>
<p style="color: #666;">Profiled at: {ts}</p>"""

    def _html_profiling_summary(self) -> str:
        assert self.pr is not None
        rows = []
        for tp in self.pr.tables:
            sampled = "No"
            if tp.sampled and tp.sample_fraction is not None:
                sampled = f"Yes ({tp.sample_fraction:.0%})"
            rows.append(
                f"<tr><td><b>{tp.table_name}</b></td>"
                f"<td style='text-align:right'>{tp.row_count:,}</td>"
                f"<td style='text-align:right'>{len(tp.column_profiles)}</td>"
                f"<td>{sampled}</td></tr>"
            )

        # Column details per table
        detail_sections: list[str] = []
        for tp in self.pr.tables:
            col_rows = []
            for cp in tp.column_profiles:
                col_rows.append(
                    f"<tr><td>{cp.column_name}</td><td>{cp.spark_type}</td>"
                    f"<td style='text-align:right'>{cp.uniqueness:.0%}</td>"
                    f"<td style='text-align:right'>{cp.null_rate:.1%}</td>"
                    f"<td>{cp.dominant_pattern.value}</td></tr>"
                )
            detail_sections.append(
                f"<details><summary style='cursor:pointer; color:#2563eb; margin:8px 0;'>"
                f"{tp.table_name} columns</summary>"
                f"<table style='{self._table_style()}'>"
                f"<tr style='background:#f1f5f9;'><th>Column</th><th>Type</th>"
                f"<th>Uniqueness</th><th>Null %</th><th>Pattern</th></tr>"
                f"{''.join(col_rows)}</table></details>"
            )

        return (
            f"<h2>Profiling Summary</h2>"
            f"<table style='{self._table_style()}'>"
            f"<tr style='background:#f1f5f9;'><th>Table</th><th>Rows</th>"
            f"<th>Columns</th><th>Sampled</th></tr>"
            f"{''.join(rows)}</table>"
            f"{''.join(detail_sections)}"
        )

    def _html_business_keys(self) -> str:
        assert self.ar is not None
        rows = []
        for table_name, candidates in sorted(self.ar.business_keys.items()):
            for bk in candidates[:3]:
                color = self._confidence_color(bk.score)
                reasoning = ", ".join(bk.reasoning) if bk.reasoning else ""
                rows.append(
                    f"<tr><td><b>{table_name}</b></td><td>{bk.column_name}</td>"
                    f"<td style='text-align:right; color:{color}'>{bk.score:.2f}</td>"
                    f"<td style='text-align:right'>{bk.uniqueness:.0%}</td>"
                    f"<td style='text-align:right'>{bk.null_rate:.1%}</td>"
                    f"<td style='font-size:0.85em'>{reasoning}</td></tr>"
                )
        return (
            f"<h2>Business Key Candidates</h2>"
            f"<table style='{self._table_style()}'>"
            f"<tr style='background:#f1f5f9;'><th>Table</th><th>Column</th>"
            f"<th>Score</th><th>Uniqueness</th><th>Null %</th><th>Reasoning</th></tr>"
            f"{''.join(rows)}</table>"
        )

    def _html_cross_table(self) -> str:
        assert self.ar is not None

        sim_rows = []
        for m in self.ar.similarity_matches:
            color = self._confidence_color(m.composite_score)
            sim_rows.append(
                f"<tr><td>{m.table_a}.{m.column_a}</td>"
                f"<td>{m.table_b}.{m.column_b}</td>"
                f"<td style='text-align:right; color:{color}'>{m.composite_score:.2f}</td>"
                f"<td>{m.match_type}</td>"
                f"<td style='text-align:right'>{m.name_similarity:.2f}</td>"
                f"<td style='text-align:right'>{m.statistical_similarity:.2f}</td>"
                f"<td style='text-align:right'>{m.value_overlap:.2f}</td></tr>"
            )

        rel_rows = []
        for r in self.ar.relationships:
            color = self._confidence_color(r.confidence)
            rel_rows.append(
                f"<tr><td>{r.parent_table}.{r.parent_column}</td>"
                f"<td>{r.child_table}.{r.child_column}</td>"
                f"<td style='text-align:right; color:{color}'>{r.confidence:.2f}</td>"
                f"<td>{r.cardinality}</td></tr>"
            )

        sim_table = (
            f"<h3>Column Similarity</h3>"
            f"<table style='{self._table_style()}'>"
            f"<tr style='background:#f1f5f9;'><th>Column A</th><th>Column B</th>"
            f"<th>Score</th><th>Type</th><th>Name</th><th>Stats</th><th>Values</th></tr>"
            f"{''.join(sim_rows) or '<tr><td colspan=7>None detected</td></tr>'}"
            f"</table>"
        )

        rel_table = (
            f"<h3>FK Relationships</h3>"
            f"<table style='{self._table_style()}'>"
            f"<tr style='background:#f1f5f9;'><th>Parent</th><th>Child</th>"
            f"<th>Confidence</th><th>Cardinality</th></tr>"
            f"{''.join(rel_rows) or '<tr><td colspan=4>None detected</td></tr>'}"
            f"</table>"
        )

        return f"<h2>Cross-Table Matches</h2>{sim_table}{rel_table}"

    def _html_historization(self) -> str:
        assert self.ar is not None
        rows = []
        for table_name, hist in sorted(self.ar.historization.items()):
            if hist.scd_type == SCDType.NONE:
                rows.append(
                    f"<tr><td><b>{table_name}</b></td><td>none</td>"
                    f"<td>-</td><td>-</td><td>-</td></tr>"
                )
            else:
                temporal = ", ".join(
                    f"{tc.column_name} ({tc.role})" for tc in hist.temporal_columns
                )
                vk = ", ".join(hist.version_key_columns) or "-"
                color = self._confidence_color(hist.confidence)
                rows.append(
                    f"<tr><td><b>{table_name}</b></td>"
                    f"<td>{hist.scd_type.value}</td>"
                    f"<td style='color:{color}'>{hist.confidence:.2f}</td>"
                    f"<td style='font-size:0.85em'>{temporal}</td>"
                    f"<td>{vk}</td></tr>"
                )

        return (
            f"<h2>Historization</h2>"
            f"<table style='{self._table_style()}'>"
            f"<tr style='background:#f1f5f9;'><th>Table</th><th>SCD Type</th>"
            f"<th>Confidence</th><th>Temporal Columns</th><th>Version Key</th></tr>"
            f"{''.join(rows)}</table>"
        )

    def _html_vault_spec(self) -> str:
        assert self.vs is not None

        hub_rows = []
        for h in self.vs.hubs:
            hub_rows.append(
                f"<tr><td><b>{h.hub_name}</b></td>"
                f"<td>{', '.join(h.business_key_columns)}</td>"
                f"<td>{h.hash_key_name}</td>"
                f"<td>{', '.join(h.source_tables)}</td></tr>"
            )

        link_rows = []
        for lnk in self.vs.links:
            fk_detail = "; ".join(
                f"{hub} <- {', '.join(cols)}"
                for hub, cols in lnk.foreign_key_columns.items()
            )
            link_rows.append(
                f"<tr><td><b>{lnk.link_name}</b></td>"
                f"<td>{', '.join(lnk.hub_references)}</td>"
                f"<td style='font-size:0.85em'>{fk_detail}</td>"
                f"<td>{', '.join(lnk.source_tables)}</td></tr>"
            )

        sat_rows = []
        for sat in self.vs.satellites:
            if sat.is_effectivity:
                cols_str = "(effectivity satellite)"
            else:
                cols = list(sat.descriptive_columns[:5])
                if len(sat.descriptive_columns) > 5:
                    cols.append(f"... (+{len(sat.descriptive_columns) - 5} more)")
                cols_str = ", ".join(cols)
            scd = sat.scd_type.value if sat.scd_type != SCDType.NONE else "-"
            sat_rows.append(
                f"<tr><td><b>{sat.satellite_name}</b></td>"
                f"<td>{sat.parent_name}</td>"
                f"<td style='font-size:0.85em'>{cols_str}</td>"
                f"<td>{scd}</td></tr>"
            )

        return (
            f"<h2>Data Vault Suggestion</h2>"
            f"<h3>Hubs ({len(self.vs.hubs)})</h3>"
            f"<table style='{self._table_style()}'>"
            f"<tr style='background:#f1f5f9;'><th>Hub</th><th>Business Keys</th>"
            f"<th>Hash Key</th><th>Sources</th></tr>"
            f"{''.join(hub_rows)}</table>"
            f"<h3>Links ({len(self.vs.links)})</h3>"
            f"<table style='{self._table_style()}'>"
            f"<tr style='background:#f1f5f9;'><th>Link</th><th>Hub References</th>"
            f"<th>FK Columns</th><th>Sources</th></tr>"
            f"{''.join(link_rows) or '<tr><td colspan=4>None</td></tr>'}"
            f"</table>"
            f"<h3>Satellites ({len(self.vs.satellites)})</h3>"
            f"<table style='{self._table_style()}'>"
            f"<tr style='background:#f1f5f9;'><th>Satellite</th><th>Parent</th>"
            f"<th>Columns</th><th>SCD</th></tr>"
            f"{''.join(sat_rows)}</table>"
        )

    def _html_generated_files(self) -> str:
        assert self.files is not None
        categories: dict[str, list[str]] = {
            "Staging": [],
            "Hubs": [],
            "Links": [],
            "Satellites": [],
            "YAML": [],
        }
        for path in sorted(self.files.keys()):
            name = path.rsplit("/", 1)[-1]
            if "staging/" in path and name.endswith(".sql"):
                categories["Staging"].append(name)
            elif "hubs/" in path:
                categories["Hubs"].append(name)
            elif "links/" in path:
                categories["Links"].append(name)
            elif "satellites/" in path:
                categories["Satellites"].append(name)
            elif name.endswith(".yml"):
                categories["YAML"].append(name)

        rows = []
        for cat, files in categories.items():
            if files:
                rows.append(
                    f"<tr><td><b>{cat}</b></td>"
                    f"<td>{len(files)}</td>"
                    f"<td style='font-size:0.85em'>{', '.join(files)}</td></tr>"
                )
        return (
            f"<h2>Generated Files</h2>"
            f"<table style='{self._table_style()}'>"
            f"<tr style='background:#f1f5f9;'><th>Category</th><th>Count</th>"
            f"<th>Files</th></tr>"
            f"{''.join(rows)}</table>"
        )

    @staticmethod
    def _table_style() -> str:
        return (
            "border-collapse: collapse; width: 100%; margin: 8px 0; "
            "font-size: 14px; "
            "border: 1px solid #e2e8f0; "
        )

    @staticmethod
    def _confidence_color(score: float) -> str:
        if score >= 0.8:
            return "#16a34a"  # green
        if score >= 0.5:
            return "#ca8a04"  # yellow
        return "#dc2626"  # red
