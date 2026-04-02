"""Propose Data Vault 2.0 Hubs, Links, and Satellites from analysis results."""

from __future__ import annotations

from vibe_profiler.config import VaultConfig
from vibe_profiler.models.analysis import (
    AnalysisResult,
    Relationship,
)
from vibe_profiler.models.profile import ProfileResult
from vibe_profiler.models.temporal import SCDType
from vibe_profiler.models.vault_spec import DataVaultSpec, HubSpec, LinkSpec, SatelliteSpec
from vibe_profiler.utils import normalize_name


class VaultSuggestionEngine:
    """Transform profiling + analysis results into a Data Vault 2.0 specification."""

    def __init__(self, config: VaultConfig | None = None) -> None:
        self.cfg = config or VaultConfig()

    def suggest(
        self,
        profile_result: ProfileResult,
        analysis_result: AnalysisResult,
    ) -> DataVaultSpec:
        hubs = self._build_hubs(profile_result, analysis_result)
        hub_lookup = {h.hub_name: h for h in hubs}
        links = self._build_links(analysis_result.relationships, hub_lookup, profile_result)
        satellites = self._build_satellites(
            profile_result, analysis_result, hubs, links
        )
        source_mappings = self._build_source_mappings(hubs, links, satellites)

        return DataVaultSpec(
            hubs=tuple(hubs),
            links=tuple(links),
            satellites=tuple(satellites),
            source_mappings=source_mappings,
        )

    # ------------------------------------------------------------------
    # Hub construction
    # ------------------------------------------------------------------

    def _build_hubs(
        self,
        profile_result: ProfileResult,
        analysis: AnalysisResult,
    ) -> list[HubSpec]:
        """Create one Hub per distinct business entity.

        Columns matched via similarity across tables are merged into the same Hub.
        """
        # Union-Find to group similar BK columns into the same Hub
        bk_to_group: dict[tuple[str, str], int] = {}
        group_counter = 0

        for table_name, candidates in analysis.business_keys.items():
            if not candidates:
                continue
            top = candidates[0]
            key = (table_name, top.column_name)
            if key not in bk_to_group:
                bk_to_group[key] = group_counter
                group_counter += 1

        # Merge groups for columns that are similar
        for match in analysis.similarity_matches:
            key_a = (match.table_a, match.column_a)
            key_b = (match.table_b, match.column_b)
            if key_a in bk_to_group and key_b in bk_to_group:
                # Merge: assign both to the lower group id
                old_id = bk_to_group[key_b]
                new_id = bk_to_group[key_a]
                if old_id != new_id:
                    for k, v in bk_to_group.items():
                        if v == old_id:
                            bk_to_group[k] = new_id

        # Build hubs from groups
        groups: dict[int, list[tuple[str, str]]] = {}
        for key, gid in bk_to_group.items():
            groups.setdefault(gid, []).append(key)

        hubs: list[HubSpec] = []
        for members in groups.values():
            # Derive hub name from the first member's column, stripping suffixes
            base_name = normalize_name(members[0][1]).removesuffix("_id").removesuffix("_key")
            hub_name = f"{self.cfg.hub_prefix}{base_name}"
            bk_columns = tuple(col for _, col in members)
            source_tables = tuple(sorted({tbl for tbl, _ in members}))
            hash_key = f"{base_name}{self.cfg.hash_key_suffix}"

            hubs.append(
                HubSpec(
                    hub_name=hub_name,
                    business_key_columns=bk_columns,
                    source_tables=source_tables,
                    hash_key_name=hash_key,
                )
            )

        return hubs

    # ------------------------------------------------------------------
    # Link construction
    # ------------------------------------------------------------------

    def _build_links(
        self,
        relationships: tuple[Relationship, ...],
        hub_lookup: dict[str, HubSpec],
        profile_result: ProfileResult,
    ) -> list[LinkSpec]:
        """Create Links from detected relationships between Hubs."""
        # Map table→hub
        table_to_hub: dict[str, str] = {}
        for hub in hub_lookup.values():
            for src in hub.source_tables:
                table_to_hub[src] = hub.hub_name

        seen: set[frozenset[str]] = set()
        links: list[LinkSpec] = []

        for rel in relationships:
            parent_hub = table_to_hub.get(rel.parent_table)
            child_hub = table_to_hub.get(rel.child_table)
            if not parent_hub or not child_hub or parent_hub == child_hub:
                continue

            pair = frozenset({parent_hub, child_hub})
            if pair in seen:
                continue
            seen.add(pair)

            hub_refs = tuple(sorted(pair))
            base = "_".join(
                h.removeprefix(self.cfg.hub_prefix) for h in hub_refs
            )
            link_name = f"{self.cfg.link_prefix}{base}"
            hash_key = f"{base}{self.cfg.hash_key_suffix}"

            fk_cols: dict[str, tuple[str, ...]] = {}
            for h in hub_refs:
                hub_spec = hub_lookup[h]
                fk_cols[h] = hub_spec.business_key_columns

            source_tables = tuple(
                sorted({rel.parent_table, rel.child_table})
            )

            links.append(
                LinkSpec(
                    link_name=link_name,
                    hub_references=hub_refs,
                    foreign_key_columns=fk_cols,
                    source_tables=source_tables,
                    hash_key_name=hash_key,
                )
            )

        return links

    # ------------------------------------------------------------------
    # Satellite construction
    # ------------------------------------------------------------------

    def _build_satellites(
        self,
        profile_result: ProfileResult,
        analysis: AnalysisResult,
        hubs: list[HubSpec],
        links: list[LinkSpec],
    ) -> list[SatelliteSpec]:
        """One satellite per source table per parent hub/link for descriptive columns."""
        # Map table → hub
        table_to_hub: dict[str, HubSpec] = {}
        for hub in hubs:
            for src in hub.source_tables:
                table_to_hub[src] = hub

        # Collect BK + temporal column names to exclude from satellites
        exclude: dict[str, set[str]] = {}
        for table_name, candidates in analysis.business_keys.items():
            exclude.setdefault(table_name, set())
            for c in candidates[:1]:  # only top candidate
                exclude[table_name].add(c.column_name)
        for hist_info in analysis.historization.values():
            exclude.setdefault(hist_info.table_name, set())
            for tc in hist_info.temporal_columns:
                exclude[hist_info.table_name].add(tc.column_name)

        satellites: list[SatelliteSpec] = []

        for tp in profile_result.tables:
            hub = table_to_hub.get(tp.table_name)
            if hub is None:
                continue

            excluded = exclude.get(tp.table_name, set())
            desc_cols = tuple(
                cp.column_name
                for cp in tp.column_profiles
                if cp.column_name not in excluded
            )

            if not desc_cols:
                continue

            hist = analysis.historization.get(tp.table_name)
            scd = hist.scd_type if hist else SCDType.NONE

            base = normalize_name(tp.table_name)
            sat_name = f"{self.cfg.sat_prefix}{hub.hub_name.removeprefix(self.cfg.hub_prefix)}_{base}"

            satellites.append(
                SatelliteSpec(
                    satellite_name=sat_name,
                    parent_name=hub.hub_name,
                    parent_type="hub",
                    descriptive_columns=desc_cols,
                    source_table=tp.table_name,
                    hashdiff_name=self.cfg.hashdiff_name,
                    is_effectivity=False,
                    scd_type=scd,
                )
            )

        # Effectivity satellites for links with temporal data
        for link in links:
            for src in link.source_tables:
                hist = analysis.historization.get(src)
                if hist and hist.scd_type in (SCDType.TYPE2, SCDType.SNAPSHOT):
                    eff_name = f"{self.cfg.sat_prefix}eff_{link.link_name.removeprefix(self.cfg.link_prefix)}"
                    satellites.append(
                        SatelliteSpec(
                            satellite_name=eff_name,
                            parent_name=link.link_name,
                            parent_type="link",
                            descriptive_columns=(),
                            source_table=src,
                            hashdiff_name=self.cfg.hashdiff_name,
                            is_effectivity=True,
                            scd_type=hist.scd_type,
                        )
                    )
                    break  # one effectivity sat per link

        return satellites

    # ------------------------------------------------------------------

    @staticmethod
    def _build_source_mappings(
        hubs: list[HubSpec],
        links: list[LinkSpec],
        satellites: list[SatelliteSpec],
    ) -> dict[str, list[str]]:
        mappings: dict[str, list[str]] = {}
        for h in hubs:
            for src in h.source_tables:
                mappings.setdefault(src, []).append(h.hub_name)
        for lnk in links:
            for src in lnk.source_tables:
                mappings.setdefault(src, []).append(lnk.link_name)
        for sat in satellites:
            mappings.setdefault(sat.source_table, []).append(sat.satellite_name)
        return mappings
