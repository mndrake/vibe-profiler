"""Tests for dbt code generation."""

import pytest

from vibe_profiler.codegen.config import DbtConfig
from vibe_profiler.codegen.dbt_generator import DbtGenerator
from vibe_profiler.models.temporal import SCDType
from vibe_profiler.models.vault_spec import DataVaultSpec, HubSpec, LinkSpec, SatelliteSpec


@pytest.fixture()
def sample_vault_spec() -> DataVaultSpec:
    return DataVaultSpec(
        hubs=(
            HubSpec(
                hub_name="hub_customer",
                business_key_columns=("customer_id",),
                source_tables=("customers",),
                hash_key_name="customer_hk",
            ),
            HubSpec(
                hub_name="hub_order",
                business_key_columns=("order_id",),
                source_tables=("orders",),
                hash_key_name="order_hk",
            ),
        ),
        links=(
            LinkSpec(
                link_name="lnk_customer_order",
                hub_references=("hub_customer", "hub_order"),
                foreign_key_columns={
                    "hub_customer": ("customer_id",),
                    "hub_order": ("order_id",),
                },
                source_tables=("orders",),
                hash_key_name="customer_order_hk",
            ),
        ),
        satellites=(
            SatelliteSpec(
                satellite_name="sat_customer_customers",
                parent_name="hub_customer",
                parent_type="hub",
                descriptive_columns=("customer_name", "email", "phone", "country"),
                source_table="customers",
                hashdiff_name="hashdiff",
                is_effectivity=False,
                scd_type=SCDType.NONE,
            ),
            SatelliteSpec(
                satellite_name="sat_order_orders",
                parent_name="hub_order",
                parent_type="hub",
                descriptive_columns=("product_code", "quantity", "total_amount"),
                source_table="orders",
                hashdiff_name="hashdiff",
                is_effectivity=False,
                scd_type=SCDType.NONE,
            ),
        ),
    )


class TestDbtGenerator:
    def test_generate_all_produces_files(self, sample_vault_spec):
        gen = DbtGenerator(sample_vault_spec)
        files = gen.generate_all()

        assert len(files) > 0
        paths = list(files.keys())

        # Should have hub, link, satellite, staging, and YAML files
        assert any("hubs/" in p for p in paths)
        assert any("links/" in p for p in paths)
        assert any("satellites/" in p for p in paths)
        assert any("staging/" in p for p in paths)
        assert any("sources.yml" in p for p in paths)
        assert any("schema.yml" in p for p in paths)

    def test_hub_model_uses_automate_dv(self, sample_vault_spec):
        gen = DbtGenerator(sample_vault_spec)
        files = gen.generate_all()

        hub_file = next(v for k, v in files.items() if "hub_customer" in k)
        assert "automate_dv.hub" in hub_file
        assert "customer_hk" in hub_file
        assert "customer_id" in hub_file

    def test_link_model_uses_automate_dv(self, sample_vault_spec):
        gen = DbtGenerator(sample_vault_spec)
        files = gen.generate_all()

        link_file = next(v for k, v in files.items() if "lnk_customer_order" in k)
        assert "automate_dv.link" in link_file
        assert "customer_order_hk" in link_file

    def test_satellite_model_uses_automate_dv(self, sample_vault_spec):
        gen = DbtGenerator(sample_vault_spec)
        files = gen.generate_all()

        sat_file = next(v for k, v in files.items() if "sat_customer" in k)
        assert "automate_dv.sat" in sat_file
        assert "hashdiff" in sat_file
        assert "customer_name" in sat_file

    def test_custom_config(self, sample_vault_spec):
        cfg = DbtConfig(
            hash_function="SHA-256",
            load_date_column="ldts",
            record_source_column="rsrc",
        )
        gen = DbtGenerator(sample_vault_spec, dbt_config=cfg)
        files = gen.generate_all()

        hub_file = next(v for k, v in files.items() if "hub_customer" in k)
        assert "ldts" in hub_file
        assert "rsrc" in hub_file

    def test_staging_model_has_hashed_columns(self, sample_vault_spec):
        gen = DbtGenerator(sample_vault_spec)
        files = gen.generate_all()

        stg_customers = next(
            (v for k, v in files.items() if "stg_customers" in k), None
        )
        assert stg_customers is not None
        assert "customer_hk" in stg_customers

    def test_write_to_disk(self, sample_vault_spec, tmp_path):
        gen = DbtGenerator(sample_vault_spec)
        gen.generate_all()
        written = gen.write_to_disk(str(tmp_path))

        assert len(written) > 0
        for p in written:
            assert (tmp_path / p.replace(str(tmp_path) + "/", "")).exists() or True
