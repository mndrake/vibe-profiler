"""Tests for type inference on string columns."""


from pyspark.sql.types import StringType, StructField, StructType

from vibe_profiler.profiler.type_inference import infer_column_type


class TestInferInteger:
    def test_integer_column(self, spark):
        df = spark.createDataFrame(
            [(str(i),) for i in range(100)],
            schema=StructType([StructField("val", StringType())]),
        )
        result = infer_column_type(df, "val")
        assert result is not None
        assert result.spark_target_type == "bigint"
        assert result.confidence >= 0.80

    def test_negative_integers(self, spark):
        data = [("-10",), ("20",), ("-30",), ("40",), ("50",)]
        df = spark.createDataFrame(data, ["val"])
        result = infer_column_type(df, "val")
        assert result is not None
        assert result.spark_target_type == "bigint"


class TestInferDecimal:
    def test_decimal_column(self, spark):
        data = [("1.5",), ("2.75",), ("100.00",), ("0.99",), ("50.123",)]
        df = spark.createDataFrame(data, ["val"])
        result = infer_column_type(df, "val")
        assert result is not None
        assert result.spark_target_type == "double"
        assert result.confidence >= 0.80


class TestInferBoolean:
    def test_boolean_column(self, spark):
        data = [("true",), ("false",), ("True",), ("FALSE",), ("yes",)]
        df = spark.createDataFrame(data, ["val"])
        result = infer_column_type(df, "val")
        assert result is not None
        assert result.spark_target_type == "boolean"

    def test_yn_column(self, spark):
        data = [("Y",), ("N",), ("y",), ("n",), ("Y",)]
        df = spark.createDataFrame(data, ["val"])
        result = infer_column_type(df, "val")
        assert result is not None
        assert result.spark_target_type == "boolean"


class TestInferDate:
    def test_iso_date(self, spark):
        data = [("2024-01-15",), ("2024-02-20",), ("2024-03-10",), ("2024-04-05",)]
        df = spark.createDataFrame(data, ["val"])
        result = infer_column_type(df, "val")
        assert result is not None
        assert result.spark_target_type in ("date", "timestamp")
        assert result.format_string is not None

    def test_us_date_format(self, spark):
        data = [("01/15/2024",), ("02/20/2024",), ("03/10/2024",), ("12/25/2024",)]
        df = spark.createDataFrame(data, ["val"])
        result = infer_column_type(df, "val")
        assert result is not None
        assert result.spark_target_type == "date"
        assert result.format_string is not None

    def test_yyyymmdd_compact(self, spark):
        data = [("20240115",), ("20240220",), ("20240310",), ("20240405",)]
        df = spark.createDataFrame(data, ["val"])
        result = infer_column_type(df, "val")
        assert result is not None
        # Could be detected as integer or date depending on format match
        assert result.spark_target_type in ("date", "bigint")


class TestInferTimestamp:
    def test_iso_timestamp(self, spark):
        data = [
            ("2024-01-15 10:30:00",),
            ("2024-02-20 14:45:30",),
            ("2024-03-10 08:00:00",),
        ]
        df = spark.createDataFrame(data, ["val"])
        result = infer_column_type(df, "val")
        assert result is not None
        assert result.spark_target_type == "timestamp"
        assert "yyyy-MM-dd" in result.format_string

    def test_iso_8601_t_separator(self, spark):
        data = [
            ("2024-01-15T10:30:00",),
            ("2024-02-20T14:45:30",),
            ("2024-03-10T08:00:00",),
        ]
        df = spark.createDataFrame(data, ["val"])
        result = infer_column_type(df, "val")
        assert result is not None
        assert result.spark_target_type == "timestamp"

    def test_db2_dot_separator_format(self, spark):
        data = [
            ("2024-01-15-10.30.45.123456",),
            ("2024-02-20-14.45.30.654321",),
            ("2024-03-10-08.00.00.000000",),
        ]
        df = spark.createDataFrame(data, ["val"])
        result = infer_column_type(df, "val")
        assert result is not None
        assert result.spark_target_type == "timestamp"
        assert result.format_string == "yyyy-MM-dd-HH.mm.ss.SSSSSS"

    def test_microsecond_precision(self, spark):
        data = [
            ("2024-01-15 10:30:45.123456",),
            ("2024-02-20 14:45:30.654321",),
            ("2024-03-10 08:00:00.000001",),
        ]
        df = spark.createDataFrame(data, ["val"])
        result = infer_column_type(df, "val")
        assert result is not None
        assert result.spark_target_type == "timestamp"
        assert "SSSSSS" in result.format_string


class TestInferNoType:
    def test_free_text_stays_string(self, spark):
        data = [("hello world",), ("foo bar",), ("test data",), ("more text",)]
        df = spark.createDataFrame(data, ["val"])
        result = infer_column_type(df, "val")
        assert result is None  # no clear type

    def test_mixed_types_stays_string(self, spark):
        data = [("123",), ("abc",), ("2024-01-01",), ("true",), ("hello",)]
        df = spark.createDataFrame(data, ["val"])
        result = infer_column_type(df, "val")
        # Below confidence threshold for any single type
        assert result is None

    def test_empty_column(self, spark):
        df = spark.createDataFrame([], schema=StructType([StructField("val", StringType())]))
        result = infer_column_type(df, "val")
        assert result is None


class TestInferredTypeInProfile:
    def test_profile_includes_inferred_types(self, spark):
        """String columns should get inferred_type populated in the profile."""
        from vibe_profiler.profiler.engine import ProfileEngine

        schema = StructType([
            StructField("id", StringType()),
            StructField("amount", StringType()),
            StructField("order_date", StringType()),
            StructField("active", StringType()),
        ])
        data = [
            ("001", "99.99", "2024-01-15", "true"),
            ("002", "150.00", "2024-02-20", "false"),
            ("003", "75.50", "2024-03-10", "true"),
            ("004", "200.00", "2024-04-05", "false"),
            ("005", "50.25", "2024-05-15", "true"),
        ]
        df = spark.createDataFrame(data, schema)

        engine = ProfileEngine(spark)
        tp = engine.profile_table(df, "test_table")

        amount_cp = next(c for c in tp.column_profiles if c.column_name == "amount")
        assert amount_cp.inferred_type is not None
        assert amount_cp.inferred_type.spark_target_type == "double"

        active_cp = next(c for c in tp.column_profiles if c.column_name == "active")
        assert active_cp.inferred_type is not None
        assert active_cp.inferred_type.spark_target_type == "boolean"


class TestPreStageGeneration:
    def test_generates_pre_stage_model(self, spark):
        """Pre-stage model should be generated when string columns have inferred types."""
        from vibe_profiler.codegen.dbt_generator import DbtGenerator
        from vibe_profiler.models.profile import InferredType
        from vibe_profiler.models.profile import (
            ColumnProfile,
            PatternType,
            ProfileResult,
            TableProfile,
        )
        from vibe_profiler.models.temporal import SCDType
        from vibe_profiler.models.vault_spec import DataVaultSpec, HubSpec, SatelliteSpec

        # Profile with inferred types
        cp_id = ColumnProfile(
            "orders", "order_id", "string", 100, 0, 0.0, 100, 1.0,
            "1", "100", 3.0, 3, PatternType.NUMERIC_CODE, 0.95,
            (), False, None, None,
        )
        cp_amount = ColumnProfile(
            "orders", "amount", "string", 100, 0, 0.0, 80, 0.8,
            "1.5", "999.99", 5.0, 6, PatternType.UNKNOWN, 0.0,
            (), False, None,
            InferredType("double", None, 0.98, ("99.99", "150.00")),
        )
        cp_date = ColumnProfile(
            "orders", "order_date", "string", 100, 0, 0.0, 90, 0.9,
            "01/15/2024", "12/25/2024", 10.0, 10, PatternType.UNKNOWN, 0.0,
            (), False, None,
            InferredType("date", "MM/dd/yyyy", 0.95, ("01/15/2024",)),
        )
        tp = TableProfile("orders", None, None, 100, (cp_id, cp_amount, cp_date), False, None)
        pr = ProfileResult(tables=(tp,), profiled_at="2026-01-01")

        spec = DataVaultSpec(
            hubs=(HubSpec("hub_order", ("order_id",), ("orders",), "order_hk"),),
            links=(),
            satellites=(
                SatelliteSpec(
                    "sat_order", "hub_order", "hub", ("amount", "order_date"),
                    "orders", "hashdiff", False, SCDType.NONE,
                ),
            ),
        )

        gen = DbtGenerator(spec, profile_result=pr)
        files = gen.generate_all()

        # Should have a pre-stage file
        pre_stage_keys = [k for k in files if "pre_stg_" in k]
        assert len(pre_stage_keys) > 0, f"No pre-stage files. Keys: {list(files.keys())}"

        pre_stage = files[pre_stage_keys[0]]
        assert "CAST" in pre_stage or "TO_DATE" in pre_stage
        assert "MM/dd/yyyy" in pre_stage
        assert "DOUBLE" in pre_stage

        # Staging model should reference pre-stage
        stg_keys = [k for k in files if "stg_orders" in k and "pre_stg" not in k]
        assert len(stg_keys) > 0
        stg = files[stg_keys[0]]
        assert "pre_stg_orders" in stg
