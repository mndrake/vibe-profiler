"""Shared test fixtures: local SparkSession and sample DataFrames."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from datetime import datetime


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("vibe-profiler-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )


@pytest.fixture()
def customers_df(spark: SparkSession) -> DataFrame:
    schema = StructType([
        StructField("customer_id", StringType()),
        StructField("customer_name", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
        StructField("country", StringType()),
        StructField("created_at", TimestampType()),
    ])
    data = [
        ("C001", "Alice", "alice@example.com", "+1-555-0001", "US", datetime(2023, 1, 1)),
        ("C002", "Bob", "bob@example.com", "+1-555-0002", "US", datetime(2023, 1, 2)),
        ("C003", "Charlie", "charlie@example.com", "+1-555-0003", "UK", datetime(2023, 1, 3)),
        ("C004", "Diana", "diana@example.com", "+1-555-0004", "DE", datetime(2023, 1, 4)),
        ("C005", "Eve", "eve@example.com", "+1-555-0005", "FR", datetime(2023, 1, 5)),
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture()
def orders_df(spark: SparkSession) -> DataFrame:
    schema = StructType([
        StructField("order_id", StringType()),
        StructField("cust_id", StringType()),
        StructField("product_code", StringType()),
        StructField("quantity", IntegerType()),
        StructField("total_amount", LongType()),
        StructField("order_date", DateType()),
    ])
    from datetime import date

    data = [
        ("ORD-001", "C001", "PROD-A", 2, 100, date(2023, 2, 1)),
        ("ORD-002", "C001", "PROD-B", 1, 50, date(2023, 2, 2)),
        ("ORD-003", "C002", "PROD-A", 3, 150, date(2023, 2, 3)),
        ("ORD-004", "C003", "PROD-C", 1, 75, date(2023, 2, 4)),
        ("ORD-005", "C004", "PROD-B", 2, 100, date(2023, 2, 5)),
        ("ORD-006", "C005", "PROD-A", 1, 50, date(2023, 2, 6)),
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture()
def orders_history_df(spark: SparkSession) -> DataFrame:
    """Orders with SCD Type 2 versioning."""
    schema = StructType([
        StructField("order_id", StringType()),
        StructField("cust_id", StringType()),
        StructField("status", StringType()),
        StructField("total_amount", LongType()),
        StructField("valid_from", TimestampType()),
        StructField("valid_to", TimestampType()),
    ])
    data = [
        ("ORD-001", "C001", "pending", 100, datetime(2023, 2, 1), datetime(2023, 2, 3)),
        ("ORD-001", "C001", "shipped", 100, datetime(2023, 2, 3), datetime(9999, 12, 31)),
        ("ORD-002", "C001", "pending", 50, datetime(2023, 2, 2), datetime(2023, 2, 4)),
        ("ORD-002", "C001", "delivered", 50, datetime(2023, 2, 4), datetime(9999, 12, 31)),
        ("ORD-003", "C002", "pending", 150, datetime(2023, 2, 3), datetime(9999, 12, 31)),
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture()
def sample_tables(customers_df, orders_df) -> dict[str, DataFrame]:
    return {"customers": customers_df, "orders": orders_df}
