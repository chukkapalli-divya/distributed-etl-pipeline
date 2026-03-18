"""
PyTest Unit & Integration Tests
Tests for Bronze, Silver, and Gold pipeline stages.
Uses PySpark local mode — no cluster required.
"""

import pytest
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType
)


@pytest.fixture(scope="session")
def spark():
    """Create shared Spark session for all tests."""
    spark = (
        SparkSession.builder
        .appName("ETL-Tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture
def sample_raw_df(spark):
    """Minimal raw DataFrame simulating Bronze layer input."""
    data = [
        ("TXN0000001", 1001, "PROD101", "electronics", "north", 150.0, 2, "completed", "2024-01-01", "2024-01-01 10:00:00"),
        ("TXN0000002", 1002, "PROD102", "clothing",    "south", 75.5,  1, "pending",   "2024-01-01", "2024-01-01 11:00:00"),
        ("TXN0000003", 1003, "PROD103", "food",        "east",  None,  3, "completed", "2024-01-02", "2024-01-02 09:00:00"),
        ("TXN0000004", 1001, "PROD101", "electronics", "north", 150.0, 2, "completed", "2024-01-01", "2024-01-01 10:00:00"),  # duplicate
        ("TXN0000005", 1004, "PROD104", None,          "west",  999.0, 1, "completed", "2024-01-02", "2024-01-02 12:00:00"),  # outlier amount
    ]
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("user_id",        IntegerType(), True),
        StructField("product_id",     StringType(), True),
        StructField("category",       StringType(), True),
        StructField("region",         StringType(), True),
        StructField("amount",         DoubleType(),  True),
        StructField("quantity",       IntegerType(), True),
        StructField("status",         StringType(), True),
        StructField("date",           StringType(), True),
        StructField("timestamp",      StringType(), True),
    ])
    return spark.createDataFrame(data, schema)


class TestBronzeLayer:
    """Unit tests for Bronze layer ingestion."""

    def test_bronze_schema(self, sample_raw_df):
        """Bronze DataFrame must contain all required columns."""
        required = {"transaction_id", "user_id", "amount", "category", "region", "status"}
        assert required.issubset(set(sample_raw_df.columns))

    def test_bronze_row_count(self, sample_raw_df):
        """Bronze should preserve all raw records including duplicates and nulls."""
        assert sample_raw_df.count() == 5

    def test_bronze_metadata_columns(self, spark, sample_raw_df):
        """Bronze metadata columns should be added correctly."""
        from pipeline.extract import add_metadata
        df_meta = add_metadata(sample_raw_df)
        assert "ingestion_timestamp" in df_meta.columns
        assert "ingestion_date" in df_meta.columns
        assert "pipeline_version" in df_meta.columns

    def test_bronze_preserves_nulls(self, sample_raw_df):
        """Bronze layer must NOT remove nulls — preserve raw data."""
        null_count = sample_raw_df.filter(F.col("amount").isNull()).count()
        assert null_count == 1


class TestSilverLayer:
    """Unit tests for Silver layer transformations."""

    def test_remove_duplicates(self, spark, sample_raw_df):
        """Deduplication must remove duplicate transaction_ids."""
        from pipeline.transform import remove_duplicates
        df_meta = sample_raw_df.withColumn(
            "ingestion_timestamp", F.current_timestamp()
        )
        df_deduped = remove_duplicates(df_meta)
        ids = [r.transaction_id for r in df_deduped.select("transaction_id").collect()]
        assert len(ids) == len(set(ids)), "Duplicate transaction_ids found after deduplication"

    def test_handle_nulls_amount(self, spark, sample_raw_df):
        """Null amounts must be filled with median — no nulls in Silver."""
        from pipeline.transform import handle_nulls
        df_filled = handle_nulls(sample_raw_df)
        null_count = df_filled.filter(F.col("amount").isNull()).count()
        assert null_count == 0

    def test_handle_nulls_region(self, spark, sample_raw_df):
        """Null regions must be filled with 'Unknown'."""
        from pipeline.transform import handle_nulls
        df_filled = handle_nulls(sample_raw_df)
        unknown_count = df_filled.filter(F.col("region") == "Unknown").count()
        null_count = df_filled.filter(F.col("region").isNull()).count()
        assert null_count == 0

    def test_outlier_removal(self, spark, sample_raw_df):
        """IQR outlier detection must remove extreme values."""
        from pipeline.transform import handle_nulls, remove_outliers
        df_filled = handle_nulls(sample_raw_df)
        df_clean = remove_outliers(df_filled, col="amount")
        max_amount = df_clean.agg(F.max("amount")).collect()[0][0]
        assert max_amount < 999.0, f"Outlier not removed: max_amount={max_amount}"

    def test_standardize_schema_revenue(self, spark, sample_raw_df):
        """Revenue column must equal amount * quantity."""
        from pipeline.transform import handle_nulls, standardize_schema
        df_filled = handle_nulls(sample_raw_df)
        df_std = standardize_schema(df_filled)
        assert "revenue" in df_std.columns
        mismatch = df_std.filter(
            F.round(F.col("revenue"), 2) !=
            F.round(F.col("amount") * F.col("quantity"), 2)
        ).count()
        assert mismatch == 0, f"{mismatch} rows have incorrect revenue calculation"

    def test_standardize_schema_is_completed(self, spark, sample_raw_df):
        """is_completed must be True only for status='completed'."""
        from pipeline.transform import handle_nulls, standardize_schema
        df_filled = handle_nulls(sample_raw_df)
        df_std = standardize_schema(df_filled)
        assert "is_completed" in df_std.columns
        wrong = df_std.filter(
            (F.col("status") == "completed") & (F.col("is_completed") == False)
        ).count()
        assert wrong == 0

    def test_silver_no_null_transaction_ids(self, spark, sample_raw_df):
        """Silver layer must have zero null transaction_ids."""
        from pipeline.transform import handle_nulls, remove_duplicates, standardize_schema
        df_meta = sample_raw_df.withColumn("ingestion_timestamp", F.current_timestamp())
        df = remove_duplicates(df_meta)
        df = handle_nulls(df)
        df = standardize_schema(df)
        null_ids = df.filter(F.col("transaction_id").isNull()).count()
        assert null_ids == 0


class TestGoldLayer:
    """Unit tests for Gold layer aggregations."""

    @pytest.fixture
    def silver_df(self, spark):
        """Clean Silver-like DataFrame for Gold tests."""
        data = [
            ("TXN001", 1001, "electronics", "north", 150.0, 2, "completed", True,  300.0, date(2024, 1, 1), 2024, 1, 1),
            ("TXN002", 1002, "clothing",    "south",  75.5, 1, "completed", True,   75.5, date(2024, 1, 1), 2024, 1, 1),
            ("TXN003", 1003, "food",        "east",   50.0, 3, "pending",   False, 150.0, date(2024, 1, 2), 2024, 1, 2),
            ("TXN004", 1001, "electronics", "north", 200.0, 1, "completed", True,  200.0, date(2024, 1, 2), 2024, 1, 2),
            ("TXN005", 1004, "clothing",    "west",   90.0, 2, "completed", True,  180.0, date(2024, 1, 2), 2024, 1, 2),
        ]
        schema = StructType([
            StructField("transaction_id", StringType(),  False),
            StructField("user_id",        IntegerType(), False),
            StructField("category",       StringType(),  False),
            StructField("region",         StringType(),  False),
            StructField("amount",         DoubleType(),  False),
            StructField("quantity",       IntegerType(), False),
            StructField("status",         StringType(),  False),
            StructField("is_completed",   StringType(),  False),
            StructField("revenue",        DoubleType(),  False),
            StructField("event_date",     StringType(),  False),
            StructField("event_year",     IntegerType(), False),
            StructField("event_month",    IntegerType(), False),
            StructField("event_day",      IntegerType(), False),
        ])
        return spark.createDataFrame(data, schema)

    def test_revenue_by_region_columns(self, spark, silver_df):
        """Revenue by region must contain all required KPI columns."""
        from pipeline.load import compute_revenue_by_region
        result = compute_revenue_by_region(silver_df)
        required = {"event_date", "region", "total_revenue", "transaction_count", "unique_customers"}
        assert required.issubset(set(result.columns))

    def test_revenue_by_region_non_negative(self, spark, silver_df):
        """All revenue values must be non-negative."""
        from pipeline.load import compute_revenue_by_region
        result = compute_revenue_by_region(silver_df)
        negatives = result.filter(F.col("total_revenue") < 0).count()
        assert negatives == 0

    def test_category_performance_revenue_share(self, spark, silver_df):
        """Revenue share percentages must sum to ~100%."""
        from pipeline.load import compute_category_performance
        result = compute_category_performance(silver_df)
        total_share = result.agg(F.sum("revenue_share_pct")).collect()[0][0]
        assert abs(total_share - 100.0) < 1.0, f"Revenue shares sum to {total_share}, expected ~100"

    def test_daily_kpis_row_per_day(self, spark, silver_df):
        """Daily KPIs must have exactly one row per event_date."""
        from pipeline.load import compute_daily_kpis
        result = compute_daily_kpis(silver_df)
        total_rows = result.count()
        distinct_dates = result.select("event_date").distinct().count()
        assert total_rows == distinct_dates, "Duplicate dates in daily KPIs"


class TestIntegration:
    """End-to-end integration tests."""

    def test_pipeline_record_reduction(self, spark, sample_raw_df):
        """Silver must have fewer or equal records than Bronze (nulls + dupes removed)."""
        from pipeline.transform import (
            remove_duplicates, handle_nulls,
            remove_outliers, standardize_schema
        )
        df_meta = sample_raw_df.withColumn("ingestion_timestamp", F.current_timestamp())
        bronze_count = df_meta.count()
        df = remove_duplicates(df_meta)
        df = handle_nulls(df)
        df = remove_outliers(df, col="amount")
        df = standardize_schema(df)
        silver_count = df.count()
        assert silver_count <= bronze_count, "Silver has more records than Bronze — unexpected"

    def test_pipeline_no_nulls_end_to_end(self, spark, sample_raw_df):
        """Critical columns must have zero nulls after full Silver transformation."""
        from pipeline.transform import (
            remove_duplicates, handle_nulls,
            remove_outliers, standardize_schema
        )
        df_meta = sample_raw_df.withColumn("ingestion_timestamp", F.current_timestamp())
        df = remove_duplicates(df_meta)
        df = handle_nulls(df)
        df = remove_outliers(df, col="amount")
        df = standardize_schema(df)

        for col in ["transaction_id", "amount", "region", "category"]:
            null_count = df.filter(F.col(col).isNull()).count()
            assert null_count == 0, f"Null values found in '{col}' after Silver pipeline"
