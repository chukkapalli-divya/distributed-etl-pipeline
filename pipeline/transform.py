"""
Silver Layer — Data Cleaning & Transformation
Reads Bronze Parquet, applies data quality rules,
standardizes schema, removes duplicates and outliers,
and writes clean Silver layer for downstream analytics.
"""

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session(app_name: str = "ETL-Silver-Transform") -> SparkSession:
    """Initialize Spark session with adaptive query execution enabled."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )


def read_bronze(spark: SparkSession, bronze_path: str) -> DataFrame:
    """Read Bronze Parquet layer with predicate pushdown."""
    logger.info(f"Reading Bronze layer from: {bronze_path}")
    df = spark.read.parquet(bronze_path)
    logger.info(f"Bronze records loaded: {df.count():,}")
    return df


def remove_duplicates(df: DataFrame) -> DataFrame:
    """
    Deduplicate on transaction_id — keep latest record.
    Uses window function to rank by ingestion_timestamp.
    """
    from pyspark.sql.window import Window

    logger.info("Removing duplicate records...")
    before = df.count()

    window = Window.partitionBy("transaction_id").orderBy(
        F.col("ingestion_timestamp").desc()
    )

    df_deduped = (
        df.withColumn("row_num", F.row_number().over(window))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    after = df_deduped.count()
    logger.info(f"Duplicates removed: {before - after:,} | Remaining: {after:,}")
    return df_deduped


def handle_nulls(df: DataFrame) -> DataFrame:
    """
    Handle null values with domain-appropriate strategies:
    - amount: fill with median (robust to outliers)
    - region: fill with 'Unknown'
    - category: fill with 'Unknown'
    - Drop records with null transaction_id (primary key)
    """
    logger.info("Handling null values...")

    df = df.filter(F.col("transaction_id").isNotNull())

    median_amount = df.approxQuantile("amount", [0.5], 0.01)[0]
    df = df.fillna({
        "amount": median_amount,
        "region": "Unknown",
        "category": "Unknown",
        "status": "unknown",
    })

    logger.info(f"Null handling complete. Median amount fill: {median_amount:.2f}")
    return df


def remove_outliers(df: DataFrame, col: str = "amount") -> DataFrame:
    """
    IQR-based outlier detection and removal.
    Removes records outside [Q1 - 1.5*IQR, Q3 + 1.5*IQR].
    """
    logger.info(f"Applying IQR outlier detection on '{col}'...")

    quantiles = df.approxQuantile(col, [0.25, 0.75], 0.01)
    q1, q3 = quantiles[0], quantiles[1]
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr

    before = df.count()
    df_clean = df.filter(F.col(col).between(lower, upper))
    after = df_clean.count()

    logger.info(f"IQR bounds: [{lower:.2f}, {upper:.2f}]")
    logger.info(f"Outliers removed: {before - after:,}")
    return df_clean


def standardize_schema(df: DataFrame) -> DataFrame:
    """
    Standardize data types, column names, and derived features.
    Applies consistent formatting for downstream analytics.
    """
    logger.info("Standardizing schema...")

    df = (
        df
        .withColumn("transaction_id", F.upper(F.trim(F.col("transaction_id"))))
        .withColumn("category", F.lower(F.trim(F.col("category"))))
        .withColumn("region", F.lower(F.trim(F.col("region"))))
        .withColumn("status", F.lower(F.trim(F.col("status"))))
        .withColumn("amount", F.round(F.col("amount"), 2))
        .withColumn("event_timestamp", F.col("timestamp").cast(TimestampType()))
        .withColumn("event_date", F.to_date(F.col("date"), "yyyy-MM-dd"))
        .withColumn("event_year", F.year(F.col("event_date")))
        .withColumn("event_month", F.month(F.col("event_date")))
        .withColumn("event_day", F.dayofmonth(F.col("event_date")))
        .withColumn("revenue", F.round(F.col("amount") * F.col("quantity"), 2))
        .withColumn("is_completed", F.col("status") == "completed")
        .drop("timestamp", "date")
    )

    return df


def validate_silver(df: DataFrame) -> dict:
    """
    Silver-layer data quality assertions.
    Raises exception if critical checks fail.
    """
    logger.info("Running Silver layer validation...")

    total = df.count()
    null_ids = df.filter(F.col("transaction_id").isNull()).count()
    null_amounts = df.filter(F.col("amount").isNull()).count()
    negative_amounts = df.filter(F.col("amount") < 0).count()
    negative_revenue = df.filter(F.col("revenue") < 0).count()

    assert null_ids == 0, f"CRITICAL: {null_ids} null transaction IDs in Silver layer"
    assert null_amounts == 0, f"CRITICAL: {null_amounts} null amounts in Silver layer"
    assert negative_amounts == 0, f"CRITICAL: {negative_amounts} negative amounts"
    assert negative_revenue == 0, f"CRITICAL: {negative_revenue} negative revenue"

    stats = {
        "total_records": total,
        "null_transaction_ids": null_ids,
        "null_amounts": null_amounts,
        "negative_amounts": negative_amounts,
    }

    logger.info(f"Silver validation PASSED. Total records: {total:,}")
    return stats


def write_silver(df: DataFrame, output_path: str):
    """
    Write Silver layer partitioned by event_year and event_month.
    Sorted within partitions for optimal downstream query performance.
    """
    logger.info(f"Writing Silver layer to: {output_path}")

    (
        df
        .repartition("event_year", "event_month")
        .sortWithinPartitions("event_date", "transaction_id")
        .write
        .mode("overwrite")
        .partitionBy("event_year", "event_month")
        .parquet(output_path)
    )

    logger.info("Silver layer written successfully")


def run_silver_pipeline(
    bronze_path: str = "output/bronze",
    silver_path: str = "output/silver"
):
    """End-to-end Silver layer execution."""
    logger.info("=" * 50)
    logger.info("Starting Silver Layer Pipeline")
    logger.info("=" * 50)

    spark = create_spark_session()

    try:
        df_bronze = read_bronze(spark, bronze_path)
        df = remove_duplicates(df_bronze)
        df = handle_nulls(df)
        df = remove_outliers(df, col="amount")
        df = standardize_schema(df)
        stats = validate_silver(df)
        write_silver(df, silver_path)

        logger.info("Silver pipeline completed successfully")
        return df, stats

    except AssertionError as e:
        logger.error(f"Data quality check FAILED: {e}")
        raise

    except Exception as e:
        logger.error(f"Silver pipeline failed: {e}")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    run_silver_pipeline()
