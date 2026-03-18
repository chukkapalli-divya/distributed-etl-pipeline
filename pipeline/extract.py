"""
Bronze Layer — Raw Data Ingestion
Reads raw CSV data, applies minimal schema enforcement,
and writes to Parquet format partitioned by date.
No business logic — raw data preserved as-is.
"""

import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, TimestampType
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


RAW_SCHEMA = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("region", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("date", StringType(), True),
    StructField("timestamp", StringType(), True),
])


def create_spark_session(app_name: str = "ETL-Bronze-Extract") -> SparkSession:
    """Initialize Spark session with optimized configuration."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.default.parallelism", "200")
        .getOrCreate()
    )


def ingest_raw_data(spark: SparkSession, input_path: str):
    """
    Read raw CSV data with schema enforcement.
    Bronze layer — minimal transformations, preserve raw data.
    """
    logger.info(f"Ingesting raw data from: {input_path}")

    df = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(RAW_SCHEMA)
        .csv(input_path)
    )

    row_count = df.count()
    logger.info(f"Ingested {row_count:,} raw records")

    return df


def add_metadata(df):
    """Add pipeline metadata columns for lineage tracking."""
    return df.withColumns({
        "ingestion_timestamp": F.current_timestamp(),
        "ingestion_date": F.current_date(),
        "source_file": F.input_file_name(),
        "pipeline_version": F.lit("1.0.0"),
    })


def validate_bronze(df) -> dict:
    """
    Basic bronze-layer validation.
    Log counts — do NOT reject records at this stage.
    """
    total = df.count()
    null_transaction_ids = df.filter(F.col("transaction_id").isNull()).count()
    null_amounts = df.filter(F.col("amount").isNull()).count()
    null_regions = df.filter(F.col("region").isNull()).count()

    stats = {
        "total_records": total,
        "null_transaction_ids": null_transaction_ids,
        "null_amounts": null_amounts,
        "null_regions": null_regions,
        "completeness_pct": round((1 - null_amounts / total) * 100, 2),
    }

    logger.info("Bronze validation stats:")
    for k, v in stats.items():
        logger.info(f"  {k}: {v}")

    return stats


def write_bronze(df, output_path: str):
    """
    Write Bronze layer to Parquet partitioned by ingestion_date.
    Partitioning enables predicate pushdown for downstream reads.
    """
    logger.info(f"Writing Bronze layer to: {output_path}")

    (
        df.repartition("ingestion_date")
        .write
        .mode("append")
        .partitionBy("ingestion_date")
        .parquet(output_path)
    )

    logger.info("Bronze layer written successfully")


def run_bronze_pipeline(
    input_path: str = "data/sample_data.csv",
    output_path: str = "output/bronze"
):
    """End-to-end Bronze layer execution."""
    logger.info("=" * 50)
    logger.info("Starting Bronze Layer Pipeline")
    logger.info("=" * 50)

    spark = create_spark_session()

    try:
        df_raw = ingest_raw_data(spark, input_path)
        df_meta = add_metadata(df_raw)
        stats = validate_bronze(df_meta)
        write_bronze(df_meta, output_path)

        logger.info("Bronze pipeline completed successfully")
        logger.info(f"Total records ingested: {stats['total_records']:,}")
        return df_meta, stats

    except Exception as e:
        logger.error(f"Bronze pipeline failed: {e}")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    run_bronze_pipeline()
