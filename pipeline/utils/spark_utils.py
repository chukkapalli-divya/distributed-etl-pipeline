"""
Spark Utilities — Session factory and shared configuration.
Centralizes Spark configuration to ensure consistency across all pipeline stages.
"""

from pyspark.sql import SparkSession


def get_spark_session(
    app_name: str,
    shuffle_partitions: int = 200,
    broadcast_threshold_mb: int = 10,
    enable_aqe: bool = True,
) -> SparkSession:
    """
    Create or retrieve existing Spark session with optimized configuration.

    Optimization flags applied:
    - Adaptive Query Execution (AQE): auto-coalesces partitions post-shuffle
    - Skew join optimization: splits skewed partitions automatically
    - Broadcast join threshold: avoids sort-merge joins on small tables
    - Dynamic partition pruning: skips irrelevant partitions at runtime
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", str(enable_aqe).lower())
        .config("spark.sql.adaptive.coalescePartitions.enabled", str(enable_aqe).lower())
        .config("spark.sql.adaptive.skewJoin.enabled", str(enable_aqe).lower())
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.autoBroadcastJoinThreshold", f"{broadcast_threshold_mb}mb")
        .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.sql.parquet.mergeSchema", "false")
    )

    return builder.getOrCreate()


def stop_spark(spark: SparkSession):
    """Gracefully stop Spark session and release resources."""
    if spark:
        spark.stop()
