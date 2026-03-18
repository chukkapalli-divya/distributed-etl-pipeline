"""
Anomaly Detection — Z-score based revenue anomaly flagging.
Identifies statistically significant deviations in daily KPIs
and writes flagged records to Gold layer for downstream alerting.
"""

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def compute_zscore_anomalies(df: DataFrame, metric_col: str, threshold: float = 3.0) -> DataFrame:
    """
    Detect anomalies using Z-score method.
    Flags records where abs(z_score) > threshold (default: 3.0 standard deviations).

    Z-score = (observed - mean) / std_dev

    Why Z-score for this use case:
    - Daily revenue follows approximately normal distribution at scale
    - Z-score > 3.0 captures ~0.3% of extreme events (true anomalies)
    - More interpretable than IQR for time-series anomaly reporting
    """
    window_all = Window.partitionBy(F.lit(1))

    df_with_stats = df.withColumns({
        "metric_mean": F.avg(metric_col).over(window_all),
        "metric_std": F.stddev(metric_col).over(window_all),
    })

    df_zscore = df_with_stats.withColumn(
        "z_score",
        F.round(
            (F.col(metric_col) - F.col("metric_mean")) / F.col("metric_std"),
            3
        )
    )

    df_flagged = df_zscore.withColumns({
        "is_anomaly": F.abs(F.col("z_score")) > threshold,
        "anomaly_direction": F.when(F.col("z_score") > threshold, "spike")
                              .when(F.col("z_score") < -threshold, "drop")
                              .otherwise("normal"),
        "deviation_pct": F.round(
            (F.col(metric_col) - F.col("metric_mean")) / F.col("metric_mean") * 100,
            2
        ),
        "threshold_used": F.lit(threshold),
    })

    anomaly_count = df_flagged.filter(F.col("is_anomaly")).count()
    logger.info(f"Anomaly detection on '{metric_col}': {anomaly_count} anomalies flagged (threshold: {threshold}σ)")

    return df_flagged.drop("metric_mean", "metric_std")


def detect_revenue_anomalies(df_daily_kpis: DataFrame) -> DataFrame:
    """
    Run anomaly detection on daily revenue and transaction count.
    Combines both signals into a unified anomaly report.
    """
    logger.info("Running revenue anomaly detection...")

    df_revenue_anomalies = compute_zscore_anomalies(df_daily_kpis, "daily_revenue", threshold=3.0)
    df_txn_anomalies = compute_zscore_anomalies(df_daily_kpis, "daily_transactions", threshold=3.0)

    df_combined = (
        df_revenue_anomalies
        .withColumnRenamed("z_score", "revenue_zscore")
        .withColumnRenamed("is_anomaly", "revenue_anomaly")
        .withColumnRenamed("anomaly_direction", "revenue_direction")
        .withColumnRenamed("deviation_pct", "revenue_deviation_pct")
        .join(
            df_txn_anomalies.select(
                "event_date",
                F.col("z_score").alias("txn_zscore"),
                F.col("is_anomaly").alias("txn_anomaly"),
                F.col("deviation_pct").alias("txn_deviation_pct"),
            ),
            on="event_date",
            how="left"
        )
        .withColumn(
            "any_anomaly",
            F.col("revenue_anomaly") | F.col("txn_anomaly")
        )
        .withColumn(
            "severity",
            F.when(F.abs(F.col("revenue_zscore")) > 5.0, "critical")
             .when(F.abs(F.col("revenue_zscore")) > 4.0, "high")
             .when(F.abs(F.col("revenue_zscore")) > 3.0, "medium")
             .otherwise("normal")
        )
        .orderBy("event_date")
    )

    total_anomalies = df_combined.filter(F.col("any_anomaly")).count()
    critical = df_combined.filter(F.col("severity") == "critical").count()
    logger.info(f"Total anomalous days: {total_anomalies} | Critical: {critical}")

    return df_combined


def run_anomaly_pipeline(
    silver_path: str = "output/silver",
    gold_path: str = "output/gold/anomaly_report"
):
    """Standalone anomaly detection pipeline."""
    from pipeline.utils.spark_utils import get_spark_session, stop_spark

    spark = get_spark_session("ETL-Anomaly-Detection", shuffle_partitions=50)

    try:
        df_silver = spark.read.parquet(silver_path)

        df_daily = (
            df_silver.groupBy("event_date")
            .agg(
                F.sum("revenue").alias("daily_revenue"),
                F.count("transaction_id").alias("daily_transactions"),
            )
        )

        df_anomalies = detect_revenue_anomalies(df_daily)

        (
            df_anomalies
            .coalesce(1)
            .write
            .mode("overwrite")
            .parquet(gold_path)
        )

        logger.info(f"Anomaly report written to: {gold_path}")

    finally:
        stop_spark(spark)


if __name__ == "__main__":
    run_anomaly_pipeline()
