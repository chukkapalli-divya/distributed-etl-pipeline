"""
Gold Layer — Aggregations & Business-Ready Analytics
Reads Silver layer, computes KPIs and business metrics,
and writes Gold layer analytics-ready output.
"""

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session(app_name: str = "ETL-Gold-Load") -> SparkSession:
    """Initialize Spark session with broadcast join optimization."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", "10mb")
        .config("spark.sql.shuffle.partitions", "100")
        .getOrCreate()
    )


def read_silver(spark: SparkSession, silver_path: str) -> DataFrame:
    """Read Silver Parquet layer."""
    logger.info(f"Reading Silver layer from: {silver_path}")
    df = spark.read.parquet(silver_path)
    logger.info(f"Silver records loaded: {df.count():,}")
    return df


def compute_revenue_by_region(df: DataFrame) -> DataFrame:
    """
    Gold Table 1: Daily revenue aggregated by region.
    KPI: total_revenue, avg_order_value, transaction_count.
    """
    logger.info("Computing revenue by region...")

    return (
        df.filter(F.col("is_completed") == True)
        .groupBy("event_date", "region")
        .agg(
            F.sum("revenue").alias("total_revenue"),
            F.avg("amount").alias("avg_order_value"),
            F.count("transaction_id").alias("transaction_count"),
            F.sum("quantity").alias("total_units_sold"),
            F.countDistinct("user_id").alias("unique_customers"),
        )
        .withColumn("total_revenue", F.round(F.col("total_revenue"), 2))
        .withColumn("avg_order_value", F.round(F.col("avg_order_value"), 2))
        .orderBy("event_date", "region")
    )


def compute_category_performance(df: DataFrame) -> DataFrame:
    """
    Gold Table 2: Category-level performance metrics.
    KPI: revenue share, conversion rate, avg quantity per order.
    """
    logger.info("Computing category performance...")

    total_revenue = df.filter(F.col("is_completed")).agg(
        F.sum("revenue").alias("total")
    ).collect()[0]["total"]

    return (
        df.groupBy("category")
        .agg(
            F.sum(F.when(F.col("is_completed"), F.col("revenue")).otherwise(0))
             .alias("completed_revenue"),
            F.count("transaction_id").alias("total_orders"),
            F.sum(F.col("is_completed").cast("int")).alias("completed_orders"),
            F.avg("quantity").alias("avg_quantity_per_order"),
            F.countDistinct("user_id").alias("unique_customers"),
        )
        .withColumn(
            "conversion_rate",
            F.round(F.col("completed_orders") / F.col("total_orders") * 100, 2)
        )
        .withColumn(
            "revenue_share_pct",
            F.round(F.col("completed_revenue") / total_revenue * 100, 2)
        )
        .withColumn("avg_quantity_per_order", F.round(F.col("avg_quantity_per_order"), 2))
        .orderBy(F.col("completed_revenue").desc())
    )


def compute_daily_kpis(df: DataFrame) -> DataFrame:
    """
    Gold Table 3: Daily business KPIs with 7-day rolling averages.
    Uses window functions for trend analysis.
    """
    logger.info("Computing daily KPIs with rolling averages...")

    daily = (
        df.groupBy("event_date")
        .agg(
            F.sum("revenue").alias("daily_revenue"),
            F.count("transaction_id").alias("daily_transactions"),
            F.countDistinct("user_id").alias("daily_active_users"),
            F.avg("amount").alias("avg_transaction_value"),
            F.sum(F.col("is_completed").cast("int")).alias("completed_transactions"),
        )
        .withColumn("daily_revenue", F.round(F.col("daily_revenue"), 2))
        .withColumn("avg_transaction_value", F.round(F.col("avg_transaction_value"), 2))
    )

    window_7d = (
        Window.orderBy(F.col("event_date").cast("long"))
        .rowsBetween(-6, 0)
    )

    return (
        daily
        .withColumn("revenue_7d_avg", F.round(F.avg("daily_revenue").over(window_7d), 2))
        .withColumn("txn_7d_avg", F.round(F.avg("daily_transactions").over(window_7d), 2))
        .orderBy("event_date")
    )


def compute_user_retention(df: DataFrame) -> DataFrame:
    """
    Gold Table 4: User retention — first purchase vs repeat purchase.
    Identifies new vs returning customers per day.
    """
    logger.info("Computing user retention metrics...")

    first_purchase = (
        df.filter(F.col("is_completed"))
        .groupBy("user_id")
        .agg(F.min("event_date").alias("first_purchase_date"))
    )

    return (
        df.filter(F.col("is_completed"))
        .join(first_purchase, on="user_id", how="left")
        .withColumn(
            "customer_type",
            F.when(F.col("event_date") == F.col("first_purchase_date"), "new")
             .otherwise("returning")
        )
        .groupBy("event_date", "customer_type")
        .agg(
            F.countDistinct("user_id").alias("customer_count"),
            F.sum("revenue").alias("revenue"),
        )
        .withColumn("revenue", F.round(F.col("revenue"), 2))
        .orderBy("event_date", "customer_type")
    )


def write_gold_table(df: DataFrame, path: str, table_name: str):
    """Write individual Gold table to Parquet."""
    logger.info(f"Writing Gold table '{table_name}' to: {path}")
    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .parquet(path)
    )
    logger.info(f"Gold table '{table_name}' written: {df.count():,} rows")


def run_gold_pipeline(
    silver_path: str = "output/silver",
    gold_path: str = "output/gold"
):
    """End-to-end Gold layer execution."""
    logger.info("=" * 50)
    logger.info("Starting Gold Layer Pipeline")
    logger.info("=" * 50)

    spark = create_spark_session()

    try:
        df_silver = read_silver(spark, silver_path)
        df_silver.cache()
        logger.info("Silver DataFrame cached for reuse across Gold tables")

        revenue_by_region = compute_revenue_by_region(df_silver)
        write_gold_table(revenue_by_region, f"{gold_path}/revenue_by_region", "revenue_by_region")

        category_perf = compute_category_performance(df_silver)
        write_gold_table(category_perf, f"{gold_path}/category_performance", "category_performance")

        daily_kpis = compute_daily_kpis(df_silver)
        write_gold_table(daily_kpis, f"{gold_path}/daily_kpis", "daily_kpis")

        user_retention = compute_user_retention(df_silver)
        write_gold_table(user_retention, f"{gold_path}/user_retention", "user_retention")

        df_silver.unpersist()

        logger.info("Gold pipeline completed successfully")
        logger.info(f"Gold tables written to: {gold_path}")

    except Exception as e:
        logger.error(f"Gold pipeline failed: {e}")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    run_gold_pipeline()
