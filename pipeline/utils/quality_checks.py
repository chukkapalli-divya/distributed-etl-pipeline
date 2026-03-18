"""
Data Quality Checks — Reusable assertion functions.
Used across Bronze, Silver, and Gold pipeline stages.
Raises AssertionError on critical failures to halt pipeline execution.
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def assert_no_nulls(df: DataFrame, columns: list, layer: str = ""):
    """Assert that specified columns contain zero null values."""
    for col in columns:
        null_count = df.filter(F.col(col).isNull()).count()
        assert null_count == 0, (
            f"[{layer}] CRITICAL: Column '{col}' has {null_count} null values"
        )
    logger.info(f"[{layer}] Null check PASSED for columns: {columns}")


def assert_no_negatives(df: DataFrame, columns: list, layer: str = ""):
    """Assert that numeric columns contain no negative values."""
    for col in columns:
        neg_count = df.filter(F.col(col) < 0).count()
        assert neg_count == 0, (
            f"[{layer}] CRITICAL: Column '{col}' has {neg_count} negative values"
        )
    logger.info(f"[{layer}] Negative value check PASSED for: {columns}")


def assert_no_duplicates(df: DataFrame, key_cols: list, layer: str = ""):
    """Assert that key columns form a unique identifier."""
    total = df.count()
    distinct = df.select(key_cols).distinct().count()
    assert total == distinct, (
        f"[{layer}] CRITICAL: Found {total - distinct} duplicate rows on {key_cols}"
    )
    logger.info(f"[{layer}] Uniqueness check PASSED on: {key_cols}")


def assert_min_row_count(df: DataFrame, min_rows: int, layer: str = ""):
    """Assert that DataFrame meets a minimum row count threshold."""
    count = df.count()
    assert count >= min_rows, (
        f"[{layer}] CRITICAL: Only {count} rows found, expected >= {min_rows}"
    )
    logger.info(f"[{layer}] Row count check PASSED: {count:,} rows (min: {min_rows:,})")


def assert_value_in_set(df: DataFrame, col: str, valid_values: set, layer: str = ""):
    """Assert that all values in a column belong to a known set."""
    invalid = df.filter(~F.col(col).isin(list(valid_values))).count()
    assert invalid == 0, (
        f"[{layer}] CRITICAL: Column '{col}' has {invalid} values outside {valid_values}"
    )
    logger.info(f"[{layer}] Domain check PASSED for '{col}'")


def assert_revenue_integrity(df: DataFrame, layer: str = "Gold"):
    """Assert revenue = amount * quantity for all rows."""
    mismatch = df.filter(
        F.round(F.col("revenue"), 2) !=
        F.round(F.col("amount") * F.col("quantity"), 2)
    ).count()
    assert mismatch == 0, (
        f"[{layer}] CRITICAL: {mismatch} rows have revenue != amount * quantity"
    )
    logger.info(f"[{layer}] Revenue integrity check PASSED")


def log_quality_report(df: DataFrame, layer: str):
    """Log a summary data quality report for a given layer."""
    total = df.count()
    null_counts = {
        col: df.filter(F.col(col).isNull()).count()
        for col in df.columns
    }
    logger.info(f"\n{'='*50}")
    logger.info(f"DATA QUALITY REPORT — {layer}")
    logger.info(f"Total records: {total:,}")
    logger.info("Null counts per column:")
    for col, count in null_counts.items():
        status = "OK" if count == 0 else f"WARNING: {count} nulls"
        logger.info(f"  {col}: {status}")
    logger.info(f"{'='*50}\n")
