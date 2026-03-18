"""
PyTest Configuration — Shared fixtures for all test modules.
SparkSession is session-scoped to avoid repeated startup overhead.
"""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """
    Session-scoped SparkSession for all tests.
    Local mode with 2 cores — no cluster required.
    UI disabled to reduce noise during test runs.
    """
    session = (
        SparkSession.builder
        .appName("ETL-Test-Suite")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def sample_csv_path(tmp_path_factory):
    """Write sample CSV to temp directory for integration tests."""
    import pandas as pd
    import numpy as np

    tmp_path = tmp_path_factory.mktemp("data")
    csv_path = str(tmp_path / "sample_data.csv")

    np.random.seed(42)
    n = 500
    df = pd.DataFrame({
        "transaction_id": [f"TXN{str(i).zfill(7)}" for i in range(1, n + 1)],
        "user_id": np.random.randint(1000, 9999, n),
        "product_id": [f"PROD{np.random.randint(100,999)}" for _ in range(n)],
        "category": np.random.choice(["electronics","clothing","food","health","sports"], n),
        "region": np.random.choice(["north","south","east","west","central"], n),
        "amount": np.round(np.random.uniform(10.0, 300.0, n), 2),
        "quantity": np.random.randint(1, 10, n),
        "status": np.random.choice(["completed","pending","cancelled"], n, p=[0.75,0.15,0.10]),
        "date": ["2024-01-01"] * n,
        "timestamp": ["2024-01-01 10:00:00"] * n,
    })
    df.to_csv(csv_path, index=False)
    return csv_path
