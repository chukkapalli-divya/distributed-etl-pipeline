"""
Airflow DAG — Distributed ETL Pipeline Orchestration
Schedules and monitors Bronze → Silver → Gold pipeline execution.
Includes fault-tolerant retry logic and data quality gate.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
import logging

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "divya_chukkapalli",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

DAG_CONFIG = {
    "dag_id": "distributed_etl_pipeline",
    "description": "Bronze → Silver → Gold Medallion ETL Pipeline",
    "schedule_interval": "0 2 * * *",  # Daily at 2 AM
    "catchup": False,
    "max_active_runs": 1,
    "tags": ["etl", "data-engineering", "medallion"],
}


def run_bronze(**context):
    """Task: Execute Bronze layer ingestion."""
    from pipeline.extract import run_bronze_pipeline
    logger.info(f"Starting Bronze pipeline | execution_date: {context['ds']}")
    _, stats = run_bronze_pipeline(
        input_path="data/sample_data.csv",
        output_path=f"output/bronze/dt={context['ds']}"
    )
    context["task_instance"].xcom_push(key="bronze_stats", value=stats)
    logger.info(f"Bronze complete: {stats['total_records']:,} records ingested")


def run_silver(**context):
    """Task: Execute Silver layer transformation."""
    from pipeline.transform import run_silver_pipeline
    bronze_stats = context["task_instance"].xcom_pull(
        task_ids="bronze_ingestion", key="bronze_stats"
    )
    logger.info(f"Starting Silver pipeline | Bronze records: {bronze_stats['total_records']:,}")
    _, stats = run_silver_pipeline(
        bronze_path=f"output/bronze/dt={context['ds']}",
        silver_path=f"output/silver/dt={context['ds']}"
    )
    context["task_instance"].xcom_push(key="silver_stats", value=stats)
    logger.info(f"Silver complete: {stats['total_records']:,} records cleaned")


def data_quality_gate(**context):
    """
    Branch: Check Silver data quality before Gold.
    Routes to Gold if quality passes, else quarantine.
    """
    silver_stats = context["task_instance"].xcom_pull(
        task_ids="silver_transform", key="silver_stats"
    )

    if silver_stats is None:
        logger.error("No Silver stats found — routing to quarantine")
        return "quarantine_data"

    null_pct = silver_stats.get("null_amounts", 1) / max(silver_stats.get("total_records", 1), 1)

    if null_pct > 0.05:
        logger.warning(f"Quality gate FAILED: null_pct={null_pct:.2%} > 5% threshold")
        return "quarantine_data"

    logger.info(f"Quality gate PASSED: null_pct={null_pct:.2%}")
    return "gold_load"


def run_gold(**context):
    """Task: Execute Gold layer aggregation."""
    from pipeline.load import run_gold_pipeline
    logger.info(f"Starting Gold pipeline | execution_date: {context['ds']}")
    run_gold_pipeline(
        silver_path=f"output/silver/dt={context['ds']}",
        gold_path=f"output/gold/dt={context['ds']}"
    )
    logger.info("Gold pipeline complete — analytics tables ready")


def send_pipeline_alert(**context):
    """Task: Log pipeline summary on completion."""
    bronze_stats = context["task_instance"].xcom_pull(
        task_ids="bronze_ingestion", key="bronze_stats"
    )
    silver_stats = context["task_instance"].xcom_pull(
        task_ids="silver_transform", key="silver_stats"
    )
    logger.info("=" * 50)
    logger.info("PIPELINE SUMMARY")
    logger.info(f"  Bronze records: {bronze_stats.get('total_records', 'N/A'):,}")
    logger.info(f"  Silver records: {silver_stats.get('total_records', 'N/A'):,}")
    logger.info(f"  Execution date: {context['ds']}")
    logger.info("=" * 50)


with DAG(default_args=DEFAULT_ARGS, **DAG_CONFIG) as dag:

    start = EmptyOperator(task_id="start")

    bronze_task = PythonOperator(
        task_id="bronze_ingestion",
        python_callable=run_bronze,
        provide_context=True,
    )

    silver_task = PythonOperator(
        task_id="silver_transform",
        python_callable=run_silver,
        provide_context=True,
    )

    quality_gate = BranchPythonOperator(
        task_id="data_quality_gate",
        python_callable=data_quality_gate,
        provide_context=True,
    )

    gold_task = PythonOperator(
        task_id="gold_load",
        python_callable=run_gold,
        provide_context=True,
    )

    quarantine = EmptyOperator(task_id="quarantine_data")

    summary = PythonOperator(
        task_id="pipeline_summary",
        python_callable=send_pipeline_alert,
        provide_context=True,
        trigger_rule="none_failed_min_one_success",
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success",
    )

    start >> bronze_task >> silver_task >> quality_gate
    quality_gate >> [gold_task, quarantine]
    gold_task >> summary >> end
    quarantine >> end
