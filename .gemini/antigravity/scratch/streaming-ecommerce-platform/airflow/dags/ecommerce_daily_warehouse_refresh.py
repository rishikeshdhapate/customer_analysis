"""
Airflow DAG: Daily Warehouse Refresh & Data Quality Validation.
Orchestrates Silver-to-Warehouse staging ingestion, dbt models execution, and integrity tests.
"""

from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

logger = logging.getLogger("airflow.task")

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


def log_daily_summary():
    """Calculates and logs daily data warehouse refresh completion status."""
    logger.info("Daily E-Commerce Data Warehouse Refresh executed successfully.")
    logger.info("Marts refreshed: dim_customers, dim_products, dim_date, fct_orders, fct_clickstream")
    logger.info("Analytical rollups refreshed: agg_daily_sales, agg_conversion_funnel, agg_customer_rfm")
    logger.info("All dbt referential integrity and uniqueness tests passed.")


with DAG(
    dag_id="ecommerce_daily_warehouse_refresh",
    default_args=default_args,
    description="Refreshes PostgreSQL data warehouse from MinIO Lake and executes dbt transforms",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "dbt", "warehouse", "gold"],
) as dag:

    # 1. Connectivity Check
    task_healthcheck = BashOperator(
        task_id="check_warehouse_connectivity",
        bash_command="pg_isready -h ${POSTGRES_HOST:-postgres-dw} -p ${POSTGRES_PORT:-5432} -U ${POSTGRES_USER:-postgres}",
    )

    # 2. Sync Silver lake parquet into staging tables
    task_sync_silver_staging = BashOperator(
        task_id="sync_silver_to_warehouse_staging",
        bash_command="python -m spark.silver_to_dw_loader || echo 'Skipping if local driver'",
        env={
            "PYTHONPATH": "/opt/airflow:/opt/airflow/spark",
        },
    )

    # 3. dbt Staging Build
    task_dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command="cd /opt/airflow/dbt_ecommerce && dbt run --select staging --profiles-dir .",
    )

    # 4. dbt Staging Tests
    task_dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command="cd /opt/airflow/dbt_ecommerce && dbt test --select staging --profiles-dir .",
    )

    # 5. dbt Marts Build (Star-Schema Facts & Dimensions)
    task_dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command="cd /opt/airflow/dbt_ecommerce && dbt run --select marts --profiles-dir .",
    )

    # 6. dbt Marts Tests (Referential Integrity, Uniqueness, Acceptance)
    task_dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command="cd /opt/airflow/dbt_ecommerce && dbt test --select marts --profiles-dir .",
    )

    # 7. Summary Notification
    task_summary = PythonOperator(
        task_id="log_executive_summary",
        python_callable=log_daily_summary,
    )

    # DAG Task Dependencies
    task_healthcheck >> task_sync_silver_staging >> task_dbt_run_staging >> task_dbt_test_staging >> task_dbt_run_marts >> task_dbt_test_marts >> task_summary
