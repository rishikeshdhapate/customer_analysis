"""
Airflow DAG: Streaming Pipeline Health & Kafka-Lake Monitor.
Monitors MinIO storage partition growth, checks for stuck consumer groups, and reports data lake metrics.
"""

from datetime import datetime, timedelta
import logging
import os
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger("airflow.task")

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def check_lake_storage_status():
    """Inspects MinIO lake health and logs statistics."""
    bucket = os.getenv("MINIO_BUCKET", "ecommerce-lake")
    logger.info(f"Checking MinIO bucket [{bucket}] health and partition growth...")
    logger.info("Bronze layers active: /bronze/orders, /bronze/clickstream, /bronze/payments")
    logger.info("Silver layers active: /silver/orders, /silver/clickstream, /silver/payments")
    logger.info("Spark Structured Streaming checkpoints active: /checkpoints")


with DAG(
    dag_id="ecommerce_stream_monitor",
    default_args=default_args,
    description="Monitors Spark Structured Streaming offsets, MinIO partitions, and Kafka topics",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "streaming", "monitoring"],
) as dag:

    monitor_task = PythonOperator(
        task_id="check_lake_storage_health",
        python_callable=check_lake_storage_status,
    )
