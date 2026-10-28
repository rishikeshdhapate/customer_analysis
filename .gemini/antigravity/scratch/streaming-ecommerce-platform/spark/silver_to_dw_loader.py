"""
Silver Lake to Data Warehouse Loader.
Reads cleaned Silver Parquet data from MinIO and syncs it into PostgreSQL Warehouse staging tables.
Can be triggered periodically via Airflow or as a streaming microbatch listener.
"""

import os
import logging
from pyspark.sql import SparkSession
from spark.utils.spark_session import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("SilverToDWLoader")

LAKE_BUCKET = os.getenv("MINIO_BUCKET", "ecommerce-lake")
SILVER_BASE = f"s3a://{LAKE_BUCKET}/silver"

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres-dw")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "ecommerce_dw")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
JDBC_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}


def load_silver_to_postgres(spark: SparkSession):
    """Loads Silver parquet data from MinIO and writes to PostgreSQL staging tables."""
    logger.info("Connecting to MinIO and reading Silver tables...")

    # Load Orders
    try:
        df_orders = spark.read.parquet(f"{SILVER_BASE}/orders")
        logger.info(f"Loaded {df_orders.count()} records from Silver Orders.")
        df_orders.write.mode("overwrite").jdbc(
            url=JDBC_URL,
            table="public.stg_raw_orders",
            properties=JDBC_PROPERTIES,
        )
        logger.info("Silver Orders synced to stg_raw_orders.")
    except Exception as e:
        logger.warning(f"Could not load Silver Orders (table might not exist yet): {e}")

    # Load Clickstream
    try:
        df_clicks = spark.read.parquet(f"{SILVER_BASE}/clickstream")
        logger.info(f"Loaded {df_clicks.count()} records from Silver Clickstream.")
        df_clicks.write.mode("overwrite").jdbc(
            url=JDBC_URL,
            table="public.stg_raw_clickstream",
            properties=JDBC_PROPERTIES,
        )
        logger.info("Silver Clickstream synced to stg_raw_clickstream.")
    except Exception as e:
        logger.warning(f"Could not load Silver Clickstream: {e}")

    # Load Payments
    try:
        df_payments = spark.read.parquet(f"{SILVER_BASE}/payments")
        logger.info(f"Loaded {df_payments.count()} records from Silver Payments.")
        df_payments.write.mode("overwrite").jdbc(
            url=JDBC_URL,
            table="public.stg_raw_payments",
            properties=JDBC_PROPERTIES,
        )
        logger.info("Silver Payments synced to stg_raw_payments.")
    except Exception as e:
        logger.warning(f"Could not load Silver Payments: {e}")


if __name__ == "__main__":
    spark_session = get_spark_session("SilverToDWLoaderBatch")
    load_silver_to_postgres(spark_session)
    spark_session.stop()
