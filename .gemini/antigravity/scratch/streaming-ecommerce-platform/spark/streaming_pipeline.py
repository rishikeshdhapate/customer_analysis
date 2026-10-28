"""
Spark Structured Streaming Pipeline.
Ingests Kafka topics (orders, clickstream, payments), validates schema,
deduplicates with watermarks, and writes to Bronze and Silver layers in MinIO (S3A).
"""

import os
import logging
from pyspark.sql.functions import col, from_json, current_timestamp

from spark.utils.spark_session import get_spark_session
from spark.schemas.event_schemas import (
    CLICKSTREAM_SCHEMA,
    ORDER_SCHEMA,
    PAYMENT_SCHEMA,
)
from spark.utils.quality_checks import (
    clean_orders_stream,
    clean_clickstream_stream,
    clean_payments_stream,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("StreamingPipeline")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_ORDERS = os.getenv("KAFKA_TOPIC_ORDERS", "ecommerce.orders")
TOPIC_CLICKSTREAM = os.getenv("KAFKA_TOPIC_CLICKSTREAM", "ecommerce.clickstream")
TOPIC_PAYMENTS = os.getenv("KAFKA_TOPIC_PAYMENTS", "ecommerce.payments")

LAKE_BUCKET = os.getenv("MINIO_BUCKET", "ecommerce-lake")
BRONZE_BASE = f"s3a://{LAKE_BUCKET}/bronze"
SILVER_BASE = f"s3a://{LAKE_BUCKET}/silver"
CHECKPOINT_BASE = f"s3a://{LAKE_BUCKET}/checkpoints"


def start_stream_processing(trigger_once: bool = False):
    """
    Launches streaming queries for orders, clickstream, and payments.
    """
    spark = get_spark_session("EcommerceStructuredStreaming")

    logger.info(f"Subscribing to Kafka brokers at: {KAFKA_BOOTSTRAP}")

    # ---------------------------------------------------------
    # 1. READ KAFKA STREAMS
    # ---------------------------------------------------------
    def read_kafka_topic(topic_name: str):
        return (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", topic_name)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )

    raw_orders_stream = read_kafka_topic(TOPIC_ORDERS)
    raw_clicks_stream = read_kafka_topic(TOPIC_CLICKSTREAM)
    raw_payments_stream = read_kafka_topic(TOPIC_PAYMENTS)

    # ---------------------------------------------------------
    # 2. BRONZE LAYER (RAW ARCHIVE)
    # ---------------------------------------------------------
    # Bronze captures raw string payload, kafka metadata, and ingestion timestamp
    def create_bronze_df(raw_stream):
        return raw_stream.select(
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("raw_payload"),
            col("topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
            current_timestamp().alias("bronze_ingested_at"),
        )

    bronze_orders = create_bronze_df(raw_orders_stream)
    bronze_clicks = create_bronze_df(raw_clicks_stream)
    bronze_payments = create_bronze_df(raw_payments_stream)

    # ---------------------------------------------------------
    # 3. SILVER LAYER (PARSED, CLEANED, DEDUPLICATED)
    # ---------------------------------------------------------
    parsed_orders = (
        raw_orders_stream.select(from_json(col("value").cast("string"), ORDER_SCHEMA).alias("data"))
        .select("data.*")
    )
    silver_orders = clean_orders_stream(parsed_orders)

    parsed_clicks = (
        raw_clicks_stream.select(from_json(col("value").cast("string"), CLICKSTREAM_SCHEMA).alias("data"))
        .select("data.*")
    )
    silver_clicks = clean_clickstream_stream(parsed_clicks)

    parsed_payments = (
        raw_payments_stream.select(from_json(col("value").cast("string"), PAYMENT_SCHEMA).alias("data"))
        .select("data.*")
    )
    silver_payments = clean_payments_stream(parsed_payments)

    # ---------------------------------------------------------
    # 4. SINK WRITERS
    # ---------------------------------------------------------
    trigger_config = {"availableNow": True} if trigger_once else {"processingTime": "10 seconds"}

    queries = []

    # Bronze Sinks
    logger.info("Initializing Bronze Sinks...")
    queries.append(
        bronze_orders.writeStream.format("parquet")
        .outputMode("append")
        .option("path", f"{BRONZE_BASE}/orders")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/bronze_orders")
        .trigger(**trigger_config)
        .start()
    )

    queries.append(
        bronze_clicks.writeStream.format("parquet")
        .outputMode("append")
        .option("path", f"{BRONZE_BASE}/clickstream")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/bronze_clicks")
        .trigger(**trigger_config)
        .start()
    )

    queries.append(
        bronze_payments.writeStream.format("parquet")
        .outputMode("append")
        .option("path", f"{BRONZE_BASE}/payments")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/bronze_payments")
        .trigger(**trigger_config)
        .start()
    )

    # Silver Sinks (Partitioned by Date)
    logger.info("Initializing Silver Sinks (Partitioned by Date)...")
    queries.append(
        silver_orders.writeStream.format("parquet")
        .outputMode("append")
        .partitionBy("order_date")
        .option("path", f"{SILVER_BASE}/orders")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/silver_orders")
        .trigger(**trigger_config)
        .start()
    )

    queries.append(
        silver_clicks.writeStream.format("parquet")
        .outputMode("append")
        .partitionBy("event_date")
        .option("path", f"{SILVER_BASE}/clickstream")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/silver_clicks")
        .trigger(**trigger_config)
        .start()
    )

    queries.append(
        silver_payments.writeStream.format("parquet")
        .outputMode("append")
        .partitionBy("payment_date")
        .option("path", f"{SILVER_BASE}/payments")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/silver_payments")
        .trigger(**trigger_config)
        .start()
    )

    logger.info("All 6 streaming queries started successfully. Awaiting execution...")

    if trigger_once:
        for q in queries:
            q.awaitTermination()
        logger.info("Batch streaming cycle completed.")
    else:
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    import sys
    run_once = "--once" in sys.argv
    start_stream_processing(trigger_once=run_once)
