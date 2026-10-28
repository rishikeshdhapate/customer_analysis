"""
Data Quality, schema validation, and cleaning transformations for PySpark Streaming.
Handles null filtering, deduplication, event-time watermarking, and date partitioning.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    to_timestamp,
    to_date,
    current_timestamp,
    coalesce,
    lit,
)


def clean_orders_stream(df: DataFrame, watermark_delay: str = "10 minutes") -> DataFrame:
    """
    Cleans raw orders stream:
    - Casts timestamps
    - Filters null primary identifiers and invalid numeric values
    - Applies watermark on order_timestamp
    - Deduplicates by order_id
    - Adds partition column order_date
    """
    cleaned = (
        df.filter(col("order_id").isNotNull() & col("customer_id").isNotNull() & col("product_id").isNotNull())
        .filter((col("quantity") > 0) & (col("total_amount") > 0.0))
        .withColumn("order_timestamp", to_timestamp(col("order_timestamp")))
        .withColumn("order_date", to_date(col("order_timestamp")))
        .withColumn("ingested_at", current_timestamp())
        .withWatermark("order_timestamp", watermark_delay)
        .dropDuplicates(["order_id"])
    )
    return cleaned


def clean_clickstream_stream(df: DataFrame, watermark_delay: str = "10 minutes") -> DataFrame:
    """
    Cleans raw clickstream:
    - Validates event types (view, add_to_cart, checkout)
    - Replaces null referrer with 'direct'
    - Applies watermark on event_timestamp
    - Deduplicates by event_id
    - Adds partition column event_date
    """
    cleaned = (
        df.filter(col("event_id").isNotNull() & col("session_id").isNotNull() & col("customer_id").isNotNull())
        .filter(col("event_type").isin(["view", "add_to_cart", "checkout"]))
        .withColumn("referrer", coalesce(col("referrer"), lit("direct")))
        .withColumn("event_timestamp", to_timestamp(col("event_timestamp")))
        .withColumn("event_date", to_date(col("event_timestamp")))
        .withColumn("ingested_at", current_timestamp())
        .withWatermark("event_timestamp", watermark_delay)
        .dropDuplicates(["event_id"])
    )
    return cleaned


def clean_payments_stream(df: DataFrame, watermark_delay: str = "10 minutes") -> DataFrame:
    """
    Cleans raw payments stream:
    - Validates payment_id, order_id, amount
    - Applies watermark on payment_timestamp
    - Deduplicates by payment_id
    - Adds partition column payment_date
    """
    cleaned = (
        df.filter(col("payment_id").isNotNull() & col("order_id").isNotNull() & col("customer_id").isNotNull())
        .filter(col("amount") > 0.0)
        .withColumn("payment_timestamp", to_timestamp(col("payment_timestamp")))
        .withColumn("payment_date", to_date(col("payment_timestamp")))
        .withColumn("ingested_at", current_timestamp())
        .withWatermark("payment_timestamp", watermark_delay)
        .dropDuplicates(["payment_id"])
    )
    return cleaned
