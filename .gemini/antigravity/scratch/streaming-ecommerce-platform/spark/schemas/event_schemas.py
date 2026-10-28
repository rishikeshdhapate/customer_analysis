"""
PySpark StructType Schemas for Kafka E-Commerce Streams.
Defines explicit field types for Schema-on-Read validation and streaming deserialization.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
    BooleanType,
)


CLICKSTREAM_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("session_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), True),
    StructField("event_type", StringType(), False),
    StructField("page_url", StringType(), True),
    StructField("referrer", StringType(), True),
    StructField("device", StringType(), True),
    StructField("event_timestamp", StringType(), False),
])

ORDER_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("session_id", StringType(), True),
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("total_amount", DoubleType(), False),
    StructField("order_status", StringType(), True),
    StructField("order_timestamp", StringType(), False),
])

PAYMENT_SCHEMA = StructType([
    StructField("payment_id", StringType(), False),
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("payment_method", StringType(), False),
    StructField("payment_status", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("payment_timestamp", StringType(), False),
])

CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("name", StringType(), False),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("signup_date", StringType(), True),
])
